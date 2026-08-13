/*-------------------------------------------------------------------------
 *
 * numeric.c
 *	  An exact numeric data type for the Postgres database system
 *
 * Original coding 1998, Jan Wieck.  Heavily revised 2003, Tom Lane.
 *
 * Many of the algorithmic ideas are borrowed from David M. Smith's "FM"
 * multiple-precision math library, most recently published as Algorithm
 * 786: Multiple-Precision Complex Arithmetic and Functions, ACM
 * Transactions on Mathematical Software, Vol. 24, No. 4, December 1998,
 * pages 359-367.
 *
 * Copyright (c) 1998-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/numeric.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <ctype.h>
#include <float.h>
#include <limits.h>
#include <math.h>

#include "common/hashfn.h"
#include "common/int.h"
#include "funcapi.h"
#include "lib/hyperloglog.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "port/pg_bitutils.h"	/* pg_leftmost_one_pos64() */
#include "nodes/supportnodes.h"
#include "optimizer/optimizer.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/float.h"
#include "utils/guc.h"
#include "utils/numeric.h"
#include "utils/pg_lsn.h"
#include "utils/sortsupport.h"

/* ----------
 * Uncomment the following to enable compilation of dump_numeric()
 * and dump_var() and to get a dump of any result produced by make_result().
 * ----------
#define NUMERIC_DEBUG
 */


/* ----------
 * Local data types
 *
 * Numeric values are represented in a base-NBASE floating point format.
 * Each "digit" ranges from 0 to NBASE-1.  The type NumericDigit is signed
 * and wide enough to store a digit.  We assume that NBASE*NBASE can fit in
 * an int.  Although the purely calculational routines could handle any even
 * NBASE that's less than sqrt(INT_MAX), in practice we are only interested
 * in NBASE a power of ten, so that I/O conversions and decimal rounding
 * are easy.  Also, it's actually more efficient if NBASE is rather less than
 * sqrt(INT_MAX), so that there is "headroom" for mul_var and div_var to
 * postpone processing carries.
 *
 * Values of NBASE other than 10000 are considered of historical interest only
 * and are no longer supported in any sense; no mechanism exists for the client
 * to discover the base, so every client supporting binary mode expects the
 * base-10000 format.  If you plan to change this, also note the numeric
 * abbreviation code, which assumes NBASE=10000.
 * ----------
 */

#if 0
#define NBASE		10
#define HALF_NBASE	5
#define DEC_DIGITS	1			/* decimal digits per NBASE digit */
#define MUL_GUARD_DIGITS	4	/* these are measured in NBASE digits */
#define DIV_GUARD_DIGITS	8

typedef signed char NumericDigit;
#endif

#if 0
#define NBASE		100
#define HALF_NBASE	50
#define DEC_DIGITS	2			/* decimal digits per NBASE digit */
#define MUL_GUARD_DIGITS	3	/* these are measured in NBASE digits */
#define DIV_GUARD_DIGITS	6

typedef signed char NumericDigit;
#endif

#if 1
#define NBASE		10000
#define HALF_NBASE	5000
#define DEC_DIGITS	4			/* decimal digits per NBASE digit */
#define MUL_GUARD_DIGITS	2	/* these are measured in NBASE digits */
#define DIV_GUARD_DIGITS	4

typedef int16 NumericDigit;
#endif

#define NBASE_SQR	(NBASE * NBASE)

/*
 * The Numeric type as stored on disk and in memory.
 *
 * EXPERIMENTAL REPRESENTATION -- see SPEC-numeric-as-dec128.md.  This is a
 * one-off benchmarking build, not a supported storage format change.
 *
 * A value is a scaled integer: mantissa / 10^scale.  The mantissa is a
 * signed 123-bit integer and the scale is a 4-bit field (0..15), packed
 * together into a fixed 16-byte struct with no varlena header at all
 * (pg_type.typlen = 16, typstorage = 'p').  The struct is kept as two int64
 * halves rather than a bare 128-bit field because a Datum is only
 * guaranteed 8-byte aligned, and dereferencing a misaligned native 128-bit
 * integer is undefined behaviour; two aligned 64-bit loads and a shift are
 * always correct.
 *
 * NaN, +Infinity and -Infinity are encoded as mantissa magnitudes just
 * above the largest representable 37-digit value (NUMERIC_MAX_MANT):
 * mantissa == NUMERIC_MAX_MANT+1 is NaN (NaN carries no sign, so only the
 * positive form is used), mantissa == NUMERIC_MAX_MANT+2 is +Infinity, and
 * mantissa == -(NUMERIC_MAX_MANT+2) is -Infinity.  This mirrors the classic
 * format's use of otherwise-impossible header bit patterns for specials:
 * the mantissa range between 10^37 and 2^123 is never produced by ordinary
 * arithmetic, so a handful of values in it are free to repurpose.  The scale
 * field is unused (zero) for special values.
 *
 * There is no digit array: NUMERIC_DIGITS()/NUMERIC_NDIGITS()/NUMERIC_WEIGHT
 * have no counterpart here.  Every function that used to inspect or patch
 * the packed header directly now goes through NumericVar via
 * numeric_dec128_to_var()/numeric_var_to_dec128() (the two "points of
 * coupling" the spec calls out), except for abs/negate/compare/hash, which
 * operate on the scaled-integer mantissa directly since that is at least as
 * cheap as unpacking and repacking would be.
 */

#ifndef HAVE_INT128
#error "the numeric-as-dec128 representation requires a native 128-bit integer type"
#endif

struct NumericData
{
	int64		n_hi;			/* high 64 bits of the packed value */
	uint64		n_lo;			/* low 64 bits; low 4 bits hold the scale */
};

/*
 * utils/numeric.h only forward-declares "struct NumericData" (the type's
 * contents are private to this file) and typedefs the pointer type
 * "Numeric" from it; it never typedefs the bare struct name.  Do that here,
 * locally, so the rest of this file can write "NumericData" instead of
 * "struct NumericData" everywhere -- writing the bare (undeclared-as-a-type)
 * name without this would silently parse as an implicit int under GCC's
 * K&R compatibility fallback rather than fail outright, which is exactly
 * the kind of "compiles but is nonsense" bug this whole patch is trying not
 * to introduce.
 */
typedef struct NumericData NumericData;

/*
 * utils/numeric.h can't see this struct's fields (it's forward-declared
 * there only), so DatumGetNumericCopy() has its own NUMERIC_DATUM_SIZE
 * constant mirroring this size.  Catch the two drifting apart.
 */
StaticAssertDecl(sizeof(NumericData) == 16,
				  "NUMERIC_DATUM_SIZE in numeric.h must match sizeof(NumericData)");

/*
 * Sentinel tags used for a NumericVar's "sign" field.  These are opaque
 * integer tags at the NumericVar level; they no longer double as literal
 * on-disk header bits (there is no header), but the names and values are
 * kept unchanged since dozens of call sites compare against them.
 */
#define NUMERIC_SIGN_MASK	0xC000
#define NUMERIC_POS			0x0000
#define NUMERIC_NEG			0x4000
#define NUMERIC_SPECIAL		0xC000
#define NUMERIC_NAN			0xC000
#define NUMERIC_PINF		0xD000
#define NUMERIC_NINF		0xF000

#define NUMERIC_SCALE_BITS			4
#define NUMERIC_SCALE_MASK			((uint64) 0xf)
#define NUMERIC_MAX_STORED_SCALE	15
#define NUMERIC_MAX_MANT_DIGITS		37

/* 10^18, the largest power of ten an int64 literal can carry */
#define NUMERIC_POW10_P18	((int128) INT64CONST(1000000000000000000))
/* 10^37, needs 123 bits; 2^123 = 1.06e37, so it just fits in a native int128 */
#define NUMERIC_POW10_MAX	(NUMERIC_POW10_P18 * NUMERIC_POW10_P18 * 10)

/* widest mantissa the encoding holds, and the special-value sentinels above it */
#define NUMERIC_MAX_MANT	(NUMERIC_POW10_MAX - 1)
#define NUMERIC_NAN_MANT	(NUMERIC_MAX_MANT + 1)
#define NUMERIC_PINF_MANT	(NUMERIC_MAX_MANT + 2)
#define NUMERIC_NINF_MANT	(-(NUMERIC_MAX_MANT + 2))

/*
 * Unpack/pack the two 64-bit halves into/from the mantissa and scale.
 */
static inline int128
numeric_packed(const NumericData *n)
{
	return ((int128) n->n_hi << 64) | (int128) n->n_lo;
}

static inline int
numeric_pack_scale(const NumericData *n)
{
	return (int) (n->n_lo & NUMERIC_SCALE_MASK);
}

static inline int128
numeric_pack_mant(const NumericData *n)
{
	return numeric_packed(n) >> NUMERIC_SCALE_BITS;
}

static inline void
numeric_pack_store(NumericData *n, int128 mant, int scale)
{
	int128		packed;

	/*
	 * A scale outside [0, NUMERIC_MAX_STORED_SCALE] would OR straight into the
	 * mantissa's low bits below and corrupt the value with no error.  Every
	 * write to a numeric goes through here, so assert the invariant rather
	 * than trusting each caller to have maintained it.  (Special values pass
	 * a mantissa above NUMERIC_MAX_MANT with scale 0, which is fine.)
	 */
	Assert(scale >= 0 && scale <= NUMERIC_MAX_STORED_SCALE);

	packed = ((int128) mant << NUMERIC_SCALE_BITS) |
		(int128) (unsigned int) scale;

	n->n_hi = (int64) (packed >> 64);
	n->n_lo = (uint64) packed;
}

/*
 * Is this a special (NaN or infinity) value?
 *
 * Spelled as an inline function rather than a macro so that the 128-bit load
 * and shift behind numeric_pack_mant() happens once per test.  As a macro
 * naming its argument twice, this evaluated it twice, and PostgreSQL builds
 * with -fno-strict-aliasing, which limits how much of that the compiler is
 * allowed to fold away.
 */
static inline bool
numeric_is_special_val(const NumericData *n)
{
	int128		mant = numeric_pack_mant(n);

	return mant > NUMERIC_MAX_MANT || mant < -NUMERIC_MAX_MANT;
}

#define NUMERIC_IS_SPECIAL(n)	numeric_is_special_val(n)
#define NUMERIC_IS_NAN(n)	(numeric_pack_mant(n) == NUMERIC_NAN_MANT)
#define NUMERIC_IS_PINF(n)	(numeric_pack_mant(n) == NUMERIC_PINF_MANT)
#define NUMERIC_IS_NINF(n)	(numeric_pack_mant(n) == NUMERIC_NINF_MANT)
#define NUMERIC_IS_INF(n)	(NUMERIC_IS_PINF(n) || NUMERIC_IS_NINF(n))

/*
 * Sign and display scale of a *finite* value.  (Never called on a special
 * value; the classic format's comment to that effect still applies.)
 */
#define NUMERIC_SIGN(n)		(numeric_pack_mant(n) < 0 ? NUMERIC_NEG : NUMERIC_POS)
#define NUMERIC_DSCALE(n)	(numeric_pack_scale(n))

/*
 * These three constants are NumericVar-level bounds carried over unchanged
 * from the classic packed format.  They don't describe the physical dec128
 * struct at all -- NUMERIC_DSCALE_MASK/MAX is a generic ceiling arithmetic
 * clamps display scale to before ever reaching a pack/unpack boundary, and
 * NUMERIC_WEIGHT_MAX bounds int16 n_weight, a field the classic long format
 * had and NumericVar still has (var->weight is a plain int, unaffected by
 * the storage-format change).  Both stay put; only the packed struct they
 * used to also describe went away.
 */
#define NUMERIC_DSCALE_MASK			0x3FFF
#define NUMERIC_DSCALE_MAX			NUMERIC_DSCALE_MASK
#define NUMERIC_WEIGHT_MAX			PG_INT16_MAX

/* powers of ten, used by the mantissa-level compare/scale helpers below */
static const int128 numeric_dec128_pow10[NUMERIC_MAX_MANT_DIGITS + 1] = {
	1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000,
	1000000000, 10000000000, 100000000000, 1000000000000,
	10000000000000, 100000000000000, 1000000000000000,
	10000000000000000, 100000000000000000, NUMERIC_POW10_P18,
	NUMERIC_POW10_P18 * 10, NUMERIC_POW10_P18 * 100,
	NUMERIC_POW10_P18 * 1000, NUMERIC_POW10_P18 * 10000,
	NUMERIC_POW10_P18 * 100000, NUMERIC_POW10_P18 * 1000000,
	NUMERIC_POW10_P18 * 10000000, NUMERIC_POW10_P18 * 100000000,
	NUMERIC_POW10_P18 * 1000000000, NUMERIC_POW10_P18 * 10000000000,
	NUMERIC_POW10_P18 * 100000000000, NUMERIC_POW10_P18 * 1000000000000,
	NUMERIC_POW10_P18 * 10000000000000, NUMERIC_POW10_P18 * 100000000000000,
	NUMERIC_POW10_P18 * 1000000000000000,
	NUMERIC_POW10_P18 * 10000000000000000,
	NUMERIC_POW10_P18 * 100000000000000000,
	NUMERIC_POW10_P18 * NUMERIC_POW10_P18,
	NUMERIC_POW10_P18 * NUMERIC_POW10_P18 * 10
};

/*
 * Powers of ten that fit a uint64.  Used wherever a value is known to have
 * dropped below 2^64, so that the division can be done in 64-bit arithmetic:
 * neither x86-64 nor AArch64 has a 128-bit divide instruction, so a
 * uint128 division by a *variable* divisor compiles to a libgcc __udivti3
 * call costing tens of cycles, while a uint64 division by one of these
 * constants is strength-reduced by the compiler into a multiply.
 */
static const uint64 numeric_dec128_pow10_u64[20] = {
	UINT64CONST(1),
	UINT64CONST(10),
	UINT64CONST(100),
	UINT64CONST(1000),
	UINT64CONST(10000),
	UINT64CONST(100000),
	UINT64CONST(1000000),
	UINT64CONST(10000000),
	UINT64CONST(100000000),
	UINT64CONST(1000000000),
	UINT64CONST(10000000000),
	UINT64CONST(100000000000),
	UINT64CONST(1000000000000),
	UINT64CONST(10000000000000),
	UINT64CONST(100000000000000),
	UINT64CONST(1000000000000000),
	UINT64CONST(10000000000000000),
	UINT64CONST(100000000000000000),
	UINT64CONST(1000000000000000000),
	UINT64CONST(10000000000000000000)
};

/* leading NBASE limbs an abbreviated key can possibly consult */
#define NUMERIC_ABBREV_LIMBS	4

/*
 * Multiply by 10^n, reporting overflow rather than wrapping.  Ported from
 * dec128_try_scale_up() in the dec64-prototype extension.
 */
static inline bool
numeric_dec128_try_scale_up(int128 mant, int n, int128 *result)
{
	Assert(n >= 0);

	if (unlikely(n > NUMERIC_MAX_MANT_DIGITS))
		return false;

	return !__builtin_mul_overflow(mant, numeric_dec128_pow10[n], result);
}

/*
 * Compare two finite values by aligning to a common scale and comparing
 * mantissas, without unpacking either side into a NumericVar.  Ported from
 * dec128_compare() in the dec64-prototype extension: when raising the
 * smaller side to the larger scale would overflow, the answer follows from
 * the sign alone, since a value that outgrows the encoding at the other's
 * scale is larger in magnitude than anything the other side can hold.
 */
static int
numeric_dec128_compare(Numeric num1, Numeric num2)
{
	int			s1 = numeric_pack_scale(num1);
	int			s2 = numeric_pack_scale(num2);
	int128		m1 = numeric_pack_mant(num1);
	int128		m2 = numeric_pack_mant(num2);
	int128		scaled;

	if (likely(s1 == s2))
		return (m1 > m2) ? 1 : ((m1 < m2) ? -1 : 0);

	if (s1 < s2)
	{
		if (unlikely(!numeric_dec128_try_scale_up(m1, s2 - s1, &scaled)))
			return (m1 > 0) ? 1 : -1;
		m1 = scaled;
	}
	else
	{
		if (unlikely(!numeric_dec128_try_scale_up(m2, s1 - s2, &scaled)))
			return (m2 > 0) ? -1 : 1;
		m2 = scaled;
	}

	return (m1 > m2) ? 1 : ((m1 < m2) ? -1 : 0);
}

/*
 * Reduce a finite value to canonical form by dropping fractional trailing
 * zeros, so that equal values hash alike: 1.5 and 1.50 must hash the same.
 * Ported from dec128_canonical() in the dec64-prototype extension.
 */
static void
numeric_dec128_canonical(Numeric num, NumericData *out)
{
	int128		mant = numeric_pack_mant(num);
	int			scale = numeric_pack_scale(num);
	bool		neg = (mant < 0);
	uint128		mag;

	if (mant == 0)
	{
		numeric_pack_store(out, 0, 0);
		return;
	}

	/* negate in unsigned arithmetic; see numeric_dec128_int128_to_var() */
	mag = neg ? (~(uint128) mant) + 1 : (uint128) mant;

	/*
	 * Strip trailing fractional zeroes in descending power-of-two chunks
	 * rather than one digit at a time, on the magnitude, and in 64-bit
	 * arithmetic whenever the value fits.
	 *
	 * All three details are about avoiding division instructions.  Constant
	 * divisors let the compiler strength-reduce a division into a multiply, but
	 * it only does so reliably for *unsigned* operands that fit a machine
	 * register -- signed 128-bit division by a constant still emits a libgcc
	 * __divti3 call, and so does the unsigned 128-bit form.  hash_numeric()
	 * runs once per row per side of a hash join or hash aggregate, so the
	 * difference between a multiply and a function call matters here.
	 */
#define STRIP_TRAILING_ZEROES(var, pow, n) \
	while (scale >= (n) && ((var) % (pow)) == 0) \
	{ \
		(var) /= (pow); \
		scale -= (n); \
	}

	if (likely((mag >> 64) == 0))
	{
		uint64		u = (uint64) mag;

		STRIP_TRAILING_ZEROES(u, UINT64CONST(100000000), 8);
		STRIP_TRAILING_ZEROES(u, UINT64CONST(10000), 4);
		STRIP_TRAILING_ZEROES(u, UINT64CONST(100), 2);
		STRIP_TRAILING_ZEROES(u, UINT64CONST(10), 1);

		mag = (uint128) u;
	}
	else
	{
		/* wide values are rare; take the library calls rather than duplicate */
		STRIP_TRAILING_ZEROES(mag, (uint128) UINT64CONST(100000000), 8);
		STRIP_TRAILING_ZEROES(mag, (uint128) UINT64CONST(10000), 4);
		STRIP_TRAILING_ZEROES(mag, (uint128) UINT64CONST(100), 2);
		STRIP_TRAILING_ZEROES(mag, (uint128) UINT64CONST(10), 1);
	}

#undef STRIP_TRAILING_ZEROES

	numeric_pack_store(out, neg ? -(int128) mag : (int128) mag, scale);
}

/*
 * numeric_dec128_ndigits() -
 *
 *	Number of decimal digits in a non-negative uint128, i.e. the smallest d
 *	with mag < 10^d.  Zero counts as one digit, matching how the digit string
 *	"0" is rendered.
 *
 *	Derived from the bit length rather than by trial division: log10(2) is
 *	about 0.30103, so (bits * 77) >> 8 estimates the digit count to within one,
 *	and a single comparison corrects it.  The previous version looped up to
 *	NUMERIC_MAX_MANT_DIGITS times comparing 128-bit values, and
 *	numeric_dec128_div_rscale() calls this twice for every division.
 */
static inline int
numeric_dec128_ndigits(uint128 mag)
{
	uint64		hi = (uint64) (mag >> 64);
	int			bits;
	int			d;

	/*
	 * Only legal mantissas reach here, which bounds the estimate below and
	 * keeps every numeric_dec128_pow10[] subscript in range.
	 */
	Assert(mag <= (uint128) NUMERIC_MAX_MANT);

	if (mag == 0)
		return 1;

	/* bit length; pg_leftmost_one_pos64() gives the 0-based highest set bit */
	if (hi != 0)
		bits = 65 + pg_leftmost_one_pos64(hi);
	else
		bits = 1 + pg_leftmost_one_pos64((uint64) mag);

	/*
	 * 1233/4096 approximates log10(2) from above, so this never underestimates
	 * and overestimates by at most one; one comparison corrects it downwards.
	 *
	 * Note the pre-correction estimate can be NUMERIC_MAX_MANT_DIGITS + 1: a
	 * 37-nine mantissa is 123 bits, and (123 * 1233 >> 12) + 1 is 38.  That is
	 * why the subscript below is d - 1 and not d -- numeric_dec128_pow10[] runs
	 * to NUMERIC_MAX_MANT_DIGITS inclusive, so d - 1 is always in range, while
	 * d would not be.  Since mag is bounded by NUMERIC_MAX_MANT, i.e. strictly
	 * below 10^NUMERIC_MAX_MANT_DIGITS, the correction always fires in that
	 * case and the returned value is back in range.
	 */
	d = ((bits * 1233) >> 12) + 1;
	Assert(d >= 1 && d <= NUMERIC_MAX_MANT_DIGITS + 1);

	if (d > 1 && mag < (uint128) numeric_dec128_pow10[d - 1])
		d--;

	Assert(d >= 1 && d <= NUMERIC_MAX_MANT_DIGITS);
	Assert(d == 1 || mag >= (uint128) numeric_dec128_pow10[d - 1]);

	return d;
}

/*
 * numeric_dec128_div_rscale() -
 *
 *	Reproduce select_div_scale()'s result scale directly from two packed
 *	operands, without unpacking either into a NumericVar.
 *
 *	This exists so the division fast path is *behaviour-preserving*: the
 *	scale of a numeric division result is data-dependent (see
 *	select_div_scale()), and this build already diverges from stock numeric by
 *	clamping it to NUMERIC_MAX_STORED_SCALE.  Adding a second, different rule
 *	here -- for instance always returning the maximum scale, as the dec128
 *	extension prototype does -- would change results that the Phase 3
 *	differential run has already validated.  So we mirror select_div_scale()
 *	exactly, clamp exactly as numeric_div_opt_error() does, and the fast path
 *	is then indistinguishable from the slow one apart from speed.
 *
 *	select_div_scale() needs, for each input, the base-NBASE weight of its
 *	leading nonzero limb and that limb's value.  Both follow from the mantissa
 *	without building any digit array.  For a value mant * 10^-scale whose
 *	magnitude has d decimal digits, the leading decimal digit sits at decimal
 *	exponent (d - 1 - scale), so the leading NBASE limb has weight
 *		w = floor((d - 1 - scale) / DEC_DIGITS)
 *	and that limb's value is
 *		floor(|mant| / 10^(scale + DEC_DIGITS*w))
 *	where a negative exponent means a multiply instead.  The limb is by
 *	definition below NBASE, so neither direction can overflow.
 */
static inline int
numeric_dec128_div_rscale(Numeric num1, Numeric num2)
{
	int			rscale;
	int			qweight;
	int			w[2];
	int			firstdigit[2];
	Numeric		nums[2];
	int			i;

	nums[0] = num1;
	nums[1] = num2;

	for (i = 0; i < 2; i++)
	{
		int128		mant = numeric_pack_mant(nums[i]);
		int			scale = numeric_pack_scale(nums[i]);
		uint128		mag = (mant < 0) ? (uint128) (-mant) : (uint128) mant;
		int			d;
		int			e;

		/* select_div_scale() uses weight 0 and first digit 0 for a zero */
		if (mag == 0)
		{
			w[i] = 0;
			firstdigit[i] = 0;
			continue;
		}

		d = numeric_dec128_ndigits(mag);

		/* floor division, valid for a negative numerator too */
		e = d - 1 - scale;
		w[i] = (e >= 0) ? (e / DEC_DIGITS)
			: -(((-e) + DEC_DIGITS - 1) / DEC_DIGITS);

		e = scale + DEC_DIGITS * w[i];
		if (e >= 0)
			firstdigit[i] = (int) (mag / (uint128) numeric_dec128_pow10[e]);
		else
			firstdigit[i] = (int) (mag * (uint128) numeric_dec128_pow10[-e]);

		Assert(firstdigit[i] > 0 && firstdigit[i] < NBASE);
	}

	qweight = w[0] - w[1];
	if (firstdigit[0] <= firstdigit[1])
		qweight--;

	rscale = NUMERIC_MIN_SIG_DIGITS - qweight * DEC_DIGITS;
	rscale = Max(rscale, numeric_pack_scale(num1));
	rscale = Max(rscale, numeric_pack_scale(num2));
	rscale = Max(rscale, NUMERIC_MIN_DISPLAY_SCALE);
	rscale = Min(rscale, NUMERIC_MAX_DISPLAY_SCALE);

	/* the clamp numeric_div_opt_error() applies; see its comment */
	return Min(rscale, NUMERIC_MAX_STORED_SCALE);
}

/*
 * numeric_dec128_scaled_div() -
 *
 *	Compute round(a * 10^shift / b) for non-negative a and positive b, with
 *	round-half-away-from-zero.  Ported from dec128_scaled_div() in the
 *	dec64-prototype extension; returns false on overflow instead of raising,
 *	so the caller can fall back.
 */
static inline bool
numeric_dec128_scaled_div(uint128 a, uint128 b, int shift, uint128 *result)
{
	uint128		num;
	uint128		q;
	uint128		r;
	int			i;

	Assert(b > 0);
	Assert(shift >= 0);

	if (unlikely(shift > NUMERIC_MAX_MANT_DIGITS))
		return false;

	/*
	 * Fast path: if the shifted numerator still fits, one division does the
	 * whole job.  Money-sized mantissas are nowhere near 10^37, so this is
	 * what almost every real division takes; the digit-at-a-time loop below
	 * exists for operands that genuinely fill the type.  Without the fast
	 * path the loop runs up to 37 128-bit divisions per row, which is slower
	 * than stock numeric rather than faster.
	 */
	if (likely(!__builtin_mul_overflow(a, (uint128) numeric_dec128_pow10[shift],
									   &num)))
	{
		/*
		 * Prefer 64-bit division when both operands fit.  A uint128 division by
		 * a runtime divisor is a libgcc __udivti3 call; the 64-bit form is a
		 * single hardware instruction.  Money-sized operands land here even
		 * after the scale shift.
		 */
		if ((num >> 64) == 0 && (b >> 64) == 0)
		{
			uint64		n64 = (uint64) num;
			uint64		b64 = (uint64) b;
			uint64		q64 = n64 / b64;
			uint64		r64 = n64 % b64;

			/* r64 < b64 <= UINT64_MAX, so the doubling is done in uint128 */
			if ((uint128) r64 * 2 >= (uint128) b64)
				q64++;

			*result = (uint128) q64;
			return true;
		}

		q = num / b;
		r = num % b;

		/* r < b, so r * 2 cannot overflow */
		if (r * 2 >= b)
			q++;

		*result = q;
		return true;
	}

	q = a / b;
	r = a % b;

	for (i = 0; i < shift; i++)
	{
		if (unlikely(__builtin_mul_overflow(q, (uint128) 10, &q)))
			return false;

		/* r < b <= 10^37, so r * 10 stays inside the unsigned range */
		r *= 10;
		q += r / b;
		r %= b;
	}

	if (r * 2 >= b)
	{
		if (unlikely(__builtin_add_overflow(q, (uint128) 1, &q)))
			return false;
	}

	*result = q;
	return true;
}

/*
 * numeric_dec128_div() -
 *
 *	Fast path for division, entirely on mantissas.  Only called on finite
 *	operands.  Returns false if it cannot produce the answer, in which case
 *	the caller must take the NumericVar path; a zero divisor is *not* handled
 *	here, since raising the error is the caller's job.
 *
 *	Result scale matches numeric_div_opt_error()'s exactly -- see
 *	numeric_dec128_div_rscale().
 */
static inline bool
numeric_dec128_div(Numeric num1, Numeric num2, NumericData *out)
{
	int128		m1 = numeric_pack_mant(num1);
	int128		m2 = numeric_pack_mant(num2);
	int			s1 = numeric_pack_scale(num1);
	int			s2 = numeric_pack_scale(num2);
	bool		neg = ((m1 < 0) != (m2 < 0));
	uint128		ua;
	uint128		ub;
	uint128		q;
	int			rscale;
	int			shift;

	if (unlikely(m2 == 0))
		return false;			/* caller raises division_by_zero */

	rscale = numeric_dec128_div_rscale(num1, num2);

	/*
	 * We want round(m1*10^-s1 / (m2*10^-s2) * 10^rscale), i.e.
	 * round(m1 * 10^(rscale + s2 - s1) / m2).  The shift is non-negative
	 * because rscale is at least max(s1, s2) >= s1 (select_div_scale() takes
	 * the max of both input scales), so no truncating pre-division is needed.
	 */
	shift = rscale + s2 - s1;
	if (unlikely(shift < 0))
		return false;			/* can't happen; be safe rather than wrong */

	ua = (m1 < 0) ? (uint128) (-m1) : (uint128) m1;
	ub = (m2 < 0) ? (uint128) (-m2) : (uint128) m2;

	if (unlikely(!numeric_dec128_scaled_div(ua, ub, shift, &q)))
		return false;

	if (unlikely(q > (uint128) NUMERIC_MAX_MANT))
		return false;

	numeric_pack_store(out, neg ? -(int128) q : (int128) q, rscale);
	return true;
}

/*
 * numeric_dec128_out() -
 *
 *	Render a finite dec128 value as decimal text, straight from the mantissa.
 *	Result is palloc'd.  Caller must not call this on a special value.
 *
 *	numeric_out() used to reach this same string by way of
 *	init_var_from_num() + get_str_from_var(), which built the decimal
 *	representation, threw it away in favour of the NBASE limbs, and then built
 *	it a second time from those limbs.  Text output is on the path of every
 *	client-visible query returning a numeric, so the round trip is worth
 *	skipping.  The output must stay byte-identical to get_str_from_var(),
 *	including its padding of the fraction to exactly dscale digits.
 */
static char *
numeric_dec128_out(Numeric num)
{
	int128		mant = numeric_pack_mant(num);
	int			scale = numeric_pack_scale(num);
	bool		neg = (mant < 0);
	uint128		u;
	/* sign, up to NUMERIC_MAX_MANT_DIGITS digits, decimal point, NUL */
	char		buf[NUMERIC_MAX_MANT_DIGITS + 4];
	char	   *cur = buf + sizeof(buf);
	int			i;

	Assert(!NUMERIC_IS_SPECIAL(num));
	Assert(scale >= 0 && scale <= NUMERIC_MAX_STORED_SCALE);
	Assert(mant >= -NUMERIC_MAX_MANT && mant <= NUMERIC_MAX_MANT);

	u = neg ? (uint128) (-mant) : (uint128) mant;

	*(--cur) = '\0';

	for (i = 0; i < scale; i++)
	{
		*(--cur) = (char) ('0' + (int) (u % 10));
		u /= 10;
	}

	if (scale > 0)
		*(--cur) = '.';

	/* the integer part, or a lone "0" for a value below 1 */
	if (u == 0)
		*(--cur) = '0';
	else
	{
		while (u > 0)
		{
			*(--cur) = (char) ('0' + (int) (u % 10));
			u /= 10;
		}
	}

	/*
	 * A zero mantissa is never negative in this encoding, so this can't emit
	 * "-0" where get_str_from_var() would have emitted "0".
	 */
	if (neg)
		*(--cur) = '-';

	Assert(cur >= buf);

	return pstrdup(cur);
}

/*
 * numeric_dec128_add_sub() -
 *
 *	Fast path for add/subtract that never touches NumericVar: align both
 *	operands' mantissas to a common scale (the larger of the two -- both are
 *	already within [0, NUMERIC_MAX_STORED_SCALE], so the common scale is
 *	too) and add or subtract directly as int128.  Only called on finite
 *	(non-special) operands.
 *
 *	Returns false if the fast path can't handle it: either aligning the
 *	narrower-scaled operand up would overflow int128, or the sum's magnitude
 *	exceeds what dec128 can hold.  The caller then falls back to the
 *	NumericVar path, which knows how to produce the right error.  Scale
 *	overflow is not a possibility here, since addition and subtraction never
 *	push the scale beyond max(scale1, scale2).
 *
 *	Note the sum itself cannot overflow int128 once both operands are aligned:
 *	each is bounded by NUMERIC_MAX_MANT (~10^37) and int128 holds ~1.7*10^38,
 *	so it is the NUMERIC_MAX_MANT range check, not the add, that rejects
 *	out-of-range results.
 */
static inline bool
numeric_dec128_add_sub(Numeric num1, Numeric num2, bool subtract, NumericData *out)
{
	int			s1 = numeric_pack_scale(num1);
	int			s2 = numeric_pack_scale(num2);
	int128		m1 = numeric_pack_mant(num1);
	int128		m2 = numeric_pack_mant(num2);
	int			scale;
	int128		sum;

	if (subtract)
		m2 = -m2;

	if (s1 < s2)
	{
		if (unlikely(!numeric_dec128_try_scale_up(m1, s2 - s1, &m1)))
			return false;
		scale = s2;
	}
	else if (s2 < s1)
	{
		if (unlikely(!numeric_dec128_try_scale_up(m2, s1 - s2, &m2)))
			return false;
		scale = s1;
	}
	else
		scale = s1;

	sum = m1 + m2;

	if (unlikely(sum > NUMERIC_MAX_MANT || sum < -NUMERIC_MAX_MANT))
		return false;

	numeric_pack_store(out, sum, scale);
	return true;
}

/*
 * numeric_dec128_mul() -
 *
 *	Fast path for multiplication that never touches NumericVar: multiply the
 *	mantissas directly as int128 and add the scales.  Only called on finite
 *	(non-special) operands.  Returns false when it cannot produce the answer,
 *	in which case the caller falls back to mul_var().
 *
 *	The exact product has scale s1 + s2, which can be up to twice
 *	NUMERIC_MAX_STORED_SCALE and therefore often exceeds what dec128 stores.
 *	An earlier version simply gave up in that case, which turned out to be a
 *	significant and very unevenly distributed cost: a column whose values came
 *	out of a division carries scale NUMERIC_MAX_STORED_SCALE (division always
 *	clamps to it), so s1 + s2 was 30 and the fast path *never* ran for such a
 *	column.  Measured on 1.5M rows, sum(v*v) took 61 ms on a numeric(12,2)
 *	column and 112 ms on a division-derived one holding the same magnitudes --
 *	a 1.8x penalty decided purely by where the values came from.
 *
 *	So instead of giving up we round here, exactly as the general path would:
 *	mul_var() computes the product at rscale s1 + s2 and make_result() then
 *	rounds it to NUMERIC_MAX_STORED_SCALE via round_var(), which rounds half
 *	away from zero.  Doing the same division on the mantissa is
 *	bit-for-bit equivalent, and the order of operations matters -- round first,
 *	range-check after -- because a product that overflows dec128 before
 *	rounding may well fit once rounded.
 */
static inline bool
numeric_dec128_mul(Numeric num1, Numeric num2, NumericData *out)
{
	int			s1 = numeric_pack_scale(num1);
	int			s2 = numeric_pack_scale(num2);
	int128		m1 = numeric_pack_mant(num1);
	int128		m2 = numeric_pack_mant(num2);
	int			scale = s1 + s2;
	int128		product;

	/*
	 * Both mantissas fit 37 digits, so their exact product can need up to 74
	 * and overflow int128 for large operands.  That is not a wrong answer, just
	 * one this path cannot compute: mul_var() has no width limit, so the caller
	 * still gets the right result (possibly an honest overflow error) from the
	 * general path.
	 */
	if (unlikely(__builtin_mul_overflow(m1, m2, &product)))
		return false;

	if (scale > NUMERIC_MAX_STORED_SCALE)
	{
		int			drop = scale - NUMERIC_MAX_STORED_SCALE;
		int128		divisor;
		int128		rem;

		/* s1 and s2 are each bounded by NUMERIC_MAX_STORED_SCALE */
		Assert(drop >= 1 && drop <= NUMERIC_MAX_STORED_SCALE);

		divisor = numeric_dec128_pow10[drop];
		rem = product % divisor;
		product /= divisor;

		/*
		 * Round half away from zero, matching round_var().  C truncates
		 * division toward zero and gives the remainder the dividend's sign, so
		 * the two directions need separate nudges.  rem is bounded by divisor
		 * (at most 10^15), so the doubling cannot overflow.
		 */
		if (rem > 0 && rem * 2 >= divisor)
			product++;
		else if (rem < 0 && (-rem) * 2 >= divisor)
			product--;

		scale = NUMERIC_MAX_STORED_SCALE;
	}

	if (unlikely(product > NUMERIC_MAX_MANT || product < -NUMERIC_MAX_MANT))
		return false;

	numeric_pack_store(out, product, scale);
	return true;
}

/* ----------
 * NumericVar is the format we use for arithmetic.  The digit-array part
 * is the same as the NumericData storage format, but the header is more
 * complex.
 *
 * The value represented by a NumericVar is determined by the sign, weight,
 * ndigits, and digits[] array.  If it is a "special" value (NaN or Inf)
 * then only the sign field matters; ndigits should be zero, and the weight
 * and dscale fields are ignored.
 *
 * Note: the first digit of a NumericVar's value is assumed to be multiplied
 * by NBASE ** weight.  Another way to say it is that there are weight+1
 * digits before the decimal point.  It is possible to have weight < 0.
 *
 * buf points at the physical start of the palloc'd digit buffer for the
 * NumericVar.  digits points at the first digit in actual use (the one
 * with the specified weight).  We normally leave an unused digit or two
 * (preset to zeroes) between buf and digits, so that there is room to store
 * a carry out of the top digit without reallocating space.  We just need to
 * decrement digits (and increment weight) to make room for the carry digit.
 * (There is no such extra space in a numeric value stored in the database,
 * only in a NumericVar in memory.)
 *
 * If buf is NULL then the digit buffer isn't actually palloc'd and should
 * not be freed --- see the constants below for an example.
 *
 * dscale, or display scale, is the nominal precision expressed as number
 * of digits after the decimal point (it must always be >= 0 at present).
 * dscale may be more than the number of physically stored fractional digits,
 * implying that we have suppressed storage of significant trailing zeroes.
 * It should never be less than the number of stored digits, since that would
 * imply hiding digits that are present.  NOTE that dscale is always expressed
 * in *decimal* digits, and so it may correspond to a fractional number of
 * base-NBASE digits --- divide by DEC_DIGITS to convert to NBASE digits.
 *
 * rscale, or result scale, is the target precision for a computation.
 * Like dscale it is expressed as number of *decimal* digits after the decimal
 * point, and is always >= 0 at present.
 * Note that rscale is not stored in variables --- it's figured on-the-fly
 * from the dscales of the inputs.
 *
 * While we consistently use "weight" to refer to the base-NBASE weight of
 * a numeric value, it is convenient in some scale-related calculations to
 * make use of the base-10 weight (ie, the approximate log10 of the value).
 * To avoid confusion, such a decimal-units weight is called a "dweight".
 *
 * NB: All the variable-level functions are written in a style that makes it
 * possible to give one and the same variable as argument and destination.
 * This is feasible because the digit buffer is separate from the variable.
 * ----------
 */
typedef struct NumericVar
{
	int			ndigits;		/* # of digits in digits[] - can be 0! */
	int			weight;			/* weight of first digit */
	int			sign;			/* NUMERIC_POS, _NEG, _NAN, _PINF, or _NINF */
	int			dscale;			/* display scale */
	NumericDigit *buf;			/* start of palloc'd space for digits[] */
	NumericDigit *digits;		/* base-NBASE digits */
} NumericVar;


/* ----------
 * Data for generate_series
 * ----------
 */
typedef struct
{
	NumericVar	current;
	NumericVar	stop;
	NumericVar	step;
} generate_series_numeric_fctx;


/* ----------
 * Sort support.
 * ----------
 */
typedef struct
{
	/*
	 * EXPERIMENTAL (SPEC-numeric-as-dec128.md): the "void *buf" realignment
	 * buffer that used to live here is gone.  It existed so that a
	 * short-header varlena could be copied somewhere aligned before being read
	 * as a Numeric; a fixed-width, non-toastable dec128 value is read in place.
	 */
	int64		input_count;	/* number of non-null values seen */
	bool		estimating;		/* true if estimating cardinality */

	hyperLogLogState abbr_card; /* cardinality estimator */
} NumericSortSupport;


/* ----------
 * Fast sum accumulator.
 *
 * NumericSumAccum is used to implement SUM(), and other standard aggregates
 * that track the sum of input values.  It uses 32-bit integers to store the
 * digits, instead of the normal 16-bit integers (with NBASE=10000).  This
 * way, we can safely accumulate up to NBASE - 1 values without propagating
 * carry, before risking overflow of any of the digits.  'num_uncarried'
 * tracks how many values have been accumulated without propagating carry.
 *
 * Positive and negative values are accumulated separately, in 'pos_digits'
 * and 'neg_digits'.  This is simpler and faster than deciding whether to add
 * or subtract from the current value, for each new value (see sub_var() for
 * the logic we avoid by doing this).  Both buffers are of same size, and
 * have the same weight and scale.  In accum_sum_final(), the positive and
 * negative sums are added together to produce the final result.
 *
 * When a new value has a larger ndigits or weight than the accumulator
 * currently does, the accumulator is enlarged to accommodate the new value.
 * We normally have one zero digit reserved for carry propagation, and that
 * is indicated by the 'have_carry_space' flag.  When accum_sum_carry() uses
 * up the reserved digit, it clears the 'have_carry_space' flag.  The next
 * call to accum_sum_add() will enlarge the buffer, to make room for the
 * extra digit, and set the flag again.
 *
 * To initialize a new accumulator, simply reset all fields to zeros.
 *
 * The accumulator does not handle NaNs.
 * ----------
 */
typedef struct NumericSumAccum
{
	int			ndigits;
	int			weight;
	int			dscale;
	int			num_uncarried;
	bool		have_carry_space;
	int32	   *pos_digits;
	int32	   *neg_digits;
} NumericSumAccum;


/*
 * We define our own macros for packing and unpacking abbreviated-key
 * representations for numeric values in order to avoid depending on
 * USE_FLOAT8_BYVAL.  The type of abbreviation we use is based only on
 * the size of a datum, not the argument-passing convention for float8.
 *
 * The range of abbreviations for finite values is from +PG_INT64/32_MAX
 * to -PG_INT64/32_MAX.  NaN has the abbreviation PG_INT64/32_MIN, and we
 * define the sort ordering to make that work out properly (see further
 * comments below).  PINF and NINF share the abbreviations of the largest
 * and smallest finite abbreviation classes.
 */
#define NUMERIC_ABBREV_BITS (SIZEOF_DATUM * BITS_PER_BYTE)
#if SIZEOF_DATUM == 8
#define NumericAbbrevGetDatum(X) ((Datum) (X))
#define DatumGetNumericAbbrev(X) ((int64) (X))
#define NUMERIC_ABBREV_NAN		 NumericAbbrevGetDatum(PG_INT64_MIN)
#define NUMERIC_ABBREV_PINF		 NumericAbbrevGetDatum(-PG_INT64_MAX)
#define NUMERIC_ABBREV_NINF		 NumericAbbrevGetDatum(PG_INT64_MAX)
#else
#define NumericAbbrevGetDatum(X) ((Datum) (X))
#define DatumGetNumericAbbrev(X) ((int32) (X))
#define NUMERIC_ABBREV_NAN		 NumericAbbrevGetDatum(PG_INT32_MIN)
#define NUMERIC_ABBREV_PINF		 NumericAbbrevGetDatum(-PG_INT32_MAX)
#define NUMERIC_ABBREV_NINF		 NumericAbbrevGetDatum(PG_INT32_MAX)
#endif


/* ----------
 * Some preinitialized constants
 * ----------
 */
static const NumericDigit const_zero_data[1] = {0};
static const NumericVar const_zero =
{0, 0, NUMERIC_POS, 0, NULL, (NumericDigit *) const_zero_data};

static const NumericDigit const_one_data[1] = {1};
static const NumericVar const_one =
{1, 0, NUMERIC_POS, 0, NULL, (NumericDigit *) const_one_data};

static const NumericVar const_minus_one =
{1, 0, NUMERIC_NEG, 0, NULL, (NumericDigit *) const_one_data};

static const NumericDigit const_two_data[1] = {2};
static const NumericVar const_two =
{1, 0, NUMERIC_POS, 0, NULL, (NumericDigit *) const_two_data};

#if DEC_DIGITS == 4
static const NumericDigit const_zero_point_nine_data[1] = {9000};
#elif DEC_DIGITS == 2
static const NumericDigit const_zero_point_nine_data[1] = {90};
#elif DEC_DIGITS == 1
static const NumericDigit const_zero_point_nine_data[1] = {9};
#endif
static const NumericVar const_zero_point_nine =
{1, -1, NUMERIC_POS, 1, NULL, (NumericDigit *) const_zero_point_nine_data};

#if DEC_DIGITS == 4
static const NumericDigit const_one_point_one_data[2] = {1, 1000};
#elif DEC_DIGITS == 2
static const NumericDigit const_one_point_one_data[2] = {1, 10};
#elif DEC_DIGITS == 1
static const NumericDigit const_one_point_one_data[2] = {1, 1};
#endif
static const NumericVar const_one_point_one =
{2, 0, NUMERIC_POS, 1, NULL, (NumericDigit *) const_one_point_one_data};

static const NumericVar const_nan =
{0, 0, NUMERIC_NAN, 0, NULL, NULL};

static const NumericVar const_pinf =
{0, 0, NUMERIC_PINF, 0, NULL, NULL};

static const NumericVar const_ninf =
{0, 0, NUMERIC_NINF, 0, NULL, NULL};

#if DEC_DIGITS == 4
static const int round_powers[4] = {0, 1000, 100, 10};
#endif


/* ----------
 * Local functions
 * ----------
 */

#ifdef NUMERIC_DEBUG
static void dump_numeric(const char *str, Numeric num);
static void dump_var(const char *str, NumericVar *var);
#else
#define dump_numeric(s,n)
#define dump_var(s,v)
#endif

#define digitbuf_alloc(ndigits)  \
	((NumericDigit *) palloc((ndigits) * sizeof(NumericDigit)))
#define digitbuf_free(buf)	\
	do { \
		 if ((buf) != NULL) \
			 pfree(buf); \
	} while (0)

#define init_var(v)		memset(v, 0, sizeof(NumericVar))

/*
 * NUMERIC_DIGITS/NUMERIC_NDIGITS/NUMERIC_CAN_BE_SHORT have no counterpart:
 * dec128 has no digit array and no short/long header choice.  Every former
 * caller now goes through NumericVar (numeric_dec128_to_var()) or operates
 * on the mantissa directly (numeric_dec128_compare(), numeric_dec128_canonical()).
 */

static void alloc_var(NumericVar *var, int ndigits);
static void free_var(NumericVar *var);
static void zero_var(NumericVar *var);

static bool set_var_from_str(const char *str, const char *cp,
							 NumericVar *dest, const char **endptr,
							 Node *escontext);
static bool set_var_from_non_decimal_integer_str(const char *str,
												 const char *cp, int sign,
												 int base, NumericVar *dest,
												 const char **endptr,
												 Node *escontext);
static void set_var_from_num(Numeric num, NumericVar *dest);
static void init_var_from_num(Numeric num, NumericVar *dest);
static void numeric_dec128_to_var(Numeric num, NumericVar *dest);
static void numeric_dec128_int128_to_var(int128 val, int scale,
										 NumericVar *dest);
static Datum numeric_abbrev_convert_dec128(Numeric value,
										   NumericSortSupport *nss);
static void numeric_var_to_dec128(const NumericVar *var, NumericData *out,
								  bool *have_error);
static void set_var_from_var(const NumericVar *value, NumericVar *dest);
static char *get_str_from_var(const NumericVar *var);
static char *get_str_from_var_sci(const NumericVar *var, int rscale);

static void numericvar_serialize(StringInfo buf, const NumericVar *var);
static void numericvar_deserialize(StringInfo buf, NumericVar *var);

static Numeric duplicate_numeric(Numeric num);
static Numeric make_result(const NumericVar *var);
static Numeric make_result_opt_error(const NumericVar *var, bool *have_error);

static bool apply_typmod(NumericVar *var, int32 typmod, Node *escontext);
static bool apply_typmod_special(Numeric num, int32 typmod, Node *escontext);

static bool numericvar_to_int32(const NumericVar *var, int32 *result);
static bool numericvar_to_int64(const NumericVar *var, int64 *result);
static void int64_to_numericvar(int64 val, NumericVar *var);
static bool numericvar_to_uint64(const NumericVar *var, uint64 *result);
#ifdef HAVE_INT128
static bool numericvar_to_int128(const NumericVar *var, int128 *result);
static void int128_to_numericvar(int128 val, NumericVar *var);
#endif
static double numericvar_to_double_no_overflow(const NumericVar *var);

static Datum numeric_abbrev_convert(Datum original_datum, SortSupport ssup);
static bool numeric_abbrev_abort(int memtupcount, SortSupport ssup);
static int	numeric_fast_cmp(Datum x, Datum y, SortSupport ssup);
static int	numeric_cmp_abbrev(Datum x, Datum y, SortSupport ssup);

static Datum numeric_abbrev_convert_var(const NumericVar *var,
										NumericSortSupport *nss);

static int	cmp_numerics(Numeric num1, Numeric num2);
static int	cmp_var(const NumericVar *var1, const NumericVar *var2);
static int	cmp_var_common(const NumericDigit *var1digits, int var1ndigits,
						   int var1weight, int var1sign,
						   const NumericDigit *var2digits, int var2ndigits,
						   int var2weight, int var2sign);
static void add_var(const NumericVar *var1, const NumericVar *var2,
					NumericVar *result);
static void sub_var(const NumericVar *var1, const NumericVar *var2,
					NumericVar *result);
static void mul_var(const NumericVar *var1, const NumericVar *var2,
					NumericVar *result,
					int rscale);
static void mul_var_short(const NumericVar *var1, const NumericVar *var2,
						  NumericVar *result);
static void div_var(const NumericVar *var1, const NumericVar *var2,
					NumericVar *result, int rscale, bool round, bool exact);
static void div_var_int(const NumericVar *var, int ival, int ival_weight,
						NumericVar *result, int rscale, bool round);
#ifdef HAVE_INT128
static void div_var_int64(const NumericVar *var, int64 ival, int ival_weight,
						  NumericVar *result, int rscale, bool round);
#endif
static int	select_div_scale(const NumericVar *var1, const NumericVar *var2);
static void mod_var(const NumericVar *var1, const NumericVar *var2,
					NumericVar *result);
static void div_mod_var(const NumericVar *var1, const NumericVar *var2,
						NumericVar *quot, NumericVar *rem);
static void ceil_var(const NumericVar *var, NumericVar *result);
static void floor_var(const NumericVar *var, NumericVar *result);

static void gcd_var(const NumericVar *var1, const NumericVar *var2,
					NumericVar *result);
static void sqrt_var(const NumericVar *arg, NumericVar *result, int rscale);
static void exp_var(const NumericVar *arg, NumericVar *result, int rscale);
static int	estimate_ln_dweight(const NumericVar *var);
static void ln_var(const NumericVar *arg, NumericVar *result, int rscale);
static void log_var(const NumericVar *base, const NumericVar *num,
					NumericVar *result);
static void power_var(const NumericVar *base, const NumericVar *exp,
					  NumericVar *result);
static void power_var_int(const NumericVar *base, int exp, int exp_dscale,
						  NumericVar *result);
static void power_ten_int(int exp, NumericVar *result);
static void random_var(pg_prng_state *state, const NumericVar *rmin,
					   const NumericVar *rmax, NumericVar *result);

static int	cmp_abs(const NumericVar *var1, const NumericVar *var2);
static int	cmp_abs_common(const NumericDigit *var1digits, int var1ndigits,
						   int var1weight,
						   const NumericDigit *var2digits, int var2ndigits,
						   int var2weight);
static void add_abs(const NumericVar *var1, const NumericVar *var2,
					NumericVar *result);
static void sub_abs(const NumericVar *var1, const NumericVar *var2,
					NumericVar *result);
static void round_var(NumericVar *var, int rscale);
static void trunc_var(NumericVar *var, int rscale);
static void strip_var(NumericVar *var);
static void compute_bucket(Numeric operand, Numeric bound1, Numeric bound2,
						   const NumericVar *count_var,
						   NumericVar *result_var);

static void accum_sum_add(NumericSumAccum *accum, const NumericVar *val);
static void accum_sum_rescale(NumericSumAccum *accum, const NumericVar *val);
static void accum_sum_carry(NumericSumAccum *accum);
static void accum_sum_reset(NumericSumAccum *accum);
static void accum_sum_final(NumericSumAccum *accum, NumericVar *result);
static void accum_sum_copy(NumericSumAccum *dst, NumericSumAccum *src);
static void accum_sum_combine(NumericSumAccum *accum, NumericSumAccum *accum2);


/* ----------------------------------------------------------------------
 *
 * Input-, output- and rounding-functions
 *
 * ----------------------------------------------------------------------
 */


/*
 * numeric_in() -
 *
 *	Input function for numeric data type
 */
Datum
numeric_in(PG_FUNCTION_ARGS)
{
	char	   *str = PG_GETARG_CSTRING(0);
#ifdef NOT_USED
	Oid			typelem = PG_GETARG_OID(1);
#endif
	int32		typmod = PG_GETARG_INT32(2);
	Node	   *escontext = fcinfo->context;
	Numeric		res;
	const char *cp;
	const char *numstart;
	int			sign;

	/* Skip leading spaces */
	cp = str;
	while (*cp)
	{
		if (!isspace((unsigned char) *cp))
			break;
		cp++;
	}

	/*
	 * Process the number's sign. This duplicates logic in set_var_from_str(),
	 * but it's worth doing here, since it simplifies the handling of
	 * infinities and non-decimal integers.
	 */
	numstart = cp;
	sign = NUMERIC_POS;

	if (*cp == '+')
		cp++;
	else if (*cp == '-')
	{
		sign = NUMERIC_NEG;
		cp++;
	}

	/*
	 * Check for NaN and infinities.  We recognize the same strings allowed by
	 * float8in().
	 *
	 * Since all other legal inputs have a digit or a decimal point after the
	 * sign, we need only check for NaN/infinity if that's not the case.
	 */
	if (!isdigit((unsigned char) *cp) && *cp != '.')
	{
		/*
		 * The number must be NaN or infinity; anything else can only be a
		 * syntax error. Note that NaN mustn't have a sign.
		 */
		if (pg_strncasecmp(numstart, "NaN", 3) == 0)
		{
			res = make_result(&const_nan);
			cp = numstart + 3;
		}
		else if (pg_strncasecmp(cp, "Infinity", 8) == 0)
		{
			res = make_result(sign == NUMERIC_POS ? &const_pinf : &const_ninf);
			cp += 8;
		}
		else if (pg_strncasecmp(cp, "inf", 3) == 0)
		{
			res = make_result(sign == NUMERIC_POS ? &const_pinf : &const_ninf);
			cp += 3;
		}
		else
			goto invalid_syntax;

		/*
		 * Check for trailing junk; there should be nothing left but spaces.
		 *
		 * We intentionally do this check before applying the typmod because
		 * we would like to throw any trailing-junk syntax error before any
		 * semantic error resulting from apply_typmod_special().
		 */
		while (*cp)
		{
			if (!isspace((unsigned char) *cp))
				goto invalid_syntax;
			cp++;
		}

		if (!apply_typmod_special(res, typmod, escontext))
			PG_RETURN_NULL();
	}
	else
	{
		/*
		 * We have a normal numeric value, which may be a non-decimal integer
		 * or a regular decimal number.
		 */
		NumericVar	value;
		int			base;
		bool		have_error;

		init_var(&value);

		/*
		 * Determine the number's base by looking for a non-decimal prefix
		 * indicator ("0x", "0o", or "0b").
		 */
		if (cp[0] == '0')
		{
			switch (cp[1])
			{
				case 'x':
				case 'X':
					base = 16;
					break;
				case 'o':
				case 'O':
					base = 8;
					break;
				case 'b':
				case 'B':
					base = 2;
					break;
				default:
					base = 10;
			}
		}
		else
			base = 10;

		/* Parse the rest of the number and apply the sign */
		if (base == 10)
		{
			if (!set_var_from_str(str, cp, &value, &cp, escontext))
				PG_RETURN_NULL();
			value.sign = sign;
		}
		else
		{
			if (!set_var_from_non_decimal_integer_str(str, cp + 2, sign, base,
													  &value, &cp, escontext))
				PG_RETURN_NULL();
		}

		/*
		 * Should be nothing left but spaces. As above, throw any typmod error
		 * after finishing syntax check.
		 */
		while (*cp)
		{
			if (!isspace((unsigned char) *cp))
				goto invalid_syntax;
			cp++;
		}

		if (!apply_typmod(&value, typmod, escontext))
			PG_RETURN_NULL();

		res = make_result_opt_error(&value, &have_error);

		if (have_error)
			ereturn(escontext, (Datum) 0,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("value overflows numeric format")));

		free_var(&value);
	}

	PG_RETURN_NUMERIC(res);

invalid_syntax:
	ereturn(escontext, (Datum) 0,
			(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
			 errmsg("invalid input syntax for type %s: \"%s\"",
					"numeric", str)));
}


/*
 * numeric_out() -
 *
 *	Output function for numeric data type
 */
Datum
numeric_out(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);

	/*
	 * Handle NaN and infinities
	 */
	if (NUMERIC_IS_SPECIAL(num))
	{
		if (NUMERIC_IS_PINF(num))
			PG_RETURN_CSTRING(pstrdup("Infinity"));
		else if (NUMERIC_IS_NINF(num))
			PG_RETURN_CSTRING(pstrdup("-Infinity"));
		else
			PG_RETURN_CSTRING(pstrdup("NaN"));
	}

	PG_RETURN_CSTRING(numeric_dec128_out(num));
}

/*
 * numeric_is_nan() -
 *
 *	Is Numeric value a NaN?
 */
bool
numeric_is_nan(Numeric num)
{
	return NUMERIC_IS_NAN(num);
}

/*
 * numeric_is_inf() -
 *
 *	Is Numeric value an infinity?
 */
bool
numeric_is_inf(Numeric num)
{
	return NUMERIC_IS_INF(num);
}

/*
 * numeric_is_integral() -
 *
 *	Is Numeric value integral?
 */
static bool
numeric_is_integral(Numeric num)
{
	NumericVar	arg;

	/* Reject NaN, but infinities are considered integral */
	if (NUMERIC_IS_SPECIAL(num))
	{
		if (NUMERIC_IS_NAN(num))
			return false;
		return true;
	}

	/* Integral if there are no digits to the right of the decimal point */
	init_var_from_num(num, &arg);

	return (arg.ndigits == 0 || arg.ndigits <= arg.weight + 1);
}

/*
 * make_numeric_typmod() -
 *
 *	Pack numeric precision and scale values into a typmod.  The upper 16 bits
 *	are used for the precision (though actually not all these bits are needed,
 *	since the maximum allowed precision is 1000).  The lower 16 bits are for
 *	the scale, but since the scale is constrained to the range [-1000, 1000],
 *	we use just the lower 11 of those 16 bits, and leave the remaining 5 bits
 *	unset, for possible future use.
 *
 *	For purely historical reasons VARHDRSZ is then added to the result, thus
 *	the unused space in the upper 16 bits is not all as freely available as it
 *	might seem.  (We can't let the result overflow to a negative int32, as
 *	other parts of the system would interpret that as not-a-valid-typmod.)
 */
static inline int32
make_numeric_typmod(int precision, int scale)
{
	return ((precision << 16) | (scale & 0x7ff)) + VARHDRSZ;
}

/*
 * Because of the offset, valid numeric typmods are at least VARHDRSZ
 */
static inline bool
is_valid_numeric_typmod(int32 typmod)
{
	return typmod >= (int32) VARHDRSZ;
}

/*
 * numeric_typmod_precision() -
 *
 *	Extract the precision from a numeric typmod --- see make_numeric_typmod().
 */
static inline int
numeric_typmod_precision(int32 typmod)
{
	return ((typmod - VARHDRSZ) >> 16) & 0xffff;
}

/*
 * numeric_typmod_scale() -
 *
 *	Extract the scale from a numeric typmod --- see make_numeric_typmod().
 *
 *	Note that the scale may be negative, so we must do sign extension when
 *	unpacking it.  We do this using the bit hack (x^1024)-1024, which sign
 *	extends an 11-bit two's complement number x.
 */
static inline int
numeric_typmod_scale(int32 typmod)
{
	return (((typmod - VARHDRSZ) & 0x7ff) ^ 1024) - 1024;
}

/*
 * numeric_maximum_size() -
 *
 *	Maximum size of a numeric with given typmod, or -1 if unlimited/unknown.
 */
int32
numeric_maximum_size(int32 typmod)
{
	if (!is_valid_numeric_typmod(typmod))
		return -1;

	/*
	 * EXPERIMENTAL (SPEC-numeric-as-dec128.md): a dec128-backed numeric is a
	 * fixed 16-byte scaled integer, so its size no longer depends on the
	 * declared precision/scale at all.  (pg_type.typlen is 16, not -1, so
	 * get_typavgwidth() shouldn't even reach this function for numeric
	 * anymore -- it's kept working regardless, in case something still
	 * calls it directly by name.)
	 */
	return sizeof(NumericData);
}

/*
 * numeric_out_sci() -
 *
 *	Output function for numeric data type in scientific notation.
 */
char *
numeric_out_sci(Numeric num, int scale)
{
	NumericVar	x;
	char	   *str;

	/*
	 * Handle NaN and infinities
	 */
	if (NUMERIC_IS_SPECIAL(num))
	{
		if (NUMERIC_IS_PINF(num))
			return pstrdup("Infinity");
		else if (NUMERIC_IS_NINF(num))
			return pstrdup("-Infinity");
		else
			return pstrdup("NaN");
	}

	init_var_from_num(num, &x);

	str = get_str_from_var_sci(&x, scale);

	return str;
}

/*
 * numeric_normalize() -
 *
 *	Output function for numeric data type, suppressing insignificant trailing
 *	zeroes and then any trailing decimal point.  The intent of this is to
 *	produce strings that are equal if and only if the input numeric values
 *	compare equal.
 */
char *
numeric_normalize(Numeric num)
{
	NumericVar	x;
	char	   *str;
	int			last;

	/*
	 * Handle NaN and infinities
	 */
	if (NUMERIC_IS_SPECIAL(num))
	{
		if (NUMERIC_IS_PINF(num))
			return pstrdup("Infinity");
		else if (NUMERIC_IS_NINF(num))
			return pstrdup("-Infinity");
		else
			return pstrdup("NaN");
	}

	init_var_from_num(num, &x);

	str = get_str_from_var(&x);

	/* If there's no decimal point, there's certainly nothing to remove. */
	if (strchr(str, '.') != NULL)
	{
		/*
		 * Back up over trailing fractional zeroes.  Since there is a decimal
		 * point, this loop will terminate safely.
		 */
		last = strlen(str) - 1;
		while (str[last] == '0')
			last--;

		/* We want to get rid of the decimal point too, if it's now last. */
		if (str[last] == '.')
			last--;

		/* Delete whatever we backed up over. */
		str[last + 1] = '\0';
	}

	return str;
}

/*
 *		numeric_recv			- converts external binary format to numeric
 *
 * External format is a sequence of int16's:
 * ndigits, weight, sign, dscale, NumericDigits.
 */
Datum
numeric_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);

#ifdef NOT_USED
	Oid			typelem = PG_GETARG_OID(1);
#endif
	int32		typmod = PG_GETARG_INT32(2);
	NumericVar	value;
	Numeric		res;
	int			len,
				i;

	init_var(&value);

	len = (uint16) pq_getmsgint(buf, sizeof(uint16));

	alloc_var(&value, len);

	value.weight = (int16) pq_getmsgint(buf, sizeof(int16));
	/* we allow any int16 for weight --- OK? */

	value.sign = (uint16) pq_getmsgint(buf, sizeof(uint16));
	if (!(value.sign == NUMERIC_POS ||
		  value.sign == NUMERIC_NEG ||
		  value.sign == NUMERIC_NAN ||
		  value.sign == NUMERIC_PINF ||
		  value.sign == NUMERIC_NINF))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
				 errmsg("invalid sign in external \"numeric\" value")));

	value.dscale = (uint16) pq_getmsgint(buf, sizeof(uint16));
	if ((value.dscale & NUMERIC_DSCALE_MASK) != value.dscale)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
				 errmsg("invalid scale in external \"numeric\" value")));

	for (i = 0; i < len; i++)
	{
		NumericDigit d = pq_getmsgint(buf, sizeof(NumericDigit));

		if (d < 0 || d >= NBASE)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
					 errmsg("invalid digit in external \"numeric\" value")));
		value.digits[i] = d;
	}

	/*
	 * If the given dscale would hide any digits, truncate those digits away.
	 * We could alternatively throw an error, but that would take a bunch of
	 * extra code (about as much as trunc_var involves), and it might cause
	 * client compatibility issues.  Be careful not to apply trunc_var to
	 * special values, as it could do the wrong thing; we don't need it
	 * anyway, since make_result will ignore all but the sign field.
	 *
	 * After doing that, be sure to check the typmod restriction.
	 */
	if (value.sign == NUMERIC_POS ||
		value.sign == NUMERIC_NEG)
	{
		trunc_var(&value, value.dscale);

		(void) apply_typmod(&value, typmod, NULL);

		res = make_result(&value);
	}
	else
	{
		/* apply_typmod_special wants us to make the Numeric first */
		res = make_result(&value);

		(void) apply_typmod_special(res, typmod, NULL);
	}

	free_var(&value);

	PG_RETURN_NUMERIC(res);
}

/*
 *		numeric_send			- converts numeric to binary format
 */
Datum
numeric_send(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);
	NumericVar	x;
	StringInfoData buf;
	int			i;

	pq_begintypsend(&buf);

	/*
	 * EXPERIMENTAL (SPEC-numeric-as-dec128.md): init_var_from_num() /
	 * numeric_dec128_to_var() must never be called on a special value (see
	 * the Assert there) -- unlike the historical varlena-backed version,
	 * which could trivially hand back an empty-digits NumericVar for NaN or
	 * Infinity without doing any real conversion.  Every other caller
	 * already guards this with NUMERIC_IS_SPECIAL() (e.g. numeric_out()
	 * above); this function was left as "don't touch, it'll just work" per
	 * the spec's sec. 3.4 rationale, but that rationale only covers finite
	 * values.  Without this check, sending a NaN/Infinity numeric in binary
	 * silently reinterprets its sentinel mantissa as a huge finite value
	 * (e.g. NaN's sentinel renders as 10^37), which numeric_recv() then
	 * rejects as an overflow -- or worse, could round-trip as a bogus
	 * finite number.  Reproduce with: COPY a NaN/Infinity numeric out and
	 * back in WITH (FORMAT binary).
	 */
	if (NUMERIC_IS_SPECIAL(num))
	{
		int16		sign;

		if (NUMERIC_IS_PINF(num))
			sign = NUMERIC_PINF;
		else if (NUMERIC_IS_NINF(num))
			sign = NUMERIC_NINF;
		else
			sign = NUMERIC_NAN;

		pq_sendint16(&buf, 0);	/* ndigits */
		pq_sendint16(&buf, 0);	/* weight */
		pq_sendint16(&buf, sign);
		pq_sendint16(&buf, 0);	/* dscale */

		PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
	}

	init_var_from_num(num, &x);

	pq_sendint16(&buf, x.ndigits);
	pq_sendint16(&buf, x.weight);
	pq_sendint16(&buf, x.sign);
	pq_sendint16(&buf, x.dscale);
	for (i = 0; i < x.ndigits; i++)
		pq_sendint16(&buf, x.digits[i]);

	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}


/*
 * numeric_support()
 *
 * Planner support function for the numeric() length coercion function.
 *
 * Flatten calls that solely represent increases in allowable precision.
 * Scale changes mutate every datum, so they are unoptimizable.  Some values,
 * e.g. 1E-1001, can only fit into an unconstrained numeric, so a change from
 * an unconstrained numeric to any constrained numeric is also unoptimizable.
 */
Datum
numeric_support(PG_FUNCTION_ARGS)
{
	Node	   *rawreq = (Node *) PG_GETARG_POINTER(0);
	Node	   *ret = NULL;

	if (IsA(rawreq, SupportRequestSimplify))
	{
		SupportRequestSimplify *req = (SupportRequestSimplify *) rawreq;
		FuncExpr   *expr = req->fcall;
		Node	   *typmod;

		Assert(list_length(expr->args) >= 2);

		typmod = (Node *) lsecond(expr->args);

		if (IsA(typmod, Const) && !((Const *) typmod)->constisnull)
		{
			Node	   *source = (Node *) linitial(expr->args);
			int32		old_typmod = exprTypmod(source);
			int32		new_typmod = DatumGetInt32(((Const *) typmod)->constvalue);
			int32		old_scale = numeric_typmod_scale(old_typmod);
			int32		new_scale = numeric_typmod_scale(new_typmod);
			int32		old_precision = numeric_typmod_precision(old_typmod);
			int32		new_precision = numeric_typmod_precision(new_typmod);

			/*
			 * If new_typmod is invalid, the destination is unconstrained;
			 * that's always OK.  If old_typmod is valid, the source is
			 * constrained, and we're OK if the scale is unchanged and the
			 * precision is not decreasing.  See further notes in function
			 * header comment.
			 */
			if (!is_valid_numeric_typmod(new_typmod) ||
				(is_valid_numeric_typmod(old_typmod) &&
				 new_scale == old_scale && new_precision >= old_precision))
				ret = relabel_to_typmod(source, new_typmod);
		}
	}

	PG_RETURN_POINTER(ret);
}

/*
 * numeric() -
 *
 *	This is a special function called by the Postgres database system
 *	before a value is stored in a tuple's attribute. The precision and
 *	scale of the attribute have to be applied on the value.
 */
Datum
numeric		(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);
	int32		typmod = PG_GETARG_INT32(1);
	Numeric		new;
	NumericVar	var;

	/*
	 * Handle NaN and infinities: if apply_typmod_special doesn't complain,
	 * just return a copy of the input.
	 */
	if (NUMERIC_IS_SPECIAL(num))
	{
		(void) apply_typmod_special(num, typmod, NULL);
		PG_RETURN_NUMERIC(duplicate_numeric(num));
	}

	/*
	 * If the value isn't a valid type modifier, simply return a copy of the
	 * input value
	 */
	if (!is_valid_numeric_typmod(typmod))
		PG_RETURN_NUMERIC(duplicate_numeric(num));

	/*
	 * Unpack the number into a variable and let apply_typmod() do it.
	 *
	 * The classic packed format could sometimes patch its header in place
	 * and skip the unpack/repack entirely when the value was already known
	 * to be in bounds.  dec128 has no header to patch -- only a scaled
	 * integer -- so that shortcut has no counterpart here; every call goes
	 * through NumericVar.  (A dec128-native version of this fast path,
	 * rescaling the mantissa directly, is a candidate for the later
	 * acceleration pass, not this correctness pass.)
	 */
	init_var(&var);

	set_var_from_num(num, &var);
	(void) apply_typmod(&var, typmod, NULL);
	new = make_result(&var);

	free_var(&var);

	PG_RETURN_NUMERIC(new);
}

Datum
numerictypmodin(PG_FUNCTION_ARGS)
{
	ArrayType  *ta = PG_GETARG_ARRAYTYPE_P(0);
	int32	   *tl;
	int			n;
	int32		typmod;

	tl = ArrayGetIntegerTypmods(ta, &n);

	if (n == 2)
	{
		if (tl[0] < 1 || tl[0] > NUMERIC_MAX_PRECISION)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("NUMERIC precision %d must be between 1 and %d",
							tl[0], NUMERIC_MAX_PRECISION)));
		if (tl[1] < NUMERIC_MIN_SCALE || tl[1] > NUMERIC_MAX_SCALE)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("NUMERIC scale %d must be between %d and %d",
							tl[1], NUMERIC_MIN_SCALE, NUMERIC_MAX_SCALE)));

		/*
		 * EXPERIMENTAL (SPEC-numeric-as-dec128.md): reject a declared scale
		 * this representation cannot honour.
		 *
		 * dec128 stores at most NUMERIC_MAX_STORED_SCALE fractional digits, and
		 * apply_typmod() would quietly round a value down to that.  Without this
		 * check "numeric(38,20)" is accepted and then stores 15 decimals with
		 * neither error nor warning -- a column that promises more precision
		 * than it keeps is worse than one that refuses to be created.  Fail at
		 * DDL time instead, where it is visible and fixable.
		 *
		 * Note this is a user-visible restriction that stock numeric does not
		 * have (it allows scale up to NUMERIC_MAX_SCALE), so a schema using
		 * wider scales will not load on this build at all.  That is deliberate:
		 * for an experiment gated on provable correctness, refusing the schema
		 * is the honest failure mode.
		 */
		if (tl[1] > NUMERIC_MAX_STORED_SCALE)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("NUMERIC scale %d exceeds the maximum supported by this build",
							tl[1]),
					 errdetail("This server stores numeric values with at most %d fractional digits.",
							   NUMERIC_MAX_STORED_SCALE),
					 errhint("Use a scale of %d or less.",
							 NUMERIC_MAX_STORED_SCALE)));

		typmod = make_numeric_typmod(tl[0], tl[1]);
	}
	else if (n == 1)
	{
		if (tl[0] < 1 || tl[0] > NUMERIC_MAX_PRECISION)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("NUMERIC precision %d must be between 1 and %d",
							tl[0], NUMERIC_MAX_PRECISION)));
		/* scale defaults to zero */
		typmod = make_numeric_typmod(tl[0], 0);
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid NUMERIC type modifier")));
		typmod = 0;				/* keep compiler quiet */
	}

	PG_RETURN_INT32(typmod);
}

Datum
numerictypmodout(PG_FUNCTION_ARGS)
{
	int32		typmod = PG_GETARG_INT32(0);
	char	   *res = (char *) palloc(64);

	if (is_valid_numeric_typmod(typmod))
		snprintf(res, 64, "(%d,%d)",
				 numeric_typmod_precision(typmod),
				 numeric_typmod_scale(typmod));
	else
		*res = '\0';

	PG_RETURN_CSTRING(res);
}


/* ----------------------------------------------------------------------
 *
 * Sign manipulation, rounding and the like
 *
 * ----------------------------------------------------------------------
 */

Datum
numeric_abs(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);
	Numeric		res = (Numeric) palloc(sizeof(NumericData));
	int128		mant = numeric_pack_mant(num);

	/*
	 * Do it the easy way directly on the packed format: abs() of a scaled
	 * integer is just abs() of the mantissa.  This changes -Inf to Inf and
	 * doesn't affect NaN, so the special sentinels need no separate case
	 * except that NaN's sentinel is already non-negative.
	 */
	if (NUMERIC_IS_NINF(num))
		mant = NUMERIC_PINF_MANT;
	else if (mant < 0)
		mant = -mant;

	numeric_pack_store(res, mant, numeric_pack_scale(num));

	PG_RETURN_NUMERIC(res);
}


Datum
numeric_uminus(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);
	Numeric		res = (Numeric) palloc(sizeof(NumericData));
	int128		mant = numeric_pack_mant(num);

	/*
	 * Do it the easy way directly on the packed format: negate the
	 * mantissa.  Flip the sign if it's Inf or -Inf; NaN is unaffected; zero
	 * is its own negation, so no separate case is needed for it.
	 */
	if (NUMERIC_IS_PINF(num))
		mant = NUMERIC_NINF_MANT;
	else if (NUMERIC_IS_NINF(num))
		mant = NUMERIC_PINF_MANT;
	else if (!NUMERIC_IS_NAN(num))
		mant = -mant;

	numeric_pack_store(res, mant, numeric_pack_scale(num));

	PG_RETURN_NUMERIC(res);
}


Datum
numeric_uplus(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);

	PG_RETURN_NUMERIC(duplicate_numeric(num));
}


/*
 * numeric_sign_internal() -
 *
 * Returns -1 if the argument is less than 0, 0 if the argument is equal
 * to 0, and 1 if the argument is greater than zero.  Caller must have
 * taken care of the NaN case, but we can handle infinities here.
 */
static int
numeric_sign_internal(Numeric num)
{
	int128		mant;

	if (NUMERIC_IS_SPECIAL(num))
	{
		Assert(!NUMERIC_IS_NAN(num));
		/* Must be Inf or -Inf */
		if (NUMERIC_IS_PINF(num))
			return 1;
		else
			return -1;
	}

	/* Otherwise the sign of a scaled integer is just the sign of the mantissa */
	mant = numeric_pack_mant(num);
	return (mant > 0) - (mant < 0);
}

/*
 * numeric_sign() -
 *
 * returns -1 if the argument is less than 0, 0 if the argument is equal
 * to 0, and 1 if the argument is greater than zero.
 */
Datum
numeric_sign(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);

	/*
	 * Handle NaN (infinities can be handled normally)
	 */
	if (NUMERIC_IS_NAN(num))
		PG_RETURN_NUMERIC(make_result(&const_nan));

	switch (numeric_sign_internal(num))
	{
		case 0:
			PG_RETURN_NUMERIC(make_result(&const_zero));
		case 1:
			PG_RETURN_NUMERIC(make_result(&const_one));
		case -1:
			PG_RETURN_NUMERIC(make_result(&const_minus_one));
	}

	Assert(false);
	return (Datum) 0;
}


/*
 * numeric_round() -
 *
 *	Round a value to have 'scale' digits after the decimal point.
 *	We allow negative 'scale', implying rounding before the decimal
 *	point --- Oracle interprets rounding that way.
 */
Datum
numeric_round(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);
	int32		scale = PG_GETARG_INT32(1);
	Numeric		res;
	NumericVar	arg;

	/*
	 * Handle NaN and infinities
	 */
	if (NUMERIC_IS_SPECIAL(num))
		PG_RETURN_NUMERIC(duplicate_numeric(num));

	/*
	 * Limit the scale value to avoid possible overflow in calculations.
	 *
	 * These limits are based on the maximum number of digits a Numeric value
	 * can have before and after the decimal point, but we must allow for one
	 * extra digit before the decimal point, in case the most significant
	 * digit rounds up; we must check if that causes Numeric overflow.
	 */
	scale = Max(scale, -(NUMERIC_WEIGHT_MAX + 1) * DEC_DIGITS - 1);
	scale = Min(scale, NUMERIC_DSCALE_MAX);

	/*
	 * Unpack the argument and round it at the proper digit position
	 */
	init_var(&arg);
	set_var_from_num(num, &arg);

	round_var(&arg, scale);

	/* We don't allow negative output dscale */
	if (scale < 0)
		arg.dscale = 0;

	/*
	 * Return the rounded result
	 */
	res = make_result(&arg);

	free_var(&arg);
	PG_RETURN_NUMERIC(res);
}


/*
 * numeric_trunc() -
 *
 *	Truncate a value to have 'scale' digits after the decimal point.
 *	We allow negative 'scale', implying a truncation before the decimal
 *	point --- Oracle interprets truncation that way.
 */
Datum
numeric_trunc(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);
	int32		scale = PG_GETARG_INT32(1);
	Numeric		res;
	NumericVar	arg;

	/*
	 * Handle NaN and infinities
	 */
	if (NUMERIC_IS_SPECIAL(num))
		PG_RETURN_NUMERIC(duplicate_numeric(num));

	/*
	 * Limit the scale value to avoid possible overflow in calculations.
	 *
	 * These limits are based on the maximum number of digits a Numeric value
	 * can have before and after the decimal point.
	 */
	scale = Max(scale, -(NUMERIC_WEIGHT_MAX + 1) * DEC_DIGITS);
	scale = Min(scale, NUMERIC_DSCALE_MAX);

	/*
	 * Unpack the argument and truncate it at the proper digit position
	 */
	init_var(&arg);
	set_var_from_num(num, &arg);

	trunc_var(&arg, scale);

	/* We don't allow negative output dscale */
	if (scale < 0)
		arg.dscale = 0;

	/*
	 * Return the truncated result
	 */
	res = make_result(&arg);

	free_var(&arg);
	PG_RETURN_NUMERIC(res);
}


/*
 * numeric_ceil() -
 *
 *	Return the smallest integer greater than or equal to the argument
 */
Datum
numeric_ceil(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);
	Numeric		res;
	NumericVar	result;

	/*
	 * Handle NaN and infinities
	 */
	if (NUMERIC_IS_SPECIAL(num))
		PG_RETURN_NUMERIC(duplicate_numeric(num));

	init_var_from_num(num, &result);
	ceil_var(&result, &result);

	res = make_result(&result);
	free_var(&result);

	PG_RETURN_NUMERIC(res);
}


/*
 * numeric_floor() -
 *
 *	Return the largest integer equal to or less than the argument
 */
Datum
numeric_floor(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);
	Numeric		res;
	NumericVar	result;

	/*
	 * Handle NaN and infinities
	 */
	if (NUMERIC_IS_SPECIAL(num))
		PG_RETURN_NUMERIC(duplicate_numeric(num));

	init_var_from_num(num, &result);
	floor_var(&result, &result);

	res = make_result(&result);
	free_var(&result);

	PG_RETURN_NUMERIC(res);
}


/*
 * generate_series_numeric() -
 *
 *	Generate series of numeric.
 */
Datum
generate_series_numeric(PG_FUNCTION_ARGS)
{
	return generate_series_step_numeric(fcinfo);
}

Datum
generate_series_step_numeric(PG_FUNCTION_ARGS)
{
	generate_series_numeric_fctx *fctx;
	FuncCallContext *funcctx;
	MemoryContext oldcontext;

	if (SRF_IS_FIRSTCALL())
	{
		Numeric		start_num = PG_GETARG_NUMERIC(0);
		Numeric		stop_num = PG_GETARG_NUMERIC(1);
		NumericVar	steploc = const_one;

		/* Reject NaN and infinities in start and stop values */
		if (NUMERIC_IS_SPECIAL(start_num))
		{
			if (NUMERIC_IS_NAN(start_num))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("start value cannot be NaN")));
			else
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("start value cannot be infinity")));
		}
		if (NUMERIC_IS_SPECIAL(stop_num))
		{
			if (NUMERIC_IS_NAN(stop_num))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("stop value cannot be NaN")));
			else
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("stop value cannot be infinity")));
		}

		/* see if we were given an explicit step size */
		if (PG_NARGS() == 3)
		{
			Numeric		step_num = PG_GETARG_NUMERIC(2);

			if (NUMERIC_IS_SPECIAL(step_num))
			{
				if (NUMERIC_IS_NAN(step_num))
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("step size cannot be NaN")));
				else
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("step size cannot be infinity")));
			}

			init_var_from_num(step_num, &steploc);

			if (cmp_var(&steploc, &const_zero) == 0)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("step size cannot equal zero")));
		}

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/*
		 * Switch to memory context appropriate for multiple function calls.
		 */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* allocate memory for user context */
		fctx = (generate_series_numeric_fctx *)
			palloc(sizeof(generate_series_numeric_fctx));

		/*
		 * Use fctx to keep state from call to call. Seed current with the
		 * original start value.
		 *
		 * We must still copy the start_num and stop_num values rather than
		 * pointing at them, but note the reason has changed: they are no longer
		 * detoasted copies (a dec128 numeric is never toasted), they are
		 * argument pointers into the per-call context, which is reset before
		 * the next call.  fctx lives in multi_call_memory_ctx and outlives
		 * them, so pointing at them would dangle.  Do not "simplify" these
		 * copies away on the grounds that detoasting is gone.
		 */
		init_var(&fctx->current);
		init_var(&fctx->stop);
		init_var(&fctx->step);

		set_var_from_num(start_num, &fctx->current);
		set_var_from_num(stop_num, &fctx->stop);
		set_var_from_var(&steploc, &fctx->step);

		funcctx->user_fctx = fctx;
		MemoryContextSwitchTo(oldcontext);
	}

	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();

	/*
	 * Get the saved state and use current state as the result of this
	 * iteration.
	 */
	fctx = funcctx->user_fctx;

	if ((fctx->step.sign == NUMERIC_POS &&
		 cmp_var(&fctx->current, &fctx->stop) <= 0) ||
		(fctx->step.sign == NUMERIC_NEG &&
		 cmp_var(&fctx->current, &fctx->stop) >= 0))
	{
		Numeric		result = make_result(&fctx->current);

		/* switch to memory context appropriate for iteration calculation */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* increment current in preparation for next iteration */
		add_var(&fctx->current, &fctx->step, &fctx->current);
		MemoryContextSwitchTo(oldcontext);

		/* do when there is more left to send */
		SRF_RETURN_NEXT(funcctx, NumericGetDatum(result));
	}
	else
		/* do when there is no more left */
		SRF_RETURN_DONE(funcctx);
}

/*
 * Planner support function for generate_series(numeric, numeric [, numeric])
 */
Datum
generate_series_numeric_support(PG_FUNCTION_ARGS)
{
	Node	   *rawreq = (Node *) PG_GETARG_POINTER(0);
	Node	   *ret = NULL;

	if (IsA(rawreq, SupportRequestRows))
	{
		/* Try to estimate the number of rows returned */
		SupportRequestRows *req = (SupportRequestRows *) rawreq;

		if (is_funcclause(req->node))	/* be paranoid */
		{
			List	   *args = ((FuncExpr *) req->node)->args;
			Node	   *arg1,
					   *arg2,
					   *arg3;

			/* We can use estimated argument values here */
			arg1 = estimate_expression_value(req->root, linitial(args));
			arg2 = estimate_expression_value(req->root, lsecond(args));
			if (list_length(args) >= 3)
				arg3 = estimate_expression_value(req->root, lthird(args));
			else
				arg3 = NULL;

			/*
			 * If any argument is constant NULL, we can safely assume that
			 * zero rows are returned.  Otherwise, if they're all non-NULL
			 * constants, we can calculate the number of rows that will be
			 * returned.
			 */
			if ((IsA(arg1, Const) &&
				 ((Const *) arg1)->constisnull) ||
				(IsA(arg2, Const) &&
				 ((Const *) arg2)->constisnull) ||
				(arg3 != NULL && IsA(arg3, Const) &&
				 ((Const *) arg3)->constisnull))
			{
				req->rows = 0;
				ret = (Node *) req;
			}
			else if (IsA(arg1, Const) &&
					 IsA(arg2, Const) &&
					 (arg3 == NULL || IsA(arg3, Const)))
			{
				Numeric		start_num;
				Numeric		stop_num;
				NumericVar	step = const_one;

				/*
				 * If any argument is NaN or infinity, generate_series() will
				 * error out, so we needn't produce an estimate.
				 */
				start_num = DatumGetNumeric(((Const *) arg1)->constvalue);
				stop_num = DatumGetNumeric(((Const *) arg2)->constvalue);

				if (NUMERIC_IS_SPECIAL(start_num) ||
					NUMERIC_IS_SPECIAL(stop_num))
					PG_RETURN_POINTER(NULL);

				if (arg3)
				{
					Numeric		step_num;

					step_num = DatumGetNumeric(((Const *) arg3)->constvalue);

					if (NUMERIC_IS_SPECIAL(step_num))
						PG_RETURN_POINTER(NULL);

					init_var_from_num(step_num, &step);
				}

				/*
				 * The number of rows that will be returned is given by
				 * floor((stop - start) / step) + 1, if the sign of step
				 * matches the sign of stop - start.  Otherwise, no rows will
				 * be returned.
				 */
				if (cmp_var(&step, &const_zero) != 0)
				{
					NumericVar	start;
					NumericVar	stop;
					NumericVar	res;

					init_var_from_num(start_num, &start);
					init_var_from_num(stop_num, &stop);

					init_var(&res);
					sub_var(&stop, &start, &res);

					if (step.sign != res.sign)
					{
						/* no rows will be returned */
						req->rows = 0;
						ret = (Node *) req;
					}
					else
					{
						if (arg3)
							div_var(&res, &step, &res, 0, false, false);
						else
							trunc_var(&res, 0); /* step = 1 */

						req->rows = numericvar_to_double_no_overflow(&res) + 1;
						ret = (Node *) req;
					}

					free_var(&res);
				}
			}
		}
	}

	PG_RETURN_POINTER(ret);
}


/*
 * Implements the numeric version of the width_bucket() function
 * defined by SQL2003. See also width_bucket_float8().
 *
 * 'bound1' and 'bound2' are the lower and upper bounds of the
 * histogram's range, respectively. 'count' is the number of buckets
 * in the histogram. width_bucket() returns an integer indicating the
 * bucket number that 'operand' belongs to in an equiwidth histogram
 * with the specified characteristics. An operand smaller than the
 * lower bound is assigned to bucket 0. An operand greater than or equal
 * to the upper bound is assigned to an additional bucket (with number
 * count+1). We don't allow "NaN" for any of the numeric inputs, and we
 * don't allow either of the histogram bounds to be +/- infinity.
 */
Datum
width_bucket_numeric(PG_FUNCTION_ARGS)
{
	Numeric		operand = PG_GETARG_NUMERIC(0);
	Numeric		bound1 = PG_GETARG_NUMERIC(1);
	Numeric		bound2 = PG_GETARG_NUMERIC(2);
	int32		count = PG_GETARG_INT32(3);
	NumericVar	count_var;
	NumericVar	result_var;
	int32		result;

	if (count <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION),
				 errmsg("count must be greater than zero")));

	if (NUMERIC_IS_SPECIAL(operand) ||
		NUMERIC_IS_SPECIAL(bound1) ||
		NUMERIC_IS_SPECIAL(bound2))
	{
		if (NUMERIC_IS_NAN(operand) ||
			NUMERIC_IS_NAN(bound1) ||
			NUMERIC_IS_NAN(bound2))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION),
					 errmsg("operand, lower bound, and upper bound cannot be NaN")));
		/* We allow "operand" to be infinite; cmp_numerics will cope */
		if (NUMERIC_IS_INF(bound1) || NUMERIC_IS_INF(bound2))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION),
					 errmsg("lower and upper bounds must be finite")));
	}

	init_var(&result_var);
	init_var(&count_var);

	/* Convert 'count' to a numeric, for ease of use later */
	int64_to_numericvar((int64) count, &count_var);

	switch (cmp_numerics(bound1, bound2))
	{
		case 0:
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION),
					 errmsg("lower bound cannot equal upper bound")));
			break;

			/* bound1 < bound2 */
		case -1:
			if (cmp_numerics(operand, bound1) < 0)
				set_var_from_var(&const_zero, &result_var);
			else if (cmp_numerics(operand, bound2) >= 0)
				add_var(&count_var, &const_one, &result_var);
			else
				compute_bucket(operand, bound1, bound2, &count_var,
							   &result_var);
			break;

			/* bound1 > bound2 */
		case 1:
			if (cmp_numerics(operand, bound1) > 0)
				set_var_from_var(&const_zero, &result_var);
			else if (cmp_numerics(operand, bound2) <= 0)
				add_var(&count_var, &const_one, &result_var);
			else
				compute_bucket(operand, bound1, bound2, &count_var,
							   &result_var);
			break;
	}

	/* if result exceeds the range of a legal int4, we ereport here */
	if (!numericvar_to_int32(&result_var, &result))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("integer out of range")));

	free_var(&count_var);
	free_var(&result_var);

	PG_RETURN_INT32(result);
}

/*
 * 'operand' is inside the bucket range, so determine the correct
 * bucket for it to go in. The calculations performed by this function
 * are derived directly from the SQL2003 spec. Note however that we
 * multiply by count before dividing, to avoid unnecessary roundoff error.
 */
static void
compute_bucket(Numeric operand, Numeric bound1, Numeric bound2,
			   const NumericVar *count_var, NumericVar *result_var)
{
	NumericVar	bound1_var;
	NumericVar	bound2_var;
	NumericVar	operand_var;

	init_var_from_num(bound1, &bound1_var);
	init_var_from_num(bound2, &bound2_var);
	init_var_from_num(operand, &operand_var);

	/*
	 * Per spec, bound1 is inclusive and bound2 is exclusive, and so we have
	 * bound1 <= operand < bound2 or bound1 >= operand > bound2.  Either way,
	 * the result is ((operand - bound1) * count) / (bound2 - bound1) + 1,
	 * where the quotient is computed using floor division (i.e., division to
	 * zero decimal places with truncation), which guarantees that the result
	 * is in the range [1, count].  Reversing the bounds doesn't affect the
	 * computation, because the signs cancel out when dividing.
	 */
	sub_var(&operand_var, &bound1_var, &operand_var);
	sub_var(&bound2_var, &bound1_var, &bound2_var);

	mul_var(&operand_var, count_var, &operand_var,
			operand_var.dscale + count_var->dscale);
	div_var(&operand_var, &bound2_var, result_var, 0, false, true);
	add_var(result_var, &const_one, result_var);

	free_var(&bound1_var);
	free_var(&bound2_var);
	free_var(&operand_var);
}

/* ----------------------------------------------------------------------
 *
 * Comparison functions
 *
 * EXPERIMENTAL (SPEC-numeric-as-dec128.md): this section used to open with a
 * warning that btree indexes need these routines not to leak, so working
 * copies of toasted datums must be freed.  That no longer applies and the
 * warning has been removed rather than left to mislead: numeric is now
 * fixed-width (typlen 16), pass-by-reference, and non-toastable
 * (typstorage 'p'), so the storage layer never compresses or moves a value
 * out of line, DatumGetNumeric() is a plain pointer cast, and no comparison
 * routine here ever holds a copy to free.  The PG_FREE_IF_COPY() calls that
 * used to follow each comparison were unreachable and are gone.
 *
 * Sort support:
 *
 * We implement the sortsupport strategy routine in order to get the benefit of
 * abbreviation.  (The original rationale also cited palloc/pfree cycles from
 * detoasting packed values for alignment; that cost no longer exists, but
 * abbreviation is still worth having, since it lets most comparisons be
 * decided on a by-value key without dereferencing either datum.)
 *
 * Two different representations are used for the abbreviated form, one in
 * int32 and one in int64, whichever fits into a by-value Datum.  In both cases
 * the representation is negated relative to the original value, because we use
 * the largest negative value for NaN, which sorts higher than other values. We
 * convert the absolute value of the numeric to a 31-bit or 63-bit positive
 * value, and then negate it if the original number was positive.
 *
 * We abort the abbreviation process if the abbreviation cardinality is below
 * 0.01% of the row count (1 per 10k non-null rows).  The actual break-even
 * point is somewhat below that, perhaps 1 per 30k (at 1 per 100k there's a
 * very small penalty), but we don't want to build up too many abbreviated
 * values before first testing for abort, so we take the slightly pessimistic
 * number.  We make no attempt to estimate the cardinality of the real values,
 * since it plays no part in the cost model here (if the abbreviation is equal,
 * the cost of comparing equal and unequal underlying values is comparable).
 * We discontinue even checking for abort (saving us the hashing overhead) if
 * the estimated cardinality gets to 100k; that would be enough to support many
 * billions of rows while doing no worse than breaking even.
 *
 * ----------------------------------------------------------------------
 */

/*
 * Sort support strategy routine.
 */
Datum
numeric_sortsupport(PG_FUNCTION_ARGS)
{
	SortSupport ssup = (SortSupport) PG_GETARG_POINTER(0);

	ssup->comparator = numeric_fast_cmp;

	if (ssup->abbreviate)
	{
		NumericSortSupport *nss;
		MemoryContext oldcontext = MemoryContextSwitchTo(ssup->ssup_cxt);

		nss = palloc(sizeof(NumericSortSupport));

		/* no realignment buffer to allocate; see NumericSortSupport */

		nss->input_count = 0;
		nss->estimating = true;
		initHyperLogLog(&nss->abbr_card, 10);

		ssup->ssup_extra = nss;

		ssup->abbrev_full_comparator = ssup->comparator;
		ssup->comparator = numeric_cmp_abbrev;
		ssup->abbrev_converter = numeric_abbrev_convert;
		ssup->abbrev_abort = numeric_abbrev_abort;

		MemoryContextSwitchTo(oldcontext);
	}

	PG_RETURN_VOID();
}

/*
 * Abbreviate a numeric datum, handling NaNs (must not leak memory!)
 *
 * EXPERIMENTAL (SPEC-numeric-as-dec128.md): this used to begin with
 * PG_DETOAST_DATUM_PACKED() and a VARATT_IS_SHORT() realignment dance.  Both
 * are not merely unnecessary now that numeric is fixed-width, non-toastable
 * and never short-headered -- they were actively unsafe, because they
 * reinterpret the first byte of the packed value as a varlena header.  A
 * mantissa with bit 60 set makes VARATT_IS_SHORT() read true, whereupon
 * VARSIZE_SHORT() - VARHDRSZ_SHORT underflows to (size_t) -1 and the memcpy
 * ran off the end of the datum: "ORDER BY" over values of 19 or more
 * significant digits crashed the backend outright.  Smaller values have a zero
 * high half, which read as a not-short header, which is the only reason this
 * went unnoticed.  (Same class of bug as the jsonb_util.c and jsonpath.c
 * fixes; this was the third and last site.)
 *
 * Reading the datum in place also removes the whole realignment buffer and,
 * for finite values, the NumericVar round trip: the scaled-integer mantissa is
 * a better source for an abbreviated key than a digit array, and needs no
 * allocation to obtain.
 */
static Datum
numeric_abbrev_convert(Datum original_datum, SortSupport ssup)
{
	NumericSortSupport *nss = ssup->ssup_extra;
	Numeric		value = (Numeric) DatumGetPointer(original_datum);
	Datum		result;

	nss->input_count += 1;

	if (NUMERIC_IS_SPECIAL(value))
	{
		if (NUMERIC_IS_PINF(value))
			result = NUMERIC_ABBREV_PINF;
		else if (NUMERIC_IS_NINF(value))
			result = NUMERIC_ABBREV_NINF;
		else
			result = NUMERIC_ABBREV_NAN;
	}
	else
		result = numeric_abbrev_convert_dec128(value, nss);

	return result;
}

/*
 * Build an abbreviated key for a finite dec128 value.
 *
 * The abbreviated key formats (see numeric_abbrev_convert_var() below) consult
 * only a value's sign, its base-NBASE weight, and its leading few NBASE limbs.
 * All of those follow from the scaled-integer mantissa arithmetically, so we
 * extract them here and hand them to the existing key builder rather than
 * unpacking a whole NumericVar -- and rather than duplicating the bit packing,
 * which is fiddly, order-critical, and has two variants.
 *
 * For a value mant * 10^-scale whose magnitude has d decimal digits, the
 * leading decimal digit sits at decimal exponent (d - 1 - scale), so the
 * leading limb has weight floor((d - 1 - scale) / DEC_DIGITS) and holds
 * ((d - 1 - scale) mod DEC_DIGITS) + 1 significant digits.  Knowing that, we
 * take just enough leading decimal digits to fill NUMERIC_ABBREV_LIMBS limbs,
 * which is at most 16 -- comfortably inside a uint64, so every division after
 * the first is 64-bit and gets strength-reduced.
 *
 * Getting this wrong would silently corrupt sort order rather than fail
 * visibly, so an assert-enabled build cross-checks the extracted weight and
 * limbs against a real unpack.
 */
static Datum
numeric_abbrev_convert_dec128(Numeric value, NumericSortSupport *nss)
{
	int128		mant = numeric_pack_mant(value);
	int			scale = numeric_pack_scale(value);
	uint128		mag;
	NumericDigit limbs[NUMERIC_ABBREV_LIMBS] = {0};
	NumericVar	var;
	int			i;

	Assert(!NUMERIC_IS_SPECIAL(value));

	/* negate in unsigned arithmetic; see numeric_dec128_int128_to_var() */
	mag = (mant < 0) ? (~(uint128) mant) + 1 : (uint128) mant;

	/*
	 * A stack NumericVar with buf == NULL: the key builders only read the
	 * fields set here and never allocate or free, so this must not be passed
	 * to anything else.
	 */
	var.buf = NULL;
	var.digits = limbs;
	var.dscale = scale;
	var.sign = (mant < 0) ? NUMERIC_NEG : NUMERIC_POS;

	if (mag == 0)
	{
		/* strip_var()'s canonical zero; the builders special-case ndigits 0 */
		var.ndigits = 0;
		var.weight = 0;
		var.sign = NUMERIC_POS;
	}
	else
	{
		int			d = numeric_dec128_ndigits(mag);
		int			e = d - 1 - scale;
		int			lead;
		int			want;
		int			take;
		int			drop;
		uint64		u;

		/* floor division, which is what a negative exponent needs */
		var.weight = (e >= 0) ? e / DEC_DIGITS
			: -(((-e) + DEC_DIGITS - 1) / DEC_DIGITS);

		lead = e - DEC_DIGITS * var.weight + 1;
		Assert(lead >= 1 && lead <= DEC_DIGITS);

		want = lead + DEC_DIGITS * (NUMERIC_ABBREV_LIMBS - 1);
		take = Min(d, want);
		drop = d - take;

		/*
		 * One 128-bit division at most, and only when the value has more
		 * digits than the key can use.  Afterwards u holds `take` decimal
		 * digits, hence at most 16, so the rest is 64-bit work.
		 */
		u = (uint64) (drop > 0
					  ? mag / (uint128) numeric_dec128_pow10[drop]
					  : mag);

		/* right-pad so the digits land on the limb boundaries */
		u *= numeric_dec128_pow10_u64[want - take];

		for (i = NUMERIC_ABBREV_LIMBS - 1; i > 0; i--)
		{
			limbs[i] = (NumericDigit) (u % NBASE);
			u /= NBASE;
		}
		limbs[0] = (NumericDigit) u;
		Assert(limbs[0] > 0 && limbs[0] < NBASE);

		var.ndigits = NUMERIC_ABBREV_LIMBS;
	}

#ifdef USE_ASSERT_CHECKING
	{
		NumericVar	chk;

		init_var_from_num(value, &chk);
		Assert(chk.sign == var.sign);
		if (chk.ndigits == 0)
			Assert(var.ndigits == 0);
		else
		{
			Assert(var.weight == chk.weight);
			for (i = 0; i < NUMERIC_ABBREV_LIMBS; i++)
				Assert(limbs[i] ==
					   (i < chk.ndigits ? chk.digits[i] : (NumericDigit) 0));
		}
		free_var(&chk);
	}
#endif

	return numeric_abbrev_convert_var(&var, nss);
}

/*
 * Consider whether to abort abbreviation.
 *
 * We pay no attention to the cardinality of the non-abbreviated data. There is
 * no reason to do so: unlike text, we have no fast check for equal values, so
 * we pay the full overhead whenever the abbreviations are equal regardless of
 * whether the underlying values are also equal.
 */
static bool
numeric_abbrev_abort(int memtupcount, SortSupport ssup)
{
	NumericSortSupport *nss = ssup->ssup_extra;
	double		abbr_card;

	if (memtupcount < 10000 || nss->input_count < 10000 || !nss->estimating)
		return false;

	abbr_card = estimateHyperLogLog(&nss->abbr_card);

	/*
	 * If we have >100k distinct values, then even if we were sorting many
	 * billion rows we'd likely still break even, and the penalty of undoing
	 * that many rows of abbrevs would probably not be worth it. Stop even
	 * counting at that point.
	 */
	if (abbr_card > 100000.0)
	{
		if (trace_sort)
			elog(LOG,
				 "numeric_abbrev: estimation ends at cardinality %f"
				 " after " INT64_FORMAT " values (%d rows)",
				 abbr_card, nss->input_count, memtupcount);
		nss->estimating = false;
		return false;
	}

	/*
	 * Target minimum cardinality is 1 per ~10k of non-null inputs.  (The
	 * break even point is somewhere between one per 100k rows, where
	 * abbreviation has a very slight penalty, and 1 per 10k where it wins by
	 * a measurable percentage.)  We use the relatively pessimistic 10k
	 * threshold, and add a 0.5 row fudge factor, because it allows us to
	 * abort earlier on genuinely pathological data where we've had exactly
	 * one abbreviated value in the first 10k (non-null) rows.
	 */
	if (abbr_card < nss->input_count / 10000.0 + 0.5)
	{
		if (trace_sort)
			elog(LOG,
				 "numeric_abbrev: aborting abbreviation at cardinality %f"
				 " below threshold %f after " INT64_FORMAT " values (%d rows)",
				 abbr_card, nss->input_count / 10000.0 + 0.5,
				 nss->input_count, memtupcount);
		return true;
	}

	if (trace_sort)
		elog(LOG,
			 "numeric_abbrev: cardinality %f"
			 " after " INT64_FORMAT " values (%d rows)",
			 abbr_card, nss->input_count, memtupcount);

	return false;
}

/*
 * Non-fmgr interface to the comparison routine to allow sortsupport to elide
 * the fmgr call.  It is also a required part of the sort support API when
 * abbreviations are performed.
 *
 * EXPERIMENTAL (SPEC-numeric-as-dec128.md): the two palloc/pfree cycles this
 * used to worry about are gone, along with the free-if-copy checks that
 * followed the comparison.  A dec128 numeric is fixed-width, non-toastable and
 * never short-headered, so DatumGetNumeric() is a plain pointer cast and can
 * never hand back a copy to free.
 */
static int
numeric_fast_cmp(Datum x, Datum y, SortSupport ssup)
{
	return cmp_numerics((Numeric) DatumGetPointer(x),
						(Numeric) DatumGetPointer(y));
}

/*
 * Compare abbreviations of values. (Abbreviations may be equal where the true
 * values differ, but if the abbreviations differ, they must reflect the
 * ordering of the true values.)
 */
static int
numeric_cmp_abbrev(Datum x, Datum y, SortSupport ssup)
{
	/*
	 * NOTE WELL: this is intentionally backwards, because the abbreviation is
	 * negated relative to the original value, to handle NaN/infinity cases.
	 */
	if (DatumGetNumericAbbrev(x) < DatumGetNumericAbbrev(y))
		return 1;
	if (DatumGetNumericAbbrev(x) > DatumGetNumericAbbrev(y))
		return -1;
	return 0;
}

/*
 * Abbreviate a NumericVar according to the available bit size.
 *
 * The 31-bit value is constructed as:
 *
 *	0 + 7bits digit weight + 24 bits digit value
 *
 * where the digit weight is in single decimal digits, not digit words, and
 * stored in excess-44 representation[1]. The 24-bit digit value is the 7 most
 * significant decimal digits of the value converted to binary. Values whose
 * weights would fall outside the representable range are rounded off to zero
 * (which is also used to represent actual zeros) or to 0x7FFFFFFF (which
 * otherwise cannot occur). Abbreviation therefore fails to gain any advantage
 * where values are outside the range 10^-44 to 10^83, which is not considered
 * to be a serious limitation, or when values are of the same magnitude and
 * equal in the first 7 decimal digits, which is considered to be an
 * unavoidable limitation given the available bits. (Stealing three more bits
 * to compare another digit would narrow the range of representable weights by
 * a factor of 8, which starts to look like a real limiting factor.)
 *
 * (The value 44 for the excess is essentially arbitrary)
 *
 * The 63-bit value is constructed as:
 *
 *	0 + 7bits weight + 4 x 14-bit packed digit words
 *
 * The weight in this case is again stored in excess-44, but this time it is
 * the original weight in digit words (i.e. powers of 10000). The first four
 * digit words of the value (if present; trailing zeros are assumed as needed)
 * are packed into 14 bits each to form the rest of the value. Again,
 * out-of-range values are rounded off to 0 or 0x7FFFFFFFFFFFFFFF. The
 * representable range in this case is 10^-176 to 10^332, which is considered
 * to be good enough for all practical purposes, and comparison of 4 words
 * means that at least 13 decimal digits are compared, which is considered to
 * be a reasonable compromise between effectiveness and efficiency in computing
 * the abbreviation.
 *
 * (The value 44 for the excess is even more arbitrary here, it was chosen just
 * to match the value used in the 31-bit case)
 *
 * [1] - Excess-k representation means that the value is offset by adding 'k'
 * and then treated as unsigned, so the smallest representable value is stored
 * with all bits zero. This allows simple comparisons to work on the composite
 * value.
 */

#if NUMERIC_ABBREV_BITS == 64

static Datum
numeric_abbrev_convert_var(const NumericVar *var, NumericSortSupport *nss)
{
	int			ndigits = var->ndigits;
	int			weight = var->weight;
	int64		result;

	if (ndigits == 0 || weight < -44)
	{
		result = 0;
	}
	else if (weight > 83)
	{
		result = PG_INT64_MAX;
	}
	else
	{
		result = ((int64) (weight + 44) << 56);

		switch (ndigits)
		{
			default:
				result |= ((int64) var->digits[3]);
				/* FALLTHROUGH */
			case 3:
				result |= ((int64) var->digits[2]) << 14;
				/* FALLTHROUGH */
			case 2:
				result |= ((int64) var->digits[1]) << 28;
				/* FALLTHROUGH */
			case 1:
				result |= ((int64) var->digits[0]) << 42;
				break;
		}
	}

	/* the abbrev is negated relative to the original */
	if (var->sign == NUMERIC_POS)
		result = -result;

	if (nss->estimating)
	{
		uint32		tmp = ((uint32) result
						   ^ (uint32) ((uint64) result >> 32));

		addHyperLogLog(&nss->abbr_card, DatumGetUInt32(hash_uint32(tmp)));
	}

	return NumericAbbrevGetDatum(result);
}

#endif							/* NUMERIC_ABBREV_BITS == 64 */

#if NUMERIC_ABBREV_BITS == 32

static Datum
numeric_abbrev_convert_var(const NumericVar *var, NumericSortSupport *nss)
{
	int			ndigits = var->ndigits;
	int			weight = var->weight;
	int32		result;

	if (ndigits == 0 || weight < -11)
	{
		result = 0;
	}
	else if (weight > 20)
	{
		result = PG_INT32_MAX;
	}
	else
	{
		NumericDigit nxt1 = (ndigits > 1) ? var->digits[1] : 0;

		weight = (weight + 11) * 4;

		result = var->digits[0];

		/*
		 * "result" now has 1 to 4 nonzero decimal digits. We pack in more
		 * digits to make 7 in total (largest we can fit in 24 bits)
		 */

		if (result > 999)
		{
			/* already have 4 digits, add 3 more */
			result = (result * 1000) + (nxt1 / 10);
			weight += 3;
		}
		else if (result > 99)
		{
			/* already have 3 digits, add 4 more */
			result = (result * 10000) + nxt1;
			weight += 2;
		}
		else if (result > 9)
		{
			NumericDigit nxt2 = (ndigits > 2) ? var->digits[2] : 0;

			/* already have 2 digits, add 5 more */
			result = (result * 100000) + (nxt1 * 10) + (nxt2 / 1000);
			weight += 1;
		}
		else
		{
			NumericDigit nxt2 = (ndigits > 2) ? var->digits[2] : 0;

			/* already have 1 digit, add 6 more */
			result = (result * 1000000) + (nxt1 * 100) + (nxt2 / 100);
		}

		result = result | (weight << 24);
	}

	/* the abbrev is negated relative to the original */
	if (var->sign == NUMERIC_POS)
		result = -result;

	if (nss->estimating)
	{
		uint32		tmp = (uint32) result;

		addHyperLogLog(&nss->abbr_card, DatumGetUInt32(hash_uint32(tmp)));
	}

	return NumericAbbrevGetDatum(result);
}

#endif							/* NUMERIC_ABBREV_BITS == 32 */

/*
 * Ordinary (non-sortsupport) comparisons follow.
 */

Datum
numeric_cmp(PG_FUNCTION_ARGS)
{
	Numeric		num1 = PG_GETARG_NUMERIC(0);
	Numeric		num2 = PG_GETARG_NUMERIC(1);
	int			result;

	result = cmp_numerics(num1, num2);

	PG_RETURN_INT32(result);
}


Datum
numeric_eq(PG_FUNCTION_ARGS)
{
	Numeric		num1 = PG_GETARG_NUMERIC(0);
	Numeric		num2 = PG_GETARG_NUMERIC(1);
	bool		result;

	result = cmp_numerics(num1, num2) == 0;

	PG_RETURN_BOOL(result);
}

Datum
numeric_ne(PG_FUNCTION_ARGS)
{
	Numeric		num1 = PG_GETARG_NUMERIC(0);
	Numeric		num2 = PG_GETARG_NUMERIC(1);
	bool		result;

	result = cmp_numerics(num1, num2) != 0;

	PG_RETURN_BOOL(result);
}

Datum
numeric_gt(PG_FUNCTION_ARGS)
{
	Numeric		num1 = PG_GETARG_NUMERIC(0);
	Numeric		num2 = PG_GETARG_NUMERIC(1);
	bool		result;

	result = cmp_numerics(num1, num2) > 0;

	PG_RETURN_BOOL(result);
}

Datum
numeric_ge(PG_FUNCTION_ARGS)
{
	Numeric		num1 = PG_GETARG_NUMERIC(0);
	Numeric		num2 = PG_GETARG_NUMERIC(1);
	bool		result;

	result = cmp_numerics(num1, num2) >= 0;

	PG_RETURN_BOOL(result);
}

Datum
numeric_lt(PG_FUNCTION_ARGS)
{
	Numeric		num1 = PG_GETARG_NUMERIC(0);
	Numeric		num2 = PG_GETARG_NUMERIC(1);
	bool		result;

	result = cmp_numerics(num1, num2) < 0;

	PG_RETURN_BOOL(result);
}

Datum
numeric_le(PG_FUNCTION_ARGS)
{
	Numeric		num1 = PG_GETARG_NUMERIC(0);
	Numeric		num2 = PG_GETARG_NUMERIC(1);
	bool		result;

	result = cmp_numerics(num1, num2) <= 0;

	PG_RETURN_BOOL(result);
}

static int
cmp_numerics(Numeric num1, Numeric num2)
{
	int			result;

	/*
	 * We consider all NANs to be equal and larger than any non-NAN (including
	 * Infinity).  This is somewhat arbitrary; the important thing is to have
	 * a consistent sort order.
	 */
	if (NUMERIC_IS_SPECIAL(num1))
	{
		if (NUMERIC_IS_NAN(num1))
		{
			if (NUMERIC_IS_NAN(num2))
				result = 0;		/* NAN = NAN */
			else
				result = 1;		/* NAN > non-NAN */
		}
		else if (NUMERIC_IS_PINF(num1))
		{
			if (NUMERIC_IS_NAN(num2))
				result = -1;	/* PINF < NAN */
			else if (NUMERIC_IS_PINF(num2))
				result = 0;		/* PINF = PINF */
			else
				result = 1;		/* PINF > anything else */
		}
		else					/* num1 must be NINF */
		{
			if (NUMERIC_IS_NINF(num2))
				result = 0;		/* NINF = NINF */
			else
				result = -1;	/* NINF < anything else */
		}
	}
	else if (NUMERIC_IS_SPECIAL(num2))
	{
		if (NUMERIC_IS_NINF(num2))
			result = 1;			/* normal > NINF */
		else
			result = -1;		/* normal < NAN or PINF */
	}
	else
	{
		result = numeric_dec128_compare(num1, num2);
	}

	return result;
}

/*
 * in_range support function for numeric.
 */
Datum
in_range_numeric_numeric(PG_FUNCTION_ARGS)
{
	Numeric		val = PG_GETARG_NUMERIC(0);
	Numeric		base = PG_GETARG_NUMERIC(1);
	Numeric		offset = PG_GETARG_NUMERIC(2);
	bool		sub = PG_GETARG_BOOL(3);
	bool		less = PG_GETARG_BOOL(4);
	bool		result;

	/*
	 * Reject negative (including -Inf) or NaN offset.  Negative is per spec,
	 * and NaN is because appropriate semantics for that seem non-obvious.
	 */
	if (NUMERIC_IS_NAN(offset) ||
		NUMERIC_IS_NINF(offset) ||
		NUMERIC_SIGN(offset) == NUMERIC_NEG)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE),
				 errmsg("invalid preceding or following size in window function")));

	/*
	 * Deal with cases where val and/or base is NaN, following the rule that
	 * NaN sorts after non-NaN (cf cmp_numerics).  The offset cannot affect
	 * the conclusion.
	 */
	if (NUMERIC_IS_NAN(val))
	{
		if (NUMERIC_IS_NAN(base))
			result = true;		/* NAN = NAN */
		else
			result = !less;		/* NAN > non-NAN */
	}
	else if (NUMERIC_IS_NAN(base))
	{
		result = less;			/* non-NAN < NAN */
	}

	/*
	 * Deal with infinite offset (necessarily +Inf, at this point).
	 */
	else if (NUMERIC_IS_SPECIAL(offset))
	{
		Assert(NUMERIC_IS_PINF(offset));
		if (sub ? NUMERIC_IS_PINF(base) : NUMERIC_IS_NINF(base))
		{
			/*
			 * base +/- offset would produce NaN, so return true for any val
			 * (see in_range_float8_float8() for reasoning).
			 */
			result = true;
		}
		else if (sub)
		{
			/* base - offset must be -inf */
			if (less)
				result = NUMERIC_IS_NINF(val);	/* only -inf is <= sum */
			else
				result = true;	/* any val is >= sum */
		}
		else
		{
			/* base + offset must be +inf */
			if (less)
				result = true;	/* any val is <= sum */
			else
				result = NUMERIC_IS_PINF(val);	/* only +inf is >= sum */
		}
	}

	/*
	 * Deal with cases where val and/or base is infinite.  The offset, being
	 * now known finite, cannot affect the conclusion.
	 */
	else if (NUMERIC_IS_SPECIAL(val))
	{
		if (NUMERIC_IS_PINF(val))
		{
			if (NUMERIC_IS_PINF(base))
				result = true;	/* PINF = PINF */
			else
				result = !less; /* PINF > any other non-NAN */
		}
		else					/* val must be NINF */
		{
			if (NUMERIC_IS_NINF(base))
				result = true;	/* NINF = NINF */
			else
				result = less;	/* NINF < anything else */
		}
	}
	else if (NUMERIC_IS_SPECIAL(base))
	{
		if (NUMERIC_IS_NINF(base))
			result = !less;		/* normal > NINF */
		else
			result = less;		/* normal < PINF */
	}
	else
	{
		/*
		 * Otherwise go ahead and compute base +/- offset.  While it's
		 * possible for this to overflow the numeric format, it's unlikely
		 * enough that we don't take measures to prevent it.
		 */
		NumericVar	valv;
		NumericVar	basev;
		NumericVar	offsetv;
		NumericVar	sum;

		init_var_from_num(val, &valv);
		init_var_from_num(base, &basev);
		init_var_from_num(offset, &offsetv);
		init_var(&sum);

		if (sub)
			sub_var(&basev, &offsetv, &sum);
		else
			add_var(&basev, &offsetv, &sum);

		if (less)
			result = (cmp_var(&valv, &sum) <= 0);
		else
			result = (cmp_var(&valv, &sum) >= 0);

		free_var(&sum);
	}

	PG_RETURN_BOOL(result);
}

Datum
hash_numeric(PG_FUNCTION_ARGS)
{
	Numeric		key = PG_GETARG_NUMERIC(0);
	NumericData canon;

	/* If it's NaN or infinity, don't try to hash the rest of the fields */
	if (NUMERIC_IS_SPECIAL(key))
		PG_RETURN_UINT32(0);

	/*
	 * Reduce to canonical (trailing-zero-stripped) form first: two numerics
	 * can compare equal with different scales (1.5 = 1.50) and must hash the
	 * same.  The canonical mantissa's sign is included in the hashed bytes;
	 * that's fine, since a sign difference implies inequality anyway.
	 */
	numeric_dec128_canonical(key, &canon);

	PG_RETURN_DATUM(hash_any((unsigned char *) &canon, sizeof(NumericData)));
}

/*
 * Returns 64-bit value by hashing a value to a 64-bit value, with a seed.
 * Otherwise, similar to hash_numeric.
 */
Datum
hash_numeric_extended(PG_FUNCTION_ARGS)
{
	Numeric		key = PG_GETARG_NUMERIC(0);
	uint64		seed = PG_GETARG_INT64(1);
	NumericData canon;

	/* If it's NaN or infinity, don't try to hash the rest of the fields */
	if (NUMERIC_IS_SPECIAL(key))
		PG_RETURN_UINT64(seed);

	numeric_dec128_canonical(key, &canon);

	PG_RETURN_DATUM(hash_any_extended((unsigned char *) &canon,
									  sizeof(NumericData), seed));
}


/* ----------------------------------------------------------------------
 *
 * Basic arithmetic functions
 *
 * ----------------------------------------------------------------------
 */


/*
 * numeric_add() -
 *
 *	Add two numerics
 */
Datum
numeric_add(PG_FUNCTION_ARGS)
{
	Numeric		num1 = PG_GETARG_NUMERIC(0);
	Numeric		num2 = PG_GETARG_NUMERIC(1);
	Numeric		res;

	res = numeric_add_opt_error(num1, num2, NULL);

	PG_RETURN_NUMERIC(res);
}

/*
 * numeric_add_opt_error() -
 *
 *	Internal version of numeric_add().  If "*have_error" flag is provided,
 *	on error it's set to true, NULL returned.  This is helpful when caller
 *	need to handle errors by itself.
 */
Numeric
numeric_add_opt_error(Numeric num1, Numeric num2, bool *have_error)
{
	NumericVar	arg1;
	NumericVar	arg2;
	NumericVar	result;
	Numeric		res;

	/*
	 * Handle NaN and infinities
	 */
	if (NUMERIC_IS_SPECIAL(num1) || NUMERIC_IS_SPECIAL(num2))
	{
		if (NUMERIC_IS_NAN(num1) || NUMERIC_IS_NAN(num2))
			return make_result(&const_nan);
		if (NUMERIC_IS_PINF(num1))
		{
			if (NUMERIC_IS_NINF(num2))
				return make_result(&const_nan); /* Inf + -Inf */
			else
				return make_result(&const_pinf);
		}
		if (NUMERIC_IS_NINF(num1))
		{
			if (NUMERIC_IS_PINF(num2))
				return make_result(&const_nan); /* -Inf + Inf */
			else
				return make_result(&const_ninf);
		}
		/* by here, num1 must be finite, so num2 is not */
		if (NUMERIC_IS_PINF(num2))
			return make_result(&const_pinf);
		Assert(NUMERIC_IS_NINF(num2));
		return make_result(&const_ninf);
	}

	/*
	 * Fast path (SPEC-numeric-as-dec128.md sec. 6 step 7): try direct
	 * mantissa arithmetic first, without going anywhere near NumericVar.
	 * Falls back to the general path below for anything it can't handle
	 * (mainly: aligning the scales, or the result, would leave dec128's
	 * 37-digit range).
	 *
	 * Compute into a local and copy out only on success, so that the fallback
	 * costs no allocation at all.  An earlier version palloc'd up front and
	 * pfree'd on the way out, which put an AllocSetAlloc/AllocSetFree pair --
	 * for a payload no bigger than its own chunk header -- in front of every
	 * slow-path operation.
	 */
	{
		NumericData fast;

		if (numeric_dec128_add_sub(num1, num2, false, &fast))
		{
			res = (Numeric) palloc(sizeof(NumericData));
			*res = fast;
			if (have_error)
				*have_error = false;
			return res;
		}
	}

	/*
	 * Unpack the values, let add_var() compute the result and return it.
	 */
	init_var_from_num(num1, &arg1);
	init_var_from_num(num2, &arg2);

	init_var(&result);
	add_var(&arg1, &arg2, &result);

	res = make_result_opt_error(&result, have_error);

	free_var(&result);

	return res;
}


/*
 * numeric_sub() -
 *
 *	Subtract one numeric from another
 */
Datum
numeric_sub(PG_FUNCTION_ARGS)
{
	Numeric		num1 = PG_GETARG_NUMERIC(0);
	Numeric		num2 = PG_GETARG_NUMERIC(1);
	Numeric		res;

	res = numeric_sub_opt_error(num1, num2, NULL);

	PG_RETURN_NUMERIC(res);
}


/*
 * numeric_sub_opt_error() -
 *
 *	Internal version of numeric_sub().  If "*have_error" flag is provided,
 *	on error it's set to true, NULL returned.  This is helpful when caller
 *	need to handle errors by itself.
 */
Numeric
numeric_sub_opt_error(Numeric num1, Numeric num2, bool *have_error)
{
	NumericVar	arg1;
	NumericVar	arg2;
	NumericVar	result;
	Numeric		res;

	/*
	 * Handle NaN and infinities
	 */
	if (NUMERIC_IS_SPECIAL(num1) || NUMERIC_IS_SPECIAL(num2))
	{
		if (NUMERIC_IS_NAN(num1) || NUMERIC_IS_NAN(num2))
			return make_result(&const_nan);
		if (NUMERIC_IS_PINF(num1))
		{
			if (NUMERIC_IS_PINF(num2))
				return make_result(&const_nan); /* Inf - Inf */
			else
				return make_result(&const_pinf);
		}
		if (NUMERIC_IS_NINF(num1))
		{
			if (NUMERIC_IS_NINF(num2))
				return make_result(&const_nan); /* -Inf - -Inf */
			else
				return make_result(&const_ninf);
		}
		/* by here, num1 must be finite, so num2 is not */
		if (NUMERIC_IS_PINF(num2))
			return make_result(&const_ninf);
		Assert(NUMERIC_IS_NINF(num2));
		return make_result(&const_pinf);
	}

	/* Fast path -- see the matching comment in numeric_add_opt_error() */
	{
		NumericData fast;

		if (numeric_dec128_add_sub(num1, num2, true, &fast))
		{
			res = (Numeric) palloc(sizeof(NumericData));
			*res = fast;
			if (have_error)
				*have_error = false;
			return res;
		}
	}

	/*
	 * Unpack the values, let sub_var() compute the result and return it.
	 */
	init_var_from_num(num1, &arg1);
	init_var_from_num(num2, &arg2);

	init_var(&result);
	sub_var(&arg1, &arg2, &result);

	res = make_result_opt_error(&result, have_error);

	free_var(&result);

	return res;
}


/*
 * numeric_mul() -
 *
 *	Calculate the product of two numerics
 */
Datum
numeric_mul(PG_FUNCTION_ARGS)
{
	Numeric		num1 = PG_GETARG_NUMERIC(0);
	Numeric		num2 = PG_GETARG_NUMERIC(1);
	Numeric		res;

	res = numeric_mul_opt_error(num1, num2, NULL);

	PG_RETURN_NUMERIC(res);
}


/*
 * numeric_mul_opt_error() -
 *
 *	Internal version of numeric_mul().  If "*have_error" flag is provided,
 *	on error it's set to true, NULL returned.  This is helpful when caller
 *	need to handle errors by itself.
 */
Numeric
numeric_mul_opt_error(Numeric num1, Numeric num2, bool *have_error)
{
	NumericVar	arg1;
	NumericVar	arg2;
	NumericVar	result;
	Numeric		res;

	/*
	 * Handle NaN and infinities
	 */
	if (NUMERIC_IS_SPECIAL(num1) || NUMERIC_IS_SPECIAL(num2))
	{
		if (NUMERIC_IS_NAN(num1) || NUMERIC_IS_NAN(num2))
			return make_result(&const_nan);
		if (NUMERIC_IS_PINF(num1))
		{
			switch (numeric_sign_internal(num2))
			{
				case 0:
					return make_result(&const_nan); /* Inf * 0 */
				case 1:
					return make_result(&const_pinf);
				case -1:
					return make_result(&const_ninf);
			}
			Assert(false);
		}
		if (NUMERIC_IS_NINF(num1))
		{
			switch (numeric_sign_internal(num2))
			{
				case 0:
					return make_result(&const_nan); /* -Inf * 0 */
				case 1:
					return make_result(&const_ninf);
				case -1:
					return make_result(&const_pinf);
			}
			Assert(false);
		}
		/* by here, num1 must be finite, so num2 is not */
		if (NUMERIC_IS_PINF(num2))
		{
			switch (numeric_sign_internal(num1))
			{
				case 0:
					return make_result(&const_nan); /* 0 * Inf */
				case 1:
					return make_result(&const_pinf);
				case -1:
					return make_result(&const_ninf);
			}
			Assert(false);
		}
		Assert(NUMERIC_IS_NINF(num2));
		switch (numeric_sign_internal(num1))
		{
			case 0:
				return make_result(&const_nan); /* 0 * -Inf */
			case 1:
				return make_result(&const_ninf);
			case -1:
				return make_result(&const_pinf);
		}
		Assert(false);
	}

	/*
	 * Unpack the values, let mul_var() compute the result and return it.
	 * Unlike add_var() and sub_var(), mul_var() will round its result. In the
	 * case of numeric_mul(), which is invoked for the * operator on numerics,
	 * we request exact representation for the product (rscale = sum(dscale of
	 * arg1, dscale of arg2)).  If the exact result has more digits after the
	 * decimal point than can be stored in a numeric, we round it.  Rounding
	 * after computing the exact result ensures that the final result is
	 * correctly rounded (rounding in mul_var() using a truncated product
	 * would not guarantee this).
	 */

	/*
	 * Fast path (SPEC-numeric-as-dec128.md sec. 6 step 7): direct mantissa
	 * multiply, only when the exact product needs no rounding at all.  See
	 * numeric_dec128_mul()'s comment for why this is deliberately narrow.
	 */
	{
		NumericData fast;

		if (numeric_dec128_mul(num1, num2, &fast))
		{
			res = (Numeric) palloc(sizeof(NumericData));
			*res = fast;
			if (have_error)
				*have_error = false;
			return res;
		}
	}

	init_var_from_num(num1, &arg1);
	init_var_from_num(num2, &arg2);

	init_var(&result);
	mul_var(&arg1, &arg2, &result, arg1.dscale + arg2.dscale);

	if (result.dscale > NUMERIC_DSCALE_MAX)
		round_var(&result, NUMERIC_DSCALE_MAX);

	res = make_result_opt_error(&result, have_error);

	free_var(&result);

	return res;
}


/*
 * numeric_div() -
 *
 *	Divide one numeric into another
 */
Datum
numeric_div(PG_FUNCTION_ARGS)
{
	Numeric		num1 = PG_GETARG_NUMERIC(0);
	Numeric		num2 = PG_GETARG_NUMERIC(1);
	Numeric		res;

	res = numeric_div_opt_error(num1, num2, NULL);

	PG_RETURN_NUMERIC(res);
}


/*
 * numeric_div_opt_error() -
 *
 *	Internal version of numeric_div().  If "*have_error" flag is provided,
 *	on error it's set to true, NULL returned.  This is helpful when caller
 *	need to handle errors by itself.
 */
Numeric
numeric_div_opt_error(Numeric num1, Numeric num2, bool *have_error)
{
	NumericVar	arg1;
	NumericVar	arg2;
	NumericVar	result;
	Numeric		res;
	int			rscale;

	if (have_error)
		*have_error = false;

	/*
	 * Handle NaN and infinities
	 */
	if (NUMERIC_IS_SPECIAL(num1) || NUMERIC_IS_SPECIAL(num2))
	{
		if (NUMERIC_IS_NAN(num1) || NUMERIC_IS_NAN(num2))
			return make_result(&const_nan);
		if (NUMERIC_IS_PINF(num1))
		{
			if (NUMERIC_IS_SPECIAL(num2))
				return make_result(&const_nan); /* Inf / [-]Inf */
			switch (numeric_sign_internal(num2))
			{
				case 0:
					if (have_error)
					{
						*have_error = true;
						return NULL;
					}
					ereport(ERROR,
							(errcode(ERRCODE_DIVISION_BY_ZERO),
							 errmsg("division by zero")));
					break;
				case 1:
					return make_result(&const_pinf);
				case -1:
					return make_result(&const_ninf);
			}
			Assert(false);
		}
		if (NUMERIC_IS_NINF(num1))
		{
			if (NUMERIC_IS_SPECIAL(num2))
				return make_result(&const_nan); /* -Inf / [-]Inf */
			switch (numeric_sign_internal(num2))
			{
				case 0:
					if (have_error)
					{
						*have_error = true;
						return NULL;
					}
					ereport(ERROR,
							(errcode(ERRCODE_DIVISION_BY_ZERO),
							 errmsg("division by zero")));
					break;
				case 1:
					return make_result(&const_ninf);
				case -1:
					return make_result(&const_pinf);
			}
			Assert(false);
		}
		/* by here, num1 must be finite, so num2 is not */

		/*
		 * POSIX would have us return zero or minus zero if num1 is zero, and
		 * otherwise throw an underflow error.  But the numeric type doesn't
		 * really do underflow, so let's just return zero.
		 */
		return make_result(&const_zero);
	}

	/*
	 * Fast path: long division on the mantissas, at the same result scale the
	 * general path below would pick.  Skipped for a zero divisor so that the
	 * error still comes from div_var(), keeping one code path responsible for
	 * it.
	 */
	if (numeric_pack_mant(num2) != 0)
	{
		NumericData fast;

		if (numeric_dec128_div(num1, num2, &fast))
		{
			res = (Numeric) palloc(sizeof(NumericData));
			*res = fast;
			if (have_error)
				*have_error = false;
			return res;
		}
	}

	/*
	 * Unpack the arguments
	 */
	init_var_from_num(num1, &arg1);
	init_var_from_num(num2, &arg2);

	init_var(&result);

	/*
	 * Select scale for division result
	 *
	 * EXPERIMENTAL (SPEC-numeric-as-dec128.md): select_div_scale() aims for
	 * NUMERIC_MIN_SIG_DIGITS (16) significant digits, which very commonly
	 * exceeds dec128's NUMERIC_MAX_STORED_SCALE (15) -- e.g. plain 2/1 asks
	 * for rscale 16.  Uncapped, nearly every division would overflow the
	 * packed format instead of just losing a bit of precision.  This is the
	 * documented "division scale diverges from stock numeric" regression
	 * (SPEC-numeric-as-dec128.md sec. 3.3): clamp rather than error.
	 */
	rscale = select_div_scale(&arg1, &arg2);
	rscale = Min(rscale, NUMERIC_MAX_STORED_SCALE);

	/*
	 * If "have_error" is provided, check for division by zero here
	 */
	if (have_error && (arg2.ndigits == 0 || arg2.digits[0] == 0))
	{
		*have_error = true;
		return NULL;
	}

	/*
	 * Do the divide and return the result
	 */
	div_var(&arg1, &arg2, &result, rscale, true, true);

	res = make_result_opt_error(&result, have_error);

	free_var(&result);

	return res;
}


/*
 * numeric_div_trunc() -
 *
 *	Divide one numeric into another, truncating the result to an integer
 */
Datum
numeric_div_trunc(PG_FUNCTION_ARGS)
{
	Numeric		num1 = PG_GETARG_NUMERIC(0);
	Numeric		num2 = PG_GETARG_NUMERIC(1);
	NumericVar	arg1;
	NumericVar	arg2;
	NumericVar	result;
	Numeric		res;

	/*
	 * Handle NaN and infinities
	 */
	if (NUMERIC_IS_SPECIAL(num1) || NUMERIC_IS_SPECIAL(num2))
	{
		if (NUMERIC_IS_NAN(num1) || NUMERIC_IS_NAN(num2))
			PG_RETURN_NUMERIC(make_result(&const_nan));
		if (NUMERIC_IS_PINF(num1))
		{
			if (NUMERIC_IS_SPECIAL(num2))
				PG_RETURN_NUMERIC(make_result(&const_nan)); /* Inf / [-]Inf */
			switch (numeric_sign_internal(num2))
			{
				case 0:
					ereport(ERROR,
							(errcode(ERRCODE_DIVISION_BY_ZERO),
							 errmsg("division by zero")));
					break;
				case 1:
					PG_RETURN_NUMERIC(make_result(&const_pinf));
				case -1:
					PG_RETURN_NUMERIC(make_result(&const_ninf));
			}
			Assert(false);
		}
		if (NUMERIC_IS_NINF(num1))
		{
			if (NUMERIC_IS_SPECIAL(num2))
				PG_RETURN_NUMERIC(make_result(&const_nan)); /* -Inf / [-]Inf */
			switch (numeric_sign_internal(num2))
			{
				case 0:
					ereport(ERROR,
							(errcode(ERRCODE_DIVISION_BY_ZERO),
							 errmsg("division by zero")));
					break;
				case 1:
					PG_RETURN_NUMERIC(make_result(&const_ninf));
				case -1:
					PG_RETURN_NUMERIC(make_result(&const_pinf));
			}
			Assert(false);
		}
		/* by here, num1 must be finite, so num2 is not */

		/*
		 * POSIX would have us return zero or minus zero if num1 is zero, and
		 * otherwise throw an underflow error.  But the numeric type doesn't
		 * really do underflow, so let's just return zero.
		 */
		PG_RETURN_NUMERIC(make_result(&const_zero));
	}

	/*
	 * Unpack the arguments
	 */
	init_var_from_num(num1, &arg1);
	init_var_from_num(num2, &arg2);

	init_var(&result);

	/*
	 * Do the divide and return the result
	 */
	div_var(&arg1, &arg2, &result, 0, false, true);

	res = make_result(&result);

	free_var(&result);

	PG_RETURN_NUMERIC(res);
}


/*
 * numeric_mod() -
 *
 *	Calculate the modulo of two numerics
 */
Datum
numeric_mod(PG_FUNCTION_ARGS)
{
	Numeric		num1 = PG_GETARG_NUMERIC(0);
	Numeric		num2 = PG_GETARG_NUMERIC(1);
	Numeric		res;

	res = numeric_mod_opt_error(num1, num2, NULL);

	PG_RETURN_NUMERIC(res);
}


/*
 * numeric_mod_opt_error() -
 *
 *	Internal version of numeric_mod().  If "*have_error" flag is provided,
 *	on error it's set to true, NULL returned.  This is helpful when caller
 *	need to handle errors by itself.
 */
Numeric
numeric_mod_opt_error(Numeric num1, Numeric num2, bool *have_error)
{
	Numeric		res;
	NumericVar	arg1;
	NumericVar	arg2;
	NumericVar	result;

	if (have_error)
		*have_error = false;

	/*
	 * Handle NaN and infinities.  We follow POSIX fmod() on this, except that
	 * POSIX treats x-is-infinite and y-is-zero identically, raising EDOM and
	 * returning NaN.  We choose to throw error only for y-is-zero.
	 */
	if (NUMERIC_IS_SPECIAL(num1) || NUMERIC_IS_SPECIAL(num2))
	{
		if (NUMERIC_IS_NAN(num1) || NUMERIC_IS_NAN(num2))
			return make_result(&const_nan);
		if (NUMERIC_IS_INF(num1))
		{
			if (numeric_sign_internal(num2) == 0)
			{
				if (have_error)
				{
					*have_error = true;
					return NULL;
				}
				ereport(ERROR,
						(errcode(ERRCODE_DIVISION_BY_ZERO),
						 errmsg("division by zero")));
			}
			/* Inf % any nonzero = NaN */
			return make_result(&const_nan);
		}
		/* num2 must be [-]Inf; result is num1 regardless of sign of num2 */
		return duplicate_numeric(num1);
	}

	init_var_from_num(num1, &arg1);
	init_var_from_num(num2, &arg2);

	init_var(&result);

	/*
	 * If "have_error" is provided, check for division by zero here
	 */
	if (have_error && (arg2.ndigits == 0 || arg2.digits[0] == 0))
	{
		*have_error = true;
		return NULL;
	}

	mod_var(&arg1, &arg2, &result);

	res = make_result_opt_error(&result, NULL);

	free_var(&result);

	return res;
}


/*
 * numeric_inc() -
 *
 *	Increment a number by one
 */
Datum
numeric_inc(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);
	NumericVar	arg;
	Numeric		res;

	/*
	 * Handle NaN and infinities
	 */
	if (NUMERIC_IS_SPECIAL(num))
		PG_RETURN_NUMERIC(duplicate_numeric(num));

	/*
	 * Compute the result and return it
	 */
	init_var_from_num(num, &arg);

	add_var(&arg, &const_one, &arg);

	res = make_result(&arg);

	free_var(&arg);

	PG_RETURN_NUMERIC(res);
}


/*
 * numeric_smaller() -
 *
 *	Return the smaller of two numbers
 */
Datum
numeric_smaller(PG_FUNCTION_ARGS)
{
	Numeric		num1 = PG_GETARG_NUMERIC(0);
	Numeric		num2 = PG_GETARG_NUMERIC(1);

	/*
	 * Use cmp_numerics so that this will agree with the comparison operators,
	 * particularly as regards comparisons involving NaN.
	 */
	if (cmp_numerics(num1, num2) < 0)
		PG_RETURN_NUMERIC(num1);
	else
		PG_RETURN_NUMERIC(num2);
}


/*
 * numeric_larger() -
 *
 *	Return the larger of two numbers
 */
Datum
numeric_larger(PG_FUNCTION_ARGS)
{
	Numeric		num1 = PG_GETARG_NUMERIC(0);
	Numeric		num2 = PG_GETARG_NUMERIC(1);

	/*
	 * Use cmp_numerics so that this will agree with the comparison operators,
	 * particularly as regards comparisons involving NaN.
	 */
	if (cmp_numerics(num1, num2) > 0)
		PG_RETURN_NUMERIC(num1);
	else
		PG_RETURN_NUMERIC(num2);
}


/* ----------------------------------------------------------------------
 *
 * Advanced math functions
 *
 * ----------------------------------------------------------------------
 */

/*
 * numeric_gcd() -
 *
 *	Calculate the greatest common divisor of two numerics
 */
Datum
numeric_gcd(PG_FUNCTION_ARGS)
{
	Numeric		num1 = PG_GETARG_NUMERIC(0);
	Numeric		num2 = PG_GETARG_NUMERIC(1);
	NumericVar	arg1;
	NumericVar	arg2;
	NumericVar	result;
	Numeric		res;

	/*
	 * Handle NaN and infinities: we consider the result to be NaN in all such
	 * cases.
	 */
	if (NUMERIC_IS_SPECIAL(num1) || NUMERIC_IS_SPECIAL(num2))
		PG_RETURN_NUMERIC(make_result(&const_nan));

	/*
	 * Unpack the arguments
	 */
	init_var_from_num(num1, &arg1);
	init_var_from_num(num2, &arg2);

	init_var(&result);

	/*
	 * Find the GCD and return the result
	 */
	gcd_var(&arg1, &arg2, &result);

	res = make_result(&result);

	free_var(&result);

	PG_RETURN_NUMERIC(res);
}


/*
 * numeric_lcm() -
 *
 *	Calculate the least common multiple of two numerics
 */
Datum
numeric_lcm(PG_FUNCTION_ARGS)
{
	Numeric		num1 = PG_GETARG_NUMERIC(0);
	Numeric		num2 = PG_GETARG_NUMERIC(1);
	NumericVar	arg1;
	NumericVar	arg2;
	NumericVar	result;
	Numeric		res;

	/*
	 * Handle NaN and infinities: we consider the result to be NaN in all such
	 * cases.
	 */
	if (NUMERIC_IS_SPECIAL(num1) || NUMERIC_IS_SPECIAL(num2))
		PG_RETURN_NUMERIC(make_result(&const_nan));

	/*
	 * Unpack the arguments
	 */
	init_var_from_num(num1, &arg1);
	init_var_from_num(num2, &arg2);

	init_var(&result);

	/*
	 * Compute the result using lcm(x, y) = abs(x / gcd(x, y) * y), returning
	 * zero if either input is zero.
	 *
	 * Note that the division is guaranteed to be exact, returning an integer
	 * result, so the LCM is an integral multiple of both x and y.  A display
	 * scale of Min(x.dscale, y.dscale) would be sufficient to represent it,
	 * but as with other numeric functions, we choose to return a result whose
	 * display scale is no smaller than either input.
	 */
	if (arg1.ndigits == 0 || arg2.ndigits == 0)
		set_var_from_var(&const_zero, &result);
	else
	{
		gcd_var(&arg1, &arg2, &result);
		div_var(&arg1, &result, &result, 0, false, true);
		mul_var(&arg2, &result, &result, arg2.dscale);
		result.sign = NUMERIC_POS;
	}

	result.dscale = Max(arg1.dscale, arg2.dscale);

	res = make_result(&result);

	free_var(&result);

	PG_RETURN_NUMERIC(res);
}


/*
 * numeric_fac()
 *
 * Compute factorial
 */
Datum
numeric_fac(PG_FUNCTION_ARGS)
{
	int64		num = PG_GETARG_INT64(0);
	Numeric		res;
	NumericVar	fact;
	NumericVar	result;

	if (num < 0)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("factorial of a negative number is undefined")));
	if (num <= 1)
	{
		res = make_result(&const_one);
		PG_RETURN_NUMERIC(res);
	}
	/* Fail immediately if the result would overflow */
	if (num > 32177)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("value overflows numeric format")));

	init_var(&fact);
	init_var(&result);

	int64_to_numericvar(num, &result);

	for (num = num - 1; num > 1; num--)
	{
		/* this loop can take awhile, so allow it to be interrupted */
		CHECK_FOR_INTERRUPTS();

		int64_to_numericvar(num, &fact);

		mul_var(&result, &fact, &result, 0);
	}

	res = make_result(&result);

	free_var(&fact);
	free_var(&result);

	PG_RETURN_NUMERIC(res);
}


/*
 * numeric_sqrt() -
 *
 *	Compute the square root of a numeric.
 */
Datum
numeric_sqrt(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);
	Numeric		res;
	NumericVar	arg;
	NumericVar	result;
	int			sweight;
	int			rscale;

	/*
	 * Handle NaN and infinities
	 */
	if (NUMERIC_IS_SPECIAL(num))
	{
		/* error should match that in sqrt_var() */
		if (NUMERIC_IS_NINF(num))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION),
					 errmsg("cannot take square root of a negative number")));
		/* For NAN or PINF, just duplicate the input */
		PG_RETURN_NUMERIC(duplicate_numeric(num));
	}

	/*
	 * Unpack the argument and determine the result scale.  We choose a scale
	 * to give at least NUMERIC_MIN_SIG_DIGITS significant digits; but in any
	 * case not less than the input's dscale.
	 */
	init_var_from_num(num, &arg);

	init_var(&result);

	/*
	 * Assume the input was normalized, so arg.weight is accurate.  The result
	 * then has at least sweight = floor(arg.weight * DEC_DIGITS / 2 + 1)
	 * digits before the decimal point.  When DEC_DIGITS is even, we can save
	 * a few cycles, since the division is exact and there is no need to round
	 * towards negative infinity.
	 */
#if DEC_DIGITS == ((DEC_DIGITS / 2) * 2)
	sweight = arg.weight * DEC_DIGITS / 2 + 1;
#else
	if (arg.weight >= 0)
		sweight = arg.weight * DEC_DIGITS / 2 + 1;
	else
		sweight = 1 - (1 - arg.weight * DEC_DIGITS) / 2;
#endif

	rscale = NUMERIC_MIN_SIG_DIGITS - sweight;
	rscale = Max(rscale, arg.dscale);
	rscale = Max(rscale, NUMERIC_MIN_DISPLAY_SCALE);
	rscale = Min(rscale, NUMERIC_MAX_DISPLAY_SCALE);

	/*
	 * Let sqrt_var() do the calculation and return the result.
	 */
	sqrt_var(&arg, &result, rscale);

	res = make_result(&result);

	free_var(&result);

	PG_RETURN_NUMERIC(res);
}


/*
 * numeric_exp() -
 *
 *	Raise e to the power of x
 */
Datum
numeric_exp(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);
	Numeric		res;
	NumericVar	arg;
	NumericVar	result;
	int			rscale;
	double		val;

	/*
	 * Handle NaN and infinities
	 */
	if (NUMERIC_IS_SPECIAL(num))
	{
		/* Per POSIX, exp(-Inf) is zero */
		if (NUMERIC_IS_NINF(num))
			PG_RETURN_NUMERIC(make_result(&const_zero));
		/* For NAN or PINF, just duplicate the input */
		PG_RETURN_NUMERIC(duplicate_numeric(num));
	}

	/*
	 * Unpack the argument and determine the result scale.  We choose a scale
	 * to give at least NUMERIC_MIN_SIG_DIGITS significant digits; but in any
	 * case not less than the input's dscale.
	 */
	init_var_from_num(num, &arg);

	init_var(&result);

	/* convert input to float8, ignoring overflow */
	val = numericvar_to_double_no_overflow(&arg);

	/*
	 * log10(result) = num * log10(e), so this is approximately the decimal
	 * weight of the result:
	 */
	val *= 0.434294481903252;

	/* limit to something that won't cause integer overflow */
	val = Max(val, -NUMERIC_MAX_RESULT_SCALE);
	val = Min(val, NUMERIC_MAX_RESULT_SCALE);

	rscale = NUMERIC_MIN_SIG_DIGITS - (int) val;
	rscale = Max(rscale, arg.dscale);
	rscale = Max(rscale, NUMERIC_MIN_DISPLAY_SCALE);
	rscale = Min(rscale, NUMERIC_MAX_DISPLAY_SCALE);

	/*
	 * Let exp_var() do the calculation and return the result.
	 */
	exp_var(&arg, &result, rscale);

	res = make_result(&result);

	free_var(&result);

	PG_RETURN_NUMERIC(res);
}


/*
 * numeric_ln() -
 *
 *	Compute the natural logarithm of x
 */
Datum
numeric_ln(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);
	Numeric		res;
	NumericVar	arg;
	NumericVar	result;
	int			ln_dweight;
	int			rscale;

	/*
	 * Handle NaN and infinities
	 */
	if (NUMERIC_IS_SPECIAL(num))
	{
		if (NUMERIC_IS_NINF(num))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_ARGUMENT_FOR_LOG),
					 errmsg("cannot take logarithm of a negative number")));
		/* For NAN or PINF, just duplicate the input */
		PG_RETURN_NUMERIC(duplicate_numeric(num));
	}

	init_var_from_num(num, &arg);
	init_var(&result);

	/* Estimated dweight of logarithm */
	ln_dweight = estimate_ln_dweight(&arg);

	rscale = NUMERIC_MIN_SIG_DIGITS - ln_dweight;
	rscale = Max(rscale, arg.dscale);
	rscale = Max(rscale, NUMERIC_MIN_DISPLAY_SCALE);
	rscale = Min(rscale, NUMERIC_MAX_DISPLAY_SCALE);

	ln_var(&arg, &result, rscale);

	res = make_result(&result);

	free_var(&result);

	PG_RETURN_NUMERIC(res);
}


/*
 * numeric_log() -
 *
 *	Compute the logarithm of x in a given base
 */
Datum
numeric_log(PG_FUNCTION_ARGS)
{
	Numeric		num1 = PG_GETARG_NUMERIC(0);
	Numeric		num2 = PG_GETARG_NUMERIC(1);
	Numeric		res;
	NumericVar	arg1;
	NumericVar	arg2;
	NumericVar	result;

	/*
	 * Handle NaN and infinities
	 */
	if (NUMERIC_IS_SPECIAL(num1) || NUMERIC_IS_SPECIAL(num2))
	{
		int			sign1,
					sign2;

		if (NUMERIC_IS_NAN(num1) || NUMERIC_IS_NAN(num2))
			PG_RETURN_NUMERIC(make_result(&const_nan));
		/* fail on negative inputs including -Inf, as log_var would */
		sign1 = numeric_sign_internal(num1);
		sign2 = numeric_sign_internal(num2);
		if (sign1 < 0 || sign2 < 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_ARGUMENT_FOR_LOG),
					 errmsg("cannot take logarithm of a negative number")));
		/* fail on zero inputs, as log_var would */
		if (sign1 == 0 || sign2 == 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_ARGUMENT_FOR_LOG),
					 errmsg("cannot take logarithm of zero")));
		if (NUMERIC_IS_PINF(num1))
		{
			/* log(Inf, Inf) reduces to Inf/Inf, so it's NaN */
			if (NUMERIC_IS_PINF(num2))
				PG_RETURN_NUMERIC(make_result(&const_nan));
			/* log(Inf, finite-positive) is zero (we don't throw underflow) */
			PG_RETURN_NUMERIC(make_result(&const_zero));
		}
		Assert(NUMERIC_IS_PINF(num2));
		/* log(finite-positive, Inf) is Inf */
		PG_RETURN_NUMERIC(make_result(&const_pinf));
	}

	/*
	 * Initialize things
	 */
	init_var_from_num(num1, &arg1);
	init_var_from_num(num2, &arg2);
	init_var(&result);

	/*
	 * Call log_var() to compute and return the result; note it handles scale
	 * selection itself.
	 */
	log_var(&arg1, &arg2, &result);

	res = make_result(&result);

	free_var(&result);

	PG_RETURN_NUMERIC(res);
}


/*
 * numeric_power() -
 *
 *	Raise x to the power of y
 */
Datum
numeric_power(PG_FUNCTION_ARGS)
{
	Numeric		num1 = PG_GETARG_NUMERIC(0);
	Numeric		num2 = PG_GETARG_NUMERIC(1);
	Numeric		res;
	NumericVar	arg1;
	NumericVar	arg2;
	NumericVar	result;
	int			sign1,
				sign2;

	/*
	 * Handle NaN and infinities
	 */
	if (NUMERIC_IS_SPECIAL(num1) || NUMERIC_IS_SPECIAL(num2))
	{
		/*
		 * We follow the POSIX spec for pow(3), which says that NaN ^ 0 = 1,
		 * and 1 ^ NaN = 1, while all other cases with NaN inputs yield NaN
		 * (with no error).
		 */
		if (NUMERIC_IS_NAN(num1))
		{
			if (!NUMERIC_IS_SPECIAL(num2))
			{
				init_var_from_num(num2, &arg2);
				if (cmp_var(&arg2, &const_zero) == 0)
					PG_RETURN_NUMERIC(make_result(&const_one));
			}
			PG_RETURN_NUMERIC(make_result(&const_nan));
		}
		if (NUMERIC_IS_NAN(num2))
		{
			if (!NUMERIC_IS_SPECIAL(num1))
			{
				init_var_from_num(num1, &arg1);
				if (cmp_var(&arg1, &const_one) == 0)
					PG_RETURN_NUMERIC(make_result(&const_one));
			}
			PG_RETURN_NUMERIC(make_result(&const_nan));
		}
		/* At least one input is infinite, but error rules still apply */
		sign1 = numeric_sign_internal(num1);
		sign2 = numeric_sign_internal(num2);
		if (sign1 == 0 && sign2 < 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION),
					 errmsg("zero raised to a negative power is undefined")));
		if (sign1 < 0 && !numeric_is_integral(num2))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION),
					 errmsg("a negative number raised to a non-integer power yields a complex result")));

		/*
		 * POSIX gives this series of rules for pow(3) with infinite inputs:
		 *
		 * For any value of y, if x is +1, 1.0 shall be returned.
		 */
		if (!NUMERIC_IS_SPECIAL(num1))
		{
			init_var_from_num(num1, &arg1);
			if (cmp_var(&arg1, &const_one) == 0)
				PG_RETURN_NUMERIC(make_result(&const_one));
		}

		/*
		 * For any value of x, if y is [-]0, 1.0 shall be returned.
		 */
		if (sign2 == 0)
			PG_RETURN_NUMERIC(make_result(&const_one));

		/*
		 * For any odd integer value of y > 0, if x is [-]0, [-]0 shall be
		 * returned.  For y > 0 and not an odd integer, if x is [-]0, +0 shall
		 * be returned.  (Since we don't deal in minus zero, we need not
		 * distinguish these two cases.)
		 */
		if (sign1 == 0 && sign2 > 0)
			PG_RETURN_NUMERIC(make_result(&const_zero));

		/*
		 * If x is -1, and y is [-]Inf, 1.0 shall be returned.
		 *
		 * For |x| < 1, if y is -Inf, +Inf shall be returned.
		 *
		 * For |x| > 1, if y is -Inf, +0 shall be returned.
		 *
		 * For |x| < 1, if y is +Inf, +0 shall be returned.
		 *
		 * For |x| > 1, if y is +Inf, +Inf shall be returned.
		 */
		if (NUMERIC_IS_INF(num2))
		{
			bool		abs_x_gt_one;

			if (NUMERIC_IS_SPECIAL(num1))
				abs_x_gt_one = true;	/* x is either Inf or -Inf */
			else
			{
				init_var_from_num(num1, &arg1);
				if (cmp_var(&arg1, &const_minus_one) == 0)
					PG_RETURN_NUMERIC(make_result(&const_one));
				arg1.sign = NUMERIC_POS;	/* now arg1 = abs(x) */
				abs_x_gt_one = (cmp_var(&arg1, &const_one) > 0);
			}
			if (abs_x_gt_one == (sign2 > 0))
				PG_RETURN_NUMERIC(make_result(&const_pinf));
			else
				PG_RETURN_NUMERIC(make_result(&const_zero));
		}

		/*
		 * For y < 0, if x is +Inf, +0 shall be returned.
		 *
		 * For y > 0, if x is +Inf, +Inf shall be returned.
		 */
		if (NUMERIC_IS_PINF(num1))
		{
			if (sign2 > 0)
				PG_RETURN_NUMERIC(make_result(&const_pinf));
			else
				PG_RETURN_NUMERIC(make_result(&const_zero));
		}

		Assert(NUMERIC_IS_NINF(num1));

		/*
		 * For y an odd integer < 0, if x is -Inf, -0 shall be returned.  For
		 * y < 0 and not an odd integer, if x is -Inf, +0 shall be returned.
		 * (Again, we need not distinguish these two cases.)
		 */
		if (sign2 < 0)
			PG_RETURN_NUMERIC(make_result(&const_zero));

		/*
		 * For y an odd integer > 0, if x is -Inf, -Inf shall be returned. For
		 * y > 0 and not an odd integer, if x is -Inf, +Inf shall be returned.
		 */
		init_var_from_num(num2, &arg2);
		if (arg2.ndigits > 0 && arg2.ndigits == arg2.weight + 1 &&
			(arg2.digits[arg2.ndigits - 1] & 1))
			PG_RETURN_NUMERIC(make_result(&const_ninf));
		else
			PG_RETURN_NUMERIC(make_result(&const_pinf));
	}

	/*
	 * The SQL spec requires that we emit a particular SQLSTATE error code for
	 * certain error conditions.  Specifically, we don't return a
	 * divide-by-zero error code for 0 ^ -1.  Raising a negative number to a
	 * non-integer power must produce the same error code, but that case is
	 * handled in power_var().
	 */
	sign1 = numeric_sign_internal(num1);
	sign2 = numeric_sign_internal(num2);

	if (sign1 == 0 && sign2 < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION),
				 errmsg("zero raised to a negative power is undefined")));

	/*
	 * Initialize things
	 */
	init_var(&result);
	init_var_from_num(num1, &arg1);
	init_var_from_num(num2, &arg2);

	/*
	 * Call power_var() to compute and return the result; note it handles
	 * scale selection itself.
	 */
	power_var(&arg1, &arg2, &result);

	res = make_result(&result);

	free_var(&result);

	PG_RETURN_NUMERIC(res);
}

/*
 * numeric_scale() -
 *
 *	Returns the scale, i.e. the count of decimal digits in the fractional part
 */
Datum
numeric_scale(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);

	if (NUMERIC_IS_SPECIAL(num))
		PG_RETURN_NULL();

	PG_RETURN_INT32(NUMERIC_DSCALE(num));
}

/*
 * Calculate minimum scale for value.
 */
static int
get_min_scale(NumericVar *var)
{
	int			min_scale;
	int			last_digit_pos;

	/*
	 * Ordinarily, the input value will be "stripped" so that the last
	 * NumericDigit is nonzero.  But we don't want to get into an infinite
	 * loop if it isn't, so explicitly find the last nonzero digit.
	 */
	last_digit_pos = var->ndigits - 1;
	while (last_digit_pos >= 0 &&
		   var->digits[last_digit_pos] == 0)
		last_digit_pos--;

	if (last_digit_pos >= 0)
	{
		/* compute min_scale assuming that last ndigit has no zeroes */
		min_scale = (last_digit_pos - var->weight) * DEC_DIGITS;

		/*
		 * We could get a negative result if there are no digits after the
		 * decimal point.  In this case the min_scale must be zero.
		 */
		if (min_scale > 0)
		{
			/*
			 * Reduce min_scale if trailing digit(s) in last NumericDigit are
			 * zero.
			 */
			NumericDigit last_digit = var->digits[last_digit_pos];

			while (last_digit % 10 == 0)
			{
				min_scale--;
				last_digit /= 10;
			}
		}
		else
			min_scale = 0;
	}
	else
		min_scale = 0;			/* result if input is zero */

	return min_scale;
}

/*
 * Returns minimum scale required to represent supplied value without loss.
 */
Datum
numeric_min_scale(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);
	NumericVar	arg;
	int			min_scale;

	if (NUMERIC_IS_SPECIAL(num))
		PG_RETURN_NULL();

	init_var_from_num(num, &arg);
	min_scale = get_min_scale(&arg);
	free_var(&arg);

	PG_RETURN_INT32(min_scale);
}

/*
 * Reduce scale of numeric value to represent supplied value without loss.
 */
Datum
numeric_trim_scale(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);
	Numeric		res;
	NumericVar	result;

	if (NUMERIC_IS_SPECIAL(num))
		PG_RETURN_NUMERIC(duplicate_numeric(num));

	init_var_from_num(num, &result);
	result.dscale = get_min_scale(&result);
	res = make_result(&result);
	free_var(&result);

	PG_RETURN_NUMERIC(res);
}

/*
 * Return a random numeric value in the range [rmin, rmax].
 */
Numeric
random_numeric(pg_prng_state *state, Numeric rmin, Numeric rmax)
{
	NumericVar	rmin_var;
	NumericVar	rmax_var;
	NumericVar	result;
	Numeric		res;

	/* Range bounds must not be NaN/infinity */
	if (NUMERIC_IS_SPECIAL(rmin))
	{
		if (NUMERIC_IS_NAN(rmin))
			ereport(ERROR,
					errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("lower bound cannot be NaN"));
		else
			ereport(ERROR,
					errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("lower bound cannot be infinity"));
	}
	if (NUMERIC_IS_SPECIAL(rmax))
	{
		if (NUMERIC_IS_NAN(rmax))
			ereport(ERROR,
					errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("upper bound cannot be NaN"));
		else
			ereport(ERROR,
					errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("upper bound cannot be infinity"));
	}

	/* Return a random value in the range [rmin, rmax] */
	init_var_from_num(rmin, &rmin_var);
	init_var_from_num(rmax, &rmax_var);

	init_var(&result);

	random_var(state, &rmin_var, &rmax_var, &result);

	res = make_result(&result);

	free_var(&result);

	return res;
}


/* ----------------------------------------------------------------------
 *
 * Type conversion functions
 *
 * ----------------------------------------------------------------------
 */

Numeric
int64_to_numeric(int64 val)
{
	Numeric		res;
	NumericVar	result;

	init_var(&result);

	int64_to_numericvar(val, &result);

	res = make_result(&result);

	free_var(&result);

	return res;
}

/*
 * Convert val1/(10**log10val2) to numeric.  This is much faster than normal
 * numeric division.
 */
Numeric
int64_div_fast_to_numeric(int64 val1, int log10val2)
{
	Numeric		res;
	NumericVar	result;
	int			rscale;
	int			w;
	int			m;

	init_var(&result);

	/* result scale */
	rscale = log10val2 < 0 ? 0 : log10val2;

	/* how much to decrease the weight by */
	w = log10val2 / DEC_DIGITS;
	/* how much is left to divide by */
	m = log10val2 % DEC_DIGITS;
	if (m < 0)
	{
		m += DEC_DIGITS;
		w--;
	}

	/*
	 * If there is anything left to divide by (10^m with 0 < m < DEC_DIGITS),
	 * multiply the dividend by 10^(DEC_DIGITS - m), and shift the weight by
	 * one more.
	 */
	if (m > 0)
	{
#if DEC_DIGITS == 4
		static const int pow10[] = {1, 10, 100, 1000};
#elif DEC_DIGITS == 2
		static const int pow10[] = {1, 10};
#elif DEC_DIGITS == 1
		static const int pow10[] = {1};
#else
#error unsupported NBASE
#endif
		int64		factor = pow10[DEC_DIGITS - m];
		int64		new_val1;

		StaticAssertDecl(lengthof(pow10) == DEC_DIGITS, "mismatch with DEC_DIGITS");

		if (unlikely(pg_mul_s64_overflow(val1, factor, &new_val1)))
		{
#ifdef HAVE_INT128
			/* do the multiplication using 128-bit integers */
			int128		tmp;

			tmp = (int128) val1 * (int128) factor;

			int128_to_numericvar(tmp, &result);
#else
			/* do the multiplication using numerics */
			NumericVar	tmp;

			init_var(&tmp);

			int64_to_numericvar(val1, &result);
			int64_to_numericvar(factor, &tmp);
			mul_var(&result, &tmp, &result, 0);

			free_var(&tmp);
#endif
		}
		else
			int64_to_numericvar(new_val1, &result);

		w++;
	}
	else
		int64_to_numericvar(val1, &result);

	result.weight -= w;
	result.dscale = rscale;

	res = make_result(&result);

	free_var(&result);

	return res;
}

Datum
int4_numeric(PG_FUNCTION_ARGS)
{
	int32		val = PG_GETARG_INT32(0);

	PG_RETURN_NUMERIC(int64_to_numeric(val));
}

int32
numeric_int4_opt_error(Numeric num, bool *have_error)
{
	NumericVar	x;
	int32		result;

	if (have_error)
		*have_error = false;

	if (NUMERIC_IS_SPECIAL(num))
	{
		if (have_error)
		{
			*have_error = true;
			return 0;
		}
		else
		{
			if (NUMERIC_IS_NAN(num))
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot convert NaN to %s", "integer")));
			else
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot convert infinity to %s", "integer")));
		}
	}

	/* Convert to variable format, then convert to int4 */
	init_var_from_num(num, &x);

	if (!numericvar_to_int32(&x, &result))
	{
		if (have_error)
		{
			*have_error = true;
			return 0;
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("integer out of range")));
		}
	}

	return result;
}

Datum
numeric_int4(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);

	PG_RETURN_INT32(numeric_int4_opt_error(num, NULL));
}

/*
 * Given a NumericVar, convert it to an int32. If the NumericVar
 * exceeds the range of an int32, false is returned, otherwise true is returned.
 * The input NumericVar is *not* free'd.
 */
static bool
numericvar_to_int32(const NumericVar *var, int32 *result)
{
	int64		val;

	if (!numericvar_to_int64(var, &val))
		return false;

	if (unlikely(val < PG_INT32_MIN) || unlikely(val > PG_INT32_MAX))
		return false;

	/* Down-convert to int4 */
	*result = (int32) val;

	return true;
}

Datum
int8_numeric(PG_FUNCTION_ARGS)
{
	int64		val = PG_GETARG_INT64(0);

	PG_RETURN_NUMERIC(int64_to_numeric(val));
}

int64
numeric_int8_opt_error(Numeric num, bool *have_error)
{
	NumericVar	x;
	int64		result;

	if (have_error)
		*have_error = false;

	if (NUMERIC_IS_SPECIAL(num))
	{
		if (have_error)
		{
			*have_error = true;
			return 0;
		}
		else
		{
			if (NUMERIC_IS_NAN(num))
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot convert NaN to %s", "bigint")));
			else
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot convert infinity to %s", "bigint")));
		}
	}

	/* Convert to variable format, then convert to int8 */
	init_var_from_num(num, &x);

	if (!numericvar_to_int64(&x, &result))
	{
		if (have_error)
		{
			*have_error = true;
			return 0;
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("bigint out of range")));
		}
	}

	return result;
}

Datum
numeric_int8(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);

	PG_RETURN_INT64(numeric_int8_opt_error(num, NULL));
}


Datum
int2_numeric(PG_FUNCTION_ARGS)
{
	int16		val = PG_GETARG_INT16(0);

	PG_RETURN_NUMERIC(int64_to_numeric(val));
}


Datum
numeric_int2(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);
	NumericVar	x;
	int64		val;
	int16		result;

	if (NUMERIC_IS_SPECIAL(num))
	{
		if (NUMERIC_IS_NAN(num))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot convert NaN to %s", "smallint")));
		else
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot convert infinity to %s", "smallint")));
	}

	/* Convert to variable format and thence to int8 */
	init_var_from_num(num, &x);

	if (!numericvar_to_int64(&x, &val))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("smallint out of range")));

	if (unlikely(val < PG_INT16_MIN) || unlikely(val > PG_INT16_MAX))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("smallint out of range")));

	/* Down-convert to int2 */
	result = (int16) val;

	PG_RETURN_INT16(result);
}


Datum
float8_numeric(PG_FUNCTION_ARGS)
{
	float8		val = PG_GETARG_FLOAT8(0);
	Numeric		res;
	NumericVar	result;
	char		buf[DBL_DIG + 100];
	const char *endptr;

	if (isnan(val))
		PG_RETURN_NUMERIC(make_result(&const_nan));

	if (isinf(val))
	{
		if (val < 0)
			PG_RETURN_NUMERIC(make_result(&const_ninf));
		else
			PG_RETURN_NUMERIC(make_result(&const_pinf));
	}

	snprintf(buf, sizeof(buf), "%.*g", DBL_DIG, val);

	init_var(&result);

	/* Assume we need not worry about leading/trailing spaces */
	(void) set_var_from_str(buf, buf, &result, &endptr, NULL);

	res = make_result(&result);

	free_var(&result);

	PG_RETURN_NUMERIC(res);
}


Datum
numeric_float8(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);
	char	   *tmp;
	Datum		result;

	if (NUMERIC_IS_SPECIAL(num))
	{
		if (NUMERIC_IS_PINF(num))
			PG_RETURN_FLOAT8(get_float8_infinity());
		else if (NUMERIC_IS_NINF(num))
			PG_RETURN_FLOAT8(-get_float8_infinity());
		else
			PG_RETURN_FLOAT8(get_float8_nan());
	}

	tmp = DatumGetCString(DirectFunctionCall1(numeric_out,
											  NumericGetDatum(num)));

	result = DirectFunctionCall1(float8in, CStringGetDatum(tmp));

	pfree(tmp);

	PG_RETURN_DATUM(result);
}


/*
 * Convert numeric to float8; if out of range, return +/- HUGE_VAL
 *
 * (internal helper function, not directly callable from SQL)
 */
Datum
numeric_float8_no_overflow(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);
	double		val;

	if (NUMERIC_IS_SPECIAL(num))
	{
		if (NUMERIC_IS_PINF(num))
			val = HUGE_VAL;
		else if (NUMERIC_IS_NINF(num))
			val = -HUGE_VAL;
		else
			val = get_float8_nan();
	}
	else
	{
		NumericVar	x;

		init_var_from_num(num, &x);
		val = numericvar_to_double_no_overflow(&x);
	}

	PG_RETURN_FLOAT8(val);
}

Datum
float4_numeric(PG_FUNCTION_ARGS)
{
	float4		val = PG_GETARG_FLOAT4(0);
	Numeric		res;
	NumericVar	result;
	char		buf[FLT_DIG + 100];
	const char *endptr;

	if (isnan(val))
		PG_RETURN_NUMERIC(make_result(&const_nan));

	if (isinf(val))
	{
		if (val < 0)
			PG_RETURN_NUMERIC(make_result(&const_ninf));
		else
			PG_RETURN_NUMERIC(make_result(&const_pinf));
	}

	snprintf(buf, sizeof(buf), "%.*g", FLT_DIG, val);

	init_var(&result);

	/* Assume we need not worry about leading/trailing spaces */
	(void) set_var_from_str(buf, buf, &result, &endptr, NULL);

	res = make_result(&result);

	free_var(&result);

	PG_RETURN_NUMERIC(res);
}


Datum
numeric_float4(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);
	char	   *tmp;
	Datum		result;

	if (NUMERIC_IS_SPECIAL(num))
	{
		if (NUMERIC_IS_PINF(num))
			PG_RETURN_FLOAT4(get_float4_infinity());
		else if (NUMERIC_IS_NINF(num))
			PG_RETURN_FLOAT4(-get_float4_infinity());
		else
			PG_RETURN_FLOAT4(get_float4_nan());
	}

	tmp = DatumGetCString(DirectFunctionCall1(numeric_out,
											  NumericGetDatum(num)));

	result = DirectFunctionCall1(float4in, CStringGetDatum(tmp));

	pfree(tmp);

	PG_RETURN_DATUM(result);
}


Datum
numeric_pg_lsn(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);
	NumericVar	x;
	XLogRecPtr	result;

	if (NUMERIC_IS_SPECIAL(num))
	{
		if (NUMERIC_IS_NAN(num))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot convert NaN to %s", "pg_lsn")));
		else
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot convert infinity to %s", "pg_lsn")));
	}

	/* Convert to variable format and thence to pg_lsn */
	init_var_from_num(num, &x);

	if (!numericvar_to_uint64(&x, (uint64 *) &result))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("pg_lsn out of range")));

	PG_RETURN_LSN(result);
}


/* ----------------------------------------------------------------------
 *
 * Aggregate functions
 *
 * The transition datatype for all these aggregates is declared as INTERNAL.
 * Actually, it's a pointer to a NumericAggState allocated in the aggregate
 * context.  The digit buffers for the NumericVars will be there too.
 *
 * On platforms which support 128-bit integers some aggregates instead use a
 * 128-bit integer based transition datatype to speed up calculations.
 *
 * ----------------------------------------------------------------------
 */

typedef struct NumericAggState
{
	bool		calcSumX2;		/* if true, calculate sumX2 */

	/*
	 * EXPERIMENTAL (SPEC-numeric-as-dec128.md): int128 fast sum, adapted from
	 * 0003-Use-an-int128-fast-sum-in-sum-numeric-and-avg-numeri.patch.
	 *
	 * While fastSum the running sum lives in sumX128 at decimal scale
	 * fastScale; once promoted (fastSum cleared) it lives in the sumX
	 * digit-array accumulator.  Promotion is one-way and exact, so results
	 * never depend on whether or when it happened.
	 *
	 * The dec128 representation makes this much cheaper than it is upstream: a
	 * stored numeric *is* an int128 coefficient plus a scale, so accumulating
	 * one needs no NumericVar and no digit array at all -- which is the whole
	 * point, because init_var_from_num() in do_numeric_accum() is precisely
	 * what made sum() and avg() 15% slower than stock in the Phase 4 run.
	 *
	 * The flag is deliberately spelled positively, so that *zero* means the
	 * digit-array representation.  Several code paths build a NumericAggState
	 * by palloc0() or memset() and then populate sumX/sumX2 by hand --
	 * numeric_deserialize(), numeric_avg_deserialize() and
	 * numeric_poly_stddev_internal() all do -- and with the sense reversed
	 * every one of those silently claimed to hold an int128 sum it had never
	 * filled in, which reads back as zero rather than failing.  Defaulting to
	 * the safe representation makes that class of mistake unreachable instead
	 * of merely fixed.
	 *
	 * States that must never use the fast form at all (calcSumX2 ones -- a sum
	 * of squares does not fit int128 for realistic input) are created with
	 * fastSum = false, so the digit-array path stays the single unconditional
	 * fallback for every consumer.
	 */
	bool		fastSum;
	int128		sumX128;		/* running sum while fastSum */
	int			fastScale;		/* decimal scale of sumX128 */

	MemoryContext agg_context;	/* context we're calculating in */
	int64		N;				/* count of processed numbers */
	NumericSumAccum sumX;		/* sum of processed numbers */
	NumericSumAccum sumX2;		/* sum of squares of processed numbers */
	int			maxScale;		/* maximum scale seen so far */
	int64		maxScaleCount;	/* number of values seen with maximum scale */
	/* These counts are *not* included in N!  Use NA_TOTAL_COUNT() as needed */
	int64		NaNcount;		/* count of NaN values */
	int64		pInfcount;		/* count of +Inf values */
	int64		nInfcount;		/* count of -Inf values */
} NumericAggState;

#define NA_TOTAL_COUNT(na) \
	((na)->N + (na)->NaNcount + (na)->pInfcount + (na)->nInfcount)

/*
 * Prepare state data for a numeric aggregate function that needs to compute
 * sum, count and optionally sum of squares of the input.
 */
static NumericAggState *
makeNumericAggState(FunctionCallInfo fcinfo, bool calcSumX2)
{
	NumericAggState *state;
	MemoryContext agg_context;
	MemoryContext old_context;

	if (!AggCheckCallContext(fcinfo, &agg_context))
		elog(ERROR, "aggregate function called in non-aggregate context");

	old_context = MemoryContextSwitchTo(agg_context);

	state = (NumericAggState *) palloc0(sizeof(NumericAggState));
	state->calcSumX2 = calcSumX2;
	/* sumX2 users never enter the fast representation; see struct comment */
	state->fastSum = !calcSumX2;
	state->agg_context = agg_context;

	MemoryContextSwitchTo(old_context);

	return state;
}

/*
 * Like makeNumericAggState(), but allocate the state in the current memory
 * context.
 */
static NumericAggState *
makeNumericAggStateCurrentContext(bool calcSumX2)
{
	NumericAggState *state;

	state = (NumericAggState *) palloc0(sizeof(NumericAggState));
	state->calcSumX2 = calcSumX2;
	/* same rule as makeNumericAggState() */
	state->fastSum = !calcSumX2;
	state->agg_context = CurrentMemoryContext;

	return state;
}

/*
 * Convert the int128 fast sum into the digit-array accumulator and switch the
 * state permanently to promoted mode.
 *
 * Exact, so the aggregate result never depends on whether or when this ran.
 * Only the sum moves: an input that failed to accumulate the fast way must
 * still be processed by the digit-array path afterwards, including its N++.
 */
static void
promote_numeric_agg_state(NumericAggState *state)
{
	NumericVar	tmp;
	MemoryContext old_context;

	Assert(state->fastSum);
	Assert(!state->calcSumX2);

	/* build the temporary var in the short-lived caller context ... */
	init_var(&tmp);
	numeric_dec128_int128_to_var(state->sumX128, state->fastScale, &tmp);

	/* ... but the accumulator's own buffers must live in the agg context */
	old_context = MemoryContextSwitchTo(state->agg_context);
	accum_sum_add(&state->sumX, &tmp);
	MemoryContextSwitchTo(old_context);

	free_var(&tmp);

	state->fastSum = false;
#ifdef USE_ASSERT_CHECKING
	/* poison the fast fields; nothing may read them past this point */
	state->sumX128 = 0;
	state->fastScale = -1;
#endif
}

/*
 * Try to add one packed finite value into the int128 fast sum.
 *
 * Returns false if it doesn't fit, in which case the state still correctly
 * represents everything accumulated so far (rescaling an exact sum is exact),
 * and the caller promotes and reprocesses this input on the digit-array path.
 *
 * Note there is no NumericVar anywhere in here: mant and scale come straight
 * out of the packed datum.  That is the entire reason this is faster than the
 * upstream version of the same idea.
 */
static inline bool
numeric_dec128_fast_add(NumericAggState *state, int128 mant, int scale,
						bool subtract)
{
	int128		c = subtract ? -mant : mant;
	int128		sum;

	Assert(state->fastSum);
	Assert(!state->calcSumX2);
	Assert(scale >= 0 && scale <= NUMERIC_MAX_STORED_SCALE);

	if (scale > state->fastScale)
	{
		/*
		 * Widen the accumulator to the new input's scale, maintaining the
		 * invariant that fastScale is the widest scale accumulated so far --
		 * which is what makes the finalised display scale agree with the
		 * digit-array path.
		 */
		if (unlikely(!numeric_dec128_try_scale_up(state->sumX128,
												  scale - state->fastScale,
												  &state->sumX128)))
			return false;
		state->fastScale = scale;
	}
	else if (scale < state->fastScale)
	{
		/* bring the input up to the accumulator's scale */
		if (unlikely(!numeric_dec128_try_scale_up(c, state->fastScale - scale,
												  &c)))
			return false;
	}

	if (unlikely(__builtin_add_overflow(state->sumX128, c, &sum)))
		return false;

	state->sumX128 = sum;
	return true;
}

/*
 * Accumulate a new input value for numeric aggregate functions.
 */
static void
do_numeric_accum(NumericAggState *state, Numeric newval)
{
	NumericVar	X;
	NumericVar	X2;
	MemoryContext old_context;
	int			newdscale;

	/* Count NaN/infinity inputs separately from all else */
	if (NUMERIC_IS_SPECIAL(newval))
	{
		if (NUMERIC_IS_PINF(newval))
			state->pInfcount++;
		else if (NUMERIC_IS_NINF(newval))
			state->nInfcount++;
		else
			state->NaNcount++;
		return;
	}

	/*
	 * The display scale is readable straight from the packed datum, so the
	 * bookkeeping below no longer needs the value unpacked.
	 */
	newdscale = numeric_pack_scale(newval);

	/*
	 * Track the highest input dscale that we've seen, to support inverse
	 * transitions (see do_numeric_discard).
	 */
	if (newdscale > state->maxScale)
	{
		state->maxScale = newdscale;
		state->maxScaleCount = 1;
	}
	else if (newdscale == state->maxScale)
		state->maxScaleCount++;

	/*
	 * Fast path: accumulate directly into the int128 sum -- no unpack, no
	 * allocation, no memory context switch.  On the first input that doesn't
	 * fit, promote the accumulated sum exactly and fall through so this input
	 * takes the regular path (including its N++).
	 */
	if (state->fastSum)
	{
		if (likely(numeric_dec128_fast_add(state, numeric_pack_mant(newval),
										   newdscale, false)))
		{
			state->N++;
			return;
		}
		promote_numeric_agg_state(state);
	}

	/* load processed number in short-lived context */
	init_var_from_num(newval, &X);

	/* if we need X^2, calculate that in short-lived context */
	if (state->calcSumX2)
	{
		init_var(&X2);
		mul_var(&X, &X, &X2, X.dscale * 2);
	}

	/* The rest of this needs to work in the aggregate context */
	old_context = MemoryContextSwitchTo(state->agg_context);

	state->N++;

	/* Accumulate sums */
	accum_sum_add(&(state->sumX), &X);

	if (state->calcSumX2)
		accum_sum_add(&(state->sumX2), &X2);

	MemoryContextSwitchTo(old_context);
}

/*
 * Attempt to remove an input value from the aggregated state.
 *
 * If the value cannot be removed then the function will return false; the
 * possible reasons for failing are described below.
 *
 * If we aggregate the values 1.01 and 2 then the result will be 3.01.
 * If we are then asked to un-aggregate the 1.01 then we must fail as we
 * won't be able to tell what the new aggregated value's dscale should be.
 * We don't want to return 2.00 (dscale = 2), since the sum's dscale would
 * have been zero if we'd really aggregated only 2.
 *
 * Note: alternatively, we could count the number of inputs with each possible
 * dscale (up to some sane limit).  Not yet clear if it's worth the trouble.
 */
static bool
do_numeric_discard(NumericAggState *state, Numeric newval)
{
	NumericVar	X;
	NumericVar	X2;
	MemoryContext old_context;

	/* Count NaN/infinity inputs separately from all else */
	if (NUMERIC_IS_SPECIAL(newval))
	{
		if (NUMERIC_IS_PINF(newval))
			state->pInfcount--;
		else if (NUMERIC_IS_NINF(newval))
			state->nInfcount--;
		else
			state->NaNcount--;
		return true;
	}

	/*
	 * state->sumX's dscale is the maximum dscale of any of the inputs.
	 * Removing the last input with that dscale would require us to recompute
	 * the maximum dscale of the *remaining* inputs, which we cannot do unless
	 * no more non-NaN inputs remain at all.  So we report a failure instead,
	 * and force the aggregation to be redone from scratch.
	 *
	 * The dscale test reads the packed datum directly, so this can decide to
	 * fail before doing any unpacking work.
	 */
	if (numeric_pack_scale(newval) == state->maxScale)
	{
		if (state->maxScaleCount > 1 || state->maxScale == 0)
		{
			/*
			 * Some remaining inputs have same dscale, or dscale hasn't gotten
			 * above zero anyway
			 */
			state->maxScaleCount--;
		}
		else if (state->N == 1)
		{
			/* No remaining non-NaN inputs at all, so reset maxScale */
			state->maxScale = 0;
			state->maxScaleCount = 0;
		}
		else
		{
			/* Correct new maxScale is uncertain, must fail */
			return false;
		}
	}

	/*
	 * Fast-representation inverse.  A subtraction can overflow even when both
	 * operands fit (opposite signs near the range limits), so it is checked
	 * like the forward case; any failure promotes and falls through.
	 */
	if (state->fastSum)
	{
		if (state->N == 1)
		{
			/*
			 * Removing the last input: mirror the accum_sum_reset() in the
			 * digit-array branch below, which zeroes the accumulator's scale
			 * too.  The maxScale bookkeeping above is shared and already done.
			 */
			state->sumX128 = 0;
			state->fastScale = 0;
			state->N = 0;
			return true;
		}

		if (likely(numeric_dec128_fast_add(state, numeric_pack_mant(newval),
										   numeric_pack_scale(newval), true)))
		{
			state->N--;
			return true;
		}
		promote_numeric_agg_state(state);
	}

	/* load processed number in short-lived context */
	init_var_from_num(newval, &X);

	/* if we need X^2, calculate that in short-lived context */
	if (state->calcSumX2)
	{
		init_var(&X2);
		mul_var(&X, &X, &X2, X.dscale * 2);
	}

	/* The rest of this needs to work in the aggregate context */
	old_context = MemoryContextSwitchTo(state->agg_context);

	if (state->N-- > 1)
	{
		/* Negate X, to subtract it from the sum */
		X.sign = (X.sign == NUMERIC_POS ? NUMERIC_NEG : NUMERIC_POS);
		accum_sum_add(&(state->sumX), &X);

		if (state->calcSumX2)
		{
			/* Negate X^2. X^2 is always positive */
			X2.sign = NUMERIC_NEG;
			accum_sum_add(&(state->sumX2), &X2);
		}
	}
	else
	{
		/* Zero the sums */
		Assert(state->N == 0);

		accum_sum_reset(&state->sumX);
		if (state->calcSumX2)
			accum_sum_reset(&state->sumX2);
	}

	MemoryContextSwitchTo(old_context);

	return true;
}

/*
 * Generic transition function for numeric aggregates that require sumX2.
 */
Datum
numeric_accum(PG_FUNCTION_ARGS)
{
	NumericAggState *state;

	state = PG_ARGISNULL(0) ? NULL : (NumericAggState *) PG_GETARG_POINTER(0);

	/* Create the state data on the first call */
	if (state == NULL)
		state = makeNumericAggState(fcinfo, true);

	if (!PG_ARGISNULL(1))
		do_numeric_accum(state, PG_GETARG_NUMERIC(1));

	PG_RETURN_POINTER(state);
}

/*
 * Generic combine function for numeric aggregates which require sumX2
 */
Datum
numeric_combine(PG_FUNCTION_ARGS)
{
	NumericAggState *state1;
	NumericAggState *state2;
	MemoryContext agg_context;
	MemoryContext old_context;

	if (!AggCheckCallContext(fcinfo, &agg_context))
		elog(ERROR, "aggregate function called in non-aggregate context");

	state1 = PG_ARGISNULL(0) ? NULL : (NumericAggState *) PG_GETARG_POINTER(0);
	state2 = PG_ARGISNULL(1) ? NULL : (NumericAggState *) PG_GETARG_POINTER(1);

	if (state2 == NULL)
		PG_RETURN_POINTER(state1);

	/* manually copy all fields from state2 to state1 */
	if (state1 == NULL)
	{
		old_context = MemoryContextSwitchTo(agg_context);

		state1 = makeNumericAggStateCurrentContext(true);
		state1->N = state2->N;
		state1->NaNcount = state2->NaNcount;
		state1->pInfcount = state2->pInfcount;
		state1->nInfcount = state2->nInfcount;
		state1->maxScale = state2->maxScale;
		state1->maxScaleCount = state2->maxScaleCount;

		accum_sum_copy(&state1->sumX, &state2->sumX);
		accum_sum_copy(&state1->sumX2, &state2->sumX2);

		MemoryContextSwitchTo(old_context);

		PG_RETURN_POINTER(state1);
	}

	state1->N += state2->N;
	state1->NaNcount += state2->NaNcount;
	state1->pInfcount += state2->pInfcount;
	state1->nInfcount += state2->nInfcount;

	if (state2->N > 0)
	{
		/*
		 * These are currently only needed for moving aggregates, but let's do
		 * the right thing anyway...
		 */
		if (state2->maxScale > state1->maxScale)
		{
			state1->maxScale = state2->maxScale;
			state1->maxScaleCount = state2->maxScaleCount;
		}
		else if (state2->maxScale == state1->maxScale)
			state1->maxScaleCount += state2->maxScaleCount;

		/* The rest of this needs to work in the aggregate context */
		old_context = MemoryContextSwitchTo(agg_context);

		/* Accumulate sums */
		accum_sum_combine(&state1->sumX, &state2->sumX);
		accum_sum_combine(&state1->sumX2, &state2->sumX2);

		MemoryContextSwitchTo(old_context);
	}
	PG_RETURN_POINTER(state1);
}

/*
 * Generic transition function for numeric aggregates that don't require sumX2.
 */
Datum
numeric_avg_accum(PG_FUNCTION_ARGS)
{
	NumericAggState *state;

	state = PG_ARGISNULL(0) ? NULL : (NumericAggState *) PG_GETARG_POINTER(0);

	/* Create the state data on the first call */
	if (state == NULL)
		state = makeNumericAggState(fcinfo, false);

	if (!PG_ARGISNULL(1))
		do_numeric_accum(state, PG_GETARG_NUMERIC(1));

	PG_RETURN_POINTER(state);
}

/*
 * Combine function for numeric aggregates which don't require sumX2
 */
Datum
numeric_avg_combine(PG_FUNCTION_ARGS)
{
	NumericAggState *state1;
	NumericAggState *state2;
	MemoryContext agg_context;
	MemoryContext old_context;

	if (!AggCheckCallContext(fcinfo, &agg_context))
		elog(ERROR, "aggregate function called in non-aggregate context");

	state1 = PG_ARGISNULL(0) ? NULL : (NumericAggState *) PG_GETARG_POINTER(0);
	state2 = PG_ARGISNULL(1) ? NULL : (NumericAggState *) PG_GETARG_POINTER(1);

	if (state2 == NULL)
		PG_RETURN_POINTER(state1);

	/* manually copy all fields from state2 to state1 */
	if (state1 == NULL)
	{
		old_context = MemoryContextSwitchTo(agg_context);

		state1 = makeNumericAggStateCurrentContext(false);
		state1->N = state2->N;
		state1->NaNcount = state2->NaNcount;
		state1->pInfcount = state2->pInfcount;
		state1->nInfcount = state2->nInfcount;
		state1->maxScale = state2->maxScale;
		state1->maxScaleCount = state2->maxScaleCount;

		/* clone state2's representation, whichever mode it is in */
		state1->fastSum = state2->fastSum;
		if (!state2->fastSum)
			accum_sum_copy(&state1->sumX, &state2->sumX);
		else
		{
			state1->sumX128 = state2->sumX128;
			state1->fastScale = state2->fastScale;
		}

		MemoryContextSwitchTo(old_context);

		PG_RETURN_POINTER(state1);
	}

	state1->N += state2->N;
	state1->NaNcount += state2->NaNcount;
	state1->pInfcount += state2->pInfcount;
	state1->nInfcount += state2->nInfcount;

	if (state2->N > 0)
	{
		/*
		 * These are currently only needed for moving aggregates, but let's do
		 * the right thing anyway...
		 */
		if (state2->maxScale > state1->maxScale)
		{
			state1->maxScale = state2->maxScale;
			state1->maxScaleCount = state2->maxScaleCount;
		}
		else if (state2->maxScale == state1->maxScale)
			state1->maxScaleCount += state2->maxScaleCount;

		/*
		 * If both partial sums are still in the fast representation, merge
		 * them there.  Rescaling an exact sum is exact, so a later overflow
		 * leaves state1 still correct and we simply demote below.
		 */
		if (state1->fastSum && state2->fastSum)
		{
			int128		saved = state1->sumX128;
			int			savedScale = state1->fastScale;

			if (numeric_dec128_fast_add(state1, state2->sumX128,
										state2->fastScale, false))
				PG_RETURN_POINTER(state1);

			/* restore, since a partial rescale may have modified state1 */
			state1->sumX128 = saved;
			state1->fastScale = savedScale;
		}

		/* At least one side needs the digit-array path: promote state1 */
		if (state1->fastSum)
			promote_numeric_agg_state(state1);

		if (!state2->fastSum)
		{
			/* The rest of this needs to work in the aggregate context */
			old_context = MemoryContextSwitchTo(agg_context);

			/* Accumulate sums */
			accum_sum_combine(&state1->sumX, &state2->sumX);

			MemoryContextSwitchTo(old_context);
		}
		else
		{
			/* materialise state2's int128 sum and add it in */
			NumericVar	tmp;

			init_var(&tmp);
			numeric_dec128_int128_to_var(state2->sumX128, state2->fastScale,
										 &tmp);

			old_context = MemoryContextSwitchTo(agg_context);
			accum_sum_add(&state1->sumX, &tmp);
			MemoryContextSwitchTo(old_context);

			free_var(&tmp);
		}
	}
	PG_RETURN_POINTER(state1);
}

/*
 * numeric_avg_serialize
 *		Serialize NumericAggState for numeric aggregates that don't require
 *		sumX2.
 */
Datum
numeric_avg_serialize(PG_FUNCTION_ARGS)
{
	NumericAggState *state;
	StringInfoData buf;
	bytea	   *result;
	NumericVar	tmp_var;

	/* Ensure we disallow calling when not in aggregate context */
	if (!AggCheckCallContext(fcinfo, NULL))
		elog(ERROR, "aggregate function called in non-aggregate context");

	state = (NumericAggState *) PG_GETARG_POINTER(0);

	/*
	 * Materialise the fast sum before serialising, so the wire format stays
	 * byte-for-byte what it always was and numeric_avg_deserialize() needs no
	 * changes at all.  Upstream's version of this patch instead adds a mode
	 * byte and ships the int128 directly, because folding here costs a carry
	 * propagation and a digit repack.  We accept that cost: it is paid once per
	 * group per worker rather than per row, and it keeps the parallel protocol
	 * out of the diff.  The consequence is that the fast representation's
	 * benefit stops at the worker boundary -- worth revisiting if parallel
	 * aggregation turns out to matter for the decision.
	 */
	if (state->fastSum)
		promote_numeric_agg_state(state);

	init_var(&tmp_var);

	pq_begintypsend(&buf);

	/* N */
	pq_sendint64(&buf, state->N);

	/* sumX */
	accum_sum_final(&state->sumX, &tmp_var);
	numericvar_serialize(&buf, &tmp_var);

	/* maxScale */
	pq_sendint32(&buf, state->maxScale);

	/* maxScaleCount */
	pq_sendint64(&buf, state->maxScaleCount);

	/* NaNcount */
	pq_sendint64(&buf, state->NaNcount);

	/* pInfcount */
	pq_sendint64(&buf, state->pInfcount);

	/* nInfcount */
	pq_sendint64(&buf, state->nInfcount);

	result = pq_endtypsend(&buf);

	free_var(&tmp_var);

	PG_RETURN_BYTEA_P(result);
}

/*
 * numeric_avg_deserialize
 *		Deserialize bytea into NumericAggState for numeric aggregates that
 *		don't require sumX2.
 */
Datum
numeric_avg_deserialize(PG_FUNCTION_ARGS)
{
	bytea	   *sstate;
	NumericAggState *result;
	StringInfoData buf;
	NumericVar	tmp_var;

	if (!AggCheckCallContext(fcinfo, NULL))
		elog(ERROR, "aggregate function called in non-aggregate context");

	sstate = PG_GETARG_BYTEA_PP(0);

	init_var(&tmp_var);

	/*
	 * Initialize a StringInfo so that we can "receive" it using the standard
	 * recv-function infrastructure.
	 */
	initReadOnlyStringInfo(&buf, VARDATA_ANY(sstate),
						   VARSIZE_ANY_EXHDR(sstate));

	result = makeNumericAggStateCurrentContext(false);

	/*
	 * The serialised form always carries a digit-array sum (see
	 * numeric_avg_serialize(), which promotes before writing), and we restore
	 * it into sumX below.  Clear fastSum, which the constructor would otherwise
	 * have set for a non-sumX2 state: leaving it set would make the finalisers
	 * read sumX128 -- never written here, so zero -- and silently return the
	 * wrong sum instead of failing.
	 */
	result->fastSum = false;

	/* N */
	result->N = pq_getmsgint64(&buf);

	/* sumX */
	numericvar_deserialize(&buf, &tmp_var);
	accum_sum_add(&(result->sumX), &tmp_var);

	/* maxScale */
	result->maxScale = pq_getmsgint(&buf, 4);

	/* maxScaleCount */
	result->maxScaleCount = pq_getmsgint64(&buf);

	/* NaNcount */
	result->NaNcount = pq_getmsgint64(&buf);

	/* pInfcount */
	result->pInfcount = pq_getmsgint64(&buf);

	/* nInfcount */
	result->nInfcount = pq_getmsgint64(&buf);

	pq_getmsgend(&buf);

	free_var(&tmp_var);

	PG_RETURN_POINTER(result);
}

/*
 * numeric_serialize
 *		Serialization function for NumericAggState for numeric aggregates that
 *		require sumX2.
 */
Datum
numeric_serialize(PG_FUNCTION_ARGS)
{
	NumericAggState *state;
	StringInfoData buf;
	bytea	   *result;
	NumericVar	tmp_var;

	/* Ensure we disallow calling when not in aggregate context */
	if (!AggCheckCallContext(fcinfo, NULL))
		elog(ERROR, "aggregate function called in non-aggregate context");

	state = (NumericAggState *) PG_GETARG_POINTER(0);

	init_var(&tmp_var);

	pq_begintypsend(&buf);

	/* N */
	pq_sendint64(&buf, state->N);

	/* sumX */
	accum_sum_final(&state->sumX, &tmp_var);
	numericvar_serialize(&buf, &tmp_var);

	/* sumX2 */
	accum_sum_final(&state->sumX2, &tmp_var);
	numericvar_serialize(&buf, &tmp_var);

	/* maxScale */
	pq_sendint32(&buf, state->maxScale);

	/* maxScaleCount */
	pq_sendint64(&buf, state->maxScaleCount);

	/* NaNcount */
	pq_sendint64(&buf, state->NaNcount);

	/* pInfcount */
	pq_sendint64(&buf, state->pInfcount);

	/* nInfcount */
	pq_sendint64(&buf, state->nInfcount);

	result = pq_endtypsend(&buf);

	free_var(&tmp_var);

	PG_RETURN_BYTEA_P(result);
}

/*
 * numeric_deserialize
 *		Deserialization function for NumericAggState for numeric aggregates that
 *		require sumX2.
 */
Datum
numeric_deserialize(PG_FUNCTION_ARGS)
{
	bytea	   *sstate;
	NumericAggState *result;
	StringInfoData buf;
	NumericVar	tmp_var;

	if (!AggCheckCallContext(fcinfo, NULL))
		elog(ERROR, "aggregate function called in non-aggregate context");

	sstate = PG_GETARG_BYTEA_PP(0);

	init_var(&tmp_var);

	/*
	 * Initialize a StringInfo so that we can "receive" it using the standard
	 * recv-function infrastructure.
	 */
	initReadOnlyStringInfo(&buf, VARDATA_ANY(sstate),
						   VARSIZE_ANY_EXHDR(sstate));

	/*
	 * Note this passes calcSumX2 = false even though the state being restored
	 * *does* carry a sum of squares: the flag only drives whether the
	 * transition function computes one, and deserialisation fills sumX2 in
	 * directly below.  So clear fastSum explicitly rather than relying on the
	 * constructor's calcSumX2 rule -- the fast int128 representation has no
	 * sumX2 counterpart, and numeric_stddev_internal() reads state->sumX
	 * unconditionally.
	 */
	result = makeNumericAggStateCurrentContext(false);
	result->fastSum = false;

	/* N */
	result->N = pq_getmsgint64(&buf);

	/* sumX */
	numericvar_deserialize(&buf, &tmp_var);
	accum_sum_add(&(result->sumX), &tmp_var);

	/* sumX2 */
	numericvar_deserialize(&buf, &tmp_var);
	accum_sum_add(&(result->sumX2), &tmp_var);

	/* maxScale */
	result->maxScale = pq_getmsgint(&buf, 4);

	/* maxScaleCount */
	result->maxScaleCount = pq_getmsgint64(&buf);

	/* NaNcount */
	result->NaNcount = pq_getmsgint64(&buf);

	/* pInfcount */
	result->pInfcount = pq_getmsgint64(&buf);

	/* nInfcount */
	result->nInfcount = pq_getmsgint64(&buf);

	pq_getmsgend(&buf);

	free_var(&tmp_var);

	PG_RETURN_POINTER(result);
}

/*
 * Generic inverse transition function for numeric aggregates
 * (with or without requirement for X^2).
 */
Datum
numeric_accum_inv(PG_FUNCTION_ARGS)
{
	NumericAggState *state;

	state = PG_ARGISNULL(0) ? NULL : (NumericAggState *) PG_GETARG_POINTER(0);

	/* Should not get here with no state */
	if (state == NULL)
		elog(ERROR, "numeric_accum_inv called with NULL state");

	if (!PG_ARGISNULL(1))
	{
		/* If we fail to perform the inverse transition, return NULL */
		if (!do_numeric_discard(state, PG_GETARG_NUMERIC(1)))
			PG_RETURN_NULL();
	}

	PG_RETURN_POINTER(state);
}


/*
 * Integer data types in general use Numeric accumulators to share code
 * and avoid risk of overflow.
 *
 * However for performance reasons optimized special-purpose accumulator
 * routines are used when possible.
 *
 * On platforms with 128-bit integer support, the 128-bit routines will be
 * used when sum(X) or sum(X*X) fit into 128-bit.
 *
 * For 16 and 32 bit inputs, the N and sum(X) fit into 64-bit so the 64-bit
 * accumulators will be used for SUM and AVG of these data types.
 */

#ifdef HAVE_INT128
typedef struct Int128AggState
{
	bool		calcSumX2;		/* if true, calculate sumX2 */
	int64		N;				/* count of processed numbers */
	int128		sumX;			/* sum of processed numbers */
	int128		sumX2;			/* sum of squares of processed numbers */
} Int128AggState;

/*
 * Prepare state data for a 128-bit aggregate function that needs to compute
 * sum, count and optionally sum of squares of the input.
 */
static Int128AggState *
makeInt128AggState(FunctionCallInfo fcinfo, bool calcSumX2)
{
	Int128AggState *state;
	MemoryContext agg_context;
	MemoryContext old_context;

	if (!AggCheckCallContext(fcinfo, &agg_context))
		elog(ERROR, "aggregate function called in non-aggregate context");

	old_context = MemoryContextSwitchTo(agg_context);

	state = (Int128AggState *) palloc0(sizeof(Int128AggState));
	state->calcSumX2 = calcSumX2;

	MemoryContextSwitchTo(old_context);

	return state;
}

/*
 * Like makeInt128AggState(), but allocate the state in the current memory
 * context.
 */
static Int128AggState *
makeInt128AggStateCurrentContext(bool calcSumX2)
{
	Int128AggState *state;

	state = (Int128AggState *) palloc0(sizeof(Int128AggState));
	state->calcSumX2 = calcSumX2;

	return state;
}

/*
 * Accumulate a new input value for 128-bit aggregate functions.
 */
static void
do_int128_accum(Int128AggState *state, int128 newval)
{
	if (state->calcSumX2)
		state->sumX2 += newval * newval;

	state->sumX += newval;
	state->N++;
}

/*
 * Remove an input value from the aggregated state.
 */
static void
do_int128_discard(Int128AggState *state, int128 newval)
{
	if (state->calcSumX2)
		state->sumX2 -= newval * newval;

	state->sumX -= newval;
	state->N--;
}

typedef Int128AggState PolyNumAggState;
#define makePolyNumAggState makeInt128AggState
#define makePolyNumAggStateCurrentContext makeInt128AggStateCurrentContext
#else
typedef NumericAggState PolyNumAggState;
#define makePolyNumAggState makeNumericAggState
#define makePolyNumAggStateCurrentContext makeNumericAggStateCurrentContext
#endif

Datum
int2_accum(PG_FUNCTION_ARGS)
{
	PolyNumAggState *state;

	state = PG_ARGISNULL(0) ? NULL : (PolyNumAggState *) PG_GETARG_POINTER(0);

	/* Create the state data on the first call */
	if (state == NULL)
		state = makePolyNumAggState(fcinfo, true);

	if (!PG_ARGISNULL(1))
	{
#ifdef HAVE_INT128
		do_int128_accum(state, (int128) PG_GETARG_INT16(1));
#else
		do_numeric_accum(state, int64_to_numeric(PG_GETARG_INT16(1)));
#endif
	}

	PG_RETURN_POINTER(state);
}

Datum
int4_accum(PG_FUNCTION_ARGS)
{
	PolyNumAggState *state;

	state = PG_ARGISNULL(0) ? NULL : (PolyNumAggState *) PG_GETARG_POINTER(0);

	/* Create the state data on the first call */
	if (state == NULL)
		state = makePolyNumAggState(fcinfo, true);

	if (!PG_ARGISNULL(1))
	{
#ifdef HAVE_INT128
		do_int128_accum(state, (int128) PG_GETARG_INT32(1));
#else
		do_numeric_accum(state, int64_to_numeric(PG_GETARG_INT32(1)));
#endif
	}

	PG_RETURN_POINTER(state);
}

Datum
int8_accum(PG_FUNCTION_ARGS)
{
	NumericAggState *state;

	state = PG_ARGISNULL(0) ? NULL : (NumericAggState *) PG_GETARG_POINTER(0);

	/* Create the state data on the first call */
	if (state == NULL)
		state = makeNumericAggState(fcinfo, true);

	if (!PG_ARGISNULL(1))
		do_numeric_accum(state, int64_to_numeric(PG_GETARG_INT64(1)));

	PG_RETURN_POINTER(state);
}

/*
 * Combine function for numeric aggregates which require sumX2
 */
Datum
numeric_poly_combine(PG_FUNCTION_ARGS)
{
	PolyNumAggState *state1;
	PolyNumAggState *state2;
	MemoryContext agg_context;
	MemoryContext old_context;

	if (!AggCheckCallContext(fcinfo, &agg_context))
		elog(ERROR, "aggregate function called in non-aggregate context");

	state1 = PG_ARGISNULL(0) ? NULL : (PolyNumAggState *) PG_GETARG_POINTER(0);
	state2 = PG_ARGISNULL(1) ? NULL : (PolyNumAggState *) PG_GETARG_POINTER(1);

	if (state2 == NULL)
		PG_RETURN_POINTER(state1);

	/* manually copy all fields from state2 to state1 */
	if (state1 == NULL)
	{
		old_context = MemoryContextSwitchTo(agg_context);

		state1 = makePolyNumAggState(fcinfo, true);
		state1->N = state2->N;

#ifdef HAVE_INT128
		state1->sumX = state2->sumX;
		state1->sumX2 = state2->sumX2;
#else
		accum_sum_copy(&state1->sumX, &state2->sumX);
		accum_sum_copy(&state1->sumX2, &state2->sumX2);
#endif

		MemoryContextSwitchTo(old_context);

		PG_RETURN_POINTER(state1);
	}

	if (state2->N > 0)
	{
		state1->N += state2->N;

#ifdef HAVE_INT128
		state1->sumX += state2->sumX;
		state1->sumX2 += state2->sumX2;
#else
		/* The rest of this needs to work in the aggregate context */
		old_context = MemoryContextSwitchTo(agg_context);

		/* Accumulate sums */
		accum_sum_combine(&state1->sumX, &state2->sumX);
		accum_sum_combine(&state1->sumX2, &state2->sumX2);

		MemoryContextSwitchTo(old_context);
#endif

	}
	PG_RETURN_POINTER(state1);
}

/*
 * numeric_poly_serialize
 *		Serialize PolyNumAggState into bytea for aggregate functions which
 *		require sumX2.
 */
Datum
numeric_poly_serialize(PG_FUNCTION_ARGS)
{
	PolyNumAggState *state;
	StringInfoData buf;
	bytea	   *result;
	NumericVar	tmp_var;

	/* Ensure we disallow calling when not in aggregate context */
	if (!AggCheckCallContext(fcinfo, NULL))
		elog(ERROR, "aggregate function called in non-aggregate context");

	state = (PolyNumAggState *) PG_GETARG_POINTER(0);

	/*
	 * If the platform supports int128 then sumX and sumX2 will be a 128 bit
	 * integer type. Here we'll convert that into a numeric type so that the
	 * combine state is in the same format for both int128 enabled machines
	 * and machines which don't support that type. The logic here is that one
	 * day we might like to send these over to another server for further
	 * processing and we want a standard format to work with.
	 */

	init_var(&tmp_var);

	pq_begintypsend(&buf);

	/* N */
	pq_sendint64(&buf, state->N);

	/* sumX */
#ifdef HAVE_INT128
	int128_to_numericvar(state->sumX, &tmp_var);
#else
	accum_sum_final(&state->sumX, &tmp_var);
#endif
	numericvar_serialize(&buf, &tmp_var);

	/* sumX2 */
#ifdef HAVE_INT128
	int128_to_numericvar(state->sumX2, &tmp_var);
#else
	accum_sum_final(&state->sumX2, &tmp_var);
#endif
	numericvar_serialize(&buf, &tmp_var);

	result = pq_endtypsend(&buf);

	free_var(&tmp_var);

	PG_RETURN_BYTEA_P(result);
}

/*
 * numeric_poly_deserialize
 *		Deserialize PolyNumAggState from bytea for aggregate functions which
 *		require sumX2.
 */
Datum
numeric_poly_deserialize(PG_FUNCTION_ARGS)
{
	bytea	   *sstate;
	PolyNumAggState *result;
	StringInfoData buf;
	NumericVar	tmp_var;

	if (!AggCheckCallContext(fcinfo, NULL))
		elog(ERROR, "aggregate function called in non-aggregate context");

	sstate = PG_GETARG_BYTEA_PP(0);

	init_var(&tmp_var);

	/*
	 * Initialize a StringInfo so that we can "receive" it using the standard
	 * recv-function infrastructure.
	 */
	initReadOnlyStringInfo(&buf, VARDATA_ANY(sstate),
						   VARSIZE_ANY_EXHDR(sstate));

	result = makePolyNumAggStateCurrentContext(false);

	/* N */
	result->N = pq_getmsgint64(&buf);

	/* sumX */
	numericvar_deserialize(&buf, &tmp_var);
#ifdef HAVE_INT128
	numericvar_to_int128(&tmp_var, &result->sumX);
#else
	accum_sum_add(&result->sumX, &tmp_var);
#endif

	/* sumX2 */
	numericvar_deserialize(&buf, &tmp_var);
#ifdef HAVE_INT128
	numericvar_to_int128(&tmp_var, &result->sumX2);
#else
	accum_sum_add(&result->sumX2, &tmp_var);
#endif

	pq_getmsgend(&buf);

	free_var(&tmp_var);

	PG_RETURN_POINTER(result);
}

/*
 * Transition function for int8 input when we don't need sumX2.
 */
Datum
int8_avg_accum(PG_FUNCTION_ARGS)
{
	PolyNumAggState *state;

	state = PG_ARGISNULL(0) ? NULL : (PolyNumAggState *) PG_GETARG_POINTER(0);

	/* Create the state data on the first call */
	if (state == NULL)
		state = makePolyNumAggState(fcinfo, false);

	if (!PG_ARGISNULL(1))
	{
#ifdef HAVE_INT128
		do_int128_accum(state, (int128) PG_GETARG_INT64(1));
#else
		do_numeric_accum(state, int64_to_numeric(PG_GETARG_INT64(1)));
#endif
	}

	PG_RETURN_POINTER(state);
}

/*
 * Combine function for PolyNumAggState for aggregates which don't require
 * sumX2
 */
Datum
int8_avg_combine(PG_FUNCTION_ARGS)
{
	PolyNumAggState *state1;
	PolyNumAggState *state2;
	MemoryContext agg_context;
	MemoryContext old_context;

	if (!AggCheckCallContext(fcinfo, &agg_context))
		elog(ERROR, "aggregate function called in non-aggregate context");

	state1 = PG_ARGISNULL(0) ? NULL : (PolyNumAggState *) PG_GETARG_POINTER(0);
	state2 = PG_ARGISNULL(1) ? NULL : (PolyNumAggState *) PG_GETARG_POINTER(1);

	if (state2 == NULL)
		PG_RETURN_POINTER(state1);

	/* manually copy all fields from state2 to state1 */
	if (state1 == NULL)
	{
		old_context = MemoryContextSwitchTo(agg_context);

		state1 = makePolyNumAggState(fcinfo, false);
		state1->N = state2->N;

#ifdef HAVE_INT128
		state1->sumX = state2->sumX;
#else
		accum_sum_copy(&state1->sumX, &state2->sumX);
#endif
		MemoryContextSwitchTo(old_context);

		PG_RETURN_POINTER(state1);
	}

	if (state2->N > 0)
	{
		state1->N += state2->N;

#ifdef HAVE_INT128
		state1->sumX += state2->sumX;
#else
		/* The rest of this needs to work in the aggregate context */
		old_context = MemoryContextSwitchTo(agg_context);

		/* Accumulate sums */
		accum_sum_combine(&state1->sumX, &state2->sumX);

		MemoryContextSwitchTo(old_context);
#endif

	}
	PG_RETURN_POINTER(state1);
}

/*
 * int8_avg_serialize
 *		Serialize PolyNumAggState into bytea using the standard
 *		recv-function infrastructure.
 */
Datum
int8_avg_serialize(PG_FUNCTION_ARGS)
{
	PolyNumAggState *state;
	StringInfoData buf;
	bytea	   *result;
	NumericVar	tmp_var;

	/* Ensure we disallow calling when not in aggregate context */
	if (!AggCheckCallContext(fcinfo, NULL))
		elog(ERROR, "aggregate function called in non-aggregate context");

	state = (PolyNumAggState *) PG_GETARG_POINTER(0);

	/*
	 * If the platform supports int128 then sumX will be a 128 integer type.
	 * Here we'll convert that into a numeric type so that the combine state
	 * is in the same format for both int128 enabled machines and machines
	 * which don't support that type. The logic here is that one day we might
	 * like to send these over to another server for further processing and we
	 * want a standard format to work with.
	 */

	init_var(&tmp_var);

	pq_begintypsend(&buf);

	/* N */
	pq_sendint64(&buf, state->N);

	/* sumX */
#ifdef HAVE_INT128
	int128_to_numericvar(state->sumX, &tmp_var);
#else
	accum_sum_final(&state->sumX, &tmp_var);
#endif
	numericvar_serialize(&buf, &tmp_var);

	result = pq_endtypsend(&buf);

	free_var(&tmp_var);

	PG_RETURN_BYTEA_P(result);
}

/*
 * int8_avg_deserialize
 *		Deserialize bytea back into PolyNumAggState.
 */
Datum
int8_avg_deserialize(PG_FUNCTION_ARGS)
{
	bytea	   *sstate;
	PolyNumAggState *result;
	StringInfoData buf;
	NumericVar	tmp_var;

	if (!AggCheckCallContext(fcinfo, NULL))
		elog(ERROR, "aggregate function called in non-aggregate context");

	sstate = PG_GETARG_BYTEA_PP(0);

	init_var(&tmp_var);

	/*
	 * Initialize a StringInfo so that we can "receive" it using the standard
	 * recv-function infrastructure.
	 */
	initReadOnlyStringInfo(&buf, VARDATA_ANY(sstate),
						   VARSIZE_ANY_EXHDR(sstate));

	result = makePolyNumAggStateCurrentContext(false);

	/* N */
	result->N = pq_getmsgint64(&buf);

	/* sumX */
	numericvar_deserialize(&buf, &tmp_var);
#ifdef HAVE_INT128
	numericvar_to_int128(&tmp_var, &result->sumX);
#else
	accum_sum_add(&result->sumX, &tmp_var);
#endif

	pq_getmsgend(&buf);

	free_var(&tmp_var);

	PG_RETURN_POINTER(result);
}

/*
 * Inverse transition functions to go with the above.
 */

Datum
int2_accum_inv(PG_FUNCTION_ARGS)
{
	PolyNumAggState *state;

	state = PG_ARGISNULL(0) ? NULL : (PolyNumAggState *) PG_GETARG_POINTER(0);

	/* Should not get here with no state */
	if (state == NULL)
		elog(ERROR, "int2_accum_inv called with NULL state");

	if (!PG_ARGISNULL(1))
	{
#ifdef HAVE_INT128
		do_int128_discard(state, (int128) PG_GETARG_INT16(1));
#else
		/* Should never fail, all inputs have dscale 0 */
		if (!do_numeric_discard(state, int64_to_numeric(PG_GETARG_INT16(1))))
			elog(ERROR, "do_numeric_discard failed unexpectedly");
#endif
	}

	PG_RETURN_POINTER(state);
}

Datum
int4_accum_inv(PG_FUNCTION_ARGS)
{
	PolyNumAggState *state;

	state = PG_ARGISNULL(0) ? NULL : (PolyNumAggState *) PG_GETARG_POINTER(0);

	/* Should not get here with no state */
	if (state == NULL)
		elog(ERROR, "int4_accum_inv called with NULL state");

	if (!PG_ARGISNULL(1))
	{
#ifdef HAVE_INT128
		do_int128_discard(state, (int128) PG_GETARG_INT32(1));
#else
		/* Should never fail, all inputs have dscale 0 */
		if (!do_numeric_discard(state, int64_to_numeric(PG_GETARG_INT32(1))))
			elog(ERROR, "do_numeric_discard failed unexpectedly");
#endif
	}

	PG_RETURN_POINTER(state);
}

Datum
int8_accum_inv(PG_FUNCTION_ARGS)
{
	NumericAggState *state;

	state = PG_ARGISNULL(0) ? NULL : (NumericAggState *) PG_GETARG_POINTER(0);

	/* Should not get here with no state */
	if (state == NULL)
		elog(ERROR, "int8_accum_inv called with NULL state");

	if (!PG_ARGISNULL(1))
	{
		/* Should never fail, all inputs have dscale 0 */
		if (!do_numeric_discard(state, int64_to_numeric(PG_GETARG_INT64(1))))
			elog(ERROR, "do_numeric_discard failed unexpectedly");
	}

	PG_RETURN_POINTER(state);
}

Datum
int8_avg_accum_inv(PG_FUNCTION_ARGS)
{
	PolyNumAggState *state;

	state = PG_ARGISNULL(0) ? NULL : (PolyNumAggState *) PG_GETARG_POINTER(0);

	/* Should not get here with no state */
	if (state == NULL)
		elog(ERROR, "int8_avg_accum_inv called with NULL state");

	if (!PG_ARGISNULL(1))
	{
#ifdef HAVE_INT128
		do_int128_discard(state, (int128) PG_GETARG_INT64(1));
#else
		/* Should never fail, all inputs have dscale 0 */
		if (!do_numeric_discard(state, int64_to_numeric(PG_GETARG_INT64(1))))
			elog(ERROR, "do_numeric_discard failed unexpectedly");
#endif
	}

	PG_RETURN_POINTER(state);
}

Datum
numeric_poly_sum(PG_FUNCTION_ARGS)
{
#ifdef HAVE_INT128
	PolyNumAggState *state;
	Numeric		res;
	NumericVar	result;

	state = PG_ARGISNULL(0) ? NULL : (PolyNumAggState *) PG_GETARG_POINTER(0);

	/* If there were no non-null inputs, return NULL */
	if (state == NULL || state->N == 0)
		PG_RETURN_NULL();

	init_var(&result);

	int128_to_numericvar(state->sumX, &result);

	res = make_result(&result);

	free_var(&result);

	PG_RETURN_NUMERIC(res);
#else
	return numeric_sum(fcinfo);
#endif
}

Datum
numeric_poly_avg(PG_FUNCTION_ARGS)
{
#ifdef HAVE_INT128
	PolyNumAggState *state;
	NumericVar	result;
	Datum		countd,
				sumd;

	state = PG_ARGISNULL(0) ? NULL : (PolyNumAggState *) PG_GETARG_POINTER(0);

	/* If there were no non-null inputs, return NULL */
	if (state == NULL || state->N == 0)
		PG_RETURN_NULL();

	init_var(&result);

	int128_to_numericvar(state->sumX, &result);

	countd = NumericGetDatum(int64_to_numeric(state->N));
	sumd = NumericGetDatum(make_result(&result));

	free_var(&result);

	PG_RETURN_DATUM(DirectFunctionCall2(numeric_div, sumd, countd));
#else
	return numeric_avg(fcinfo);
#endif
}

Datum
numeric_avg(PG_FUNCTION_ARGS)
{
	NumericAggState *state;
	Datum		N_datum;
	Datum		sumX_datum;
	NumericVar	sumX_var;

	state = PG_ARGISNULL(0) ? NULL : (NumericAggState *) PG_GETARG_POINTER(0);

	/* If there were no non-null inputs, return NULL */
	if (state == NULL || NA_TOTAL_COUNT(state) == 0)
		PG_RETURN_NULL();

	if (state->NaNcount > 0)	/* there was at least one NaN input */
		PG_RETURN_NUMERIC(make_result(&const_nan));

	/* adding plus and minus infinities gives NaN */
	if (state->pInfcount > 0 && state->nInfcount > 0)
		PG_RETURN_NUMERIC(make_result(&const_nan));
	if (state->pInfcount > 0)
		PG_RETURN_NUMERIC(make_result(&const_pinf));
	if (state->nInfcount > 0)
		PG_RETURN_NUMERIC(make_result(&const_ninf));

	N_datum = NumericGetDatum(int64_to_numeric(state->N));

	init_var(&sumX_var);
	if (state->fastSum)
		numeric_dec128_int128_to_var(state->sumX128, state->fastScale,
									 &sumX_var);
	else
		accum_sum_final(&state->sumX, &sumX_var);
	sumX_datum = NumericGetDatum(make_result(&sumX_var));
	free_var(&sumX_var);

	PG_RETURN_DATUM(DirectFunctionCall2(numeric_div, sumX_datum, N_datum));
}

Datum
numeric_sum(PG_FUNCTION_ARGS)
{
	NumericAggState *state;
	NumericVar	sumX_var;
	Numeric		result;

	state = PG_ARGISNULL(0) ? NULL : (NumericAggState *) PG_GETARG_POINTER(0);

	/* If there were no non-null inputs, return NULL */
	if (state == NULL || NA_TOTAL_COUNT(state) == 0)
		PG_RETURN_NULL();

	if (state->NaNcount > 0)	/* there was at least one NaN input */
		PG_RETURN_NUMERIC(make_result(&const_nan));

	/* adding plus and minus infinities gives NaN */
	if (state->pInfcount > 0 && state->nInfcount > 0)
		PG_RETURN_NUMERIC(make_result(&const_nan));
	if (state->pInfcount > 0)
		PG_RETURN_NUMERIC(make_result(&const_pinf));
	if (state->nInfcount > 0)
		PG_RETURN_NUMERIC(make_result(&const_ninf));

	init_var(&sumX_var);
	if (state->fastSum)
		numeric_dec128_int128_to_var(state->sumX128, state->fastScale,
									 &sumX_var);
	else
		accum_sum_final(&state->sumX, &sumX_var);
	result = make_result(&sumX_var);
	free_var(&sumX_var);

	PG_RETURN_NUMERIC(result);
}

/*
 * Workhorse routine for the standard deviance and variance
 * aggregates. 'state' is aggregate's transition state.
 * 'variance' specifies whether we should calculate the
 * variance or the standard deviation. 'sample' indicates whether the
 * caller is interested in the sample or the population
 * variance/stddev.
 *
 * If appropriate variance statistic is undefined for the input,
 * *is_null is set to true and NULL is returned.
 */
static Numeric
numeric_stddev_internal(NumericAggState *state,
						bool variance, bool sample,
						bool *is_null)
{
	Numeric		res;
	NumericVar	vN,
				vsumX,
				vsumX2,
				vNminus1;
	int64		totCount;
	int			rscale;

	/*
	 * Sample stddev and variance are undefined when N <= 1; population stddev
	 * is undefined when N == 0.  Return NULL in either case (note that NaNs
	 * and infinities count as normal inputs for this purpose).
	 */
	if (state == NULL || (totCount = NA_TOTAL_COUNT(state)) == 0)
	{
		*is_null = true;
		return NULL;
	}

	if (sample && totCount <= 1)
	{
		*is_null = true;
		return NULL;
	}

	*is_null = false;

	/*
	 * Deal with NaN and infinity cases.  By analogy to the behavior of the
	 * float8 functions, any infinity input produces NaN output.
	 */
	if (state->NaNcount > 0 || state->pInfcount > 0 || state->nInfcount > 0)
		return make_result(&const_nan);

	/* OK, normal calculation applies */
	init_var(&vN);
	init_var(&vsumX);
	init_var(&vsumX2);

	/*
	 * A state that tracks sumX2 is created already promoted (see
	 * NumericAggState), so the fast representation can never reach here.
	 */
	Assert(!state->fastSum);

	int64_to_numericvar(state->N, &vN);
	accum_sum_final(&(state->sumX), &vsumX);
	accum_sum_final(&(state->sumX2), &vsumX2);

	init_var(&vNminus1);
	sub_var(&vN, &const_one, &vNminus1);

	/* compute rscale for mul_var calls */
	rscale = vsumX.dscale * 2;

	mul_var(&vsumX, &vsumX, &vsumX, rscale);	/* vsumX = sumX * sumX */
	mul_var(&vN, &vsumX2, &vsumX2, rscale); /* vsumX2 = N * sumX2 */
	sub_var(&vsumX2, &vsumX, &vsumX2);	/* N * sumX2 - sumX * sumX */

	if (cmp_var(&vsumX2, &const_zero) <= 0)
	{
		/* Watch out for roundoff error producing a negative numerator */
		res = make_result(&const_zero);
	}
	else
	{
		if (sample)
			mul_var(&vN, &vNminus1, &vNminus1, 0);	/* N * (N - 1) */
		else
			mul_var(&vN, &vN, &vNminus1, 0);	/* N * N */

		/*
		 * EXPERIMENTAL (SPEC-numeric-as-dec128.md): same clamp as
		 * numeric_div_opt_error() -- this rscale feeds both div_var() below
		 * and the sqrt_var() a few lines down, and the result of either is
		 * packed straight into dec128 by make_result().  Per the spec's
		 * risk table, unclamped this made stddev/variance overflow on
		 * ordinary inputs instead of just losing precision.
		 */
		rscale = select_div_scale(&vsumX2, &vNminus1);
		rscale = Min(rscale, NUMERIC_MAX_STORED_SCALE);
		div_var(&vsumX2, &vNminus1, &vsumX, rscale, true, true);	/* variance */
		if (!variance)
			sqrt_var(&vsumX, &vsumX, rscale);	/* stddev */

		res = make_result(&vsumX);
	}

	free_var(&vNminus1);
	free_var(&vsumX);
	free_var(&vsumX2);

	return res;
}

Datum
numeric_var_samp(PG_FUNCTION_ARGS)
{
	NumericAggState *state;
	Numeric		res;
	bool		is_null;

	state = PG_ARGISNULL(0) ? NULL : (NumericAggState *) PG_GETARG_POINTER(0);

	res = numeric_stddev_internal(state, true, true, &is_null);

	if (is_null)
		PG_RETURN_NULL();
	else
		PG_RETURN_NUMERIC(res);
}

Datum
numeric_stddev_samp(PG_FUNCTION_ARGS)
{
	NumericAggState *state;
	Numeric		res;
	bool		is_null;

	state = PG_ARGISNULL(0) ? NULL : (NumericAggState *) PG_GETARG_POINTER(0);

	res = numeric_stddev_internal(state, false, true, &is_null);

	if (is_null)
		PG_RETURN_NULL();
	else
		PG_RETURN_NUMERIC(res);
}

Datum
numeric_var_pop(PG_FUNCTION_ARGS)
{
	NumericAggState *state;
	Numeric		res;
	bool		is_null;

	state = PG_ARGISNULL(0) ? NULL : (NumericAggState *) PG_GETARG_POINTER(0);

	res = numeric_stddev_internal(state, true, false, &is_null);

	if (is_null)
		PG_RETURN_NULL();
	else
		PG_RETURN_NUMERIC(res);
}

Datum
numeric_stddev_pop(PG_FUNCTION_ARGS)
{
	NumericAggState *state;
	Numeric		res;
	bool		is_null;

	state = PG_ARGISNULL(0) ? NULL : (NumericAggState *) PG_GETARG_POINTER(0);

	res = numeric_stddev_internal(state, false, false, &is_null);

	if (is_null)
		PG_RETURN_NULL();
	else
		PG_RETURN_NUMERIC(res);
}

#ifdef HAVE_INT128
static Numeric
numeric_poly_stddev_internal(Int128AggState *state,
							 bool variance, bool sample,
							 bool *is_null)
{
	NumericAggState numstate;
	Numeric		res;

	/* Initialize an empty agg state */
	memset(&numstate, 0, sizeof(NumericAggState));

	if (state)
	{
		NumericVar	tmp_var;

		numstate.N = state->N;

		init_var(&tmp_var);

		int128_to_numericvar(state->sumX, &tmp_var);
		accum_sum_add(&numstate.sumX, &tmp_var);

		int128_to_numericvar(state->sumX2, &tmp_var);
		accum_sum_add(&numstate.sumX2, &tmp_var);

		free_var(&tmp_var);
	}

	res = numeric_stddev_internal(&numstate, variance, sample, is_null);

	if (numstate.sumX.ndigits > 0)
	{
		pfree(numstate.sumX.pos_digits);
		pfree(numstate.sumX.neg_digits);
	}
	if (numstate.sumX2.ndigits > 0)
	{
		pfree(numstate.sumX2.pos_digits);
		pfree(numstate.sumX2.neg_digits);
	}

	return res;
}
#endif

Datum
numeric_poly_var_samp(PG_FUNCTION_ARGS)
{
#ifdef HAVE_INT128
	PolyNumAggState *state;
	Numeric		res;
	bool		is_null;

	state = PG_ARGISNULL(0) ? NULL : (PolyNumAggState *) PG_GETARG_POINTER(0);

	res = numeric_poly_stddev_internal(state, true, true, &is_null);

	if (is_null)
		PG_RETURN_NULL();
	else
		PG_RETURN_NUMERIC(res);
#else
	return numeric_var_samp(fcinfo);
#endif
}

Datum
numeric_poly_stddev_samp(PG_FUNCTION_ARGS)
{
#ifdef HAVE_INT128
	PolyNumAggState *state;
	Numeric		res;
	bool		is_null;

	state = PG_ARGISNULL(0) ? NULL : (PolyNumAggState *) PG_GETARG_POINTER(0);

	res = numeric_poly_stddev_internal(state, false, true, &is_null);

	if (is_null)
		PG_RETURN_NULL();
	else
		PG_RETURN_NUMERIC(res);
#else
	return numeric_stddev_samp(fcinfo);
#endif
}

Datum
numeric_poly_var_pop(PG_FUNCTION_ARGS)
{
#ifdef HAVE_INT128
	PolyNumAggState *state;
	Numeric		res;
	bool		is_null;

	state = PG_ARGISNULL(0) ? NULL : (PolyNumAggState *) PG_GETARG_POINTER(0);

	res = numeric_poly_stddev_internal(state, true, false, &is_null);

	if (is_null)
		PG_RETURN_NULL();
	else
		PG_RETURN_NUMERIC(res);
#else
	return numeric_var_pop(fcinfo);
#endif
}

Datum
numeric_poly_stddev_pop(PG_FUNCTION_ARGS)
{
#ifdef HAVE_INT128
	PolyNumAggState *state;
	Numeric		res;
	bool		is_null;

	state = PG_ARGISNULL(0) ? NULL : (PolyNumAggState *) PG_GETARG_POINTER(0);

	res = numeric_poly_stddev_internal(state, false, false, &is_null);

	if (is_null)
		PG_RETURN_NULL();
	else
		PG_RETURN_NUMERIC(res);
#else
	return numeric_stddev_pop(fcinfo);
#endif
}

/*
 * SUM transition functions for integer datatypes.
 *
 * To avoid overflow, we use accumulators wider than the input datatype.
 * A Numeric accumulator is needed for int8 input; for int4 and int2
 * inputs, we use int8 accumulators which should be sufficient for practical
 * purposes.  (The latter two therefore don't really belong in this file,
 * but we keep them here anyway.)
 *
 * Because SQL defines the SUM() of no values to be NULL, not zero,
 * the initial condition of the transition data value needs to be NULL. This
 * means we can't rely on ExecAgg to automatically insert the first non-null
 * data value into the transition data: it doesn't know how to do the type
 * conversion.  The upshot is that these routines have to be marked non-strict
 * and handle substitution of the first non-null input themselves.
 *
 * Note: these functions are used only in plain aggregation mode.
 * In moving-aggregate mode, we use intX_avg_accum and intX_avg_accum_inv.
 */

Datum
int2_sum(PG_FUNCTION_ARGS)
{
	int64		newval;

	if (PG_ARGISNULL(0))
	{
		/* No non-null input seen so far... */
		if (PG_ARGISNULL(1))
			PG_RETURN_NULL();	/* still no non-null */
		/* This is the first non-null input. */
		newval = (int64) PG_GETARG_INT16(1);
		PG_RETURN_INT64(newval);
	}

	/*
	 * If we're invoked as an aggregate, we can cheat and modify our first
	 * parameter in-place to avoid palloc overhead. If not, we need to return
	 * the new value of the transition variable. (If int8 is pass-by-value,
	 * then of course this is useless as well as incorrect, so just ifdef it
	 * out.)
	 */
#ifndef USE_FLOAT8_BYVAL		/* controls int8 too */
	if (AggCheckCallContext(fcinfo, NULL))
	{
		int64	   *oldsum = (int64 *) PG_GETARG_POINTER(0);

		/* Leave the running sum unchanged in the new input is null */
		if (!PG_ARGISNULL(1))
			*oldsum = *oldsum + (int64) PG_GETARG_INT16(1);

		PG_RETURN_POINTER(oldsum);
	}
	else
#endif
	{
		int64		oldsum = PG_GETARG_INT64(0);

		/* Leave sum unchanged if new input is null. */
		if (PG_ARGISNULL(1))
			PG_RETURN_INT64(oldsum);

		/* OK to do the addition. */
		newval = oldsum + (int64) PG_GETARG_INT16(1);

		PG_RETURN_INT64(newval);
	}
}

Datum
int4_sum(PG_FUNCTION_ARGS)
{
	int64		newval;

	if (PG_ARGISNULL(0))
	{
		/* No non-null input seen so far... */
		if (PG_ARGISNULL(1))
			PG_RETURN_NULL();	/* still no non-null */
		/* This is the first non-null input. */
		newval = (int64) PG_GETARG_INT32(1);
		PG_RETURN_INT64(newval);
	}

	/*
	 * If we're invoked as an aggregate, we can cheat and modify our first
	 * parameter in-place to avoid palloc overhead. If not, we need to return
	 * the new value of the transition variable. (If int8 is pass-by-value,
	 * then of course this is useless as well as incorrect, so just ifdef it
	 * out.)
	 */
#ifndef USE_FLOAT8_BYVAL		/* controls int8 too */
	if (AggCheckCallContext(fcinfo, NULL))
	{
		int64	   *oldsum = (int64 *) PG_GETARG_POINTER(0);

		/* Leave the running sum unchanged in the new input is null */
		if (!PG_ARGISNULL(1))
			*oldsum = *oldsum + (int64) PG_GETARG_INT32(1);

		PG_RETURN_POINTER(oldsum);
	}
	else
#endif
	{
		int64		oldsum = PG_GETARG_INT64(0);

		/* Leave sum unchanged if new input is null. */
		if (PG_ARGISNULL(1))
			PG_RETURN_INT64(oldsum);

		/* OK to do the addition. */
		newval = oldsum + (int64) PG_GETARG_INT32(1);

		PG_RETURN_INT64(newval);
	}
}

/*
 * Note: this function is obsolete, it's no longer used for SUM(int8).
 */
Datum
int8_sum(PG_FUNCTION_ARGS)
{
	Numeric		oldsum;

	if (PG_ARGISNULL(0))
	{
		/* No non-null input seen so far... */
		if (PG_ARGISNULL(1))
			PG_RETURN_NULL();	/* still no non-null */
		/* This is the first non-null input. */
		PG_RETURN_NUMERIC(int64_to_numeric(PG_GETARG_INT64(1)));
	}

	/*
	 * Note that we cannot special-case the aggregate case here, as we do for
	 * int2_sum and int4_sum: numeric is of variable size, so we cannot modify
	 * our first parameter in-place.
	 */

	oldsum = PG_GETARG_NUMERIC(0);

	/* Leave sum unchanged if new input is null. */
	if (PG_ARGISNULL(1))
		PG_RETURN_NUMERIC(oldsum);

	/* OK to do the addition. */
	PG_RETURN_DATUM(DirectFunctionCall2(numeric_add,
										NumericGetDatum(oldsum),
										NumericGetDatum(int64_to_numeric(PG_GETARG_INT64(1)))));
}


/*
 * Routines for avg(int2) and avg(int4).  The transition datatype
 * is a two-element int8 array, holding count and sum.
 *
 * These functions are also used for sum(int2) and sum(int4) when
 * operating in moving-aggregate mode, since for correct inverse transitions
 * we need to count the inputs.
 */

typedef struct Int8TransTypeData
{
	int64		count;
	int64		sum;
} Int8TransTypeData;

Datum
int2_avg_accum(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray;
	int16		newval = PG_GETARG_INT16(1);
	Int8TransTypeData *transdata;

	/*
	 * If we're invoked as an aggregate, we can cheat and modify our first
	 * parameter in-place to reduce palloc overhead. Otherwise we need to make
	 * a copy of it before scribbling on it.
	 */
	if (AggCheckCallContext(fcinfo, NULL))
		transarray = PG_GETARG_ARRAYTYPE_P(0);
	else
		transarray = PG_GETARG_ARRAYTYPE_P_COPY(0);

	if (ARR_HASNULL(transarray) ||
		ARR_SIZE(transarray) != ARR_OVERHEAD_NONULLS(1) + sizeof(Int8TransTypeData))
		elog(ERROR, "expected 2-element int8 array");

	transdata = (Int8TransTypeData *) ARR_DATA_PTR(transarray);
	transdata->count++;
	transdata->sum += newval;

	PG_RETURN_ARRAYTYPE_P(transarray);
}

Datum
int4_avg_accum(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray;
	int32		newval = PG_GETARG_INT32(1);
	Int8TransTypeData *transdata;

	/*
	 * If we're invoked as an aggregate, we can cheat and modify our first
	 * parameter in-place to reduce palloc overhead. Otherwise we need to make
	 * a copy of it before scribbling on it.
	 */
	if (AggCheckCallContext(fcinfo, NULL))
		transarray = PG_GETARG_ARRAYTYPE_P(0);
	else
		transarray = PG_GETARG_ARRAYTYPE_P_COPY(0);

	if (ARR_HASNULL(transarray) ||
		ARR_SIZE(transarray) != ARR_OVERHEAD_NONULLS(1) + sizeof(Int8TransTypeData))
		elog(ERROR, "expected 2-element int8 array");

	transdata = (Int8TransTypeData *) ARR_DATA_PTR(transarray);
	transdata->count++;
	transdata->sum += newval;

	PG_RETURN_ARRAYTYPE_P(transarray);
}

Datum
int4_avg_combine(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray1;
	ArrayType  *transarray2;
	Int8TransTypeData *state1;
	Int8TransTypeData *state2;

	if (!AggCheckCallContext(fcinfo, NULL))
		elog(ERROR, "aggregate function called in non-aggregate context");

	transarray1 = PG_GETARG_ARRAYTYPE_P(0);
	transarray2 = PG_GETARG_ARRAYTYPE_P(1);

	if (ARR_HASNULL(transarray1) ||
		ARR_SIZE(transarray1) != ARR_OVERHEAD_NONULLS(1) + sizeof(Int8TransTypeData))
		elog(ERROR, "expected 2-element int8 array");

	if (ARR_HASNULL(transarray2) ||
		ARR_SIZE(transarray2) != ARR_OVERHEAD_NONULLS(1) + sizeof(Int8TransTypeData))
		elog(ERROR, "expected 2-element int8 array");

	state1 = (Int8TransTypeData *) ARR_DATA_PTR(transarray1);
	state2 = (Int8TransTypeData *) ARR_DATA_PTR(transarray2);

	state1->count += state2->count;
	state1->sum += state2->sum;

	PG_RETURN_ARRAYTYPE_P(transarray1);
}

Datum
int2_avg_accum_inv(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray;
	int16		newval = PG_GETARG_INT16(1);
	Int8TransTypeData *transdata;

	/*
	 * If we're invoked as an aggregate, we can cheat and modify our first
	 * parameter in-place to reduce palloc overhead. Otherwise we need to make
	 * a copy of it before scribbling on it.
	 */
	if (AggCheckCallContext(fcinfo, NULL))
		transarray = PG_GETARG_ARRAYTYPE_P(0);
	else
		transarray = PG_GETARG_ARRAYTYPE_P_COPY(0);

	if (ARR_HASNULL(transarray) ||
		ARR_SIZE(transarray) != ARR_OVERHEAD_NONULLS(1) + sizeof(Int8TransTypeData))
		elog(ERROR, "expected 2-element int8 array");

	transdata = (Int8TransTypeData *) ARR_DATA_PTR(transarray);
	transdata->count--;
	transdata->sum -= newval;

	PG_RETURN_ARRAYTYPE_P(transarray);
}

Datum
int4_avg_accum_inv(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray;
	int32		newval = PG_GETARG_INT32(1);
	Int8TransTypeData *transdata;

	/*
	 * If we're invoked as an aggregate, we can cheat and modify our first
	 * parameter in-place to reduce palloc overhead. Otherwise we need to make
	 * a copy of it before scribbling on it.
	 */
	if (AggCheckCallContext(fcinfo, NULL))
		transarray = PG_GETARG_ARRAYTYPE_P(0);
	else
		transarray = PG_GETARG_ARRAYTYPE_P_COPY(0);

	if (ARR_HASNULL(transarray) ||
		ARR_SIZE(transarray) != ARR_OVERHEAD_NONULLS(1) + sizeof(Int8TransTypeData))
		elog(ERROR, "expected 2-element int8 array");

	transdata = (Int8TransTypeData *) ARR_DATA_PTR(transarray);
	transdata->count--;
	transdata->sum -= newval;

	PG_RETURN_ARRAYTYPE_P(transarray);
}

Datum
int8_avg(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray = PG_GETARG_ARRAYTYPE_P(0);
	Int8TransTypeData *transdata;
	Datum		countd,
				sumd;

	if (ARR_HASNULL(transarray) ||
		ARR_SIZE(transarray) != ARR_OVERHEAD_NONULLS(1) + sizeof(Int8TransTypeData))
		elog(ERROR, "expected 2-element int8 array");
	transdata = (Int8TransTypeData *) ARR_DATA_PTR(transarray);

	/* SQL defines AVG of no values to be NULL */
	if (transdata->count == 0)
		PG_RETURN_NULL();

	countd = NumericGetDatum(int64_to_numeric(transdata->count));
	sumd = NumericGetDatum(int64_to_numeric(transdata->sum));

	PG_RETURN_DATUM(DirectFunctionCall2(numeric_div, sumd, countd));
}

/*
 * SUM(int2) and SUM(int4) both return int8, so we can use this
 * final function for both.
 */
Datum
int2int4_sum(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray = PG_GETARG_ARRAYTYPE_P(0);
	Int8TransTypeData *transdata;

	if (ARR_HASNULL(transarray) ||
		ARR_SIZE(transarray) != ARR_OVERHEAD_NONULLS(1) + sizeof(Int8TransTypeData))
		elog(ERROR, "expected 2-element int8 array");
	transdata = (Int8TransTypeData *) ARR_DATA_PTR(transarray);

	/* SQL defines SUM of no values to be NULL */
	if (transdata->count == 0)
		PG_RETURN_NULL();

	PG_RETURN_DATUM(Int64GetDatumFast(transdata->sum));
}


/* ----------------------------------------------------------------------
 *
 * Debug support
 *
 * ----------------------------------------------------------------------
 */

#ifdef NUMERIC_DEBUG

/*
 * dump_numeric() - Dump a value in the db storage format for debugging
 */
static void
dump_numeric(const char *str, Numeric num)
{
	/*
	 * EXPERIMENTAL (SPEC-numeric-as-dec128.md): there's no digit array or
	 * header to walk anymore, just a scaled 128-bit integer; print that
	 * directly instead of the classic NumericDigit dump.
	 */
	if (NUMERIC_IS_NAN(num))
		printf("%s: NUMERIC NaN\n", str);
	else if (NUMERIC_IS_PINF(num))
		printf("%s: NUMERIC Infinity\n", str);
	else if (NUMERIC_IS_NINF(num))
		printf("%s: NUMERIC -Infinity\n", str);
	else
	{
		/*
		 * No portable printf specifier for a 128-bit integer is assumed
		 * here; print the two 64-bit halves instead (both have standard PG
		 * format macros), which is all a developer needs to eyeball this.
		 */
		printf("%s: NUMERIC scale=%d hi=" INT64_FORMAT " lo=" UINT64_FORMAT "\n",
			   str, numeric_pack_scale(num), num->n_hi, num->n_lo);
	}
}


/*
 * dump_var() - Dump a value in the variable format for debugging
 */
static void
dump_var(const char *str, NumericVar *var)
{
	int			i;

	printf("%s: VAR w=%d d=%d ", str, var->weight, var->dscale);
	switch (var->sign)
	{
		case NUMERIC_POS:
			printf("POS");
			break;
		case NUMERIC_NEG:
			printf("NEG");
			break;
		case NUMERIC_NAN:
			printf("NaN");
			break;
		case NUMERIC_PINF:
			printf("Infinity");
			break;
		case NUMERIC_NINF:
			printf("-Infinity");
			break;
		default:
			printf("SIGN=0x%x", var->sign);
			break;
	}

	for (i = 0; i < var->ndigits; i++)
		printf(" %0*d", DEC_DIGITS, var->digits[i]);

	printf("\n");
}
#endif							/* NUMERIC_DEBUG */


/* ----------------------------------------------------------------------
 *
 * Local functions follow
 *
 * In general, these do not support "special" (NaN or infinity) inputs;
 * callers should handle those possibilities first.
 * (There are one or two exceptions, noted in their header comments.)
 *
 * ----------------------------------------------------------------------
 */


/*
 * alloc_var() -
 *
 *	Allocate a digit buffer of ndigits digits (plus a spare digit for rounding)
 */
static void
alloc_var(NumericVar *var, int ndigits)
{
	digitbuf_free(var->buf);
	var->buf = digitbuf_alloc(ndigits + 1);
	var->buf[0] = 0;			/* spare digit for rounding */
	var->digits = var->buf + 1;
	var->ndigits = ndigits;
}


/*
 * free_var() -
 *
 *	Return the digit buffer of a variable to the free pool
 */
static void
free_var(NumericVar *var)
{
	digitbuf_free(var->buf);
	var->buf = NULL;
	var->digits = NULL;
	var->sign = NUMERIC_NAN;
}


/*
 * zero_var() -
 *
 *	Set a variable to ZERO.
 *	Note: its dscale is not touched.
 */
static void
zero_var(NumericVar *var)
{
	digitbuf_free(var->buf);
	var->buf = NULL;
	var->digits = NULL;
	var->ndigits = 0;
	var->weight = 0;			/* by convention; doesn't really matter */
	var->sign = NUMERIC_POS;	/* anything but NAN... */
}


/*
 * set_var_from_str()
 *
 *	Parse a string and put the number into a variable
 *
 * This function does not handle leading or trailing spaces.  It returns
 * the end+1 position parsed into *endptr, so that caller can check for
 * trailing spaces/garbage if deemed necessary.
 *
 * cp is the place to actually start parsing; str is what to use in error
 * reports.  (Typically cp would be the same except advanced over spaces.)
 *
 * Returns true on success, false on failure (if escontext points to an
 * ErrorSaveContext; otherwise errors are thrown).
 */
static bool
set_var_from_str(const char *str, const char *cp,
				 NumericVar *dest, const char **endptr,
				 Node *escontext)
{
	bool		have_dp = false;
	int			i;
	unsigned char *decdigits;
	int			sign = NUMERIC_POS;
	int			dweight = -1;
	int			ddigits;
	int			dscale = 0;
	int			weight;
	int			ndigits;
	int			offset;
	NumericDigit *digits;

	/*
	 * We first parse the string to extract decimal digits and determine the
	 * correct decimal weight.  Then convert to NBASE representation.
	 */
	switch (*cp)
	{
		case '+':
			sign = NUMERIC_POS;
			cp++;
			break;

		case '-':
			sign = NUMERIC_NEG;
			cp++;
			break;
	}

	if (*cp == '.')
	{
		have_dp = true;
		cp++;
	}

	if (!isdigit((unsigned char) *cp))
		goto invalid_syntax;

	decdigits = (unsigned char *) palloc(strlen(cp) + DEC_DIGITS * 2);

	/* leading padding for digit alignment later */
	memset(decdigits, 0, DEC_DIGITS);
	i = DEC_DIGITS;

	while (*cp)
	{
		if (isdigit((unsigned char) *cp))
		{
			decdigits[i++] = *cp++ - '0';
			if (!have_dp)
				dweight++;
			else
				dscale++;
		}
		else if (*cp == '.')
		{
			if (have_dp)
				goto invalid_syntax;
			have_dp = true;
			cp++;
			/* decimal point must not be followed by underscore */
			if (*cp == '_')
				goto invalid_syntax;
		}
		else if (*cp == '_')
		{
			/* underscore must be followed by more digits */
			cp++;
			if (!isdigit((unsigned char) *cp))
				goto invalid_syntax;
		}
		else
			break;
	}

	ddigits = i - DEC_DIGITS;
	/* trailing padding for digit alignment later */
	memset(decdigits + i, 0, DEC_DIGITS - 1);

	/* Handle exponent, if any */
	if (*cp == 'e' || *cp == 'E')
	{
		int64		exponent = 0;
		bool		neg = false;

		/*
		 * At this point, dweight and dscale can't be more than about
		 * INT_MAX/2 due to the MaxAllocSize limit on string length, so
		 * constraining the exponent similarly should be enough to prevent
		 * integer overflow in this function.  If the value is too large to
		 * fit in storage format, make_result() will complain about it later;
		 * for consistency use the same ereport errcode/text as make_result().
		 */

		/* exponent sign */
		cp++;
		if (*cp == '+')
			cp++;
		else if (*cp == '-')
		{
			neg = true;
			cp++;
		}

		/* exponent digits */
		if (!isdigit((unsigned char) *cp))
			goto invalid_syntax;

		while (*cp)
		{
			if (isdigit((unsigned char) *cp))
			{
				exponent = exponent * 10 + (*cp++ - '0');
				if (exponent > PG_INT32_MAX / 2)
					goto out_of_range;
			}
			else if (*cp == '_')
			{
				/* underscore must be followed by more digits */
				cp++;
				if (!isdigit((unsigned char) *cp))
					goto invalid_syntax;
			}
			else
				break;
		}

		if (neg)
			exponent = -exponent;

		dweight += (int) exponent;
		dscale -= (int) exponent;
		if (dscale < 0)
			dscale = 0;
	}

	/*
	 * Okay, convert pure-decimal representation to base NBASE.  First we need
	 * to determine the converted weight and ndigits.  offset is the number of
	 * decimal zeroes to insert before the first given digit to have a
	 * correctly aligned first NBASE digit.
	 */
	if (dweight >= 0)
		weight = (dweight + 1 + DEC_DIGITS - 1) / DEC_DIGITS - 1;
	else
		weight = -((-dweight - 1) / DEC_DIGITS + 1);
	offset = (weight + 1) * DEC_DIGITS - (dweight + 1);
	ndigits = (ddigits + offset + DEC_DIGITS - 1) / DEC_DIGITS;

	alloc_var(dest, ndigits);
	dest->sign = sign;
	dest->weight = weight;
	dest->dscale = dscale;

	i = DEC_DIGITS - offset;
	digits = dest->digits;

	while (ndigits-- > 0)
	{
#if DEC_DIGITS == 4
		*digits++ = ((decdigits[i] * 10 + decdigits[i + 1]) * 10 +
					 decdigits[i + 2]) * 10 + decdigits[i + 3];
#elif DEC_DIGITS == 2
		*digits++ = decdigits[i] * 10 + decdigits[i + 1];
#elif DEC_DIGITS == 1
		*digits++ = decdigits[i];
#else
#error unsupported NBASE
#endif
		i += DEC_DIGITS;
	}

	pfree(decdigits);

	/* Strip any leading/trailing zeroes, and normalize weight if zero */
	strip_var(dest);

	/* Return end+1 position for caller */
	*endptr = cp;

	return true;

out_of_range:
	ereturn(escontext, false,
			(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
			 errmsg("value overflows numeric format")));

invalid_syntax:
	ereturn(escontext, false,
			(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
			 errmsg("invalid input syntax for type %s: \"%s\"",
					"numeric", str)));
}


/*
 * Return the numeric value of a single hex digit.
 */
static inline int
xdigit_value(char dig)
{
	return dig >= '0' && dig <= '9' ? dig - '0' :
		dig >= 'a' && dig <= 'f' ? dig - 'a' + 10 :
		dig >= 'A' && dig <= 'F' ? dig - 'A' + 10 : -1;
}

/*
 * set_var_from_non_decimal_integer_str()
 *
 *	Parse a string containing a non-decimal integer
 *
 * This function does not handle leading or trailing spaces.  It returns
 * the end+1 position parsed into *endptr, so that caller can check for
 * trailing spaces/garbage if deemed necessary.
 *
 * cp is the place to actually start parsing; str is what to use in error
 * reports.  The number's sign and base prefix indicator (e.g., "0x") are
 * assumed to have already been parsed, so cp should point to the number's
 * first digit in the base specified.
 *
 * base is expected to be 2, 8 or 16.
 *
 * Returns true on success, false on failure (if escontext points to an
 * ErrorSaveContext; otherwise errors are thrown).
 */
static bool
set_var_from_non_decimal_integer_str(const char *str, const char *cp, int sign,
									 int base, NumericVar *dest,
									 const char **endptr, Node *escontext)
{
	const char *firstdigit = cp;
	int64		tmp;
	int64		mul;
	NumericVar	tmp_var;

	init_var(&tmp_var);

	zero_var(dest);

	/*
	 * Process input digits in groups that fit in int64.  Here "tmp" is the
	 * value of the digits in the group, and "mul" is base^n, where n is the
	 * number of digits in the group.  Thus tmp < mul, and we must start a new
	 * group when mul * base threatens to overflow PG_INT64_MAX.
	 */
	tmp = 0;
	mul = 1;

	if (base == 16)
	{
		while (*cp)
		{
			if (isxdigit((unsigned char) *cp))
			{
				if (mul > PG_INT64_MAX / 16)
				{
					/* Add the contribution from this group of digits */
					int64_to_numericvar(mul, &tmp_var);
					mul_var(dest, &tmp_var, dest, 0);
					int64_to_numericvar(tmp, &tmp_var);
					add_var(dest, &tmp_var, dest);

					/* Result will overflow if weight overflows int16 */
					if (dest->weight > NUMERIC_WEIGHT_MAX)
						goto out_of_range;

					/* Begin a new group */
					tmp = 0;
					mul = 1;
				}

				tmp = tmp * 16 + xdigit_value(*cp++);
				mul = mul * 16;
			}
			else if (*cp == '_')
			{
				/* Underscore must be followed by more digits */
				cp++;
				if (!isxdigit((unsigned char) *cp))
					goto invalid_syntax;
			}
			else
				break;
		}
	}
	else if (base == 8)
	{
		while (*cp)
		{
			if (*cp >= '0' && *cp <= '7')
			{
				if (mul > PG_INT64_MAX / 8)
				{
					/* Add the contribution from this group of digits */
					int64_to_numericvar(mul, &tmp_var);
					mul_var(dest, &tmp_var, dest, 0);
					int64_to_numericvar(tmp, &tmp_var);
					add_var(dest, &tmp_var, dest);

					/* Result will overflow if weight overflows int16 */
					if (dest->weight > NUMERIC_WEIGHT_MAX)
						goto out_of_range;

					/* Begin a new group */
					tmp = 0;
					mul = 1;
				}

				tmp = tmp * 8 + (*cp++ - '0');
				mul = mul * 8;
			}
			else if (*cp == '_')
			{
				/* Underscore must be followed by more digits */
				cp++;
				if (*cp < '0' || *cp > '7')
					goto invalid_syntax;
			}
			else
				break;
		}
	}
	else if (base == 2)
	{
		while (*cp)
		{
			if (*cp >= '0' && *cp <= '1')
			{
				if (mul > PG_INT64_MAX / 2)
				{
					/* Add the contribution from this group of digits */
					int64_to_numericvar(mul, &tmp_var);
					mul_var(dest, &tmp_var, dest, 0);
					int64_to_numericvar(tmp, &tmp_var);
					add_var(dest, &tmp_var, dest);

					/* Result will overflow if weight overflows int16 */
					if (dest->weight > NUMERIC_WEIGHT_MAX)
						goto out_of_range;

					/* Begin a new group */
					tmp = 0;
					mul = 1;
				}

				tmp = tmp * 2 + (*cp++ - '0');
				mul = mul * 2;
			}
			else if (*cp == '_')
			{
				/* Underscore must be followed by more digits */
				cp++;
				if (*cp < '0' || *cp > '1')
					goto invalid_syntax;
			}
			else
				break;
		}
	}
	else
		/* Should never happen; treat as invalid input */
		goto invalid_syntax;

	/* Check that we got at least one digit */
	if (unlikely(cp == firstdigit))
		goto invalid_syntax;

	/* Add the contribution from the final group of digits */
	int64_to_numericvar(mul, &tmp_var);
	mul_var(dest, &tmp_var, dest, 0);
	int64_to_numericvar(tmp, &tmp_var);
	add_var(dest, &tmp_var, dest);

	if (dest->weight > NUMERIC_WEIGHT_MAX)
		goto out_of_range;

	dest->sign = sign;

	free_var(&tmp_var);

	/* Return end+1 position for caller */
	*endptr = cp;

	return true;

out_of_range:
	ereturn(escontext, false,
			(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
			 errmsg("value overflows numeric format")));

invalid_syntax:
	ereturn(escontext, false,
			(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
			 errmsg("invalid input syntax for type %s: \"%s\"",
					"numeric", str)));
}


/*
 * NBASE limbs needed to hold any int128 coefficient.  An int128 spans at most
 * 39 decimal digits; the fractional part is padded up to a limb boundary
 * separately, so this only has to cover the integer part.
 */
#define NUMERIC_MAX_INT128_LIMBS	((39 + DEC_DIGITS - 1) / DEC_DIGITS)

/*
 * numeric_dec128_int128_to_var() -
 *
 *	Convert an arbitrary int128 coefficient at decimal scale 'scale' into a
 *	NumericVar, i.e. build the variable whose value is val * 10^(-scale).
 *
 *	This is the general form of the dec128 unpack, and the workhorse behind
 *	both numeric_dec128_to_var() and the int128 fast-sum accumulator.  It has
 *	to accept the whole int128 range rather than just legal mantissas, because
 *	the accumulator hands it running totals that may exceed NUMERIC_MAX_MANT --
 *	that overflow is exactly what makes the aggregate state promote.
 *
 *	It converts straight from the scaled integer to base-NBASE limbs.  An
 *	earlier version of the unpack rendered the mantissa to decimal text and
 *	reparsed it with set_var_from_str().  That was correct, but it put two
 *	string conversions and two pallocs on the hottest path in the type: every
 *	caller of init_var_from_num(), which includes do_numeric_accum() and
 *	therefore sum() and avg().  It made this build slower than stock numeric
 *	for everything the mantissa-level fast paths don't catch, which defeats the
 *	point of the experiment.
 *
 *	Why the obvious approach doesn't work is worth recording, since it is what
 *	drove the text detour in the first place: you cannot pre-multiply the whole
 *	coefficient up to a limb boundary.  Aligning a scale that isn't a multiple
 *	of DEC_DIGITS means shifting by up to DEC_DIGITS - 1 further decimal
 *	digits, and a near-39-digit value times 10^3 overflows int128.  Splitting
 *	into integer and fractional parts first sidesteps that: the fractional part
 *	is bounded by 10^NUMERIC_MAX_STORED_SCALE, so padding *it* to a limb
 *	boundary stays under 10^18 and fits an int64 with room to spare.
 */
static void
numeric_dec128_int128_to_var(int128 val, int scale, NumericVar *dest)
{
	bool		neg = (val < 0);
	uint128		umant;
	uint128		ipart;
	uint64		fpart;
	int			nint_limbs = 0;
	int			nfrac_limbs;
	int			pad;
	NumericDigit int_limbs[NUMERIC_MAX_INT128_LIMBS];
	NumericDigit *dptr;
	int			i;

	Assert(scale >= 0 && scale <= NUMERIC_MAX_STORED_SCALE);

	/*
	 * Negate in unsigned arithmetic: -val would be undefined for INT128_MIN,
	 * which is reachable here since callers may pass any int128.
	 */
	umant = neg ? (~(uint128) val) + 1 : (uint128) val;

	init_var(dest);
	dest->dscale = scale;

	if (umant == 0)
	{
		/* strip_var()'s canonical zero, but preserving the display scale */
		dest->ndigits = 0;
		dest->weight = 0;
		dest->sign = NUMERIC_POS;
		return;
	}

	dest->sign = neg ? NUMERIC_NEG : NUMERIC_POS;

	nfrac_limbs = (scale + DEC_DIGITS - 1) / DEC_DIGITS;
	pad = nfrac_limbs * DEC_DIGITS - scale;

	/*
	 * Fast path: if the whole coefficient, aligned up to a limb boundary, fits
	 * a uint64, extract every limb in 64-bit arithmetic.  This is worth a
	 * separate path because dividing a uint128 by a *variable* power of ten
	 * compiles to a libgcc __udivti3 call, whereas these divisions are by the
	 * compile-time constant NBASE and get strength-reduced into multiplies.
	 * Anything up to 16 significant digits lands here, which covers ordinary
	 * money and measurement columns; only genuinely wide values fall through.
	 */
	if (umant <= (uint128) (PG_UINT64_MAX / numeric_dec128_pow10_u64[pad]))
	{
		uint64		u = (uint64) umant * numeric_dec128_pow10_u64[pad];
		int			n = 0;

		while (u != 0)
		{
			Assert(n < NUMERIC_MAX_INT128_LIMBS);
			int_limbs[n++] = (NumericDigit) (u % NBASE);
			u /= NBASE;
		}

		alloc_var(dest, n);

		/*
		 * n counts limbs from the least significant end, and nfrac_limbs of
		 * them are fractional, so the leading limb's weight follows directly.
		 * It goes negative for a value below 1, which is correct.
		 */
		dest->weight = n - 1 - nfrac_limbs;

		dptr = dest->digits;
		for (i = n - 1; i >= 0; i--)
			*dptr++ = int_limbs[i];

		strip_var(dest);
		return;
	}

	ipart = umant / (uint128) numeric_dec128_pow10[scale];
	fpart = (uint64) (umant % (uint128) numeric_dec128_pow10[scale]);

	/*
	 * Pad the fraction on the right so it fills a whole number of limbs.  See
	 * the function comment for why this multiply is safe here and would not
	 * have been on the undivided mantissa.
	 */
	fpart *= numeric_dec128_pow10_u64[pad];

	/*
	 * Integer limbs, least significant first; the top one is never zero.  Drop
	 * into 64-bit arithmetic as soon as the remaining value fits, for the same
	 * strength-reduction reason as the fast path above.
	 */
	while ((ipart >> 64) != 0)
	{
		Assert(nint_limbs < NUMERIC_MAX_INT128_LIMBS);
		int_limbs[nint_limbs++] = (NumericDigit) (ipart % NBASE);
		ipart /= NBASE;
	}
	{
		uint64		u = (uint64) ipart;

		while (u != 0)
		{
			Assert(nint_limbs < NUMERIC_MAX_INT128_LIMBS);
			int_limbs[nint_limbs++] = (NumericDigit) (u % NBASE);
			u /= NBASE;
		}
	}

	alloc_var(dest, nint_limbs + nfrac_limbs);

	/*
	 * A value below 1 has no integer limbs at all, and its first digit is the
	 * leading fractional limb, whose weight is -1.
	 */
	dest->weight = nint_limbs - 1;

	dptr = dest->digits;
	for (i = nint_limbs - 1; i >= 0; i--)
		*dptr++ = int_limbs[i];
	for (i = nfrac_limbs - 1; i >= 0; i--)
	{
		dptr[i] = (NumericDigit) (fpart % NBASE);
		fpart /= NBASE;
	}

	/*
	 * Leading zero limbs are normal here (0.00001234 at scale 8 produces
	 * limbs 0000,1234), and trailing zero limbs come from padding; strip_var()
	 * handles both and fixes up weight.  dscale is left alone -- it records
	 * the display scale, not the number of stored digits.
	 */
	strip_var(dest);
}

/*
 * numeric_dec128_to_var() -
 *
 *	Point of coupling #1 (SPEC-numeric-as-dec128.md sec. 3.4): decode a packed
 *	dec128 value into a NumericVar.  Caller must not call this on a special
 *	value -- the sentinel mantissas above NUMERIC_MAX_MANT are not numbers, and
 *	converting one would silently produce a huge finite value.
 */
static void
numeric_dec128_to_var(Numeric num, NumericVar *dest)
{
	Assert(!NUMERIC_IS_SPECIAL(num));

	numeric_dec128_int128_to_var(numeric_pack_mant(num),
								 numeric_pack_scale(num), dest);
}

/*
 * set_var_from_num() -
 *
 *	Convert the packed db format into a variable
 */
static void
set_var_from_num(Numeric num, NumericVar *dest)
{
	numeric_dec128_to_var(num, dest);
}


/*
 * init_var_from_num() -
 *
 *	Initialize a variable from packed db format.
 *
 *	NOTE: unlike the classic packed format, dec128 has no digit array to
 *	point into, so (unlike the historical version of this function) this
 *	always allocates a fresh digit buffer.  It is therefore safe -- indeed
 *	necessary -- to free_var() the result once it is no longer needed, same
 *	as any other NumericVar obtained from set_var_from_num().
 */
static void
init_var_from_num(Numeric num, NumericVar *dest)
{
	numeric_dec128_to_var(num, dest);
}


/*
 * set_var_from_var() -
 *
 *	Copy one variable into another
 */
static void
set_var_from_var(const NumericVar *value, NumericVar *dest)
{
	NumericDigit *newbuf;

	newbuf = digitbuf_alloc(value->ndigits + 1);
	newbuf[0] = 0;				/* spare digit for rounding */
	if (value->ndigits > 0)		/* else value->digits might be null */
		memcpy(newbuf + 1, value->digits,
			   value->ndigits * sizeof(NumericDigit));

	digitbuf_free(dest->buf);

	memmove(dest, value, sizeof(NumericVar));
	dest->buf = newbuf;
	dest->digits = newbuf + 1;
}


/*
 * get_str_from_var() -
 *
 *	Convert a var to text representation (guts of numeric_out).
 *	The var is displayed to the number of digits indicated by its dscale.
 *	Returns a palloc'd string.
 */
static char *
get_str_from_var(const NumericVar *var)
{
	int			dscale;
	char	   *str;
	char	   *cp;
	char	   *endcp;
	int			i;
	int			d;
	NumericDigit dig;

#if DEC_DIGITS > 1
	NumericDigit d1;
#endif

	dscale = var->dscale;

	/*
	 * Allocate space for the result.
	 *
	 * i is set to the # of decimal digits before decimal point. dscale is the
	 * # of decimal digits we will print after decimal point. We may generate
	 * as many as DEC_DIGITS-1 excess digits at the end, and in addition we
	 * need room for sign, decimal point, null terminator.
	 */
	i = (var->weight + 1) * DEC_DIGITS;
	if (i <= 0)
		i = 1;

	str = palloc(i + dscale + DEC_DIGITS + 2);
	cp = str;

	/*
	 * Output a dash for negative values
	 */
	if (var->sign == NUMERIC_NEG)
		*cp++ = '-';

	/*
	 * Output all digits before the decimal point
	 */
	if (var->weight < 0)
	{
		d = var->weight + 1;
		*cp++ = '0';
	}
	else
	{
		for (d = 0; d <= var->weight; d++)
		{
			dig = (d < var->ndigits) ? var->digits[d] : 0;
			/* In the first digit, suppress extra leading decimal zeroes */
#if DEC_DIGITS == 4
			{
				bool		putit = (d > 0);

				d1 = dig / 1000;
				dig -= d1 * 1000;
				putit |= (d1 > 0);
				if (putit)
					*cp++ = d1 + '0';
				d1 = dig / 100;
				dig -= d1 * 100;
				putit |= (d1 > 0);
				if (putit)
					*cp++ = d1 + '0';
				d1 = dig / 10;
				dig -= d1 * 10;
				putit |= (d1 > 0);
				if (putit)
					*cp++ = d1 + '0';
				*cp++ = dig + '0';
			}
#elif DEC_DIGITS == 2
			d1 = dig / 10;
			dig -= d1 * 10;
			if (d1 > 0 || d > 0)
				*cp++ = d1 + '0';
			*cp++ = dig + '0';
#elif DEC_DIGITS == 1
			*cp++ = dig + '0';
#else
#error unsupported NBASE
#endif
		}
	}

	/*
	 * If requested, output a decimal point and all the digits that follow it.
	 * We initially put out a multiple of DEC_DIGITS digits, then truncate if
	 * needed.
	 */
	if (dscale > 0)
	{
		*cp++ = '.';
		endcp = cp + dscale;
		for (i = 0; i < dscale; d++, i += DEC_DIGITS)
		{
			dig = (d >= 0 && d < var->ndigits) ? var->digits[d] : 0;
#if DEC_DIGITS == 4
			d1 = dig / 1000;
			dig -= d1 * 1000;
			*cp++ = d1 + '0';
			d1 = dig / 100;
			dig -= d1 * 100;
			*cp++ = d1 + '0';
			d1 = dig / 10;
			dig -= d1 * 10;
			*cp++ = d1 + '0';
			*cp++ = dig + '0';
#elif DEC_DIGITS == 2
			d1 = dig / 10;
			dig -= d1 * 10;
			*cp++ = d1 + '0';
			*cp++ = dig + '0';
#elif DEC_DIGITS == 1
			*cp++ = dig + '0';
#else
#error unsupported NBASE
#endif
		}
		cp = endcp;
	}

	/*
	 * terminate the string and return it
	 */
	*cp = '\0';
	return str;
}

/*
 * get_str_from_var_sci() -
 *
 *	Convert a var to a normalised scientific notation text representation.
 *	This function does the heavy lifting for numeric_out_sci().
 *
 *	This notation has the general form a * 10^b, where a is known as the
 *	"significand" and b is known as the "exponent".
 *
 *	Because we can't do superscript in ASCII (and because we want to copy
 *	printf's behaviour) we display the exponent using E notation, with a
 *	minimum of two exponent digits.
 *
 *	For example, the value 1234 could be output as 1.2e+03.
 *
 *	We assume that the exponent can fit into an int32.
 *
 *	rscale is the number of decimal digits desired after the decimal point in
 *	the output, negative values will be treated as meaning zero.
 *
 *	Returns a palloc'd string.
 */
static char *
get_str_from_var_sci(const NumericVar *var, int rscale)
{
	int32		exponent;
	NumericVar	tmp_var;
	size_t		len;
	char	   *str;
	char	   *sig_out;

	if (rscale < 0)
		rscale = 0;

	/*
	 * Determine the exponent of this number in normalised form.
	 *
	 * This is the exponent required to represent the number with only one
	 * significant digit before the decimal place.
	 */
	if (var->ndigits > 0)
	{
		exponent = (var->weight + 1) * DEC_DIGITS;

		/*
		 * Compensate for leading decimal zeroes in the first numeric digit by
		 * decrementing the exponent.
		 */
		exponent -= DEC_DIGITS - (int) log10(var->digits[0]);
	}
	else
	{
		/*
		 * If var has no digits, then it must be zero.
		 *
		 * Zero doesn't technically have a meaningful exponent in normalised
		 * notation, but we just display the exponent as zero for consistency
		 * of output.
		 */
		exponent = 0;
	}

	/*
	 * Divide var by 10^exponent to get the significand, rounding to rscale
	 * decimal digits in the process.
	 */
	init_var(&tmp_var);

	power_ten_int(exponent, &tmp_var);
	div_var(var, &tmp_var, &tmp_var, rscale, true, true);
	sig_out = get_str_from_var(&tmp_var);

	free_var(&tmp_var);

	/*
	 * Allocate space for the result.
	 *
	 * In addition to the significand, we need room for the exponent
	 * decoration ("e"), the sign of the exponent, up to 10 digits for the
	 * exponent itself, and of course the null terminator.
	 */
	len = strlen(sig_out) + 13;
	str = palloc(len);
	snprintf(str, len, "%se%+03d", sig_out, exponent);

	pfree(sig_out);

	return str;
}


/*
 * numericvar_serialize - serialize NumericVar to binary format
 *
 * At variable level, no checks are performed on the weight or dscale, allowing
 * us to pass around intermediate values with higher precision than supported
 * by the numeric type.  Note: this is incompatible with numeric_send/recv(),
 * which use 16-bit integers for these fields.
 */
static void
numericvar_serialize(StringInfo buf, const NumericVar *var)
{
	int			i;

	pq_sendint32(buf, var->ndigits);
	pq_sendint32(buf, var->weight);
	pq_sendint32(buf, var->sign);
	pq_sendint32(buf, var->dscale);
	for (i = 0; i < var->ndigits; i++)
		pq_sendint16(buf, var->digits[i]);
}

/*
 * numericvar_deserialize - deserialize binary format to NumericVar
 */
static void
numericvar_deserialize(StringInfo buf, NumericVar *var)
{
	int			len,
				i;

	len = pq_getmsgint(buf, sizeof(int32));

	alloc_var(var, len);		/* sets var->ndigits */

	var->weight = pq_getmsgint(buf, sizeof(int32));
	var->sign = pq_getmsgint(buf, sizeof(int32));
	var->dscale = pq_getmsgint(buf, sizeof(int32));
	for (i = 0; i < len; i++)
		var->digits[i] = pq_getmsgint(buf, sizeof(int16));
}


/*
 * duplicate_numeric() - copy a packed-format Numeric
 *
 * This will handle NaN and Infinity cases.
 */
static Numeric
duplicate_numeric(Numeric num)
{
	Numeric		res;

	res = (Numeric) palloc(sizeof(NumericData));
	memcpy(res, num, sizeof(NumericData));
	return res;
}

/*
 * numeric_var_to_dec128() -
 *
 *	Point of coupling #2 (SPEC-numeric-as-dec128.md sec. 3.4): pack a
 *	NumericVar into a dec128 value.  Renders the variable to decimal text
 *	with the existing get_str_from_var() (the same routine numeric_out()
 *	uses, which always pads to exactly var->dscale fractional digits) and
 *	reparses that text digit-by-digit, mirroring dec128_in()'s parser in the
 *	dec64-prototype extension.  Going through text avoids the overflow trap
 *	of a limb-boundary-padding multiply (aligning a scale that isn't a
 *	multiple of DEC_DIGITS can require shifting a near-37-digit mantissa by
 *	up to 3 more decimal digits, which does not fit in int128); a direct
 *	digit-limb fast path is left for the later acceleration pass.
 *
 *	A scale greater than NUMERIC_MAX_STORED_SCALE (15) is *not* treated as
 *	an overflow: it is rounded away, the same way apply_typmod() rounds a
 *	value down to a target typmod's scale.  This matches the documented
 *	"division scale diverges from stock numeric" regression
 *	(SPEC-numeric-as-dec128.md sec. 3.3) -- callers like numeric_div(),
 *	the variance/stddev finalizers, and float8-to-numeric conversion
 *	routinely produce more fractional digits than dec128 can store, and
 *	should lose precision gracefully rather than fail outright.
 *
 *	On genuine overflow (more than 37 significant digits, even after that
 *	rounding): if have_error is non-NULL, sets *have_error and leaves *out
 *	untouched; otherwise raises ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, matching
 *	the historical int16-header-overflow behaviour this replaces.
 *
 *	Like numeric_dec128_to_var(), this used to go through decimal text --
 *	get_str_from_var() followed by a digit-by-digit reparse -- and for the same
 *	reason: a limb-boundary alignment multiply can overflow int128.  Here the
 *	fix is to round the variable down to the target scale *first*, with the
 *	existing round_var(), which bounds the leftover misalignment to at most
 *	DEC_DIGITS - 1 digits.  After that the conversion is a plain Horner loop
 *	over the limbs with overflow checks.
 */
static void
numeric_var_to_dec128(const NumericVar *var, NumericData *out, bool *have_error)
{
	int128		mant = 0;
	int			sign = var->sign;
	int			ts;
	int			stored_scale;
	int			cur_scale;
	int			i;
	bool		overflow = false;
	NumericVar	rounded;
	bool		have_rounded = false;

	Assert(sign == NUMERIC_POS || sign == NUMERIC_NEG);

	ts = Min(var->dscale, NUMERIC_MAX_STORED_SCALE);

	/*
	 * Round away anything we can't keep.  Two separate reasons to get here:
	 * the display scale exceeds what dec128 stores (the documented division
	 * regression), or the variable physically carries more fractional digits
	 * than its own dscale advertises.  The latter shouldn't happen -- dscale
	 * is documented never to be less than the number of stored digits -- but
	 * handling it costs one comparison and keeps the Horner loop below from
	 * having to cope with an unbounded misalignment.
	 */
	stored_scale = DEC_DIGITS * (var->ndigits - 1 - var->weight);
	if (var->dscale > ts || stored_scale > ts)
	{
		init_var(&rounded);
		set_var_from_var(var, &rounded);
		round_var(&rounded, ts);
		have_rounded = true;
		var = &rounded;
	}

	/*
	 * Accumulate the limbs.  The digits are all non-negative, so this builds
	 * the magnitude; the sign is applied at the very end.
	 */
	for (i = 0; i < var->ndigits; i++)
	{
		if (unlikely(__builtin_mul_overflow(mant, (int128) NBASE, &mant)) ||
			unlikely(__builtin_add_overflow(mant, (int128) var->digits[i],
											&mant)))
		{
			overflow = true;
			break;
		}
	}

	/*
	 * mant now represents the value scaled by 10^cur_scale.  Note this can be
	 * negative, for a value whose limbs all sit above the decimal point.
	 */
	cur_scale = DEC_DIGITS * (var->ndigits - 1 - var->weight);

	if (unlikely(overflow))
	{
		/* nothing more to do; fall through to the error path below */
	}
	else if (mant == 0)
	{
		/* zero at any scale is just zero; skip the alignment arithmetic */
	}
	else if (cur_scale < ts)
	{
		if (unlikely(!numeric_dec128_try_scale_up(mant, ts - cur_scale, &mant)))
			overflow = true;
	}
	else if (cur_scale > ts)
	{
		int128		divisor;
		int128		rem;

		/*
		 * Only the tail of a partially-filled limb can be left over here,
		 * because we rounded to ts above -- so this drop is at most
		 * DEC_DIGITS - 1 digits and those digits are zeroes.  Round half away
		 * from zero regardless, to match round_var() rather than silently
		 * truncating if that assumption ever stops holding.
		 */
		Assert(cur_scale - ts < DEC_DIGITS);
		divisor = numeric_dec128_pow10[cur_scale - ts];
		rem = mant % divisor;
		mant /= divisor;
		if (rem > 0 && rem * 2 >= divisor)
			mant++;
		else if (rem < 0 && (-rem) * 2 >= divisor)
			mant--;
	}

	if (!overflow &&
		(mant > NUMERIC_MAX_MANT || mant < -NUMERIC_MAX_MANT))
		overflow = true;

	/*
	 * Release the working copy before the error path, so that an ereport()
	 * here doesn't strand it until the surrounding context is reset.
	 */
	if (have_rounded)
		free_var(&rounded);

	if (unlikely(overflow))
	{
		if (have_error)
		{
			*have_error = true;
			return;
		}
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("value overflows numeric format")));
	}

	numeric_pack_store(out, sign == NUMERIC_NEG ? -mant : mant, ts);
}

/*
 * make_result_opt_error() -
 *
 *	Create the packed dec128 numeric format in palloc()'d memory from
 *	a variable.  This will handle NaN and Infinity cases.
 *
 *	If "have_error" isn't NULL, on overflow *have_error is set to true and
 *	NULL is returned.  This is helpful when caller needs to handle errors.
 */
static Numeric
make_result_opt_error(const NumericVar *var, bool *have_error)
{
	Numeric		result;
	int			sign = var->sign;

	if (have_error)
		*have_error = false;

	result = (Numeric) palloc(sizeof(NumericData));

	if ((sign & NUMERIC_SIGN_MASK) == NUMERIC_SPECIAL)
	{
		/*
		 * Verify valid special value.  This could be just an Assert, perhaps,
		 * but it seems worthwhile to expend a few cycles to ensure that we
		 * never write any nonzero reserved bits to disk.
		 */
		int128		mant;

		if (sign == NUMERIC_NAN)
			mant = NUMERIC_NAN_MANT;
		else if (sign == NUMERIC_PINF)
			mant = NUMERIC_PINF_MANT;
		else if (sign == NUMERIC_NINF)
			mant = NUMERIC_NINF_MANT;
		else
		{
			elog(ERROR, "invalid numeric sign value 0x%x", sign);
			mant = 0;			/* keep compiler quiet */
		}

		numeric_pack_store(result, mant, 0);

		dump_numeric("make_result()", result);
		return result;
	}

	/*
	 * numeric_var_to_dec128() renders through get_str_from_var(), the same
	 * routine numeric_out() uses, so it handles zero (however many, or few,
	 * zero digits var happens to carry -- the rendered text is "0" or
	 * "0.000..." either way) and the sign-of-zero-is-positive convention
	 * without a separate case here.
	 */
	numeric_var_to_dec128(var, result, have_error);
	if (have_error && *have_error)
		return NULL;

	dump_numeric("make_result()", result);
	return result;
}


/*
 * make_result() -
 *
 *	An interface to make_result_opt_error() without "have_error" argument.
 */
static Numeric
make_result(const NumericVar *var)
{
	return make_result_opt_error(var, NULL);
}


/*
 * apply_typmod() -
 *
 *	Do bounds checking and rounding according to the specified typmod.
 *	Note that this is only applied to normal finite values.
 *
 * Returns true on success, false on failure (if escontext points to an
 * ErrorSaveContext; otherwise errors are thrown).
 */
static bool
apply_typmod(NumericVar *var, int32 typmod, Node *escontext)
{
	int			precision;
	int			scale;
	int			maxdigits;
	int			ddigits;
	int			i;

	/* Do nothing if we have an invalid typmod */
	if (!is_valid_numeric_typmod(typmod))
		return true;

	precision = numeric_typmod_precision(typmod);
	scale = numeric_typmod_scale(typmod);
	maxdigits = precision - scale;

	/* Round to target scale (and set var->dscale) */
	round_var(var, scale);

	/* but don't allow var->dscale to be negative */
	if (var->dscale < 0)
		var->dscale = 0;

	/*
	 * Check for overflow - note we can't do this before rounding, because
	 * rounding could raise the weight.  Also note that the var's weight could
	 * be inflated by leading zeroes, which will be stripped before storage
	 * but perhaps might not have been yet. In any case, we must recognize a
	 * true zero, whose weight doesn't mean anything.
	 */
	ddigits = (var->weight + 1) * DEC_DIGITS;
	if (ddigits > maxdigits)
	{
		/* Determine true weight; and check for all-zero result */
		for (i = 0; i < var->ndigits; i++)
		{
			NumericDigit dig = var->digits[i];

			if (dig)
			{
				/* Adjust for any high-order decimal zero digits */
#if DEC_DIGITS == 4
				if (dig < 10)
					ddigits -= 3;
				else if (dig < 100)
					ddigits -= 2;
				else if (dig < 1000)
					ddigits -= 1;
#elif DEC_DIGITS == 2
				if (dig < 10)
					ddigits -= 1;
#elif DEC_DIGITS == 1
				/* no adjustment */
#else
#error unsupported NBASE
#endif
				if (ddigits > maxdigits)
					ereturn(escontext, false,
							(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
							 errmsg("numeric field overflow"),
							 errdetail("A field with precision %d, scale %d must round to an absolute value less than %s%d.",
									   precision, scale,
					/* Display 10^0 as 1 */
									   maxdigits ? "10^" : "",
									   maxdigits ? maxdigits : 1
									   )));
				break;
			}
			ddigits -= DEC_DIGITS;
		}
	}

	return true;
}

/*
 * apply_typmod_special() -
 *
 *	Do bounds checking according to the specified typmod, for an Inf or NaN.
 *	For convenience of most callers, the value is presented in packed form.
 *
 * Returns true on success, false on failure (if escontext points to an
 * ErrorSaveContext; otherwise errors are thrown).
 */
static bool
apply_typmod_special(Numeric num, int32 typmod, Node *escontext)
{
	int			precision;
	int			scale;

	Assert(NUMERIC_IS_SPECIAL(num));	/* caller error if not */

	/*
	 * NaN is allowed regardless of the typmod; that's rather dubious perhaps,
	 * but it's a longstanding behavior.  Inf is rejected if we have any
	 * typmod restriction, since an infinity shouldn't be claimed to fit in
	 * any finite number of digits.
	 */
	if (NUMERIC_IS_NAN(num))
		return true;

	/* Do nothing if we have a default typmod (-1) */
	if (!is_valid_numeric_typmod(typmod))
		return true;

	precision = numeric_typmod_precision(typmod);
	scale = numeric_typmod_scale(typmod);

	ereturn(escontext, false,
			(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
			 errmsg("numeric field overflow"),
			 errdetail("A field with precision %d, scale %d cannot hold an infinite value.",
					   precision, scale)));
}


/*
 * Convert numeric to int8, rounding if needed.
 *
 * If overflow, return false (no error is raised).  Return true if okay.
 */
static bool
numericvar_to_int64(const NumericVar *var, int64 *result)
{
	NumericDigit *digits;
	int			ndigits;
	int			weight;
	int			i;
	int64		val;
	bool		neg;
	NumericVar	rounded;

	/* Round to nearest integer */
	init_var(&rounded);
	set_var_from_var(var, &rounded);
	round_var(&rounded, 0);

	/* Check for zero input */
	strip_var(&rounded);
	ndigits = rounded.ndigits;
	if (ndigits == 0)
	{
		*result = 0;
		free_var(&rounded);
		return true;
	}

	/*
	 * For input like 10000000000, we must treat stripped digits as real. So
	 * the loop assumes there are weight+1 digits before the decimal point.
	 */
	weight = rounded.weight;
	Assert(weight >= 0 && ndigits <= weight + 1);

	/*
	 * Construct the result. To avoid issues with converting a value
	 * corresponding to INT64_MIN (which can't be represented as a positive 64
	 * bit two's complement integer), accumulate value as a negative number.
	 */
	digits = rounded.digits;
	neg = (rounded.sign == NUMERIC_NEG);
	val = -digits[0];
	for (i = 1; i <= weight; i++)
	{
		if (unlikely(pg_mul_s64_overflow(val, NBASE, &val)))
		{
			free_var(&rounded);
			return false;
		}

		if (i < ndigits)
		{
			if (unlikely(pg_sub_s64_overflow(val, digits[i], &val)))
			{
				free_var(&rounded);
				return false;
			}
		}
	}

	free_var(&rounded);

	if (!neg)
	{
		if (unlikely(val == PG_INT64_MIN))
			return false;
		val = -val;
	}
	*result = val;

	return true;
}

/*
 * Convert int8 value to numeric.
 */
static void
int64_to_numericvar(int64 val, NumericVar *var)
{
	uint64		uval,
				newuval;
	NumericDigit *ptr;
	int			ndigits;

	/* int64 can require at most 19 decimal digits; add one for safety */
	alloc_var(var, 20 / DEC_DIGITS);
	if (val < 0)
	{
		var->sign = NUMERIC_NEG;
		uval = pg_abs_s64(val);
	}
	else
	{
		var->sign = NUMERIC_POS;
		uval = val;
	}
	var->dscale = 0;
	if (val == 0)
	{
		var->ndigits = 0;
		var->weight = 0;
		return;
	}
	ptr = var->digits + var->ndigits;
	ndigits = 0;
	do
	{
		ptr--;
		ndigits++;
		newuval = uval / NBASE;
		*ptr = uval - newuval * NBASE;
		uval = newuval;
	} while (uval);
	var->digits = ptr;
	var->ndigits = ndigits;
	var->weight = ndigits - 1;
}

/*
 * Convert numeric to uint64, rounding if needed.
 *
 * If overflow, return false (no error is raised).  Return true if okay.
 */
static bool
numericvar_to_uint64(const NumericVar *var, uint64 *result)
{
	NumericDigit *digits;
	int			ndigits;
	int			weight;
	int			i;
	uint64		val;
	NumericVar	rounded;

	/* Round to nearest integer */
	init_var(&rounded);
	set_var_from_var(var, &rounded);
	round_var(&rounded, 0);

	/* Check for zero input */
	strip_var(&rounded);
	ndigits = rounded.ndigits;
	if (ndigits == 0)
	{
		*result = 0;
		free_var(&rounded);
		return true;
	}

	/* Check for negative input */
	if (rounded.sign == NUMERIC_NEG)
	{
		free_var(&rounded);
		return false;
	}

	/*
	 * For input like 10000000000, we must treat stripped digits as real. So
	 * the loop assumes there are weight+1 digits before the decimal point.
	 */
	weight = rounded.weight;
	Assert(weight >= 0 && ndigits <= weight + 1);

	/* Construct the result */
	digits = rounded.digits;
	val = digits[0];
	for (i = 1; i <= weight; i++)
	{
		if (unlikely(pg_mul_u64_overflow(val, NBASE, &val)))
		{
			free_var(&rounded);
			return false;
		}

		if (i < ndigits)
		{
			if (unlikely(pg_add_u64_overflow(val, digits[i], &val)))
			{
				free_var(&rounded);
				return false;
			}
		}
	}

	free_var(&rounded);

	*result = val;

	return true;
}

#ifdef HAVE_INT128
/*
 * Convert numeric to int128, rounding if needed.
 *
 * If overflow, return false (no error is raised).  Return true if okay.
 */
static bool
numericvar_to_int128(const NumericVar *var, int128 *result)
{
	NumericDigit *digits;
	int			ndigits;
	int			weight;
	int			i;
	int128		val,
				oldval;
	bool		neg;
	NumericVar	rounded;

	/* Round to nearest integer */
	init_var(&rounded);
	set_var_from_var(var, &rounded);
	round_var(&rounded, 0);

	/* Check for zero input */
	strip_var(&rounded);
	ndigits = rounded.ndigits;
	if (ndigits == 0)
	{
		*result = 0;
		free_var(&rounded);
		return true;
	}

	/*
	 * For input like 10000000000, we must treat stripped digits as real. So
	 * the loop assumes there are weight+1 digits before the decimal point.
	 */
	weight = rounded.weight;
	Assert(weight >= 0 && ndigits <= weight + 1);

	/* Construct the result */
	digits = rounded.digits;
	neg = (rounded.sign == NUMERIC_NEG);
	val = digits[0];
	for (i = 1; i <= weight; i++)
	{
		oldval = val;
		val *= NBASE;
		if (i < ndigits)
			val += digits[i];

		/*
		 * The overflow check is a bit tricky because we want to accept
		 * INT128_MIN, which will overflow the positive accumulator.  We can
		 * detect this case easily though because INT128_MIN is the only
		 * nonzero value for which -val == val (on a two's complement machine,
		 * anyway).
		 */
		if ((val / NBASE) != oldval)	/* possible overflow? */
		{
			if (!neg || (-val) != val || val == 0 || oldval < 0)
			{
				free_var(&rounded);
				return false;
			}
		}
	}

	free_var(&rounded);

	*result = neg ? -val : val;
	return true;
}

/*
 * Convert 128 bit integer to numeric.
 */
static void
int128_to_numericvar(int128 val, NumericVar *var)
{
	uint128		uval,
				newuval;
	NumericDigit *ptr;
	int			ndigits;

	/* int128 can require at most 39 decimal digits; add one for safety */
	alloc_var(var, 40 / DEC_DIGITS);
	if (val < 0)
	{
		var->sign = NUMERIC_NEG;
		uval = -val;
	}
	else
	{
		var->sign = NUMERIC_POS;
		uval = val;
	}
	var->dscale = 0;
	if (val == 0)
	{
		var->ndigits = 0;
		var->weight = 0;
		return;
	}
	ptr = var->digits + var->ndigits;
	ndigits = 0;
	do
	{
		ptr--;
		ndigits++;
		newuval = uval / NBASE;
		*ptr = uval - newuval * NBASE;
		uval = newuval;
	} while (uval);
	var->digits = ptr;
	var->ndigits = ndigits;
	var->weight = ndigits - 1;
}
#endif

/*
 * Convert a NumericVar to float8; if out of range, return +/- HUGE_VAL
 */
static double
numericvar_to_double_no_overflow(const NumericVar *var)
{
	char	   *tmp;
	double		val;
	char	   *endptr;

	tmp = get_str_from_var(var);

	/* unlike float8in, we ignore ERANGE from strtod */
	val = strtod(tmp, &endptr);
	if (*endptr != '\0')
	{
		/* shouldn't happen ... */
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type %s: \"%s\"",
						"double precision", tmp)));
	}

	pfree(tmp);

	return val;
}


/*
 * cmp_var() -
 *
 *	Compare two values on variable level.  We assume zeroes have been
 *	truncated to no digits.
 */
static int
cmp_var(const NumericVar *var1, const NumericVar *var2)
{
	return cmp_var_common(var1->digits, var1->ndigits,
						  var1->weight, var1->sign,
						  var2->digits, var2->ndigits,
						  var2->weight, var2->sign);
}

/*
 * cmp_var_common() -
 *
 *	Main routine of cmp_var(). This function can be used by both
 *	NumericVar and Numeric.
 */
static int
cmp_var_common(const NumericDigit *var1digits, int var1ndigits,
			   int var1weight, int var1sign,
			   const NumericDigit *var2digits, int var2ndigits,
			   int var2weight, int var2sign)
{
	if (var1ndigits == 0)
	{
		if (var2ndigits == 0)
			return 0;
		if (var2sign == NUMERIC_NEG)
			return 1;
		return -1;
	}
	if (var2ndigits == 0)
	{
		if (var1sign == NUMERIC_POS)
			return 1;
		return -1;
	}

	if (var1sign == NUMERIC_POS)
	{
		if (var2sign == NUMERIC_NEG)
			return 1;
		return cmp_abs_common(var1digits, var1ndigits, var1weight,
							  var2digits, var2ndigits, var2weight);
	}

	if (var2sign == NUMERIC_POS)
		return -1;

	return cmp_abs_common(var2digits, var2ndigits, var2weight,
						  var1digits, var1ndigits, var1weight);
}


/*
 * add_var() -
 *
 *	Full version of add functionality on variable level (handling signs).
 *	result might point to one of the operands too without danger.
 */
static void
add_var(const NumericVar *var1, const NumericVar *var2, NumericVar *result)
{
	/*
	 * Decide on the signs of the two variables what to do
	 */
	if (var1->sign == NUMERIC_POS)
	{
		if (var2->sign == NUMERIC_POS)
		{
			/*
			 * Both are positive result = +(ABS(var1) + ABS(var2))
			 */
			add_abs(var1, var2, result);
			result->sign = NUMERIC_POS;
		}
		else
		{
			/*
			 * var1 is positive, var2 is negative Must compare absolute values
			 */
			switch (cmp_abs(var1, var2))
			{
				case 0:
					/* ----------
					 * ABS(var1) == ABS(var2)
					 * result = ZERO
					 * ----------
					 */
					zero_var(result);
					result->dscale = Max(var1->dscale, var2->dscale);
					break;

				case 1:
					/* ----------
					 * ABS(var1) > ABS(var2)
					 * result = +(ABS(var1) - ABS(var2))
					 * ----------
					 */
					sub_abs(var1, var2, result);
					result->sign = NUMERIC_POS;
					break;

				case -1:
					/* ----------
					 * ABS(var1) < ABS(var2)
					 * result = -(ABS(var2) - ABS(var1))
					 * ----------
					 */
					sub_abs(var2, var1, result);
					result->sign = NUMERIC_NEG;
					break;
			}
		}
	}
	else
	{
		if (var2->sign == NUMERIC_POS)
		{
			/* ----------
			 * var1 is negative, var2 is positive
			 * Must compare absolute values
			 * ----------
			 */
			switch (cmp_abs(var1, var2))
			{
				case 0:
					/* ----------
					 * ABS(var1) == ABS(var2)
					 * result = ZERO
					 * ----------
					 */
					zero_var(result);
					result->dscale = Max(var1->dscale, var2->dscale);
					break;

				case 1:
					/* ----------
					 * ABS(var1) > ABS(var2)
					 * result = -(ABS(var1) - ABS(var2))
					 * ----------
					 */
					sub_abs(var1, var2, result);
					result->sign = NUMERIC_NEG;
					break;

				case -1:
					/* ----------
					 * ABS(var1) < ABS(var2)
					 * result = +(ABS(var2) - ABS(var1))
					 * ----------
					 */
					sub_abs(var2, var1, result);
					result->sign = NUMERIC_POS;
					break;
			}
		}
		else
		{
			/* ----------
			 * Both are negative
			 * result = -(ABS(var1) + ABS(var2))
			 * ----------
			 */
			add_abs(var1, var2, result);
			result->sign = NUMERIC_NEG;
		}
	}
}


/*
 * sub_var() -
 *
 *	Full version of sub functionality on variable level (handling signs).
 *	result might point to one of the operands too without danger.
 */
static void
sub_var(const NumericVar *var1, const NumericVar *var2, NumericVar *result)
{
	/*
	 * Decide on the signs of the two variables what to do
	 */
	if (var1->sign == NUMERIC_POS)
	{
		if (var2->sign == NUMERIC_NEG)
		{
			/* ----------
			 * var1 is positive, var2 is negative
			 * result = +(ABS(var1) + ABS(var2))
			 * ----------
			 */
			add_abs(var1, var2, result);
			result->sign = NUMERIC_POS;
		}
		else
		{
			/* ----------
			 * Both are positive
			 * Must compare absolute values
			 * ----------
			 */
			switch (cmp_abs(var1, var2))
			{
				case 0:
					/* ----------
					 * ABS(var1) == ABS(var2)
					 * result = ZERO
					 * ----------
					 */
					zero_var(result);
					result->dscale = Max(var1->dscale, var2->dscale);
					break;

				case 1:
					/* ----------
					 * ABS(var1) > ABS(var2)
					 * result = +(ABS(var1) - ABS(var2))
					 * ----------
					 */
					sub_abs(var1, var2, result);
					result->sign = NUMERIC_POS;
					break;

				case -1:
					/* ----------
					 * ABS(var1) < ABS(var2)
					 * result = -(ABS(var2) - ABS(var1))
					 * ----------
					 */
					sub_abs(var2, var1, result);
					result->sign = NUMERIC_NEG;
					break;
			}
		}
	}
	else
	{
		if (var2->sign == NUMERIC_NEG)
		{
			/* ----------
			 * Both are negative
			 * Must compare absolute values
			 * ----------
			 */
			switch (cmp_abs(var1, var2))
			{
				case 0:
					/* ----------
					 * ABS(var1) == ABS(var2)
					 * result = ZERO
					 * ----------
					 */
					zero_var(result);
					result->dscale = Max(var1->dscale, var2->dscale);
					break;

				case 1:
					/* ----------
					 * ABS(var1) > ABS(var2)
					 * result = -(ABS(var1) - ABS(var2))
					 * ----------
					 */
					sub_abs(var1, var2, result);
					result->sign = NUMERIC_NEG;
					break;

				case -1:
					/* ----------
					 * ABS(var1) < ABS(var2)
					 * result = +(ABS(var2) - ABS(var1))
					 * ----------
					 */
					sub_abs(var2, var1, result);
					result->sign = NUMERIC_POS;
					break;
			}
		}
		else
		{
			/* ----------
			 * var1 is negative, var2 is positive
			 * result = -(ABS(var1) + ABS(var2))
			 * ----------
			 */
			add_abs(var1, var2, result);
			result->sign = NUMERIC_NEG;
		}
	}
}


/*
 * mul_var() -
 *
 *	Multiplication on variable level. Product of var1 * var2 is stored
 *	in result.  Result is rounded to no more than rscale fractional digits.
 */
static void
mul_var(const NumericVar *var1, const NumericVar *var2, NumericVar *result,
		int rscale)
{
	int			res_ndigits;
	int			res_ndigitpairs;
	int			res_sign;
	int			res_weight;
	int			pair_offset;
	int			maxdigits;
	int			maxdigitpairs;
	uint64	   *dig,
			   *dig_i1_off;
	uint64		maxdig;
	uint64		carry;
	uint64		newdig;
	int			var1ndigits;
	int			var2ndigits;
	int			var1ndigitpairs;
	int			var2ndigitpairs;
	NumericDigit *var1digits;
	NumericDigit *var2digits;
	uint32		var1digitpair;
	uint32	   *var2digitpairs;
	NumericDigit *res_digits;
	int			i,
				i1,
				i2,
				i2limit;

	/*
	 * Arrange for var1 to be the shorter of the two numbers.  This improves
	 * performance because the inner multiplication loop is much simpler than
	 * the outer loop, so it's better to have a smaller number of iterations
	 * of the outer loop.  This also reduces the number of times that the
	 * accumulator array needs to be normalized.
	 */
	if (var1->ndigits > var2->ndigits)
	{
		const NumericVar *tmp = var1;

		var1 = var2;
		var2 = tmp;
	}

	/* copy these values into local vars for speed in inner loop */
	var1ndigits = var1->ndigits;
	var2ndigits = var2->ndigits;
	var1digits = var1->digits;
	var2digits = var2->digits;

	if (var1ndigits == 0)
	{
		/* one or both inputs is zero; so is result */
		zero_var(result);
		result->dscale = rscale;
		return;
	}

	/*
	 * If var1 has 1-6 digits and the exact result was requested, delegate to
	 * mul_var_short() which uses a faster direct multiplication algorithm.
	 */
	if (var1ndigits <= 6 && rscale == var1->dscale + var2->dscale)
	{
		mul_var_short(var1, var2, result);
		return;
	}

	/* Determine result sign */
	if (var1->sign == var2->sign)
		res_sign = NUMERIC_POS;
	else
		res_sign = NUMERIC_NEG;

	/*
	 * Determine the number of result digits to compute and the (maximum
	 * possible) result weight.  If the exact result would have more than
	 * rscale fractional digits, truncate the computation with
	 * MUL_GUARD_DIGITS guard digits, i.e., ignore input digits that would
	 * only contribute to the right of that.  (This will give the exact
	 * rounded-to-rscale answer unless carries out of the ignored positions
	 * would have propagated through more than MUL_GUARD_DIGITS digits.)
	 *
	 * Note: an exact computation could not produce more than var1ndigits +
	 * var2ndigits digits, but we allocate at least one extra output digit in
	 * case rscale-driven rounding produces a carry out of the highest exact
	 * digit.
	 *
	 * The computation itself is done using base-NBASE^2 arithmetic, so we
	 * actually process the input digits in pairs, producing a base-NBASE^2
	 * intermediate result.  This significantly improves performance, since
	 * schoolbook multiplication is O(N^2) in the number of input digits, and
	 * working in base NBASE^2 effectively halves "N".
	 *
	 * Note: in a truncated computation, we must compute at least one extra
	 * output digit to ensure that all the guard digits are fully computed.
	 */
	/* digit pairs in each input */
	var1ndigitpairs = (var1ndigits + 1) / 2;
	var2ndigitpairs = (var2ndigits + 1) / 2;

	/* digits in exact result */
	res_ndigits = var1ndigits + var2ndigits;

	/* digit pairs in exact result with at least one extra output digit */
	res_ndigitpairs = res_ndigits / 2 + 1;

	/* pair offset to align result to end of dig[] */
	pair_offset = res_ndigitpairs - var1ndigitpairs - var2ndigitpairs + 1;

	/* maximum possible result weight (odd-length inputs shifted up below) */
	res_weight = var1->weight + var2->weight + 1 + 2 * res_ndigitpairs -
		res_ndigits - (var1ndigits & 1) - (var2ndigits & 1);

	/* rscale-based truncation with at least one extra output digit */
	maxdigits = res_weight + 1 + (rscale + DEC_DIGITS - 1) / DEC_DIGITS +
		MUL_GUARD_DIGITS;
	maxdigitpairs = maxdigits / 2 + 1;

	res_ndigitpairs = Min(res_ndigitpairs, maxdigitpairs);
	res_ndigits = 2 * res_ndigitpairs;

	/*
	 * In the computation below, digit pair i1 of var1 and digit pair i2 of
	 * var2 are multiplied and added to digit i1+i2+pair_offset of dig[]. Thus
	 * input digit pairs with index >= res_ndigitpairs - pair_offset don't
	 * contribute to the result, and can be ignored.
	 */
	if (res_ndigitpairs <= pair_offset)
	{
		/* All input digits will be ignored; so result is zero */
		zero_var(result);
		result->dscale = rscale;
		return;
	}
	var1ndigitpairs = Min(var1ndigitpairs, res_ndigitpairs - pair_offset);
	var2ndigitpairs = Min(var2ndigitpairs, res_ndigitpairs - pair_offset);

	/*
	 * We do the arithmetic in an array "dig[]" of unsigned 64-bit integers.
	 * Since PG_UINT64_MAX is much larger than NBASE^4, this gives us a lot of
	 * headroom to avoid normalizing carries immediately.
	 *
	 * maxdig tracks the maximum possible value of any dig[] entry; when this
	 * threatens to exceed PG_UINT64_MAX, we take the time to propagate
	 * carries.  Furthermore, we need to ensure that overflow doesn't occur
	 * during the carry propagation passes either.  The carry values could be
	 * as much as PG_UINT64_MAX / NBASE^2, so really we must normalize when
	 * digits threaten to exceed PG_UINT64_MAX - PG_UINT64_MAX / NBASE^2.
	 *
	 * To avoid overflow in maxdig itself, it actually represents the maximum
	 * possible value divided by NBASE^2-1, i.e., at the top of the loop it is
	 * known that no dig[] entry exceeds maxdig * (NBASE^2-1).
	 *
	 * The conversion of var1 to base NBASE^2 is done on the fly, as each new
	 * digit is required.  The digits of var2 are converted upfront, and
	 * stored at the end of dig[].  To avoid loss of precision, the input
	 * digits are aligned with the start of digit pair array, effectively
	 * shifting them up (multiplying by NBASE) if the inputs have an odd
	 * number of NBASE digits.
	 */
	dig = (uint64 *) palloc(res_ndigitpairs * sizeof(uint64) +
							var2ndigitpairs * sizeof(uint32));

	/* convert var2 to base NBASE^2, shifting up if its length is odd */
	var2digitpairs = (uint32 *) (dig + res_ndigitpairs);

	for (i2 = 0; i2 < var2ndigitpairs - 1; i2++)
		var2digitpairs[i2] = var2digits[2 * i2] * NBASE + var2digits[2 * i2 + 1];

	if (2 * i2 + 1 < var2ndigits)
		var2digitpairs[i2] = var2digits[2 * i2] * NBASE + var2digits[2 * i2 + 1];
	else
		var2digitpairs[i2] = var2digits[2 * i2] * NBASE;

	/*
	 * Start by multiplying var2 by the least significant contributing digit
	 * pair from var1, storing the results at the end of dig[], and filling
	 * the leading digits with zeros.
	 *
	 * The loop here is the same as the inner loop below, except that we set
	 * the results in dig[], rather than adding to them.  This is the
	 * performance bottleneck for multiplication, so we want to keep it simple
	 * enough so that it can be auto-vectorized.  Accordingly, process the
	 * digits left-to-right even though schoolbook multiplication would
	 * suggest right-to-left.  Since we aren't propagating carries in this
	 * loop, the order does not matter.
	 */
	i1 = var1ndigitpairs - 1;
	if (2 * i1 + 1 < var1ndigits)
		var1digitpair = var1digits[2 * i1] * NBASE + var1digits[2 * i1 + 1];
	else
		var1digitpair = var1digits[2 * i1] * NBASE;
	maxdig = var1digitpair;

	i2limit = Min(var2ndigitpairs, res_ndigitpairs - i1 - pair_offset);
	dig_i1_off = &dig[i1 + pair_offset];

	memset(dig, 0, (i1 + pair_offset) * sizeof(uint64));
	for (i2 = 0; i2 < i2limit; i2++)
		dig_i1_off[i2] = (uint64) var1digitpair * var2digitpairs[i2];

	/*
	 * Next, multiply var2 by the remaining digit pairs from var1, adding the
	 * results to dig[] at the appropriate offsets, and normalizing whenever
	 * there is a risk of any dig[] entry overflowing.
	 */
	for (i1 = i1 - 1; i1 >= 0; i1--)
	{
		var1digitpair = var1digits[2 * i1] * NBASE + var1digits[2 * i1 + 1];
		if (var1digitpair == 0)
			continue;

		/* Time to normalize? */
		maxdig += var1digitpair;
		if (maxdig > (PG_UINT64_MAX - PG_UINT64_MAX / NBASE_SQR) / (NBASE_SQR - 1))
		{
			/* Yes, do it (to base NBASE^2) */
			carry = 0;
			for (i = res_ndigitpairs - 1; i >= 0; i--)
			{
				newdig = dig[i] + carry;
				if (newdig >= NBASE_SQR)
				{
					carry = newdig / NBASE_SQR;
					newdig -= carry * NBASE_SQR;
				}
				else
					carry = 0;
				dig[i] = newdig;
			}
			Assert(carry == 0);
			/* Reset maxdig to indicate new worst-case */
			maxdig = 1 + var1digitpair;
		}

		/* Multiply and add */
		i2limit = Min(var2ndigitpairs, res_ndigitpairs - i1 - pair_offset);
		dig_i1_off = &dig[i1 + pair_offset];

		for (i2 = 0; i2 < i2limit; i2++)
			dig_i1_off[i2] += (uint64) var1digitpair * var2digitpairs[i2];
	}

	/*
	 * Now we do a final carry propagation pass to normalize back to base
	 * NBASE^2, and construct the base-NBASE result digits.  Note that this is
	 * still done at full precision w/guard digits.
	 */
	alloc_var(result, res_ndigits);
	res_digits = result->digits;
	carry = 0;
	for (i = res_ndigitpairs - 1; i >= 0; i--)
	{
		newdig = dig[i] + carry;
		if (newdig >= NBASE_SQR)
		{
			carry = newdig / NBASE_SQR;
			newdig -= carry * NBASE_SQR;
		}
		else
			carry = 0;
		res_digits[2 * i + 1] = (NumericDigit) ((uint32) newdig % NBASE);
		res_digits[2 * i] = (NumericDigit) ((uint32) newdig / NBASE);
	}
	Assert(carry == 0);

	pfree(dig);

	/*
	 * Finally, round the result to the requested precision.
	 */
	result->weight = res_weight;
	result->sign = res_sign;

	/* Round to target rscale (and set result->dscale) */
	round_var(result, rscale);

	/* Strip leading and trailing zeroes */
	strip_var(result);
}


/*
 * mul_var_short() -
 *
 *	Special-case multiplication function used when var1 has 1-6 digits, var2
 *	has at least as many digits as var1, and the exact product var1 * var2 is
 *	requested.
 */
static void
mul_var_short(const NumericVar *var1, const NumericVar *var2,
			  NumericVar *result)
{
	int			var1ndigits = var1->ndigits;
	int			var2ndigits = var2->ndigits;
	NumericDigit *var1digits = var1->digits;
	NumericDigit *var2digits = var2->digits;
	int			res_sign;
	int			res_weight;
	int			res_ndigits;
	NumericDigit *res_buf;
	NumericDigit *res_digits;
	uint32		carry = 0;
	uint32		term;

	/* Check preconditions */
	Assert(var1ndigits >= 1);
	Assert(var1ndigits <= 6);
	Assert(var2ndigits >= var1ndigits);

	/*
	 * Determine the result sign, weight, and number of digits to calculate.
	 * The weight figured here is correct if the product has no leading zero
	 * digits; otherwise strip_var() will fix things up.  Note that, unlike
	 * mul_var(), we do not need to allocate an extra output digit, because we
	 * are not rounding here.
	 */
	if (var1->sign == var2->sign)
		res_sign = NUMERIC_POS;
	else
		res_sign = NUMERIC_NEG;
	res_weight = var1->weight + var2->weight + 1;
	res_ndigits = var1ndigits + var2ndigits;

	/* Allocate result digit array */
	res_buf = digitbuf_alloc(res_ndigits + 1);
	res_buf[0] = 0;				/* spare digit for later rounding */
	res_digits = res_buf + 1;

	/*
	 * Compute the result digits in reverse, in one pass, propagating the
	 * carry up as we go.  The i'th result digit consists of the sum of the
	 * products var1digits[i1] * var2digits[i2] for which i = i1 + i2 + 1.
	 */
#define PRODSUM1(v1,i1,v2,i2) ((v1)[(i1)] * (v2)[(i2)])
#define PRODSUM2(v1,i1,v2,i2) (PRODSUM1(v1,i1,v2,i2) + (v1)[(i1)+1] * (v2)[(i2)-1])
#define PRODSUM3(v1,i1,v2,i2) (PRODSUM2(v1,i1,v2,i2) + (v1)[(i1)+2] * (v2)[(i2)-2])
#define PRODSUM4(v1,i1,v2,i2) (PRODSUM3(v1,i1,v2,i2) + (v1)[(i1)+3] * (v2)[(i2)-3])
#define PRODSUM5(v1,i1,v2,i2) (PRODSUM4(v1,i1,v2,i2) + (v1)[(i1)+4] * (v2)[(i2)-4])
#define PRODSUM6(v1,i1,v2,i2) (PRODSUM5(v1,i1,v2,i2) + (v1)[(i1)+5] * (v2)[(i2)-5])

	switch (var1ndigits)
	{
		case 1:
			/* ---------
			 * 1-digit case:
			 *		var1ndigits = 1
			 *		var2ndigits >= 1
			 *		res_ndigits = var2ndigits + 1
			 * ----------
			 */
			for (int i = var2ndigits - 1; i >= 0; i--)
			{
				term = PRODSUM1(var1digits, 0, var2digits, i) + carry;
				res_digits[i + 1] = (NumericDigit) (term % NBASE);
				carry = term / NBASE;
			}
			res_digits[0] = (NumericDigit) carry;
			break;

		case 2:
			/* ---------
			 * 2-digit case:
			 *		var1ndigits = 2
			 *		var2ndigits >= 2
			 *		res_ndigits = var2ndigits + 2
			 * ----------
			 */
			/* last result digit and carry */
			term = PRODSUM1(var1digits, 1, var2digits, var2ndigits - 1);
			res_digits[res_ndigits - 1] = (NumericDigit) (term % NBASE);
			carry = term / NBASE;

			/* remaining digits, except for the first two */
			for (int i = var2ndigits - 1; i >= 1; i--)
			{
				term = PRODSUM2(var1digits, 0, var2digits, i) + carry;
				res_digits[i + 1] = (NumericDigit) (term % NBASE);
				carry = term / NBASE;
			}
			break;

		case 3:
			/* ---------
			 * 3-digit case:
			 *		var1ndigits = 3
			 *		var2ndigits >= 3
			 *		res_ndigits = var2ndigits + 3
			 * ----------
			 */
			/* last two result digits */
			term = PRODSUM1(var1digits, 2, var2digits, var2ndigits - 1);
			res_digits[res_ndigits - 1] = (NumericDigit) (term % NBASE);
			carry = term / NBASE;

			term = PRODSUM2(var1digits, 1, var2digits, var2ndigits - 1) + carry;
			res_digits[res_ndigits - 2] = (NumericDigit) (term % NBASE);
			carry = term / NBASE;

			/* remaining digits, except for the first three */
			for (int i = var2ndigits - 1; i >= 2; i--)
			{
				term = PRODSUM3(var1digits, 0, var2digits, i) + carry;
				res_digits[i + 1] = (NumericDigit) (term % NBASE);
				carry = term / NBASE;
			}
			break;

		case 4:
			/* ---------
			 * 4-digit case:
			 *		var1ndigits = 4
			 *		var2ndigits >= 4
			 *		res_ndigits = var2ndigits + 4
			 * ----------
			 */
			/* last three result digits */
			term = PRODSUM1(var1digits, 3, var2digits, var2ndigits - 1);
			res_digits[res_ndigits - 1] = (NumericDigit) (term % NBASE);
			carry = term / NBASE;

			term = PRODSUM2(var1digits, 2, var2digits, var2ndigits - 1) + carry;
			res_digits[res_ndigits - 2] = (NumericDigit) (term % NBASE);
			carry = term / NBASE;

			term = PRODSUM3(var1digits, 1, var2digits, var2ndigits - 1) + carry;
			res_digits[res_ndigits - 3] = (NumericDigit) (term % NBASE);
			carry = term / NBASE;

			/* remaining digits, except for the first four */
			for (int i = var2ndigits - 1; i >= 3; i--)
			{
				term = PRODSUM4(var1digits, 0, var2digits, i) + carry;
				res_digits[i + 1] = (NumericDigit) (term % NBASE);
				carry = term / NBASE;
			}
			break;

		case 5:
			/* ---------
			 * 5-digit case:
			 *		var1ndigits = 5
			 *		var2ndigits >= 5
			 *		res_ndigits = var2ndigits + 5
			 * ----------
			 */
			/* last four result digits */
			term = PRODSUM1(var1digits, 4, var2digits, var2ndigits - 1);
			res_digits[res_ndigits - 1] = (NumericDigit) (term % NBASE);
			carry = term / NBASE;

			term = PRODSUM2(var1digits, 3, var2digits, var2ndigits - 1) + carry;
			res_digits[res_ndigits - 2] = (NumericDigit) (term % NBASE);
			carry = term / NBASE;

			term = PRODSUM3(var1digits, 2, var2digits, var2ndigits - 1) + carry;
			res_digits[res_ndigits - 3] = (NumericDigit) (term % NBASE);
			carry = term / NBASE;

			term = PRODSUM4(var1digits, 1, var2digits, var2ndigits - 1) + carry;
			res_digits[res_ndigits - 4] = (NumericDigit) (term % NBASE);
			carry = term / NBASE;

			/* remaining digits, except for the first five */
			for (int i = var2ndigits - 1; i >= 4; i--)
			{
				term = PRODSUM5(var1digits, 0, var2digits, i) + carry;
				res_digits[i + 1] = (NumericDigit) (term % NBASE);
				carry = term / NBASE;
			}
			break;

		case 6:
			/* ---------
			 * 6-digit case:
			 *		var1ndigits = 6
			 *		var2ndigits >= 6
			 *		res_ndigits = var2ndigits + 6
			 * ----------
			 */
			/* last five result digits */
			term = PRODSUM1(var1digits, 5, var2digits, var2ndigits - 1);
			res_digits[res_ndigits - 1] = (NumericDigit) (term % NBASE);
			carry = term / NBASE;

			term = PRODSUM2(var1digits, 4, var2digits, var2ndigits - 1) + carry;
			res_digits[res_ndigits - 2] = (NumericDigit) (term % NBASE);
			carry = term / NBASE;

			term = PRODSUM3(var1digits, 3, var2digits, var2ndigits - 1) + carry;
			res_digits[res_ndigits - 3] = (NumericDigit) (term % NBASE);
			carry = term / NBASE;

			term = PRODSUM4(var1digits, 2, var2digits, var2ndigits - 1) + carry;
			res_digits[res_ndigits - 4] = (NumericDigit) (term % NBASE);
			carry = term / NBASE;

			term = PRODSUM5(var1digits, 1, var2digits, var2ndigits - 1) + carry;
			res_digits[res_ndigits - 5] = (NumericDigit) (term % NBASE);
			carry = term / NBASE;

			/* remaining digits, except for the first six */
			for (int i = var2ndigits - 1; i >= 5; i--)
			{
				term = PRODSUM6(var1digits, 0, var2digits, i) + carry;
				res_digits[i + 1] = (NumericDigit) (term % NBASE);
				carry = term / NBASE;
			}
			break;
	}

	/*
	 * Finally, for var1ndigits > 1, compute the remaining var1ndigits most
	 * significant result digits.
	 */
	switch (var1ndigits)
	{
		case 6:
			term = PRODSUM5(var1digits, 0, var2digits, 4) + carry;
			res_digits[5] = (NumericDigit) (term % NBASE);
			carry = term / NBASE;
			/* FALLTHROUGH */
		case 5:
			term = PRODSUM4(var1digits, 0, var2digits, 3) + carry;
			res_digits[4] = (NumericDigit) (term % NBASE);
			carry = term / NBASE;
			/* FALLTHROUGH */
		case 4:
			term = PRODSUM3(var1digits, 0, var2digits, 2) + carry;
			res_digits[3] = (NumericDigit) (term % NBASE);
			carry = term / NBASE;
			/* FALLTHROUGH */
		case 3:
			term = PRODSUM2(var1digits, 0, var2digits, 1) + carry;
			res_digits[2] = (NumericDigit) (term % NBASE);
			carry = term / NBASE;
			/* FALLTHROUGH */
		case 2:
			term = PRODSUM1(var1digits, 0, var2digits, 0) + carry;
			res_digits[1] = (NumericDigit) (term % NBASE);
			res_digits[0] = (NumericDigit) (term / NBASE);
			break;
	}

	/* Store the product in result */
	digitbuf_free(result->buf);
	result->ndigits = res_ndigits;
	result->buf = res_buf;
	result->digits = res_digits;
	result->weight = res_weight;
	result->sign = res_sign;
	result->dscale = var1->dscale + var2->dscale;

	/* Strip leading and trailing zeroes */
	strip_var(result);
}


/*
 * div_var() -
 *
 *	Compute the quotient var1 / var2 to rscale fractional digits.
 *
 *	If "round" is true, the result is rounded at the rscale'th digit; if
 *	false, it is truncated (towards zero) at that digit.
 *
 *	If "exact" is true, the exact result is computed to the specified rscale;
 *	if false, successive quotient digits are approximated up to rscale plus
 *	DIV_GUARD_DIGITS extra digits, ignoring all contributions from digits to
 *	the right of that, before rounding or truncating to the specified rscale.
 *	This can be significantly faster, and usually gives the same result as the
 *	exact computation, but it may occasionally be off by one in the final
 *	digit, if contributions from the ignored digits would have propagated
 *	through the guard digits.  This is good enough for the transcendental
 *	functions, where small errors are acceptable.
 */
static void
div_var(const NumericVar *var1, const NumericVar *var2, NumericVar *result,
		int rscale, bool round, bool exact)
{
	int			var1ndigits = var1->ndigits;
	int			var2ndigits = var2->ndigits;
	int			res_sign;
	int			res_weight;
	int			res_ndigits;
	int			var1ndigitpairs;
	int			var2ndigitpairs;
	int			res_ndigitpairs;
	int			div_ndigitpairs;
	int64	   *dividend;
	int32	   *divisor;
	double		fdivisor,
				fdivisorinverse,
				fdividend,
				fquotient;
	int64		maxdiv;
	int			qi;
	int32		qdigit;
	int64		carry;
	int64		newdig;
	int64	   *remainder;
	NumericDigit *res_digits;
	int			i;

	/*
	 * First of all division by zero check; we must not be handed an
	 * unnormalized divisor.
	 */
	if (var2ndigits == 0 || var2->digits[0] == 0)
		ereport(ERROR,
				(errcode(ERRCODE_DIVISION_BY_ZERO),
				 errmsg("division by zero")));

	/*
	 * If the divisor has just one or two digits, delegate to div_var_int(),
	 * which uses fast short division.
	 *
	 * Similarly, on platforms with 128-bit integer support, delegate to
	 * div_var_int64() for divisors with three or four digits.
	 */
	if (var2ndigits <= 2)
	{
		int			idivisor;
		int			idivisor_weight;

		idivisor = var2->digits[0];
		idivisor_weight = var2->weight;
		if (var2ndigits == 2)
		{
			idivisor = idivisor * NBASE + var2->digits[1];
			idivisor_weight--;
		}
		if (var2->sign == NUMERIC_NEG)
			idivisor = -idivisor;

		div_var_int(var1, idivisor, idivisor_weight, result, rscale, round);
		return;
	}
#ifdef HAVE_INT128
	if (var2ndigits <= 4)
	{
		int64		idivisor;
		int			idivisor_weight;

		idivisor = var2->digits[0];
		idivisor_weight = var2->weight;
		for (i = 1; i < var2ndigits; i++)
		{
			idivisor = idivisor * NBASE + var2->digits[i];
			idivisor_weight--;
		}
		if (var2->sign == NUMERIC_NEG)
			idivisor = -idivisor;

		div_var_int64(var1, idivisor, idivisor_weight, result, rscale, round);
		return;
	}
#endif

	/*
	 * Otherwise, perform full long division.
	 */

	/* Result zero check */
	if (var1ndigits == 0)
	{
		zero_var(result);
		result->dscale = rscale;
		return;
	}

	/*
	 * The approximate computation can be significantly faster than the exact
	 * one, since the working dividend is var2ndigitpairs base-NBASE^2 digits
	 * shorter below.  However, that comes with the tradeoff of computing
	 * DIV_GUARD_DIGITS extra base-NBASE result digits.  Ignoring all other
	 * overheads, that suggests that, in theory, the approximate computation
	 * will only be faster than the exact one when var2ndigits is greater than
	 * 2 * (DIV_GUARD_DIGITS + 1), independent of the size of var1.
	 *
	 * Thus, we're better off doing an exact computation when var2 is shorter
	 * than this.  Empirically, it has been found that the exact threshold is
	 * a little higher, due to other overheads in the outer division loop.
	 */
	if (var2ndigits <= 2 * (DIV_GUARD_DIGITS + 2))
		exact = true;

	/*
	 * Determine the result sign, weight and number of digits to calculate.
	 * The weight figured here is correct if the emitted quotient has no
	 * leading zero digits; otherwise strip_var() will fix things up.
	 */
	if (var1->sign == var2->sign)
		res_sign = NUMERIC_POS;
	else
		res_sign = NUMERIC_NEG;
	res_weight = var1->weight - var2->weight + 1;
	/* The number of accurate result digits we need to produce: */
	res_ndigits = res_weight + 1 + (rscale + DEC_DIGITS - 1) / DEC_DIGITS;
	/* ... but always at least 1 */
	res_ndigits = Max(res_ndigits, 1);
	/* If rounding needed, figure one more digit to ensure correct result */
	if (round)
		res_ndigits++;
	/* Add guard digits for roundoff error when producing approx result */
	if (!exact)
		res_ndigits += DIV_GUARD_DIGITS;

	/*
	 * The computation itself is done using base-NBASE^2 arithmetic, so we
	 * actually process the input digits in pairs, producing a base-NBASE^2
	 * intermediate result.  This significantly improves performance, since
	 * the computation is O(N^2) in the number of input digits, and working in
	 * base NBASE^2 effectively halves "N".
	 */
	var1ndigitpairs = (var1ndigits + 1) / 2;
	var2ndigitpairs = (var2ndigits + 1) / 2;
	res_ndigitpairs = (res_ndigits + 1) / 2;
	res_ndigits = 2 * res_ndigitpairs;

	/*
	 * We do the arithmetic in an array "dividend[]" of signed 64-bit
	 * integers.  Since PG_INT64_MAX is much larger than NBASE^4, this gives
	 * us a lot of headroom to avoid normalizing carries immediately.
	 *
	 * When performing an exact computation, the working dividend requires
	 * res_ndigitpairs + var2ndigitpairs digits.  If var1 is larger than that,
	 * the extra digits do not contribute to the result, and are ignored.
	 *
	 * When performing an approximate computation, the working dividend only
	 * requires res_ndigitpairs digits (which includes the extra guard
	 * digits).  All input digits beyond that are ignored.
	 */
	if (exact)
	{
		div_ndigitpairs = res_ndigitpairs + var2ndigitpairs;
		var1ndigitpairs = Min(var1ndigitpairs, div_ndigitpairs);
	}
	else
	{
		div_ndigitpairs = res_ndigitpairs;
		var1ndigitpairs = Min(var1ndigitpairs, div_ndigitpairs);
		var2ndigitpairs = Min(var2ndigitpairs, div_ndigitpairs);
	}

	/*
	 * Allocate room for the working dividend (div_ndigitpairs 64-bit digits)
	 * plus the divisor (var2ndigitpairs 32-bit base-NBASE^2 digits).
	 *
	 * For convenience, we allocate one extra dividend digit, which is set to
	 * zero and not counted in div_ndigitpairs, so that the main loop below
	 * can safely read and write the (qi+1)'th digit in the approximate case.
	 */
	dividend = (int64 *) palloc((div_ndigitpairs + 1) * sizeof(int64) +
								var2ndigitpairs * sizeof(int32));
	divisor = (int32 *) (dividend + div_ndigitpairs + 1);

	/* load var1 into dividend[0 .. var1ndigitpairs-1], zeroing the rest */
	for (i = 0; i < var1ndigitpairs - 1; i++)
		dividend[i] = var1->digits[2 * i] * NBASE + var1->digits[2 * i + 1];

	if (2 * i + 1 < var1ndigits)
		dividend[i] = var1->digits[2 * i] * NBASE + var1->digits[2 * i + 1];
	else
		dividend[i] = var1->digits[2 * i] * NBASE;

	memset(dividend + i + 1, 0, (div_ndigitpairs - i) * sizeof(int64));

	/* load var2 into divisor[0 .. var2ndigitpairs-1] */
	for (i = 0; i < var2ndigitpairs - 1; i++)
		divisor[i] = var2->digits[2 * i] * NBASE + var2->digits[2 * i + 1];

	if (2 * i + 1 < var2ndigits)
		divisor[i] = var2->digits[2 * i] * NBASE + var2->digits[2 * i + 1];
	else
		divisor[i] = var2->digits[2 * i] * NBASE;

	/*
	 * We estimate each quotient digit using floating-point arithmetic, taking
	 * the first 2 base-NBASE^2 digits of the (current) dividend and divisor.
	 * This must be float to avoid overflow.
	 *
	 * Since the floating-point dividend and divisor use 4 base-NBASE input
	 * digits, they include roughly 40-53 bits of information from their
	 * respective inputs (assuming NBASE is 10000), which fits well in IEEE
	 * double-precision variables.  The relative error in the floating-point
	 * quotient digit will then be less than around 2/NBASE^3, so the
	 * estimated base-NBASE^2 quotient digit will typically be correct, and
	 * should not be off by more than one from the correct value.
	 */
	fdivisor = (double) divisor[0] * NBASE_SQR;
	if (var2ndigitpairs > 1)
		fdivisor += (double) divisor[1];
	fdivisorinverse = 1.0 / fdivisor;

	/*
	 * maxdiv tracks the maximum possible absolute value of any dividend[]
	 * entry; when this threatens to exceed PG_INT64_MAX, we take the time to
	 * propagate carries.  Furthermore, we need to ensure that overflow
	 * doesn't occur during the carry propagation passes either.  The carry
	 * values may have an absolute value as high as PG_INT64_MAX/NBASE^2 + 1,
	 * so really we must normalize when digits threaten to exceed PG_INT64_MAX
	 * - PG_INT64_MAX/NBASE^2 - 1.
	 *
	 * To avoid overflow in maxdiv itself, it represents the max absolute
	 * value divided by NBASE^2-1, i.e., at the top of the loop it is known
	 * that no dividend[] entry has an absolute value exceeding maxdiv *
	 * (NBASE^2-1).
	 *
	 * Actually, though, that holds good only for dividend[] entries after
	 * dividend[qi]; the adjustment done at the bottom of the loop may cause
	 * dividend[qi + 1] to exceed the maxdiv limit, so that dividend[qi] in
	 * the next iteration is beyond the limit.  This does not cause problems,
	 * as explained below.
	 */
	maxdiv = 1;

	/*
	 * Outer loop computes next quotient digit, which goes in dividend[qi].
	 */
	for (qi = 0; qi < res_ndigitpairs; qi++)
	{
		/* Approximate the current dividend value */
		fdividend = (double) dividend[qi] * NBASE_SQR;
		fdividend += (double) dividend[qi + 1];

		/* Compute the (approximate) quotient digit */
		fquotient = fdividend * fdivisorinverse;
		qdigit = (fquotient >= 0.0) ? ((int32) fquotient) :
			(((int32) fquotient) - 1);	/* truncate towards -infinity */

		if (qdigit != 0)
		{
			/* Do we need to normalize now? */
			maxdiv += i64abs(qdigit);
			if (maxdiv > (PG_INT64_MAX - PG_INT64_MAX / NBASE_SQR - 1) / (NBASE_SQR - 1))
			{
				/*
				 * Yes, do it.  Note that if var2ndigitpairs is much smaller
				 * than div_ndigitpairs, we can save a significant amount of
				 * effort here by noting that we only need to normalise those
				 * dividend[] entries touched where prior iterations
				 * subtracted multiples of the divisor.
				 */
				carry = 0;
				for (i = Min(qi + var2ndigitpairs - 2, div_ndigitpairs - 1); i > qi; i--)
				{
					newdig = dividend[i] + carry;
					if (newdig < 0)
					{
						carry = -((-newdig - 1) / NBASE_SQR) - 1;
						newdig -= carry * NBASE_SQR;
					}
					else if (newdig >= NBASE_SQR)
					{
						carry = newdig / NBASE_SQR;
						newdig -= carry * NBASE_SQR;
					}
					else
						carry = 0;
					dividend[i] = newdig;
				}
				dividend[qi] += carry;

				/*
				 * All the dividend[] digits except possibly dividend[qi] are
				 * now in the range 0..NBASE^2-1.  We do not need to consider
				 * dividend[qi] in the maxdiv value anymore, so we can reset
				 * maxdiv to 1.
				 */
				maxdiv = 1;

				/*
				 * Recompute the quotient digit since new info may have
				 * propagated into the top two dividend digits.
				 */
				fdividend = (double) dividend[qi] * NBASE_SQR;
				fdividend += (double) dividend[qi + 1];
				fquotient = fdividend * fdivisorinverse;
				qdigit = (fquotient >= 0.0) ? ((int32) fquotient) :
					(((int32) fquotient) - 1);	/* truncate towards -infinity */

				maxdiv += i64abs(qdigit);
			}

			/*
			 * Subtract off the appropriate multiple of the divisor.
			 *
			 * The digits beyond dividend[qi] cannot overflow, because we know
			 * they will fall within the maxdiv limit.  As for dividend[qi]
			 * itself, note that qdigit is approximately trunc(dividend[qi] /
			 * divisor[0]), which would make the new value simply dividend[qi]
			 * mod divisor[0].  The lower-order terms in qdigit can change
			 * this result by not more than about twice PG_INT64_MAX/NBASE^2,
			 * so overflow is impossible.
			 *
			 * This inner loop is the performance bottleneck for division, so
			 * code it in the same way as the inner loop of mul_var() so that
			 * it can be auto-vectorized.
			 */
			if (qdigit != 0)
			{
				int			istop = Min(var2ndigitpairs, div_ndigitpairs - qi);
				int64	   *dividend_qi = &dividend[qi];

				for (i = 0; i < istop; i++)
					dividend_qi[i] -= (int64) qdigit * divisor[i];
			}
		}

		/*
		 * The dividend digit we are about to replace might still be nonzero.
		 * Fold it into the next digit position.
		 *
		 * There is no risk of overflow here, although proving that requires
		 * some care.  Much as with the argument for dividend[qi] not
		 * overflowing, if we consider the first two terms in the numerator
		 * and denominator of qdigit, we can see that the final value of
		 * dividend[qi + 1] will be approximately a remainder mod
		 * (divisor[0]*NBASE^2 + divisor[1]).  Accounting for the lower-order
		 * terms is a bit complicated but ends up adding not much more than
		 * PG_INT64_MAX/NBASE^2 to the possible range.  Thus, dividend[qi + 1]
		 * cannot overflow here, and in its role as dividend[qi] in the next
		 * loop iteration, it can't be large enough to cause overflow in the
		 * carry propagation step (if any), either.
		 *
		 * But having said that: dividend[qi] can be more than
		 * PG_INT64_MAX/NBASE^2, as noted above, which means that the product
		 * dividend[qi] * NBASE^2 *can* overflow.  When that happens, adding
		 * it to dividend[qi + 1] will always cause a canceling overflow so
		 * that the end result is correct.  We could avoid the intermediate
		 * overflow by doing the multiplication and addition using unsigned
		 * int64 arithmetic, which is modulo 2^64, but so far there appears no
		 * need.
		 */
		dividend[qi + 1] += dividend[qi] * NBASE_SQR;

		dividend[qi] = qdigit;
	}

	/*
	 * If an exact result was requested, use the remainder to correct the
	 * approximate quotient.  The remainder is in dividend[], immediately
	 * after the quotient digits.  Note, however, that although the remainder
	 * starts at dividend[qi = res_ndigitpairs], the first digit is the result
	 * of folding two remainder digits into one above, and the remainder
	 * currently only occupies var2ndigitpairs - 1 digits (the last digit of
	 * the working dividend was untouched by the computation above).  Thus we
	 * expand the remainder down by one base-NBASE^2 digit when we normalize
	 * it, so that it completely fills the last var2ndigitpairs digits of the
	 * dividend array.
	 */
	if (exact)
	{
		/* Normalize the remainder, expanding it down by one digit */
		remainder = &dividend[qi];
		carry = 0;
		for (i = var2ndigitpairs - 2; i >= 0; i--)
		{
			newdig = remainder[i] + carry;
			if (newdig < 0)
			{
				carry = -((-newdig - 1) / NBASE_SQR) - 1;
				newdig -= carry * NBASE_SQR;
			}
			else if (newdig >= NBASE_SQR)
			{
				carry = newdig / NBASE_SQR;
				newdig -= carry * NBASE_SQR;
			}
			else
				carry = 0;
			remainder[i + 1] = newdig;
		}
		remainder[0] = carry;

		if (remainder[0] < 0)
		{
			/*
			 * The remainder is negative, so the approximate quotient is too
			 * large.  Correct by reducing the quotient by one and adding the
			 * divisor to the remainder until the remainder is positive.  We
			 * expect the quotient to be off by at most one, which has been
			 * borne out in all testing, but not conclusively proven, so we
			 * allow for larger corrections, just in case.
			 */
			do
			{
				/* Add the divisor to the remainder */
				carry = 0;
				for (i = var2ndigitpairs - 1; i > 0; i--)
				{
					newdig = remainder[i] + divisor[i] + carry;
					if (newdig >= NBASE_SQR)
					{
						remainder[i] = newdig - NBASE_SQR;
						carry = 1;
					}
					else
					{
						remainder[i] = newdig;
						carry = 0;
					}
				}
				remainder[0] += divisor[0] + carry;

				/* Subtract 1 from the quotient (propagating carries later) */
				dividend[qi - 1]--;

			} while (remainder[0] < 0);
		}
		else
		{
			/*
			 * The remainder is nonnegative.  If it's greater than or equal to
			 * the divisor, then the approximate quotient is too small and
			 * must be corrected.  As above, we don't expect to have to apply
			 * more than one correction, but allow for it just in case.
			 */
			while (true)
			{
				bool		less = false;

				/* Is remainder < divisor? */
				for (i = 0; i < var2ndigitpairs; i++)
				{
					if (remainder[i] < divisor[i])
					{
						less = true;
						break;
					}
					if (remainder[i] > divisor[i])
						break;	/* remainder > divisor */
				}
				if (less)
					break;		/* quotient is correct */

				/* Subtract the divisor from the remainder */
				carry = 0;
				for (i = var2ndigitpairs - 1; i > 0; i--)
				{
					newdig = remainder[i] - divisor[i] + carry;
					if (newdig < 0)
					{
						remainder[i] = newdig + NBASE_SQR;
						carry = -1;
					}
					else
					{
						remainder[i] = newdig;
						carry = 0;
					}
				}
				remainder[0] = remainder[0] - divisor[0] + carry;

				/* Add 1 to the quotient (propagating carries later) */
				dividend[qi - 1]++;
			}
		}
	}

	/*
	 * Because the quotient digits were estimates that might have been off by
	 * one (and we didn't bother propagating carries when adjusting the
	 * quotient above), some quotient digits might be out of range, so do a
	 * final carry propagation pass to normalize back to base NBASE^2, and
	 * construct the base-NBASE result digits.  Note that this is still done
	 * at full precision w/guard digits.
	 */
	alloc_var(result, res_ndigits);
	res_digits = result->digits;
	carry = 0;
	for (i = res_ndigitpairs - 1; i >= 0; i--)
	{
		newdig = dividend[i] + carry;
		if (newdig < 0)
		{
			carry = -((-newdig - 1) / NBASE_SQR) - 1;
			newdig -= carry * NBASE_SQR;
		}
		else if (newdig >= NBASE_SQR)
		{
			carry = newdig / NBASE_SQR;
			newdig -= carry * NBASE_SQR;
		}
		else
			carry = 0;
		res_digits[2 * i + 1] = (NumericDigit) ((uint32) newdig % NBASE);
		res_digits[2 * i] = (NumericDigit) ((uint32) newdig / NBASE);
	}
	Assert(carry == 0);

	pfree(dividend);

	/*
	 * Finally, round or truncate the result to the requested precision.
	 */
	result->weight = res_weight;
	result->sign = res_sign;

	/* Round or truncate to target rscale (and set result->dscale) */
	if (round)
		round_var(result, rscale);
	else
		trunc_var(result, rscale);

	/* Strip leading and trailing zeroes */
	strip_var(result);
}


/*
 * div_var_int() -
 *
 *	Divide a numeric variable by a 32-bit integer with the specified weight.
 *	The quotient var / (ival * NBASE^ival_weight) is stored in result.
 */
static void
div_var_int(const NumericVar *var, int ival, int ival_weight,
			NumericVar *result, int rscale, bool round)
{
	NumericDigit *var_digits = var->digits;
	int			var_ndigits = var->ndigits;
	int			res_sign;
	int			res_weight;
	int			res_ndigits;
	NumericDigit *res_buf;
	NumericDigit *res_digits;
	uint32		divisor;
	int			i;

	/* Guard against division by zero */
	if (ival == 0)
		ereport(ERROR,
				errcode(ERRCODE_DIVISION_BY_ZERO),
				errmsg("division by zero"));

	/* Result zero check */
	if (var_ndigits == 0)
	{
		zero_var(result);
		result->dscale = rscale;
		return;
	}

	/*
	 * Determine the result sign, weight and number of digits to calculate.
	 * The weight figured here is correct if the emitted quotient has no
	 * leading zero digits; otherwise strip_var() will fix things up.
	 */
	if (var->sign == NUMERIC_POS)
		res_sign = ival > 0 ? NUMERIC_POS : NUMERIC_NEG;
	else
		res_sign = ival > 0 ? NUMERIC_NEG : NUMERIC_POS;
	res_weight = var->weight - ival_weight;
	/* The number of accurate result digits we need to produce: */
	res_ndigits = res_weight + 1 + (rscale + DEC_DIGITS - 1) / DEC_DIGITS;
	/* ... but always at least 1 */
	res_ndigits = Max(res_ndigits, 1);
	/* If rounding needed, figure one more digit to ensure correct result */
	if (round)
		res_ndigits++;

	res_buf = digitbuf_alloc(res_ndigits + 1);
	res_buf[0] = 0;				/* spare digit for later rounding */
	res_digits = res_buf + 1;

	/*
	 * Now compute the quotient digits.  This is the short division algorithm
	 * described in Knuth volume 2, section 4.3.1 exercise 16, except that we
	 * allow the divisor to exceed the internal base.
	 *
	 * In this algorithm, the carry from one digit to the next is at most
	 * divisor - 1.  Therefore, while processing the next digit, carry may
	 * become as large as divisor * NBASE - 1, and so it requires a 64-bit
	 * integer if this exceeds UINT_MAX.
	 */
	divisor = abs(ival);

	if (divisor <= UINT_MAX / NBASE)
	{
		/* carry cannot overflow 32 bits */
		uint32		carry = 0;

		for (i = 0; i < res_ndigits; i++)
		{
			carry = carry * NBASE + (i < var_ndigits ? var_digits[i] : 0);
			res_digits[i] = (NumericDigit) (carry / divisor);
			carry = carry % divisor;
		}
	}
	else
	{
		/* carry may exceed 32 bits */
		uint64		carry = 0;

		for (i = 0; i < res_ndigits; i++)
		{
			carry = carry * NBASE + (i < var_ndigits ? var_digits[i] : 0);
			res_digits[i] = (NumericDigit) (carry / divisor);
			carry = carry % divisor;
		}
	}

	/* Store the quotient in result */
	digitbuf_free(result->buf);
	result->ndigits = res_ndigits;
	result->buf = res_buf;
	result->digits = res_digits;
	result->weight = res_weight;
	result->sign = res_sign;

	/* Round or truncate to target rscale (and set result->dscale) */
	if (round)
		round_var(result, rscale);
	else
		trunc_var(result, rscale);

	/* Strip leading/trailing zeroes */
	strip_var(result);
}


#ifdef HAVE_INT128
/*
 * div_var_int64() -
 *
 *	Divide a numeric variable by a 64-bit integer with the specified weight.
 *	The quotient var / (ival * NBASE^ival_weight) is stored in result.
 *
 *	This duplicates the logic in div_var_int(), so any changes made there
 *	should be made here too.
 */
static void
div_var_int64(const NumericVar *var, int64 ival, int ival_weight,
			  NumericVar *result, int rscale, bool round)
{
	NumericDigit *var_digits = var->digits;
	int			var_ndigits = var->ndigits;
	int			res_sign;
	int			res_weight;
	int			res_ndigits;
	NumericDigit *res_buf;
	NumericDigit *res_digits;
	uint64		divisor;
	int			i;

	/* Guard against division by zero */
	if (ival == 0)
		ereport(ERROR,
				errcode(ERRCODE_DIVISION_BY_ZERO),
				errmsg("division by zero"));

	/* Result zero check */
	if (var_ndigits == 0)
	{
		zero_var(result);
		result->dscale = rscale;
		return;
	}

	/*
	 * Determine the result sign, weight and number of digits to calculate.
	 * The weight figured here is correct if the emitted quotient has no
	 * leading zero digits; otherwise strip_var() will fix things up.
	 */
	if (var->sign == NUMERIC_POS)
		res_sign = ival > 0 ? NUMERIC_POS : NUMERIC_NEG;
	else
		res_sign = ival > 0 ? NUMERIC_NEG : NUMERIC_POS;
	res_weight = var->weight - ival_weight;
	/* The number of accurate result digits we need to produce: */
	res_ndigits = res_weight + 1 + (rscale + DEC_DIGITS - 1) / DEC_DIGITS;
	/* ... but always at least 1 */
	res_ndigits = Max(res_ndigits, 1);
	/* If rounding needed, figure one more digit to ensure correct result */
	if (round)
		res_ndigits++;

	res_buf = digitbuf_alloc(res_ndigits + 1);
	res_buf[0] = 0;				/* spare digit for later rounding */
	res_digits = res_buf + 1;

	/*
	 * Now compute the quotient digits.  This is the short division algorithm
	 * described in Knuth volume 2, section 4.3.1 exercise 16, except that we
	 * allow the divisor to exceed the internal base.
	 *
	 * In this algorithm, the carry from one digit to the next is at most
	 * divisor - 1.  Therefore, while processing the next digit, carry may
	 * become as large as divisor * NBASE - 1, and so it requires a 128-bit
	 * integer if this exceeds PG_UINT64_MAX.
	 */
	divisor = i64abs(ival);

	if (divisor <= PG_UINT64_MAX / NBASE)
	{
		/* carry cannot overflow 64 bits */
		uint64		carry = 0;

		for (i = 0; i < res_ndigits; i++)
		{
			carry = carry * NBASE + (i < var_ndigits ? var_digits[i] : 0);
			res_digits[i] = (NumericDigit) (carry / divisor);
			carry = carry % divisor;
		}
	}
	else
	{
		/* carry may exceed 64 bits */
		uint128		carry = 0;

		for (i = 0; i < res_ndigits; i++)
		{
			carry = carry * NBASE + (i < var_ndigits ? var_digits[i] : 0);
			res_digits[i] = (NumericDigit) (carry / divisor);
			carry = carry % divisor;
		}
	}

	/* Store the quotient in result */
	digitbuf_free(result->buf);
	result->ndigits = res_ndigits;
	result->buf = res_buf;
	result->digits = res_digits;
	result->weight = res_weight;
	result->sign = res_sign;

	/* Round or truncate to target rscale (and set result->dscale) */
	if (round)
		round_var(result, rscale);
	else
		trunc_var(result, rscale);

	/* Strip leading/trailing zeroes */
	strip_var(result);
}
#endif


/*
 * Default scale selection for division
 *
 * Returns the appropriate result scale for the division result.
 */
static int
select_div_scale(const NumericVar *var1, const NumericVar *var2)
{
	int			weight1,
				weight2,
				qweight,
				i;
	NumericDigit firstdigit1,
				firstdigit2;
	int			rscale;

	/*
	 * The result scale of a division isn't specified in any SQL standard. For
	 * PostgreSQL we select a result scale that will give at least
	 * NUMERIC_MIN_SIG_DIGITS significant digits, so that numeric gives a
	 * result no less accurate than float8; but use a scale not less than
	 * either input's display scale.
	 */

	/* Get the actual (normalized) weight and first digit of each input */

	weight1 = 0;				/* values to use if var1 is zero */
	firstdigit1 = 0;
	for (i = 0; i < var1->ndigits; i++)
	{
		firstdigit1 = var1->digits[i];
		if (firstdigit1 != 0)
		{
			weight1 = var1->weight - i;
			break;
		}
	}

	weight2 = 0;				/* values to use if var2 is zero */
	firstdigit2 = 0;
	for (i = 0; i < var2->ndigits; i++)
	{
		firstdigit2 = var2->digits[i];
		if (firstdigit2 != 0)
		{
			weight2 = var2->weight - i;
			break;
		}
	}

	/*
	 * Estimate weight of quotient.  If the two first digits are equal, we
	 * can't be sure, but assume that var1 is less than var2.
	 */
	qweight = weight1 - weight2;
	if (firstdigit1 <= firstdigit2)
		qweight--;

	/* Select result scale */
	rscale = NUMERIC_MIN_SIG_DIGITS - qweight * DEC_DIGITS;
	rscale = Max(rscale, var1->dscale);
	rscale = Max(rscale, var2->dscale);
	rscale = Max(rscale, NUMERIC_MIN_DISPLAY_SCALE);
	rscale = Min(rscale, NUMERIC_MAX_DISPLAY_SCALE);

	return rscale;
}


/*
 * mod_var() -
 *
 *	Calculate the modulo of two numerics at variable level
 */
static void
mod_var(const NumericVar *var1, const NumericVar *var2, NumericVar *result)
{
	NumericVar	tmp;

	init_var(&tmp);

	/* ---------
	 * We do this using the equation
	 *		mod(x,y) = x - trunc(x/y)*y
	 * div_var can be persuaded to give us trunc(x/y) directly.
	 * ----------
	 */
	div_var(var1, var2, &tmp, 0, false, true);

	mul_var(var2, &tmp, &tmp, var2->dscale);

	sub_var(var1, &tmp, result);

	free_var(&tmp);
}


/*
 * div_mod_var() -
 *
 *	Calculate the truncated integer quotient and numeric remainder of two
 *	numeric variables.  The remainder is precise to var2's dscale.
 */
static void
div_mod_var(const NumericVar *var1, const NumericVar *var2,
			NumericVar *quot, NumericVar *rem)
{
	NumericVar	q;
	NumericVar	r;

	init_var(&q);
	init_var(&r);

	/*
	 * Use div_var() with exact = false to get an initial estimate for the
	 * integer quotient (truncated towards zero).  This might be slightly
	 * inaccurate, but we correct it below.
	 */
	div_var(var1, var2, &q, 0, false, false);

	/* Compute initial estimate of remainder using the quotient estimate. */
	mul_var(var2, &q, &r, var2->dscale);
	sub_var(var1, &r, &r);

	/*
	 * Adjust the results if necessary --- the remainder should have the same
	 * sign as var1, and its absolute value should be less than the absolute
	 * value of var2.
	 */
	while (r.ndigits != 0 && r.sign != var1->sign)
	{
		/* The absolute value of the quotient is too large */
		if (var1->sign == var2->sign)
		{
			sub_var(&q, &const_one, &q);
			add_var(&r, var2, &r);
		}
		else
		{
			add_var(&q, &const_one, &q);
			sub_var(&r, var2, &r);
		}
	}

	while (cmp_abs(&r, var2) >= 0)
	{
		/* The absolute value of the quotient is too small */
		if (var1->sign == var2->sign)
		{
			add_var(&q, &const_one, &q);
			sub_var(&r, var2, &r);
		}
		else
		{
			sub_var(&q, &const_one, &q);
			add_var(&r, var2, &r);
		}
	}

	set_var_from_var(&q, quot);
	set_var_from_var(&r, rem);

	free_var(&q);
	free_var(&r);
}


/*
 * ceil_var() -
 *
 *	Return the smallest integer greater than or equal to the argument
 *	on variable level
 */
static void
ceil_var(const NumericVar *var, NumericVar *result)
{
	NumericVar	tmp;

	init_var(&tmp);
	set_var_from_var(var, &tmp);

	trunc_var(&tmp, 0);

	if (var->sign == NUMERIC_POS && cmp_var(var, &tmp) != 0)
		add_var(&tmp, &const_one, &tmp);

	set_var_from_var(&tmp, result);
	free_var(&tmp);
}


/*
 * floor_var() -
 *
 *	Return the largest integer equal to or less than the argument
 *	on variable level
 */
static void
floor_var(const NumericVar *var, NumericVar *result)
{
	NumericVar	tmp;

	init_var(&tmp);
	set_var_from_var(var, &tmp);

	trunc_var(&tmp, 0);

	if (var->sign == NUMERIC_NEG && cmp_var(var, &tmp) != 0)
		sub_var(&tmp, &const_one, &tmp);

	set_var_from_var(&tmp, result);
	free_var(&tmp);
}


/*
 * gcd_var() -
 *
 *	Calculate the greatest common divisor of two numerics at variable level
 */
static void
gcd_var(const NumericVar *var1, const NumericVar *var2, NumericVar *result)
{
	int			res_dscale;
	int			cmp;
	NumericVar	tmp_arg;
	NumericVar	mod;

	res_dscale = Max(var1->dscale, var2->dscale);

	/*
	 * Arrange for var1 to be the number with the greater absolute value.
	 *
	 * This would happen automatically in the loop below, but avoids an
	 * expensive modulo operation.
	 */
	cmp = cmp_abs(var1, var2);
	if (cmp < 0)
	{
		const NumericVar *tmp = var1;

		var1 = var2;
		var2 = tmp;
	}

	/*
	 * Also avoid the taking the modulo if the inputs have the same absolute
	 * value, or if the smaller input is zero.
	 */
	if (cmp == 0 || var2->ndigits == 0)
	{
		set_var_from_var(var1, result);
		result->sign = NUMERIC_POS;
		result->dscale = res_dscale;
		return;
	}

	init_var(&tmp_arg);
	init_var(&mod);

	/* Use the Euclidean algorithm to find the GCD */
	set_var_from_var(var1, &tmp_arg);
	set_var_from_var(var2, result);

	for (;;)
	{
		/* this loop can take a while, so allow it to be interrupted */
		CHECK_FOR_INTERRUPTS();

		mod_var(&tmp_arg, result, &mod);
		if (mod.ndigits == 0)
			break;
		set_var_from_var(result, &tmp_arg);
		set_var_from_var(&mod, result);
	}
	result->sign = NUMERIC_POS;
	result->dscale = res_dscale;

	free_var(&tmp_arg);
	free_var(&mod);
}


/*
 * sqrt_var() -
 *
 *	Compute the square root of x using the Karatsuba Square Root algorithm.
 *	NOTE: we allow rscale < 0 here, implying rounding before the decimal
 *	point.
 */
static void
sqrt_var(const NumericVar *arg, NumericVar *result, int rscale)
{
	int			stat;
	int			res_weight;
	int			res_ndigits;
	int			src_ndigits;
	int			step;
	int			ndigits[32];
	int			blen;
	int64		arg_int64;
	int			src_idx;
	int64		s_int64;
	int64		r_int64;
	NumericVar	s_var;
	NumericVar	r_var;
	NumericVar	a0_var;
	NumericVar	a1_var;
	NumericVar	q_var;
	NumericVar	u_var;

	stat = cmp_var(arg, &const_zero);
	if (stat == 0)
	{
		zero_var(result);
		result->dscale = rscale;
		return;
	}

	/*
	 * SQL2003 defines sqrt() in terms of power, so we need to emit the right
	 * SQLSTATE error code if the operand is negative.
	 */
	if (stat < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION),
				 errmsg("cannot take square root of a negative number")));

	init_var(&s_var);
	init_var(&r_var);
	init_var(&a0_var);
	init_var(&a1_var);
	init_var(&q_var);
	init_var(&u_var);

	/*
	 * The result weight is half the input weight, rounded towards minus
	 * infinity --- res_weight = floor(arg->weight / 2).
	 */
	if (arg->weight >= 0)
		res_weight = arg->weight / 2;
	else
		res_weight = -((-arg->weight - 1) / 2 + 1);

	/*
	 * Number of NBASE digits to compute.  To ensure correct rounding, compute
	 * at least 1 extra decimal digit.  We explicitly allow rscale to be
	 * negative here, but must always compute at least 1 NBASE digit.  Thus
	 * res_ndigits = res_weight + 1 + ceil((rscale + 1) / DEC_DIGITS) or 1.
	 */
	if (rscale + 1 >= 0)
		res_ndigits = res_weight + 1 + (rscale + DEC_DIGITS) / DEC_DIGITS;
	else
		res_ndigits = res_weight + 1 - (-rscale - 1) / DEC_DIGITS;
	res_ndigits = Max(res_ndigits, 1);

	/*
	 * Number of source NBASE digits logically required to produce a result
	 * with this precision --- every digit before the decimal point, plus 2
	 * for each result digit after the decimal point (or minus 2 for each
	 * result digit we round before the decimal point).
	 */
	src_ndigits = arg->weight + 1 + (res_ndigits - res_weight - 1) * 2;
	src_ndigits = Max(src_ndigits, 1);

	/* ----------
	 * From this point on, we treat the input and the result as integers and
	 * compute the integer square root and remainder using the Karatsuba
	 * Square Root algorithm, which may be written recursively as follows:
	 *
	 *	SqrtRem(n = a3*b^3 + a2*b^2 + a1*b + a0):
	 *		[ for some base b, and coefficients a0,a1,a2,a3 chosen so that
	 *		  0 <= a0,a1,a2 < b and a3 >= b/4 ]
	 *		Let (s,r) = SqrtRem(a3*b + a2)
	 *		Let (q,u) = DivRem(r*b + a1, 2*s)
	 *		Let s = s*b + q
	 *		Let r = u*b + a0 - q^2
	 *		If r < 0 Then
	 *			Let r = r + s
	 *			Let s = s - 1
	 *			Let r = r + s
	 *		Return (s,r)
	 *
	 * See "Karatsuba Square Root", Paul Zimmermann, INRIA Research Report
	 * RR-3805, November 1999.  At the time of writing this was available
	 * on the net at <https://hal.inria.fr/inria-00072854>.
	 *
	 * The way to read the assumption "n = a3*b^3 + a2*b^2 + a1*b + a0" is
	 * "choose a base b such that n requires at least four base-b digits to
	 * express; then those digits are a3,a2,a1,a0, with a3 possibly larger
	 * than b".  For optimal performance, b should have approximately a
	 * quarter the number of digits in the input, so that the outer square
	 * root computes roughly twice as many digits as the inner one.  For
	 * simplicity, we choose b = NBASE^blen, an integer power of NBASE.
	 *
	 * We implement the algorithm iteratively rather than recursively, to
	 * allow the working variables to be reused.  With this approach, each
	 * digit of the input is read precisely once --- src_idx tracks the number
	 * of input digits used so far.
	 *
	 * The array ndigits[] holds the number of NBASE digits of the input that
	 * will have been used at the end of each iteration, which roughly doubles
	 * each time.  Note that the array elements are stored in reverse order,
	 * so if the final iteration requires src_ndigits = 37 input digits, the
	 * array will contain [37,19,11,7,5,3], and we would start by computing
	 * the square root of the 3 most significant NBASE digits.
	 *
	 * In each iteration, we choose blen to be the largest integer for which
	 * the input number has a3 >= b/4, when written in the form above.  In
	 * general, this means blen = src_ndigits / 4 (truncated), but if
	 * src_ndigits is a multiple of 4, that might lead to the coefficient a3
	 * being less than b/4 (if the first input digit is less than NBASE/4), in
	 * which case we choose blen = src_ndigits / 4 - 1.  The number of digits
	 * in the inner square root is then src_ndigits - 2*blen.  So, for
	 * example, if we have src_ndigits = 26 initially, the array ndigits[]
	 * will be either [26,14,8,4] or [26,14,8,6,4], depending on the size of
	 * the first input digit.
	 *
	 * Additionally, we can put an upper bound on the number of steps required
	 * as follows --- suppose that the number of source digits is an n-bit
	 * number in the range [2^(n-1), 2^n-1], then blen will be in the range
	 * [2^(n-3)-1, 2^(n-2)-1] and the number of digits in the inner square
	 * root will be in the range [2^(n-2), 2^(n-1)+1].  In the next step, blen
	 * will be in the range [2^(n-4)-1, 2^(n-3)] and the number of digits in
	 * the next inner square root will be in the range [2^(n-3), 2^(n-2)+1].
	 * This pattern repeats, and in the worst case the array ndigits[] will
	 * contain [2^n-1, 2^(n-1)+1, 2^(n-2)+1, ... 9, 5, 3], and the computation
	 * will require n steps.  Therefore, since all digit array sizes are
	 * signed 32-bit integers, the number of steps required is guaranteed to
	 * be less than 32.
	 * ----------
	 */
	step = 0;
	while ((ndigits[step] = src_ndigits) > 4)
	{
		/* Choose b so that a3 >= b/4, as described above */
		blen = src_ndigits / 4;
		if (blen * 4 == src_ndigits && arg->digits[0] < NBASE / 4)
			blen--;

		/* Number of digits in the next step (inner square root) */
		src_ndigits -= 2 * blen;
		step++;
	}

	/*
	 * First iteration (innermost square root and remainder):
	 *
	 * Here src_ndigits <= 4, and the input fits in an int64.  Its square root
	 * has at most 9 decimal digits, so estimate it using double precision
	 * arithmetic, which will in fact almost certainly return the correct
	 * result with no further correction required.
	 */
	arg_int64 = arg->digits[0];
	for (src_idx = 1; src_idx < src_ndigits; src_idx++)
	{
		arg_int64 *= NBASE;
		if (src_idx < arg->ndigits)
			arg_int64 += arg->digits[src_idx];
	}

	s_int64 = (int64) sqrt((double) arg_int64);
	r_int64 = arg_int64 - s_int64 * s_int64;

	/*
	 * Use Newton's method to correct the result, if necessary.
	 *
	 * This uses integer division with truncation to compute the truncated
	 * integer square root by iterating using the formula x -> (x + n/x) / 2.
	 * This is known to converge to isqrt(n), unless n+1 is a perfect square.
	 * If n+1 is a perfect square, the sequence will oscillate between the two
	 * values isqrt(n) and isqrt(n)+1, so we can be assured of convergence by
	 * checking the remainder.
	 */
	while (r_int64 < 0 || r_int64 > 2 * s_int64)
	{
		s_int64 = (s_int64 + arg_int64 / s_int64) / 2;
		r_int64 = arg_int64 - s_int64 * s_int64;
	}

	/*
	 * Iterations with src_ndigits <= 8:
	 *
	 * The next 1 or 2 iterations compute larger (outer) square roots with
	 * src_ndigits <= 8, so the result still fits in an int64 (even though the
	 * input no longer does) and we can continue to compute using int64
	 * variables to avoid more expensive numeric computations.
	 *
	 * It is fairly easy to see that there is no risk of the intermediate
	 * values below overflowing 64-bit integers.  In the worst case, the
	 * previous iteration will have computed a 3-digit square root (of a
	 * 6-digit input less than NBASE^6 / 4), so at the start of this
	 * iteration, s will be less than NBASE^3 / 2 = 10^12 / 2, and r will be
	 * less than 10^12.  In this case, blen will be 1, so numer will be less
	 * than 10^17, and denom will be less than 10^12 (and hence u will also be
	 * less than 10^12).  Finally, since q^2 = u*b + a0 - r, we can also be
	 * sure that q^2 < 10^17.  Therefore all these quantities fit comfortably
	 * in 64-bit integers.
	 */
	step--;
	while (step >= 0 && (src_ndigits = ndigits[step]) <= 8)
	{
		int			b;
		int			a0;
		int			a1;
		int			i;
		int64		numer;
		int64		denom;
		int64		q;
		int64		u;

		blen = (src_ndigits - src_idx) / 2;

		/* Extract a1 and a0, and compute b */
		a0 = 0;
		a1 = 0;
		b = 1;

		for (i = 0; i < blen; i++, src_idx++)
		{
			b *= NBASE;
			a1 *= NBASE;
			if (src_idx < arg->ndigits)
				a1 += arg->digits[src_idx];
		}

		for (i = 0; i < blen; i++, src_idx++)
		{
			a0 *= NBASE;
			if (src_idx < arg->ndigits)
				a0 += arg->digits[src_idx];
		}

		/* Compute (q,u) = DivRem(r*b + a1, 2*s) */
		numer = r_int64 * b + a1;
		denom = 2 * s_int64;
		q = numer / denom;
		u = numer - q * denom;

		/* Compute s = s*b + q and r = u*b + a0 - q^2 */
		s_int64 = s_int64 * b + q;
		r_int64 = u * b + a0 - q * q;

		if (r_int64 < 0)
		{
			/* s is too large by 1; set r += s, s--, r += s */
			r_int64 += s_int64;
			s_int64--;
			r_int64 += s_int64;
		}

		Assert(src_idx == src_ndigits); /* All input digits consumed */
		step--;
	}

	/*
	 * On platforms with 128-bit integer support, we can further delay the
	 * need to use numeric variables.
	 */
#ifdef HAVE_INT128
	if (step >= 0)
	{
		int128		s_int128;
		int128		r_int128;

		s_int128 = s_int64;
		r_int128 = r_int64;

		/*
		 * Iterations with src_ndigits <= 16:
		 *
		 * The result fits in an int128 (even though the input doesn't) so we
		 * use int128 variables to avoid more expensive numeric computations.
		 */
		while (step >= 0 && (src_ndigits = ndigits[step]) <= 16)
		{
			int64		b;
			int64		a0;
			int64		a1;
			int64		i;
			int128		numer;
			int128		denom;
			int128		q;
			int128		u;

			blen = (src_ndigits - src_idx) / 2;

			/* Extract a1 and a0, and compute b */
			a0 = 0;
			a1 = 0;
			b = 1;

			for (i = 0; i < blen; i++, src_idx++)
			{
				b *= NBASE;
				a1 *= NBASE;
				if (src_idx < arg->ndigits)
					a1 += arg->digits[src_idx];
			}

			for (i = 0; i < blen; i++, src_idx++)
			{
				a0 *= NBASE;
				if (src_idx < arg->ndigits)
					a0 += arg->digits[src_idx];
			}

			/* Compute (q,u) = DivRem(r*b + a1, 2*s) */
			numer = r_int128 * b + a1;
			denom = 2 * s_int128;
			q = numer / denom;
			u = numer - q * denom;

			/* Compute s = s*b + q and r = u*b + a0 - q^2 */
			s_int128 = s_int128 * b + q;
			r_int128 = u * b + a0 - q * q;

			if (r_int128 < 0)
			{
				/* s is too large by 1; set r += s, s--, r += s */
				r_int128 += s_int128;
				s_int128--;
				r_int128 += s_int128;
			}

			Assert(src_idx == src_ndigits); /* All input digits consumed */
			step--;
		}

		/*
		 * All remaining iterations require numeric variables.  Convert the
		 * integer values to NumericVar and continue.  Note that in the final
		 * iteration we don't need the remainder, so we can save a few cycles
		 * there by not fully computing it.
		 */
		int128_to_numericvar(s_int128, &s_var);
		if (step >= 0)
			int128_to_numericvar(r_int128, &r_var);
	}
	else
	{
		int64_to_numericvar(s_int64, &s_var);
		/* step < 0, so we certainly don't need r */
	}
#else							/* !HAVE_INT128 */
	int64_to_numericvar(s_int64, &s_var);
	if (step >= 0)
		int64_to_numericvar(r_int64, &r_var);
#endif							/* HAVE_INT128 */

	/*
	 * The remaining iterations with src_ndigits > 8 (or 16, if have int128)
	 * use numeric variables.
	 */
	while (step >= 0)
	{
		int			tmp_len;

		src_ndigits = ndigits[step];
		blen = (src_ndigits - src_idx) / 2;

		/* Extract a1 and a0 */
		if (src_idx < arg->ndigits)
		{
			tmp_len = Min(blen, arg->ndigits - src_idx);
			alloc_var(&a1_var, tmp_len);
			memcpy(a1_var.digits, arg->digits + src_idx,
				   tmp_len * sizeof(NumericDigit));
			a1_var.weight = blen - 1;
			a1_var.sign = NUMERIC_POS;
			a1_var.dscale = 0;
			strip_var(&a1_var);
		}
		else
		{
			zero_var(&a1_var);
			a1_var.dscale = 0;
		}
		src_idx += blen;

		if (src_idx < arg->ndigits)
		{
			tmp_len = Min(blen, arg->ndigits - src_idx);
			alloc_var(&a0_var, tmp_len);
			memcpy(a0_var.digits, arg->digits + src_idx,
				   tmp_len * sizeof(NumericDigit));
			a0_var.weight = blen - 1;
			a0_var.sign = NUMERIC_POS;
			a0_var.dscale = 0;
			strip_var(&a0_var);
		}
		else
		{
			zero_var(&a0_var);
			a0_var.dscale = 0;
		}
		src_idx += blen;

		/* Compute (q,u) = DivRem(r*b + a1, 2*s) */
		set_var_from_var(&r_var, &q_var);
		q_var.weight += blen;
		add_var(&q_var, &a1_var, &q_var);
		add_var(&s_var, &s_var, &u_var);
		div_mod_var(&q_var, &u_var, &q_var, &u_var);

		/* Compute s = s*b + q */
		s_var.weight += blen;
		add_var(&s_var, &q_var, &s_var);

		/*
		 * Compute r = u*b + a0 - q^2.
		 *
		 * In the final iteration, we don't actually need r; we just need to
		 * know whether it is negative, so that we know whether to adjust s.
		 * So instead of the final subtraction we can just compare.
		 */
		u_var.weight += blen;
		add_var(&u_var, &a0_var, &u_var);
		mul_var(&q_var, &q_var, &q_var, 0);

		if (step > 0)
		{
			/* Need r for later iterations */
			sub_var(&u_var, &q_var, &r_var);
			if (r_var.sign == NUMERIC_NEG)
			{
				/* s is too large by 1; set r += s, s--, r += s */
				add_var(&r_var, &s_var, &r_var);
				sub_var(&s_var, &const_one, &s_var);
				add_var(&r_var, &s_var, &r_var);
			}
		}
		else
		{
			/* Don't need r anymore, except to test if s is too large by 1 */
			if (cmp_var(&u_var, &q_var) < 0)
				sub_var(&s_var, &const_one, &s_var);
		}

		Assert(src_idx == src_ndigits); /* All input digits consumed */
		step--;
	}

	/*
	 * Construct the final result, rounding it to the requested precision.
	 */
	set_var_from_var(&s_var, result);
	result->weight = res_weight;
	result->sign = NUMERIC_POS;

	/* Round to target rscale (and set result->dscale) */
	round_var(result, rscale);

	/* Strip leading and trailing zeroes */
	strip_var(result);

	free_var(&s_var);
	free_var(&r_var);
	free_var(&a0_var);
	free_var(&a1_var);
	free_var(&q_var);
	free_var(&u_var);
}


/*
 * exp_var() -
 *
 *	Raise e to the power of x, computed to rscale fractional digits
 */
static void
exp_var(const NumericVar *arg, NumericVar *result, int rscale)
{
	NumericVar	x;
	NumericVar	elem;
	int			ni;
	double		val;
	int			dweight;
	int			ndiv2;
	int			sig_digits;
	int			local_rscale;

	init_var(&x);
	init_var(&elem);

	set_var_from_var(arg, &x);

	/*
	 * Estimate the dweight of the result using floating point arithmetic, so
	 * that we can choose an appropriate local rscale for the calculation.
	 */
	val = numericvar_to_double_no_overflow(&x);

	/* Guard against overflow/underflow */
	/* If you change this limit, see also power_var()'s limit */
	if (fabs(val) >= NUMERIC_MAX_RESULT_SCALE * 3)
	{
		if (val > 0)
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("value overflows numeric format")));
		zero_var(result);
		result->dscale = rscale;
		return;
	}

	/* decimal weight = log10(e^x) = x * log10(e) */
	dweight = (int) (val * 0.434294481903252);

	/*
	 * Reduce x to the range -0.01 <= x <= 0.01 (approximately) by dividing by
	 * 2^ndiv2, to improve the convergence rate of the Taylor series.
	 *
	 * Note that the overflow check above ensures that fabs(x) < 6000, which
	 * means that ndiv2 <= 20 here.
	 */
	if (fabs(val) > 0.01)
	{
		ndiv2 = 1;
		val /= 2;

		while (fabs(val) > 0.01)
		{
			ndiv2++;
			val /= 2;
		}

		local_rscale = x.dscale + ndiv2;
		div_var_int(&x, 1 << ndiv2, 0, &x, local_rscale, true);
	}
	else
		ndiv2 = 0;

	/*
	 * Set the scale for the Taylor series expansion.  The final result has
	 * (dweight + rscale + 1) significant digits.  In addition, we have to
	 * raise the Taylor series result to the power 2^ndiv2, which introduces
	 * an error of up to around log10(2^ndiv2) digits, so work with this many
	 * extra digits of precision (plus a few more for good measure).
	 */
	sig_digits = 1 + dweight + rscale + (int) (ndiv2 * 0.301029995663981);
	sig_digits = Max(sig_digits, 0) + 8;

	local_rscale = sig_digits - 1;

	/*
	 * Use the Taylor series
	 *
	 * exp(x) = 1 + x + x^2/2! + x^3/3! + ...
	 *
	 * Given the limited range of x, this should converge reasonably quickly.
	 * We run the series until the terms fall below the local_rscale limit.
	 */
	add_var(&const_one, &x, result);

	mul_var(&x, &x, &elem, local_rscale);
	ni = 2;
	div_var_int(&elem, ni, 0, &elem, local_rscale, true);

	while (elem.ndigits != 0)
	{
		add_var(result, &elem, result);

		mul_var(&elem, &x, &elem, local_rscale);
		ni++;
		div_var_int(&elem, ni, 0, &elem, local_rscale, true);
	}

	/*
	 * Compensate for the argument range reduction.  Since the weight of the
	 * result doubles with each multiplication, we can reduce the local rscale
	 * as we proceed.
	 */
	while (ndiv2-- > 0)
	{
		local_rscale = sig_digits - result->weight * 2 * DEC_DIGITS;
		local_rscale = Max(local_rscale, NUMERIC_MIN_DISPLAY_SCALE);
		mul_var(result, result, result, local_rscale);
	}

	/* Round to requested rscale */
	round_var(result, rscale);

	free_var(&x);
	free_var(&elem);
}


/*
 * Estimate the dweight of the most significant decimal digit of the natural
 * logarithm of a number.
 *
 * Essentially, we're approximating log10(abs(ln(var))).  This is used to
 * determine the appropriate rscale when computing natural logarithms.
 *
 * Note: many callers call this before range-checking the input.  Therefore,
 * we must be robust against values that are invalid to apply ln() to.
 * We don't wish to throw an error here, so just return zero in such cases.
 */
static int
estimate_ln_dweight(const NumericVar *var)
{
	int			ln_dweight;

	/* Caller should fail on ln(negative), but for the moment return zero */
	if (var->sign != NUMERIC_POS)
		return 0;

	if (cmp_var(var, &const_zero_point_nine) >= 0 &&
		cmp_var(var, &const_one_point_one) <= 0)
	{
		/*
		 * 0.9 <= var <= 1.1
		 *
		 * ln(var) has a negative weight (possibly very large).  To get a
		 * reasonably accurate result, estimate it using ln(1+x) ~= x.
		 */
		NumericVar	x;

		init_var(&x);
		sub_var(var, &const_one, &x);

		if (x.ndigits > 0)
		{
			/* Use weight of most significant decimal digit of x */
			ln_dweight = x.weight * DEC_DIGITS + (int) log10(x.digits[0]);
		}
		else
		{
			/* x = 0.  Since ln(1) = 0 exactly, we don't need extra digits */
			ln_dweight = 0;
		}

		free_var(&x);
	}
	else
	{
		/*
		 * Estimate the logarithm using the first couple of digits from the
		 * input number.  This will give an accurate result whenever the input
		 * is not too close to 1.
		 */
		if (var->ndigits > 0)
		{
			int			digits;
			int			dweight;
			double		ln_var;

			digits = var->digits[0];
			dweight = var->weight * DEC_DIGITS;

			if (var->ndigits > 1)
			{
				digits = digits * NBASE + var->digits[1];
				dweight -= DEC_DIGITS;
			}

			/*----------
			 * We have var ~= digits * 10^dweight
			 * so ln(var) ~= ln(digits) + dweight * ln(10)
			 *----------
			 */
			ln_var = log((double) digits) + dweight * 2.302585092994046;
			ln_dweight = (int) log10(fabs(ln_var));
		}
		else
		{
			/* Caller should fail on ln(0), but for the moment return zero */
			ln_dweight = 0;
		}
	}

	return ln_dweight;
}


/*
 * ln_var() -
 *
 *	Compute the natural log of x
 */
static void
ln_var(const NumericVar *arg, NumericVar *result, int rscale)
{
	NumericVar	x;
	NumericVar	xx;
	int			ni;
	NumericVar	elem;
	NumericVar	fact;
	int			nsqrt;
	int			local_rscale;
	int			cmp;

	cmp = cmp_var(arg, &const_zero);
	if (cmp == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_ARGUMENT_FOR_LOG),
				 errmsg("cannot take logarithm of zero")));
	else if (cmp < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_ARGUMENT_FOR_LOG),
				 errmsg("cannot take logarithm of a negative number")));

	init_var(&x);
	init_var(&xx);
	init_var(&elem);
	init_var(&fact);

	set_var_from_var(arg, &x);
	set_var_from_var(&const_two, &fact);

	/*
	 * Reduce input into range 0.9 < x < 1.1 with repeated sqrt() operations.
	 *
	 * The final logarithm will have up to around rscale+6 significant digits.
	 * Each sqrt() will roughly halve the weight of x, so adjust the local
	 * rscale as we work so that we keep this many significant digits at each
	 * step (plus a few more for good measure).
	 *
	 * Note that we allow local_rscale < 0 during this input reduction
	 * process, which implies rounding before the decimal point.  sqrt_var()
	 * explicitly supports this, and it significantly reduces the work
	 * required to reduce very large inputs to the required range.  Once the
	 * input reduction is complete, x.weight will be 0 and its display scale
	 * will be non-negative again.
	 */
	nsqrt = 0;
	while (cmp_var(&x, &const_zero_point_nine) <= 0)
	{
		local_rscale = rscale - x.weight * DEC_DIGITS / 2 + 8;
		sqrt_var(&x, &x, local_rscale);
		mul_var(&fact, &const_two, &fact, 0);
		nsqrt++;
	}
	while (cmp_var(&x, &const_one_point_one) >= 0)
	{
		local_rscale = rscale - x.weight * DEC_DIGITS / 2 + 8;
		sqrt_var(&x, &x, local_rscale);
		mul_var(&fact, &const_two, &fact, 0);
		nsqrt++;
	}

	/*
	 * We use the Taylor series for 0.5 * ln((1+z)/(1-z)),
	 *
	 * z + z^3/3 + z^5/5 + ...
	 *
	 * where z = (x-1)/(x+1) is in the range (approximately) -0.053 .. 0.048
	 * due to the above range-reduction of x.
	 *
	 * The convergence of this is not as fast as one would like, but is
	 * tolerable given that z is small.
	 *
	 * The Taylor series result will be multiplied by 2^(nsqrt+1), which has a
	 * decimal weight of (nsqrt+1) * log10(2), so work with this many extra
	 * digits of precision (plus a few more for good measure).
	 */
	local_rscale = rscale + (int) ((nsqrt + 1) * 0.301029995663981) + 8;

	sub_var(&x, &const_one, result);
	add_var(&x, &const_one, &elem);
	div_var(result, &elem, result, local_rscale, true, false);
	set_var_from_var(result, &xx);
	mul_var(result, result, &x, local_rscale);

	ni = 1;

	for (;;)
	{
		ni += 2;
		mul_var(&xx, &x, &xx, local_rscale);
		div_var_int(&xx, ni, 0, &elem, local_rscale, true);

		if (elem.ndigits == 0)
			break;

		add_var(result, &elem, result);

		if (elem.weight < (result->weight - local_rscale * 2 / DEC_DIGITS))
			break;
	}

	/* Compensate for argument range reduction, round to requested rscale */
	mul_var(result, &fact, result, rscale);

	free_var(&x);
	free_var(&xx);
	free_var(&elem);
	free_var(&fact);
}


/*
 * log_var() -
 *
 *	Compute the logarithm of num in a given base.
 *
 *	Note: this routine chooses dscale of the result.
 */
static void
log_var(const NumericVar *base, const NumericVar *num, NumericVar *result)
{
	NumericVar	ln_base;
	NumericVar	ln_num;
	int			ln_base_dweight;
	int			ln_num_dweight;
	int			result_dweight;
	int			rscale;
	int			ln_base_rscale;
	int			ln_num_rscale;

	init_var(&ln_base);
	init_var(&ln_num);

	/* Estimated dweights of ln(base), ln(num) and the final result */
	ln_base_dweight = estimate_ln_dweight(base);
	ln_num_dweight = estimate_ln_dweight(num);
	result_dweight = ln_num_dweight - ln_base_dweight;

	/*
	 * Select the scale of the result so that it will have at least
	 * NUMERIC_MIN_SIG_DIGITS significant digits and is not less than either
	 * input's display scale.
	 */
	rscale = NUMERIC_MIN_SIG_DIGITS - result_dweight;
	rscale = Max(rscale, base->dscale);
	rscale = Max(rscale, num->dscale);
	rscale = Max(rscale, NUMERIC_MIN_DISPLAY_SCALE);
	rscale = Min(rscale, NUMERIC_MAX_DISPLAY_SCALE);

	/*
	 * Set the scales for ln(base) and ln(num) so that they each have more
	 * significant digits than the final result.
	 */
	ln_base_rscale = rscale + result_dweight - ln_base_dweight + 8;
	ln_base_rscale = Max(ln_base_rscale, NUMERIC_MIN_DISPLAY_SCALE);

	ln_num_rscale = rscale + result_dweight - ln_num_dweight + 8;
	ln_num_rscale = Max(ln_num_rscale, NUMERIC_MIN_DISPLAY_SCALE);

	/* Form natural logarithms */
	ln_var(base, &ln_base, ln_base_rscale);
	ln_var(num, &ln_num, ln_num_rscale);

	/* Divide and round to the required scale */
	div_var(&ln_num, &ln_base, result, rscale, true, false);

	free_var(&ln_num);
	free_var(&ln_base);
}


/*
 * power_var() -
 *
 *	Raise base to the power of exp
 *
 *	Note: this routine chooses dscale of the result.
 */
static void
power_var(const NumericVar *base, const NumericVar *exp, NumericVar *result)
{
	int			res_sign;
	NumericVar	abs_base;
	NumericVar	ln_base;
	NumericVar	ln_num;
	int			ln_dweight;
	int			rscale;
	int			sig_digits;
	int			local_rscale;
	double		val;

	/* If exp can be represented as an integer, use power_var_int */
	if (exp->ndigits == 0 || exp->ndigits <= exp->weight + 1)
	{
		/* exact integer, but does it fit in int? */
		int64		expval64;

		if (numericvar_to_int64(exp, &expval64))
		{
			if (expval64 >= PG_INT32_MIN && expval64 <= PG_INT32_MAX)
			{
				/* Okay, use power_var_int */
				power_var_int(base, (int) expval64, exp->dscale, result);
				return;
			}
		}
	}

	/*
	 * This avoids log(0) for cases of 0 raised to a non-integer.  0 ^ 0 is
	 * handled by power_var_int().
	 */
	if (cmp_var(base, &const_zero) == 0)
	{
		set_var_from_var(&const_zero, result);
		result->dscale = NUMERIC_MIN_SIG_DIGITS;	/* no need to round */
		return;
	}

	init_var(&abs_base);
	init_var(&ln_base);
	init_var(&ln_num);

	/*
	 * If base is negative, insist that exp be an integer.  The result is then
	 * positive if exp is even and negative if exp is odd.
	 */
	if (base->sign == NUMERIC_NEG)
	{
		/*
		 * Check that exp is an integer.  This error code is defined by the
		 * SQL standard, and matches other errors in numeric_power().
		 */
		if (exp->ndigits > 0 && exp->ndigits > exp->weight + 1)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION),
					 errmsg("a negative number raised to a non-integer power yields a complex result")));

		/* Test if exp is odd or even */
		if (exp->ndigits > 0 && exp->ndigits == exp->weight + 1 &&
			(exp->digits[exp->ndigits - 1] & 1))
			res_sign = NUMERIC_NEG;
		else
			res_sign = NUMERIC_POS;

		/* Then work with abs(base) below */
		set_var_from_var(base, &abs_base);
		abs_base.sign = NUMERIC_POS;
		base = &abs_base;
	}
	else
		res_sign = NUMERIC_POS;

	/*----------
	 * Decide on the scale for the ln() calculation.  For this we need an
	 * estimate of the weight of the result, which we obtain by doing an
	 * initial low-precision calculation of exp * ln(base).
	 *
	 * We want result = e ^ (exp * ln(base))
	 * so result dweight = log10(result) = exp * ln(base) * log10(e)
	 *
	 * We also perform a crude overflow test here so that we can exit early if
	 * the full-precision result is sure to overflow, and to guard against
	 * integer overflow when determining the scale for the real calculation.
	 * exp_var() supports inputs up to NUMERIC_MAX_RESULT_SCALE * 3, so the
	 * result will overflow if exp * ln(base) >= NUMERIC_MAX_RESULT_SCALE * 3.
	 * Since the values here are only approximations, we apply a small fuzz
	 * factor to this overflow test and let exp_var() determine the exact
	 * overflow threshold so that it is consistent for all inputs.
	 *----------
	 */
	ln_dweight = estimate_ln_dweight(base);

	/*
	 * Set the scale for the low-precision calculation, computing ln(base) to
	 * around 8 significant digits.  Note that ln_dweight may be as small as
	 * -NUMERIC_DSCALE_MAX, so the scale may exceed NUMERIC_MAX_DISPLAY_SCALE
	 * here.
	 */
	local_rscale = 8 - ln_dweight;
	local_rscale = Max(local_rscale, NUMERIC_MIN_DISPLAY_SCALE);

	ln_var(base, &ln_base, local_rscale);

	mul_var(&ln_base, exp, &ln_num, local_rscale);

	val = numericvar_to_double_no_overflow(&ln_num);

	/* initial overflow/underflow test with fuzz factor */
	if (fabs(val) > NUMERIC_MAX_RESULT_SCALE * 3.01)
	{
		if (val > 0)
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("value overflows numeric format")));
		zero_var(result);
		result->dscale = NUMERIC_MAX_DISPLAY_SCALE;
		return;
	}

	val *= 0.434294481903252;	/* approximate decimal result weight */

	/* choose the result scale */
	rscale = NUMERIC_MIN_SIG_DIGITS - (int) val;
	rscale = Max(rscale, base->dscale);
	rscale = Max(rscale, exp->dscale);
	rscale = Max(rscale, NUMERIC_MIN_DISPLAY_SCALE);
	rscale = Min(rscale, NUMERIC_MAX_DISPLAY_SCALE);

	/* significant digits required in the result */
	sig_digits = rscale + (int) val;
	sig_digits = Max(sig_digits, 0);

	/* set the scale for the real exp * ln(base) calculation */
	local_rscale = sig_digits - ln_dweight + 8;
	local_rscale = Max(local_rscale, NUMERIC_MIN_DISPLAY_SCALE);

	/* and do the real calculation */

	ln_var(base, &ln_base, local_rscale);

	mul_var(&ln_base, exp, &ln_num, local_rscale);

	exp_var(&ln_num, result, rscale);

	if (res_sign == NUMERIC_NEG && result->ndigits > 0)
		result->sign = NUMERIC_NEG;

	free_var(&ln_num);
	free_var(&ln_base);
	free_var(&abs_base);
}

/*
 * power_var_int() -
 *
 *	Raise base to the power of exp, where exp is an integer.
 *
 *	Note: this routine chooses dscale of the result.
 */
static void
power_var_int(const NumericVar *base, int exp, int exp_dscale,
			  NumericVar *result)
{
	double		f;
	int			p;
	int			i;
	int			rscale;
	int			sig_digits;
	unsigned int mask;
	bool		neg;
	NumericVar	base_prod;
	int			local_rscale;

	/*
	 * Choose the result scale.  For this we need an estimate of the decimal
	 * weight of the result, which we obtain by approximating using double
	 * precision arithmetic.
	 *
	 * We also perform crude overflow/underflow tests here so that we can exit
	 * early if the result is sure to overflow/underflow, and to guard against
	 * integer overflow when choosing the result scale.
	 */
	if (base->ndigits != 0)
	{
		/*----------
		 * Choose f (double) and p (int) such that base ~= f * 10^p.
		 * Then log10(result) = log10(base^exp) ~= exp * (log10(f) + p).
		 *----------
		 */
		f = base->digits[0];
		p = base->weight * DEC_DIGITS;

		for (i = 1; i < base->ndigits && i * DEC_DIGITS < 16; i++)
		{
			f = f * NBASE + base->digits[i];
			p -= DEC_DIGITS;
		}

		f = exp * (log10(f) + p);	/* approximate decimal result weight */
	}
	else
		f = 0;					/* result is 0 or 1 (weight 0), or error */

	/* overflow/underflow tests with fuzz factors */
	if (f > (NUMERIC_WEIGHT_MAX + 1) * DEC_DIGITS)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("value overflows numeric format")));
	if (f + 1 < -NUMERIC_MAX_DISPLAY_SCALE)
	{
		zero_var(result);
		result->dscale = NUMERIC_MAX_DISPLAY_SCALE;
		return;
	}

	/*
	 * Choose the result scale in the same way as power_var(), so it has at
	 * least NUMERIC_MIN_SIG_DIGITS significant digits and is not less than
	 * either input's display scale.
	 */
	rscale = NUMERIC_MIN_SIG_DIGITS - (int) f;
	rscale = Max(rscale, base->dscale);
	rscale = Max(rscale, exp_dscale);
	rscale = Max(rscale, NUMERIC_MIN_DISPLAY_SCALE);
	rscale = Min(rscale, NUMERIC_MAX_DISPLAY_SCALE);

	/* Handle some common special cases, as well as corner cases */
	switch (exp)
	{
		case 0:

			/*
			 * While 0 ^ 0 can be either 1 or indeterminate (error), we treat
			 * it as 1 because most programming languages do this. SQL:2003
			 * also requires a return value of 1.
			 * https://en.wikipedia.org/wiki/Exponentiation#Zero_to_the_zero_power
			 */
			set_var_from_var(&const_one, result);
			result->dscale = rscale;	/* no need to round */
			return;
		case 1:
			set_var_from_var(base, result);
			round_var(result, rscale);
			return;
		case -1:
			div_var(&const_one, base, result, rscale, true, true);
			return;
		case 2:
			mul_var(base, base, result, rscale);
			return;
		default:
			break;
	}

	/* Handle the special case where the base is zero */
	if (base->ndigits == 0)
	{
		if (exp < 0)
			ereport(ERROR,
					(errcode(ERRCODE_DIVISION_BY_ZERO),
					 errmsg("division by zero")));
		zero_var(result);
		result->dscale = rscale;
		return;
	}

	/*
	 * The general case repeatedly multiplies base according to the bit
	 * pattern of exp.
	 *
	 * The local rscale used for each multiplication is varied to keep a fixed
	 * number of significant digits, sufficient to give the required result
	 * scale.
	 */

	/*
	 * Approximate number of significant digits in the result.  Note that the
	 * underflow test above, together with the choice of rscale, ensures that
	 * this approximation is necessarily > 0.
	 */
	sig_digits = 1 + rscale + (int) f;

	/*
	 * The multiplications to produce the result may introduce an error of up
	 * to around log10(abs(exp)) digits, so work with this many extra digits
	 * of precision (plus a few more for good measure).
	 */
	sig_digits += (int) log(fabs((double) exp)) + 8;

	/*
	 * Now we can proceed with the multiplications.
	 */
	neg = (exp < 0);
	mask = pg_abs_s32(exp);

	init_var(&base_prod);
	set_var_from_var(base, &base_prod);

	if (mask & 1)
		set_var_from_var(base, result);
	else
		set_var_from_var(&const_one, result);

	while ((mask >>= 1) > 0)
	{
		/*
		 * Do the multiplications using rscales large enough to hold the
		 * results to the required number of significant digits, but don't
		 * waste time by exceeding the scales of the numbers themselves.
		 */
		local_rscale = sig_digits - 2 * base_prod.weight * DEC_DIGITS;
		local_rscale = Min(local_rscale, 2 * base_prod.dscale);
		local_rscale = Max(local_rscale, NUMERIC_MIN_DISPLAY_SCALE);

		mul_var(&base_prod, &base_prod, &base_prod, local_rscale);

		if (mask & 1)
		{
			local_rscale = sig_digits -
				(base_prod.weight + result->weight) * DEC_DIGITS;
			local_rscale = Min(local_rscale,
							   base_prod.dscale + result->dscale);
			local_rscale = Max(local_rscale, NUMERIC_MIN_DISPLAY_SCALE);

			mul_var(&base_prod, result, result, local_rscale);
		}

		/*
		 * When abs(base) > 1, the number of digits to the left of the decimal
		 * point in base_prod doubles at each iteration, so if exp is large we
		 * could easily spend large amounts of time and memory space doing the
		 * multiplications.  But once the weight exceeds what will fit in
		 * int16, the final result is guaranteed to overflow (or underflow, if
		 * exp < 0), so we can give up before wasting too many cycles.
		 */
		if (base_prod.weight > NUMERIC_WEIGHT_MAX ||
			result->weight > NUMERIC_WEIGHT_MAX)
		{
			/* overflow, unless neg, in which case result should be 0 */
			if (!neg)
				ereport(ERROR,
						(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
						 errmsg("value overflows numeric format")));
			zero_var(result);
			neg = false;
			break;
		}
	}

	free_var(&base_prod);

	/* Compensate for input sign, and round to requested rscale */
	if (neg)
		div_var(&const_one, result, result, rscale, true, false);
	else
		round_var(result, rscale);
}

/*
 * power_ten_int() -
 *
 *	Raise ten to the power of exp, where exp is an integer.  Note that unlike
 *	power_var_int(), this does no overflow/underflow checking or rounding.
 */
static void
power_ten_int(int exp, NumericVar *result)
{
	/* Construct the result directly, starting from 10^0 = 1 */
	set_var_from_var(&const_one, result);

	/* Scale needed to represent the result exactly */
	result->dscale = exp < 0 ? -exp : 0;

	/* Base-NBASE weight of result and remaining exponent */
	if (exp >= 0)
		result->weight = exp / DEC_DIGITS;
	else
		result->weight = (exp + 1) / DEC_DIGITS - 1;

	exp -= result->weight * DEC_DIGITS;

	/* Final adjustment of the result's single NBASE digit */
	while (exp-- > 0)
		result->digits[0] *= 10;
}

/*
 * random_var() - return a random value in the range [rmin, rmax].
 */
static void
random_var(pg_prng_state *state, const NumericVar *rmin,
		   const NumericVar *rmax, NumericVar *result)
{
	int			rscale;
	NumericVar	rlen;
	int			res_ndigits;
	int			n;
	int			pow10;
	int			i;
	uint64		rlen64;
	int			rlen64_ndigits;

	rscale = Max(rmin->dscale, rmax->dscale);

	/* Compute rlen = rmax - rmin and check the range bounds */
	init_var(&rlen);
	sub_var(rmax, rmin, &rlen);

	if (rlen.sign == NUMERIC_NEG)
		ereport(ERROR,
				errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("lower bound must be less than or equal to upper bound"));

	/* Special case for an empty range */
	if (rlen.ndigits == 0)
	{
		set_var_from_var(rmin, result);
		result->dscale = rscale;
		free_var(&rlen);
		return;
	}

	/*
	 * Otherwise, select a random value in the range [0, rlen = rmax - rmin],
	 * and shift it to the required range by adding rmin.
	 */

	/* Required result digits */
	res_ndigits = rlen.weight + 1 + (rscale + DEC_DIGITS - 1) / DEC_DIGITS;

	/*
	 * To get the required rscale, the final result digit must be a multiple
	 * of pow10 = 10^n, where n = (-rscale) mod DEC_DIGITS.
	 */
	n = ((rscale + DEC_DIGITS - 1) / DEC_DIGITS) * DEC_DIGITS - rscale;
	pow10 = 1;
	for (i = 0; i < n; i++)
		pow10 *= 10;

	/*
	 * To choose a random value uniformly from the range [0, rlen], we choose
	 * from the slightly larger range [0, rlen2], where rlen2 is formed from
	 * rlen by copying the first 4 NBASE digits, and setting all remaining
	 * decimal digits to "9".
	 *
	 * Without loss of generality, we can ignore the weight of rlen2 and treat
	 * it as a pure integer for the purposes of this discussion.  The process
	 * above gives rlen2 + 1 = rlen64 * 10^N, for some integer N, where rlen64
	 * is a 64-bit integer formed from the first 4 NBASE digits copied from
	 * rlen.  Since this trivially factors into smaller pieces that fit in
	 * 64-bit integers, the task of choosing a random value uniformly from the
	 * rlen2 + 1 possible values in [0, rlen2] is much simpler.
	 *
	 * If the random value selected is too large, it is rejected, and we try
	 * again until we get a result <= rlen, ensuring that the overall result
	 * is uniform (no particular value is any more likely than any other).
	 *
	 * Since rlen64 holds 4 NBASE digits from rlen, it contains at least
	 * DEC_DIGITS * 3 + 1 decimal digits (i.e., at least 13 decimal digits,
	 * when DEC_DIGITS is 4). Therefore the probability of needing to reject
	 * the value chosen and retry is less than 1e-13.
	 */
	rlen64 = (uint64) rlen.digits[0];
	rlen64_ndigits = 1;
	while (rlen64_ndigits < res_ndigits && rlen64_ndigits < 4)
	{
		rlen64 *= NBASE;
		if (rlen64_ndigits < rlen.ndigits)
			rlen64 += rlen.digits[rlen64_ndigits];
		rlen64_ndigits++;
	}

	/* Loop until we get a result <= rlen */
	do
	{
		NumericDigit *res_digits;
		uint64		rand;
		int			whole_ndigits;

		alloc_var(result, res_ndigits);
		result->sign = NUMERIC_POS;
		result->weight = rlen.weight;
		result->dscale = rscale;
		res_digits = result->digits;

		/*
		 * Set the first rlen64_ndigits using a random value in [0, rlen64].
		 *
		 * If this is the whole result, and rscale is not a multiple of
		 * DEC_DIGITS (pow10 from above is not 1), then we need this to be a
		 * multiple of pow10.
		 */
		if (rlen64_ndigits == res_ndigits && pow10 != 1)
			rand = pg_prng_uint64_range(state, 0, rlen64 / pow10) * pow10;
		else
			rand = pg_prng_uint64_range(state, 0, rlen64);

		for (i = rlen64_ndigits - 1; i >= 0; i--)
		{
			res_digits[i] = (NumericDigit) (rand % NBASE);
			rand = rand / NBASE;
		}

		/*
		 * Set the remaining digits to random values in range [0, NBASE),
		 * noting that the last digit needs to be a multiple of pow10.
		 */
		whole_ndigits = res_ndigits;
		if (pow10 != 1)
			whole_ndigits--;

		/* Set whole digits in groups of 4 for best performance */
		i = rlen64_ndigits;
		while (i < whole_ndigits - 3)
		{
			rand = pg_prng_uint64_range(state, 0,
										(uint64) NBASE * NBASE * NBASE * NBASE - 1);
			res_digits[i++] = (NumericDigit) (rand % NBASE);
			rand = rand / NBASE;
			res_digits[i++] = (NumericDigit) (rand % NBASE);
			rand = rand / NBASE;
			res_digits[i++] = (NumericDigit) (rand % NBASE);
			rand = rand / NBASE;
			res_digits[i++] = (NumericDigit) rand;
		}

		/* Remaining whole digits */
		while (i < whole_ndigits)
		{
			rand = pg_prng_uint64_range(state, 0, NBASE - 1);
			res_digits[i++] = (NumericDigit) rand;
		}

		/* Final partial digit (multiple of pow10) */
		if (i < res_ndigits)
		{
			rand = pg_prng_uint64_range(state, 0, NBASE / pow10 - 1) * pow10;
			res_digits[i] = (NumericDigit) rand;
		}

		/* Remove leading/trailing zeroes */
		strip_var(result);

		/* If result > rlen, try again */

	} while (cmp_var(result, &rlen) > 0);

	/* Offset the result to the required range */
	add_var(result, rmin, result);

	free_var(&rlen);
}


/* ----------------------------------------------------------------------
 *
 * Following are the lowest level functions that operate unsigned
 * on the variable level
 *
 * ----------------------------------------------------------------------
 */


/* ----------
 * cmp_abs() -
 *
 *	Compare the absolute values of var1 and var2
 *	Returns:	-1 for ABS(var1) < ABS(var2)
 *				0  for ABS(var1) == ABS(var2)
 *				1  for ABS(var1) > ABS(var2)
 * ----------
 */
static int
cmp_abs(const NumericVar *var1, const NumericVar *var2)
{
	return cmp_abs_common(var1->digits, var1->ndigits, var1->weight,
						  var2->digits, var2->ndigits, var2->weight);
}

/* ----------
 * cmp_abs_common() -
 *
 *	Main routine of cmp_abs(). This function can be used by both
 *	NumericVar and Numeric.
 * ----------
 */
static int
cmp_abs_common(const NumericDigit *var1digits, int var1ndigits, int var1weight,
			   const NumericDigit *var2digits, int var2ndigits, int var2weight)
{
	int			i1 = 0;
	int			i2 = 0;

	/* Check any digits before the first common digit */

	while (var1weight > var2weight && i1 < var1ndigits)
	{
		if (var1digits[i1++] != 0)
			return 1;
		var1weight--;
	}
	while (var2weight > var1weight && i2 < var2ndigits)
	{
		if (var2digits[i2++] != 0)
			return -1;
		var2weight--;
	}

	/* At this point, either w1 == w2 or we've run out of digits */

	if (var1weight == var2weight)
	{
		while (i1 < var1ndigits && i2 < var2ndigits)
		{
			int			stat = var1digits[i1++] - var2digits[i2++];

			if (stat)
			{
				if (stat > 0)
					return 1;
				return -1;
			}
		}
	}

	/*
	 * At this point, we've run out of digits on one side or the other; so any
	 * remaining nonzero digits imply that side is larger
	 */
	while (i1 < var1ndigits)
	{
		if (var1digits[i1++] != 0)
			return 1;
	}
	while (i2 < var2ndigits)
	{
		if (var2digits[i2++] != 0)
			return -1;
	}

	return 0;
}


/*
 * add_abs() -
 *
 *	Add the absolute values of two variables into result.
 *	result might point to one of the operands without danger.
 */
static void
add_abs(const NumericVar *var1, const NumericVar *var2, NumericVar *result)
{
	NumericDigit *res_buf;
	NumericDigit *res_digits;
	int			res_ndigits;
	int			res_weight;
	int			res_rscale,
				rscale1,
				rscale2;
	int			res_dscale;
	int			i,
				i1,
				i2;
	int			carry = 0;

	/* copy these values into local vars for speed in inner loop */
	int			var1ndigits = var1->ndigits;
	int			var2ndigits = var2->ndigits;
	NumericDigit *var1digits = var1->digits;
	NumericDigit *var2digits = var2->digits;

	res_weight = Max(var1->weight, var2->weight) + 1;

	res_dscale = Max(var1->dscale, var2->dscale);

	/* Note: here we are figuring rscale in base-NBASE digits */
	rscale1 = var1->ndigits - var1->weight - 1;
	rscale2 = var2->ndigits - var2->weight - 1;
	res_rscale = Max(rscale1, rscale2);

	res_ndigits = res_rscale + res_weight + 1;
	if (res_ndigits <= 0)
		res_ndigits = 1;

	res_buf = digitbuf_alloc(res_ndigits + 1);
	res_buf[0] = 0;				/* spare digit for later rounding */
	res_digits = res_buf + 1;

	i1 = res_rscale + var1->weight + 1;
	i2 = res_rscale + var2->weight + 1;
	for (i = res_ndigits - 1; i >= 0; i--)
	{
		i1--;
		i2--;
		if (i1 >= 0 && i1 < var1ndigits)
			carry += var1digits[i1];
		if (i2 >= 0 && i2 < var2ndigits)
			carry += var2digits[i2];

		if (carry >= NBASE)
		{
			res_digits[i] = carry - NBASE;
			carry = 1;
		}
		else
		{
			res_digits[i] = carry;
			carry = 0;
		}
	}

	Assert(carry == 0);			/* else we failed to allow for carry out */

	digitbuf_free(result->buf);
	result->ndigits = res_ndigits;
	result->buf = res_buf;
	result->digits = res_digits;
	result->weight = res_weight;
	result->dscale = res_dscale;

	/* Remove leading/trailing zeroes */
	strip_var(result);
}


/*
 * sub_abs()
 *
 *	Subtract the absolute value of var2 from the absolute value of var1
 *	and store in result. result might point to one of the operands
 *	without danger.
 *
 *	ABS(var1) MUST BE GREATER OR EQUAL ABS(var2) !!!
 */
static void
sub_abs(const NumericVar *var1, const NumericVar *var2, NumericVar *result)
{
	NumericDigit *res_buf;
	NumericDigit *res_digits;
	int			res_ndigits;
	int			res_weight;
	int			res_rscale,
				rscale1,
				rscale2;
	int			res_dscale;
	int			i,
				i1,
				i2;
	int			borrow = 0;

	/* copy these values into local vars for speed in inner loop */
	int			var1ndigits = var1->ndigits;
	int			var2ndigits = var2->ndigits;
	NumericDigit *var1digits = var1->digits;
	NumericDigit *var2digits = var2->digits;

	res_weight = var1->weight;

	res_dscale = Max(var1->dscale, var2->dscale);

	/* Note: here we are figuring rscale in base-NBASE digits */
	rscale1 = var1->ndigits - var1->weight - 1;
	rscale2 = var2->ndigits - var2->weight - 1;
	res_rscale = Max(rscale1, rscale2);

	res_ndigits = res_rscale + res_weight + 1;
	if (res_ndigits <= 0)
		res_ndigits = 1;

	res_buf = digitbuf_alloc(res_ndigits + 1);
	res_buf[0] = 0;				/* spare digit for later rounding */
	res_digits = res_buf + 1;

	i1 = res_rscale + var1->weight + 1;
	i2 = res_rscale + var2->weight + 1;
	for (i = res_ndigits - 1; i >= 0; i--)
	{
		i1--;
		i2--;
		if (i1 >= 0 && i1 < var1ndigits)
			borrow += var1digits[i1];
		if (i2 >= 0 && i2 < var2ndigits)
			borrow -= var2digits[i2];

		if (borrow < 0)
		{
			res_digits[i] = borrow + NBASE;
			borrow = -1;
		}
		else
		{
			res_digits[i] = borrow;
			borrow = 0;
		}
	}

	Assert(borrow == 0);		/* else caller gave us var1 < var2 */

	digitbuf_free(result->buf);
	result->ndigits = res_ndigits;
	result->buf = res_buf;
	result->digits = res_digits;
	result->weight = res_weight;
	result->dscale = res_dscale;

	/* Remove leading/trailing zeroes */
	strip_var(result);
}

/*
 * round_var
 *
 * Round the value of a variable to no more than rscale decimal digits
 * after the decimal point.  NOTE: we allow rscale < 0 here, implying
 * rounding before the decimal point.
 */
static void
round_var(NumericVar *var, int rscale)
{
	NumericDigit *digits = var->digits;
	int			di;
	int			ndigits;
	int			carry;

	var->dscale = rscale;

	/* decimal digits wanted */
	di = (var->weight + 1) * DEC_DIGITS + rscale;

	/*
	 * If di = 0, the value loses all digits, but could round up to 1 if its
	 * first extra digit is >= 5.  If di < 0 the result must be 0.
	 */
	if (di < 0)
	{
		var->ndigits = 0;
		var->weight = 0;
		var->sign = NUMERIC_POS;
	}
	else
	{
		/* NBASE digits wanted */
		ndigits = (di + DEC_DIGITS - 1) / DEC_DIGITS;

		/* 0, or number of decimal digits to keep in last NBASE digit */
		di %= DEC_DIGITS;

		if (ndigits < var->ndigits ||
			(ndigits == var->ndigits && di > 0))
		{
			var->ndigits = ndigits;

#if DEC_DIGITS == 1
			/* di must be zero */
			carry = (digits[ndigits] >= HALF_NBASE) ? 1 : 0;
#else
			if (di == 0)
				carry = (digits[ndigits] >= HALF_NBASE) ? 1 : 0;
			else
			{
				/* Must round within last NBASE digit */
				int			extra,
							pow10;

#if DEC_DIGITS == 4
				pow10 = round_powers[di];
#elif DEC_DIGITS == 2
				pow10 = 10;
#else
#error unsupported NBASE
#endif
				extra = digits[--ndigits] % pow10;
				digits[ndigits] -= extra;
				carry = 0;
				if (extra >= pow10 / 2)
				{
					pow10 += digits[ndigits];
					if (pow10 >= NBASE)
					{
						pow10 -= NBASE;
						carry = 1;
					}
					digits[ndigits] = pow10;
				}
			}
#endif

			/* Propagate carry if needed */
			while (carry)
			{
				carry += digits[--ndigits];
				if (carry >= NBASE)
				{
					digits[ndigits] = carry - NBASE;
					carry = 1;
				}
				else
				{
					digits[ndigits] = carry;
					carry = 0;
				}
			}

			if (ndigits < 0)
			{
				Assert(ndigits == -1);	/* better not have added > 1 digit */
				Assert(var->digits > var->buf);
				var->digits--;
				var->ndigits++;
				var->weight++;
			}
		}
	}
}

/*
 * trunc_var
 *
 * Truncate (towards zero) the value of a variable at rscale decimal digits
 * after the decimal point.  NOTE: we allow rscale < 0 here, implying
 * truncation before the decimal point.
 */
static void
trunc_var(NumericVar *var, int rscale)
{
	int			di;
	int			ndigits;

	var->dscale = rscale;

	/* decimal digits wanted */
	di = (var->weight + 1) * DEC_DIGITS + rscale;

	/*
	 * If di <= 0, the value loses all digits.
	 */
	if (di <= 0)
	{
		var->ndigits = 0;
		var->weight = 0;
		var->sign = NUMERIC_POS;
	}
	else
	{
		/* NBASE digits wanted */
		ndigits = (di + DEC_DIGITS - 1) / DEC_DIGITS;

		if (ndigits <= var->ndigits)
		{
			var->ndigits = ndigits;

#if DEC_DIGITS == 1
			/* no within-digit stuff to worry about */
#else
			/* 0, or number of decimal digits to keep in last NBASE digit */
			di %= DEC_DIGITS;

			if (di > 0)
			{
				/* Must truncate within last NBASE digit */
				NumericDigit *digits = var->digits;
				int			extra,
							pow10;

#if DEC_DIGITS == 4
				pow10 = round_powers[di];
#elif DEC_DIGITS == 2
				pow10 = 10;
#else
#error unsupported NBASE
#endif
				extra = digits[--ndigits] % pow10;
				digits[ndigits] -= extra;
			}
#endif
		}
	}
}

/*
 * strip_var
 *
 * Strip any leading and trailing zeroes from a numeric variable
 */
static void
strip_var(NumericVar *var)
{
	NumericDigit *digits = var->digits;
	int			ndigits = var->ndigits;

	/* Strip leading zeroes */
	while (ndigits > 0 && *digits == 0)
	{
		digits++;
		var->weight--;
		ndigits--;
	}

	/* Strip trailing zeroes */
	while (ndigits > 0 && digits[ndigits - 1] == 0)
		ndigits--;

	/* If it's zero, normalize the sign and weight */
	if (ndigits == 0)
	{
		var->sign = NUMERIC_POS;
		var->weight = 0;
	}

	var->digits = digits;
	var->ndigits = ndigits;
}


/* ----------------------------------------------------------------------
 *
 * Fast sum accumulator functions
 *
 * ----------------------------------------------------------------------
 */

/*
 * Reset the accumulator's value to zero.  The buffers to hold the digits
 * are not free'd.
 */
static void
accum_sum_reset(NumericSumAccum *accum)
{
	int			i;

	accum->dscale = 0;
	for (i = 0; i < accum->ndigits; i++)
	{
		accum->pos_digits[i] = 0;
		accum->neg_digits[i] = 0;
	}
}

/*
 * Accumulate a new value.
 */
static void
accum_sum_add(NumericSumAccum *accum, const NumericVar *val)
{
	int32	   *accum_digits;
	int			i,
				val_i;
	int			val_ndigits;
	NumericDigit *val_digits;

	/*
	 * If we have accumulated too many values since the last carry
	 * propagation, do it now, to avoid overflowing.  (We could allow more
	 * than NBASE - 1, if we reserved two extra digits, rather than one, for
	 * carry propagation.  But even with NBASE - 1, this needs to be done so
	 * seldom, that the performance difference is negligible.)
	 */
	if (accum->num_uncarried == NBASE - 1)
		accum_sum_carry(accum);

	/*
	 * Adjust the weight or scale of the old value, so that it can accommodate
	 * the new value.
	 */
	accum_sum_rescale(accum, val);

	/* */
	if (val->sign == NUMERIC_POS)
		accum_digits = accum->pos_digits;
	else
		accum_digits = accum->neg_digits;

	/* copy these values into local vars for speed in loop */
	val_ndigits = val->ndigits;
	val_digits = val->digits;

	i = accum->weight - val->weight;
	for (val_i = 0; val_i < val_ndigits; val_i++)
	{
		accum_digits[i] += (int32) val_digits[val_i];
		i++;
	}

	accum->num_uncarried++;
}

/*
 * Propagate carries.
 */
static void
accum_sum_carry(NumericSumAccum *accum)
{
	int			i;
	int			ndigits;
	int32	   *dig;
	int32		carry;
	int32		newdig = 0;

	/*
	 * If no new values have been added since last carry propagation, nothing
	 * to do.
	 */
	if (accum->num_uncarried == 0)
		return;

	/*
	 * We maintain that the weight of the accumulator is always one larger
	 * than needed to hold the current value, before carrying, to make sure
	 * there is enough space for the possible extra digit when carry is
	 * propagated.  We cannot expand the buffer here, unless we require
	 * callers of accum_sum_final() to switch to the right memory context.
	 */
	Assert(accum->pos_digits[0] == 0 && accum->neg_digits[0] == 0);

	ndigits = accum->ndigits;

	/* Propagate carry in the positive sum */
	dig = accum->pos_digits;
	carry = 0;
	for (i = ndigits - 1; i >= 0; i--)
	{
		newdig = dig[i] + carry;
		if (newdig >= NBASE)
		{
			carry = newdig / NBASE;
			newdig -= carry * NBASE;
		}
		else
			carry = 0;
		dig[i] = newdig;
	}
	/* Did we use up the digit reserved for carry propagation? */
	if (newdig > 0)
		accum->have_carry_space = false;

	/* And the same for the negative sum */
	dig = accum->neg_digits;
	carry = 0;
	for (i = ndigits - 1; i >= 0; i--)
	{
		newdig = dig[i] + carry;
		if (newdig >= NBASE)
		{
			carry = newdig / NBASE;
			newdig -= carry * NBASE;
		}
		else
			carry = 0;
		dig[i] = newdig;
	}
	if (newdig > 0)
		accum->have_carry_space = false;

	accum->num_uncarried = 0;
}

/*
 * Re-scale accumulator to accommodate new value.
 *
 * If the new value has more digits than the current digit buffers in the
 * accumulator, enlarge the buffers.
 */
static void
accum_sum_rescale(NumericSumAccum *accum, const NumericVar *val)
{
	int			old_weight = accum->weight;
	int			old_ndigits = accum->ndigits;
	int			accum_ndigits;
	int			accum_weight;
	int			accum_rscale;
	int			val_rscale;

	accum_weight = old_weight;
	accum_ndigits = old_ndigits;

	/*
	 * Does the new value have a larger weight? If so, enlarge the buffers,
	 * and shift the existing value to the new weight, by adding leading
	 * zeros.
	 *
	 * We enforce that the accumulator always has a weight one larger than
	 * needed for the inputs, so that we have space for an extra digit at the
	 * final carry-propagation phase, if necessary.
	 */
	if (val->weight >= accum_weight)
	{
		accum_weight = val->weight + 1;
		accum_ndigits = accum_ndigits + (accum_weight - old_weight);
	}

	/*
	 * Even though the new value is small, we might've used up the space
	 * reserved for the carry digit in the last call to accum_sum_carry().  If
	 * so, enlarge to make room for another one.
	 */
	else if (!accum->have_carry_space)
	{
		accum_weight++;
		accum_ndigits++;
	}

	/* Is the new value wider on the right side? */
	accum_rscale = accum_ndigits - accum_weight - 1;
	val_rscale = val->ndigits - val->weight - 1;
	if (val_rscale > accum_rscale)
		accum_ndigits = accum_ndigits + (val_rscale - accum_rscale);

	if (accum_ndigits != old_ndigits ||
		accum_weight != old_weight)
	{
		int32	   *new_pos_digits;
		int32	   *new_neg_digits;
		int			weightdiff;

		weightdiff = accum_weight - old_weight;

		new_pos_digits = palloc0(accum_ndigits * sizeof(int32));
		new_neg_digits = palloc0(accum_ndigits * sizeof(int32));

		if (accum->pos_digits)
		{
			memcpy(&new_pos_digits[weightdiff], accum->pos_digits,
				   old_ndigits * sizeof(int32));
			pfree(accum->pos_digits);

			memcpy(&new_neg_digits[weightdiff], accum->neg_digits,
				   old_ndigits * sizeof(int32));
			pfree(accum->neg_digits);
		}

		accum->pos_digits = new_pos_digits;
		accum->neg_digits = new_neg_digits;

		accum->weight = accum_weight;
		accum->ndigits = accum_ndigits;

		Assert(accum->pos_digits[0] == 0 && accum->neg_digits[0] == 0);
		accum->have_carry_space = true;
	}

	if (val->dscale > accum->dscale)
		accum->dscale = val->dscale;
}

/*
 * Return the current value of the accumulator.  This perform final carry
 * propagation, and adds together the positive and negative sums.
 *
 * Unlike all the other routines, the caller is not required to switch to
 * the memory context that holds the accumulator.
 */
static void
accum_sum_final(NumericSumAccum *accum, NumericVar *result)
{
	int			i;
	NumericVar	pos_var;
	NumericVar	neg_var;

	if (accum->ndigits == 0)
	{
		set_var_from_var(&const_zero, result);
		return;
	}

	/* Perform final carry */
	accum_sum_carry(accum);

	/* Create NumericVars representing the positive and negative sums */
	init_var(&pos_var);
	init_var(&neg_var);

	pos_var.ndigits = neg_var.ndigits = accum->ndigits;
	pos_var.weight = neg_var.weight = accum->weight;
	pos_var.dscale = neg_var.dscale = accum->dscale;
	pos_var.sign = NUMERIC_POS;
	neg_var.sign = NUMERIC_NEG;

	pos_var.buf = pos_var.digits = digitbuf_alloc(accum->ndigits);
	neg_var.buf = neg_var.digits = digitbuf_alloc(accum->ndigits);

	for (i = 0; i < accum->ndigits; i++)
	{
		Assert(accum->pos_digits[i] < NBASE);
		pos_var.digits[i] = (int16) accum->pos_digits[i];

		Assert(accum->neg_digits[i] < NBASE);
		neg_var.digits[i] = (int16) accum->neg_digits[i];
	}

	/* And add them together */
	add_var(&pos_var, &neg_var, result);

	/* Remove leading/trailing zeroes */
	strip_var(result);
}

/*
 * Copy an accumulator's state.
 *
 * 'dst' is assumed to be uninitialized beforehand.  No attempt is made at
 * freeing old values.
 */
static void
accum_sum_copy(NumericSumAccum *dst, NumericSumAccum *src)
{
	dst->pos_digits = palloc(src->ndigits * sizeof(int32));
	dst->neg_digits = palloc(src->ndigits * sizeof(int32));

	memcpy(dst->pos_digits, src->pos_digits, src->ndigits * sizeof(int32));
	memcpy(dst->neg_digits, src->neg_digits, src->ndigits * sizeof(int32));
	dst->num_uncarried = src->num_uncarried;
	dst->ndigits = src->ndigits;
	dst->weight = src->weight;
	dst->dscale = src->dscale;
}

/*
 * Add the current value of 'accum2' into 'accum'.
 */
static void
accum_sum_combine(NumericSumAccum *accum, NumericSumAccum *accum2)
{
	NumericVar	tmp_var;

	init_var(&tmp_var);

	accum_sum_final(accum2, &tmp_var);
	accum_sum_add(accum, &tmp_var);

	free_var(&tmp_var);
}
