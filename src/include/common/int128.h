/*-------------------------------------------------------------------------
 *
 * int128.h
 *	  Roll-our-own 128-bit integer arithmetic.
 *
 * We make use of the native int128 type if there is one, otherwise
 * implement things the hard way based on two int64 halves.
 *
 * See src/test/modules/test_int128 for a simple test harness for this file.
 *
 * Copyright (c) 2017-2026, PostgreSQL Global Development Group
 *
 * src/include/common/int128.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef INT128_H
#define INT128_H

/*
 * For testing purposes, use of native int128 can be switched on/off by
 * predefining USE_NATIVE_INT128.
 */
#ifndef USE_NATIVE_INT128
#ifdef HAVE_INT128
#define USE_NATIVE_INT128 1
#else
#define USE_NATIVE_INT128 0
#endif
#endif

/*
 * If native int128 support is enabled, INT128 is just int128. Otherwise, it
 * is a structure with separate 64-bit high and low parts.
 *
 * We lay out the INT128 structure with the same content and byte ordering
 * that a native int128 type would (probably) have.  This makes no difference
 * for ordinary use of INT128, but allows union'ing INT128 with int128 for
 * testing purposes.
 *
 * PG_INT128_HI_INT64 and PG_INT128_LO_UINT64 allow the (signed) high and
 * (unsigned) low 64-bit integer parts to be extracted portably on all
 * platforms.
 */
#if USE_NATIVE_INT128

typedef int128 INT128;

#define PG_INT128_HI_INT64(i128)	((int64) ((i128) >> 64))
#define PG_INT128_LO_UINT64(i128)	((uint64) (i128))

#else

typedef struct
{
#ifdef WORDS_BIGENDIAN
	int64		hi;				/* most significant 64 bits, including sign */
	uint64		lo;				/* least significant 64 bits, without sign */
#else
	uint64		lo;				/* least significant 64 bits, without sign */
	int64		hi;				/* most significant 64 bits, including sign */
#endif
} INT128;

#define PG_INT128_HI_INT64(i128)	((i128).hi)
#define PG_INT128_LO_UINT64(i128)	((i128).lo)

#endif

/*
 * Construct an INT128 from (signed) high and (unsigned) low 64-bit integer
 * parts.
 */
static inline INT128
make_int128(int64 hi, uint64 lo)
{
#if USE_NATIVE_INT128
	return (((int128) hi) << 64) + lo;
#else
	INT128		val;

	val.hi = hi;
	val.lo = lo;
	return val;
#endif
}

/*
 * Add an unsigned int64 value into an INT128 variable.
 */
static inline void
int128_add_uint64(INT128 *i128, uint64 v)
{
#if USE_NATIVE_INT128
	*i128 += v;
#else
	/*
	 * First add the value to the .lo part, then check to see if a carry needs
	 * to be propagated into the .hi part.  Since this is unsigned integer
	 * arithmetic, which is just modular arithmetic, a carry is needed if the
	 * new .lo part is less than the old .lo part (i.e., if modular
	 * wrap-around occurred).  Writing this in the form below, rather than
	 * using an "if" statement causes modern compilers to produce branchless
	 * machine code identical to the native code.
	 */
	uint64		oldlo = i128->lo;

	i128->lo += v;
	i128->hi += (i128->lo < oldlo);
#endif
}

/*
 * Add a signed int64 value into an INT128 variable.
 */
static inline void
int128_add_int64(INT128 *i128, int64 v)
{
#if USE_NATIVE_INT128
	*i128 += v;
#else
	/*
	 * This is much like the above except that the carry logic differs for
	 * negative v -- we need to subtract 1 from the .hi part if the new .lo
	 * value is greater than the old .lo value.  That can be achieved without
	 * any branching by adding the sign bit from v (v >> 63 = 0 or -1) to the
	 * previous result (for negative v, if the new .lo value is less than the
	 * old .lo value, the two terms cancel and we leave the .hi part
	 * unchanged, otherwise we subtract 1 from the .hi part).  With modern
	 * compilers this often produces machine code identical to the native
	 * code.
	 */
	uint64		oldlo = i128->lo;

	i128->lo += v;
	i128->hi += (i128->lo < oldlo) + (v >> 63);
#endif
}

/*
 * Add an INT128 value into an INT128 variable.
 */
static inline void
int128_add_int128(INT128 *i128, INT128 v)
{
#if USE_NATIVE_INT128
	*i128 += v;
#else
	int128_add_uint64(i128, v.lo);
	i128->hi += v.hi;
#endif
}

/*
 * Subtract an unsigned int64 value from an INT128 variable.
 */
static inline void
int128_sub_uint64(INT128 *i128, uint64 v)
{
#if USE_NATIVE_INT128
	*i128 -= v;
#else
	/*
	 * This is like int128_add_uint64(), except we must propagate a borrow to
	 * (subtract 1 from) the .hi part if the new .lo part is greater than the
	 * old .lo part.
	 */
	uint64		oldlo = i128->lo;

	i128->lo -= v;
	i128->hi -= (i128->lo > oldlo);
#endif
}

/*
 * Subtract a signed int64 value from an INT128 variable.
 */
static inline void
int128_sub_int64(INT128 *i128, int64 v)
{
#if USE_NATIVE_INT128
	*i128 -= v;
#else
	/* Like int128_add_int64() with the sign of v inverted */
	uint64		oldlo = i128->lo;

	i128->lo -= v;
	i128->hi -= (i128->lo > oldlo) + (v >> 63);
#endif
}

/*
 * INT64_HI_INT32 extracts the most significant 32 bits of int64 as int32.
 * INT64_LO_UINT32 extracts the least significant 32 bits as uint32.
 */
#define INT64_HI_INT32(i64)		((int32) ((i64) >> 32))
#define INT64_LO_UINT32(i64)	((uint32) (i64))

/*
 * Add the 128-bit product of two int64 values into an INT128 variable.
 */
static inline void
int128_add_int64_mul_int64(INT128 *i128, int64 x, int64 y)
{
#if USE_NATIVE_INT128
	/*
	 * XXX with a stupid compiler, this could actually be less efficient than
	 * the non-native implementation; maybe we should do it by hand always?
	 */
	*i128 += (int128) x * (int128) y;
#else
	/* INT64_HI_INT32 must use arithmetic right shift */
	StaticAssertDecl(((int64) -1 >> 1) == (int64) -1,
					 "arithmetic right shift is needed");

	/*----------
	 * Form the 128-bit product x * y using 64-bit arithmetic.
	 * Considering each 64-bit input as having 32-bit high and low parts,
	 * we can compute
	 *
	 *	 x * y = ((x.hi << 32) + x.lo) * (((y.hi << 32) + y.lo)
	 *		   = (x.hi * y.hi) << 64 +
	 *			 (x.hi * y.lo) << 32 +
	 *			 (x.lo * y.hi) << 32 +
	 *			 x.lo * y.lo
	 *
	 * Each individual product is of 32-bit terms so it won't overflow when
	 * computed in 64-bit arithmetic.  Then we just have to shift it to the
	 * correct position while adding into the 128-bit result.  We must also
	 * keep in mind that the "lo" parts must be treated as unsigned.
	 *----------
	 */

	/* No need to work hard if product must be zero */
	if (x != 0 && y != 0)
	{
		int32		x_hi = INT64_HI_INT32(x);
		uint32		x_lo = INT64_LO_UINT32(x);
		int32		y_hi = INT64_HI_INT32(y);
		uint32		y_lo = INT64_LO_UINT32(y);
		int64		tmp;

		/* the first term */
		i128->hi += (int64) x_hi * (int64) y_hi;

		/* the second term: sign-extended with the sign of x */
		tmp = (int64) x_hi * (int64) y_lo;
		i128->hi += INT64_HI_INT32(tmp);
		int128_add_uint64(i128, ((uint64) INT64_LO_UINT32(tmp)) << 32);

		/* the third term: sign-extended with the sign of y */
		tmp = (int64) x_lo * (int64) y_hi;
		i128->hi += INT64_HI_INT32(tmp);
		int128_add_uint64(i128, ((uint64) INT64_LO_UINT32(tmp)) << 32);

		/* the fourth term: always unsigned */
		int128_add_uint64(i128, (uint64) x_lo * (uint64) y_lo);
	}
#endif
}

/*
 * Subtract the 128-bit product of two int64 values from an INT128 variable.
 */
static inline void
int128_sub_int64_mul_int64(INT128 *i128, int64 x, int64 y)
{
#if USE_NATIVE_INT128
	*i128 -= (int128) x * (int128) y;
#else
	/* As above, except subtract the 128-bit product */
	if (x != 0 && y != 0)
	{
		int32		x_hi = INT64_HI_INT32(x);
		uint32		x_lo = INT64_LO_UINT32(x);
		int32		y_hi = INT64_HI_INT32(y);
		uint32		y_lo = INT64_LO_UINT32(y);
		int64		tmp;

		/* the first term */
		i128->hi -= (int64) x_hi * (int64) y_hi;

		/* the second term: sign-extended with the sign of x */
		tmp = (int64) x_hi * (int64) y_lo;
		i128->hi -= INT64_HI_INT32(tmp);
		int128_sub_uint64(i128, ((uint64) INT64_LO_UINT32(tmp)) << 32);

		/* the third term: sign-extended with the sign of y */
		tmp = (int64) x_lo * (int64) y_hi;
		i128->hi -= INT64_HI_INT32(tmp);
		int128_sub_uint64(i128, ((uint64) INT64_LO_UINT32(tmp)) << 32);

		/* the fourth term: always unsigned */
		int128_sub_uint64(i128, (uint64) x_lo * (uint64) y_lo);
	}
#endif
}

/*
 * Divide an INT128 variable by a signed int32 value, returning the quotient
 * and remainder.  The remainder will have the same sign as *i128.
 *
 * Note: This provides no protection against dividing by 0, or dividing
 * INT128_MIN by -1, which overflows.  It is the caller's responsibility to
 * guard against those.
 */
static inline void
int128_div_mod_int32(INT128 *i128, int32 v, int32 *remainder)
{
#if USE_NATIVE_INT128
	int128		old_i128 = *i128;

	*i128 /= v;
	*remainder = (int32) (old_i128 - *i128 * v);
#else
	/*
	 * To avoid any intermediate values overflowing (as happens if INT64_MIN
	 * is divided by -1), we first compute the quotient abs(*i128) / abs(v)
	 * using unsigned 64-bit arithmetic, and then fix the signs up at the end.
	 *
	 * The quotient is computed using the short division algorithm described
	 * in Knuth volume 2, section 4.3.1 exercise 16 (cf. div_var_int() in
	 * numeric.c).  Since the absolute value of the divisor is known to be at
	 * most 2^31, the remainder carried from one digit to the next is at most
	 * 2^31 - 1, and so there is no danger of overflow when this is combined
	 * with the next digit (a 32-bit unsigned integer).
	 */
	uint64		n_hi;
	uint64		n_lo;
	uint32		d;
	uint64		q;
	uint64		r;
	uint64		tmp;

	/* numerator: absolute value of *i128 */
	if (i128->hi < 0)
	{
		n_hi = 0 - ((uint64) i128->hi);
		n_lo = 0 - i128->lo;
		if (n_lo != 0)
			n_hi--;
	}
	else
	{
		n_hi = i128->hi;
		n_lo = i128->lo;
	}

	/* denominator: absolute value of v */
	d = abs(v);

	/* quotient and remainder of high 64 bits */
	q = n_hi / d;
	r = n_hi % d;
	n_hi = q;

	/* quotient and remainder of next 32 bits (upper half of n_lo) */
	tmp = (r << 32) + (n_lo >> 32);
	q = tmp / d;
	r = tmp % d;

	/* quotient and remainder of last 32 bits (lower half of n_lo) */
	tmp = (r << 32) + (uint32) n_lo;
	n_lo = q << 32;
	q = tmp / d;
	r = tmp % d;
	n_lo += q;

	/* final remainder should have the same sign as *i128 */
	*remainder = i128->hi < 0 ? (int32) (0 - r) : (int32) r;

	/* store the quotient in *i128, negating it if necessary */
	if ((i128->hi < 0) != (v < 0))
	{
		n_hi = 0 - n_hi;
		n_lo = 0 - n_lo;
		if (n_lo != 0)
			n_hi--;
	}
	i128->hi = (int64) n_hi;
	i128->lo = n_lo;
#endif
}

/*
 * Test if an INT128 value is zero.
 */
static inline bool
int128_is_zero(INT128 x)
{
#if USE_NATIVE_INT128
	return x == 0;
#else
	return x.hi == 0 && x.lo == 0;
#endif
}

/*
 * Return the sign of an INT128 value (returns -1, 0, or +1).
 */
static inline int
int128_sign(INT128 x)
{
#if USE_NATIVE_INT128
	if (x < 0)
		return -1;
	if (x > 0)
		return 1;
	return 0;
#else
	if (x.hi < 0)
		return -1;
	if (x.hi > 0)
		return 1;
	if (x.lo > 0)
		return 1;
	return 0;
#endif
}

/*
 * Compare two INT128 values, return -1, 0, or +1.
 */
static inline int
int128_compare(INT128 x, INT128 y)
{
#if USE_NATIVE_INT128
	if (x < y)
		return -1;
	if (x > y)
		return 1;
	return 0;
#else
	if (x.hi < y.hi)
		return -1;
	if (x.hi > y.hi)
		return 1;
	if (x.lo < y.lo)
		return -1;
	if (x.lo > y.lo)
		return 1;
	return 0;
#endif
}

/*
 * Widen int64 to INT128.
 */
static inline INT128
int64_to_int128(int64 v)
{
#if USE_NATIVE_INT128
	return (INT128) v;
#else
	INT128		val;

	val.lo = (uint64) v;
	val.hi = (v < 0) ? -INT64CONST(1) : INT64CONST(0);
	return val;
#endif
}

/*
 * Convert INT128 to int64 (losing any high-order bits).
 * This also works fine for casting down to uint64.
 */
static inline int64
int128_to_int64(INT128 val)
{
#if USE_NATIVE_INT128
	return (int64) val;
#else
	return (int64) val.lo;
#endif
}

/*
 * Overflow-checked operations.
 *
 * These are for callers that must detect overflow and fall back to a wider
 * representation rather than error out, so on overflow they leave their
 * target untouched.
 */

/*
 * Add an INT128 value into an INT128 variable, checking for overflow.
 *
 * Returns true (leaving *i128 unchanged) if the sum does not fit,
 * otherwise updates *i128 and returns false.
 */
static inline bool
int128_add_int128_overflow(INT128 *i128, INT128 v)
{
#if USE_NATIVE_INT128 && defined(HAVE__BUILTIN_OP_OVERFLOW)
	/*
	 * Compilers expand a 128-bit add-with-overflow inline (add/adc and a
	 * flag test), so unlike multiplication this needs no runtime library
	 * support.
	 */
	int128		result;

	if (__builtin_add_overflow(*i128, v, &result))
		return true;
	*i128 = result;
	return false;
#else
	/*
	 * Signed overflow occurs iff the addends have the same sign and the
	 * result's sign differs from it.  Compute the raw sum entirely in
	 * unsigned arithmetic (which wraps by definition, so there is no
	 * undefined behaviour to trip over), then apply the sign rule.
	 */
	INT128		result = *i128;
	bool		lhs_neg = (PG_INT128_HI_INT64(result) < 0);
	bool		rhs_neg = (PG_INT128_HI_INT64(v) < 0);

#if USE_NATIVE_INT128
	result = (int128) ((uint128) result + (uint128) v);
#else
	{
		uint64		oldlo = result.lo;

		result.lo += v.lo;
		result.hi = (int64) ((uint64) result.hi + (uint64) v.hi +
							 (result.lo < oldlo));
	}
#endif
	if (lhs_neg == rhs_neg &&
		(PG_INT128_HI_INT64(result) < 0) != lhs_neg)
		return true;
	*i128 = result;
	return false;
#endif
}

/*
 * Largest k for which 10^k is representable in a signed 64-bit integer.
 */
#define INT64_POW10_MAX		18

/*
 * 10^k as int64, for 0 <= k <= INT64_POW10_MAX.  It is up to the caller to
 * keep the index within that range.
 */
static const int64 pow10_int64[INT64_POW10_MAX + 1] = {
	INT64CONST(1),
	INT64CONST(10),
	INT64CONST(100),
	INT64CONST(1000),
	INT64CONST(10000),
	INT64CONST(100000),
	INT64CONST(1000000),
	INT64CONST(10000000),
	INT64CONST(100000000),
	INT64CONST(1000000000),
	INT64CONST(10000000000),
	INT64CONST(100000000000),
	INT64CONST(1000000000000),
	INT64CONST(10000000000000),
	INT64CONST(100000000000000),
	INT64CONST(1000000000000000),
	INT64CONST(10000000000000000),
	INT64CONST(100000000000000000),
	INT64CONST(1000000000000000000)
};

/*
 * Largest k for which 10^k is representable in a signed 128-bit integer.
 */
#define INT128_POW10_MAX	38

/*
 * Multiply an INT128 variable by 10^k, checking for overflow.
 *
 * Returns true (leaving *i128 unchanged) if the product does not fit,
 * otherwise updates *i128 and returns false.  Any k >= 0 is accepted:
 * zero stays zero, and any other value overflows for k > INT128_POW10_MAX.
 *
 * This is the decimal-rescaling primitive: it aligns fixed-point
 * coefficients to a common scale.
 *
 * The overflow test is exact and the same on every platform: v * 10^k fits
 * iff |v| <= floor((2^127 - 1) / 10^k).  One bound serves both signs,
 * because 2^127 is not a multiple of 10: no product of 10^k (k > 0) can
 * equal -2^127, so the extra negative value adds nothing.  Once the bound
 * holds, the multiplication itself cannot overflow.
 *
 * __builtin_mul_overflow() is deliberately not used.  configure only
 * verifies it for 64-bit operands; for 128-bit operands some compilers emit
 * a call to a runtime helper (__muloti4) that libgcc does not provide.
 */
static inline bool
int128_mul_pow10_overflow(INT128 *i128, int k)
{
	/* limits[k] = floor((2^127 - 1) / 10^k), as {high, low} 64-bit halves */
	static const uint64 limits[INT128_POW10_MAX + 1][2] = {
		{UINT64CONST(0x7FFFFFFFFFFFFFFF), UINT64CONST(0xFFFFFFFFFFFFFFFF)},
		{UINT64CONST(0x0CCCCCCCCCCCCCCC), UINT64CONST(0xCCCCCCCCCCCCCCCC)},
		{UINT64CONST(0x0147AE147AE147AE), UINT64CONST(0x147AE147AE147AE1)},
		{UINT64CONST(0x0020C49BA5E353F7), UINT64CONST(0xCED916872B020C49)},
		{UINT64CONST(0x000346DC5D638865), UINT64CONST(0x94AF4F0D844D013A)},
		{UINT64CONST(0x000053E2D6238DA3), UINT64CONST(0xC21187E7C06E19B9)},
		{UINT64CONST(0x000008637BD05AF6), UINT64CONST(0xC69B5A63F9A49C2C)},
		{UINT64CONST(0x000000D6BF94D5E5), UINT64CONST(0x7A42BC3D32907604)},
		{UINT64CONST(0x00000015798EE230), UINT64CONST(0x8C39DF9FB841A566)},
		{UINT64CONST(0x0000000225C17D04), UINT64CONST(0xDAD2965CC5A02A23)},
		{UINT64CONST(0x0000000036F9BFB3), UINT64CONST(0xAF7B756FAD5CD103)},
		{UINT64CONST(0x00000000057F5FF8), UINT64CONST(0x5E592557F7BC7B4D)},
		{UINT64CONST(0x00000000008CBCCC), UINT64CONST(0x096F5088CBF93F87)},
		{UINT64CONST(0x00000000000E12E1), UINT64CONST(0x3424BB40E132865A)},
		{UINT64CONST(0x0000000000016849), UINT64CONST(0xB86A12B9B01EA709)},
		{UINT64CONST(0x0000000000002407), UINT64CONST(0x5F3DCEAC2B3643E7)},
		{UINT64CONST(0x000000000000039A), UINT64CONST(0x5652FB1137856D30)},
		{UINT64CONST(0x000000000000005C), UINT64CONST(0x3BD5191B525A2484)},
		{UINT64CONST(0x0000000000000009), UINT64CONST(0x392EE8E921D5D073)},
		{UINT64CONST(0x0000000000000000), UINT64CONST(0xEC1E4A7DB69561A5)},
		{UINT64CONST(0x0000000000000000), UINT64CONST(0x179CA10C9242235D)},
		{UINT64CONST(0x0000000000000000), UINT64CONST(0x025C768141D369EF)},
		{UINT64CONST(0x0000000000000000), UINT64CONST(0x003C7240202EBDCB)},
		{UINT64CONST(0x0000000000000000), UINT64CONST(0x00060B6CD004AC94)},
		{UINT64CONST(0x0000000000000000), UINT64CONST(0x00009ABE14CD4475)},
		{UINT64CONST(0x0000000000000000), UINT64CONST(0x00000F79687AED3E)},
		{UINT64CONST(0x0000000000000000), UINT64CONST(0x0000018C240C4AEC)},
		{UINT64CONST(0x0000000000000000), UINT64CONST(0x000000279D346DE4)},
		{UINT64CONST(0x0000000000000000), UINT64CONST(0x00000003F61ED7CA)},
		{UINT64CONST(0x0000000000000000), UINT64CONST(0x0000000065697BFA)},
		{UINT64CONST(0x0000000000000000), UINT64CONST(0x000000000A2425FF)},
		{UINT64CONST(0x0000000000000000), UINT64CONST(0x0000000001039D66)},
		{UINT64CONST(0x0000000000000000), UINT64CONST(0x000000000019F623)},
		{UINT64CONST(0x0000000000000000), UINT64CONST(0x000000000002989D)},
		{UINT64CONST(0x0000000000000000), UINT64CONST(0x0000000000004276)},
		{UINT64CONST(0x0000000000000000), UINT64CONST(0x00000000000006A5)},
		{UINT64CONST(0x0000000000000000), UINT64CONST(0x00000000000000AA)},
		{UINT64CONST(0x0000000000000000), UINT64CONST(0x0000000000000011)},
		{UINT64CONST(0x0000000000000000), UINT64CONST(0x0000000000000001)}
	};

	Assert(k >= 0);

	if (k == 0 || int128_is_zero(*i128))
		return false;
	if (k > INT128_POW10_MAX)
		return true;

#if USE_NATIVE_INT128
	{
		int128		limit = make_int128((int64) limits[k][0], limits[k][1]);
		int128		result = *i128;

		if (result > limit || result < -limit)
			return true;

		/* the bound holds, so no partial product can overflow */
		while (k > 0)
		{
			int			chunk = Min(k, INT64_POW10_MAX);

			result *= pow10_int64[chunk];
			k -= chunk;
		}
		*i128 = result;
	}
#else
	{
		static const uint32 pow10_32[] = {
			1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000,
			1000000000
		};
		bool		neg = (i128->hi < 0);
		uint64		abs_hi = (uint64) i128->hi;
		uint64		abs_lo = i128->lo;

		/* magnitude; for -2^127 this is 2^127, still representable here */
		if (neg)
		{
			abs_lo = 0 - abs_lo;
			abs_hi = 0 - abs_hi - (abs_lo != 0);
		}

		if (abs_hi > limits[k][0] ||
			(abs_hi == limits[k][0] && abs_lo > limits[k][1]))
			return true;

		/*
		 * Multiply the magnitude by 10^k in chunks that fit in uint32, using
		 * 32-bit-limb schoolbook multiplication.  The bound above guarantees
		 * that nothing carries out of the top limb.
		 */
		while (k > 0)
		{
			int			chunk = Min(k, 9);
			uint64		m = pow10_32[chunk];
			uint64		r0,
						r1,
						r2,
						r3;

			r0 = (abs_lo & UINT64CONST(0xFFFFFFFF)) * m;
			r1 = (abs_lo >> 32) * m + (r0 >> 32);
			r2 = (abs_hi & UINT64CONST(0xFFFFFFFF)) * m + (r1 >> 32);
			r3 = (abs_hi >> 32) * m + (r2 >> 32);
			Assert((r3 >> 31) == 0);

			abs_lo = (r1 << 32) | (r0 & UINT64CONST(0xFFFFFFFF));
			abs_hi = (r3 << 32) | (r2 & UINT64CONST(0xFFFFFFFF));
			k -= chunk;
		}

		if (neg)
		{
			abs_lo = 0 - abs_lo;
			abs_hi = 0 - abs_hi - (abs_lo != 0);
		}
		i128->hi = (int64) abs_hi;
		i128->lo = abs_lo;
	}
#endif
	return false;
}

#endif							/* INT128_H */
