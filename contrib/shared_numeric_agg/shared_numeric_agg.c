/*-------------------------------------------------------------------------
 *
 * shared_numeric_agg.c
 *	  Flat, fixed-size, in-place-updatable numeric sum/avg aggregates.
 *
 * Motivation: the stock sum(numeric)/avg(numeric) keep their running state
 * in an 'internal' C struct with growable digit buffers, which cannot live
 * in shared memory; Parallel Shared Hash Aggregation then has to serialize
 * and deserialize that state around every transition call.  These variants
 * keep the state as a plain bytea of fixed size, updated strictly in
 * place: under the shared hash table's by-ref path a transition call
 * scribbles directly on the DSA blob and returns the same pointer, so
 * advancing a group costs a digit decode plus a few int64 additions —
 * no palloc, no DSA traffic, no serialization.
 *
 * State representation: a fixed window of signed int64 "lanes", one per
 * NBASE(=10000) digit position, covering weights FLATSUM_MAX_WEIGHT down
 * to FLATSUM_MAX_WEIGHT - FLATSUM_NLANES + 1.  Adding a value adds its
 * digits into the lanes (negated for negative values — signed lanes make
 * a separate positive/negative split unnecessary).  Since every digit is
 * < 10000, a lane cannot overflow before ~9e14 accumulated values, so NO
 * carry propagation happens per row at all; the final function normalizes
 * the lane vector once per group and builds the result via numeric_in().
 *
 * Values whose digits fall outside the window draw an error; widen
 * FLATSUM_NLANES/FLATSUM_MAX_WEIGHT if your data legitimately exceeds
 * ~1e64 in magnitude or 1e-63 in scale.  NaN and +/-Infinity inputs are
 * tracked in flags and reproduce stock sum/avg semantics.
 *
 * The numeric on-disk format decoding below is copied from
 * utils/adt/numeric.c, whose struct is deliberately private; the format
 * is the stable storage format, so this is safe albeit unlovely — the
 * price of living outside numeric.c.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_aggregate.h"
#include "catalog/pg_type_d.h"
#include "fmgr.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pathnodes.h"
#include "nodes/supportnodes.h"
#include "optimizer/cost.h"
#include "parser/parse_func.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/fmgrprotos.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"
#include "varatt.h"

PG_MODULE_MAGIC;

/* ----------
 * Numeric storage format decoding, copied from utils/adt/numeric.c.
 * ----------
 */
typedef int16 NumericDigit;

#define NBASE		10000
#define DEC_DIGITS	4

struct NumericShort
{
	uint16		n_header;
	NumericDigit n_data[FLEXIBLE_ARRAY_MEMBER];
};

struct NumericLong
{
	uint16		n_sign_dscale;
	int16		n_weight;
	NumericDigit n_data[FLEXIBLE_ARRAY_MEMBER];
};

union NumericChoice
{
	uint16		n_header;
	struct NumericLong n_long;
	struct NumericShort n_short;
};

struct NumericData
{
	int32		vl_len_;
	union NumericChoice choice;
};

#define NUMERIC_SIGN_MASK	0xC000
#define NUMERIC_POS			0x0000
#define NUMERIC_NEG			0x4000
#define NUMERIC_SHORT		0x8000
#define NUMERIC_SPECIAL		0xC000

#define NUMERIC_FLAGBITS(n) ((n)->choice.n_header & NUMERIC_SIGN_MASK)
#define NUMERIC_IS_SHORT(n)		(NUMERIC_FLAGBITS(n) == NUMERIC_SHORT)
#define NUMERIC_IS_SPECIAL(n)	(NUMERIC_FLAGBITS(n) == NUMERIC_SPECIAL)

#define NUMERIC_EXT_SIGN_MASK	0xF000
#define NUMERIC_NAN				0xC000
#define NUMERIC_PINF			0xD000
#define NUMERIC_NINF			0xF000

#define NUMERIC_EXT_FLAGBITS(n)	((n)->choice.n_header & NUMERIC_EXT_SIGN_MASK)
#define NUMERIC_IS_NAN(n)		((n)->choice.n_header == NUMERIC_NAN)
#define NUMERIC_IS_PINF(n)		((n)->choice.n_header == NUMERIC_PINF)
#define NUMERIC_IS_NINF(n)		((n)->choice.n_header == NUMERIC_NINF)

#define NUMERIC_SHORT_SIGN_MASK			0x2000
#define NUMERIC_SHORT_DSCALE_MASK		0x1F80
#define NUMERIC_SHORT_DSCALE_SHIFT		7
#define NUMERIC_SHORT_WEIGHT_SIGN_MASK	0x0040
#define NUMERIC_SHORT_WEIGHT_MASK		0x003F

#define NUMERIC_HEADER_IS_SHORT(n)	(((n)->choice.n_header & 0x8000) != 0)
#define NUMERIC_HEADER_SIZE(n) \
	(VARHDRSZ + sizeof(uint16) + \
	 (NUMERIC_HEADER_IS_SHORT(n) ? 0 : sizeof(int16)))

#define NUMERIC_SIGN(n) \
	(NUMERIC_IS_SHORT(n) ? \
		(((n)->choice.n_short.n_header & NUMERIC_SHORT_SIGN_MASK) ? \
		 NUMERIC_NEG : NUMERIC_POS) : \
		(NUMERIC_IS_SPECIAL(n) ? \
		 NUMERIC_EXT_FLAGBITS(n) : NUMERIC_FLAGBITS(n)))
#define NUMERIC_DSCALE(n) \
	(NUMERIC_HEADER_IS_SHORT(n) ? \
		((n)->choice.n_short.n_header & NUMERIC_SHORT_DSCALE_MASK) \
		>> NUMERIC_SHORT_DSCALE_SHIFT \
	 : ((n)->choice.n_long.n_sign_dscale & 0x3FFF))
#define NUMERIC_WEIGHT(n) \
	(NUMERIC_HEADER_IS_SHORT(n) ? \
		(((n)->choice.n_short.n_header & NUMERIC_SHORT_WEIGHT_SIGN_MASK ? \
			~NUMERIC_SHORT_WEIGHT_MASK : 0) \
		 | ((n)->choice.n_short.n_header & NUMERIC_SHORT_WEIGHT_MASK)) \
	 : ((n)->choice.n_long.n_weight))
#define NUMERIC_DIGITS(num) \
	(NUMERIC_IS_SHORT(num) ? \
	 (num)->choice.n_short.n_data : (num)->choice.n_long.n_data)
#define NUMERIC_NDIGITS(num) \
	((VARSIZE(num) - NUMERIC_HEADER_SIZE(num)) / sizeof(NumericDigit))

/* ----------
 * Flat accumulator state (a plain bytea varlena).
 * ----------
 */

/* Highest NBASE weight covered; lanes span [MAX_WEIGHT-NLANES+1, MAX_WEIGHT] */
#define FLATSUM_MAX_WEIGHT	15	/* values up to ~1e63 */
#define FLATSUM_NLANES		32	/* fractional digits down to ~1e-64 */

#define FLATSUM_FLAG_NAN	0x01
#define FLATSUM_FLAG_PINF	0x02
#define FLATSUM_FLAG_NINF	0x04

typedef struct FlatSumState
{
	char		vl_len_[4];		/* varlena header (do not touch directly) */
	uint32		flags;
	int64		nvalues;		/* non-NULL inputs accumulated */
	int32		max_dscale;		/* stock sum reports max input dscale */
	int32		padding;
	int64		lanes[FLATSUM_NLANES];
} FlatSumState;

#define FLATSUM_STATE_SIZE	(sizeof(FlatSumState))

/* lane index for NBASE weight w */
#define FLATSUM_LANE(w)		(FLATSUM_MAX_WEIGHT - (w))

PG_FUNCTION_INFO_V1(numeric_flat_sum_trans);
PG_FUNCTION_INFO_V1(numeric_flat_sum_combine);
PG_FUNCTION_INFO_V1(numeric_flat_sum_final);
PG_FUNCTION_INFO_V1(numeric_flat_avg_final);
PG_FUNCTION_INFO_V1(shared_numeric_agg_support);

/*
 * Create a zeroed state.  In an aggregate context allocate it there (the
 * regular executor path); otherwise in CurrentMemoryContext (the shared
 * hash table path allocates in per-tuple memory and copies the blob into
 * shared memory once per group).
 */
static FlatSumState *
flatsum_new_state(FunctionCallInfo fcinfo)
{
	MemoryContext aggcontext;
	MemoryContext oldcontext;
	FlatSumState *state;

	if (AggCheckCallContext(fcinfo, &aggcontext))
		oldcontext = MemoryContextSwitchTo(aggcontext);
	else
		oldcontext = CurrentMemoryContext;	/* no switch */

	state = (FlatSumState *) palloc0(FLATSUM_STATE_SIZE);
	SET_VARSIZE(state, FLATSUM_STATE_SIZE);

	if (oldcontext != CurrentMemoryContext)
		MemoryContextSwitchTo(oldcontext);

	return state;
}

/*
 * Accumulate one numeric value into the lanes.
 */
static void
flatsum_add(FlatSumState *state, struct NumericData *num)
{
	NumericDigit *digits;
	int			ndigits;
	int			weight;
	int			dscale;
	bool		negative;
	int			i;

	if (NUMERIC_IS_SPECIAL(num))
	{
		if (NUMERIC_IS_NAN(num))
			state->flags |= FLATSUM_FLAG_NAN;
		else if (NUMERIC_IS_PINF(num))
			state->flags |= FLATSUM_FLAG_PINF;
		else
			state->flags |= FLATSUM_FLAG_NINF;
		state->nvalues++;
		return;
	}

	digits = NUMERIC_DIGITS(num);
	ndigits = NUMERIC_NDIGITS(num);
	weight = NUMERIC_WEIGHT(num);
	dscale = NUMERIC_DSCALE(num);
	negative = (NUMERIC_SIGN(num) == NUMERIC_NEG);

	if (dscale > state->max_dscale)
		state->max_dscale = dscale;

	if (ndigits > 0 &&
		(weight > FLATSUM_MAX_WEIGHT ||
		 weight - (ndigits - 1) < FLATSUM_MAX_WEIGHT - FLATSUM_NLANES + 1))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("numeric value exceeds the range supported by flat sum aggregation"),
				 errhint("Rebuild shared_numeric_agg with a wider lane window.")));

	for (i = 0; i < ndigits; i++)
	{
		int			lane = FLATSUM_LANE(weight - i);

		if (negative)
			state->lanes[lane] -= digits[i];
		else
			state->lanes[lane] += digits[i];
	}

	state->nvalues++;
}

/*
 * Transition function: sfunc(state bytea, value numeric) -> bytea.
 *
 * Not strict: it must create the state on first call.  When a state
 * exists it is updated IN PLACE and the same pointer returned — under
 * the shared hash table this mutates the shared blob directly with no
 * per-row allocation, and in the regular executor it is the sanctioned
 * AggCheckCallContext scribble pattern.
 */
Datum
numeric_flat_sum_trans(PG_FUNCTION_ARGS)
{
	FlatSumState *state;

	if (PG_ARGISNULL(0))
	{
		if (PG_ARGISNULL(1))
			PG_RETURN_NULL();	/* no state, no value: nothing to do */
		state = flatsum_new_state(fcinfo);
	}
	else
	{
		state = (FlatSumState *) PG_GETARG_BYTEA_P(0);
		Assert(VARSIZE(state) == FLATSUM_STATE_SIZE);
		if (PG_ARGISNULL(1))
			PG_RETURN_POINTER(state);	/* stock sum ignores NULL inputs */
	}

	flatsum_add(state, (struct NumericData *) PG_GETARG_NUMERIC(1));

	PG_RETURN_POINTER(state);
}

/*
 * Combine function: lane-wise addition, in place into the first state.
 */
Datum
numeric_flat_sum_combine(PG_FUNCTION_ARGS)
{
	FlatSumState *state1;
	FlatSumState *state2;
	int			i;

	if (PG_ARGISNULL(0))
	{
		if (PG_ARGISNULL(1))
			PG_RETURN_NULL();
		/* must copy: state2 may live in the other worker's serialized copy */
		state1 = flatsum_new_state(fcinfo);
		state2 = (FlatSumState *) PG_GETARG_BYTEA_P(1);
		memcpy((char *) state1 + VARHDRSZ, (char *) state2 + VARHDRSZ,
			   FLATSUM_STATE_SIZE - VARHDRSZ);
		PG_RETURN_POINTER(state1);
	}
	state1 = (FlatSumState *) PG_GETARG_BYTEA_P(0);
	Assert(VARSIZE(state1) == FLATSUM_STATE_SIZE);
	if (PG_ARGISNULL(1))
		PG_RETURN_POINTER(state1);
	state2 = (FlatSumState *) PG_GETARG_BYTEA_P(1);
	Assert(VARSIZE(state2) == FLATSUM_STATE_SIZE);

	state1->flags |= state2->flags;
	state1->nvalues += state2->nvalues;
	if (state2->max_dscale > state1->max_dscale)
		state1->max_dscale = state2->max_dscale;
	for (i = 0; i < FLATSUM_NLANES; i++)
		state1->lanes[i] += state2->lanes[i];

	PG_RETURN_POINTER(state1);
}

/*
 * Normalize the lane vector and construct the numeric result, going
 * through numeric_in() — once per group, so clarity beats cleverness.
 */
static Datum
flatsum_build_result(FlatSumState *state)
{
	int64		lanes[FLATSUM_NLANES];
	bool		negative = false;
	int			i;
	int			first;
	StringInfoData buf;

	/* specials reproduce stock sum semantics */
	if (state->flags & FLATSUM_FLAG_NAN ||
		((state->flags & FLATSUM_FLAG_PINF) &&
		 (state->flags & FLATSUM_FLAG_NINF)))
		return DirectFunctionCall3(numeric_in, CStringGetDatum("NaN"),
								   ObjectIdGetDatum(InvalidOid),
								   Int32GetDatum(-1));
	if (state->flags & FLATSUM_FLAG_PINF)
		return DirectFunctionCall3(numeric_in, CStringGetDatum("Infinity"),
								   ObjectIdGetDatum(InvalidOid),
								   Int32GetDatum(-1));
	if (state->flags & FLATSUM_FLAG_NINF)
		return DirectFunctionCall3(numeric_in, CStringGetDatum("-Infinity"),
								   ObjectIdGetDatum(InvalidOid),
								   Int32GetDatum(-1));

	memcpy(lanes, state->lanes, sizeof(lanes));

	/*
	 * Carry-propagate from the least significant lane upward so that every
	 * lane lands in [0, NBASE).  Signed lanes borrow through the same
	 * mechanism (C division truncates toward zero, hence the explicit
	 * floor adjustment).
	 */
	for (i = FLATSUM_NLANES - 1; i > 0; i--)
	{
		int64		carry = lanes[i] / NBASE;
		int64		rem = lanes[i] % NBASE;

		if (rem < 0)
		{
			rem += NBASE;
			carry -= 1;
		}
		lanes[i] = rem;
		lanes[i - 1] += carry;
	}
	/* the top lane determines the sign */
	if (lanes[0] < 0)
	{
		/* negate the whole vector and re-normalize */
		negative = true;
		for (i = 0; i < FLATSUM_NLANES; i++)
			lanes[i] = -lanes[i];
		for (i = FLATSUM_NLANES - 1; i > 0; i--)
		{
			if (lanes[i] < 0)
			{
				lanes[i] += NBASE;
				lanes[i - 1] -= 1;
			}
		}
	}
	if (lanes[0] >= NBASE)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("flat sum aggregation result exceeds the supported range")));

	/*
	 * Emit "<sign>D...D.D...D" with the decimal point after the lane whose
	 * weight is 0, then let numeric_in parse it.  Trailing fractional
	 * digits are trimmed to max_dscale (they are zero beyond it by
	 * construction, since no input carried digits there).
	 */
	initStringInfo(&buf);
	if (negative)
		appendStringInfoChar(&buf, '-');

	/* integer part: lanes for weights MAX_WEIGHT..0 */
	first = -1;
	for (i = 0; i <= FLATSUM_LANE(0); i++)
	{
		if (first < 0)
		{
			if (lanes[i] == 0 && i < FLATSUM_LANE(0))
				continue;		/* skip leading zero lanes */
			first = i;
			appendStringInfo(&buf, INT64_FORMAT, lanes[i]);
		}
		else
			appendStringInfo(&buf, "%04d", (int) lanes[i]);
	}

	/* fractional part, trimmed to max_dscale digits */
	if (state->max_dscale > 0)
	{
		char		frac[FLATSUM_NLANES * DEC_DIGITS + 1];
		int			pos = 0;
		int			dscale;

		/*
		 * Digits below the lane window are provably zero (values carrying
		 * any would have errored in flatsum_add), so an input dscale
		 * exceeding the window's fractional capacity is satisfied by
		 * zero-padding; clamp to the buffer, at the cosmetic price of
		 * displaying fewer trailing zeroes than stock sum() in that
		 * pathological case.
		 */
		dscale = Min(state->max_dscale,
					 (FLATSUM_NLANES - FLATSUM_LANE(0) - 1) * DEC_DIGITS);

		for (i = FLATSUM_LANE(0) + 1; i < FLATSUM_NLANES && pos < dscale; i++)
		{
			snprintf(frac + pos, sizeof(frac) - pos, "%04d", (int) lanes[i]);
			pos += DEC_DIGITS;
		}
		if (pos < dscale)
			memset(frac + pos, '0', dscale - pos);
		frac[dscale] = '\0';
		appendStringInfoChar(&buf, '.');
		appendStringInfoString(&buf, frac);
	}

	return DirectFunctionCall3(numeric_in, CStringGetDatum(buf.data),
							   ObjectIdGetDatum(InvalidOid),
							   Int32GetDatum(-1));
}

Datum
numeric_flat_sum_final(PG_FUNCTION_ARGS)
{
	FlatSumState *state;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
	state = (FlatSumState *) PG_GETARG_BYTEA_P(0);
	if (state->nvalues == 0)
		PG_RETURN_NULL();

	return flatsum_build_result(state);
}

Datum
numeric_flat_avg_final(PG_FUNCTION_ARGS)
{
	FlatSumState *state;
	Datum		sum;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
	state = (FlatSumState *) PG_GETARG_BYTEA_P(0);
	if (state->nvalues == 0)
		PG_RETURN_NULL();

	sum = flatsum_build_result(state);
	return DirectFunctionCall2(numeric_div, sum,
							   NumericGetDatum(int64_to_numeric(state->nvalues)));
}

/* typmod decoding, same as numeric.c's private macros */
#define FLATSUM_TYPMOD_PRECISION(t)	((((t) - VARHDRSZ) >> 16) & 0xffff)
#define FLATSUM_TYPMOD_SCALE(t)		(((((t) - VARHDRSZ) & 0x7ff) ^ 1024) - 1024)

/*
 * Planner support function, attached to pg_catalog.sum(numeric) and
 * avg(numeric) by the install script (via ALTER FUNCTION ... SUPPORT).
 *
 * On SupportRequestSimplifyAggref, replace the stock aggregate with our
 * flat-state twin so that queries become eligible for parallel shared
 * hash aggregation without any query or search_path change.
 *
 * The replacement is refused unless the argument's typmod proves that no
 * input can fall outside the flat state's lane window: the flat variants
 * raise an error there, while the stock aggregates would not.  For inputs
 * like numeric(15,2) the two are otherwise result-identical, including
 * display scale (avg divides through numeric_div on both sides).
 */
Datum
shared_numeric_agg_support(PG_FUNCTION_ARGS)
{
	Node	   *rawreq = (Node *) PG_GETARG_POINTER(0);

	if (IsA(rawreq, SupportRequestSimplifyAggref))
	{
		SupportRequestSimplifyAggref *req = (SupportRequestSimplifyAggref *) rawreq;
		Aggref	   *aggref = req->aggref;
		char	   *aggname;
		TargetEntry *tle;
		int32		typmod;
		int			precision;
		int			scale;
		Oid			nspoid;
		Oid			argtypes[1] = {NUMERICOID};
		Oid			replacement;
		Aggref	   *newagg;

		/* Rewrite only when the parallel shared hash path could profit. */
		if (!enable_parallel_hash_agg ||
			req->root == NULL ||
			req->root->glob == NULL ||
			!req->root->glob->parallelModeOK)
			PG_RETURN_POINTER(NULL);

		if (aggref->aggfnoid == F_SUM_NUMERIC)
			aggname = "sum";
		else if (aggref->aggfnoid == F_AVG_NUMERIC)
			aggname = "avg";
		else
			PG_RETURN_POINTER(NULL);

		/* plain one-argument aggregation only */
		if (aggref->aggkind != AGGKIND_NORMAL ||
			list_length(aggref->args) != 1 ||
			aggref->aggdistinct != NIL ||
			aggref->aggorder != NIL)
			PG_RETURN_POINTER(NULL);

		/* the typmod must bound the value range to the lane window */
		tle = (TargetEntry *) linitial(aggref->args);
		typmod = exprTypmod((Node *) tle->expr);
		if (typmod < (int32) VARHDRSZ)
			PG_RETURN_POINTER(NULL);
		precision = FLATSUM_TYPMOD_PRECISION(typmod);
		scale = FLATSUM_TYPMOD_SCALE(typmod);
		if (precision - scale > DEC_DIGITS * (FLATSUM_MAX_WEIGHT + 1) ||
			scale > DEC_DIGITS * (FLATSUM_NLANES - FLATSUM_MAX_WEIGHT - 1))
			PG_RETURN_POINTER(NULL);

		/*
		 * Resolve the replacement aggregate living in the same schema as
		 * this support function, independently of search_path.
		 */
		nspoid = get_func_namespace(fcinfo->flinfo->fn_oid);
		replacement =
			LookupFuncName(list_make2(makeString(get_namespace_name(nspoid)),
									  makeString(aggname)),
						   1, argtypes, true);
		if (!OidIsValid(replacement) || replacement == aggref->aggfnoid)
			PG_RETURN_POINTER(NULL);

		newagg = copyObject(aggref);
		newagg->aggfnoid = replacement;
		PG_RETURN_POINTER(newagg);
	}

	PG_RETURN_POINTER(NULL);
}
