/*-------------------------------------------------------------------------
 *
 * numeric_bounded.h
 *	  Interface between numeric.c and the bounded sum() and avg() aggregates
 *	  in numeric_bounded.c.
 *
 * The aggregates work on integer mantissas and leave everything that needs
 * to know how a numeric is represented to numeric.c, which provides it
 * through the functions below.  Nothing outside src/backend/utils/adt should
 * include this file.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/utils/adt/numeric_bounded.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NUMERIC_BOUNDED_H
#define NUMERIC_BOUNDED_H

#include "common/int128.h"
#include "utils/numeric.h"

/*
 * The highest precision the bounded aggregates accept.
 *
 * The ceiling is set by the accumulator's headroom, not by the width of a
 * mantissa.  At p = 28 a mantissa is below 10^28, a shade over 2^93, so the
 * accumulator takes about 1.7e10 addends before it can reach 2^127.  At
 * p = 36 the mantissa reaches 2^120 and a third addend could already
 * overflow, so that range would need a different accumulator.
 */
#define NUMERIC_BOUNDED_MAX_PRECISION		28

/*
 * The highest precision either factor of a folded product may have.  Both
 * mantissas go through int128_add_int64_mul_int64(), so each has to fit an
 * int64.  The product itself obeys NUMERIC_BOUNDED_MAX_PRECISION, which
 * numeric_agg_support() checks on p1 + p2.
 */
#define NUMERIC_BOUNDED_MAX_FACTOR_PRECISION	18

/* What an input of a bounded aggregate turned out to be */
typedef enum NumericBoundedKind
{
	NUMERIC_BOUNDED_ORDINARY,	/* fits; the mantissa has been computed */
	NUMERIC_BOUNDED_NAN,
	NUMERIC_BOUNDED_PINF,
	NUMERIC_BOUNDED_NINF,
	NUMERIC_BOUNDED_NOFIT,		/* does not fit the scale or the precision */
} NumericBoundedKind;

/* the general state of sum(numeric) and avg(numeric), opaque here */
struct NumericAggState;

extern NumericBoundedKind numeric_bounded_classify(Datum value,
												   int target_scale,
												   INT128 *mantissa);
extern NumericBoundedKind numeric_bounded_classify64(Datum value,
													 int target_scale,
													 int64 *mantissa);
extern Numeric numeric_bounded_special(NumericBoundedKind kind);
extern Numeric int128_div_fast_to_numeric(INT128 val1, int log10val2);
extern struct NumericAggState *numeric_avg_state_from_sum(MemoryContext agg_context,
														  Numeric sum, int64 N,
														  int64 NaNcount,
														  int64 pInfcount,
														  int64 nInfcount);
extern bool numeric_bounded_typmod(int32 typmod, int *precision, int *scale);
extern bool numeric_bounded_const_bounds(Numeric num, int *precision,
										 int *scale);

#endif							/* NUMERIC_BOUNDED_H */
