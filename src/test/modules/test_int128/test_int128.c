/*-------------------------------------------------------------------------
 *
 * test_int128.c
 *	  Testbed for roll-our-own 128-bit integer arithmetic.
 *
 * This is a standalone test program that compares the behavior of an
 * implementation in int128.h to an (assumed correct) int128 native type.
 *
 * Copyright (c) 2017-2026, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/test/modules/test_int128/test_int128.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include <time.h>

/* Require a native int128 type */
#ifdef HAVE_INT128

/*
 * By default, we test the non-native implementation in int128.h; but
 * by predefining USE_NATIVE_INT128 to 1, you can test the native
 * implementation, just to be sure.
 */
#ifndef USE_NATIVE_INT128
#define USE_NATIVE_INT128 0
#endif

/*
 * The overflow-checked primitives have a third configuration that neither
 * setting of USE_NATIVE_INT128 reaches on a compiler that has the overflow
 * builtins: native int128 arithmetic combined with the manual sign rule.
 * Defining TEST_INT128_NO_BUILTIN_OVERFLOW suppresses the builtins so that
 * combination can be built and checked here as well.
 */
#ifdef TEST_INT128_NO_BUILTIN_OVERFLOW
#undef HAVE__BUILTIN_OP_OVERFLOW
#endif

#include "common/int128.h"
#include "common/pg_prng.h"

/*
 * We assume the parts of this union are laid out compatibly.
 */
typedef union
{
	int128		i128;
	INT128		I128;
	struct
	{
#ifdef WORDS_BIGENDIAN
		int64		hi;
		uint64		lo;
#else
		uint64		lo;
		int64		hi;
#endif
	}			hl;
} test128;

#define INT128_HEX_FORMAT	"%016" PRIx64 "%016" PRIx64

/*
 * Control version of comparator.
 */
static inline int
my_int128_compare(int128 x, int128 y)
{
	if (x < y)
		return -1;
	if (x > y)
		return 1;
	return 0;
}

/*
 * Main program.
 *
 * Generates a lot of random numbers and tests the implementation for each.
 * The results should be reproducible, since we use a fixed PRNG seed.
 *
 * You can give a loop count if you don't like the default 1B iterations.
 */
int
main(int argc, char **argv)
{
	long		count;

	pg_prng_seed(&pg_global_prng_state, (uint64) time(NULL));

	if (argc >= 2)
		count = strtol(argv[1], NULL, 0);
	else
		count = 1000000000;

	/*
	 * Deterministic boundary checks for the overflow-checked operations,
	 * hitting the exact edges that random inputs will practically never find.
	 * A failure here means overflow detection is wrong at the range limits.
	 */
	{
		const int128 max128 = (int128) ((~(uint128) 0) >> 1);	/* 2^127 - 1 */
		const int128 min128 = -max128 - 1;	/* -2^127 */
		test128		a;
		test128		saved;

		/* max + 1 must overflow and leave the accumulator unchanged */
		a.i128 = max128;
		saved = a;
		if (!int128_add_int128_overflow(&a.I128, int64_to_int128(1)) ||
			a.hl.hi != saved.hl.hi || a.hl.lo != saved.hl.lo)
		{
			printf("add_overflow failure: max + 1\n");
			return 1;
		}

		/* max + (-1) must not overflow */
		a.i128 = max128;
		if (int128_add_int128_overflow(&a.I128, int64_to_int128(-1)) ||
			a.i128 != max128 - 1)
		{
			printf("add_overflow failure: max + (-1)\n");
			return 1;
		}

		/* min - 1 must overflow and leave the accumulator unchanged */
		a.i128 = min128;
		saved = a;
		if (!int128_sub_int128_overflow(&a.I128, int64_to_int128(1)) ||
			a.hl.hi != saved.hl.hi || a.hl.lo != saved.hl.lo)
		{
			printf("sub_overflow failure: min - 1\n");
			return 1;
		}

		/* min - min must not overflow (opposite-sign result crossing zero) */
		a.i128 = min128;
		saved = a;
		if (int128_sub_int128_overflow(&a.I128, saved.I128) || a.i128 != 0)
		{
			printf("sub_overflow failure: min - min\n");
			return 1;
		}

		/* max - (-1) must overflow */
		a.i128 = max128;
		if (!int128_sub_int128_overflow(&a.I128, int64_to_int128(-1)))
		{
			printf("sub_overflow failure: max - (-1)\n");
			return 1;
		}

		/* (max / 10) * 10 fits; (max / 10 + 1) * 10 must overflow */
		a.i128 = max128 / 10;
		if (int128_mul_pow10_overflow(&a.I128, 1) ||
			a.i128 != (max128 / 10) * 10)
		{
			printf("mul_pow10 failure: (max / 10) * 10\n");
			return 1;
		}
		a.i128 = max128 / 10 + 1;
		saved = a;
		if (!int128_mul_pow10_overflow(&a.I128, 1) ||
			a.hl.hi != saved.hl.hi || a.hl.lo != saved.hl.lo)
		{
			printf("mul_pow10 failure: (max / 10 + 1) * 10\n");
			return 1;
		}

		/* k = 0 must be a no-op even at the extremes */
		a.i128 = min128;
		if (int128_mul_pow10_overflow(&a.I128, 0) || a.i128 != min128)
		{
			printf("mul_pow10 failure: min * 10^0\n");
			return 1;
		}

		/* zero stays zero for the largest exponent */
		a.i128 = 0;
		if (int128_mul_pow10_overflow(&a.I128, 38) || a.i128 != 0)
		{
			printf("mul_pow10 failure: 0 * 10^38\n");
			return 1;
		}

		/* 1 * 10^38 fits (10^38 < 2^127); 10 * 10^38 must overflow */
		a.i128 = 1;
		if (int128_mul_pow10_overflow(&a.I128, 38))
		{
			printf("mul_pow10 failure: 1 * 10^38\n");
			return 1;
		}
		a.i128 = 10;
		if (!int128_mul_pow10_overflow(&a.I128, 38))
		{
			printf("mul_pow10 failure: 10 * 10^38\n");
			return 1;
		}
	}

	while (count-- > 0)
	{
		int64		x = pg_prng_int64(&pg_global_prng_state);
		int64		y = pg_prng_int64(&pg_global_prng_state);
		int64		z = pg_prng_int64(&pg_global_prng_state);
		int64		w = pg_prng_int64(&pg_global_prng_state);
		int32		z32 = pg_prng_int32(&pg_global_prng_state);
		test128		t1;
		test128		t2;
		test128		t3;
		int32		r1;
		int32		r2;

		/* prevent division by zero in the 128/32-bit division test */
		while (z32 == 0)
			z32 = pg_prng_int32(&pg_global_prng_state);

		/* check unsigned addition */
		t1.hl.hi = x;
		t1.hl.lo = y;
		t2 = t1;
		t1.i128 += (int128) (uint64) z;
		int128_add_uint64(&t2.I128, (uint64) z);

		if (t1.hl.hi != t2.hl.hi || t1.hl.lo != t2.hl.lo)
		{
			printf(INT128_HEX_FORMAT " + unsigned %016" PRIx64 "\n", x, y, z);
			printf("native = " INT128_HEX_FORMAT "\n", t1.hl.hi, t1.hl.lo);
			printf("result = " INT128_HEX_FORMAT "\n", t2.hl.hi, t2.hl.lo);
			return 1;
		}

		/* check signed addition */
		t1.hl.hi = x;
		t1.hl.lo = y;
		t2 = t1;
		t1.i128 += (int128) z;
		int128_add_int64(&t2.I128, z);

		if (t1.hl.hi != t2.hl.hi || t1.hl.lo != t2.hl.lo)
		{
			printf(INT128_HEX_FORMAT " + signed %016" PRIx64 "\n", x, y, z);
			printf("native = " INT128_HEX_FORMAT "\n", t1.hl.hi, t1.hl.lo);
			printf("result = " INT128_HEX_FORMAT "\n", t2.hl.hi, t2.hl.lo);
			return 1;
		}

		/* check 128-bit signed addition */
		t1.hl.hi = x;
		t1.hl.lo = y;
		t2 = t1;
		t3.hl.hi = z;
		t3.hl.lo = w;
		t1.i128 += t3.i128;
		int128_add_int128(&t2.I128, t3.I128);

		if (t1.hl.hi != t2.hl.hi || t1.hl.lo != t2.hl.lo)
		{
			printf(INT128_HEX_FORMAT " + " INT128_HEX_FORMAT "\n", x, y, z, w);
			printf("native = " INT128_HEX_FORMAT "\n", t1.hl.hi, t1.hl.lo);
			printf("result = " INT128_HEX_FORMAT "\n", t2.hl.hi, t2.hl.lo);
			return 1;
		}

		/* check unsigned subtraction */
		t1.hl.hi = x;
		t1.hl.lo = y;
		t2 = t1;
		t1.i128 -= (int128) (uint64) z;
		int128_sub_uint64(&t2.I128, (uint64) z);

		if (t1.hl.hi != t2.hl.hi || t1.hl.lo != t2.hl.lo)
		{
			printf(INT128_HEX_FORMAT " - unsigned %016" PRIx64 "\n", x, y, z);
			printf("native = " INT128_HEX_FORMAT "\n", t1.hl.hi, t1.hl.lo);
			printf("result = " INT128_HEX_FORMAT "\n", t2.hl.hi, t2.hl.lo);
			return 1;
		}

		/* check signed subtraction */
		t1.hl.hi = x;
		t1.hl.lo = y;
		t2 = t1;
		t1.i128 -= (int128) z;
		int128_sub_int64(&t2.I128, z);

		if (t1.hl.hi != t2.hl.hi || t1.hl.lo != t2.hl.lo)
		{
			printf(INT128_HEX_FORMAT " - signed %016" PRIx64 "\n", x, y, z);
			printf("native = " INT128_HEX_FORMAT "\n", t1.hl.hi, t1.hl.lo);
			printf("result = " INT128_HEX_FORMAT "\n", t2.hl.hi, t2.hl.lo);
			return 1;
		}

		/* check 64x64-bit multiply-add */
		t1.hl.hi = x;
		t1.hl.lo = y;
		t2 = t1;
		t1.i128 += (int128) z * (int128) w;
		int128_add_int64_mul_int64(&t2.I128, z, w);

		if (t1.hl.hi != t2.hl.hi || t1.hl.lo != t2.hl.lo)
		{
			printf(INT128_HEX_FORMAT " + %016" PRIx64 " * %016" PRIx64 "\n", x, y, z, w);
			printf("native = " INT128_HEX_FORMAT "\n", t1.hl.hi, t1.hl.lo);
			printf("result = " INT128_HEX_FORMAT "\n", t2.hl.hi, t2.hl.lo);
			return 1;
		}

		/* check 64x64-bit multiply-subtract */
		t1.hl.hi = x;
		t1.hl.lo = y;
		t2 = t1;
		t1.i128 -= (int128) z * (int128) w;
		int128_sub_int64_mul_int64(&t2.I128, z, w);

		if (t1.hl.hi != t2.hl.hi || t1.hl.lo != t2.hl.lo)
		{
			printf(INT128_HEX_FORMAT " - %016" PRIx64 " * %016" PRIx64 "\n", x, y, z, w);
			printf("native = " INT128_HEX_FORMAT "\n", t1.hl.hi, t1.hl.lo);
			printf("result = " INT128_HEX_FORMAT "\n", t2.hl.hi, t2.hl.lo);
			return 1;
		}

		/* check overflow-checked 128-bit addition */
		t1.hl.hi = x;
		t1.hl.lo = y;
		t2 = t1;
		t3.hl.hi = z;
		t3.hl.lo = w;
		{
			/* reference result via wrap-around-safe unsigned arithmetic */
			test128		expected;
			bool		exp_ovf;
			bool		got_ovf;

			expected.i128 = (int128) ((uint128) t1.i128 + (uint128) t3.i128);
			exp_ovf = ((t1.i128 < 0) == (t3.i128 < 0) &&
					   (expected.i128 < 0) != (t1.i128 < 0));
			if (exp_ovf)
				expected = t1;	/* accumulator must stay unchanged */

			got_ovf = int128_add_int128_overflow(&t2.I128, t3.I128);

			if (got_ovf != exp_ovf ||
				t2.hl.hi != expected.hl.hi || t2.hl.lo != expected.hl.lo)
			{
				printf(INT128_HEX_FORMAT " +ovf " INT128_HEX_FORMAT "\n", x, y, z, w);
				printf("expected ovf=%d " INT128_HEX_FORMAT "\n",
					   exp_ovf, expected.hl.hi, expected.hl.lo);
				printf("result   ovf=%d " INT128_HEX_FORMAT "\n",
					   got_ovf, t2.hl.hi, t2.hl.lo);
				return 1;
			}
		}

		/* check overflow-checked 128-bit subtraction */
		t1.hl.hi = x;
		t1.hl.lo = y;
		t2 = t1;
		t3.hl.hi = z;
		t3.hl.lo = w;
		{
			test128		expected;
			bool		exp_ovf;
			bool		got_ovf;

			expected.i128 = (int128) ((uint128) t1.i128 - (uint128) t3.i128);
			exp_ovf = ((t1.i128 < 0) != (t3.i128 < 0) &&
					   (expected.i128 < 0) != (t1.i128 < 0));
			if (exp_ovf)
				expected = t1;

			got_ovf = int128_sub_int128_overflow(&t2.I128, t3.I128);

			if (got_ovf != exp_ovf ||
				t2.hl.hi != expected.hl.hi || t2.hl.lo != expected.hl.lo)
			{
				printf(INT128_HEX_FORMAT " -ovf " INT128_HEX_FORMAT "\n", x, y, z, w);
				printf("expected ovf=%d " INT128_HEX_FORMAT "\n",
					   exp_ovf, expected.hl.hi, expected.hl.lo);
				printf("result   ovf=%d " INT128_HEX_FORMAT "\n",
					   got_ovf, t2.hl.hi, t2.hl.lo);
				return 1;
			}
		}

		/* check overflow-checked multiplication by 10^k */
		t1.hl.hi = x;
		t1.hl.lo = y;
		t2 = t1;
		{
			const int128 max128 = (int128) ((~(uint128) 0) >> 1);
			const int128 min128 = -max128 - 1;
			int			k = (int) (pg_prng_uint32(&pg_global_prng_state) % 39);
			int128		p10 = 1;
			test128		expected;
			bool		exp_ovf;
			bool		got_ovf;

			for (int i = 0; i < k; i++)
				p10 *= 10;

			exp_ovf = (t1.i128 != 0 && k > 0 &&
					   (t1.i128 > max128 / p10 || t1.i128 < min128 / p10));
			if (exp_ovf)
				expected = t1;
			else
				expected.i128 = t1.i128 * p10;

			got_ovf = int128_mul_pow10_overflow(&t2.I128, k);

			if (got_ovf != exp_ovf ||
				t2.hl.hi != expected.hl.hi || t2.hl.lo != expected.hl.lo)
			{
				printf(INT128_HEX_FORMAT " * 10^%d\n", x, y, k);
				printf("expected ovf=%d " INT128_HEX_FORMAT "\n",
					   exp_ovf, expected.hl.hi, expected.hl.lo);
				printf("result   ovf=%d " INT128_HEX_FORMAT "\n",
					   got_ovf, t2.hl.hi, t2.hl.lo);
				return 1;
			}
		}

		/* check 128/32-bit division */
		t3.hl.hi = x;
		t3.hl.lo = y;
		t1.i128 = t3.i128 / z32;
		r1 = (int32) (t3.i128 % z32);
		t2 = t3;
		int128_div_mod_int32(&t2.I128, z32, &r2);

		if (t1.hl.hi != t2.hl.hi || t1.hl.lo != t2.hl.lo)
		{
			printf(INT128_HEX_FORMAT " / signed %08X\n", t3.hl.hi, t3.hl.lo, z32);
			printf("native = " INT128_HEX_FORMAT "\n", t1.hl.hi, t1.hl.lo);
			printf("result = " INT128_HEX_FORMAT "\n", t2.hl.hi, t2.hl.lo);
			return 1;
		}
		if (r1 != r2)
		{
			printf(INT128_HEX_FORMAT " %% signed %08X\n", t3.hl.hi, t3.hl.lo, z32);
			printf("native = %08X\n", r1);
			printf("result = %08X\n", r2);
			return 1;
		}

		/* check comparison */
		t1.hl.hi = x;
		t1.hl.lo = y;
		t2.hl.hi = z;
		t2.hl.lo = w;

		if (my_int128_compare(t1.i128, t2.i128) !=
			int128_compare(t1.I128, t2.I128))
		{
			printf("comparison failure: %d vs %d\n",
				   my_int128_compare(t1.i128, t2.i128),
				   int128_compare(t1.I128, t2.I128));
			printf("arg1 = " INT128_HEX_FORMAT "\n", t1.hl.hi, t1.hl.lo);
			printf("arg2 = " INT128_HEX_FORMAT "\n", t2.hl.hi, t2.hl.lo);
			return 1;
		}

		/* check case with identical hi parts; above will hardly ever hit it */
		t2.hl.hi = x;

		if (my_int128_compare(t1.i128, t2.i128) !=
			int128_compare(t1.I128, t2.I128))
		{
			printf("comparison failure: %d vs %d\n",
				   my_int128_compare(t1.i128, t2.i128),
				   int128_compare(t1.I128, t2.I128));
			printf("arg1 = " INT128_HEX_FORMAT "\n", t1.hl.hi, t1.hl.lo);
			printf("arg2 = " INT128_HEX_FORMAT "\n", t2.hl.hi, t2.hl.lo);
			return 1;
		}
	}

	return 0;
}

#else							/* ! HAVE_INT128 */

/*
 * For now, do nothing if we don't have a native int128 type.
 */
int
main(int argc, char **argv)
{
	printf("skipping tests: no native int128 type\n");
	return 0;
}

#endif
