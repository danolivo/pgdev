--
-- Bounded sum() and avg() over numeric
--
-- numeric_agg_support() replaces sum(numeric) and avg(numeric) with
-- bounded_numeric_sum()/bounded_numeric_avg() (or their _mul forms, when the
-- argument is a product) if the argument's precision and scale can be
-- derived at plan time and the precision is at most 28.  The
-- answer must always be that of sum() and avg(), dscale included.
--

-- invalid scale arguments
SELECT bounded_numeric_sum_mul(x, y, 20, 20)
  FROM (VALUES (1.00::numeric, 2.00)) t(x, y);
SELECT bounded_numeric_sum(v, NULL::int4) FROM (VALUES (1.00::numeric)) x(v);
-- the scale must be a constant: it is read only when a state is created
SELECT bounded_numeric_sum(v, s)
  FROM (VALUES (1.00::numeric, 2), (1.000, 3)) x(v, s);
SELECT bounded_numeric_sum(v, 29) FROM (VALUES (1.00::numeric)) x(v);
SELECT bounded_numeric_sum(v, -1) FROM (VALUES (1.00::numeric)) x(v);

-- values that fit, against the general aggregates
SELECT bounded_numeric_sum(v, 2), sum(v), bounded_numeric_avg(v, 2), avg(v)
  FROM (VALUES (1.25::numeric(18,2)), (2.50), (-0.75), (0.01)) x(v);
SELECT bounded_numeric_sum(v, 2), bounded_numeric_avg(v, 2)
  FROM (VALUES (NULL::numeric(18,2))) x(v);
SELECT bounded_numeric_sum(v, 2) FROM (VALUES (1.00::numeric)) x(v) WHERE false;
-- a sum outside int64, at a scale that is not a multiple of DEC_DIGITS
SELECT bounded_numeric_sum(v, 18)::text = sum(v)::text AS wide_matches
  FROM (SELECT '-9999999999.999999999999999999'::numeric(28,18)
		FROM generate_series(1, 1000)) x(v);

-- special values; a numeric(p,s) column cannot hold an infinity, but the
-- aggregates must follow numeric_sum() all the same
SELECT bounded_numeric_sum(v, 2)
  FROM (VALUES ('Infinity'::numeric), (1.00)) x(v);
SELECT bounded_numeric_sum(v, 2)
  FROM (VALUES ('-Infinity'::numeric), (1.00)) x(v);
SELECT bounded_numeric_sum(v, 2)
  FROM (VALUES ('Infinity'::numeric), ('-Infinity')) x(v);
SELECT bounded_numeric_sum(v, 2)
  FROM (VALUES ('NaN'::numeric), ('Infinity')) x(v);
SELECT bounded_numeric_avg(v, 2)
  FROM (VALUES ('-Infinity'::numeric), (1.00)) x(v);

--
-- A value that does not fit the scale makes the state fall back to the
-- general aggregate's; the answer stays that of sum() and avg().
--
SELECT bounded_numeric_sum(v, 2), sum(v) FROM (VALUES (1.234::numeric)) x(v);
SELECT bounded_numeric_sum(v, 2), sum(v) FROM (VALUES (1e26::numeric)) x(v);
SELECT bounded_numeric_sum(v, 2), sum(v)
  FROM (VALUES (1.25::numeric), (1e30), ('NaN')) x(v);
-- falling back after some values, with the dscale of the late value winning
SELECT bounded_numeric_sum(v, 2), sum(v), bounded_numeric_avg(v, 2), avg(v)
  FROM (VALUES (1.25::numeric), (2.50), (1.234), (-0.75)) x(v);
-- special values seen before the fallback are carried over
SELECT bounded_numeric_sum(v, 2), sum(v)
  FROM (VALUES (1.25::numeric), ('Infinity'), (1.234)) x(v);

--
-- The same in parallel: some partial states fall back and some do not, so
-- both serialized forms reach the leader and the combine function sees
-- every pairing.
--
CREATE TABLE bna_fb (g int, v numeric);
INSERT INTO bna_fb
  SELECT i % 10,
		 CASE WHEN i % 9973 = 0 THEN i * 0.001 + 0.0001 ELSE i * 0.01 END
  FROM generate_series(1, 100000) i;
ANALYZE bna_fb;
SET max_parallel_workers_per_gather = 4;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET min_parallel_table_scan_size = 0;
EXPLAIN (costs off)
  SELECT bounded_numeric_sum(v, 2), bounded_numeric_avg(v, 2) FROM bna_fb;
SELECT bounded_numeric_sum(v, 2)::text = sum(v)::text AS sum_matches,
	   bounded_numeric_avg(v, 2)::text = avg(v)::text AS avg_matches
  FROM bna_fb;
SELECT count(*) AS mismatches
  FROM (SELECT g, bounded_numeric_sum(v, 2) AS bs, sum(v) AS s,
			   bounded_numeric_avg(v, 2) AS ba, avg(v) AS a
		FROM bna_fb GROUP BY g) x
 WHERE bs::text IS DISTINCT FROM s::text OR ba::text IS DISTINCT FROM a::text;
RESET max_parallel_workers_per_gather;
RESET parallel_setup_cost;
RESET parallel_tuple_cost;
RESET min_parallel_table_scan_size;
DROP TABLE bna_fb;

--
-- When the substitution happens and when it does not
--
CREATE TABLE t_ok    (v numeric(18,2));
CREATE TABLE t_wide  (v numeric(19,2));
CREATE TABLE t_w28   (v numeric(28,20));
CREATE TABLE t_w29   (v numeric(29,2));
CREATE TABLE t_plain (v numeric);
CREATE TABLE t_int0  (v numeric(18,0));
CREATE TABLE t_frac  (v numeric(18,18));
CREATE DOMAIN money18 AS numeric(18,2);
CREATE TABLE t_dom   (v money18);
-- a domain over a domain
CREATE DOMAIN money18_2 AS money18;
CREATE TABLE t_dom2  (v money18_2);

EXPLAIN (verbose, costs off) SELECT sum(v) FROM t_ok;
EXPLAIN (verbose, costs off) SELECT sum(v) FROM t_int0;
EXPLAIN (verbose, costs off) SELECT sum(v) FROM t_frac;
-- a domain over numeric(18,2): the typmod is taken from the domain
EXPLAIN (verbose, costs off) SELECT sum(v) FROM t_dom;
EXPLAIN (verbose, costs off) SELECT sum(v) FROM t_dom2;
SELECT sum(v) FROM (VALUES ('1.005'::money18_2), ('2.00')) x(v);
-- 19 <= p <= 28 uses the same aggregate
EXPLAIN (verbose, costs off) SELECT sum(v) FROM t_wide;
EXPLAIN (verbose, costs off) SELECT sum(v) FROM t_w28;
-- p = 29 is too wide
EXPLAIN (verbose, costs off) SELECT sum(v) FROM t_w29;
-- no typmod at all
EXPLAIN (verbose, costs off) SELECT sum(v) FROM t_plain;
-- CASE and COALESCE keep a typmod that is the same in every arm
EXPLAIN (verbose, costs off)
  SELECT sum(CASE WHEN v > 0 THEN v ELSE v END) FROM t_ok;
EXPLAIN (verbose, costs off) SELECT sum(coalesce(v, v)) FROM t_ok;
-- with arms of differing typmod it is lost
EXPLAIN (verbose, costs off)
  SELECT sum(CASE WHEN v > 0 THEN v ELSE 0::numeric END) FROM t_ok;

-- A PREPARE parameter carries no typmod.  In a custom plan it is folded to a
-- Const before the support function sees it, and the constant's own digits
-- decide; in a generic plan the Param survives and nothing is substituted.
PREPARE pparam (numeric(18,2)) AS SELECT sum(x) FROM (VALUES ($1)) t(x);
EXPLAIN (verbose, costs off) EXECUTE pparam ('1.00');
EXECUTE pparam ('1.23456');
SET plan_cache_mode = force_generic_plan;
EXPLAIN (verbose, costs off) EXECUTE pparam ('1.00');
EXECUTE pparam ('1.23456');
RESET plan_cache_mode;
DEALLOCATE pparam;

--
-- Arithmetic arguments.  A product has scale s1+s2 and at most p1+p2 digits,
-- a sum or difference has scale max(s1,s2) and at most
-- max(p1-s1,p2-s2)+1+max(s1,s2).  Division is never derived.
--
CREATE TABLE t_ab (a numeric(10,2), b numeric(12,4), c numeric(20,10));

EXPLAIN (verbose, costs off) SELECT sum(a + b) FROM t_ab;
EXPLAIN (verbose, costs off) SELECT sum(a - b) FROM t_ab;
EXPLAIN (verbose, costs off) SELECT sum(-a) FROM t_ab;
-- (a * b) + a is 22 digits at scale 6, then max(16,8)+1+6 = 23
EXPLAIN (verbose, costs off) SELECT sum(a * b + a) FROM t_ab;
-- constants carry no typmod, so their digits are counted from the value
EXPLAIN (verbose, costs off) SELECT sum(a - 1) FROM t_ab;
EXPLAIN (verbose, costs off) SELECT sum(a + 0.05) FROM t_ab;
-- round() and trunc() with a constant scale
EXPLAIN (verbose, costs off) SELECT sum(round(a * b, 2)) FROM t_ab;
EXPLAIN (verbose, costs off) SELECT sum(trunc(a * b, 2)) FROM t_ab;
EXPLAIN (verbose, costs off) SELECT sum(round(a * b)) FROM t_ab;
EXPLAIN (verbose, costs off) SELECT sum(round(a, -1)) FROM t_ab;
-- a non-constant scale is not known at plan time
EXPLAIN (verbose, costs off)
  SELECT sum(round(a, g)) FROM t_ab, generate_series(1,2) g;
-- 10 + 20 = 30 > 28: too wide
EXPLAIN (verbose, costs off) SELECT sum(a * c) FROM t_ab;
-- division is never derived
EXPLAIN (verbose, costs off) SELECT sum(a / b) FROM t_ab;
-- neither is an unbounded operand
EXPLAIN (verbose, costs off) SELECT sum(a * v) FROM t_ab, t_plain;

--
-- A product at the top of the argument is folded into the transition
-- function.  Each factor goes through the same derivation and must fit 18
-- digits; together they must fit 28.
--
CREATE TABLE t_mul (grp int, a numeric(10,2), b numeric(8,4),
					d numeric(20,2), e numeric(6,2));

EXPLAIN (verbose, costs off) SELECT sum(a * b) FROM t_mul;
EXPLAIN (verbose, costs off) SELECT avg(a * b) FROM t_mul;
EXPLAIN (verbose, costs off) SELECT sum((a + 1) * b) FROM t_mul;
EXPLAIN (verbose, costs off) SELECT sum(a * 1.2) FROM t_mul;
EXPLAIN (verbose, costs off) SELECT sum(round(a, 1) * b) FROM t_mul;
-- a cast inside the product
EXPLAIN (verbose, costs off) SELECT sum(a * d::numeric(14,4)) FROM t_mul;
-- the product is not at the top: only the accumulation is specialized
EXPLAIN (verbose, costs off) SELECT sum(a * b + a) FROM t_mul;
-- a factor wider than 18 digits: only the accumulation is specialized
EXPLAIN (verbose, costs off) SELECT sum(d * e) FROM t_mul;
-- nesting: the inner product becomes the left factor
EXPLAIN (verbose, costs off) SELECT sum(a * b * a) FROM t_mul;
-- one more digit and nothing fits any more
EXPLAIN (verbose, costs off) SELECT sum(a * b * d) FROM t_mul;

INSERT INTO t_mul VALUES
	(1,  1.25,  3.1234,  1.00,  1.00),
	(1, -0.10,  0.0001, -2.50,  3.00),
	(1,  0.00,  9999.9999, 0.00, 0.00),
	(2,  99999999.99,  9999.9999,  999999999999999999.99,  9999.99),
	(2, -99999999.99, -9999.9999, -999999999999999999.99, -9999.99),
	(3,  0.01,  0.0001, 0.01, 0.01),
	(3,  NULL,  1.0000, NULL, 1.00),
	(3,  1.00,  NULL,   1.00, NULL),
	(4, 'NaN', 1.0000, 'NaN', 1.00),
	(4,  2.00, 3.0000,  2.00, 3.00);

SET enable_bounded_numeric_agg = off;
CREATE TABLE t_mulref AS
	SELECT grp,
		   sum(a * b)::text          AS s1,
		   avg(a * b)::text          AS s2,
		   sum((a + 1) * b)::text    AS s3,
		   sum(a * 1.2)::text        AS s4,
		   sum(round(a, 1) * b)::text AS s5,
		   sum(a * b + a)::text      AS s6,
		   sum(d * e)::text          AS s7,
		   sum(a * b * a)::text      AS s8
	FROM t_mul GROUP BY grp;
RESET enable_bounded_numeric_agg;

SELECT count(*) AS mul_mismatches
FROM t_mulref r
FULL JOIN (
	SELECT grp,
		   sum(a * b)::text          AS s1,
		   avg(a * b)::text          AS s2,
		   sum((a + 1) * b)::text    AS s3,
		   sum(a * 1.2)::text        AS s4,
		   sum(round(a, 1) * b)::text AS s5,
		   sum(a * b + a)::text      AS s6,
		   sum(d * e)::text          AS s7,
		   sum(a * b * a)::text      AS s8
	FROM t_mul GROUP BY grp) x USING (grp)
WHERE (r.s1, r.s2, r.s3, r.s4, r.s5, r.s6, r.s7, r.s8)
	  IS DISTINCT FROM (x.s1, x.s2, x.s3, x.s4, x.s5, x.s6, x.s7, x.s8);

SELECT grp, sum(a * b), avg(a * b) FROM t_mul GROUP BY grp ORDER BY grp;

-- Special values in a product.  A numeric(p,s) column cannot hold an
-- infinity, so these are reachable only by calling the aggregate directly,
-- but numeric_mul()'s rules must be reproduced all the same.
SELECT bounded_numeric_sum_mul(x, y, 2, 2)
  FROM (VALUES ('Infinity'::numeric, 2.00)) t(x, y);
SELECT bounded_numeric_sum_mul(x, y, 2, 2)
  FROM (VALUES ('Infinity'::numeric, -2.00)) t(x, y);
SELECT bounded_numeric_sum_mul(x, y, 2, 2)
  FROM (VALUES ('Infinity'::numeric, 0.00)) t(x, y);
SELECT bounded_numeric_sum_mul(x, y, 2, 2)
  FROM (VALUES ('-Infinity'::numeric, '-Infinity'::numeric)) t(x, y);
SELECT bounded_numeric_sum_mul(x, y, 2, 2)
  FROM (VALUES ('-Infinity'::numeric, 'Infinity'::numeric)) t(x, y);
SELECT bounded_numeric_sum_mul(x, y, 2, 2)
  FROM (VALUES ('NaN'::numeric, 'Infinity'::numeric)) t(x, y);
SELECT bounded_numeric_sum_mul(x, y, 2, 2)
  FROM (VALUES (NULL::numeric, 2.00)) t(x, y);
-- Factors that do not fit the scales they were specialized on, or a product
-- wider than 28 digits, make the state fall back; the answer is that of
-- sum(x * y).
SELECT bounded_numeric_sum_mul(x, y, 2, 2), sum(x * y)
  FROM (VALUES (1.234::numeric, 2.00)) t(x, y);
SELECT bounded_numeric_sum_mul(x, y, 2, 2), sum(x * y)
  FROM (VALUES (1e20::numeric, 2.00)) t(x, y);
SELECT bounded_numeric_sum_mul(x, y, 0, 0), sum(x * y)
  FROM (VALUES (1e15::numeric, 1e15::numeric)) t(x, y);
SELECT bounded_numeric_sum_mul(x, y, 2, 2), sum(x * y),
	   bounded_numeric_avg_mul(x, y, 2, 2), avg(x * y)
  FROM (VALUES (1.25::numeric, 2.00), (-0.50, 3.00), (1.234, 2.00),
			   (0.10, 0.10)) t(x, y);

-- the values have to match the general aggregate exactly, dscale included
INSERT INTO t_ab VALUES (1.25, 3.1234, 7.0123456789),
						(-0.10, 0.0001, -1.0000000001),
						(99999999.99, 99999999.9999, 1234567890.0123456789);

SET enable_bounded_numeric_agg = off;
SELECT sum(a * b)::text AS r1, sum(a + b)::text AS r2, sum(a - b)::text AS r3,
	   sum(-a)::text AS r4, sum(a * b + a)::text AS r5,
	   sum(a * 1.2)::text AS r6, sum(a - 1)::text AS r7,
	   sum(a * 0.05)::text AS r8,
	   sum(a * c::numeric(14,4))::text AS r9,
	   sum(round(a * b, 2))::text AS r10,
	   sum(trunc(a * b, 2))::text AS r11,
	   sum(round(a * b))::text AS r12,
	   sum(round(a, -1))::text AS r13 FROM t_ab \gset
RESET enable_bounded_numeric_agg;

SELECT sum(a * b)::text = :'r1' AS mul,
	   sum(a + b)::text = :'r2' AS add,
	   sum(a - b)::text = :'r3' AS sub,
	   sum(-a)::text = :'r4' AS neg,
	   sum(a * b + a)::text = :'r5' AS nested,
	   sum(a * 1.2)::text = :'r6' AS const_mul,
	   sum(a - 1)::text = :'r7' AS const_sub,
	   sum(a * 0.05)::text = :'r8' AS const_small
FROM t_ab;

SELECT sum(a * c::numeric(14,4))::text = :'r9' AS cast_mul,
	   sum(round(a * b, 2))::text = :'r10' AS rounded,
	   sum(trunc(a * b, 2))::text = :'r11' AS truncated,
	   sum(round(a * b))::text = :'r12' AS rounded0,
	   sum(round(a, -1))::text = :'r13' AS rounded_neg
FROM t_ab;

-- rounding that carries into an extra integer digit: round(9.99, 1) is 10.0
SELECT sum(round(v, 1))::text FROM (VALUES ('9.99'::numeric(3,2))) x(v);

-- a plpgsql variable carries a typmod and is coerced on assignment
CREATE FUNCTION bna_plpgsql_probe() RETURNS numeric LANGUAGE plpgsql AS $$
DECLARE
	x numeric(10,2) := 1.005;
BEGIN
	RETURN (SELECT sum(y) FROM (VALUES (x), (x)) t(y));
END
$$;
SELECT bna_plpgsql_probe();
DROP FUNCTION bna_plpgsql_probe();

--
-- avg() shares the transition function with sum() and differs only in the
-- final function
--
EXPLAIN (verbose, costs off) SELECT avg(v) FROM t_ok;
EXPLAIN (verbose, costs off) SELECT avg(a + b) FROM t_ab;
EXPLAIN (verbose, costs off) SELECT avg(v) FROM t_w29;

-- shapes that are left alone
EXPLAIN (verbose, costs off) SELECT sum(DISTINCT v) FROM t_ok;
EXPLAIN (verbose, costs off) SELECT sum(v ORDER BY v) FROM t_ok;
EXPLAIN (verbose, costs off) SELECT sum(v) FILTER (WHERE v > 0) FROM t_ok;
EXPLAIN (verbose, costs off) SELECT sum(v) OVER () FROM t_ok;
EXPLAIN (verbose, costs off) SELECT max(v) FROM t_ok;
-- an outer-level aggregate
EXPLAIN (verbose, costs off)
  SELECT (SELECT sum(t_ok.v) FROM t_int0 LIMIT 1) FROM t_ok;
-- the switch
SET enable_bounded_numeric_agg = off;
EXPLAIN (verbose, costs off) SELECT sum(v), avg(v) FROM t_ok;
RESET enable_bounded_numeric_agg;

--
-- Values
--
INSERT INTO t_ok VALUES (1.00), (2.50), (-0.75), (0.01), (1000000000000000.00);

SELECT sum(v) FROM t_ok;
SELECT sum(v) FROM t_ok WHERE false;
SELECT sum(v) FROM (VALUES (NULL::numeric(18,2))) x(v);
SELECT sum(v) FROM (VALUES (1.00::numeric(18,2)), (NULL)) x(v);
SELECT sum(v) OVER (ORDER BY v) FROM t_ok;

-- the result's scale must not depend on the values
SELECT sum(v), pg_typeof(sum(v)) FROM (VALUES (1.00::numeric(18,2))) x(v);
SELECT sum(v) FROM (VALUES (0.00::numeric(18,2))) x(v);
SELECT sum(v) FROM (VALUES (0::numeric(18,0))) x(v);

-- precision boundaries
SELECT sum(v) FROM (VALUES ('999999999999999999'::numeric(18,0))) x(v);
SELECT sum(v) FROM (VALUES ('9999999999999999.99'::numeric(18,2))) x(v);
SELECT sum(v) FROM (VALUES ('0.999999999999999999'::numeric(18,18))) x(v);
SELECT sum(v) FROM (VALUES ('-9999999999999999.99'::numeric(18,2)),
						   ('-9999999999999999.99')) x(v);

-- the sum stops fitting in an int64
SELECT sum(v) FROM (SELECT '9999999999999999.99'::numeric(18,2)
					FROM generate_series(1, 20)) x(v);

-- 19 <= p <= 28: each mantissa is already too wide for an int64
SELECT sum(v) FROM (VALUES ('12345678901234567.89'::numeric(19,2))) x(v);
SELECT sum(v) FROM (VALUES ('99999999999999999.99'::numeric(19,2)),
						   ('99999999999999999.99')) x(v);
SELECT sum(v) FROM (VALUES ('99999999.99999999999999999999'::numeric(28,20))) x(v);
SELECT sum(v) FROM (VALUES ('-99999999.99999999999999999999'::numeric(28,20)),
						   ('0.00000000000000000001')) x(v);
SELECT sum(v) FROM (VALUES ('9999999999999999999999999999'::numeric(28,0))) x(v);
SELECT sum(v) FROM (VALUES ('-9999999999999999999999999999'::numeric(28,0)),
						   ('-9999999999999999999999999999')) x(v);
SELECT sum(v) FROM (VALUES ('0.00'::numeric(19,2))) x(v);
SELECT sum(v) FROM (VALUES ('1.00'::numeric(19,2)), (NULL)) x(v);
SELECT sum(v) FROM (VALUES ('NaN'::numeric(19,2)), ('1.00')) x(v);
SELECT sum(v) FROM (SELECT '99999999999999999.99'::numeric(19,2)
					FROM generate_series(1, 1000)) x(v);
SELECT sum(v) FROM (SELECT '-0.00000000000000000001'::numeric(28,20)
					FROM generate_series(1, 1000)) x(v);
SELECT avg(v) FROM (SELECT '99999999999999999.99'::numeric(19,2)
					FROM generate_series(1, 1000)) x(v);

-- Special values.  A numeric(18,2) column holds NaN but not the infinities,
-- so those are exercised by calling the aggregate directly.
SELECT sum(v) FROM (VALUES ('NaN'::numeric(18,2)), (1.00)) x(v);
SELECT sum(v) FROM (VALUES ('NaN'::numeric(18,2)), (NULL)) x(v);
SELECT 'Infinity'::numeric(18,2);

--
-- Differential check against the general aggregate on wider data.  The
-- comparison is on text: numeric_eq considers 1.0 and 1.00 equal, and a
-- dscale mismatch must be caught too.
--
CREATE TABLE t_bulk (grp int, v numeric(18,4), w numeric(26,10));
INSERT INTO t_bulk
  SELECT i % 97,
		 ((i * 7919) % 1000000)::numeric / 10000,
		 ((i * 104729)::numeric * 1000000007) / 10000000000
  FROM generate_series(1, 20000) i;

SET enable_bounded_numeric_agg = off;
CREATE TABLE t_ref AS
  SELECT grp, sum(v) AS s, sum(w) AS sw, avg(v) AS a, avg(w) AS aw
  FROM t_bulk GROUP BY grp;
SELECT sum(v)::text AS ref, avg(v)::text AS avgref FROM t_bulk \gset
RESET enable_bounded_numeric_agg;

SELECT count(*) AS mismatches
FROM t_ref r
FULL JOIN (SELECT grp, sum(v) AS s, sum(w) AS sw, avg(v) AS a, avg(w) AS aw
		   FROM t_bulk GROUP BY grp) x USING (grp)
WHERE r.s::text IS DISTINCT FROM x.s::text
   OR r.sw::text IS DISTINCT FROM x.sw::text
   OR r.a::text IS DISTINCT FROM x.a::text
   OR r.aw::text IS DISTINCT FROM x.aw::text;

SELECT sum(v)::text = :'ref' AS sum_matches,
	   avg(v)::text = :'avgref' AS avg_matches
FROM t_bulk;

--
-- Parallel aggregation goes through the combine, serialize and deserialize
-- functions; a sum outside int64 with mixed signs exercises the 128-bit
-- state halves on the way.
--
CREATE TABLE t_big (v numeric(18,2), w numeric(26,10));
INSERT INTO t_big
  SELECT CASE WHEN i % 3 = 0 THEN -9999999999999999.99
			  ELSE 9999999999999999.99 END,
		 CASE WHEN i % 3 = 0 THEN -1234567890123456.7890123456
			  ELSE 9876543210987654.3210987654 END
  FROM generate_series(1, 50000) i;
ANALYZE t_big;

SET enable_bounded_numeric_agg = off;
SELECT sum(v)::text AS bigref, sum(w)::text AS bigwref,
	   avg(w)::text AS bigavgref FROM t_big \gset
SELECT sum(a * b)::text AS mulref FROM t_mul \gset
RESET enable_bounded_numeric_agg;

SET max_parallel_workers_per_gather = 4;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET min_parallel_table_scan_size = 0;
EXPLAIN (costs off) SELECT sum(v) FROM t_big;
SELECT sum(v)::text = :'bigref' AS big_parallel,
	   sum(w)::text = :'bigwref' AS wide_parallel,
	   avg(w)::text = :'bigavgref' AS avg_parallel
FROM t_big;
SELECT sum(a * b)::text = :'mulref' AS mul_parallel FROM t_mul;
RESET max_parallel_workers_per_gather;
RESET parallel_setup_cost;
RESET parallel_tuple_cost;
RESET min_parallel_table_scan_size;

SET max_parallel_workers_per_gather = 0;
SELECT sum(v)::text = :'bigref' AS big_serial,
	   sum(w)::text = :'bigwref' AS wide_serial
FROM t_big;
RESET max_parallel_workers_per_gather;

--
-- As with any other planner parameter, turning the switch off does not
-- touch a plan that is already cached: a saved generic plan keeps the
-- bounded aggregate until it is replanned.
--
SET plan_cache_mode = force_generic_plan;
PREPARE pavg AS SELECT avg(v) FROM t_ok;
EXPLAIN (verbose, costs off) EXECUTE pavg;
SET enable_bounded_numeric_agg = off;
EXPLAIN (verbose, costs off) EXECUTE pavg;
DISCARD PLANS;
EXPLAIN (verbose, costs off) EXECUTE pavg;
RESET enable_bounded_numeric_agg;
DEALLOCATE pavg;
RESET plan_cache_mode;

DROP TABLE t_ok, t_wide, t_w28, t_w29, t_plain, t_int0, t_frac, t_dom, t_dom2,
		   t_bulk, t_ref, t_big, t_ab, t_mul, t_mulref;
DROP DOMAIN money18_2;
DROP DOMAIN money18;
