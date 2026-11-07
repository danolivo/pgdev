--
-- Bounded sum() and avg() over numeric
--
-- bounded_numeric_sum() and bounded_numeric_avg() accumulate numeric values
-- of bounded precision at the scale given by their second argument.  The
-- answer must always be that of sum() and avg(), dscale included.
--

-- invalid scale arguments
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
