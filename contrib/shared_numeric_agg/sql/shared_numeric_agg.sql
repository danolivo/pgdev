CREATE EXTENSION shared_numeric_agg SCHEMA public;

-- correctness against the stock aggregates, including NULLs, negative
-- values, mixed scales and special values within the lane window
CREATE TABLE snatest (g int4, n numeric(12,4));
INSERT INTO snatest
  SELECT i % 37,
         CASE WHEN i % 11 = 0 THEN NULL
              ELSE (i % 4001 - 2000) + (i % 997) * 0.0001 END
  FROM generate_series(1, 20000) i;

SELECT count(*) AS mismatched_rows FROM (
    (SELECT g, public.sum(n) s, public.avg(n) a FROM snatest GROUP BY g
     EXCEPT
     SELECT g, pg_catalog.sum(n), pg_catalog.avg(n) FROM snatest GROUP BY g)
    UNION ALL
    (SELECT g, pg_catalog.sum(n), pg_catalog.avg(n) FROM snatest GROUP BY g
     EXCEPT
     SELECT g, public.sum(n), public.avg(n) FROM snatest GROUP BY g)
) diff;

-- all-NULL group and empty input behave like the stock aggregates
SELECT public.sum(n), public.avg(n), pg_catalog.sum(n), pg_catalog.avg(n)
FROM snatest WHERE n IS NULL;
SELECT public.sum(n), public.avg(n) FROM snatest WHERE false;

-- NaN is absorbed exactly like the stock sum
SELECT public.sum(x), pg_catalog.sum(x)
FROM unnest(ARRAY['1.5'::numeric, 'NaN', '2']) u(x);

-- a value outside the lane window must raise an error rather than lose
-- digits silently
SELECT public.sum(x) FROM unnest(ARRAY[1e70::numeric]) u(x);

DROP TABLE snatest;

-- The support function rewrites the stock sum(numeric)/avg(numeric) into
-- the flat-state twins when parallel shared hash aggregation is enabled
-- and the argument's typmod bounds every value into the lane window.  A
-- "Parallel HashAggregate" plan is itself proof of the rewrite: the stock
-- aggregates' 'internal' transition state is not eligible for it.
CREATE TABLE snapar (g int4, n numeric(12,4));
INSERT INTO snapar
  SELECT i % 5000, (i % 4001 - 2000) + (i % 997) * 0.0001
  FROM generate_series(1, 20000) i;
ANALYZE snapar;

SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET min_parallel_table_scan_size = 0;
SET max_parallel_workers_per_gather = 2;
SET enable_parallel_hash_agg = on;

EXPLAIN (COSTS OFF)
SELECT g, sum(n), avg(n) FROM snapar GROUP BY g;

-- typmod that cannot be proven safe: no rewrite, ordinary plan
EXPLAIN (COSTS OFF)
SELECT g, sum(n::numeric) FROM snapar GROUP BY g;

-- results must match the stock aggregates (the rewrite is refused when
-- the GUC is off, so that is the reference)
CREATE TABLE snapar_on AS SELECT g, sum(n) s, avg(n) a FROM snapar GROUP BY g;
SET enable_parallel_hash_agg = off;
CREATE TABLE snapar_off AS SELECT g, sum(n) s, avg(n) a FROM snapar GROUP BY g;
RESET enable_parallel_hash_agg;

SELECT count(*) AS mismatched_rows FROM (
    (TABLE snapar_on EXCEPT TABLE snapar_off)
    UNION ALL
    (TABLE snapar_off EXCEPT TABLE snapar_on)
) diff;

RESET parallel_setup_cost;
RESET parallel_tuple_cost;
RESET min_parallel_table_scan_size;
RESET max_parallel_workers_per_gather;
DROP TABLE snapar, snapar_on, snapar_off;
