-- Corner-case tables added for the production run.
-- 30% NULLs: the transition function is skipped for those rows entirely.
DROP TABLE IF EXISTS t_null;
CREATE TABLE t_null AS
SELECT (CASE WHEN i % 10 < 3 THEN NULL
             ELSE (((i % 99999999)::numeric)/100)::numeric(10,2) END) v
FROM generate_series(1,5000000) i;

-- NaN / +-Inf mixed in: specials bypass the accumulator and are counted
-- separately, so they exercise the early-out rather than the fast path.
DROP TABLE IF EXISTS t_spec;
CREATE TABLE t_spec AS
SELECT (CASE WHEN i % 100 = 0 THEN 'NaN'::numeric
             WHEN i % 100 = 1 THEN 'Infinity'::numeric
             WHEN i % 100 = 2 THEN '-Infinity'::numeric
             ELSE (((i % 99999999)::numeric)/100)::numeric(10,2)::numeric END) v
FROM generate_series(1,5000000) i;

-- Grouping key whose numerically-equal values differ bytewise at equal
-- length (same digits, different display scale), so numeric_eq()'s memcmp
-- fast path runs and misses on every comparison.  7 and 1000 are coprime, so
-- every value occurs in both representations.
DROP TABLE IF EXISTS t_gmix;
CREATE TABLE t_gmix AS
SELECT (CASE WHEN i % 7 < 4
             THEN (((i % 1000)::numeric)/10)::numeric(20,1)::numeric
             ELSE (((i % 1000)::numeric)/10)::numeric(20,2)::numeric END) g,
       (((i % 99999999)::numeric)/100)::numeric(10,2) v
FROM generate_series(1,5000000) i;

VACUUM ANALYZE t_null, t_spec, t_gmix;
CHECKPOINT;

SELECT 't_null: null fraction' k, round(100.0*count(*) FILTER (WHERE v IS NULL)/count(*),1)::text v FROM t_null
UNION ALL SELECT 't_spec: specials', (count(*) FILTER (WHERE v IS NOT NULL AND NOT (v > '-Infinity'::numeric AND v < 'Infinity'::numeric) OR v = 'NaN'::numeric))::text FROM t_spec
UNION ALL SELECT 't_gmix: values / byte images',
   (SELECT count(DISTINCT g)::text FROM t_gmix) || ' / ' ||
   (SELECT count(*)::text FROM (SELECT DISTINCT g::text, pg_column_size(g) FROM t_gmix) z)
UNION ALL SELECT 't_gmix: equal-value pairs at equal length',
   (SELECT count(*)::text FROM (SELECT g, pg_column_size(g) s FROM t_gmix GROUP BY 1,2) z
     GROUP BY g HAVING count(*) > 1 LIMIT 1);
