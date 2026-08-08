\pset pager off
-- Wide corpus of numerics, including values around the 32-digit local-buffer
-- boundary, specials, mixed display scales, and toastable monsters.
DROP TABLE IF EXISTS corpus;
CREATE TABLE corpus (id int, n numeric);
INSERT INTO corpus
SELECT g, v FROM (
  SELECT row_number() OVER () g, v FROM (
    SELECT unnest(ARRAY[
      0, -0, 0.0, 0.00, 1, -1, 1.0, 1.00, 1.000000, 0.1, 0.10, 100, 100.0,
      1e-16, 1e16, 12345.67, -12345.67, 12345.6700,
      'NaN','Infinity','-Infinity']::numeric[]) v
    UNION ALL
    -- ndigits sweep straddling NUMERIC_LOCAL_NDIGITS (=32 digits of 4 decimals)
    SELECT (repeat('9', k) || '.' || repeat('1', (k % 7)))::numeric FROM generate_series(1, 300) k
    UNION ALL
    SELECT (repeat('9', k))::numeric * (CASE WHEN k % 2 = 0 THEN -1 ELSE 1 END)
      FROM generate_series(120, 140) k
    UNION ALL
    SELECT ((i::numeric)/7)::numeric(40,20) FROM generate_series(1, 200) i
    UNION ALL
    SELECT (repeat('7', 4000))::numeric
    UNION ALL
    SELECT (repeat('3', 9000) || '.' || repeat('2', 900))::numeric
  ) s
) t;

-- 1. hash values must be bit-identical to the unpatched build
SELECT 'hash32'  AS what, md5(string_agg(hash_numeric(n)::text, ',' ORDER BY id)) AS digest FROM corpus
UNION ALL
SELECT 'hash64', md5(string_agg(hash_numeric_extended(n, 0)::text, ',' ORDER BY id)) FROM corpus
UNION ALL
SELECT 'hash64seed', md5(string_agg(hash_numeric_extended(n, 987654321)::text, ',' ORDER BY id)) FROM corpus
UNION ALL
-- 2. full pairwise comparison matrix over a subset (all six operators + cmp)
SELECT 'cmp-matrix', md5(string_agg(
        (a.n < b.n)::text || (a.n <= b.n)::text || (a.n = b.n)::text ||
        (a.n <> b.n)::text || (a.n >= b.n)::text || (a.n > b.n)::text ||
        numeric_cmp(a.n, b.n)::text, ',' ORDER BY a.id, b.id))
   FROM corpus a, corpus b WHERE a.id <= 120 AND b.id <= 120
UNION ALL
-- 3. same, but with one side forced through toast (compressed/external)
SELECT 'cmp-toasted', md5(string_agg(
        (a.n = b.n)::text || (a.n < b.n)::text || numeric_cmp(a.n, b.n)::text, ',' ORDER BY a.id, b.id))
   FROM corpus a, (SELECT id, n FROM corpus ORDER BY length(n::text) DESC LIMIT 20) b
   WHERE a.id <= 200
UNION ALL
-- 4. grouping must put numerically-equal values (any display scale) together
SELECT 'grouping', md5(string_agg(n::text || ':' || c::text, ',' ORDER BY n))
   FROM (SELECT n, count(*) c FROM corpus GROUP BY n) g
UNION ALL
-- 5. sort order
SELECT 'sortorder', md5(string_agg(n::text, ',' ORDER BY n, id)) FROM corpus
UNION ALL
-- 6. hash join / hash-based DISTINCT agreement with the sorted path
SELECT 'distinct-hash', md5(string_agg(x::text, ',' ORDER BY x)) FROM (SELECT DISTINCT n x FROM corpus) t
UNION ALL
-- 7. hash partitioning routing must be stable
SELECT 'hashpart', md5(string_agg(satisfies_hash_partition('hp'::regclass, 8, k, n)::text, ',' ORDER BY id, k))
   FROM corpus, generate_series(0,7) k
   WHERE (SELECT count(*) FROM pg_class WHERE relname = 'hp') > 0;
