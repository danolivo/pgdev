-- Schema to demonstrate MergeAppend explicit sort optimization
-- The REAL scenario: complex queries with joins on partitioned tables

DROP TABLE IF EXISTS prt1 CASCADE;
DROP TABLE IF EXISTS prt2 CASCADE;

-- Create partitioned tables (matching the regression test pattern)
CREATE TABLE prt1 (
    a INT,
    b INT,
    c TEXT
) PARTITION BY HASH (a);

CREATE TABLE prt2 (
    a INT,
    b INT,
    c TEXT
) PARTITION BY HASH (a);

-- Create 2 hash partitions for each table
CREATE TABLE prt1_p0 PARTITION OF prt1 FOR VALUES WITH (MODULUS 2, REMAINDER 0);
CREATE TABLE prt1_p1 PARTITION OF prt1 FOR VALUES WITH (MODULUS 2, REMAINDER 1);

CREATE TABLE prt2_p0 PARTITION OF prt2 FOR VALUES WITH (MODULUS 2, REMAINDER 0);
CREATE TABLE prt2_p1 PARTITION OF prt2 FOR VALUES WITH (MODULUS 2, REMAINDER 1);

-- Populate with test data
INSERT INTO prt1 SELECT i, i % 10, 'data-' || i FROM generate_series(1, 10000) i;
INSERT INTO prt2 SELECT i, i % 100, 'info-' || i FROM generate_series(1, 5000) i;

ANALYZE prt1;
ANALYZE prt2;

\echo '============================================================'
\echo 'Query: Semi-join with filter and ORDER BY'
\echo '============================================================'
\echo ''
\echo 'This demonstrates the actual MergeAppend explicit sort optimization:'
\echo '  - Each partition executes a semi-join independently'
\echo '  - Each partition produces its own result set'
\echo '  - Results need to be ordered by t1.a'
\echo ''
\echo 'OLD behavior: Sort -> Append -> [join results from all partitions]'
\echo '  - All join results are appended first'
\echo '  - Then a single Sort operation on the combined result'
\echo ''
\echo 'NEW behavior: MergeAppend -> Sort -> [join partition 0]'
\echo '                          -> Sort -> [join partition 1]'
\echo '  - Each partition''s join result is sorted independently'
\echo '  - MergeAppend merges the pre-sorted streams'
\echo '  - More efficient for multiple smaller sorts vs one large sort'
\echo ''

EXPLAIN (COSTS OFF)
SELECT t1.*
FROM prt1 t1
WHERE t1.a IN (SELECT t2.b FROM prt2 t2 WHERE t2.a < 100)
  AND t1.b = 5
ORDER BY t1.a;

\echo ''
\echo '============================================================'
\echo 'Key insight:'
\echo '============================================================'
\echo 'The optimization is NOT about index selection!'
\echo 'It is about COMPLEX SUBPATHS (joins, aggregations) where:'
\echo '  1. Each partition executes expensive operations'
\echo '  2. Each produces a moderate-sized result set'
\echo '  3. Sorting multiple small sets + merging is cheaper than'
\echo '     one large sort of the combined results'
\echo ''

-- Cleanup
DROP TABLE prt1 CASCADE;
DROP TABLE prt2 CASCADE;
