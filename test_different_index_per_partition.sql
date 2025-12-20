-- Schema demonstrating different optimal index choices per partition
-- Partition 0: Index on 'status' is optimal
-- Partition 1: Index on 'category' is optimal

DROP TABLE IF EXISTS orders CASCADE;

-- Create hash-partitioned table
CREATE TABLE orders (
    order_id INTEGER,
    status TEXT,
    category TEXT,
    amount NUMERIC(10,2)
) PARTITION BY HASH (order_id);

-- Create two hash partitions
CREATE TABLE orders_p0 PARTITION OF orders
    FOR VALUES WITH (MODULUS 2, REMAINDER 0);

CREATE TABLE orders_p1 PARTITION OF orders
    FOR VALUES WITH (MODULUS 2, REMAINDER 1);

-- Create indexes on both columns (propagated to all partitions)
CREATE INDEX orders_status_idx ON orders(status);
CREATE INDEX orders_category_idx ON orders(category);

-- Populate partition 0:
-- - status='active' is RARE (1% - high selectivity, good for index)
-- - category='premium' is COMMON (50% - low selectivity, poor for index)
INSERT INTO orders (order_id, status, category, amount)
SELECT
    i * 2,  -- Even IDs go to partition 0 (hash mod 2 = 0)
    CASE WHEN i % 100 = 0 THEN 'active' ELSE 'inactive' END,  -- 1% active
    CASE WHEN i % 2 = 0 THEN 'premium' ELSE 'standard' END,   -- 50% premium
    (random() * 1000)::numeric(10,2)
FROM generate_series(1, 100000) i;

-- Populate partition 1:
-- - status='active' is COMMON (50% - low selectivity, poor for index)
-- - category='premium' is RARE (1% - high selectivity, good for index)
INSERT INTO orders (order_id, status, category, amount)
SELECT
    i * 2 + 1,  -- Odd IDs go to partition 1 (hash mod 2 = 1)
    CASE WHEN i % 2 = 0 THEN 'active' ELSE 'inactive' END,    -- 50% active
    CASE WHEN i % 100 = 0 THEN 'premium' ELSE 'standard' END, -- 1% premium
    (random() * 1000)::numeric(10,2)
FROM generate_series(1, 100000) i;

ANALYZE orders;

\echo '============================================================'
\echo 'Data distribution per partition:'
\echo '============================================================'

SELECT 'Partition 0' as partition,
       COUNT(*) FILTER (WHERE status = 'active') as active_count,
       COUNT(*) FILTER (WHERE category = 'premium') as premium_count,
       COUNT(*) FILTER (WHERE status = 'active' AND category = 'premium') as both_count,
       COUNT(*) as total
FROM orders_p0
UNION ALL
SELECT 'Partition 1' as partition,
       COUNT(*) FILTER (WHERE status = 'active') as active_count,
       COUNT(*) FILTER (WHERE category = 'premium') as premium_count,
       COUNT(*) FILTER (WHERE status = 'active' AND category = 'premium') as both_count,
       COUNT(*) as total
FROM orders_p1;

\echo ''
\echo '============================================================'
\echo 'Query with both conditions:'
\echo '============================================================'
\echo ''
\echo 'Expected behavior per partition:'
\echo '  Partition 0: Should use status index (1% selectivity)'
\echo '              Then filter for category=premium (50% of those)'
\echo '              Total: ~500 rows examined'
\echo ''
\echo '  Partition 1: Should use category index (1% selectivity)'
\echo '              Then filter for status=active (50% of those)'
\echo '              Total: ~500 rows examined'
\echo ''

EXPLAIN (ANALYZE, COSTS, BUFFERS)
SELECT order_id, status, category, amount
FROM orders
WHERE status = 'active' AND category = 'premium'
ORDER BY order_id;

\echo ''
\echo '============================================================'
\echo 'Alternative: Disable one index to force different choice'
\echo '============================================================'

-- Force partition 0 to use status index
DROP INDEX orders_p0_category_idx;

-- Force partition 1 to use category index
DROP INDEX orders_p1_status_idx;

ANALYZE orders;

\echo ''
\echo 'With partition-specific indexes:'
EXPLAIN (ANALYZE, COSTS, BUFFERS)
SELECT order_id, status, category, amount
FROM orders
WHERE status = 'active' AND category = 'premium'
ORDER BY order_id;

-- Cleanup
DROP TABLE orders CASCADE;
