-- Schema to demonstrate MergeAppend explicit sort optimization
-- Two hash partitions with dual indexing scenario
DROP TABLE IF EXISTS orders CASCADE;
-- Create partitioned table
CREATE TABLE orders (
    order_id SERIAL,
    customer_id INTEGER NOT NULL,
    order_date DATE NOT NULL,
    amount NUMERIC(10,2),
    status TEXT
) PARTITION BY HASH (order_id);

-- Create two hash partitions
CREATE TABLE orders_p0 PARTITION OF orders
    FOR VALUES WITH (MODULUS 2, REMAINDER 0);

CREATE TABLE orders_p1 PARTITION OF orders
    FOR VALUES WITH (MODULUS 2, REMAINDER 1);

-- Create indexes on both partitions
-- Index for filtering by customer_id (high selectivity)
CREATE INDEX orders_customer_idx ON orders(customer_id);
CREATE INDEX orders_date_idx ON orders(order_date);

-- Populate with test data
-- Customer distribution: most orders spread across many customers
-- One customer (12345) has relatively few orders
INSERT INTO orders (customer_id, order_date, amount, status)
SELECT
    CASE
        WHEN i % 1000000 = 0 THEN 12345  -- Target customer: 1% of data
        ELSE (i % 100000) + 1         -- Other customers: 99% of data
    END,
    CURRENT_DATE - (i % 365),
    (random() * 1000)::numeric(10,2),
    CASE WHEN random() < 0.5 THEN 'completed' ELSE 'pending' END
FROM generate_series(1, 10000000) i;

ANALYZE orders;

-- Query scenario:
-- We want orders for customer 12345, ordered by date with LIMIT
--
-- Two competing strategies:
-- 1. Use order_date index (provides sorted output, but scans ~100k rows to find customer 12345)
-- 2. Use customer_id index (finds ~1000 rows quickly) + Sort + LIMIT
--
-- With the new optimization, MergeAppend can choose strategy #2 for each partition

\echo 'Query with ORDER BY and LIMIT for selective customer filter:'
EXPLAIN (ANALYZE, COSTS, BUFFERS)
SELECT order_id, customer_id, order_date, amount
FROM orders
WHERE customer_id = 12345
ORDER BY order_date DESC
LIMIT 10;

SELECT count(DISTINCT order_date), count(DISTINCT customer_id) FROM orders;

\echo ''
\echo 'Expected optimization:'
\echo '  - Each partition uses customer_id index (highly selective)'
\echo '  - Explicit Sort node added per partition'
\echo '  - MergeAppend combines sorted results'
\echo ''
\echo 'Alternative (suboptimal):'
\echo '  - Each partition uses order_date index'
\echo '  - Many rows scanned to filter customer_id = 12345'

-- Cleanup
-- DROP TABLE orders;
