-- ============================================================================
-- Test Scenario: Measuring Local Buffer Flush Cost
-- ============================================================================
--
-- This test measures the performance impact of flushing local (temporary)
-- buffers, which is necessary when parallel workers need to scan temp tables.
--
-- The goal is to derive a cost parameter (local_buffer_flush_cost) that can
-- be used by the optimizer to estimate the overhead of enabling parallel
-- execution on queries involving temporary tables.
--
-- ============================================================================

\timing on

-- Configuration: Ensure we have enough temp buffers for testing
SET temp_buffers = '128MB';
SET work_mem = '64MB';

-- Show current cost parameters for reference
SELECT
    name,
    setting,
    unit,
    short_desc
FROM pg_settings
WHERE name IN ('seq_page_cost', 'random_page_cost', 'cpu_tuple_cost',
               'parallel_setup_cost', 'parallel_tuple_cost')
ORDER BY name;

-- ============================================================================
-- SCENARIO 1: Baseline - Sequential scan of temp table (no flush)
-- ============================================================================
-- This establishes the baseline cost of scanning a temp table without
-- any buffer flushing overhead.

DROP TABLE IF EXISTS temp_baseline;

-- Create a moderately sized temp table (~ 100MB, ~13K pages at 8KB each)
CREATE TEMP TABLE temp_baseline AS
SELECT
    i AS id,
    md5(i::text) AS hash_val,
    random() AS rand_val,
    repeat('x', 100) AS padding
FROM generate_series(1, 1000000) i;

-- Check how many local buffers are dirty
SELECT pg_dirty_local_buffers() AS dirty_buffers_before;

-- Warm up: Sequential scan (data in local buffers)
SELECT COUNT(*), AVG(rand_val) FROM temp_baseline;

\echo 'Baseline Scan (warm cache):'
-- Measure: Sequential scan with warm cache (3 runs for stability)
SELECT COUNT(*), AVG(rand_val) FROM temp_baseline;
SELECT COUNT(*), AVG(rand_val) FROM temp_baseline;
SELECT COUNT(*), AVG(rand_val) FROM temp_baseline;

-- Check dirty buffers after scans
SELECT pg_dirty_local_buffers() AS dirty_buffers_after_scan;

-- ============================================================================
-- SCENARIO 2: Measure Flush Cost - Write and Flush
-- ============================================================================
-- This measures the cost of flushing dirty local buffers to disk.

DROP TABLE IF EXISTS temp_flush_test;

-- Create temp table and ensure it's fully dirty
CREATE TEMP TABLE temp_flush_test AS
SELECT
    i AS id,
    md5(i::text) AS hash_val,
    random() AS rand_val,
    repeat('y', 100) AS padding
FROM generate_series(1, 1000000) i;

-- Update all rows to ensure all pages are dirty
UPDATE temp_flush_test SET rand_val = random();

-- Check how many buffers are dirty
SELECT pg_dirty_local_buffers() AS dirty_buffers_before_flush;

\echo 'Flushing all local buffers:'
-- Measure: Time to flush all dirty buffers
SELECT pg_flush_local_buffers() AS buffers_flushed;

-- Verify all buffers are clean
SELECT pg_dirty_local_buffers() AS dirty_buffers_after_flush;

-- ============================================================================
-- SCENARIO 3: Flush + Scan Pattern (simulating parallel query prep)
-- ============================================================================
-- This simulates the pattern that would occur when preparing a temp table
-- for parallel scanning: flush all buffers, then scan the data.

DROP TABLE IF EXISTS temp_parallel_sim;

CREATE TEMP TABLE temp_parallel_sim AS
SELECT
    i AS id,
    md5(i::text) AS hash_val,
    random() AS rand_val,
    repeat('z', 100) AS padding
FROM generate_series(1, 1000000) i;

-- Make it all dirty
UPDATE temp_parallel_sim SET rand_val = random();

\echo 'Flush + Sequential Scan (simulating parallel query pattern):'

-- Measure: Flush then immediately scan
SELECT pg_flush_local_buffers() AS flushed;
SELECT COUNT(*), AVG(rand_val) FROM temp_parallel_sim;

-- ============================================================================
-- SCENARIO 4: Varying Dirty Buffer Counts
-- ============================================================================
-- Measure flush cost with different amounts of dirty data to understand
-- if cost scales linearly with dirty buffer count.

DROP TABLE IF EXISTS temp_partial;

\echo 'Testing flush cost at different dirty buffer levels:'

-- Test with 25% dirty
CREATE TEMP TABLE temp_partial AS
SELECT i AS id, random() AS val, repeat('a', 100) AS pad
FROM generate_series(1, 250000) i;

SELECT pg_dirty_local_buffers() AS dirty_25pct;
SELECT pg_flush_local_buffers() AS flushed_25pct;

-- Test with 50% dirty
INSERT INTO temp_partial
SELECT i AS id, random() AS val, repeat('b', 100) AS pad
FROM generate_series(250001, 500000) i;

SELECT pg_dirty_local_buffers() AS dirty_50pct;
SELECT pg_flush_local_buffers() AS flushed_50pct;

-- Test with 75% dirty
INSERT INTO temp_partial
SELECT i AS id, random() AS val, repeat('c', 100) AS pad
FROM generate_series(500001, 750000) i;

SELECT pg_dirty_local_buffers() AS dirty_75pct;
SELECT pg_flush_local_buffers() AS flushed_75pct;

-- Test with 100% dirty
INSERT INTO temp_partial
SELECT i AS id, random() AS val, repeat('d', 100) AS pad
FROM generate_series(750001, 1000000) i;

SELECT pg_dirty_local_buffers() AS dirty_100pct;
SELECT pg_flush_local_buffers() AS flushed_100pct;

-- ============================================================================
-- SCENARIO 5: Cost Comparison - Flush vs Sequential Read vs Random Read
-- ============================================================================
-- This directly compares flush cost to sequential and random page reads
-- to help establish the cost multiplier.

DROP TABLE IF EXISTS temp_cost_compare;

CREATE TEMP TABLE temp_cost_compare AS
SELECT
    i AS id,
    md5(i::text) AS hash_val,
    random() AS rand_val,
    repeat('x', 100) AS padding
FROM generate_series(1, 1000000) i;

CREATE INDEX idx_temp_cost ON temp_cost_compare(id);

-- Make all dirty
UPDATE temp_cost_compare SET rand_val = random();

\echo 'Cost Comparison Tests:'

-- 1. Flush cost
\echo '1. Flush cost:'
SELECT pg_flush_local_buffers() AS flush_result;

-- 2. Sequential scan cost (reading from disk after flush)
\echo '2. Sequential scan (cold, reading from disk):'
SELECT COUNT(*) FROM temp_cost_compare;

-- 3. Random access cost
\echo '3. Random access pattern:'
SELECT COUNT(*) FROM temp_cost_compare WHERE id IN (
    SELECT (random() * 999999)::int FROM generate_series(1, 10000)
);

-- 4. Flush + Sequential scan combined
\echo '4. Combined flush + scan:'
UPDATE temp_cost_compare SET rand_val = random();  -- Make dirty again
SELECT pg_flush_local_buffers();
SELECT COUNT(*) FROM temp_cost_compare;

-- ============================================================================
-- SCENARIO 6: Real-world pattern - Aggregation with GROUP BY
-- ============================================================================
-- Test a realistic query pattern that would benefit from parallelization

DROP TABLE IF EXISTS temp_aggregate_test;

CREATE TEMP TABLE temp_aggregate_test AS
SELECT
    (random() * 1000)::int AS category,
    (random() * 100)::int AS subcategory,
    random() * 10000 AS value,
    md5(random()::text) AS data
FROM generate_series(1, 5000000) i;

\echo 'Aggregation test - serial vs parallel-eligible:'

-- Serial execution (no flush needed)
SET max_parallel_workers_per_gather = 0;
EXPLAIN (ANALYZE, BUFFERS)
SELECT category, subcategory,
       COUNT(*), SUM(value), AVG(value)
FROM temp_aggregate_test
GROUP BY category, subcategory;

-- Clean up
DROP TABLE IF EXISTS temp_baseline;
DROP TABLE IF EXISTS temp_flush_test;
DROP TABLE IF EXISTS temp_parallel_sim;
DROP TABLE IF EXISTS temp_partial;
DROP TABLE IF EXISTS temp_cost_compare;
DROP TABLE IF EXISTS temp_aggregate_test;

\echo 'Test complete. Analyze timing results to derive flush cost.'
