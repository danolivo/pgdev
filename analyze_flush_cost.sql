-- ============================================================================
-- Flush Cost Analysis and Parameter Derivation
-- ============================================================================
--
-- This script helps analyze the results from test_local_buffer_flush_cost.sql
-- and derive an appropriate cost parameter for local buffer flushing.
--
-- The cost model should account for:
-- 1. Writing dirty pages to disk (similar to random_page_cost but sequential)
-- 2. The overhead of iterating through the buffer pool
-- 3. The fsync overhead
--
-- Proposed cost parameter: local_buffer_flush_cost
-- ============================================================================

\echo '========================================='
\echo 'Current Cost Parameters (for reference):'
\echo '========================================='

SELECT
    name,
    setting,
    unit,
    CASE
        WHEN name = 'seq_page_cost' THEN '1.0 (baseline for sequential I/O)'
        WHEN name = 'random_page_cost' THEN '4.0 (4x more expensive than sequential)'
        WHEN name = 'cpu_tuple_cost' THEN '0.01 (very cheap)'
        WHEN name = 'parallel_setup_cost' THEN '1000.0 (expensive setup)'
        WHEN name = 'parallel_tuple_cost' THEN '0.1 (per-tuple overhead)'
    END AS explanation
FROM pg_settings
WHERE name IN ('seq_page_cost', 'random_page_cost', 'cpu_tuple_cost',
               'parallel_setup_cost', 'parallel_tuple_cost')
ORDER BY name;

\echo ''
\echo '========================================='
\echo 'Proposed Cost Model:'
\echo '========================================='

\echo ''
\echo 'local_buffer_flush_cost = Cost to flush one dirty local buffer page'
\echo ''
\echo 'Reasoning:'
\echo '  - Flushing involves writing a page to disk (like shared buffer write)'
\echo '  - But it is sequential I/O since we flush in buffer pool order'
\echo '  - Includes fsync overhead which is significant'
\echo '  - Temp files are typically on fast storage (temp_tablespaces)'
\echo ''
\echo 'Expected range: 1.5 to 3.0 × seq_page_cost'
\echo ''
\echo 'Why not just seq_page_cost?'
\echo '  - fsync overhead adds latency'
\echo '  - Write is more expensive than read on many storage systems'
\echo '  - Buffer pool iteration overhead'
\echo ''
\echo 'Why less than random_page_cost?'
\echo '  - Flushing is sequential, not random I/O'
\echo '  - No seek time penalty'
\echo ''
\echo 'Recommended default: 2.0 × seq_page_cost'
\echo '  - Conservative estimate'
\echo '  - Accounts for write + fsync overhead'
\echo '  - Can be tuned based on storage characteristics'
\echo ''

\echo '========================================='
\echo 'Cost Calculation Formula:'
\echo '========================================='

\echo ''
\echo 'For a query involving parallel scan of temp table:'
\echo ''
\echo '  total_flush_cost = num_dirty_buffers × local_buffer_flush_cost'
\echo ''
\echo 'Where:'
\echo '  num_dirty_buffers = current value of dirtied_localbufs'
\echo '  local_buffer_flush_cost = 2.0 (recommended default)'
\echo ''
\echo 'Example:'
\echo '  If temp table has 1000 dirty buffers:'
\echo '  flush_cost = 1000 × 2.0 = 2000.0'
\echo ''
\echo '  This is equivalent to:'
\echo '  - 2000 sequential page reads, OR'
\echo '  - 500 random page reads, OR'
\echo '  - 2 parallel worker setups'
\echo ''
\echo 'The optimizer should compare:'
\echo '  serial_cost vs (parallel_setup_cost + flush_cost + parallel_execution_cost)'
\echo ''

\echo '========================================='
\echo 'Implementation in Cost Estimator:'
\echo '========================================='

\echo ''
\echo 'In src/backend/optimizer/path/costsize.c:'
\echo ''
\echo '  /* Cost of flushing local buffers before parallel scan */'
\echo '  if (rel->needs_temp_flush && path->parallel_workers > 0)'
\echo '  {'
\echo '      Cost flush_cost;'
\echo '      int num_dirty_buffers = get_temp_relation_dirty_buffers(rel);'
\echo '      '
\echo '      flush_cost = num_dirty_buffers * local_buffer_flush_cost;'
\echo '      startup_cost += flush_cost;'
\echo '  }'
\echo ''

\echo '========================================='
\echo 'Storage-Specific Tuning Guidelines:'
\echo '========================================='

\echo ''
\echo 'SSD/NVMe (fast random I/O):'
\echo '  local_buffer_flush_cost = 1.5'
\echo '  Reasoning: Fast writes, low fsync overhead'
\echo ''
\echo 'HDD (slow random I/O):'
\echo '  local_buffer_flush_cost = 2.5 to 3.0'
\echo '  Reasoning: Higher fsync overhead, write amplification'
\echo ''
\echo 'RAM disk / tmpfs:'
\echo '  local_buffer_flush_cost = 0.5 to 1.0'
\echo '  Reasoning: No actual disk I/O, just memory copy'
\echo ''
\echo 'Network storage (NFS, etc):'
\echo '  local_buffer_flush_cost = 3.0 to 5.0'
\echo '  Reasoning: Network latency, remote fsync overhead'
\echo ''

\echo '========================================='
\echo 'Measurement Methodology:'
\echo '========================================='

\echo ''
\echo 'To measure on your system:'
\echo ''
\echo '1. Run test_local_buffer_flush_cost.sql'
\echo '2. Record timing for:'
\echo '   - Sequential scan (warm cache)'
\echo '   - Sequential scan (cold cache, after flush)'
\echo '   - Flush operation itself'
\echo '3. Calculate:'
\echo '   flush_cost_ratio = (flush_time / num_pages) / (seq_scan_time / num_pages)'
\echo '4. Multiply by seq_page_cost to get local_buffer_flush_cost'
\echo ''
\echo 'Example calculation:'
\echo '  If flushing 10,000 pages takes 500ms'
\echo '  And sequential read of 10,000 pages takes 300ms'
\echo '  Then flush_cost_ratio = (500/10000) / (300/10000) = 1.67'
\echo '  So: local_buffer_flush_cost = 1.67 × seq_page_cost = 1.67'
\echo ''

\echo '========================================='
\echo 'GUC Parameter Definition:'
\echo '========================================='

\echo ''
\echo 'Add to src/backend/utils/misc/guc_parameters.dat:'
\echo ''
\echo '{ name => '\''local_buffer_flush_cost'\'', type => '\''real'\'','
\echo '  context => '\''PGC_USERSET'\'', group => '\''QUERY_TUNING_COST'\'','
\echo '  short_desc => '\''Planner cost of flushing a local buffer page.'\'','
\echo '  long_desc => '\''This represents the cost of writing a dirty local '\''
\echo '               '\''(temporary) buffer page to disk, including fsync overhead.'\'','
\echo '  flags => '\''GUC_EXPLAIN'\'','
\echo '  variable => '\''local_buffer_flush_cost'\'','
\echo '  boot_val => '\''2.0'\'','
\echo '  min => '\''0.0'\'','
\echo '  max => '\''DBL_MAX'\'' },'
\echo ''

\echo 'Test complete.'
