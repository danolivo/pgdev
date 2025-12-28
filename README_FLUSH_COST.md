# Local Buffer Flush Cost Testing - Quick Start

## Overview

This directory contains a comprehensive test suite to measure and analyze the cost of flushing local (temporary) buffers in PostgreSQL. This is critical for determining when parallel query execution is beneficial for queries involving temporary tables.

## Files

| File | Purpose |
|------|---------|
| `test_local_buffer_flush_cost.sql` | SQL test scenarios to measure flush performance |
| `analyze_flush_cost.sql` | Analysis and cost derivation guide |
| `measure_flush_cost.py` | Automated measurement tool (Python) |
| `FLUSH_COST_ANALYSIS.md` | Complete documentation and cost model |

## Quick Start

### Method 1: SQL Tests (Manual)

```bash
# Run comprehensive test suite
psql -U postgres -d testdb -f test_local_buffer_flush_cost.sql > results.txt

# Review results and analyze
psql -U postgres -d testdb -f analyze_flush_cost.sql
```

### Method 2: Automated Measurement (Recommended)

```bash
# Install dependencies
pip3 install psycopg2-binary

# Run automated measurement
python3 measure_flush_cost.py --host localhost --database testdb --user postgres

# Example output:
# RECOMMENDED: local_buffer_flush_cost = 2.0
# ✓ Good storage (typical SSD)
# ✓ Parallel worth considering for large datasets
```

## Understanding the Results

The tool will recommend a value for `local_buffer_flush_cost` based on your storage:

- **1.0 - 1.5**: Very fast storage (NVMe, tmpfs)
  - Parallel queries on temp tables are very attractive

- **1.5 - 2.5**: Good storage (typical SSD)
  - **Default recommendation: 2.0**
  - Parallel beneficial for moderate to large temp tables

- **2.5 - 4.0**: Slower storage (HDD, network storage)
  - Parallel requires significant speedup to justify flush cost

- **4.0+**: Very slow storage
  - Parallel on temp tables rarely beneficial

## Applying the Setting

After measuring:

```sql
-- System-wide setting (requires superuser)
ALTER SYSTEM SET local_buffer_flush_cost = 2.0;
SELECT pg_reload_conf();

-- Session-level setting
SET local_buffer_flush_cost = 2.0;

-- Verify
SHOW local_buffer_flush_cost;
```

## What Gets Measured

The test suite measures:

1. **Sequential scan time** (warm cache) - baseline performance
2. **Sequential scan time** (cold cache) - disk read performance
3. **Flush time** - time to write dirty buffers to disk
4. **Varying dirty buffer counts** - cost scaling behavior

## Cost Model Summary

```
Total Flush Cost = num_dirty_buffers × local_buffer_flush_cost

Where:
- num_dirty_buffers = value from pg_dirty_local_buffers()
- local_buffer_flush_cost = 2.0 (default, tunable)
```

The optimizer compares:

```
Serial Cost  vs  (Parallel Setup Cost + Flush Cost + Parallel Execution Cost)
```

## Example Decision Making

### Small temp table (100 dirty buffers):
```
flush_cost = 100 × 2.0 = 200
parallel_setup_cost = 1000

Decision: Flush cost negligible, use parallel if query benefits
```

### Large temp table (5000 dirty buffers):
```
flush_cost = 5000 × 2.0 = 10,000
parallel_setup_cost = 1000

Decision: Flush cost dominates, need 10x speedup to justify parallel
```

## Testing Different Scenarios

### Scenario 1: SSD vs HDD

```bash
# On SSD
python3 measure_flush_cost.py --database testdb
# Expected: 1.5 - 2.0

# On HDD
python3 measure_flush_cost.py --database testdb
# Expected: 2.5 - 3.5
```

### Scenario 2: Different Table Sizes

```bash
# Small (50 MB)
python3 measure_flush_cost.py --size-mb 50

# Medium (100 MB) - default
python3 measure_flush_cost.py --size-mb 100

# Large (500 MB)
python3 measure_flush_cost.py --size-mb 500
```

### Scenario 3: tmpfs (RAM disk)

```bash
# If temp_tablespaces is on tmpfs
python3 measure_flush_cost.py --database testdb
# Expected: 0.5 - 1.0
```

## Troubleshooting

### Issue: "Permission denied"

```sql
-- Grant permissions for test functions
GRANT EXECUTE ON FUNCTION pg_flush_local_buffers() TO testuser;
GRANT EXECUTE ON FUNCTION pg_dirty_local_buffers() TO testuser;
```

### Issue: Python script fails

```bash
# Check dependencies
pip3 install psycopg2-binary

# Or use SQL tests instead
psql -f test_local_buffer_flush_cost.sql
```

### Issue: Inconsistent results

- Run tests when system is idle
- Increase number of runs in Python script
- Check for background processes affecting I/O
- Ensure temp_tablespaces has consistent performance

## Integration with Optimizer

After deriving the cost, the optimizer uses it like this:

```c
// In costsize.c
if (rel->needs_temp_flush && path->parallel_workers > 0) {
    int num_dirty = estimate_temp_relation_dirty_buffers(rel);
    Cost flush_cost = num_dirty * local_buffer_flush_cost;
    startup_cost += flush_cost;
}
```

The optimizer will then choose serial vs parallel based on total cost.

## Next Steps

1. Run measurement on your target hardware
2. Set `local_buffer_flush_cost` appropriately
3. Test with real workloads involving temp tables
4. Monitor with `EXPLAIN (ANALYZE, BUFFERS)`
5. Adjust if needed based on production experience

## References

- Full documentation: `FLUSH_COST_ANALYSIS.md`
- Test scenarios: `test_local_buffer_flush_cost.sql`
- Cost analysis: `analyze_flush_cost.sql`
- Automated tool: `measure_flush_cost.py`

## Contact

For questions or issues, consult the full documentation in `FLUSH_COST_ANALYSIS.md`.
