# Local Buffer Flush Cost Analysis

## Executive Summary

This document describes the cost model for flushing local (temporary) buffers in PostgreSQL, which becomes necessary when enabling parallel query execution on temporary tables.

**Recommended Default:** `local_buffer_flush_cost = 2.0`

## Background

### Why Flush Local Buffers?

Local buffers are backend-private and not visible to parallel workers. To enable parallel scanning of temporary tables, all dirty buffers must be flushed to disk first, so workers can read the data via the storage layer.

### Cost Components

Flushing local buffers involves:

1. **Iteration overhead**: Scanning the local buffer pool (O(NLocBuffer))
2. **Write I/O**: Writing dirty pages to disk (sequential writes)
3. **fsync overhead**: Ensuring data durability
4. **Buffer state management**: Clearing dirty flags, updating statistics

## Cost Model

### Formula

```
total_flush_cost = num_dirty_buffers × local_buffer_flush_cost
```

Where:
- `num_dirty_buffers` = value of `dirtied_localbufs` counter
- `local_buffer_flush_cost` = cost per page (new GUC parameter)

### Comparison to Existing Cost Parameters

| Parameter | Default | Meaning |
|-----------|---------|---------|
| `seq_page_cost` | 1.0 | Cost of sequential page read (baseline) |
| `random_page_cost` | 4.0 | Cost of random page read |
| `cpu_tuple_cost` | 0.01 | Cost of processing one tuple |
| `parallel_setup_cost` | 1000.0 | Fixed cost to start parallel workers |
| `parallel_tuple_cost` | 0.1 | Per-tuple overhead in parallel mode |
| **`local_buffer_flush_cost`** | **2.0** | **Cost to flush one dirty page** |

### Why 2.0?

The value 2.0 × `seq_page_cost` represents:

1. **Higher than sequential read (1.0)** because:
   - Writes are slower than reads on most storage
   - fsync adds significant latency
   - Buffer pool iteration has CPU overhead

2. **Lower than random read (4.0)** because:
   - Flush writes are sequential, not random
   - No seek time penalty
   - Modern OSes can optimize sequential writes

3. **Much lower than parallel setup (1000.0)** because:
   - Flush cost scales with dirty buffers (typically < 500)
   - Only matters when dirty buffers exist

## When Does Flush Cost Matter?

### Scenario 1: Small Temp Table (< 100 dirty buffers)
```
flush_cost = 100 × 2.0 = 200.0
parallel_setup_cost = 1000.0

Decision: Flush cost is negligible, parallel query is beneficial
```

### Scenario 2: Medium Temp Table (~1,000 dirty buffers)
```
flush_cost = 1000 × 2.0 = 2000.0
parallel_setup_cost = 1000.0

Decision: Flush cost equals 2 parallel setups - marginal case
          Parallel beneficial only if query gains > 2x speedup
```

### Scenario 3: Large Temp Table (> 5,000 dirty buffers)
```
flush_cost = 5000 × 2.0 = 10000.0
parallel_setup_cost = 1000.0

Decision: Flush cost dominates - likely better to stay serial
          Unless parallel speedup is > 10x (rare)
```

## Storage-Specific Tuning

### SSD/NVMe
```sql
SET local_buffer_flush_cost = 1.5;
```
- Fast writes with low latency
- Minimal fsync overhead
- Parallel queries more attractive

### HDD
```sql
SET local_buffer_flush_cost = 2.5;
```
- Slower writes
- Higher fsync penalty
- Conservative estimate favors serial execution

### tmpfs/RAM disk
```sql
SET local_buffer_flush_cost = 0.5;
```
- No actual disk I/O
- Just memory operations
- Flush is very cheap

### Network Storage (NFS)
```sql
SET local_buffer_flush_cost = 4.0;
```
- Network latency
- Remote fsync overhead
- Flush is expensive

## Measurement Methodology

### Step 1: Run Benchmark
```bash
psql -f test_local_buffer_flush_cost.sql > results.txt
```

### Step 2: Extract Timings
From the output, record:
- `T_flush`: Time to flush N pages
- `T_seq_cold`: Time to sequentially scan N pages (cold cache)
- `T_seq_warm`: Time to sequentially scan N pages (warm cache)

### Step 3: Calculate Ratio
```
pages_flushed = N
time_per_flush = T_flush / N
time_per_seq_read = T_seq_cold / N

flush_cost_ratio = time_per_flush / time_per_seq_read
local_buffer_flush_cost = flush_cost_ratio × seq_page_cost
```

### Example Calculation
```
Scenario: Flushed 10,000 pages in 500ms
          Sequential scan of 10,000 pages took 300ms (cold)

time_per_flush = 500ms / 10000 = 0.05ms per page
time_per_seq_read = 300ms / 10000 = 0.03ms per page

flush_cost_ratio = 0.05 / 0.03 = 1.67

local_buffer_flush_cost = 1.67 × 1.0 = 1.67
```

Round to 1.5 or 2.0 for safety margin.

## Integration with Optimizer

### Location: `src/backend/optimizer/path/costsize.c`

```c
/*
 * cost_gather - cost a Gather or GatherMerge path
 *
 * If the path involves scanning a temporary relation that has dirty
 * local buffers, add the cost of flushing those buffers before
 * workers can access the data.
 */
void
cost_gather(GatherPath *path, PlannerInfo *root,
            RelOptInfo *rel, ParamPathInfo *param_info,
            double *rows)
{
    Cost        startup_cost = 0;
    Cost        run_cost = 0;

    /* ... existing cost calculations ... */

    /*
     * If we're scanning a temporary relation in parallel, we must
     * flush all dirty local buffers first so workers can read the data.
     */
    if (rel->needs_temp_flush && path->num_workers > 0)
    {
        int     num_dirty = estimate_temp_relation_dirty_buffers(rel);
        Cost    flush_cost;

        flush_cost = num_dirty * local_buffer_flush_cost;
        startup_cost += flush_cost;

        /* Track this for EXPLAIN output */
        path->flush_cost = flush_cost;
        path->buffers_to_flush = num_dirty;
    }

    /* ... rest of costing ... */
}
```

### Helper Function

```c
/*
 * estimate_temp_relation_dirty_buffers
 *
 * Estimate how many dirty local buffers a temp relation has.
 * This is used to cost the buffer flush operation.
 */
static int
estimate_temp_relation_dirty_buffers(RelOptInfo *rel)
{
    int     num_pages;
    double  dirty_ratio;

    /* Get relation size in pages */
    num_pages = rel->pages;

    /*
     * Assume most pages are dirty if the relation was recently
     * written. We could query dirtied_localbufs, but that's a
     * global counter, not per-relation.
     *
     * Conservative estimate: 75% of pages are dirty
     * This avoids over-optimistic parallel plans.
     */
    dirty_ratio = 0.75;

    return (int) (num_pages * dirty_ratio);
}
```

## GUC Parameter Definition

Add to `src/backend/utils/misc/guc_parameters.dat`:

```perl
{ name => 'local_buffer_flush_cost', type => 'real',
  context => 'PGC_USERSET', group => 'QUERY_TUNING_COST',
  short_desc => 'Planner cost of flushing a local buffer page.',
  long_desc => 'This represents the cost of writing a dirty local '
               '(temporary) buffer page to disk, including fsync overhead. '
               'Used when estimating the cost of parallel queries on temporary tables.',
  flags => 'GUC_EXPLAIN',
  variable => 'local_buffer_flush_cost',
  boot_val => '2.0',
  min => '0.0',
  max => 'DBL_MAX' },
```

Add declaration to `src/include/optimizer/cost.h`:

```c
extern PGDLLIMPORT double local_buffer_flush_cost;
```

## EXPLAIN Output Enhancement

When a flush occurs, EXPLAIN should show:

```
Gather  (cost=2150.00..15000.00 rows=100000 width=8)
  Workers Planned: 2
  Temp Buffers Flushed: 1000 (cost=2000.00)
  ->  Parallel Seq Scan on temp_table  (cost=0.00..5000.00 ...)
```

## Testing Checklist

- [ ] Run `test_local_buffer_flush_cost.sql` on target hardware
- [ ] Measure actual flush times
- [ ] Calculate cost ratio
- [ ] Set `local_buffer_flush_cost` appropriately
- [ ] Test with queries that have 0, 100, 1000, 10000 dirty buffers
- [ ] Verify optimizer makes correct serial vs parallel decisions
- [ ] Check EXPLAIN output shows flush cost
- [ ] Validate with `pg_stat_statements` timing data

## Future Improvements

1. **Per-relation dirty buffer tracking**: Instead of global `dirtied_localbufs`, track per temp table
2. **Adaptive costing**: Measure actual flush times and adjust cost dynamically
3. **Buffer flush caching**: Remember recent flush costs to improve estimates
4. **Partial flushing**: Only flush buffers for the specific temp table being scanned

## References

- `src/backend/storage/buffer/localbuf.c` - Local buffer management
- `src/backend/optimizer/path/costsize.c` - Cost estimation
- PostgreSQL cost parameters: https://www.postgresql.org/docs/current/runtime-config-query.html#RUNTIME-CONFIG-QUERY-CONSTANTS
