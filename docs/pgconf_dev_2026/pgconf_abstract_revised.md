# Parallel Scans of Temporary Tables: From Cost Model to Working Implementation

## Abstract (Extended Version - 350 words)

Temporary tables in PostgreSQL have been parallel-restricted since 2015, when Robert Haas noted: "We could possibly relax this if we wrote all of its local buffers at the start of the query... Writing a large number of temporary buffers could be expensive, though." This talk presents a complete working implementation that does exactly that—lifting this decade-old restriction through empirically-grounded cost modeling and surgical modifications to the planner and executor.

The implementation rests on a fundamental insight: temporary table scans can be parallel-safe if dirty local buffers are flushed to disk before launching workers. This transforms parallelism from impossible to a cost-based decision. The architecture comprises four commits, each building on the previous:

**Statistics Tracking:** Custom instrumentation tracks allocated and dirtied local buffers throughout query execution (`allocated_localbufs`, `dirtied_localbufs`). These counters increment when buffers are allocated or dirtied, and decrement when flushed, providing real-time visibility into the state of temporary storage.

**Reconceptualizing Parallel Safety:** The binary `parallel_safe` flag becomes a three-state enum: `PARALLEL_UNSAFE` (cannot parallelize), `NEEDS_TEMP_FLUSH` (can parallelize after flushing temporary buffers), and `PARALLEL_SAFE` (fully parallel-safe). This enables the optimizer to distinguish "impossible" from "expensive but possible." Each RelOptInfo gains a `needs_temp_safety` flag propagated through the query tree. The design is intentionally conservative: if any path touching a temp table exists, all paths are marked for flushing—avoiding the complexity of tracking specific tables through stored procedures and complex subqueries.

**Execution Mechanics:** Gather and GatherMerge nodes gain a `process_temp_tables` flag. Before launching workers, `FlushAllLocalBuffers()` writes all dirty temporary pages to disk. An `EState.es_tempbufs_flushed` flag prevents redundant flushes. Workers inherit `dirtied_localbufs = 0` via shared memory and access synchronized disk state exclusively—paranoid assertions enforce that workers never touch unflushed buffers. The old error "cannot access temporary tables during a parallel operation" is removed.

**Cost-Based Optimization:** The cost model adds flush overhead to path comparison: `write_page_cost × dirtied_localbufs`. Since "it seems difficult to calculate the specific set of tables, indexes and toasts that may be touched inside the subtree," the implementation flushes all temporary buffers—simple, correct, and measurable. Modified `compare_path_costs()` and `compare_fractional_path_costs()` functions temporarily add this cost when comparing paths marked `NEEDS_TEMP_FLUSH`, allowing the optimizer to make intelligent decisions: small temporary tables with few dirty buffers parallelize efficiently; large buffer pools appropriately remain sequential.

The implementation handles indexes, TOAST tables, and views, with comprehensive regression tests validating parallel index-only scans and subquery scenarios. An `extended_parallel_processing` GUC provides backward compatibility.

Code: github.com/danolivo/pgdev (branch: temp-bufers-stat-master)

---

## Suggested Talk Structure (30-45 minutes)

### 1. Introduction (5 min)
- The decade-old restriction
- Robert Haas's 2015 comment: "Writing a large number of temporary buffers could be expensive, though"
- Why this matters: temp tables are ubiquitous in ETL, analytics, and complex queries

### 2. The Core Insight (5 min)
- Temporary tables aren't inherently parallel-unsafe
- They're parallel-*expensive* (flush cost)
- This is a cost model problem, not an impossibility

### 3. Implementation Architecture (20 min)

**Commit 1: Instrumentation**
- Show code: `dirtied_localbufs++` in `MarkLocalBufferDirty()`
- Show code: `dirtied_localbufs--` in flush/invalidate paths
- Why track both allocated and dirtied? (Future work)

**Commit 2: Three-State Parallel Safety**
```c
typedef enum ParallelSafe {
    PARALLEL_UNSAFE = 0,
    NEEDS_TEMP_FLUSH,
    PARALLEL_SAFE,
} ParallelSafe;
```
- Show the old restrictive comment vs. new philosophy
- Why conservative (flush all buffers): "stored procedures may also scan temporary tables"
- Propagation through RelOptInfo.needs_temp_safety

**Commit 3: Execution**
```c
if (gather->process_temp_tables && !estate->es_tempbufs_flushed)
{
    FlushAllBuffers();
    estate->es_tempbufs_flushed = true;
}
```
- Show `FlushAllLocalBuffers()` implementation
- Show assertions in `RelationIdGetRelation()`: workers must never see unflushed buffers
- Show shared memory handoff: `fpes->dirtied_localbufs = dirtied_localbufs;`
- Workers read from disk, never from leader's local buffers

**Commit 4: Cost Integration**
```c
Cost tempbuf_flush_extra_cost() {
    return write_page_cost * dirtied_localbufs;
}
```
- Show how this integrates into `compare_path_costs()`
- Cost added temporarily for comparison only
- Paths keep original costs (important for EXPLAIN)

### 4. Design Trade-offs (5 min)
- Why flush *all* buffers, not just query-relevant ones?
  - Complexity vs. correctness
  - Stored procedures, dynamic SQL, correlated subqueries
  - "Difficult to calculate specific set of tables, indexes and toasts"
- Why `write_page_cost` (default 5.0) vs. your empirical 1.3×?
  - Conservative defaults
  - User can tune based on storage

### 5. Real-World Impact (5 min)
- Regression tests: parallel index-only scans on temp indexes
- Production usage: Postgres Pro, Tantor
- When it helps: small temp tables in large joins
- When it doesn't: queries with massive temp buffer pools

### 6. Future Work (3 min)
- Selective flushing (use `allocated_localbufs` metric)
- Parallel CREATE INDEX on temp tables (TODO comment in code)
- Better default cost calibration
- Hint bit handling during parallel scan

### 7. Q&A (remaining time)

---

## Key Code Snippets to Show

1. **The original restriction** (from git history):
```c
// 2015 comment by Robert Haas:
// "Currently, parallel workers can't access the leader's temporary
//  tables. We could possibly relax this if we wrote all of its
//  local buffers at the start of the query and made no changes
//  thereafter... Writing a large number of temporary buffers could
//  be expensive, though, and we don't have the rest of the necessary
//  infrastructure right now anyway."
```

2. **The new reality**:
```c
// 2025 implementation:
if (get_rel_persistence(rte->relid) == RELPERSISTENCE_TEMP)
{
    if (!extended_parallel_processing)
        return;  // Old behavior: bail out
    rel->needs_temp_safety = true;  // New: mark for flushing
}
```

3. **Paranoid safety checks**:
```c
// In RelationIdGetRelation():
Assert(!(rd != NULL &&
         RelationUsesLocalBuffers(rd) &&
         IsParallelWorker() &&
         dirtied_localbufs != 0));
```

4. **Cost integration**:
```c
if (path1->parallel_safe == NEEDS_TEMP_FLUSH)
{
    startup_cost1 += extra_cost;  // tempbuf_flush_extra_cost()
    total_cost1 += extra_cost;
}
```

---

## Key Talking Points

1. **This solves Robert Haas's exact design:** "write all of its local buffers at the start of the query"

2. **Cost-based, not heuristic-based:** The optimizer makes informed decisions, not blanket rules

3. **Paranoid correctness:** Multiple assertion layers ensure workers never accidentally access unflushed buffers

4. **Conservative but practical:** Flush everything rather than risk missing a table through a complex code path

5. **Production-ready:** Already deployed in commercial PostgreSQL distributions

6. **Tunable:** Users can adjust `write_page_cost` based on their storage characteristics

7. **Complete:** Handles indexes, TOAST, views, subqueries, CTEs

8. **Testable:** Regression tests validate parallel plans actually work

---

## Audience Takeaways

1. **For users:** You can now parallelize queries with temp tables by setting `extended_parallel_processing = on`

2. **For developers:** This is a model for lifting "impossible" restrictions through cost-based optimization

3. **For contributors:** Shows how to add instrumentation → redesign flags → implement execution → integrate costs

4. **For everyone:** Sometimes the "too expensive" restriction from 2015 is solvable in 2026 with better cost modeling
