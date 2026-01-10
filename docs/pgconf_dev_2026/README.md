# pgconf.dev 2026 Talk Materials

**Talk Title:** Parallel Scans of Temporary Tables: From Cost Model to Working Implementation

**Speaker:** Andrei V. Lepikhov

**Implementation Branch:** temp-bufers-stat-master

---

## Files in This Directory

### 1. `pgconf_abstract_250words.md`
**Conference-ready abstract (250 words)**
- Suitable for submission to pgconf.dev 2026
- Accurately reflects the implementation in temp-bufers-stat-master branch
- Highlights the four-commit architecture and key innovations

### 2. `pgconf_abstract_revised.md`
**Extended version with full talk structure**
- Comprehensive 350-word abstract
- Complete 30-45 minute talk outline
- Key talking points for each section
- Audience takeaways

### 3. `draft_analysis_and_improvements.md`
**Detailed analysis of original draft**
- Identifies critical issues in the original draft
- Explains mismatches between draft and code
- Provides specific wording improvements
- Documents missing elements

### 4. `key_code_snippets_for_slides.md`
**20 presentation slides with code snippets**
- Ready-to-use code examples from the implementation
- Explanations and talking points for each snippet
- Before/after comparisons
- Real-world examples and regression tests

---

## Key Implementation Details

### Four Commits Architecture

1. **Statistics Tracking** (commit 125c83adb1)
   - `allocated_localbufs` counter
   - `dirtied_localbufs` counter
   - Real-time buffer state tracking

2. **Three-State Parallel Safety** (commit 3c2cfa4431)
   - `PARALLEL_UNSAFE` - Cannot parallelize
   - `NEEDS_TEMP_FLUSH` - Can parallelize after flushing
   - `PARALLEL_SAFE` - Fully parallel-safe

3. **Execution Mechanics** (commit 0f602a22d4)
   - `FlushAllLocalBuffers()` in Gather/GatherMerge
   - `EState.es_tempbufs_flushed` flag
   - Paranoid assertions preventing unflushed buffer access

4. **Cost Integration** (commit 7c2aefe31a)
   - `tempbuf_flush_extra_cost()` function
   - Modified `compare_path_costs()` and `compare_fractional_path_costs()`
   - GUC parameters: `extended_parallel_processing`, `write_page_cost`

### Design Philosophy

**Conservative approach:** Flush ALL temporary buffers rather than attempting granular tracking, because:
- "It seems difficult to calculate specific set of tables, indexes and toasts that may be touched inside the subtree"
- "Stored procedures may also scan temporary tables"
- Simplicity and correctness over optimization

**Paranoid correctness:** Multiple assertion layers ensure workers never access unflushed buffers

**Cost-based, not heuristic:** Optimizer weighs flush cost vs. parallel speedup

---

## Historical Context

### Robert Haas's 2015 Comment (Removed by This Patch)

```c
/*
 * Currently, parallel workers can't access the leader's temporary
 * tables. We could possibly relax this if we wrote all of its
 * local buffers at the start of the query and made no changes
 * thereafter (maybe we could allow hint bit changes), and if we
 * taught the workers to read them. Writing a large number of
 * temporary buffers could be expensive, though, and we don't have
 * the rest of the necessary infrastructure right now anyway. So
 * for now, bail out if we see a temporary table.
 */
```

**This implementation does exactly what Haas suggested**, transforming "too expensive" into a measurable cost-based decision.

---

## Production Validation

- ✅ Postgres Pro (Russian PostgreSQL distribution)
- ✅ Tantor (Chinese PostgreSQL fork)
- ✅ Comprehensive regression tests in `src/test/regress/sql/temp.sql`

---

## Talk Narrative Arc

1. **The Problem** - 10-year restriction since 2015
2. **The Insight** - Parallel-expensive, not parallel-unsafe
3. **The Solution** - Four-commit architecture
4. **The Results** - Production deployments and benchmarks
5. **The Future** - Selective flushing, parallel CREATE INDEX

---

**Repository:** github.com/danolivo/pgdev
**Branch:** temp-bufers-stat-master
**Session ID:** 0fc7O
**Date:** 2026-01-10
