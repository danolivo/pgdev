# Parallel Scans of Temporary Tables: From Cost Model to Working Implementation

**Abstract (250 words):**

Temporary tables in PostgreSQL have been parallel-restricted since 2015, when Robert Haas noted: "We could possibly relax this if we wrote all of its local buffers at the start of the query... Writing a large number of temporary buffers could be expensive, though." This talk presents a complete working implementation that does exactly that—transforming an "impossible" restriction into a cost-based optimization decision.

The implementation rests on a fundamental reconceptualization: temporary table scans aren't parallel-*unsafe*, they're parallel-*expensive*. Four sequential commits build this capability. First, instrumentation tracks allocated and dirtied local buffers throughout query execution, providing cost estimation data. Second, the binary `parallel_safe` flag becomes a three-state enum (PARALLEL_UNSAFE, NEEDS_TEMP_FLUSH, PARALLEL_SAFE), enabling the planner to distinguish "impossible" from "expensive but possible." Third, Gather and GatherMerge nodes invoke `FlushAllLocalBuffers()` before launching workers, writing all dirty temporary pages to disk—workers access synchronized disk state exclusively, never touching the leader's local buffers, with paranoid assertions enforcing this separation. Fourth, modified path comparison functions add flush overhead (`write_page_cost × dirtied_localbufs`) when comparing paths, allowing intelligent cost-based decisions.

The design is intentionally conservative: all temporary buffers are flushed rather than tracking specific tables through stored procedures and complex subqueries—simpler, correct, and measurable. Small temporary tables with few dirty buffers parallelize efficiently; large buffer pools appropriately remain sequential. The implementation handles indexes, TOAST tables, and views, with comprehensive regression tests. An `extended_parallel_processing` GUC provides backward compatibility. Production deployments in Postgres Pro and Tantor validate the approach.

Code: github.com/danolivo/pgdev (branch: temp-bufers-stat-master)
