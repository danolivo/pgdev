# Benchmark: "Speed up numeric hashing and comparison"

Evaluation of commit `c3b903a` (top of `numeric-int128-agg-fastpath`) against its
parent `693023f`.  The commit removes the per-call `DatumGetNumeric()` copy from
the numeric hash and comparison support functions, unpacking into a caller-stack
buffer instead, and adds a `memcmp()` fast path to `numeric_eq()`.

The two commits differ only in `src/backend/utils/adt/numeric.c` (+187/-34) and
have the same catalog version, so the same data directory can be served by
either binary.

## Method

* Both commits configured identically: `-O2 -fno-omit-frame-pointer`, no
  assertions, `--without-icu --without-readline --without-zlib`.
* Two clusters, one per build, on the same host, identical `postgresql.conf`
  (`shared_buffers=1GB`, `work_mem=512MB`, `hash_mem_multiplier=8`,
  `max_parallel_workers_per_gather=0`, `jit=off`, `autovacuum=off`).
* Datasets generated from the same script in both clusters and verified
  byte-identical by md5 (`verify2.sql`).
* Plans compared query-by-query across builds (`plans.py`): identical
  everywhere, and no hash aggregate spilled to disk.
* 2 untimed warm-up passes, then 7 timed rounds; within a round the build that
  runs first alternates so that any drift is shared.  Reported figure is the
  median of the 7; minima are reported too and agree.
* Timing is psql `\timing`, i.e. server-side statement time only.

### Noise control

Two independent controls, because the interesting "no overhead" claims are
about deltas of a few percent:

1. **A/A control** (`results-aa-control.txt`) — a second cluster built from the
   *same* base commit, benchmarked against the first with the identical
   procedure.  Deltas range from -3.6% to +4.8%, so the instance-level noise
   floor of the A/B setup is about +-5%.
2. **Same-data-directory A/B** (`results-samedatadir.txt`) — both binaries
   started in turn against *one* data directory, 6 alternating server sessions
   x 5 repetitions.  This removes data layout, file placement and page cache
   as variables; only the executable differs.  The int8 control lands at
   -0.7%, confirming the floor is near zero in this design.

## Results (A/B, medians in ms, 5M-row tables unless noted)

| # | case | base | patched | delta | A/A noise |
|---|------|-----:|--------:|------:|----------:|
| A1 | GROUP BY numeric, **1 group** | 878.6 | 497.7 | **-43.3%** (1.77x) | -0.6% |
| A2 | 1 000 groups | 1670.9 | 825.8 | **-50.6%** (2.02x) | +2.2% |
| A3 | 1 000 groups, **80/20 skew** | 1635.7 | 805.2 | **-50.8%** (2.02x) | -0.3% |
| A4 | 100 000 groups | 1134.4 | 790.7 | **-30.3%** (1.46x) | -3.6% |
| A5 | **5M groups, all unique** | 3562.2 | 3443.8 | -3.3% | +1.1% |
| B1 | `sum(v)`, 1 group | 854.4 | 510.1 | **-40.3%** | +0.6% |
| B2 | `sum(v)`, 1 000 groups | 1656.6 | 829.4 | **-49.9%** | +1.1% |
| B3 | `sum(v)`, 80/20 skew | 1619.5 | 818.6 | **-49.4%** | -0.8% |
| B4 | `sum(v)`, 5M unique | 3556.0 | 3498.6 | -1.6% | +3.0% |
| C1 | GROUP BY **20 numeric cols**, 1M rows | 4189.4 | 1628.5 | **-61.1%** (2.56x) | -1.0% |
| C2 | mixed display scale (memcmp always misses) | 986.0 | 682.2 | **-30.8%** | -1.4% |
| C3 | 60-digit numeric (fits local buffer) | 1830.5 | 1011.7 | **-44.7%** | +0.8% |
| C4 | 200-digit numeric (**overflows buffer, palloc fallback**) | 2214.4 | 1381.0 | **-37.6%** | +0.6% |
| C5 | toasted / compressed numeric | 1160.1 | 1143.4 | -1.4% | +0.3% |
| C6 | int8 control, no numeric involved | 585.9 | 573.2 | -2.2% | +4.8% |
| D1 | hash join on numeric, 5M x 1k | 1664.6 | 964.7 | **-42.0%** (1.72x) | +1.0% |
| D2 | `SELECT DISTINCT`, 5M unique | 3571.4 | 3441.3 | -3.6% | +1.2% |
| D3 | Sort + GroupAggregate, 1 000 groups | 3172.3 | 2347.9 | **-26.0%** | -0.6% |
| D4 | `ORDER BY` numeric, 5M rows | 721.3 | 702.4 | -2.6% | +2.8% |
| D5 | `WHERE g = const` (`numeric_eq`) | 586.4 | 487.4 | **-16.9%** | +2.3% |
| D6 | `WHERE g > 0` (`numeric_gt`) | 643.6 | 515.3 | **-19.9%** | +3.0% |
| D7 | `hash_numeric()` over 5M rows | 593.2 | 488.8 | **-17.6%** | +4.4% |

Same-data-directory confirmation of the small deltas:

| case | base | patched | delta |
|------|-----:|--------:|------:|
| 1k groups (positive control) | 1731.7 | 874.8 | -49.5% |
| 5M unique groups | 3726.3 | 3589.6 | -3.7% |
| `sum(v)`, 5M unique | 3729.7 | 3616.9 | -3.0% |
| 200-digit numeric | 2186.8 | 1348.0 | -38.4% |
| toasted numeric | 1158.6 | 1164.5 | +0.5% |
| int8 control | 642.0 | 637.3 | -0.7% |
| `DISTINCT`, 5M unique | 3772.0 | 3646.0 | -3.3% |
| `ORDER BY` numeric | 750.4 | 752.4 | +0.3% |
| `WHERE g = const` | 624.8 | 532.8 | -14.7% |

## Reading of the results

**Where the win comes from.**  Subtracting the int8 control (C6, same table,
same row count, same plan shape, only the grouping column type differs) from
the single-column numeric grouping (A2) isolates the numeric-specific part of
hash aggregation: 217 ns/row before, 48 ns/row after -- a 4.5x reduction in the
part of the work the patch touches.  The end-to-end effect on a query is then
set by how much of it is numeric key handling.

**Group cardinality is what governs the payoff, not skew.**  From 1 group up to
100k groups the gain is 30-51%.  At 5M groups (every row its own group) it
collapses to ~3%: that shape is dominated by hash table memory traffic, and
each row does one hash and essentially no successful equality comparison, so
there is little for the patch to remove.  A 80/20 skew over 1000 groups behaves
exactly like the uniform 1000-group case (-50.8% vs -50.6%) -- skew changes
cache locality, not the number of hash and equality calls.

**Widest useful effect is multi-column grouping** (C1, 2.56x), because the cost
the patch removes is paid once per key column per row.

**No overhead case was found.**  Every path designed to defeat the optimisation
still comes out neutral or ahead:

* toasted/compressed values, which take the `DatumGetNumeric()` fallback in
  full: +0.5% same-data-directory, i.e. no measurable cost for the added
  branch;
* 200-digit values, which overflow `NUMERIC_LOCAL_NDIGITS` and also fall back:
  still -38%, because during grouping the equality calls are on equal values
  and the `memcmp()` path skips both unpacks entirely;
* a grouping key deliberately built so numerically-equal values differ
  bytewise at equal length (same digits, different display scale), which makes
  the `memcmp()` fast path run and miss on every comparison: still -31%;
* `ORDER BY` (+0.3%) and the int8 control (-0.7%) are flat, as expected.

`ORDER BY` gaining nothing is not a gap in the patch: `numeric_abbrev_convert()`
already carried the same stack-buffer trick upstream (`numeric.c`, "This is to
handle packed datums without needing a palloc/pfree cycle"), so sorting never
paid the copy.  The patch generalises an idiom that already existed in the same
file to the hash and comparison entry points.  The 26% on D3 comes from
`numeric_eq()` in the group boundary check, not from the sort itself.

## Correctness

* `make check` on the patched build: all 245 tests pass.
* `correctness.sql` compares md5 digests of nine properties across the two
  builds over a corpus that straddles the new `NUMERIC_LOCAL_NDIGITS` boundary
  (values of 1..300 digits), plus `NaN`, `+/-Infinity`, signed zeros, mixed
  display scales and 4000/9900-digit toasted values.  All nine digests are
  identical:

  | digest | covers |
  |--------|--------|
  | `hash32`, `hash64`, `hash64seed` | hash values unchanged -- the property that matters for hash-partitioned tables and on-disk hash / `jsonb_path_ops` entries |
  | `cmp-matrix` | 120x120 pairs x `<`, `<=`, `=`, `<>`, `>=`, `>`, `numeric_cmp` |
  | `cmp-toasted` | comparisons with compressed / out-of-line operands |
  | `grouping`, `distinct-hash` | equal values with different display scales group together |
  | `sortorder` | ordering |
  | `hashpart` | `satisfies_hash_partition` routing over 8 partitions |

## Files

| file | purpose |
|------|---------|
| `setup_cluster.sh` | initdb + config + start one cluster |
| `data.sql` | generate the datasets |
| `verify2.sql` | assert the datasets have the intended shape and are identical across clusters |
| `queries.py` | the 22 benchmark queries |
| `driver.py` | interleaved A/B driver |
| `samedir.py` | same-data-directory A/B driver |
| `plans.py` | compare plans across builds, detect hash spills |
| `correctness_pre.sql`, `correctness.sql` | cross-build equivalence digests |
| `results-ab.txt`, `results-aa-control.txt`, `results-samedatadir.txt` | raw output |

## Environment

4 vCPU / 15 GB VM, Linux 6.18, gcc 13.3, Ubuntu 24.04.  Absolute numbers are
specific to this host; the A/A control and the same-data-directory design are
what make the deltas trustworthy rather than the absolute timings.
