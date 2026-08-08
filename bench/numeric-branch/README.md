# Benchmark: the `numeric-int128-agg-fastpath` branch, commit by commit

Five-point evaluation of the whole branch against upstream `fd2b898`.

| point | commit | what it does |
|-------|--------|--------------|
| `v0`  | `fd2b898` | upstream base |
| `v0b` | `fd2b898` | **independent rebuild of the base** — the A/A noise floor |
| `v1`  | `9035685` | int128 fast sum in `sum(numeric)` / `avg(numeric)` |
| `v2`  | `693023f` | teach serialization / combine about the fast sum |
| `v3`  | `c3b903a` | speed up numeric hashing and comparison |

`2c2905f` (overflow-checked int128 primitives) adds only inline functions in
`int128.h` plus a test module and has no callers of its own, so it cannot move
a runtime number; it is covered under correctness instead of being measured.

## Verdict

* **`c3b903a` is the whole win.**  1.4x–2.6x on grouping, hash joins and
  numeric comparisons, no overhead case found.  It does not depend on the
  three commits below it and would stand alone as a patch.
* **The int128 fast-sum pair is a net negative on this host.**  No gain that
  this rig can resolve, against reproducible regressions of **+15% to +24%**
  on values with 5–9 stored digits and **+6% to +11%** on parallel
  aggregation.
* Correctness of the whole series is solid: every result is bit-identical to
  base, including display scale, specials, and the parallel paths.

## Method

Two rigs.  Both build every point with identical flags (`-O2`, no assertions)
and serve **one** data directory by starting each build against it in turn, so
data layout, file placement and page cache are constant and the executable is
the only variable.  ASLR is disabled for the postmaster so restarts do not
re-roll memory layout.  Build order is rotated and then mirrored across
sessions so linear drift cancels.  All five catalog versions are identical,
which is what makes the shared data directory legal.

1. **Broad rig** (`driver.py`, 32 queries, 10 sessions x 4 reps) — covers the
   whole matrix: group cardinality from 1 to 5M, 80/20 skew, key width, the
   parallel scaling curve, and the hash/comparison cases.
2. **Focused rig** (`driver_focus.py`, 17 queries, 10 sessions x 6 reps) —
   built after the broad rig showed the aggregate signal was diluted.  Three
   changes, each aimed at resolving a few percent:
   * **narrow, single-column tables**, so the transition function dominates
     instead of tuple deforming (`count(*)` costs 189ms of the 551ms that
     `sum(numeric)` costs on the same table — on the broad rig's 15-column
     table the aggregate was a much smaller share);
   * **repetitions run back-to-back inside one session**, so every sample of a
     query sees the same cache state;
   * **minima, not medians** — the noise here is one-sided interference
     spikes (a single 2495ms sample among ~730ms ones), which a minimum
     rejects and a median does not.

### Controls, and why the raw numbers need normalizing

Three controls run inside the benchmark itself:

| control | why it must not move | measured |
|---------|----------------------|----------|
| `count(*)` over the same table | touches no numeric code at all | **v1 +3.7%** |
| `stddev_pop` | variance states are created promoted, so the fast path is excluded by construction | v1 +7.6% raw |
| `GROUP BY int8` | no numeric hashing | flat |

`count(*)` moving by +3.7% on `v1` is the important one: it is a per-build
baseline offset (build layout plus session drift), not an effect.  The tables
below therefore report **control-normalized** deltas — each build's raw delta
minus that build's `count(*)` delta.  After normalizing, `stddev_pop` still
sits at ~+4% on v1/v2/v3, which sets the honest resolution of this rig for the
short scan-bound queries: **about +-5%**, tightening to +-1..4% on the longer
ones.  A positive control (`GROUP BY numeric`, known to move) lands at -40%,
confirming the rig does see real effects.

The A/A column (`v0b`, an independent *rebuild* of the base) has a median
absolute deviation of 1.2% and a maximum of 4.5%, so it captures code-layout
luck as well as drift.

## Results — focused rig, control-normalized, minima of 12 samples per build

| id | case | v0 ms | v0b | v1 | v2 | v3 |
|----|------|------:|----:|----:|----:|----:|
| F1 | `sum(numeric(10,2))`, 1 group — int64 lane, **best case** | 551 | −1.6 | **+0.2** | +3.1 | +5.3 |
| F2 | `avg(numeric(10,2))`, 1 group | 539 | +1.0 | **−0.3** | +1.7 | +7.1 |
| F3 | 7 x `sum`, 1 group | 2497 | −3.6 | −2.0 | −3.3 | +0.1 |
| F4 | `sum`, 1k groups, int8 key | 727 | −3.0 | −8.1 | −1.9 | +3.1 |
| F5 | `sum`, 592k groups, numeric key | 1880 | −1.3 | −5.6 | −4.0 | **−17.4** |
| F6 | `sum`, 5M groups (1 row each) | 3529 | −1.7 | −5.2 | −2.8 | −5.5 |
| F7 | `sum(25-digit)` — no int64 lane, **never promotes** | 525 | −0.2 | **+14.8** | +16.5 | +18.3 |
| F8 | `sum(numeric(32,2))` — promotes mid-scan at 1.7M rows | 564 | −1.5 | **+19.1** | +19.3 | +23.6 |
| F9 | `sum(45-digit)` — rejected by width cap, promotes at row 1 | 564 | −0.9 | **−0.4** | −0.4 | +3.8 |
| F10 | `sum`, alternating display scale | 551 | −1.2 | −1.3 | +3.2 | +6.9 |
| F11 | `stddev_pop` — control, fast path excluded | 708 | −0.1 | +3.9 | +4.5 | +4.9 |
| F13 | moving `sum`, 100-row frame (inverse transition) | 265 | −4.8 | −4.6 | −1.8 | −1.8 |
| F15 | PARALLEL 7 x `sum`, 592k groups, 2 workers | 1184 | +3.0 | +2.2 | +3.9 | −7.1 |
| F16 | PARALLEL 7 x `sum`, 592k groups, 3 workers | 1248 | +4.0 | **+10.9** | +8.2 | −7.6 |
| F17 | PARALLEL `sum(45-digit)`, 592k groups — promoted states | 1216 | +0.7 | +6.1 | +4.1 | −12.5 |
| F18 | `GROUP BY numeric`, 1k groups — positive control | 1503 | −0.2 | −3.8 | −2.6 | **−40.2** |

Broad rig, raw minima (32 queries, full matrix) are in `results-broad.txt`;
its A/A floor is looser (±5–12% on medians, ±1–7% on minima), but every sign
and magnitude above reproduces there independently.

## Reading of the results

### The regression, and why it happens

`numericvar_to_int128_scaled()` has three outcomes per input value:

| stored digits | path taken | cost |
|---|---|---|
| ≤ 4 and coefficient fits int64 | **int64 lane** — no 128-bit ops | the profitable case |
| 5–9, and `(weight+1)*4 + dscale ≤ 39` | checked 128-bit mul+add **per digit** | expensive |
| `(weight+1)*4 + dscale > 39` | rejected by the cheap width cap | one wasted test per group |

The middle band is the problem.  Values there are wide enough to skip the
int64 lane but narrow enough to keep *succeeding*, so the expensive path runs
on every row and the state never promotes out of it.  This is the band the
commit message itself identifies — "per-digit checked 128-bit multiplications
cost more than the digit-array accumulator they replace" — but the mitigation
only helps values that *fail*.  `numeric(32,2)`, an ordinary money column,
sits squarely in it.

The data confirms the mechanism directly.  F9 (45 digits, rejected by the
width cap on the first row) is **−0.4%**: immediate rejection is free.  F7
(25 digits, passes the cap and keeps succeeding) is **+14.8%**.  Same width
class, opposite outcome, and the only difference is whether the per-digit loop
runs on every row.

That points at a cheap fix: gate the fast path on the int64 lane alone —
reject `ndigits > 4` before the per-digit loop.  Those columns would then
promote on their first row and behave like F9, and the regression disappears
along with the middle band.

### The gain that is not there

F1 and F2 are the best case the design can have: `numeric(10,2)` in the int64
lane, one group, a single-column table so the transition function is the
dominant cost.  They come out at **+0.2%** and **−0.3%**.  With the rig's
±5% resolution on these queries, a gain above ~5% is excluded; the point
estimate is zero.

The high-group-count shapes (F4 −8.1%, F5 −5.6%, F6 −5.2%) do lean negative,
which is where the design *should* pay — the commit's premise is three
allocations per group per aggregate, not per row.  But `stddev_pop`, which
cannot use the fast path at all, sits at +3.9% in the same run, so a 4–5%
move is at this rig's resolution limit.  Suggestive, not resolved.

### Parallel aggregation

`v1` makes parallel aggregation **worse** — +10.9% at 3 workers, +6.1% on
promoted states — which its own commit message anticipates: folding a
fast-mode state into the digit-array form on every serialize "surrenders most
of the fast path's benefit exactly where it is largest".  `v2` is the fix for
that and does move in the right direction (+10.9 → +8.2 at 3 workers, +6.1 →
+4.1 on promoted states), but it recovers only ~2–3 points, not the "cuts the
partial-aggregate phase by half" of its commit message, and the pair still
ends up slower than base.  On four cores the worker curve only reaches 3, so
the "removes the previous anti-scaling" claim cannot be properly tested here.

One structural finding: at **1 row per group the planner declines partial
aggregation entirely** (it correctly costs partial aggregation as pure
overhead), so `693023f` cannot reach that shape at all.

### `c3b903a`

Reproduces the earlier single-commit measurement: −40% on 1k-group numeric
grouping, −17% on 592k groups, −7 to −13% on the parallel cases (via the
hash aggregate under the Gather, not via serialization), and nothing anywhere
that costs.  See `../numeric-hash-cmp/` for the dedicated run, which adds the
20-column (2.56x), hash-join, toasted and 200-digit cases.

## Correctness

* `make check` on the branch tip: **245/245 pass**.
* `test_int128`: **50M** randomized cross-checks of the *portable two-limb*
  implementation against native `__int128` (the module compiles the portable
  path as the code under test by default), plus **20M** with
  `-DUSE_NATIVE_INT128=1` to exercise the `__builtin_*_overflow` branch.
  Both clean.
* `correctness.sql` reduces 14 properties to md5 digests and compares them
  across **all five builds**.  Every digest identical:

  | property | corpus |
  |---|---|
  | `sum` / `avg`, grouped and total | scales 0–8, both signs, values straddling the 4-vs-5 stored-digit boundary, the 5–9 band, values rejected by the width cap |
  | int128 boundaries | `2^127−1`, `−2^127`, pairs summing to exactly one past the limit, `10^38−0.1` plus `0.1` |
  | variance family | `var_pop` / `var_samp` / `stddev_samp` |
  | moving `sum` / `avg` | 3-preceding and 50-preceding/10-following frames — the inverse transition |
  | specials and nulls | `NaN`, `±Infinity`, mixed, all-null groups, zeros of assorted scales |
  | parallel | the same aggregates under Finalize/Partial HashAggregate — the serialize → deserialize → combine path |

  The parallel digests also equal the serial ones within each build, so
  serialization round-trips exactly.

## Two setup bugs worth recording

Both would have produced quietly meaningless numbers:

1. `parallel_tuple_cost = 0` makes the planner gather raw rows instead of
   partial-aggregating, so **the serialization commit's code path never ran**.
   At `0.005` the plans are Finalize HashAggregate → Gather → Partial
   HashAggregate.  Every parallel query's plan is asserted, not assumed.
2. The first attempt at a "promoted states in parallel" probe used
   `numeric(32,2)` at 5 rows per group, whose partial sums never overflow, so
   nothing promoted.  Replaced with a 45-digit column that is rejected by the
   width cap and therefore promotes on its first row.

Dataset properties are verified rather than assumed (`verify.sql`): a
short-header numeric occupies `3 + 2*ndigits` bytes, so `pg_column_size`
gives the stored digit count exactly, which pins down which lane each column
takes.  The overflow column was confirmed to promote at row **1 701 412** of
5M — matching the "1.7 million rows" in `9035685`'s commit message — and the
80/20 key to put exactly 80.0% of rows in 200 of 1000 groups.

## Environment and its limits

4 vCPU / 15 GB KVM guest, Intel Xeon @ 2.80GHz, SMT already off (1 thread per
core), Linux 6.18, gcc 13.3.  No PMU, so no `perf` profiling.

The honest caveats on the negative results: this code leans on 128-bit
multiplications and divisions by constants, whose relative cost varies
considerably between microarchitectures, and four cores allow a worker curve
of only 1–3.  The regressions are 3–10x the noise floor and reproduce on two
independent rigs, so they are not artifacts; but the *absence* of a gain is
bounded at ~5%, and the commit messages' own figures were taken on other
hardware.  Reproducing them there is the obvious next step.

## Files

| file | purpose |
|------|---------|
| `bench2-build.sh` | build all five points with identical flags |
| `setup.sh` | initdb the single shared data directory |
| `data.sql`, `verify.sql` | datasets, and assertions about which fast-path lane each column takes |
| `queries.py` / `driver.py` | broad rig: 32 queries, full matrix |
| `queries_focus.py` / `driver_focus.py` | focused rig: narrow tables, back-to-back reps, minima |
| `checks.py` | `correctness` mode (cross-build digests) and `plans` mode (plan equality, spill detection) |
| `correctness.sql` | the 14-property equivalence corpus |
| `results-broad.txt`, `results-focused.txt`, `samples-*.json` | raw output and every individual sample |
