# Benchmark plan: `numeric-int128-agg-fastpath`, production build settings

Goal: state the **best, worst and average** performance change the branch
produces, separately per commit, with a measured noise floor and with build
settings a production deployment would actually use.

## 1. Why re-run with different build flags

The earlier runs used `-O2 -fno-omit-frame-pointer`.  That is not what a
distribution ships, and two of the differences bear directly on this patch:

* **`-fstack-protector-strong`** gives a stack canary to any function
  containing a local array.  `numeric_unpack_local()` places a 70-byte
  `char` array on the stack of every function it inlines into —
  `numeric_eq()`, `hash_numeric()`, `numeric_cmp()`, and the four
  inequality operators.  So this flag taxes precisely the functions
  `c3b903a` accelerates, and a build without it may overstate that commit's
  gain.  This is the single most important reason to redo the measurement.
* **`-D_FORTIFY_SOURCE`** instruments `memcpy()`, which the same patch adds
  one of per unpack.
* **`--with-llvm`** is enabled in every distro package, and `jit` defaults to
  `on` with `jit_above_cost = 100000`.  Most queries here exceed that, and
  `jit_inline_above_cost` lets LLVM inline strict operator functions from the
  installed bitcode — including `numeric_eq()`.  If that happens, the
  hash/comparison patch's benefit could look quite different in a real
  deployment than in a `jit=off` microbenchmark.

Flags used:

```
CFLAGS   = -O2 -g -pipe -Wall -Werror=format-security -fstack-protector-strong
           -fstack-clash-protection -fcf-protection -fasynchronous-unwind-tables
           -fno-omit-frame-pointer -mtune=generic
CPPFLAGS = -D_FORTIFY_SOURCE=2
configure: --with-llvm  (LLVM 18)   no --enable-cassert
```

`-march`/`-mtune=native` is deliberately **not** used: distro packages target
generic x86-64, and tuning for the benchmark host would not generalise.
`--without-icu/readline/zlib` is kept for build time; none of the three is
reachable from a numeric aggregate or comparison.

## 2. Measurement points

| point | commit | role |
|-------|--------|------|
| `p0`  | `fd2b898` | upstream base |
| `p0b` | `fd2b898` | **independent rebuild of the same commit** — the A/A noise floor, so it captures code-layout luck, not only drift |
| `p3`  | `c3b903a` | the branch exactly as submitted |
| `p4`  | `numeric-int128-agg-fastpath-v2` | restructured series with the eligibility gate narrowed to the int64 lane |

`p3` vs `p0` answers "what does the branch do today"; `p4` vs `p3` isolates the
narrowing; `p0b` vs `p0` says how much of any of it to believe.

## 3. Rig

Unchanged from what produced the tightest floor so far (median |A/A| 1.2%):

* **One data directory**, served by each build in turn — data layout, file
  placement and page cache are constant; the executable is the only variable.
* **ASLR disabled** for the postmaster, so restarts do not re-roll layout.
* **Build order rotated and mirrored** across sessions so linear drift cancels.
* **Repetitions back-to-back** within one psql session, so every sample of a
  query sees the same cache state.
* **Minima**, not medians: the interference on this host is one-sided spikes
  (a single 2495 ms sample among ~730 ms ones), which a minimum rejects.
* A **cache warm-up pass** before the first timed session — the host was
  observed to reboot mid-experiment once, and a cold page cache produced
  non-uniform drift that invalidated a whole run.
* Plans are **asserted identical** across builds per query, and checked for
  hash spills, before any timing is believed.

## 4. Controls

Three queries that *cannot* be affected by the patches run inside the same
program.  They are not decoration — a previous run showed `count(*)` moving
+3.7% on one build, which is a per-build baseline offset that has to come out
of every other number in that column.

| control | why it must not move |
|---------|----------------------|
| `count(*)` over the same table | touches no numeric code at all; used as the per-build normaliser |
| `stddev_pop` | variance states are created promoted, so the int128 fast path is excluded by construction |
| `GROUP BY int8` / `sum(int8)` | no numeric hashing, no numeric aggregate |

A **positive control** (a query already known to move) is also included, so
"no effect" can be distinguished from "rig cannot see effects".

Reported deltas are **control-normalised**: each build's raw delta minus that
build's `count(*)` delta.  The residual spread of the other controls sets the
honest resolution.

## 5. Query program

Each case is labelled with the class it is meant to represent, so "best /
worst / average" is answered from cases chosen in advance rather than picked
out of the results afterwards.

### Group A — hashing and comparison (`c3b903a`)

| id | case | class |
|----|------|-------|
| A1 | GROUP BY numeric, 1 group, 5M rows | typical |
| A2 | GROUP BY numeric, 1 000 groups | typical |
| A3 | GROUP BY numeric, 1 000 groups, **80/20 skew** | typical |
| A4 | GROUP BY numeric, 5M groups (one row each) | worst — no key reuse, memory-bound |
| A5 | GROUP BY **20 numeric columns**, 1 000 groups | best — cost removed once per key column per row |
| A6 | hash join on numeric, 5M × 1 000 | typical |
| A7 | GROUP BY **200-digit** numeric | worst — overflows the stack buffer, falls back to palloc |
| A8 | GROUP BY **toasted** numeric | worst — takes the `DatumGetNumeric()` path in full |
| A9 | GROUP BY numeric of **mixed display scale** | worst — the `memcmp()` fast path runs and misses on every comparison |
| A10 | `ORDER BY` numeric, 5M rows | neutral expected — `numeric_abbrev_convert()` already had this trick upstream |

### Group B — int128 fast sum (`9035685`)

Value widths are chosen against the eligibility rules read out of
`numericvar_to_int128_scaled()`, and verified from `pg_column_size()` (a
short-header numeric occupies `3 + 2*ndigits` bytes, so the stored digit count
is exact, not inferred):

| id | case | class |
|----|------|-------|
| B1 | `sum(numeric(10,2))`, 1 group, single-column table | **best** — int64 lane, aggregate is the dominant cost |
| B2 | `avg(numeric(10,2))`, 1 group | best |
| B3 | 7 × `sum`, 1 group | best — per-row cost paid seven times |
| B4 | `sum`, 1 000 groups | typical |
| B5 | `sum`, 1 000 groups, **80/20 skew** | typical |
| B6 | `sum`, 592 000 groups | typical — per-group state cost dominates |
| B7 | `sum`, 5M groups (one row each) | worst amortisation |
| B8 | `sum` of **25-digit** values | **worst** — skips the int64 lane, keeps succeeding, so the checked 128-bit path runs on every row and never promotes out |
| B9 | `sum(numeric(32,2))` | worst — promotes mid-scan, verified at row 1 701 412 of 5M |
| B10 | `sum` of **45-digit** values | corner — rejected by the width cap on row 1, so the state promotes immediately |
| B11 | `sum`, alternating display scale | corner — forces a scale-up conversion per row |
| B12 | `sum` with 30% NULLs | corner |
| B13 | `sum` with NaN/±Inf mixed in | corner — specials bypass the accumulator entirely |
| B14 | moving `sum`, bounded frame | corner — inverse transition |
| B15 | `sum` over sorted **GroupAggregate** | isolation — aggregate without any numeric hashing |

### Group C — parallel serialize / combine (`693023f`)

Plans are asserted to be Finalize → Gather → Partial HashAggregate.  An
earlier attempt with `parallel_tuple_cost = 0` produced plans that gathered
raw rows instead, so this commit's code path never ran; `0.005` is required.

| id | case | class |
|----|------|-------|
| C1 | 7 × `sum`, 592k groups, 1 worker | scaling curve |
| C2 | 7 × `sum`, 592k groups, 2 workers | **best** — the commit's own benchmark shape |
| C3 | 7 × `sum`, 592k groups, 3 workers | best / anti-scaling check |
| C4 | 1 × `sum`, 592k groups, 2 workers | typical |
| C5 | `sum` of 45-digit values, 592k groups | worst — every partial state promoted, so the historical digit-array format is serialized |
| C6 | `sum`, 1 000 groups | worst — serialization negligible relative to the scan |
| C7 | `sum`, 1 group | worst — no per-group serialization at all |

Known limitation: four cores cap the worker curve at 3, so the commit
message's "removes the previous anti-scaling" claim cannot be tested to the
worker counts where it was presumably observed.

### Controls
`X1` `count(*)`, `X2` `stddev_pop`, `X3` `GROUP BY int8`, `X4` `sum(int8)`.

## 6. Secondary experiment: `jit = on`

The primary run is `jit = off`, which isolates the C changes.  A subset then
re-runs with `jit = on` at default costs, because that is what a production
server does and because LLVM may inline `numeric_eq()` from bitcode, which
would change the hash/comparison result qualitatively.  Reported separately —
JIT compilation time is itself noisy, so it is a realism check, not the
headline.

## 7. How best / worst / average are computed

* **best** = the largest improvement among cases labelled *best* or *typical*
  that exceeds the noise floor.
* **worst** = the largest regression among all cases, including the corner
  cases built specifically to defeat each optimisation.
* **average** = geometric mean of per-query speedup ratios over the *typical*
  class only, since arithmetic means over ratios and mixing in deliberately
  pathological corner cases both mislead.  The full distribution is reported
  alongside it, per commit, so the mean is never the only number.

Every figure is quoted against the A/A floor from the same run.  Any delta
inside that floor is reported as "no measurable change", not as a small gain.

## 8. Correctness gate

The performance numbers are only meaningful if the builds compute the same
answers.  Before timing: 245 regression tests, `test_int128` across all three
implementations of the overflow primitives, and 14 properties reduced to md5
digests and compared across every build — sums and averages grouped and
total, the variance family, moving aggregates, int128 boundary values,
specials, NULL groups, and the parallel serialize→combine path.
