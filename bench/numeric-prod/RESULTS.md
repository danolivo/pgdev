# Results: `numeric-int128-agg-fastpath` under production build settings

Executes the programme in `PLAN.md`.  1 520 timed samples in the main run plus
320 in the JIT run, four build points, one shared data directory.

## Verdict

| commit | best | average (typical) | worst | recommendation |
|--------|------|-------------------|-------|----------------|
| `c3b903a` hashing & comparison | **−58.2% (2.39x)** | **−43.3% (1.76x)** | +1.8% (inside floor) | **commit it** |
| `9035685` int128 fast sum (int64 conversion lane) | −2.2% (inside floor) | **+3.8% (0.96x)** | **+5.6%** narrowed / +12.5% as submitted | do not commit as-is |
| `693023f` serialize/combine | not separable from grouping; the one case with no per-group serialization is **+3.7%** | | | folds into the above |

`2c2905f` adds inline functions and a test module with no callers of its own,
so it cannot move a runtime number; it is covered under correctness.

## Build settings

```
CFLAGS   = -O2 -g -pipe -Wall -Werror=format-security -fstack-protector-strong
           -fstack-clash-protection -fcf-protection -fasynchronous-unwind-tables
           -fno-omit-frame-pointer -mtune=generic
CPPFLAGS = -D_FORTIFY_SOURCE=2
configure: --with-llvm (LLVM 18), no --enable-cassert
```

No `-march`/`-mtune=native`: distro packages target generic x86-64.

## Noise floor

A/A floor (`p0b`, an independent **rebuild** of the base commit, so it captures
code-layout luck and not only drift): **median 2.1%, max 3.3%** over the
attributed queries.  Residual on the three null controls: −0.7% to −4.4%.
Anything under ~4% is reported as no measurable change.

The `count(*)` control moved by up to 2% per build, so every delta below is
that build's raw delta minus its own `count(*)` delta.

## `c3b903a` — hashing and comparison

| class | case | as submitted | narrowed series |
|-------|------|-------------:|----------------:|
| best | GROUP BY 20 numeric columns | **−56.0%** | **−58.2%** |
| typical | 1 000 groups | −44.7% | −46.2% |
| typical | 1 000 groups, 80/20 skew | −45.1% | −46.1% |
| typical | hash join, 5M x 1k | −39.4% | −41.2% |
| typical | 1 group | −38.0% | −39.4% |
| **average** | geometric mean over typical | **−41.9% (1.72x)** | **−43.3% (1.76x)** |
| worst | 200-digit keys (palloc fallback) | −40.0% | −40.0% |
| worst | mixed display scale (memcmp always misses) | −26.3% | −26.1% |
| worst | 5M groups, one row each | −4.3% | −4.1% |
| worst | toasted keys | −3.7% | +1.8% |
| neutral | `ORDER BY` numeric | −1.3% | +1.2% |

Every case built to defeat it still comes out ahead or neutral.  The two that
land inside the floor are the ones with no key reuse (5M groups) and the one
that takes the `DatumGetNumeric()` path in full (toasted).

**This got better under production flags, not worse.**  The pre-production
measurement of the same commit was −31.8% (1.47x) typical; here it is −43.3%
(1.76x).  The prediction going in was the opposite: `-fstack-protector-strong`
puts a canary on every function that inlines `numeric_unpack_local()`'s
70-byte stack array, which is exactly the set of functions this commit
accelerates.  It does — but hardening also taxes the `palloc`/`pfree` pair on
the *baseline* side, and that costs more than the canary.

## `9035685` — int128 fast sum with an int64 conversion lane

Attributed on the twelve **ungrouped** sums only, so numeric grouping cannot
contaminate the number:

| class | case | as submitted | narrowed |
|-------|------|-------------:|---------:|
| best | `sum(numeric(10,2))`, 1 group | +3.7% | +5.6% |
| best | `avg(numeric(10,2))`, 1 group | +4.8% | +4.1% |
| best | 7 x `sum`, 1 group | +4.7% | +1.8% |
| **average** | geometric mean over best/typical | **+4.4% (0.96x)** | **+3.8% (0.96x)** |
| worst | 25-digit (no int64 lane, never promotes) | **+12.5%** | +3.6% |
| worst | `numeric(32,2)` (promotes mid-scan at row 1 701 412) | +9.4% | +4.1% |
| boundary | 17-digit (first value past the new gate) | +6.8% | +3.0% |
| boundary | 16-digit (widest still eligible) | −1.4% | +0.9% |
| corner | 30% NULLs | +5.1% | +2.3% |
| corner | NaN/±Inf mixed | +5.5% | +2.8% |
| corner | 45-digit (promotes at row 1) | +1.0% | +1.1% |
| corner | alternating display scale | +2.4% | +2.0% |
| corner | moving sum (inverse transition) | +2.3% | −2.2% |

No gain in any value class, including its own best case.  Eleven of twelve
ungrouped sums lean positive; the consistency across independent queries is
stronger evidence than any single one at a 3.3% floor.

### Why it cannot win, from the code

The aggregate portion of `sum(numeric(10,2))` is **72 ns/row** (513.4 ms minus
154.0 ms for `count(*)` over the same table, 5M rows).

`accum_sum_add()` for a 3-digit value is a rarely-taken carry check, an
`accum_sum_rescale()` call that is a no-op after the first rows, and **three
`int32` additions into a preallocated array** — carry propagation is deferred
until `num_uncarried` reaches `NBASE-1`.  Single-digit nanoseconds.

The int64 lane replaces those three additions with a dscale comparison, a loop
doing `acc = acc * NBASE + digit` (a **multiply** per digit), a division by
10/100/1000, a sign flip, and a checked 128-bit add.  It does more work per row
than the code it replaces.  The +3.8% is not a failed optimisation; it is an
anti-optimisation, and the code shows it as plainly as the benchmark does.

### Where the win the commit was chasing actually lives

The premise — "three allocations per group per aggregate" — is real:
`accum_sum_rescale()` does two `palloc0` per group per aggregate
(`numeric.c:12749-12750`).  The cleanest measurement of it, "sum, 1 000 groups,
**int8 key**" (keyed on int8 so numeric hashing cannot contaminate it), showed
**−8.1%**.  That is the whole genuine benefit, and it comes from skipping
allocations, not from the per-row arithmetic.

A targeted alternative: embed small fixed-size digit arrays in
`NumericSumAccum` and `palloc` only beyond them.  The per-row path stays
byte-identical, there is no eligibility test, no promotion, no mode byte and no
serialization format change, results are unchanged by construction, and the
patch is ~50 lines instead of ~800.  The one hazard is that embedded arrays
make the struct non-relocatable, so `accum_sum_copy()`, `accum_sum_reset()` and
the combine/deserialize paths must re-point rather than copy.

## The narrowed gate

Restricting eligibility to the int64 lane recovers 3–9 points exactly where
predicted and costs nothing measurable elsewhere:

| case | as submitted | narrowed | recovered |
|------|-------------:|---------:|----------:|
| 25-digit, never promotes | +12.5% | +3.6% | **−8.9 pp** |
| `numeric(32,2)`, promotes mid-scan | +9.4% | +4.1% | −5.3 pp |
| 17-digit, first past the gate | +6.8% | +3.0% | −3.9 pp |
| 30% NULLs | +5.1% | +2.3% | −2.8 pp |
| NaN/±Inf | +5.5% | +2.8% | −2.7 pp |
| 16-digit, widest still eligible | −1.4% | +0.9% | +2.3 pp (floor) |

It halves the worst case but does not make the commit positive: ungrouped sums
remain ~+3.8%.  Mitigation, not a fix.

## `jit = on`

Re-run with `jit_above_cost = 0` and `jit_inline_above_cost = 0`, so LLVM
inlines strict operator functions from the installed bitcode — including
`numeric_eq()`.  A/A floor 1.2% median, 2.9% max: the tightest of any run.

| case | as submitted | narrowed |
|------|-------------:|---------:|
| GROUP BY numeric, 1k groups | −43.9% | −43.4% |
| GROUP BY 20 numeric columns | −56.2% | −54.4% |
| hash join on numeric | −41.0% | −39.7% |
| `sum(numeric(10,2))`, 1 group | +2.5% | +2.1% |
| 25-digit sum | +11.8% | +4.2% |
| GROUP BY int8 (control) | +0.5% | +0.1% |

JIT changes nothing.  Both conclusions survive a production JIT configuration.

## Correctness

19 properties reduced to md5 digests and compared across all four production
builds — all identical.  245/245 regression tests.  `test_int128` across all
three implementations of the overflow primitives (portable two-limb, native
with builtins, native with the manual sign rule), 50M and 20M randomized
cross-checks.

The corpus had to be **re-derived after the narrowing**: the original int128
boundary cases (`2^127-1`, `2^126`) have 10 stored digits and are now rejected
before entering the accumulator, so they tested immediate promotion rather than
accumulator overflow.  With eligible coefficients bounded by 10^18, the
per-row add cannot overflow by row count either (~10^20 rows).  The reachable
route is **scale widening**: 1 000 x 16-digit integers summing to 10^19, then
one eligible `1e-20`, forcing `fastScale` 0 -> 20 and `10^19 * 10^20 = 10^39`
past the int128 limit.  Added, along with the reversed order, groups straddling
the gate, the moving-window inverse over the same widening, and the same
through parallel serialize -> combine.

---

# Lessons learned

Each of these cost real time in this exercise.

**1. Measure with production build flags; do not reason about which flags will
help.**  The prediction that `-fstack-protector-strong` would shrink the
hash/comparison gain was confident, specific, mechanistically argued — and
backwards.  The gain grew from 1.47x to 1.76x, because hardening taxes the
baseline's `palloc`/`pfree` more than it taxes the patch's stack buffer.

**2. Put null controls inside the benchmark, and normalise by them.**  An
earlier run had `count(*)` — which touches no numeric code — moving +3.7% on
one build.  Without subtracting that, every number in that column is wrong by
3.7%.  Three controls, not one: their residual spread is the honest resolution.

**3. The A/A control must be an independent rebuild, not a copy of the same
binary.**  A copy measures drift only; a rebuild of the same commit also
measures code-layout luck, which is the larger effect.

**4. Use minima, not medians, when the noise is one-sided.**  The interference
here was single spikes — one 2 495 ms sample among 730 ms ones.  A median
carries that; a minimum rejects it.  Medians made queries look 25–125% RSD.

**5. Serve one data directory from every build in turn.**  This removed the
largest confound (data layout, file placement, page cache) and took the floor
from ±5–12% to ±1–3%.  Add ASLR-off and a mirrored session rotation.

**6. Verify the plan actually reaches the code under test.**  Setting
`parallel_tuple_cost = 0` produced plans that gathered raw rows instead of
partial-aggregating, so the serialization commit's code never ran.  It took
`0.005` to get Finalize -> Gather -> Partial HashAggregate.  Assert plan shape
per query; do not assume the setting you reached for produced it.

**7. Verify dataset properties instead of assuming them.**  Two probe columns
were degenerate on first construction: the "80/20 skew" key had 320 groups
rather than 1 000, and the "mixed display scale" column had only one
representation per value because the branch and the value shared a parity.
`pg_column_size` gives the stored digit count exactly (`3 + 2*ndigits` for a
short header) — use it rather than reasoning about the encoding.

**8. When the code changes, re-derive whether the tests still reach it.**
Narrowing the gate silently invalidated the int128 boundary cases: they now
promote at row 1 and no longer exercise the accumulator at all.  A test suite
that still passes is not evidence that it still tests anything.

**9. Attribute each query to the commit it can actually reach.**  Grouped sums
are dominated by the hashing commit, so a `sum(...) GROUP BY numeric` query
cannot measure the aggregate commit.  Only ungrouped sums separate them.  The
first analysis credited the aggregate commit with gains that belonged to
hashing.

**10. Check the machine did not change under the experiment.**  One run was
invalidated by the host rebooting mid-flight: `uptime` showed 34 minutes
against a session hours old, the page cache was cold, drift was non-uniform,
and the A/A floor blew out to ±13%.

**11. Verify a background job actually started producing output.**  The
`--with-llvm` build failed on a missing header and sat dead for two hours while
a waiter polled for a success marker that could never appear, because all the
compiler output went to per-target log files and the top-level log stayed
empty.  A prior `apt-get install` had returned rc=100, and the presence of
`llvm-config-18` on `PATH` was taken as proof it had worked — but that is the
binary; the headers are a separate package.

**12. Label cases best/typical/worst before looking at results,** and define
the average as a geometric mean over the typical class only.  An arithmetic
mean over ratios, or one that mixes in cases built specifically to defeat the
patch, produces a number that means nothing.

**13. Get a profile.**  Everything above about *where* the 72 ns/row goes is
inference from code reading and A/B deltas, because this host has no PMU
(`perf_event_paranoid=2`, no hardware counters).  A profile would have answered
in ten minutes what took a day to infer, and it is the first thing to obtain
before designing a replacement.
