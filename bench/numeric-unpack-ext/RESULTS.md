# Extending the stack unpack to the aggregate transition and arithmetic

`c3b903a` removed a per-call `DatumGetNumeric()` copy from the numeric hash
and comparison functions and got 1.76x on grouping.  The same copy was still
present in two other hot paths, which the branch never touched:

* `numeric_avg_accum()` / `numeric_accum()` detoast their input **once per
  row**;
* `numeric_add/sub/mul/div` do it **twice per operation**.

Values arriving from a tuple carry a one-byte varlena header, so each of those
is a palloc'd copy that lives only for the duration of the call.  This patch
uses the existing `numeric_unpack_local()` helper at those sites.  ~40 lines.

Safety is the same argument as the original commit: none of the callees
retains its argument.  The arithmetic workers read operands through
`init_var_from_num()` and build a fresh result; the aggregate transition
copies digits into the accumulator.  `DatumGetNumeric()` still handles
compressed, out-of-line and unusually long values.

## Result

Reference is `c3b903a` alone (`ph`); `phb` is an **independent rebuild** of
it, so the A/A column captures code-layout luck as well as drift.  Production
flags, `jit=off`, minima of 10 samples per build, normalised on `count(*)`.

| case | ph ms | A/A | **extension** |
|------|------:|----:|-----:|
| `sum(numeric)` | 596.5 | +0.9% | **−17.0%** |
| `avg(numeric)` | 588.3 | +5.1% | **−15.5%** |
| `sum(25-digit)` — wide values | 550.4 | +1.3% | **−15.6%** |
| `sum(v*v2)` | 1077.3 | +3.8% | −4.8% |
| `sum(v*v2+v3)` | 1637.8 | +1.9% | −4.8% |
| TPC-H Q1 shape, 5 aggregates | 6462.2 | +3.1% | −2.5% |
| projection only, no aggregate | 201.6 | +6.3% | +5.7% |
| `sum` GROUP BY numeric, 1k groups | 866.3 | +2.1% | +1.4% |
| moving sum (inverse transition) | 253.4 | +2.0% | +2.5% |
| `sum(int8)` — control | 256.1 | +4.0% | +0.7% |

A/A floor: median 2.6%, max 6.3%.  Geometric mean over the targeted cases:
**−9.1% (1.10x)**.

245/245 regression tests; all 19 cross-build correctness digests identical,
including the widening-overflow cases and the parallel serialize -> combine
path.

## What this explains

`sum(numeric)` costs ~79 ns/row above `count(*)`.  Removing one palloc'd copy
per row takes 17% off the entire query -- so that copy, not the accumulation,
was the dominant per-row cost of a plain numeric aggregate.

That is the missing piece behind both earlier failures.  `9035685` optimised
the accumulation *arithmetic* and left this copy in place, which is why it
measured +4% instead of anything positive: it was working downstream of the
real cost.  The inline-accumulator patch attacked the per-group allocations
and measured nothing, for the same reason.  The cost was never in the
accumulator -- it was in getting the value out of the tuple.

Arithmetic gains less (2-5%) because `add_var()`, `mul_var()` and especially
`div_var()` dominate their own cost, so operand detoasting is a smaller share.
The Q1-shaped query gains least of all at 2.5%, being division-bound.

Beforehand I projected ~25% for `sum(numeric)` and ~20% for the Q1 shape.  The
first was accurate (measured 17% of total query time, where the projection was
25% of the aggregate portion); the second was too optimistic by roughly 8x,
because I assumed every unpack was worth the same ~20 ns regardless of what
surrounded it.

Note that wide values gain as much as narrow ones: 25-digit values are seven
`NumericDigit`s, well inside the 32-digit stack buffer, so everything up to
128 decimal digits benefits.

## Status

Worth submitting, as a follow-up to `c3b903a` and independent of the int128
work.  The two cases that read slightly positive (projection-only at +5.7%,
moving sum at +2.5%) both sit within an A/A floor whose maximum is 6.3%, and
the `sum(int8)` control is clean at +0.7%.
