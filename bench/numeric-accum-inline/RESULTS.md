# Inline numeric sum accumulator: implemented, measured, does not pay

I proposed this as the alternative to the int128 fast sum: leave the per-row
path alone and attack the per-group allocations instead.  It is implemented
(`0001-inline-accumulator.patch`, branch `numeric-accum-inline` off `fd2b898`),
it is correct, and **it does not deliver the win I predicted.**

## What was built

`NumericSumAccum` gains inline `local_pos[]` / `local_neg[]` buffers.
`accum_sum_rescale()` uses them while the accumulator fits and `palloc`s only
beyond, shifting in place with `memmove` when it grows within the inline
capacity.  The per-row path is untouched: same additions, same deferred carry,
no eligibility test, no promotion, no serialization change.  ~90 lines.

Two hazards had to be handled:

* the digit pointers may now refer to storage inside the struct, so they must
  not be `pfree`'d directly — `accum_sum_free()` encapsulates that, and
  `numeric_poly_stddev_internal()`, which builds a `NumericAggState` **on the
  stack**, would otherwise have freed a stack address;
* in passing, `accum_sum_copy()` was not copying `have_carry_space`, so a
  copied accumulator enlarged itself once more than it needed to.

## Correctness

245/245 regression tests; all 19 cross-build md5 digests identical to base,
including the widening-overflow cases and the parallel serialize -> combine
path.

## The result

Production flags, `jit=off`, minima of 10 samples per build, int8 grouping
keys throughout so numeric hashing cannot contaminate the numbers, normalised
on `sum(int8)`.

| case | inline 8 | inline 6 |
|------|---------:|---------:|
| sum, 1 group (per-row check) | +2.7% | −1.7% |
| sum, 1 000 groups | +3.5% | −0.6% |
| sum, 100 000 groups | +11.1% | −3.9% |
| sum, 592 000 groups | +11.5% | −0.1% |
| 3 x sum, 592k groups (3 states/group) | +3.8% | +1.5% |
| `stddev_pop`, 592k groups (sumX and sumX2) | +6.9% | −4.7% |
| sum, 5M groups (1 row each) | +0.8% | −0.2% |
| sum(25-digit), exceeds inline capacity | +5.1% | −1.1% |
| **geometric mean over typical** | **+8.6%** | **−1.5%** |

A/A floor for the second run: median 1.9%, max 7.4%.  So **inline 6 is
neutral** — every figure is inside the floor — and inline 8 is a real
regression.

## Why inline 8 regressed: the allocator, not the digits

`NumericAggState` is 144 bytes upstream.  aset rounds every request up to a
power-of-two size class, so 144 lands in the 256-byte class.

| inline digits | sizeof | size class |
|---|---:|---:|
| upstream | 144 | 256 |
| 4 | 208 | 256 |
| 6 | 240 | 256 |
| 7 | 256 | 256 |
| **8** | **272** | **512** |

At eight digits the struct crosses the boundary and every group's state
doubles in allocated size — 151 MB to 303 MB across a 592k-group aggregate.
The extra memory traffic costs far more than two small allocations save.  The
inline capacity is therefore bounded by the allocator, not by how many digits
one would like to cover; six is the largest useful value that stays in class
(and is exactly what `numeric(15,2)` needs).

## The honest conclusion

**My prediction was wrong.**  I argued from `−8.1%` on a 1 000-group,
int8-keyed aggregate that the per-group allocations were where the real cost
sat, and that removing them would recover it.  Removing them recovers nothing
measurable.  That earlier −8.1% was −5.1% net of its own A/A control at a 4.8%
floor — marginal, and I treated it as solid.

So both attempts at making `sum(numeric)` faster have now failed for the same
underlying reason: the digit-array accumulator is already efficient in *both*
dimensions.  Per row it does a handful of `int32` additions with deferred
carry, which binary conversion cannot beat.  Per group its two allocations are
too cheap to matter.  The ~72 ns/row that `sum(numeric)` costs over
`count(*)` is somewhere else entirely — fmgr dispatch, tuple deform,
`init_var_from_num()`, the specials check, scale bookkeeping, the memory
context switches — and neither patch touches any of it.

**Recommendation: do not submit this either.**  It is neutral, and PostgreSQL
does not take neutral patches that add an ownership rule and 90 lines.  The
one piece worth salvaging independently is the `accum_sum_copy()` fix, which
is a genuine (if tiny) bug.

## What would actually be needed next

A profile.  Everything above about where the 72 ns/row goes remains inference
from code reading and A/B deltas, because this host has no PMU
(`perf_event_paranoid=2`, no hardware counters).  Two failed optimisations
have now been designed against that inference.  The next step is not another
patch — it is `perf record` on a machine with a PMU, to find out what the
per-row cost actually consists of before touching anything.
