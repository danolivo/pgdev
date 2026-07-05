# Technical specification: planner rewrite for `date_trunc('unit', x)` comparisons

**Author:** Andrei V. Lepikhov
**Status:** Draft (Stages 1–2, 4 implemented; Stages 3, 5–6 proposed)
**PostgreSQL target version:** PG20
**Discussion thread:** (not yet posted to pgsql-hackers)
**Related patches:** in `pgdev-2`, branch `date_trunc_transform_v18`
**Date:** 2026-05-14

---

## 1. Problem statement

DBAs and application developers routinely write date-grouped predicates like:

```sql
SELECT * FROM events WHERE date_trunc('day', ts) = '2024-06-15';
SELECT * FROM events WHERE date_trunc('day', ts) = some_date_column;
SELECT * FROM events WHERE date_trunc('day', ts) = date_trunc('day', other_ts);
```

These are semantically equivalent to half-open range conditions on `ts`, but PostgreSQL's planner does not see them that way. The `date_trunc` call wraps the indexable column, which has three concrete consequences:

1. **B-tree indexes on `ts` are bypassed.** A query that should run as a 50ms `Bitmap Index Scan` over a one-day slice instead runs as a seq scan over the whole table.
2. **Range partitioning on `ts` does not prune.** A query targeting one day on a table partitioned monthly scans all twelve partitions instead of one.
3. **BRIN cannot help.** BRIN's whole point is range comparisons; a function-wrapped column is opaque to it.

The user workaround — rewriting the WHERE clause by hand as `ts >= '2024-06-15' AND ts < '2024-06-16'` — is well known among power users but invisible to the long tail of application developers and ORM-generated SQL. The cost is silent: queries are slow, indexes are not used, partition tables are not pruned, and the operator wonders why their carefully placed index "isn't working".

This specification proposes a planner-time rewrite that detects the `date_trunc('day', x) = d` pattern (and, in later stages, related shapes) and replaces it with the equivalent half-open range, on the same correctness footing as the existing `LIKE 'foo%'` → range rewrite (`text_starts_with_support` in `src/backend/utils/adt/like_support.c`).

### 1.1 Concrete impact example

On a 50 000-row events fixture with a B-tree index on `ts`:

```
-- Before the rewrite
EXPLAIN SELECT * FROM events WHERE date_trunc('day', ts) = DATE '2020-06-15';
 Seq Scan on events  (cost=0.00..1167.50 rows=200 width=24)
   Filter: (date_trunc('day'::text, ts) = '2020-06-15'::date)

-- After Stage 1
 Bitmap Heap Scan on events  (cost=4.30..192.40 rows=137 width=24)
   Recheck Cond: ((ts >= '2020-06-15'::date::timestamp) AND (ts < ...))
   ->  Bitmap Index Scan on events_ts_idx
         Index Cond: ((ts >= '2020-06-15'::date::timestamp) AND (ts < ...))
```

The same query goes from a 50 000-row scan to a ~140-row index lookup.

---

## 2. Background and prior art

### 2.1 The prosupport facility

PostgreSQL has had per-function planner support functions since v12. A `pg_proc.prosupport` entry points at a C function that handles `SupportRequest*` messages, of which the relevant one here is `SupportRequestSimplify`. The handler may return an arbitrary `Node *` to replace the original `FuncExpr`/`OpExpr` during `eval_const_expressions`.

### 2.2 The LIKE rewrite — the closest existing precedent

`text_starts_with` (PG11+) and the LIKE family use prosupport to derive range conditions from a prefix pattern. The mechanism is documented in `like_support.c`:

> An example of what we're doing is `textfield LIKE 'abc%def'` from which we can generate the indexscanable conditions `textfield >= 'abc' AND textfield < 'abd'` which allow efficient scanning of an index on textfield.

The LIKE rewrite uses `SupportRequestIndexCondition` (a *refinement* request — the original LIKE remains as a recheck filter, because the range is lossy). Our case is different: when the RHS is provably midnight-aligned, the half-open range is *exact*, not lossy. We therefore use `SupportRequestSimplify` to replace the OpExpr entirely.

### 2.3 Past discussion on pgsql-hackers

A handful of threads have circled this area without producing a committed patch:

- Discussions about `date_trunc` returning `timestamp` regardless of input type (resulting in needless casts) — partially addressed by adding cross-type comparison operators in PG14.
- Periodic requests on -performance for "why doesn't my index get used with date_trunc" — the canonical reply has been "rewrite the predicate yourself".
- The PG14 addition of cross-type timestamp/date eq operators (`timestamp_eq_date`, `timestamptz_eq_date`) reduced one source of friction but did nothing for `date_trunc`-wrapped column predicates.

No proposal to implement a planner-time rewrite has reached the commitfest.

### 2.4 Subsystems involved

| Subsystem | Touched by | Stages |
|-----------|-----------|--------|
| `src/backend/utils/adt/timestamp.c` (or new `bucket_expansion.c`) | rewrite implementation | 1, 3 |
| `src/include/catalog/pg_proc.dat` | prosupport wiring | 1, 4, 5 |
| `src/backend/optimizer/util/clauses.c` | dispatch into prosupport (unchanged) | — |
| `src/backend/utils/adt/datetime.c` | unit decoding helpers reused | 1, 4 |
| `src/include/nodes/supportnodes.h` | new request type (Stage 6) | 6 |
| `src/test/regress/sql/{timestamp,timestamptz,create_index,partition_prune}.sql` | regression coverage | every stage |

---

## 3. Proposed design

### 3.1 Architecture overview

```
        ┌──────────────────────────────┐
        │ eval_const_expressions_mutator│
        │  case T_OpExpr:               │
        │   simplify_function(opfuncid) │
        └──────────────┬───────────────┘
                       │ if prosupport set
                       ▼
        ┌──────────────────────────────┐
        │  SupportRequestSimplify       │
        │   ─ FuncExpr {opfuncid, args} │
        └──────────────┬───────────────┘
                       │
                       ▼
   ┌─────────────────────────────────────────────────────┐
   │  date_trunc_eq_support()  ── Stage 1            │
   │  later: bucket_expansion_eq_support()  ── Stage 3+  │
   │                                                     │
   │  1. gate on funcid (5 EQ: 4 cross, 1 same-timestamp) │
   │  2. find date_trunc(unit, x) on either side         │
   │  3. unit ∈ {day,week,month,quarter,year} → step     │
   │  4. RHS aligned: date (day), or Const on boundary   │
   │  5. emit half-open range; fold upper when Const     │
   │  6. return Node*, replaces OpExpr                   │
   └─────────────────────────────────────────────────────┘
```

The mechanism stays in core. No new GUC. No catalog table. No background worker. The footprint is one C function plus four `pg_proc` rows touched.

### 3.2 Soundness conditions

The rewrite

```
date_trunc(unit, x) = d   →   x >= bucket_start(d) AND x < bucket_start(d) + bucket_step(unit)
```

is sound iff all three hold:

1. **Bucket alignment.** `bucket_start(d) = d`, i.e. `d` is already on a bucket boundary for the given unit. For unit `'day'` and a `date`-typed RHS, this is trivially true; the proof becomes harder for other units (see Stage 4).
2. **Finite operands.** Neither `d` nor `x` is `±infinity`. Otherwise the range `[d, d + step)` collapses to `[infinity, infinity)` (empty) and the rewrite drops rows that the original predicate matches (`date_trunc('day', infinity) = infinity` is true, but `infinity < infinity` is false).
3. **Finite step.** `bucket_step(unit)` is finite for the unit. Trivially true for all `date_trunc` units; included for completeness because Stage 6 admits user-defined bucketing functions.

#### Deployment assumption: no infinity values

Condition (2) is **not** discharged at plan time. Stage 1 targets deployments where the schema excludes `±infinity` values from the column by `CHECK` constraint (or equivalent operational guarantee), and the rewrite emits the plain half-open range unconditionally. Under that assumption the rewrite is exact for every reachable input.

A Stage 1 deployment **without** that constraint is unsafe in two scenarios:

- a row holds `col = +infinity` and a query asks `WHERE date_trunc('day', col) = 'infinity'::date` — the original matches the row; the rewrite returns the empty set;
- a row holds `col = +infinity` and a query asks `WHERE date_trunc('day', col) = some_var_d` where `some_var_d` resolves to `+infinity` at run time — same outcome.

Deployments that allow infinity values must either keep this support function unwired (revert the `pg_proc.dat` edits) or move to a defensive variant — see Open Question 10. The XXX-marked block at the end of `src/test/regress/sql/timestamp.sql` pins the divergence for future reference.

### 3.3 Hook location

The support function is attached to the **equality operator's underlying function**, not to `date_trunc`. The reason is structural: `SupportRequestSimplify` is dispatched off `OpExpr.opfuncid`. The eq function is the only entry point that gets called for `date_trunc(...) = d`. (`SupportRequestIndexCondition` *can* be driven from `date_trunc`'s prosupport, but it only produces refinement quals, not a full replacement — and we have an exact rewrite, so a full replacement is preferable.)

Stage 6 proposes a new `SupportRequestRangeExpansion` request that inverts this — the bucketing function declares its semantics, the comparison-operator handler consumes them. That requires a second caller in core to be justified.

### 3.4 No state machine, no protocol

This is a planner-time pure-function rewrite. There are no worker processes, no persistent state, no protocol exchanges, no recovery considerations. The "failure mode" relevant here is a wrong-answer bug or a plan regression, addressed in §3.6.

### 3.5 Backward compatibility

| Concern | Behaviour |
|---------|-----------|
| pg_dump | unaffected; the rewrite is plan-time, not catalog-visible |
| pg_upgrade | adds one new `pg_proc` entry (OID 8773); standard catalog upgrade applies |
| Cached plans | invalidated on catalog change as usual; behaviour stable across re-plans |
| Existing user-written workarounds (manual `>=`/`<` ranges) | continue to work as before; rewrite is idempotent — applying it to an already-rewritten query is a no-op (no `date_trunc` to match) |
| `EXPLAIN` output | changes for queries that hit the rewrite; this is a visible but expected churn |
| Extensions overriding `planner_hook` | unaffected; the rewrite is in `eval_const_expressions`, which runs before `planner_hook` |
| `SET enable_*` flags | none added in Stage 1; see Open Question 2 |

### 3.6 Correctness hazards and mitigations

| Hazard | Mitigation (Stage 1) | Open in later stages |
|--------|----------------------|----------------------|
| `±infinity` operand collapses range to empty | **deployment assumption**: target schemas exclude infinity dates by `CHECK` constraint; no planner-level guard. See §3.2 | revisit if a defensive variant is wanted — see Open Question 10 |
| RHS isn't directly `DATEOID` (e.g. Const `timestamp`, `make_date(...)`, scalar subquery) | accepted limitation; rewrite skipped (the gate `exprType(date_node) != DATEOID` is strict) | addressed in Stage 2 (Const timestamp at midnight); broader coercion-stripping out of scope |
| Same-type EQ — Stage 2 | Only `=(timestamp, timestamp)` (`F_TIMESTAMP_EQ`) is wired; it rewrites when RHS is a non-null `Const` of type `timestamp` whose value is midnight (`is_unit_aligned_const`, timezone-independent since `date_trunc(text,timestamp)` is IMMUTABLE). **`=(timestamptz, timestamptz)` is deliberately NOT supported and `timestamptz_eq` is left unwired** — a timestamptz boundary is session-timezone dependent, so no plan-time-folded range can stay equivalent to the STABLE original across a `SET TIME ZONE` under a cached generic plan (see §7 Stage 2). | Users wanting the timestamptz form write `= DATE '...'` for the timezone-safe cross-type rewrite. |
| Reverse argument order (`date_eq_timestamp` / `date_eq_timestamptz`, `F_DATE_EQ_TIMESTAMP` / `F_DATE_EQ_TIMESTAMPTZ`) | wired and active; the dispatch in `date_trunc_eq_support` swaps when `is_date_trunc(right)` matches | — |
| Cross-type upper bound `typoid < (date + interval)` evaluates to `timestamp` and may need a timezone-bridging comparison | `get_opfamily_member_for_cmptype((typoid, TIMESTAMPOID), COMPARE_LT)` resolves to `timestamp_lt_timestamp` or `timestamptz_lt_timestamp`; no synthetic cast emitted | — |
| Timezone change between plan and execution | rewrite uses STABLE `timestamptz`-arithmetic operators; same evaluation-time semantics as original | — |
| DST transition days have 23 or 25 wall-clock hours | `interval '1 day'` on `timestamptz` is calendar-correct; matches original | — |
| `search_path` hijack of `+(date, interval)` | the `OpernameGetOprid` lookup is **`pg_catalog`-qualified**; a user-defined operator earlier in `search_path` cannot intercept the rewrite | — |
| Selectivity flip: `eqsel` → `scalargesel × scalarltsel` | usually a sharper estimate; rare adversarial cases possible | covered by Stage 1 regression sweep |
| `IS NOT DISTINCT FROM` not handled (parses as `DistinctExpr`) | documented limitation | Stage 5 may cover |
| Generated column shortcut already exists | rewrite is no-op when LHS is a Var, not a date_trunc call | — |
| Plan-cache reuse across `SET TIME ZONE` | identical to pre-rewrite behaviour for `timestamptz`-typed quals | — |
| `eval_const_expressions` does not re-walk the support function's output | upper bound is folded in C when `date_node` is a non-null Const, so a `Const + Const` subtree never reaches the executor | — |

### 3.7 Interaction matrix

| Feature | Stage 1 behaviour | Notes |
|---------|-------------------|-------|
| B-tree index on `ts` | works correctly | the primary benefit |
| BRIN index on `ts` | works correctly | range form is the canonical BRIN-friendly shape |
| Hash index | not applicable | hash indexes don't support range operators |
| GIN/GiST/SP-GiST | not applicable | these don't support `=` on timestamps natively |
| Range-partitioned `ts` | works correctly | partition pruning sees the range form |
| List-partitioned `ts` | not applicable | list partitions match equality only |
| Hash-partitioned `ts` | not applicable | rewrite doesn't help hash pruning |
| Constraint exclusion | works correctly | derived from the same qual evaluation path |
| Equivalence classes | unaffected | the original eq didn't seed useful ECs anyway (function-of-Var) |
| Row-Level Security | unaffected | RLS quals are evaluated against the rewritten qual normally |
| Triggers on subscriber | not applicable | not a replication feature |
| Generated columns | unaffected | rewrite leaves Var-on-LHS quals alone |
| Sequences / large objects | not applicable | — |
| FDW (`postgres_fdw`) pushdown | works correctly | the rewritten range is shippable; the original `date_trunc` call was also shippable but bypassed the remote index for the same reason it bypassed the local one |
| Parallel query | works correctly | all introduced ops are PARALLEL SAFE |
| Cached plans / prepared statements | works correctly | rewrite is deterministic for a given parse tree |
| Two-phase commit | not applicable | — |
| Logical replication | not applicable | — |

---

## 4. Monitoring and observability

- **`EXPLAIN` (and `EXPLAIN VERBOSE`)** show the rewritten qual. Operators see the rewrite by inspecting `Filter:` / `Index Cond:` / `Recheck Cond:` text.
- **No new wait events.** Plan-time work only.
- **No new `pg_stat_*` view.** A future enhancement could expose a counter of "rewrites applied per query" via `pg_stat_statements` extensions, but no stage in this spec proposes it.
- **Failure to rewrite is silent and intentional.** The most common cause of "not rewritten" today (Stage 1) is "RHS is a Const timestamp, not a date-cast FuncExpr"; Stage 2 addresses that.

A possible Stage 5+ enhancement is to add a `DEBUG2` log line when the support function fires, gated on a developer GUC. Not proposed in this spec.

---

## 5. Performance considerations

### 5.1 Plan-time overhead

The support function is called once per matching `OpExpr` during constant folding. The cost is:

- a table lookup keyed on `opfuncid` (4 entries in Stage 1, table-driven in Stage 3),
- two `IsA(FuncExpr)` checks,
- a `DecodeUnits` call on the unit Const (already used by `timestamp_trunc`; cost is a `downcase_truncate_identifier` plus a linear scan of the units table — tens of nanoseconds),
- five node constructions on rewrite success.

On a query with a single matching qual, total added plan time is well under 1µs. The cost is paid only for qualifying queries; non-qualifying queries (Var-on-LHS, non-day unit, etc.) bail in nanoseconds.

### 5.2 Run-time benefit

The rewrite enables three classes of run-time win that the original predicate did not:

1. **Index range scans** — the principal benefit. Order-of-magnitude row reduction for selective predicates.
2. **Partition pruning** — eliminates entire partitions from the plan; benefit scales with partition count.
3. **BRIN summary checks** — turns an otherwise unindexable predicate into one that can skip blocks.

### 5.3 Pathological cases

- **Selectivity flip.** The original `date_trunc('day', ts) = d` is estimated by `eqsel` on a non-Var expression — falls back to defaults. The rewritten form is estimated by `scalargesel × scalarltsel`, hitting the histogram. The rewrite usually produces *sharper* estimates, but in skewed-data scenarios where the day in question is in an underrepresented histogram bin, the rewrite can underestimate cardinality and flip the planner to a worse join order. Mitigation: regression sweep on `partition_prune` and `join_hash`; analyse buildfarm output during commitfest review.

### 5.4 Benchmark methodology

Stage 1 smoke testing covered 50 000-row synthetic data with B-tree on `ts`. A formal benchmark suite should cover:

- B-tree index lookup, varying selectivity (1 day, 1 week, 1 month).
- BRIN scan over 10M-row time-series fixture.
- Range-partitioned table with 12 monthly partitions.
- FDW pushdown via `postgres_fdw` against a remote partitioned table.

A pgbench-style script with `\timing` recorded before and after, reported in the commit message.

---

## 6. Testing strategy

### 6.1 Regression tests

SQL test additions (all in the type-conversion suites; no dedicated file yet):

| File | Coverage |
|------|----------|
| `src/test/regress/sql/timestamp.sql` | EXPLAIN-shape proofs (cross-type + same-type timestamp, both argument orders, folded upper bound, index use); coarser-unit proofs (week/month/quarter/year) with misaligned-Const, coarser-Var and unsupported-unit bails; non-midnight/infinity Const bails; NULL RHS; cross-type, same-type and per-coarser-unit result equivalence; timestamp no-tz plan-cache **control** (no drift across `SET TIME ZONE`) |
| `src/test/regress/sql/timestamptz.sql` | cross-type EXPLAIN proofs (day + coarser-unit date Const, aligned and misaligned); a **guard** proving same-type timestamptz is *not* rewritten — a cached generic plan keeps the STABLE `date_trunc` and re-truncates across a `SET TIME ZONE`, agreeing with a fresh re-plan (would fail if the rewrite ever fired for timestamptz) |

### 6.2 Edge cases explicitly tested

- `date_trunc('day', ts) = d` with `d` finite, `d = NULL`; the infinity divergence is pinned by the XXX block (not defended at runtime — deployment assumption).
- Cross-type cases, both argument orders: `(timestamp, date)`, `(timestamptz, date)`, `(date, timestamp)`, `(date, timestamptz)`.
- Same-type cases (Stage 2): `(timestamp, timestamp)` and `(timestamptz, timestamptz)` with a midnight Const RHS → rewrite; non-midnight Const and infinity Const → bail.
- Unit gating: `'day'` rewrites; `'year'` (and other non-day units) left alone.
- Const-folded RHS: `= '2000-03-15'` (Const timestamp at midnight) now rewrites via Stage 2; `= date_trunc('day', '...'::timestamp)` folds to a midnight Const and rewrites; `= date_trunc('day', '...'::timestamp)::date` rewrites via the cross-type path.
- Same-type non-Const RHS (`date_trunc(t1) = date_trunc(t2)`, both timestamp): left alone (not a Const).
- **Plan-cache / timezone interaction**: timestamp (no-tz) stays correct across a plan-time→execute-time `SET TIME ZONE`; timestamptz does **not** (the documented hazard) — both pinned by tests.
- Result equivalence against the hand-applied range for both cross-type and same-type paths.

Not yet covered (tracked): `IS NOT DISTINCT FROM` (Stage 5), partition-pruning and BRIN proofs (dedicated `date_trunc_rewrite.sql`, Open Question 7).

### 6.3 No TAP tests required

This is a planner-only feature with no multi-process interactions. SQL regression tests are sufficient.

### 6.4 Performance regression sweep

Before each commit, run `make check-world` and inspect the buildfarm output for plan changes in `partition_prune`, `join_hash`, `select_parallel`, and any other test that touches `date_trunc`. Expected: zero unintended plan changes outside the new test files.

---

## 7. Implementation plan

The work breaks into six stages. Stage 1 is implemented in the current branch (`relocate-subplan`). Stages 2–6 are proposed and ordered by dependency.

### Stage 1 — minimum viable rewrite ✅ implemented

**Scope (delivered):**
- `date_trunc_eq_support()` in `src/backend/utils/adt/timestamp.c`, after `interval_support()`.
- Attached via `pg_proc.dat` (OID 8773) to **five** equality functions: the four cross-type `timestamp_eq_date` (2366), `timestamptz_eq_date` (2379), `date_eq_timestamp` (2340), `date_eq_timestamptz` (2353) covering both argument orders, plus the same-type `timestamp_eq` (2052). `timestamptz_eq` (1152) is deliberately **not** wired — same-type timestamptz is unsupported (§3.6, §7 Stage 2), so wiring it would only tax a very common operator at plan time. The same-type timestamp variant rewrites only when the RHS is a midnight-aligned `Const`.
- Handles unit `'day'` only.
- RHS policy: `exprType(date_node) == DATEOID` after `strip_implicit_coercions`. No explicit-cast peeling. Naturally covers bare `date` Var/Const, `make_date(...)`, scalar subqueries returning date, and any implicit coercion that resolves to date.
- Cross-type comparison shape: emits `x >= d` (using `timestamp_ge_date` / `timestamptz_ge_date` from the default btree opfamily) and `x < (d + interval '1 day')` (using `timestamp_lt_timestamp` / `timestamptz_lt_timestamp` — `date + interval` returns `timestamp` regardless of `x`'s type). No synthetic cast appears in the predicate.
- Upper-bound Const folding: when `date_node` is a non-null `Const`, `d + interval '1 day'` is computed in C via `DirectFunctionCall2(date_pl_interval, ...)` at plan time and emitted as a single `Const timestamp`. The Var-RHS path keeps the OpExpr form.
- `+(date, interval)` lookup is **`pg_catalog`-qualified** via `OpernameGetOprid` — `search_path` cannot hijack it.
- Soundness on infinity: **assumes the deployed schema excludes `±infinity` values** by `CHECK` constraint (or equivalent guarantee — see §3.2). No planner-level finiteness check; the rewrite emits the plain half-open range unconditionally. The XXX-marked test in `src/test/regress/sql/timestamp.sql` records the divergence for the future case where the same-type EQ path is enabled.
- Verified: B-tree index use on Const-RHS (Index Only Scan), correct counts under all reachable RHS finiteness/type combinations, no regression on non-matching cases (unit ≠ `'day'`, RHS not `DATEOID`, etc.).

**Known limitations carried into later stages:**
- Const-folded `'2024-01-01'::timestamp` is not recognised (parser produces a Const of type `timestamp`, not `DATEOID`) — addressed in Stage 2.
- Only `=` is rewritten; `<`, `<=`, `>`, `>=` are not — addressed in Stage 5.
- Only `'day'` is supported — addressed in Stage 4.
- Schemas that allow `±infinity` dates in the affected columns must not enable this support function — see Open Question 10 for the path to a defensive variant.

**Estimated commit size:** ~220 lines including comments (the function body is ~190 lines; the rest is `pg_proc.dat` wiring).

### Stage 2 — widen RHS proof to midnight-aligned Const timestamps (timestamp only) ✅ implemented

**Scope (delivered):**
- Added a midnight/boundary-alignment helper in `src/backend/utils/adt/timestamp.c` (introduced as `is_midnight_timestamp_const`, generalised to `is_unit_aligned_const` in Stage 4) that returns true iff the `Const`'s value is exactly on the unit boundary.
- Extended the switch to include `F_TIMESTAMP_EQ`; attached `prosupport` to `timestamp_eq` in `pg_proc.dat`.
- The RHS discriminator accepts two shapes: `exprType == DATEOID` (Stage 1 cross-type path) or a `Const` of type `timestamp` proven midnight-aligned (same-type path).
- Same-type upper-bound folding uses `timestamp_pl_interval` (IMMUTABLE) — timezone-independent.

**`timestamptz` same-type deliberately excluded (the design decision this stage settled on):**
An earlier draft also wired `=(timestamptz, timestamptz)`, folding the range with the session timezone. That is **unsound under the default `plan_cache_mode = auto`**: a cached generic plan folds a specific instant at plan time, but the STABLE original re-truncates under the execute-time zone, so the two diverge across a `SET TIME ZONE`. No plan-time rewrite can be equivalent, because whether the constant is an execute-zone midnight is not knowable when the plan is built. It was therefore dropped: `timestamptz_eq` is unwired and same-type is gated to `typoid == TIMESTAMPOID`. Consequence: the common natural form `date_trunc('day', tstz) = '2020-01-01'` (whose bare literal coerces to timestamptz) is left alone; users write `= DATE '2020-01-01'` to get the timezone-safe cross-type rewrite. `timestamp` (no time zone) is timezone-independent and fully supported.

**Verified:**
- `EXPLAIN` proof for the same-type `= '2000-03-15'::TIMESTAMP` form (folded Consts, Index Scan picked).
- Bail on non-midnight Const (`'... 06:00:00'::timestamp`).
- Both timestamptz same-type spellings (`TIMESTAMPTZ '...'` and the bare literal) keep `date_trunc` — no rewrite; a functional guard shows a cached generic plan re-truncates across a `SET TIME ZONE` and agrees with a fresh re-plan.
- Result equivalence with the hand-applied range; timestamp no-tz plan-cache control shows no drift.
- Full regression suite (`make installcheck-parallel TESTS="timestamp timestamptz"`) — 231/231 pass.

**Estimated commit size:** ~70 lines added (helper + discriminator branching + same-type Const-fold); one `prosupport` attachment (`timestamp_eq`).

### Stage 3 — module refactor and table-driven dispatch

**Goal:** Decouple mechanism from policy. Prepare the codebase to admit additional units and comparison operators without ballooning the switch.

**Scope:**
- Create `src/backend/utils/adt/bucket_expansion.c`. Move the support function there. `timestamp.c` keeps only its existing functions.
- Introduce two static tables:

```c
typedef struct {
    Oid     type;            /* TIMESTAMPOID, TIMESTAMPTZOID */
    Oid     trunc_funcid;
    Oid     date_cast_funcid;
    Oid     ge_opno, lt_opno, le_opno, gt_opno, eq_opno, pl_opno;
    Oid     pl_funcid;
} BucketLhsInfo;

typedef struct {
    int     dtk;             /* DTK_DAY, DTK_HOUR, ... */
    Interval step;
    bool    fixed_size;      /* false for month/year/week — calendar arithmetic */
    bool  (*aligned_const)(Const *c, Oid type);
    bool  (*aligned_expr)(Node *n, Oid type);
} BucketUnitInfo;
```

- Refactor `date_trunc_eq_support()` into a thin shell that does `(opfuncid → BucketLhsInfo) × (unit → BucketUnitInfo)` lookup and delegates to `try_bucket_rewrite_eq()`.
- Add `src/include/utils/bucket_expansion.h` exposing the support function declaration only (no internal types).
- Update `pg_proc.dat` `prosupport` references (or — preferred — leave them pointing at the same `proname`, which we keep). No catalog change to OIDs.

**Risk:** behaviour-preserving refactor; review burden minimal if diffed against Stage 1.

**Dependencies:** Stages 1, 2 (Stage 2 lands in the same file; refactoring afterwards is cleaner).

**Estimated commit size:** ~400 lines (mostly code movement).

### Stage 4 — unit expansion (`week`, `month`, `quarter`, `year`) ✅ implemented

**Scope (delivered):** the fixed-boundary units `week`, `month`, `quarter` and `year`, in addition to `day`. Each maps to a constant step interval:

| Unit | Step | Boundary date_trunc lands on |
|------|------|------------------------------|
| `day` | 1 day | midnight |
| `week` | 7 days | Monday 00:00 (ISO week) |
| `month` | 1 month | first of month 00:00 |
| `quarter` | 3 months | Jan/Apr/Jul/Oct 1 00:00 |
| `year` | 12 months | Jan 1 00:00 |

**The alignment shift from Stage 1–2.** For `day`, every `date` value is on a boundary, so a date-typed RHS (Const *or* Var) is accepted with no value check. For coarser units alignment is a *value* property, not a type property: `date_trunc('month', x) = '2000-03-15'::date` is always false (LHS is always a 1st), and a naive range would wrongly return rows. So coarser units rewrite **only when the RHS is a Const provably on a unit boundary**; a coarser-unit Var RHS (of any type) is left alone.

**Alignment proof.** `is_unit_aligned_const(Const *c, Datum unit_datum)` (the Stage 2 helper, renamed and generalised) answers "is `date_trunc(unit, c) == c`?" by invoking the very same `timestamp_trunc` the executor would and comparing — no hand-rolled calendar logic, so the proof cannot drift from `date_trunc`'s real semantics (this matters most for ISO `week` and `quarter`). For `day` it reduces to the midnight check. It handles only `date` and `timestamp` (both timezone-independent) and rejects infinity on every path, so all coarser-unit paths are infinity-safe by construction (only the `day` + date-Var path retains the deployment-assumption gap).

**timestamptz stays consistent with Stage 2.** Same-type timestamptz remains unsupported for all units — `is_unit_aligned_const` is never passed a timestamptz Const (the cross-type path presents a date). The cross-type **date-Const** path *does* work for timestamptz columns at every unit and is timezone-safe: the date bound and the `date + interval` upper bound are timezone-independent, and the comparison operators (`timestamptz_ge_date`, `timestamptz_lt_timestamp`) convert at execution time.

**Verified:** per-unit EXPLAIN rewrite proofs; misaligned-Const bail (month 15th, week Wednesday, quarter Feb 1); coarser-unit Var-RHS bail; unsupported-unit bail (`century`); per-unit result equivalence against the hand-applied range; full regression suite 231/231.

**Deliberately out of scope:** sub-day units (`hour`, `minute`, `second`) — they'd need a different alignment proof and are less commonly indexed this way; variable/rare units (`decade`, `century`, `millennium`) — pragmatically pointless. These fall through to no rewrite.

**Note on naming:** the C function is still `date_trunc_eq_support` (and prosrc unchanged) to avoid catalog churn, despite now handling five units. A rename is a mechanical follow-up if desired.

**Estimated commit size:** ~90 lines over Stage 2 (the `is_unit_aligned_const` rewrite, the unit→step switch, and the coarser-unit alignment gating), plus tests.

### Stage 5 — comparison operator expansion

**Goal:** Rewrite `date_trunc(unit, x) < d`, `<= d`, `> d`, `>= d` (and `IS NOT DISTINCT FROM` if practical), not just `= d`.

**Scope:**

| Comparison | Rewrite (for finite, aligned `d`) |
|------------|-----------------------------------|
| `=` | `x >= d AND x < d + step` (Stage 1) |
| `<` | `x < d` |
| `<=` | `x < d + step` |
| `>` | `x >= d + step` |
| `>=` | `x >= d` |
| `IS DISTINCT FROM` / `IS NOT DISTINCT FROM` | requires `DistinctExpr` handling, separate dispatch path; may be deferred |

- Attach the same support function to the timestamp/timestamptz `<`, `<=`, `>`, `>=` functions (and their `_date` cross-type variants where they exist).
- Dispatch on operator-kind in `BucketLhsInfo` lookup.
- Reuse the `BucketUnitInfo`-driven alignment proofs from Stage 4.

**Risk:** alignment-proof requirements differ subtly between comparisons. `=` requires `d` exactly on a boundary; `<` requires the same; `<=` allows `d` to be on a boundary, where `x = d` exact match is included. Spec each comparison explicitly with proofs in code comments.

**Dependencies:** Stages 3 and 4.

**Estimated commit size:** ~250 lines plus tests.

### Stage 6 — generic infrastructure: `SupportRequestRangeExpansion`

**Goal:** Generalise the mechanism so other bucketing functions (`time_bucket` from extensions, `floor` on numerics, `width_bucket`, `substr` for fixed prefixes) can plug in without each one paying the full prosupport-on-eq cost.

**Scope:**
- Add `SupportRequestRangeExpansion` to `src/include/nodes/supportnodes.h`:

```c
typedef struct SupportRequestRangeExpansion {
    NodeTag       type;
    PlannerInfo  *root;
    FuncExpr     *fcall;         /* the bucketing call */
    /* outputs */
    Expr         *bucket_start;  /* expression giving start of bucket for fcall's arg */
    Expr         *bucket_step;   /* step interval as an expression */
    bool          finite_only;   /* rewrite invalid for infinite inputs */
} SupportRequestRangeExpansion;
```

- Modify `eval_const_expressions_mutator`'s `OpExpr` case (or a new pre-pass in `optimizer/util/expansion.c`) to, when a comparison operator's argument is a FuncExpr whose prosupport answers `SupportRequestRangeExpansion`, build the comparison rewrite directly.
- `date_trunc`'s prosupport (newly introduced — Stage 6 also adds a prosupport to `date_trunc` itself) answers the new request type, declaring its bucket semantics in a unit-driven way.
- Remove the per-comparison-operator prosupport entries added in Stage 5; the new infrastructure subsumes them.

**Risk:** new core infrastructure; needs strong justification — typically at least three concrete callers before the abstraction earns its keep. Candidates: `date_trunc` (in-tree), `time_bucket` (TimescaleDB), `floor` on numeric (would generalise `floor(x) = n` → `x >= n AND x < n+1`).

**Dependencies:** Stage 5 (so we have data on real per-comparison-operator workload before deciding the generic infrastructure is justified).

**Estimated commit size:** ~600 lines, including the new request type, the consumption path, the producer in `date_trunc`, and the migration of Stages 1–5 onto the new mechanism.

**Risk of deferral:** if Stage 6 is never landed, Stages 1–5 are still useful as standalone wins; only the generalisation to non-`date_trunc` bucketing is lost.

---

## 8. Open questions

1. **(Resolved.)** `timestamptz` Const at midnight across timezones. Investigation confirmed the asymmetry is real and unfixable at plan time (a cached generic plan freezes the folded instant while the STABLE original re-truncates under the execute-time zone), so the same-type timestamptz path was **dropped** — see §7 Stage 2. The natural `date_trunc('day', tstz) = '2020-01-01'` form (bare literal → timestamptz) is consequently left un-rewritten; `= DATE '...'` gives the timezone-safe cross-type rewrite.

2. **Should we expose `enable_date_trunc_rewrite` (or `enable_bucket_rewrite`)?** Aligns with `enable_*` family. Useful for testing and as an escape hatch. Argument against: the rewrite is exact (Stage 1) and the GUC adds maintenance burden. Lean: no GUC until Stage 4+ when the surface area grows.

3. **`'quarter'`, `'decade'`, `'century'`, `'millennium'` in Stage 4?** `quarter` plausible; the rest are esoteric and add OID/test burden for vanishingly small benefit.

4. **(Closed.)** Earlier drafts proposed an OR-guard form to defend Var-RHS against runtime infinity. Stage 1 instead relies on the deployment assumption documented in §3.2 and emits the plain range unconditionally. The defensive variant is now tracked as Open Question 10.

5. **Should the new request type (Stage 6) live in `supportnodes.h` or in a new `expansionnodes.h`?** A shallower header graph argues for a dedicated file; consistency with existing prosupport requests argues for the same file.

6. **Pushdown to FDW (`postgres_fdw`).** The rewritten range is shippable, but the original `date_trunc` form is also shippable — does FDW already use the index on the remote side? Need to test Stage 1 against a `postgres_fdw` setup.

7. **Plan cache reuse with parameterised RHS.** A prepared statement `SELECT * FROM t WHERE date_trunc('day', ts) = $1` (param of type `date`) — does the Stage 1 rewrite fire? It depends on whether the parameter is a `Param` node or a `Var` after parsing; Stage 1 only matches Var/FuncExpr/Const cases explicitly.

8. **Cooperation with planner statistics on expression indexes.** If the user has `CREATE INDEX ON events ((date_trunc('day', ts)))`, the index condition `date_trunc('day', ts) = d` *does* match the index today. After our rewrite, the predicate no longer references `date_trunc`, and the expression index becomes unusable for this query. Need to either: (a) detect the presence of such an index and skip the rewrite, or (b) accept that users with expression indexes are deliberately bypassing the rewrite-friendly form. Probably (b), with documentation.

9. **Lean-ness pass on the Stage 1 function body.** A line-by-line review surfaced ~15 lines of dead defensive scaffolding inside `date_trunc_eq_support()`: `= NULL` initialisers on locals that no reachable path reads, a single-use `Oid eqfuncid` that can be inlined into the switch, and a single-use `Node *ret` that could be returned directly. (The earlier finding about the dead `is_date_trunc(right)` branch is no longer applicable — with the reverse-direction operators now wired, that branch carries traffic for `F_DATE_EQ_TIMESTAMP` / `F_DATE_EQ_TIMESTAMPTZ`.) Schedule alongside Stage 2 or as a standalone cleanup commit.

10. **Defensive variant for schemas that admit `±infinity` dates.** Stage 1 deliberately omits any runtime guard against infinity operands, relying on the deployment assumption in §3.2. A defensive variant — suitable for in-tree submission to pgsql-hackers, where no such assumption can be made about every user's data — would reinstate one of two patterns we explored before settling on the current shape:

    (a) **Const-RHS bail** — if `d` is a `Const` and `DATE_NOT_FINITE(...)` holds, return NULL and leave the original OpExpr in place. Cheap and exact for the literal-RHS case. Does nothing for Var/Param RHS.

    (b) **OR-guarded form** — emit `(x >= lower AND x < upper) OR (x = lower)`. The eq arm is logically redundant for finite operands and exact for `x = d = ±infinity`. Costs one comparison per row when the planner cannot prove the OR redundant; in B-tree index plans, may produce a `BitmapOr` of two scans instead of a single range scan.

    A pgsql-hackers-ready submission would probably combine (a) for Const RHS with (b) for non-Const RHS. The decision tree: when is the runtime cost of (b) acceptable vs the loss of rewrite for some Var/Param queries if (b) is omitted?

---

## 9. References

- `src/backend/utils/adt/like_support.c` — the prosupport pattern this work follows.
- `src/include/nodes/supportnodes.h` — the existing `SupportRequest*` types.
- `src/backend/optimizer/util/clauses.c`, `simplify_function()` — dispatch into prosupport.
- PostgreSQL documentation: ["Function Volatility Categories"](https://www.postgresql.org/docs/current/xfunc-volatility.html).
- pg_architect review (`/Users/danolivo/pgedge/pgdev-2`, session transcript, 2026-05-14) — design critique and corner-case enumeration that shaped Stages 2–6 of this spec.
- pgsql-hackers archives on the introduction of `prosupport` and the `text_starts_with_support` rewrite (2018–2022).

---

**Summary table — what ships when**

| Stage | Status | Deliverable | LOC | Depends on |
|-------|--------|-------------|-----|-----------|
| 1 | ✅ implemented | `date_trunc('day', x) = d` rewrite; relies on schema-level exclusion of infinity (§3.2) | ~220 | — |
| 2 | ✅ implemented | Const **timestamp** at midnight RHS (same-type); timestamptz same-type excluded (plan-cache/timezone) | ~70 | 1 |
| 3 | proposed | `bucket_expansion.c` + table-driven dispatch | ~400 | 1, 2 |
| 4 | ✅ implemented | Units `week`, `month`, `quarter`, `year` (Const-aligned RHS) | ~90 | 1, 2 |
| 5 | proposed | Comparison ops `<`, `<=`, `>`, `>=` | ~250 | 3, 4 |
| 6 | proposed | `SupportRequestRangeExpansion` generic infra | ~600 | 5 |

Stages 1–4 alone deliver the bulk of user-visible benefit. Stage 5 doubles the predicate coverage. Stage 6 is gated on the appearance of a second in-tree caller and may be deferred indefinitely without losing earlier value.
