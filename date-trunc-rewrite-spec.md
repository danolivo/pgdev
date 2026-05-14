# Technical specification: planner rewrite for `date_trunc('unit', x)` comparisons

**Author:** Andrei V. Lepikhov
**Status:** Draft (Stage 1 implemented; Stages 2–6 proposed)
**PostgreSQL target version:** PG20
**Discussion thread:** (not yet posted to pgsql-hackers)
**Related patches:** in `pgdev-2`, branch `relocate-subplan`
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
   │  date_trunc_day_eq_support()  ── Stage 1            │
   │  later: bucket_expansion_eq_support()  ── Stage 3+  │
   │                                                     │
   │  1. dispatch on opfuncid → BucketLhsInfo row        │
   │  2. find date_trunc(unit, x) on either side         │
   │  3. decode unit → BucketUnitInfo row                │
   │  4. discharge alignment proof on RHS                │
   │  5. discharge finite-RHS proof                      │
   │  6. emit half-open range  OR  OR-guarded form       │
   │  7. return Node*, replaces OpExpr                   │
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
2. **Finite RHS.** `d` is not `±infinity`. Otherwise the range `[d, d + step)` is empty (`infinity + interval = infinity`) and the rewrite drops rows that the original predicate matches.
3. **Finite step.** `bucket_step(unit)` is finite for the unit. Trivially true for all `date_trunc` units; included for completeness because Stage 6 admits user-defined bucketing functions.

When (1) holds but (2) cannot be discharged at plan time (e.g. RHS is a `Var`), Stage 1 emits the **OR-guarded** form:

```
(x >= bucket_start(d) AND x < bucket_start(d) + step) OR (x = bucket_start(d))
```

The eq arm is logically redundant for finite `d` (already covered by the range) and exact for infinite `d` (where the range is empty).

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
| `SET enable_*` flags | none added in Stage 1; see Open Question 9.2 |

### 3.6 Correctness hazards and mitigations

| Hazard | Mitigation (Stage 1) | Open in later stages |
|--------|----------------------|----------------------|
| `±infinity` RHS makes range empty | early bail for Const-infinite; OR-guard for non-Const | — |
| Const-folded `'2024-01-01'::timestamp` is not a date-cast FuncExpr | accepted limitation; rewrite skipped | addressed in Stage 2 |
| Timezone change between plan and execution | rewrite uses STABLE `timestamptz_pl_interval`; same evaluation-time semantics as original | — |
| DST transition days have 23 or 25 wall-clock hours | `interval '1 day'` on `timestamptz` is calendar-correct; matches original | — |
| Volatility downgrade: `timestamptz_eq` IMMUTABLE → result STABLE | documented in source comment; no practical effect on cached plans (already plan-time-evaluable for Const, deferred for Var either way) | — |
| Selectivity flip: `eqsel` → `scalargesel × scalarltsel` | usually a sharper estimate; rare adversarial cases possible | covered by Stage 1 regression sweep |
| `IS NOT DISTINCT FROM` not handled (parses as `DistinctExpr`) | documented limitation | Stage 5 may cover |
| Generated column shortcut already exists | rewrite is no-op when LHS is a Var, not a date_trunc call | — |
| Plan-cache reuse across `SET TIME ZONE` | identical to pre-rewrite behaviour for `timestamptz`-typed quals | — |

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
- **OR-guard cost on Var-RHS.** The Stage 1 OR-guard form adds a per-row equality test for Var RHS. For B-tree indexes on `ts`, the planner may choose a `BitmapOr` of two scans rather than one range scan; this is wider work even though still indexed. Stage 3 proposes narrowing the OR-guard scope.

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

New SQL test additions:

| File | Coverage |
|------|----------|
| `src/test/regress/sql/timestamp.sql` | core rewrite proof: EXPLAIN shape, NULL handling, infinity handling, finite-Const elision of OR-guard |
| `src/test/regress/sql/timestamptz.sql` | timestamptz variant; DST-day behaviour under `SET TIME ZONE` |
| `src/test/regress/sql/create_index.sql` | index-use proof (Bitmap Index Scan emitted) |
| `src/test/regress/sql/partition_prune.sql` | partition pruning proof on a range-partitioned table |

### 6.2 Edge cases explicitly tested

- `date_trunc('day', ts) = d` with `d` finite, `d = 'infinity'`, `d = '-infinity'`, `d = NULL`.
- Cross-type cases: `(timestamp, date)`, `(timestamptz, date)`.
- Same-type cases via explicit cast: `(timestamp, date::timestamp)`, `(timestamptz, date::timestamptz)`.
- Unit decoding: `'day'`, `'DAY'`, `'Day'`, `'days'` (an alias accepted by `DecodeUnits`).
- Non-day units (`'hour'`, `'month'`, etc. — Stage 1 must leave these alone).
- Const-folded RHS (`= '2024-01-01'::timestamp` — Stage 1 must leave alone; Stage 2 must rewrite).
- LHS is a Const (already constant-folded; rewrite must be a no-op).
- `IS NOT DISTINCT FROM` (must be left alone in all stages until Stage 5 covers it).
- Hostile input: `date_trunc('day', NULL)`, `date_trunc('garbage', ts)`.

### 6.3 No TAP tests required

This is a planner-only feature with no multi-process interactions. SQL regression tests are sufficient.

### 6.4 Performance regression sweep

Before each commit, run `make check-world` and inspect the buildfarm output for plan changes in `partition_prune`, `join_hash`, `select_parallel`, and any other test that touches `date_trunc`. Expected: zero unintended plan changes outside the new test files.

---

## 7. Implementation plan

The work breaks into six stages. Stage 1 is implemented in the current branch (`relocate-subplan`). Stages 2–6 are proposed and ordered by dependency.

### Stage 1 — minimum viable rewrite ✅ implemented

**Scope (delivered):**
- `date_trunc_day_eq_support()` in `src/backend/utils/adt/timestamp.c`, after `interval_support()`.
- Attached via `pg_proc.dat` (OID 8773) to four equality functions: `timestamp_eq` (OID 2052), `timestamptz_eq` (OID 1152), `timestamp_eq_date` (OID 2366), `timestamptz_eq_date` (OID 2379).
- Handles unit `'day'` only.
- RHS policy: bare `date` (cross-type eq) or `date_timestamp` / `date_timestamptz` cast FuncExpr (same-type eq).
- Infinity safety: bail for Const-infinite RHS; OR-guard for non-Const RHS.
- Verified: B-tree index use on Const-RHS, correct counts under all RHS finiteness/type combinations, no regression on non-matching cases.

**Known limitations carried into later stages:**
- Const-folded `'2024-01-01'::timestamp` is not recognised.
- Only `=` is rewritten; `<`, `<=`, `>`, `>=` are not.
- Only `'day'` is supported.
- OR-guard on Var-RHS slightly fattens plans for predominantly-finite data.

**Estimated commit size:** ~300 lines including comments.

### Stage 2 — widen RHS proof to midnight-aligned Const timestamps

**Goal:** Catch the most common user-written form, `date_trunc('day', ts) = '2024-06-15'` (which the parser coerces to `(timestamp, timestamp)` eq with a Const timestamp RHS — currently rejected).

**Scope:**
- Extend the RHS classifier in `date_trunc_day_eq_support()` to accept a `Const` of type `timestamp` or `timestamptz` whose value satisfies "time-of-day is exactly 00:00:00 in the relevant zone" and is finite.
- For `timestamp` Const: check `Timestamp2tm` decomposition for hour=min=sec=usec=0.
- For `timestamptz` Const: check decomposition *in the current session timezone* — note this means the rewrite for `timestamptz` Const RHS is sound only at the timezone in which the Const was originally folded. The planner already evaluated the Const under the planning-time timezone, so the value is anchored. Verify behaviour under `SET TIME ZONE` changes between PREPARE and EXECUTE in a TAP test.
- The OR-guard is no longer needed for this class of RHS (Const finite, alignment proven), so emit the plain range.

**Risk:** Const timestamptz at midnight in zone A may not be at midnight in zone B. If the timezone changes between plan and execution, the original predicate's result also changes — so the rewrite remains semantically equivalent. The subtlety is that the *finite-Const-no-OR-guard* assumption may need to be loosened for `timestamptz` Const if we want plan-cache robustness across timezone changes. Open question 9.1.

**Dependencies:** none (operates on Stage 1 codebase).

**Estimated commit size:** ~80 lines plus tests.

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

- Refactor `date_trunc_day_eq_support()` into a thin shell that does `(opfuncid → BucketLhsInfo) × (unit → BucketUnitInfo)` lookup and delegates to `try_bucket_rewrite_eq()`.
- Add `src/include/utils/bucket_expansion.h` exposing the support function declaration only (no internal types).
- Update `pg_proc.dat` `prosupport` references (or — preferred — leave them pointing at the same `proname`, which we keep). No catalog change to OIDs.

**Risk:** behaviour-preserving refactor; review burden minimal if diffed against Stage 1.

**Dependencies:** Stages 1, 2 (Stage 2 lands in the same file; refactoring afterwards is cleaner).

**Estimated commit size:** ~400 lines (mostly code movement).

### Stage 4 — unit expansion (`hour`, `month`, `year`, `week`)

**Goal:** Generalise to other commonly used units. Each unit needs:
- a step interval (`'1 hour'`, `'1 month'`, etc.);
- an alignment-proof function specific to the unit;
- regression test coverage for both Const-RHS and Var-RHS shapes.

**Scope per unit:**

| Unit | Step | Alignment proof (Const RHS) | Notes |
|------|------|----------------------------|-------|
| `hour` | `interval '1 hour'` | minute = second = usec = 0 | timestamp[tz] Const required; date doesn't express hour boundaries |
| `month` | `interval '1 month'` | day = 1, hour=min=sec=usec=0 | step is calendar-correct via Interval semantics |
| `year` | `interval '1 year'` | month = 1 (Jan), day = 1, time-of-day = 0 | — |
| `week` | `interval '1 week'` | ISO week start: Monday, time-of-day = 0 | ISO 8601 weeks; mismatches with `date_trunc('week', ...)`'s actual behaviour need verification |

**Risk:** unit semantics in `date_trunc` are subtler than they look (especially `'week'` and `'quarter'`). Each unit requires its own correctness review against the `timestamp_trunc` source. Cross-reference with `DecodeUnits` to ensure unit aliases are handled.

**Open question:** should `'quarter'`, `'decade'`, `'century'`, `'millennium'` be in scope? The first probably yes; the latter three are theoretically supportable but pragmatically pointless.

**Dependencies:** Stage 3 (table-driven dispatch).

**Estimated commit size:** ~100 lines per unit, plus tests.

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

1. **`timestamptz` Const at midnight across timezones (Stage 2).** A Const folded at plan time encodes a wall-clock instant in *some* timezone. If the session timezone changes between plan and execution, the original predicate's truth value also changes — but is the rewritten form's behaviour identical, or is there an asymmetry we miss?

2. **Should we expose `enable_date_trunc_rewrite` (or `enable_bucket_rewrite`)?** Aligns with `enable_*` family. Useful for testing and as an escape hatch. Argument against: the rewrite is exact (Stage 1) and the GUC adds maintenance burden. Lean: no GUC until Stage 4+ when the surface area grows.

3. **`'quarter'`, `'decade'`, `'century'`, `'millennium'` in Stage 4?** `quarter` plausible; the rest are esoteric and add OID/test burden for vanishingly small benefit.

4. **OR-guard scope in Stage 3 refactor.** Currently the Stage 1 OR-guard is emitted whenever RHS is non-Const. After Stage 3 we could narrow it to "RHS is a Var of a type that admits infinity" (i.e. `date`, `timestamp`, `timestamptz`), eliding it for, say, parameter symbols (which the executor can examine for finiteness at first-execution). Open: how much win is recoverable?

5. **Should the new request type (Stage 6) live in `supportnodes.h` or in a new `expansionnodes.h`?** A shallower header graph argues for a dedicated file; consistency with existing prosupport requests argues for the same file.

6. **Pushdown to FDW (`postgres_fdw`).** The rewritten range is shippable, but the original `date_trunc` form is also shippable — does FDW already use the index on the remote side? Need to test Stage 1 against a `postgres_fdw` setup.

7. **Plan cache reuse with parameterised RHS.** A prepared statement `SELECT * FROM t WHERE date_trunc('day', ts) = $1` (param of type `date`) — does the Stage 1 rewrite fire? It depends on whether the parameter is a `Param` node or a `Var` after parsing; Stage 1 only matches Var/FuncExpr/Const cases explicitly.

8. **Cooperation with planner statistics on expression indexes.** If the user has `CREATE INDEX ON events ((date_trunc('day', ts)))`, the index condition `date_trunc('day', ts) = d` *does* match the index today. After our rewrite, the predicate no longer references `date_trunc`, and the expression index becomes unusable for this query. Need to either: (a) detect the presence of such an index and skip the rewrite, or (b) accept that users with expression indexes are deliberately bypassing the rewrite-friendly form. Probably (b), with documentation.

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
| 1 | ✅ implemented | `date_trunc('day', x) = d` rewrite; infinity-safe | ~300 | — |
| 2 | proposed | Const timestamp[tz] at midnight RHS | ~80 | 1 |
| 3 | proposed | `bucket_expansion.c` + table-driven dispatch | ~400 | 1, 2 |
| 4 | proposed | Units `hour`, `month`, `year`, `week` | ~400 | 3 |
| 5 | proposed | Comparison ops `<`, `<=`, `>`, `>=` | ~250 | 3, 4 |
| 6 | proposed | `SupportRequestRangeExpansion` generic infra | ~600 | 5 |

Stages 1–4 alone deliver the bulk of user-visible benefit. Stage 5 doubles the predicate coverage. Stage 6 is gated on the appearance of a second in-tree caller and may be deferred indefinitely without losing earlier value.
