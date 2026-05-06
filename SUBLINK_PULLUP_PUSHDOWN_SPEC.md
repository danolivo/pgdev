# Pull-up SEMI/ANTI: graft adjacent to referenced rels

Status: in-development.
Branch: `claude/investigate-join-collapse-postgres-60Jms`.
Files touched: `src/backend/optimizer/prep/prepjointree.c`.

## 1. Goal

When a WHERE-level `EXISTS` / `IN` (`ANY`) subquery is pulled up into a
SEMI/ANTI join by `pull_up_sublinks`, splice the new `JoinExpr` **as close
as possible to the referenced base relation**, instead of always grafting
it at the outermost legal point. The headline benefit is that the SEMI/ANTI
join survives `join_collapse_limit` flattening as a tightly-coupled pair
with its referenced rel, so the planner can consider join orders like
`(T1 SEMI T20) ⋈ T2 ⋈ ...` for the motivating pattern

```sql
SELECT *
FROM   T1 JOIN T2 ON (...) JOIN T3 ON (...) ... JOIN T19 ON (...)
WHERE  EXISTS (SELECT 1 FROM T20 WHERE clause(T20, T1));
```

Today the SEMI sits above the entire 19-way join stack and `T20` is forced
to be joined last whenever the joinlist exceeds `join_collapse_limit`.

## 2. Where this lives

`pull_up_sublinks_qual_recurse` (`prepjointree.c`) calls
`convert_EXISTS_sublink_to_join` / `convert_ANY_sublink_to_join`, which
return a `JoinExpr` with `larg = NULL`. The current code grafts the result
at the outermost link with the canonical pattern

```c
j->larg     = *jtlink;
*jtlink     = (Node *) j;
```

This change replaces that two-line splice with a helper that picks a
deeper graft slot when one is safe.

Pull-up runs *before* `pull_up_subqueries`, `flatten_simple_union_all`,
`reduce_outer_joins`, and `deconstruct_jointree`. At pull-up time the
jointree still contains the user's explicit `JoinExpr` skeleton, which
is exactly what we need to descend through.

## 3. Algorithm

Inputs at the splice point:
- `j`     : the new SEMI/ANTI `JoinExpr` returned by `convert_*`.
- `jtlink`: `Node **` to the slot in the jointree where `j` would graft today.
- `available_rels`: the relids visible (and non-nullable) at this qual.

Steps:

1. Compute the SEMI's outer-side references:
   `target = pull_varnos(root, j->quals) ∩ available_rels`.

   The intersection drops the rarg's freshly-added RTE, leaving exactly
   the parent-query relids the SEMI's quals reference.

2. Walk down from `*jtlink`, descending only through **non-nullable**
   sides, looking for the deepest slot whose subtree relids ⊇ `target`:

   | node type     | descend into                                 |
   |---------------|----------------------------------------------|
   | `RangeTblRef` | -- (cannot go deeper)                        |
   | `FromExpr`    | the (single) child whose relids ⊇ `target`   |
   | `JOIN_INNER`  | larg or rarg (whichever covers `target`)     |
   | `JOIN_LEFT`   | larg only (rarg is nullable)                 |
   | `JOIN_RIGHT`  | rarg only (larg is nullable)                 |
   | `JOIN_FULL`   | -- (both sides nullable)                     |
   | `JOIN_SEMI` / `JOIN_ANTI` | larg only (rarg is the existence side) |

   If no single child covers `target`, the descent stops at the current
   slot. The fallback is identical to today's behaviour.

3. Splice: replace the chosen slot `*new_link` with the SEMI:

   ```c
   j->larg    = *new_link;
   *new_link  = (Node *) j;
   ```

4. The subsequent `pull_up_sublinks_qual_recurse` calls on `j->quals`
   (handling sublinks nested within the SEMI's join condition) continue
   to use `&j->larg` and `&j->rarg` as their two jtlinks. `j->larg` now
   points to a smaller subtree, but `available_rels` for that recursion
   is the new subtree's relids — strictly a subset of the previous
   `available_rels`, so any nested pull-up that succeeds was already
   guaranteed legal under the old rules. Anything that *would* have
   needed the wider scope simply fails to pull up and stays as a SubLink.

## 4. Why the nullability rule is sufficient

`available_rels` passed into `pull_up_sublinks_qual_recurse` is, by
construction, the set of rels non-nullable at this qual's vantage point
(see `prepjointree.c:740, 791-811`). The descent above only enters
non-nullable children of each enclosing join — i.e. it never crosses
*into* a nullable side. So every rel in the chosen subtree is
non-nullable in the qual's surrounding scope, which is exactly the
invariant the existing code maintains for the outermost graft.

This matches the wider planner rule "do not push WHERE-clause filters
into the nullable side of an outer join."

## 5. What the change does *not* do

- Does not introduce a GUC. The behaviour is unconditionally an
  improvement (or a no-op) under the safety rules above, so making it
  optional would just add noise.
- Does not re-examine outer joins post-`reduce_outer_joins`. We only see
  the user-written join types and conservatively leave LEFT/RIGHT
  reductions on the table.
- Does not move existing top-of-stack quals or restrict optimization
  to large joins; the rule fires for any size, but is observably useful
  only when `join_collapse_limit` would otherwise wedge the SEMI at the
  top.
- Does not handle the `under_not = true` ANTI-join case differently —
  same descent, same nullability rule, same splice.

## 6. Simplifications in the v1 implementation

- **Single descent, no backtracking.** If `target` straddles two siblings
  of a `FromExpr` (or two sides of an inner join), we stop and graft at
  the current level. We do not try to find a smaller-but-still-covering
  ancestor inside one branch.
- **No relids cache.** We call `get_relids_in_jointree` on each candidate
  child during descent. Jointrees at this stage are bounded by user
  query depth; the cost is negligible compared to the planner work it
  enables.
- **No special handling for laterals introduced by ANY-pullup.** The
  ANY pullup wraps the rarg in a subquery RTE that may be marked
  `lateral`. Lateral references still resolve correctly: the SEMI's
  quals are evaluated where the SEMI sits, and any rels they reference
  are in the SEMI's larg subtree (which we placed adjacent to them by
  construction).
- **Conservative on `JOIN_SEMI` / `JOIN_ANTI` encountered during descent.**
  These shouldn't normally appear in the jointree before pull-up runs,
  but if they do (e.g. recursive pull-up after subquery flattening) we
  refuse to descend into the rarg.

## 7. Test plan

- Existing regression suite must pass. Plan changes are expected for
  queries whose SEMI placement was load-bearing in EXPLAIN output; we
  update those expected outputs as needed.
- Add a focused case in `subselect.sql` that exercises the
  `T1 .. T19 + EXISTS(T20 referencing T1)` pattern with
  `join_collapse_limit` set low, demonstrating the new join order.

## 8. Open questions / follow-ups

- Whether to attempt re-running `pull_up_sublinks` after
  `reduce_outer_joins` to catch SEMI placements that became safe once
  outer joins were proven inner. Out of scope for v1.
- Whether the same descent should apply to the `jtlink2` case (sublinks
  pull-uppable against a second relid set during outer-join qual
  processing). v1 reuses the helper there too — the safety argument is
  identical.
