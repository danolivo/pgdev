# Code Review: MergeAppend Explicit Sort Feature

## Overview
This document contains a critical analysis of the MergeAppend explicit sort optimization patch from branch `mergeappend-explicit-sort-1`.

---

## CRITICAL BUGS

### Bug #1: Inconsistent use of cheapest_total_path regardless of cost criterion
**Severity:** HIGH
**Location:** `src/backend/optimizer/path/pathkeys.c:672`

**Problem:**
```c
Path *base_path = rel->cheapest_total_path;  // ALWAYS uses total cost
```

The function accepts a `cost_criterion` parameter (STARTUP_COST or TOTAL_COST) but ALWAYS uses `cheapest_total_path`. When `cost_criterion == STARTUP_COST`, it should use `cheapest_startup_path` instead.

**Impact:** Suboptimal plans when optimizing for startup cost (e.g., queries with LIMIT).

**Recommended Fix:**
```c
Path *base_path = (cost_criterion == STARTUP_COST) ?
                  rel->cheapest_startup_path :
                  rel->cheapest_total_path;
```

---

### Bug #2: Confusing return semantics when path == NULL
**Severity:** MEDIUM
**Location:** `src/backend/optimizer/path/pathkeys.c:690-697`

**Problem:**
```c
if (path == NULL)
    /*
     * Current pathlist doesn't fit the pathkeys. No need to check extra
     * sort path ways.
     */
    return base_path;
```

When no path satisfies the pathkeys, the function returns `base_path` (which is UNSORTED). The comment is misleading - it says "No need to check extra sort path ways" when it actually means "we're returning an unsorted path that will need sorting".

**Impact:** Confusing code that's hard to maintain. Works correctly but only because the caller checks pathkeys again.

**Recommendation:** Clarify comment or refactor to make the semantics clearer.

---

### Bug #3: Redundant cost comparison when base_path already satisfies pathkeys
**Severity:** LOW
**Location:** `src/backend/optimizer/path/pathkeys.c:700-741`

**Problem:**
```c
if (path != base_path)
{
    if (!pathkeys_count_contained_in(pathkeys, base_path->pathkeys, &presorted_keys))
    {
        // Calculate sort cost
    }
    else
    {
        // BUG: base_path already satisfies pathkeys!
        sort_path.rows = base_path->rows;
        sort_path.startup_cost = base_path->startup_cost;
        sort_path.total_cost = base_path->total_cost;
    }

    if (compare_path_costs(&sort_path, path, cost_criterion) < 0)
        return base_path;
}
```

If `base_path` already satisfies pathkeys, we're setting sort costs equal to base costs, then comparing. This is redundant.

**Impact:** Minor performance overhead, confusing logic.

**Recommendation:** If base_path satisfies pathkeys AND is cheaper, return immediately.

---

## LOGIC ISSUES

### Issue #1: Asymmetric handling between total and fractional cost functions
**Severity:** MEDIUM
**Locations:**
- `src/backend/optimizer/path/pathkeys.c:739` (total)
- `src/backend/optimizer/path/pathkeys.c:869` (fractional)

**Problem:**
```c
// Total path version:
if (compare_path_costs(&sort_path, path, cost_criterion) < 0)

// Fractional version:
if (compare_fractional_path_costs(&sort_path, path, fraction) <= 0)  // Note: <=
```

One uses `< 0`, the other uses `<= 0`. This means:
- Total path: prefer ordered path when costs are equal
- Fractional path: prefer unordered+sort when costs are equal

**Question:** Is this intentional? If yes, needs a comment explaining why.

---

### Issue #2: Behavior when ALL subpaths are unordered
**Severity:** MEDIUM
**Location:** `src/backend/optimizer/path/allpaths.c:2105-2119`

**Problem:**
```c
if (startup_has_ordered)
    add_path(rel, (Path *) create_merge_append_path(...));
```

When ALL partitions lack naturally ordered paths, `startup_has_ordered == false` and NO MergeAppend is created. The planner falls back to `Sort -> Append`.

**Question:** Is this the intended behavior? Should we still consider MergeAppend with explicit sorts on ALL children?

**Recommendation:** Add a comment explaining this design decision, or add test case verifying correctness.

---

## CODE QUALITY ISSUES

### Issue #1: Typo in comment
**Location:** `src/backend/optimizer/path/pathkeys.c:656`

```c
 * satisfies defined criterias and сonsiders one more option:
                   ^^^^^^^^^        ^^^^^^^^^
```

- "criterias" should be "criteria" (already plural)
- "сonsiders" has a Cyrillic 'с' instead of Latin 'c'

---

### Issue #2: Significant code duplication
**Severity:** MEDIUM

The functions `get_cheapest_path_for_pathkeys_ext` and `get_cheapest_fractional_path_for_pathkeys_ext` have 98% identical code (90 lines duplicated).

**Impact:** Maintenance burden - any bug fix must be applied twice.

**Recommendation:** Extract common logic into a helper function:
```c
static Path *consider_explicit_sort(PlannerInfo *root, RelOptInfo *rel,
                                   Path *sorted_path, Path *base_path,
                                   List *pathkeys, ...)
```

---

### Issue #3: Missing Assert for cheapest_startup_path
**Location:** `src/backend/optimizer/path/pathkeys.c:672`

If Bug #1 is fixed to use `cheapest_startup_path` when appropriate, add:

```c
Assert(cost_criterion != STARTUP_COST || rel->cheapest_startup_path != NULL);
```

---

## PERFORMANCE CONCERNS

### Concern #1: Cost calculation overhead
For each child relation, we perform full sort cost calculations even if the sorted path will ultimately be chosen. With 100+ partitions, this overhead accumulates.

**Recommendation:** Profile with large partition counts. Consider early-exit optimization if sorted path is significantly cheaper.

---

### Concern #2: No consideration of parallel workers
The `sort_path` cost calculations use default parallelism. If `base_path` uses parallel workers but `path` doesn't (or vice versa), cost comparison may be inaccurate.

**Recommendation:** Verify that cost_sort/cost_incremental_sort accounts for parallel execution context.

---

## TEST COVERAGE GAPS

### Gap #1: No test for STARTUP_COST criterion (Bug #1)
The regression tests don't specifically test startup cost optimization with this feature.

**Recommendation:** Add test with `LIMIT` clause to force startup cost optimization.

---

### Gap #2: No test for all-unordered partitions
What happens when NO partition has a naturally ordered path?

**Recommendation:** Add test verifying graceful fallback to `Sort -> Append`.

---

### Gap #3: No test for mixed scenarios
Current tests don't cover:
- Partition 0: has ordered path (use it)
- Partition 1: no ordered path (explicit sort cheaper)
- Partition 2: has ordered path but expensive (explicit sort cheaper)

**Recommendation:** Add comprehensive mixed-scenario test.

---

## SUMMARY OF RECOMMENDATIONS

### Must Fix (Critical)
1. **Bug #1:** Use correct `cheapest_*_path` based on `cost_criterion`

### Should Fix (Important)
2. **Bug #2:** Clarify comment or refactor return logic when `path == NULL`
3. **Bug #3:** Skip redundant comparison when `base_path` satisfies pathkeys
4. **Issue #1:** Document or fix asymmetric `<` vs `<=` comparison
5. **Code Quality:** Fix typos, reduce duplication

### Consider (Nice to Have)
6. **Issue #2:** Document behavior when all subpaths are unordered
7. **Performance:** Profile with many partitions
8. **Testing:** Improve edge case coverage

---

## OVERALL ASSESSMENT

**Correctness:** The feature appears to work correctly despite the bugs, because the caller validates pathkeys independently. However, Bug #1 (incorrect cost criterion) could cause suboptimal plans.

**Code Quality:** Moderate. Significant duplication and confusing comments hurt maintainability.

**Performance:** Likely positive impact overall, but overhead should be measured with large partition counts.

**Testing:** Adequate but could be more comprehensive for edge cases.

**Recommendation:** Fix Bug #1 before merging. Other issues can be addressed in follow-up patches.
