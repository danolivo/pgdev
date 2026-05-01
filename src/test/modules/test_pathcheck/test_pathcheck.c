/*-------------------------------------------------------------------------
 *
 * test_pathcheck.c
 *		Test code for the assert-only single-pathlist invariant tracker.
 *
 * Provides SQL-callable functions that install a set_rel_pathlist hook
 * which re-presents the cheapest existing path on each base rel back to
 * add_path().  Under USE_ASSERT_CHECKING this trips the membership-tracker
 * elog in pathcheck.c.  test_pathcheck_disable() removes the hook so the
 * rest of the suite can run normally.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/modules/test_pathcheck/test_pathcheck.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "nodes/pathnodes.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "utils/builtins.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(test_pathcheck_enable);
PG_FUNCTION_INFO_V1(test_pathcheck_disable);

static set_rel_pathlist_hook_type prev_set_rel_pathlist_hook = NULL;
static bool hook_installed = false;

static void
test_pathcheck_hook(PlannerInfo *root, RelOptInfo *rel, Index rti,
					RangeTblEntry *rte)
{
	if (prev_set_rel_pathlist_hook)
		(*prev_set_rel_pathlist_hook) (root, rel, rti, rte);

	/*
	 * Re-present the cheapest existing path.  Under USE_ASSERT_CHECKING the
	 * pathcheck tracker has it recorded already, so add_path() will trip the
	 * "already present" elog at the membership-record step.
	 */
	if (rel->pathlist != NIL)
	{
		Path	   *p = (Path *) linitial(rel->pathlist);

		add_path(rel, p);
	}
}

Datum
test_pathcheck_enable(PG_FUNCTION_ARGS)
{
	if (!hook_installed)
	{
		prev_set_rel_pathlist_hook = set_rel_pathlist_hook;
		set_rel_pathlist_hook = test_pathcheck_hook;
		hook_installed = true;
	}
	PG_RETURN_VOID();
}

Datum
test_pathcheck_disable(PG_FUNCTION_ARGS)
{
	if (hook_installed)
	{
		set_rel_pathlist_hook = prev_set_rel_pathlist_hook;
		prev_set_rel_pathlist_hook = NULL;
		hook_installed = false;
	}
	PG_RETURN_VOID();
}
