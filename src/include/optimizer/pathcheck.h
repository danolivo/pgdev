/*-------------------------------------------------------------------------
 *
 * pathcheck.h
 *	  Single-pathlist invariant tracker (assert-only).
 *
 * This is an internal core facility, active only when USE_ASSERT_CHECKING
 * is defined.  None of these symbols exist in a non-assert build, so it
 * is harmless that the header is installed alongside the rest of the
 * optimizer headers.
 *
 * Contract:
 *	A given Path pointer may appear in at most one of
 *	{RelOptInfo.pathlist, RelOptInfo.partial_pathlist} at any moment of
 *	a single planner invocation.  The membership hash is allocated in
 *	root->planner_cxt at the top of subquery_planner and torn down at
 *	the bottom; recursive subquery_planner invocations save and restore
 *	the file-static so each PlannerInfo sees its own hash.  Because
 *	every Path the planner can record lives in planner_cxt or a child
 *	thereof, hash entries cannot outlive the Paths they reference, and
 *	path_membership_forget may safely Assert(found).
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/optimizer/pathcheck.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PATHCHECK_H
#define PATHCHECK_H

#ifdef USE_ASSERT_CHECKING

#include "nodes/pathnodes.h"

extern void pathcheck_planner_init(PlannerInfo *root);
extern void pathcheck_planner_fini(void);

extern void path_membership_record(const Path *path, const RelOptInfo *rel);
extern void path_membership_forget(const Path *path);
extern void pathcheck_forget_list(const List *paths);

#endif							/* USE_ASSERT_CHECKING */

#endif							/* PATHCHECK_H */
