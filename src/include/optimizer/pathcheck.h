/*-------------------------------------------------------------------------
 *
 * pathcheck.h
 *	  Single-pathlist invariant tracker (assert-only).
 *
 * Internal core facility, active only when USE_ASSERT_CHECKING is defined.
 * The header is installed alongside other src/include/optimizer/ headers
 * for build uniformity, but the symbols it declares only exist in assert
 * builds and are not part of the supported FDW or custom-scan ABI.
 * Out-of-core callers must not rely on these names.
 *
 * Contract:
 *	A given Path pointer may appear in at most one of
 *	{RelOptInfo.pathlist, RelOptInfo.partial_pathlist} at any moment of
 *	a single planner invocation.  The membership hash is allocated in
 *	root->planner_cxt at the top of subquery_planner and torn down at
 *	the bottom; recursive subquery_planner invocations save and restore
 *	the per-backend current-hash pointer so each PlannerInfo sees its
 *	own hash.  Because every Path the planner can record lives in
 *	planner_cxt or a child thereof, hash entries cannot outlive the
 *	Paths they reference, and path_membership_forget may safely
 *	Assert(found).
 *
 *	Two narrow handoff exceptions are documented in pathcheck.c.
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

extern void *pathcheck_subscope_enter(void);
extern void pathcheck_subscope_leave(void *saved);

extern void path_membership_record(const Path *path, const RelOptInfo *rel);
extern void path_membership_forget(const Path *path);
extern void pathcheck_forget_list(const List *paths);
extern void pathcheck_replace(const Path *old_path, const Path *new_path,
							  const RelOptInfo *rel);

#endif							/* USE_ASSERT_CHECKING */

#endif							/* PATHCHECK_H */
