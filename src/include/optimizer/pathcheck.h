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
 * Hook surface for extension authors: no contract change.  set_rel_pathlist_hook,
 * set_join_pathlist_hook, and create_upper_paths_hook all reach the tracker
 * through their add_path() / add_partial_path() calls, the same way core
 * does.  An extension that adds a freshly-built Path to one RelOptInfo per
 * call -- as the pre-existing API has always required -- needs no changes.
 *
 * Contract:
 *	A given Path pointer may appear in at most one of
 *	{RelOptInfo.pathlist, RelOptInfo.partial_pathlist} at any moment of
 *	a single planner invocation.  The membership hash is allocated in
 *	root->planner_cxt at the top of subquery_planner and torn down at
 *	the bottom; recursive subquery_planner invocations save and restore
 *	the per-backend current-hash pointer so each PlannerInfo sees its
 *	own hash.  Every Path the planner records lives in planner_cxt or
 *	a child thereof, so hash entries cannot outlive their referents.
 *
 *	Forget rule: membership is forgotten only when the Path is actually
 *	pfree'd -- today that is add_path()'s dominance prune for non-IndexPath
 *	old paths and add_partial_path()'s dominance prune (which always
 *	pfrees) -- plus two documented handoff sites where a Path pointer is
 *	legitimately re-added to a different RelOptInfo (grouping_planner
 *	final-paths and create_ordered_paths).  All other path-removal sites
 *	(rel->pathlist = NIL; lfirst(lc) = newpath) leave the hash entry in
 *	place: the underlying Path lives on in planner_cxt and its address
 *	cannot be reused as a different Path before planner_cxt is reset.
 *
 *	path_membership_forget() is tolerant of "not present", because the
 *	"lfirst(lc) = newpath" substitution sites install fresh Paths into
 *	pathlist cells without recording them; when add_path() later prunes
 *	such a cell, or a handoff iterates over it, the forget is a no-op.
 *	The collision check at path_membership_record() still fires
 *	elog(ERROR) on any genuine double-add, which is the property the
 *	tracker exists to enforce.
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

#endif							/* USE_ASSERT_CHECKING */

#endif							/* PATHCHECK_H */
