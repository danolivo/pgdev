/*-------------------------------------------------------------------------
 *
 * pathcheck.h
 *	  Single-pathlist invariant tracker (assert-only).
 *
 * This header is private to src/backend/optimizer/util/.  It deliberately
 * lives outside src/include/ so that it is not installed and not visible
 * to extension authors.  The whole facility compiles to nothing when
 * USE_ASSERT_CHECKING is not defined.
 *
 * Contract (Reading A — current-membership):
 *	A given Path pointer may appear in at most one of
 *	{RelOptInfo.pathlist, RelOptInfo.partial_pathlist} at any moment of
 *	the planner invocation.  After it is removed (the in-loop deletion
 *	in add_path / add_partial_path) it may be re-added.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/optimizer/util/pathcheck.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PATHCHECK_H
#define PATHCHECK_H

#ifdef USE_ASSERT_CHECKING

#include "nodes/pathnodes.h"

extern void path_membership_record(const Path *path, const RelOptInfo *rel);
extern void path_membership_forget(const Path *path);

#endif							/* USE_ASSERT_CHECKING */

#endif							/* PATHCHECK_H */
