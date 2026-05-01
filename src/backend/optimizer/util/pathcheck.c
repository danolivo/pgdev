/*-------------------------------------------------------------------------
 *
 * pathcheck.c
 *	  Assert-only tracker that catches the same Path being added to two
 *	  pathlists.
 *
 * Asserts that any Path node is referenced by at most one pathlist
 * (RelOptInfo.pathlist or RelOptInfo.partial_pathlist) at a time.
 * Active only under USE_ASSERT_CHECKING.  FDW and custom-scan authors
 * require no changes.
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
 * Mechanism: a process-local pointer-keyed simplehash set tracks Path
 * pointers that are currently a member of some pathlist.  The set is
 * populated at the accept branch of add_path / add_partial_path and
 * depopulated at the in-loop removal branch of the same routines, and
 * also when callers zap a pathlist wholesale via a list assignment to
 * NIL — see pathcheck_forget_list().  Any attempted second insertion
 * of a pointer already in the set fires elog(ERROR) at the offending
 * add site.
 *
 * Lifetime: pathcheck_planner_init() is called from subquery_planner
 * immediately after root->planner_cxt is set; it allocates a fresh
 * hash in planner_cxt, pushes a frame onto a file-static stack that
 * remembers the previous value of the current-hash static, and
 * registers a MemoryContextCallback that pops the frame should
 * planner_cxt be torn down by an ereport(ERROR) before
 * pathcheck_planner_fini() runs.  The fini routine pops the frame
 * normally and lets planner_cxt's teardown reap the hash storage.
 *
 * The frame storage lives in planner_cxt itself, so on an error
 * unwind through that context the callback fires (LIFO at
 * reset/delete) and pops the stack before the storage is freed; the
 * outer PlannerInfo's hash, allocated in an enclosing context, is
 * untouched.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/util/pathcheck.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_ASSERT_CHECKING

#include "common/hashfn.h"
#include "nodes/nodes.h"
#include "nodes/pathnodes.h"
#include "utils/memutils.h"

#include "optimizer/pathcheck.h"


/*
 * Hash element.  Pointer-keyed set; no value payload beyond the key and
 * the simplehash bookkeeping byte.
 */
typedef struct PathMembershipEntry
{
	const Path *path;			/* hash key */
	char		status;			/* simplehash internal */
} PathMembershipEntry;

#define SH_PREFIX				pathmembership
#define SH_ELEMENT_TYPE			PathMembershipEntry
#define SH_KEY_TYPE				const Path *
#define SH_KEY					path
#define SH_HASH_KEY(tb, key)	((uint32) murmurhash64((uint64) (uintptr_t) (key)))
#define SH_EQUAL(tb, a, b)		((a) == (b))
#define SH_SCOPE				static inline
#define SH_DECLARE
#define SH_DEFINE
#include "lib/simplehash.h"


/*
 * Per-subquery_planner save frame.  Linked into the file-static stack
 * pathcheck_top.  Frames are allocated in root->planner_cxt; their
 * lifetime is at most the lifetime of that context, so on an error
 * teardown they are reaped along with the hash they refer to.  The
 * registered callback is responsible for popping the frame from the
 * stack before the storage is freed.
 */
typedef struct PathCheckFrame
{
	pathmembership_hash *saved_membership;	/* enclosing PlannerInfo's hash, or NULL */
	struct PathCheckFrame *prev;			/* previous top, or NULL */
	bool		installed;					/* true while we own path_membership */
	MemoryContextCallback cb;
} PathCheckFrame;


static pathmembership_hash *path_membership = NULL;
static PathCheckFrame *pathcheck_top = NULL;


static void path_membership_unwind(void *arg);


/*
 * pop_frame
 *	  Restore path_membership and pathcheck_top to the values they had
 *	  before 'frame' was pushed.  Marks the frame as no longer
 *	  installed; subsequent calls (e.g. the reset callback) are no-ops.
 *	  Caller must ensure 'frame' is the current top of the stack.
 */
static inline void
pop_frame(PathCheckFrame *frame)
{
	Assert(frame == pathcheck_top);
	Assert(frame->installed);
	path_membership = frame->saved_membership;
	pathcheck_top = frame->prev;
	frame->installed = false;
}

/*
 * path_membership_unwind
 *	  MemoryContextCallback fired when root->planner_cxt is reset or
 *	  deleted (typically because an ereport(ERROR) is unwinding through
 *	  the planner).  If our frame is still installed, pop it; the hash
 *	  storage and the frame itself are in planner_cxt and about to be
 *	  reaped by the same teardown.
 *
 * Memory-context callbacks fire in LIFO order at reset/delete, which
 * matches the LIFO stacking of recursive subquery_planner invocations.
 * Each frame restores its own saved pointer; the outermost frame
 * restores NULL.
 */
static void
path_membership_unwind(void *arg)
{
	PathCheckFrame *frame = (PathCheckFrame *) arg;

	if (frame->installed)
		pop_frame(frame);
}

/*
 * pathcheck_planner_init
 *	  Save the current path_membership static, allocate a fresh hash in
 *	  root->planner_cxt, and install it.  Register a MemoryContextCallback
 *	  on planner_cxt so the static is correctly restored even if an
 *	  ereport(ERROR) tears the context down before pathcheck_planner_fini
 *	  runs.
 *
 * Must be called from subquery_planner immediately after
 * root->planner_cxt is assigned, and matched by exactly one
 * pathcheck_planner_fini at the bottom.
 */
void
pathcheck_planner_init(PlannerInfo *root)
{
	PathCheckFrame *frame;
	MemoryContext oldcxt;

	Assert(root != NULL);
	Assert(root->planner_cxt != NULL);

	oldcxt = MemoryContextSwitchTo(root->planner_cxt);

	frame = (PathCheckFrame *) palloc0(sizeof(PathCheckFrame));
	frame->saved_membership = path_membership;
	frame->prev = pathcheck_top;
	frame->installed = true;
	frame->cb.func = path_membership_unwind;
	frame->cb.arg = frame;
	MemoryContextRegisterResetCallback(root->planner_cxt, &frame->cb);

	path_membership = pathmembership_create(root->planner_cxt, 256, NULL);
	pathcheck_top = frame;

	MemoryContextSwitchTo(oldcxt);
}

/*
 * pathcheck_planner_fini
 *	  Pop the topmost save frame, restoring path_membership to its
 *	  enclosing-PlannerInfo value.  The hash storage is in planner_cxt
 *	  and will be reaped with it; we don't bother destroying it
 *	  explicitly.  The reset callback we registered remains attached
 *	  but is now a no-op because pop_frame cleared its 'installed'
 *	  flag.
 *
 * Must be called at every normal exit from subquery_planner, paired
 * with the pathcheck_planner_init at entry.  The error-exit path is
 * covered by the callback, not this routine.
 */
void
pathcheck_planner_fini(void)
{
	PathCheckFrame *frame = pathcheck_top;

	/*
	 * If there is no top frame, init/fini are mispaired.  Either the
	 * caller forgot to call init, or fini is being called twice.
	 */
	Assert(frame != NULL);

	pop_frame(frame);
}

/*
 * path_membership_record
 *	  Record that 'path' is now a member of some pathlist.  Fire an
 *	  error if the path is already recorded.
 *
 * Called from add_path() and add_partial_path() at the accept branch,
 * immediately before list_insert_nth().  'rel' is the parent_rel and
 * is used only for the diagnostic.
 */
void
path_membership_record(const Path *path, const RelOptInfo *rel)
{
	bool		found;

	Assert(path != NULL);
	Assert(path_membership != NULL);

	(void) pathmembership_insert(path_membership, path, &found);
	if (found)
		elog(ERROR,
			 "path %p (nodeTag %d) already present in a pathlist (rel %u)",
			 path, (int) nodeTag(path), rel ? (unsigned) rel->relid : 0);
}

/*
 * path_membership_forget
 *	  Remove 'path' from the membership set.  Called from the in-loop
 *	  removal branches of add_path() / add_partial_path(), before the
 *	  conditional pfree (the IndexPath exception in add_path means the
 *	  pfree may not run, but the list-removal always does, and we track
 *	  list-membership rather than allocation lifetime).
 *
 * Under v3's planner-context-bound hash, the path must have been
 * recorded earlier; aliasing is impossible because all hash entries
 * die with planner_cxt.  Failure to find is an invariant violation,
 * caught by Assert.  We use Assert rather than elog(ERROR) because
 * this routine is called from inside foreach_delete_current loops
 * where throwing leaves the iterator state mid-walk.
 */
void
path_membership_forget(const Path *path)
{
	bool		found;

	Assert(path != NULL);

	found = (path_membership != NULL &&
			 pathmembership_delete(path_membership, path));

	Assert(found);
}

/*
 * pathcheck_forget_list
 *	  Forget every Path on a list.  Used at the small number of sites
 *	  that wholesale-zap a pathlist via "rel->pathlist = NIL" instead
 *	  of going through add_path()'s in-loop removal branch.  Without
 *	  this, the zapped paths' membership records would sit stale in
 *	  the hash and a subsequent add_path() of any of those pointers
 *	  would trip a spurious error.
 *
 * Caller is responsible for invoking this *before* assigning the list
 * to NIL.
 */
void
pathcheck_forget_list(const List *paths)
{
	const ListCell *lc;

	if (path_membership == NULL || paths == NIL)
		return;

	foreach(lc, paths)
	{
		const Path *path = (const Path *) lfirst(lc);

		Assert(path != NULL);
		(void) pathmembership_delete(path_membership, path);
	}
}

#endif							/* USE_ASSERT_CHECKING */
