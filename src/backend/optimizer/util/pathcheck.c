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
 * The user-facing contract lives in pathcheck.h.  Here we cover the
 * implementation: handoff exceptions, the per-backend hash stack, the
 * MemoryContextCallback unwind, and the simplehash mechanics.
 *
 * Handoff exceptions to the at-most-one-pathlist rule.  The sole
 * documented exceptions are grouping_planner's final-paths handoff and
 * the matching handoff in create_ordered_paths: a path lifted into a
 * new RelOptInfo without a wrapper has its membership record forgotten
 * before the second add_path().  The original list cell in current_rel
 * (resp. input_rel) is left in place so the FDW upper-paths hooks
 * observe the same pathlist shape they would have seen before this
 * work.  The underlying Path may be pfree'd by add_path()'s dominance
 * prune in the destination RelOptInfo, in which case the source list
 * cell points at freed memory; that is a pre-existing hazard in the
 * FDW upper-paths hook contract (postgres_fdw's add_foreign_final_paths
 * walks the input rel's pathlist after the loop), predating this patch.
 * Closing it requires changing externally-visible contract semantics
 * and is left as a separate -hackers thread.
 *
 * Mechanism: a per-backend pointer-keyed simplehash set tracks Path
 * pointers that are currently a member of some pathlist.  The set is
 * populated at the accept branch of add_path / add_partial_path and
 * depopulated only when the dominated Path is actually pfree'd; see
 * pathcheck.h for which removal sites do not pfree and why leaving
 * stale hash entries there is safe.  path_membership_forget() therefore
 * tolerates "not present": a Path that was substituted into a list cell
 * via "lfirst(lc) = newpath" was never recorded, so the prune branch of
 * add_path() finds nothing to delete when that cell later loses a
 * dominance check.  The collision check on insert remains strict: any
 * attempted second insertion of a pointer already in the set fires
 * elog(ERROR) at the offending add site, which is the property the
 * tracker exists to enforce.
 *
 * Lifetime: pathcheck_planner_init() is called from subquery_planner
 * immediately after root->planner_cxt is set; it allocates a fresh
 * hash in planner_cxt, pushes a frame onto a per-backend stack that
 * remembers the previous value of the current-hash pointer, and
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
 * Hash element.  Pointer-keyed set, with a back-reference to the
 * RelOptInfo that originally recorded the Path.  The back-reference
 * exists purely for diagnostics: when path_membership_record() detects
 * a double-add, the elog(ERROR) names the first recording rel so the
 * 3am debugger doesn't have to guess where the Path came from.  Storing
 * the pointer (rather than the Bitmapset) keeps the hot path small;
 * the full relids set is read only on the failure path.
 */
typedef struct PathMembershipEntry
{
	const Path *path;			/* hash key */
	const RelOptInfo *first_rel;	/* rel that first recorded the path */
	char		status;			/* simplehash internal */
} PathMembershipEntry;

#define SH_PREFIX				pathmembership
#define SH_ELEMENT_TYPE			PathMembershipEntry
#define SH_KEY_TYPE				const Path *
#define SH_KEY					path
/*
 * Pointer-keyed hash; murmurhash64 over the pointer value mixes well, and
 * the explicit (uint32) cast acknowledges simplehash's 32-bit hash slot
 * (truncating the high half is intentional, the low 32 bits already carry
 * sufficient entropy).
 */
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

/*
 * Snapshot of pathcheck_top at the most recent pathcheck_subscope_enter().
 * Used only by pathcheck_subscope_leave() to assert that no nested
 * subquery_planner ran inside the subscope (i.e., that the planner-frame
 * stack came back to the same height we left it at).
 */
static PathCheckFrame *pathcheck_subscope_top = NULL;


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
 * pathcheck_subscope_enter / pathcheck_subscope_leave
 *	  Save the current membership hash and install a fresh one allocated
 *	  in CurrentMemoryContext.  Used by callers that build Paths in a
 *	  short-lived sub-context which they will MemoryContextDelete at the
 *	  end of the scope (geqo_eval being the canonical example).  The
 *	  fresh hash dies with the sub-context, so its keys (which point
 *	  into that sub-context) cannot outlive their referents.
 *
 * Caller invariants:
 *	(1) CurrentMemoryContext at entry must be the sub-context, not
 *	    planner_cxt itself; the inner hash is allocated where
 *	    CurrentMemoryContext points and must die with the Paths it
 *	    tracks.
 *	(2) No nested subquery_planner may run within the subscope; its
 *	    pathcheck_planner_init would push a frame whose saved pointer
 *	    referred to the subscope hash, leaving the outer planner
 *	    trackerless after the inner fini.
 *	gimme_tree (the only caller today) satisfies both.
 *
 * Caller pattern (mirrors the existing root->join_rel_hash save/restore):
 *	MemoryContextSwitchTo(subcxt);
 *	saved = pathcheck_subscope_enter();
 *	... build paths in subcxt, call add_path ...
 *	pathcheck_subscope_leave(saved);
 *	MemoryContextDelete(subcxt);
 */
void *
pathcheck_subscope_enter(void)
{
	void	   *saved = path_membership;

	path_membership = pathmembership_create(CurrentMemoryContext, 64, NULL);
	pathcheck_subscope_top = pathcheck_top;
	return saved;
}

void
pathcheck_subscope_leave(void *saved)
{
	/*
	 * Storage for the sub-scope hash is in the caller's sub-context and
	 * will be reaped by the matching MemoryContextDelete; we just put
	 * the outer pointer back.
	 *
	 * If a future caller ran subquery_planner from inside the subscope,
	 * pathcheck_planner_init would have pushed a frame and fini popped
	 * it, leaving pathcheck_top != snapshot if the pair was unbalanced
	 * or if an inner frame is still installed.  This Assert backs the
	 * "no nested subquery_planner" caller invariant declared at
	 * pathcheck_subscope_enter().
	 */
	Assert(pathcheck_top == pathcheck_subscope_top);
	path_membership = (pathmembership_hash *) saved;
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
	PathMembershipEntry *entry;

	Assert(path != NULL);
	Assert(path_membership != NULL);

	entry = pathmembership_insert(path_membership, path, &found);
	if (found)
	{
		const RelOptInfo *first_rel = entry->first_rel;
		char	   *first_relids;
		char	   *second_relids;

		first_relids = first_rel && first_rel->relids
			? bmsToString(first_rel->relids) : pstrdup("(none)");
		second_relids = rel && rel->relids
			? bmsToString(rel->relids) : pstrdup("(none)");
		elog(ERROR,
			 "path %p (nodeTag %d) added to rel %s, already present in rel %s",
			 path, (int) nodeTag(path), second_relids, first_relids);
	}
	entry->first_rel = rel;
}

/*
 * path_membership_forget
 *	  Remove 'path' from the membership set, if present.
 *
 * Called from add_path() and add_partial_path() at the dominance-prune
 * branch, paired with the pfree of the dominated Path: forget exactly
 * when we free.  Also called from the two documented handoff sites
 * (grouping_planner final-paths, create_ordered_paths) where a Path
 * pointer is legitimately re-added to a different RelOptInfo.
 *
 * We do not Assert that the entry was present.  Some Paths reach a
 * pathlist without going through add_path() -- specifically, the
 * "lfirst(lc) = newpath" substitution sites in adjust_paths_for_srfs,
 * apply_scanjoin_target_to_paths, and recurse_set_operations install a
 * fresh Path into a list cell without recording it.  When that cell is
 * later pruned by add_path() or handed off to a new RelOptInfo, this
 * routine finds nothing to delete and that is fine: the missing entry
 * is consistent with the relaxed contract ("hash entries are forgotten
 * only when the Path is pfree'd; substituted Paths that never had an
 * entry stay invisible until they themselves are added via add_path
 * somewhere").  The collision check at path_membership_record() still
 * fires elog(ERROR) on any genuine double-add, which is the property
 * the tracker exists to enforce.
 */
void
path_membership_forget(const Path *path)
{
	Assert(path != NULL);
	/*
	 * Every caller is inside subquery_planner -- between
	 * pathcheck_planner_init() and pathcheck_planner_fini().  A NULL
	 * here means a stray forget call that escaped the planner; catch it.
	 */
	Assert(path_membership != NULL);

	(void) pathmembership_delete(path_membership, path);
}

#endif							/* USE_ASSERT_CHECKING */
