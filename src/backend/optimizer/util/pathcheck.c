/*-------------------------------------------------------------------------
 *
 * pathcheck.c
 *	  Assert-only tracker that catches the same Path being added to two
 *	  pathlists.
 *
 * Asserts that any Path node is referenced by at most one pathlist
 * (RelOptInfo.pathlist, .partial_pathlist, or equivalent) at a time.
 * The membership hash is allocated in the planner's working memory
 * context on first use and torn down by a MemoryContextCallback when
 * that context resets, so entries never outlive the Paths they
 * reference.  Active only under USE_ASSERT_CHECKING.  Re-presenting an
 * already-recorded Path to add_path() now trips an assertion; this is
 * intentional and catches a real use-after-free hazard.  FDW and
 * custom-scan authors require no changes.
 *
 * Mechanism: a process-local pointer-keyed simplehash set tracks Path
 * pointers that are currently a member of some pathlist.  The set is
 * populated at the accept branch of add_path / add_partial_path and
 * depopulated at the in-loop removal branch of the same routines, and
 * also when planner.c (and a small number of other call sites) zap a
 * pathlist wholesale via a list assignment to NIL — see
 * pathcheck_forget_list().  Any attempted second insertion of a pointer
 * already in the set fires elog(ERROR) at the offending add site.
 *
 * Lifetime: the hash is allocated lazily in CurrentMemoryContext at
 * the first record() call (which is during the planner's working
 * context).  A MemoryContextCallback registered on that context NULLs
 * the static pointer when the context resets, after which the next
 * record() will re-init in whatever the new CurrentMemoryContext is.
 * Membership entries therefore die with the Paths they describe and
 * cannot leak across statements.
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
#include "utils/palloc.h"

#include "pathcheck.h"


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
#define SH_HASH_KEY(tb, key)	murmurhash64((uint64) (uintptr_t) (key))
#define SH_EQUAL(tb, a, b)		((a) == (b))
#define SH_SCOPE				static inline
#define SH_DECLARE
#define SH_DEFINE
#include "lib/simplehash.h"


static pathmembership_hash *path_membership = NULL;
static MemoryContextCallback path_membership_reset_cb;


static void path_membership_reset(void *arg);


/*
 * path_membership_reset
 *	  MemoryContextCallback fired when the planner working context the
 *	  hash lives in is reset or deleted.  All hash storage is in that
 *	  context, so the simplehash and its bucket array are about to be
 *	  freed wholesale; we just NULL the static pointer so the next
 *	  record() re-initialises.
 *
 * Memory-context callbacks fire in LIFO order at reset/delete time.
 * Ours only NULLs a static, so ordering versus other callbacks
 * registered on the same context is irrelevant.
 */
static void
path_membership_reset(void *arg)
{
	path_membership = NULL;
}

/*
 * path_membership_record
 *	  Record that 'path' is now a member of some pathlist.  Fire an error
 *	  if the path is already recorded.
 *
 * Called from add_path() and add_partial_path() at the accept branch,
 * immediately before list_insert_nth().  'rel' is the parent_rel and
 * is used only for the diagnostic.
 *
 * On first call within a planner invocation, allocates the hash in
 * CurrentMemoryContext (which is the planner's working context at the
 * first add_path / add_partial_path) and registers a reset callback so
 * the static pointer is cleared when that context goes away.
 */
void
path_membership_record(const Path *path, const RelOptInfo *rel)
{
	bool		found;

	Assert(path != NULL);

	if (path_membership == NULL)
	{
		path_membership = pathmembership_create(CurrentMemoryContext, 256, NULL);

		path_membership_reset_cb.func = path_membership_reset;
		path_membership_reset_cb.arg = NULL;
		MemoryContextRegisterResetCallback(CurrentMemoryContext,
										   &path_membership_reset_cb);
	}

	Assert(path_membership != NULL);

	(void) pathmembership_insert(path_membership, path, &found);
	if (found)
		elog(ERROR,
			 "path %p (nodeTag %d) already present in a pathlist (rel %u)",
			 path, (int) nodeTag(path), rel ? (unsigned) rel->relid : 0);

	/*
	 * simplehash's SH_KEY mapping has already populated entry->path from the
	 * argument; nothing else to do.
	 */
}

/*
 * path_membership_forget
 *	  Remove 'path' from the membership set.  Called from the in-loop
 *	  removal branches of add_path() / add_partial_path(), before the
 *	  conditional pfree (the IndexPath exception in add_path means the
 *	  pfree may not run, but the list-removal always does, and we track
 *	  list-membership rather than allocation lifetime).
 *
 * The path must have been recorded earlier; failing to find it is an
 * invariant violation.  We use Assert rather than elog(ERROR) here
 * because this routine is called from inside foreach_delete_current
 * loops where throwing leaves the iterator state mid-walk; an Assert
 * trips a stack trace at the offending frame instead.
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
 *	  this, the zapped paths' membership records would sit stale in the
 *	  hash and a subsequent add_path() of any of those pointers — even
 *	  legitimate, freshly-allocated ones that happen to alias under
 *	  CLOBBER_FREED_MEMORY recycling — would trip a spurious assert.
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
