/*-------------------------------------------------------------------------
 *
 * pathcheck.c
 *	  Assert-only tracker that catches the same Path being added to two
 *	  pathlists.
 *
 * The planner's add_path() and add_partial_path() routines pfree paths
 * they reject (with a narrow exception for IndexPath, see the comment
 * in pathnode.c).  As a consequence, a Path that ends up referenced
 * from two different pathlists is a use-after-free in waiting: whichever
 * list rejects it first will pfree the storage out from under the other.
 * The contract is implicit in the code today; this file makes it
 * mechanically enforced under USE_ASSERT_CHECKING.
 *
 * Mechanism: a process-local pointer-keyed simplehash set tracks the
 * Path pointers that are currently a member of some pathlist.  The set
 * is populated at the accept branch of add_path / add_partial_path and
 * depopulated at the in-loop removal branch of the same routines.  Any
 * attempted second insertion of a pointer already in the set fires an
 * elog(ERROR) at the offending add site.
 *
 * The hash is allocated lazily in TopMemoryContext so that it survives
 * across statements within a session and we do not pay reallocation cost
 * on every plan.  An XactCallback clears the hash on transaction end,
 * with a WARNING on commit if it is non-empty (which would indicate
 * either an internal accounting bug or the known multi-planner-pass
 * staleness window — see README at the bottom of this file).
 *
 * The wholesale assignments rel->pathlist = NIL and
 * rel->partial_pathlist = NIL in grouping_planner (see comment in
 * planner.c near those sites) are deliberately not instrumented.  The
 * orphaned hash entries they leave behind cannot collide with any path
 * presented to add_path() afterward in current code, and they are
 * reaped at transaction end.  If a future change ever re-presents an
 * orphaned path to add_path(), the assertion will fire and we'll know
 * to instrument the zap.
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

#include "access/xact.h"
#include "common/hashfn.h"
#include "nodes/nodes.h"
#include "nodes/pathnodes.h"
#include "utils/memutils.h"

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
#define SH_HASH_KEY(tb, key)	\
	hash_bytes((const unsigned char *) &(key), sizeof(const Path *))
#define SH_EQUAL(tb, a, b)		((a) == (b))
#define SH_SCOPE				static inline
#define SH_DECLARE
#define SH_DEFINE
#include "lib/simplehash.h"


static pathmembership_hash *path_membership = NULL;
static bool xact_callback_registered = false;


static void path_membership_xact_callback(XactEvent event, void *arg);
static void path_membership_lazy_init(void);


/*
 * path_membership_lazy_init
 *	  Create the hash on first use, and register a one-shot xact callback
 *	  that resets it on transaction end.
 */
static void
path_membership_lazy_init(void)
{
	MemoryContext oldcxt;

	if (path_membership != NULL)
		return;

	/*
	 * The hash itself lives in TopMemoryContext so that it survives across
	 * statements within a session.  Initial size of 256 buckets is enough
	 * to avoid early growth in typical plans; simplehash will grow as
	 * needed.
	 */
	oldcxt = MemoryContextSwitchTo(TopMemoryContext);
	path_membership = pathmembership_create(TopMemoryContext, 256, NULL);
	MemoryContextSwitchTo(oldcxt);

	if (!xact_callback_registered)
	{
		RegisterXactCallback(path_membership_xact_callback, NULL);
		xact_callback_registered = true;
	}
}

/*
 * path_membership_record
 *	  Record that 'path' is now a member of some pathlist.  Fire an error
 *	  if the path is already recorded.
 *
 * Called from add_path() and add_partial_path() at the accept branch,
 * immediately before list_insert_nth().  'rel' is the parent_rel and
 * is used only for the diagnostic.
 */
void
path_membership_record(const Path *path, const RelOptInfo *rel)
{
	PathMembershipEntry *entry;
	bool		found;

	path_membership_lazy_init();

	entry = pathmembership_insert(path_membership, path, &found);
	if (found)
		elog(ERROR,
			 "path %p (nodeTag %d) already present in a pathlist (rel %u)",
			 path, (int) nodeTag(path), rel ? (unsigned) rel->relid : 0);

	/* simplehash leaves the key already populated by SH_KEY mapping */
	entry->path = path;
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
 * invariant violation and we elog(ERROR).
 */
void
path_membership_forget(const Path *path)
{
	/*
	 * Cannot reach here without a prior record() — the hook is only called
	 * for paths the caller pulled out of the pathlist, and getting into the
	 * pathlist requires going through path_membership_record().
	 */
	if (path_membership == NULL ||
		!pathmembership_delete(path_membership, path))
		elog(ERROR,
			 "path %p (nodeTag %d) removed from pathlist but never recorded",
			 path, (int) nodeTag(path));
}

/*
 * Transaction end callback.  Reset the hash on abort (definitively
 * needed) and on commit (defensive — under the current spec the hash
 * may still hold orphaned entries from earlier statements within the
 * same xact, since path memory dies with the planner working context
 * but the hash itself does not; see file header).
 */
static void
path_membership_xact_callback(XactEvent event, void *arg)
{
	if (path_membership == NULL)
		return;

	switch (event)
	{
		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:
			pathmembership_reset(path_membership);
			break;
		case XACT_EVENT_COMMIT:
		case XACT_EVENT_PARALLEL_COMMIT:
			if (path_membership->members > 0)
			{
				elog(WARNING,
					 "path membership tracker held %u stale entries at commit; resetting",
					 path_membership->members);
				pathmembership_reset(path_membership);
			}
			break;
		default:
			break;
	}
}

#endif							/* USE_ASSERT_CHECKING */
