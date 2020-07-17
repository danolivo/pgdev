/*-------------------------------------------------------------------------
 *
 * global_snapshot.c
 *		Support for cross-node snapshot isolation.
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL  Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/transam/global_snapshot.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/csnlog.h"
#include "access/csn_snapshot.h"
#include "access/transam.h"
#include "access/twophase.h"
#include "access/xact.h"
#include "portability/instr_time.h"
#include "storage/lmgr.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/snapmgr.h"
#include "miscadmin.h"

/* Raise a warning if imported csn exceeds ours by this value. */
#define SNAP_DESYNC_COMPLAIN (1*NSECS_PER_SEC) /* 1 second */

/*
 * SnapshotState
 *
 * Do not trust local clocks to be strictly monotonical and save last acquired
 * value so later we can compare next timestamp with it. Accessed through
 * CSNSnapshotGenerate() and CSNSnapshotSync().
 */
typedef struct
{
	CSN_t			last_max_csn;
	CSN_t			last_csn_log_wal; /* for interval we log the assign csn to wal */
	TransactionId	xmin_for_csn; /*'xmin_for_csn' for when turn xid-snapshot to csn-snapshot */
	volatile slock_t lock;
} SnapshotState;

static SnapshotState *gsState;
static TransactionId xmin_for_csn = InvalidTransactionId;

/*
 * GUC to delay advance of oldestXid for this amount of time. Also determines
 * the size SnapshotXidMap circular buffer.
 */
int csn_snapshot_defer_time;

/*
 * Enables this module.
 */
extern bool enable_csn_snapshot;

/*
 * SnapshotXidMap
 *
 * To be able to install global snapshot that points to past we need to keep
 * old versions of tuples and therefore delay advance of oldestXid.  Here we
 * keep track of correspondence between snapshot's csn and oldestXid
 * that was set at the time when the snapshot was taken.  Much like the
 * snapshot too old's OldSnapshotControlData does, but with finer granularity
 * to seconds.
 *
 * Different strategies can be employed to hold oldestXid (e.g. we can track
 * oldest csn-based snapshot among cluster nodes and map it oldestXid
 * on each node) but here implemented one that tries to avoid cross-node
 * communications which are tricky in case of postgres_fdw.
 *
 * On each snapshot acquisition CSNSnapshotMapXmin() is called and stores
 * correspondence between current csn and oldestXmin in a sparse way:
 * csn is rounded to seconds (and here we use the fact that csn
 * is just a timestamp) and oldestXmin is stored in the circular buffer where
 * rounded csn acts as an offset from current circular buffer head.
 * Size of the circular buffer is controlled by csn_snapshot_defer_time GUC.
 *
 * When global snapshot arrives from different node we check that its
 * csn is still in our map, otherwise we'll error out with "snapshot too
 * old" message.  If csn is successfully mapped to oldestXid we move
 * backend's pgxact->xmin to proc->originalXmin and fill pgxact->xmin to
 * mapped oldestXid.  That way GetOldestXmin() can take into account backends
 * with imported global snapshot and old tuple versions will be preserved.
 *
 * Also while calculating oldestXmin for our map in presence of imported
 * global snapshots we should use proc->originalXmin instead of pgxact->xmin
 * that was set during import.  Otherwise, we can create a feedback loop:
 * xmin's of imported global snapshots were calculated using our map and new
 * entries in map going to be calculated based on that xmin's, and there is
 * a risk to stuck forever with one non-increasing oldestXmin.  All other
 * callers of GetOldestXmin() are using pgxact->xmin so the old tuple versions
 * are preserved.
 */
typedef struct SnapshotXidMap
{
	int				 head;				/* offset of current freshest value */
	int				 size;				/* total size of circular buffer */
	CSN_atomic last_csn_seconds;	/* last rounded csn that changed
										 * xmin_by_second[] */
	TransactionId   *xmin_by_second;	/* circular buffer of oldestXmin's */
}
SnapshotXidMap;

static SnapshotXidMap *gsXidMap;


/* Estimate shared memory space needed */
Size
CSNSnapshotShmemSize(void)
{
	Size	size = 0;

	if (enable_csn_snapshot || csn_snapshot_defer_time > 0)
	{
		size += MAXALIGN(sizeof(SnapshotState));
	}

	if (csn_snapshot_defer_time > 0)
	{
		size += sizeof(SnapshotXidMap);
		size += csn_snapshot_defer_time*sizeof(TransactionId);
		size = MAXALIGN(size);
	}

	return size;
}

/* Init shared memory structures */
void
CSNSnapshotShmemInit()
{
	bool found;

	if (enable_csn_snapshot || csn_snapshot_defer_time > 0)
	{
		gsState = ShmemInitStruct("gsState",
								sizeof(SnapshotState),
								&found);
		if (!found)
		{
			gsState->last_max_csn = 0;
			gsState->last_csn_log_wal = 0;
			SpinLockInit(&gsState->lock);
		}
	}

	if (csn_snapshot_defer_time > 0)
	{
		gsXidMap = ShmemInitStruct("gsXidMap",
								   sizeof(SnapshotXidMap),
								   &found);
		if (!found)
		{
			int i;

			pg_atomic_init_u64(&gsXidMap->last_csn_seconds, 0);
			gsXidMap->head = 0;
			gsXidMap->size = csn_snapshot_defer_time;
			gsXidMap->xmin_by_second =
							ShmemAlloc(sizeof(TransactionId)*gsXidMap->size);

			for (i = 0; i < gsXidMap->size; i++)
				gsXidMap->xmin_by_second[i] = InvalidTransactionId;
		}
	}
}

/*
 * CSNSnapshotStartup
 *
 * Set gsXidMap entries to oldestActiveXID during startup.
 */
void
CSNSnapshotStartup(TransactionId oldestActiveXID)
{
	/*
	 * Run only if we have initialized shared memory and gsXidMap
	 * is enabled.
	 */
	if (IsNormalProcessingMode() &&
		enable_csn_snapshot && csn_snapshot_defer_time > 0)
	{
		int i;

		Assert(TransactionIdIsValid(oldestActiveXID));
		for (i = 0; i < gsXidMap->size; i++)
			gsXidMap->xmin_by_second[i] = oldestActiveXID;
		ProcArraySetCSNSnapshotXmin(oldestActiveXID);
	}
}

/*
 * CSNSnapshotMapXmin
 *
 * Maintain circular buffer of oldestXmins for several seconds in past. This
 * buffer allows to shift oldestXmin in the past when backend is importing
 * global transaction. Otherwise old versions of tuples that were needed for
 * this transaction can be recycled by other processes (vacuum, HOT, etc).
 *
 * Locking here is not trivial. Called upon each snapshot creation after
 * ProcArrayLock is released. Such usage creates several race conditions. It
 * is possible that backend who got csn called CSNSnapshotMapXmin()
 * only after other backends managed to get snapshot and complete
 * CSNSnapshotMapXmin() call, or even committed. This is safe because
 *
 *		* We already hold our xmin in MyPgXact, so our snapshot will not be
 *		  harmed even though ProcArrayLock is released.
 *
 *		* snapshot_csn is always pessmistically rounded up to the next
 *		  second.
 *
 *		* For performance reasons, xmin value for particular second is filled
 *		  only once. Because of that instead of writing to buffer just our
 *		  xmin (which is enough for our snapshot), we bump oldestXmin there --
 *		  it mitigates the possibility of damaging someone else's snapshot by
 *		  writing to the buffer too advanced value in case of slowness of
 *		  another backend who generated csn earlier, but didn't manage to
 *		  insert it before us.
 *
 *		* if CSNSnapshotMapXmin() founds a gap in several seconds between
 *		  current call and latest completed call then it should fill that gap
 *		  with latest known values instead of new one. Otherwise it is
 *		  possible (however highly unlikely) that this gap also happend
 *		  between taking snapshot and call to CSNSnapshotMapXmin() for some
 *		  backend. And we are at risk to fill circullar buffer with
 *		  oldestXmin's that are bigger then they actually were.
 */
void
CSNSnapshotMapXmin(CSN_t snapshot_csn)
{
	int offset, gap, i;
	CSN_t csn_seconds;
	CSN_t last_csn_seconds;
	volatile TransactionId oldest_deferred_xmin;
	TransactionId current_oldest_xmin, previous_oldest_xmin;

	/* Callers should check config values */
	Assert(csn_snapshot_defer_time > 0);
	Assert(gsXidMap != NULL);

	/*
	 * Round up csn to the next second -- pessimistically and safely.
	 */
	csn_seconds = (snapshot_csn / NSECS_PER_SEC + 1);

	/*
	 * Fast-path check. Avoid taking exclusive CSNSnapshotXidMapLock lock
	 * if oldestXid was already written to xmin_by_second[] for this rounded
	 * csn.
	 */
	if (pg_atomic_read_u64(&gsXidMap->last_csn_seconds) >= csn_seconds)
		return;

	/* Ok, we have new entry (or entries) */
	LWLockAcquire(CSNSnapshotXidMapLock, LW_EXCLUSIVE);

	/* Re-check last_csn_seconds under lock */
	last_csn_seconds = pg_atomic_read_u64(&gsXidMap->last_csn_seconds);
	if (last_csn_seconds >= csn_seconds)
	{
		LWLockRelease(CSNSnapshotXidMapLock);
		return;
	}
	pg_atomic_write_u64(&gsXidMap->last_csn_seconds, csn_seconds);

	/*
	 * Count oldest_xmin.
	 *
	 * It was possible to calculate oldest_xmin during corresponding snapshot
	 * creation, but GetSnapshotData() intentionally reads only PgXact, but not
	 * PgProc. And we need info about originalXmin (see comment to gsXidMap)
	 * which is stored in PgProc because of threats in comments around PgXact
	 * about extending it with new fields. So just calculate oldest_xmin again,
	 * that anyway happens quite rarely.
	 */
	current_oldest_xmin = GetOldestXmin(NULL, PROCARRAY_NON_IMPORTED_XMIN);

	previous_oldest_xmin = gsXidMap->xmin_by_second[gsXidMap->head];

	Assert(TransactionIdIsNormal(current_oldest_xmin));
	Assert(TransactionIdIsNormal(previous_oldest_xmin) || !enable_csn_snapshot);

	gap = csn_seconds - last_csn_seconds;
	offset = csn_seconds % gsXidMap->size;

	/* Sanity check before we update head and gap */
	Assert( gap >= 1 );
	Assert( (gsXidMap->head + gap) % gsXidMap->size == offset );

	gap = gap > gsXidMap->size ? gsXidMap->size : gap;
	gsXidMap->head = offset;

	/* Fill new entry with current_oldest_xmin */
	gsXidMap->xmin_by_second[offset] = current_oldest_xmin;

	/*
	 * If we have gap then fill it with previous_oldest_xmin for reasons
	 * outlined in comment above this function.
	 */
	for (i = 1; i < gap; i++)
	{
		offset = (offset + gsXidMap->size - 1) % gsXidMap->size;
		gsXidMap->xmin_by_second[offset] = previous_oldest_xmin;
	}

	oldest_deferred_xmin =
		gsXidMap->xmin_by_second[ (gsXidMap->head + 1) % gsXidMap->size ];

	LWLockRelease(CSNSnapshotXidMapLock);

	/*
	 * Advance procArray->global_snapshot_xmin after we released
	 * CSNSnapshotXidMapLock. Since we gather not xmin but oldestXmin, it
	 * never goes backwards regardless of how slow we can do that.
	 */
	Assert(TransactionIdFollowsOrEquals(oldest_deferred_xmin,
										ProcArrayGetCSNSnapshotXmin()));
	ProcArraySetCSNSnapshotXmin(oldest_deferred_xmin);
}


/*
 * CSNSnapshotToXmin
 *
 * Get oldestXmin that took place when snapshot_csn was taken.
 */
TransactionId
CSNSnapshotToXmin(CSN_t snapshot_csn)
{
	TransactionId xmin;
	CSN_t csn_seconds;
	volatile CSN_t last_csn_seconds;

	/* Callers should check config values */
	Assert(csn_snapshot_defer_time > 0);
	Assert(gsXidMap != NULL);

	/* Round down to get conservative estimates */
	csn_seconds = (snapshot_csn / NSECS_PER_SEC);

	LWLockAcquire(CSNSnapshotXidMapLock, LW_SHARED);
	last_csn_seconds = pg_atomic_read_u64(&gsXidMap->last_csn_seconds);
	if (csn_seconds > last_csn_seconds)
	{
		/* we don't have entry for this csn yet, return latest known */
		xmin = gsXidMap->xmin_by_second[gsXidMap->head];
	}
	else if (last_csn_seconds - csn_seconds < gsXidMap->size)
	{
		/* we are good, retrieve value from our map */
		Assert(last_csn_seconds % gsXidMap->size == gsXidMap->head);
		xmin = gsXidMap->xmin_by_second[csn_seconds % gsXidMap->size];
	}
	else
	{
		/* requested csn is too old, let caller know */
		xmin = InvalidTransactionId;
	}
	LWLockRelease(CSNSnapshotXidMapLock);

	return xmin;
}

/*
 * CSNSnapshotGenerate
 *
 * Generate CSN_t which is actually a local time. Also we are forcing
 * this time to be always increasing. Since now it is not uncommon to have
 * millions of read transactions per second we are trying to use nanoseconds
 * if such time resolution is available.
 */
CSN_t
CSNSnapshotGenerate(bool locked)
{
	instr_time	current_time;
	CSN_t	csn;

	Assert(enable_csn_snapshot || csn_snapshot_defer_time > 0);

	/*
	 * TODO: create some macro that add small random shift to current time.
	 */
	INSTR_TIME_SET_CURRENT(current_time);
	csn = (CSN_t) INSTR_TIME_GET_NANOSEC(current_time);

	/* TODO: change to atomics? */
	if (!locked)
		SpinLockAcquire(&gsState->lock);

	if (csn <= gsState->last_max_csn)
		csn = ++gsState->last_max_csn;
	else
		gsState->last_max_csn = csn;

	WriteAssignCSNXlogRec(csn);

	if (!locked)
		SpinLockRelease(&gsState->lock);

	return csn;
}

/*
 * CSNSnapshotSync
 *
 * Due to time desynchronization on different nodes we can receive csn
 * which is greater than csn on this node. To preserve proper isolation
 * this node needs to wait when such csn comes on local clock.
 *
 * This should happend relatively rare if nodes have running NTP/PTP/etc.
 * Complain if wait time is more than SNAP_SYNC_COMPLAIN.
 */
void
CSNSnapshotSync(CSN_t remote_gcsn)
{
	CSN_t	local_gcsn;
	CSN_t	delta;

	Assert(enable_csn_snapshot);

	for(;;)
	{
		SpinLockAcquire(&gsState->lock);
		if (gsState->last_max_csn > remote_gcsn)
		{
			/* Everything is fine */
			SpinLockRelease(&gsState->lock);
			return;
		}
		else if ((local_gcsn = CSNSnapshotGenerate(true)) >= remote_gcsn)
		{
			/*
			 * Everything is fine too, but last_max_csn wasn't updated for
			 * some time.
			 */
			SpinLockRelease(&gsState->lock);
			return;
		}
		SpinLockRelease(&gsState->lock);

		/* Okay we need to sleep now */
		delta = remote_gcsn - local_gcsn;
		if (delta > SNAP_DESYNC_COMPLAIN)
			ereport(WARNING,
				(errmsg("remote global snapshot exceeds ours by more than a second"),
				 errhint("Consider running NTPd on servers participating in global transaction")));

		/* TODO: report this sleeptime somewhere? */
		pg_usleep((long) (delta/NSECS_PER_USEC));

		/*
		 * Loop that checks to ensure that we actually slept for specified
		 * amount of time.
		 */
	}

	Assert(false); /* Should not happend */
	return;
}

/*
 * TransactionIdGetCSN
 *
 * Get CSN_t for specified TransactionId taking care about special xids,
 * xids beyond TransactionXmin and InDoubt states.
 */
CSN_t
TransactionIdGetCSN(TransactionId xid)
{
	CSN_t csn;

	Assert(enable_csn_snapshot);

	/* Handle permanent TransactionId's for which we don't have mapping */
	if (!TransactionIdIsNormal(xid))
	{
		if (xid == InvalidTransactionId)
			return AbortedCSN;
		if (xid == FrozenTransactionId || xid == BootstrapTransactionId)
			return FrozenCSN;
		Assert(false); /* Should not happend */
	}

	/*
	 * If we just switch a xid-snapsot to a csn_snapshot, we should handle a start
	 * xid for csn base check. Just in case we have prepared transaction which
	 * hold the TransactionXmin but without CSN.
	 */
	if (xmin_for_csn == InvalidTransactionId)
	{
		SpinLockAcquire(&gsState->lock);
		if(gsState->xmin_for_csn != InvalidTransactionId)
			xmin_for_csn = gsState->xmin_for_csn;
		else
			xmin_for_csn = FrozenTransactionId;
		SpinLockRelease(&gsState->lock);
	}

	if (xmin_for_csn != FrozenTransactionId ||
					TransactionIdPrecedes(xmin_for_csn, TransactionXmin))
		xmin_for_csn = TransactionXmin;

	/*
	 * For xids which less then TransactionXmin CSNLog can be already
	 * trimmed but we know that such transaction is definetly not concurrently
	 * running according to any snapshot including timetravel ones. Callers
	 * should check TransactionDidCommit after.
	 */
	if (TransactionIdPrecedes(xid, xmin_for_csn))
		return FrozenCSN;

	/* Read CSN_t from SLRU */
	csn = CSNLogGetCSN(xid);

	/*
	 * If we faced InDoubt state then transaction is beeing committed and we
	 * should wait until CSN_t will be assigned so that visibility check
	 * could decide whether tuple is in snapshot. See also comments in
	 * CSNSnapshotPrecommit().
	 */
	if (CSNIsInDoubt(csn))
	{
		XactLockTableWait(xid, NULL, NULL, XLTW_None);
		csn = CSNLogGetCSN(xid);
		Assert(CSNIsNormal(csn) ||
				CSNIsAborted(csn));
	}

	Assert(CSNIsNormal(csn) ||
			CSNIsInProgress(csn) ||
			CSNIsAborted(csn));

	return csn;
}

/*
 * XidInvisibleInCSNSnapshot
 *
 * Version of XidInMVCCSnapshot for global transactions. For non-imported
 * global snapshots this should give same results as XidInLocalMVCCSnapshot
 * (except that aborts will be shown as invisible without going to clog) and to
 * ensure such behaviour XidInMVCCSnapshot is coated with asserts that checks
 * identicalness of XidInvisibleInCSNSnapshot/XidInLocalMVCCSnapshot in
 * case of ordinary snapshot.
 */
bool
XidInvisibleInCSNSnapshot(TransactionId xid, Snapshot snapshot)
{
	CSN_t csn;

	Assert(enable_csn_snapshot);

	csn = TransactionIdGetCSN(xid);

	if (CSNIsNormal(csn))
	{
		if (csn < snapshot->csn)
			return false;
		else
			return true;
	}
	else if (CSNIsFrozen(csn))
	{
		/* It is bootstrap or frozen transaction */
		return false;
	}
	else
	{
		/* It is aborted or in-progress */
		Assert(CSNIsAborted(csn) || CSNIsInProgress(csn));
		if (CSNIsAborted(csn))
			Assert(TransactionIdDidAbort(xid));
		return true;
	}
}


/*****************************************************************************
 * Functions to handle distributed commit on transaction coordinator:
 * CSNSnapshotPrepareCurrent() / CSNSnapshotAssignCurrent().
 * Correspoding functions for remote nodes are defined in twophase.c:
 * pg_csn_snapshot_prepare/pg_csn_snapshot_assign.
 *****************************************************************************/


/*
 * CSNSnapshotPrepareCurrent
 *
 * Set InDoubt state for currently active transaction and return commit's
 * global snapshot.
 */
CSN_t
CSNSnapshotPrepareCurrent()
{
	TransactionId xid = GetCurrentTransactionIdIfAny();

	if (!enable_csn_snapshot)
		ereport(ERROR,
			(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				errmsg("could not prepare transaction for global commit"),
				errhint("Make sure the configuration parameter \"%s\" is enabled.",
						"enable_csn_snapshot")));

	if (TransactionIdIsValid(xid))
	{
		TransactionId *subxids;
		int nsubxids = xactGetCommittedChildren(&subxids);
		CSNLogSetCSN(xid, nsubxids, subxids, InDoubtCSN, false);
	}

	/* Nothing to write if we don't have xid */

	return CSNSnapshotGenerate(false);
}

/*
 * CSNSnapshotAssignCurrent
 *
 * Asign CSN_t for currently active transaction. CSN_t is supposedly
 * maximal among of values returned by CSNSnapshotPrepareCurrent and
 * pg_csn_snapshot_prepare.
 */
void
CSNSnapshotAssignCurrent(CSN_t csn)
{
	if (!enable_csn_snapshot)
		ereport(ERROR,
			(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				errmsg("could not prepare transaction for global commit"),
				errhint("Make sure the configuration parameter \"%s\" is enabled.",
						"enable_csn_snapshot")));

	if (!CSNIsNormal(csn))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("pg_csn_snapshot_assign expects normal csn")));

	/* Skip emtpty transactions */
	if (!TransactionIdIsValid(GetCurrentTransactionIdIfAny()))
		return;

	/* Set csn and defuse ProcArrayEndTransaction from assigning one */
	pg_atomic_write_u64(&MyProc->assignedCSN, csn);
}


/*****************************************************************************
 * Functions to handle global and local transactions commit.
 *
 * For local transactions CSNSnapshotPrecommit sets InDoubt state before
 * ProcArrayEndTransaction is called and transaction data potetntially becomes
 * visible to other backends. ProcArrayEndTransaction (or ProcArrayRemove in
 * twophase case) then acquires csn under ProcArray lock and stores it
 * in proc->assignedCSN. It's important that csn for commit is
 * generated under ProcArray lock, otherwise global and local snapshots won't
 * be equivalent. Consequent call to CSNSnapshotCommit will write
 * proc->assignedCSN to CSNLog.
 *
 * Same rules applies to global transaction, except that csn is already
 * assigned by CSNSnapshotAssignCurrent/pg_csn_snapshot_assign and
 * CSNSnapshotPrecommit is basically no-op.
 *
 * CSNSnapshotAbort is slightly different comparing to commit because abort
 * can skip InDoubt phase and can be called for transaction subtree.
 *****************************************************************************/


/*
 * CSNSnapshotAbort
 *
 * Abort transaction in CsnLog. We can skip InDoubt state for aborts
 * since no concurrent transactions allowed to see aborted data anyway.
 */
void
CSNSnapshotAbort(PGPROC *proc, TransactionId xid,
					int nsubxids, TransactionId *subxids)
{
	if (!enable_csn_snapshot)
		return;

	CSNLogSetCSN(xid, nsubxids, subxids, AbortedCSN, true);

	/*
	 * Clean assignedCSN anyway, as it was possibly set in
	 * CSNSnapshotAssignCurrent.
	 */
	pg_atomic_write_u64(&proc->assignedCSN, InProgressCSN);
}

/*
 * CSNSnapshotPrecommit
 *
 * Set InDoubt status for local transaction that we are going to commit.
 * This step is needed to achieve consistency between local snapshots and
 * global csn-based snapshots. We don't hold ProcArray lock while writing
 * csn for transaction in SLRU but instead we set InDoubt status before
 * transaction is deleted from ProcArray so the readers who will read csn
 * in the gap between ProcArray removal and CSN_t assignment can wait
 * until CSN_t is finally assigned. See also TransactionIdGetCSN().
 *
 * For global transaction this does nothing as InDoubt state was written
 * earlier.
 *
 * This should be called only from parallel group leader before backend is
 * deleted from ProcArray.
 */
void
CSNSnapshotPrecommit(PGPROC *proc, TransactionId xid,
					int nsubxids, TransactionId *subxids)
{
	CSN_t oldAssignedCsn = InProgressCSN;
	bool in_progress;

	if (!enable_csn_snapshot)
		return;

	/* Set InDoubt status if it is local transaction */
	in_progress = pg_atomic_compare_exchange_u64(&proc->assignedCSN,
												 &oldAssignedCsn,
												 InDoubtCSN);
	if (in_progress)
	{
		Assert(CSNIsInProgress(oldAssignedCsn));
		CSNLogSetCSN(xid, nsubxids, subxids, InDoubtCSN, true);
	}
	else
	{
		/* Otherwise we should have valid CSN_t by this time */
		Assert(CSNIsNormal(oldAssignedCsn));
		/* Also global transaction should already be in InDoubt state */
		Assert(CSNIsInDoubt(CSNLogGetCSN(xid)));
	}
}

/*
 * CSNSnapshotCommit
 *
 * Write CSN_t that were acquired earlier to CsnLog. Should be
 * preceded by CSNSnapshotPrecommit() so readers can wait until we finally
 * finished writing to SLRU.
 *
 * Should be called after ProcArrayEndTransaction, but before releasing
 * transaction locks, so that TransactionIdGetCSN can wait on this
 * lock for CSN_t.
 */
void
CSNSnapshotCommit(PGPROC *proc, TransactionId xid,
					int nsubxids, TransactionId *subxids)
{
	volatile CSN_t assigned_csn;

	if (!enable_csn_snapshot)
		return;

	if (!TransactionIdIsValid(xid))
	{
		assigned_csn = pg_atomic_read_u64(&proc->assignedCSN);
		Assert(CSNIsInProgress(assigned_csn));
		return;
	}

	/* Finally write resulting CSN_t in SLRU */
	assigned_csn = pg_atomic_read_u64(&proc->assignedCSN);
	Assert(CSNIsNormal(assigned_csn));
	CSNLogSetCSN(xid, nsubxids,
						   subxids, assigned_csn, true);

	/* Reset for next transaction */
	pg_atomic_write_u64(&proc->assignedCSN, InProgressCSN);
}

void
set_last_max_csn(CSN_t csn)
{
	gsState->last_max_csn = csn;
}

void
set_last_log_wal_csn(CSN_t csn)
{
	gsState->last_csn_log_wal = csn;
}

CSN_t
get_last_log_wal_csn(void)
{
	CSN_t last_csn_log_wal;

	last_csn_log_wal = gsState->last_csn_log_wal;

	return last_csn_log_wal;
}

/*
 * 'xmin_for_csn' for when turn xid-snapshot to csn-snapshot
 */
void
set_xmin_for_csn(void)
{
	gsState->xmin_for_csn = XidFromFullTransactionId(ShmemVariableCache->nextFullXid);
}
