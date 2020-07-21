/*-------------------------------------------------------------------------
 *
 * csn_snapshot.c
 *		Support for cross-node snapshot isolation.
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/transam/csn_snapshot.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/csn_log.h"
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

/* Raise a warning if imported snapshot_csn exceeds ours by this value. */
#define SNAP_DESYNC_COMPLAIN (1*NSECS_PER_SEC) /* 1 second */

/*
 * CSNSnapshotState
 *
 * Do not trust local clocks to be strictly monotonical and save last acquired
 * value so later we can compare next timestamp with it. Accessed through
 * GenerateCSN().
 */
typedef struct
{
	SnapshotCSN		 last_max_csn;
	volatile slock_t lock;
} CSNSnapshotState;

static CSNSnapshotState *csnState;

/*
 * Enables this module.
 */
extern bool enable_csn_snapshot;


/* Estimate shared memory space needed */
Size
CSNSnapshotShmemSize(void)
{
	Size	size = 0;

	if (enable_csn_snapshot)
	{
		size += MAXALIGN(sizeof(CSNSnapshotState));
	}

	return size;
}

/* Init shared memory structures */
void
CSNSnapshotShmemInit()
{
	bool found;

	if (enable_csn_snapshot)
	{
		csnState = ShmemInitStruct("csnState",
								sizeof(CSNSnapshotState),
								&found);
		if (!found)
		{
			csnState->last_max_csn = 0;
			SpinLockInit(&csnState->lock);
		}
	}
}

/*
 * GenerateCSN
 *
 * Generate SnapshotCSN which is actually a local time. Also we are forcing
 * this time to be always increasing. Since now it is not uncommon to have
 * millions of read transactions per second we are trying to use nanoseconds
 * if such time resolution is available.
 */
SnapshotCSN
GenerateCSN(bool locked)
{
	instr_time	current_time;
	SnapshotCSN	csn;

	Assert(enable_csn_snapshot);

	/*
	 * TODO: create some macro that add small random shift to current time.
	 */
	INSTR_TIME_SET_CURRENT(current_time);
	csn = (SnapshotCSN) INSTR_TIME_GET_NANOSEC(current_time);

	/* TODO: change to atomics? */
	if (!locked)
		SpinLockAcquire(&csnState->lock);

	if (csn <= csnState->last_max_csn)
		csn = ++csnState->last_max_csn;
	else
		csnState->last_max_csn = csn;

	if (!locked)
		SpinLockRelease(&csnState->lock);

	return csn;
}

/*
 * TransactionIdGetXidCSN
 *
 * Get XidCSN for specified TransactionId taking care about special xids,
 * xids beyond TransactionXmin and InDoubt states.
 */
XidCSN
TransactionIdGetXidCSN(TransactionId xid)
{
	XidCSN xid_csn;

	Assert(enable_csn_snapshot);

	/* Handle permanent TransactionId's for which we don't have mapping */
	if (!TransactionIdIsNormal(xid))
	{
		if (xid == InvalidTransactionId)
			return AbortedXidCSN;
		if (xid == FrozenTransactionId || xid == BootstrapTransactionId)
			return FrozenXidCSN;
		Assert(false); /* Should not happend */
	}

	/*
	 * For xids which less then TransactionXmin CSNLog can be already
	 * trimmed but we know that such transaction is definetly not concurrently
	 * running according to any snapshot including timetravel ones. Callers
	 * should check TransactionDidCommit after.
	 */
	if (TransactionIdPrecedes(xid, TransactionXmin))
		return FrozenXidCSN;

	/* Read XidCSN from SLRU */
	xid_csn = CSNLogGetCSNByXid(xid);

	/*
	 * If we faced InDoubt state then transaction is beeing committed and we
	 * should wait until XidCSN will be assigned so that visibility check
	 * could decide whether tuple is in snapshot. See also comments in
	 * CSNSnapshotPrecommit().
	 */
	if (XidCSNIsInDoubt(xid_csn))
	{
		XactLockTableWait(xid, NULL, NULL, XLTW_None);
		xid_csn = CSNLogGetCSNByXid(xid);
		Assert(XidCSNIsNormal(xid_csn) ||
				XidCSNIsAborted(xid_csn));
	}

	Assert(XidCSNIsNormal(xid_csn) ||
			XidCSNIsInProgress(xid_csn) ||
			XidCSNIsAborted(xid_csn));

	return xid_csn;
}

/*
 * XidInvisibleInCSNSnapshot
 *
 * Version of XidInMVCCSnapshot for transactions. For non-imported
 * csn snapshots this should give same results as XidInLocalMVCCSnapshot
 * (except that aborts will be shown as invisible without going to clog) and to
 * ensure such behaviour XidInMVCCSnapshot is coated with asserts that checks
 * identicalness of XidInvisibleInCSNSnapshot/XidInLocalMVCCSnapshot in
 * case of ordinary snapshot.
 */
bool
XidInvisibleInCSNSnapshot(TransactionId xid, Snapshot snapshot)
{
	XidCSN csn;

	Assert(enable_csn_snapshot);

	csn = TransactionIdGetXidCSN(xid);

	if (XidCSNIsNormal(csn))
	{
		if (csn < snapshot->snapshot_csn)
			return false;
		else
			return true;
	}
	else if (XidCSNIsFrozen(csn))
	{
		/* It is bootstrap or frozen transaction */
		return false;
	}
	else
	{
		/* It is aborted or in-progress */
		Assert(XidCSNIsAborted(csn) || XidCSNIsInProgress(csn));
		if (XidCSNIsAborted(csn))
			Assert(TransactionIdDidAbort(xid));
		return true;
	}
}


/*****************************************************************************
 * Functions to handle transactions commit.
 *
 * For local transactions CSNSnapshotPrecommit sets InDoubt state before
 * ProcArrayEndTransaction is called and transaction data potetntially becomes
 * visible to other backends. ProcArrayEndTransaction (or ProcArrayRemove in
 * twophase case) then acquires xid_csn under ProcArray lock and stores it
 * in proc->assignedXidCsn. It's important that xid_csn for commit is
 * generated under ProcArray lock, otherwise snapshots won't
 * be equivalent. Consequent call to CSNSnapshotCommit will write
 * proc->assignedXidCsn to CSNLog.
 *
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

	CSNLogSetCSN(xid, nsubxids, subxids, AbortedXidCSN);

	/*
	 * Clean assignedXidCsn anyway, as it was possibly set in
	 * XidSnapshotAssignCsnCurrent.
	 */
	pg_atomic_write_u64(&proc->assignedXidCsn, InProgressXidCSN);
}

/*
 * CSNSnapshotPrecommit
 *
 * Set InDoubt status for local transaction that we are going to commit.
 * This step is needed to achieve consistency between local snapshots and
 * csn-based snapshots. We don't hold ProcArray lock while writing
 * csn for transaction in SLRU but instead we set InDoubt status before
 * transaction is deleted from ProcArray so the readers who will read csn
 * in the gap between ProcArray removal and XidCSN assignment can wait
 * until XidCSN is finally assigned. See also TransactionIdGetXidCSN().
 *
 * This should be called only from parallel group leader before backend is
 * deleted from ProcArray.
 */
void
CSNSnapshotPrecommit(PGPROC *proc, TransactionId xid,
					int nsubxids, TransactionId *subxids)
{
	XidCSN oldassignedXidCsn = InProgressXidCSN;
	bool in_progress;

	if (!enable_csn_snapshot)
		return;

	/* Set InDoubt status if it is local transaction */
	in_progress = pg_atomic_compare_exchange_u64(&proc->assignedXidCsn,
												 &oldassignedXidCsn,
												 InDoubtXidCSN);
	if (in_progress)
	{
		Assert(XidCSNIsInProgress(oldassignedXidCsn));
		CSNLogSetCSN(xid, nsubxids,
						   subxids, InDoubtXidCSN);
	}
	else
	{
		/* Otherwise we should have valid XidCSN by this time */
		Assert(XidCSNIsNormal(oldassignedXidCsn));
		Assert(XidCSNIsInDoubt(CSNLogGetCSNByXid(xid)));
	}
}

/*
 * CSNSnapshotCommit
 *
 * Write XidCSN that were acquired earlier to CsnLog. Should be
 * preceded by CSNSnapshotPrecommit() so readers can wait until we finally
 * finished writing to SLRU.
 *
 * Should be called after ProcArrayEndTransaction, but before releasing
 * transaction locks, so that TransactionIdGetXidCSN can wait on this
 * lock for XidCSN.
 */
void
CSNSnapshotCommit(PGPROC *proc, TransactionId xid,
					int nsubxids, TransactionId *subxids)
{
	volatile XidCSN assigned_xid_csn;

	if (!enable_csn_snapshot)
		return;

	if (!TransactionIdIsValid(xid))
	{
		assigned_xid_csn = pg_atomic_read_u64(&proc->assignedXidCsn);
		Assert(XidCSNIsInProgress(assigned_xid_csn));
		return;
	}

	/* Finally write resulting XidCSN in SLRU */
	assigned_xid_csn = pg_atomic_read_u64(&proc->assignedXidCsn);
	Assert(XidCSNIsNormal(assigned_xid_csn));
	CSNLogSetCSN(xid, nsubxids,
						   subxids, assigned_xid_csn);

	/* Reset for next transaction */
	pg_atomic_write_u64(&proc->assignedXidCsn, InProgressXidCSN);
}
