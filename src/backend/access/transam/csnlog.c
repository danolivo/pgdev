/*-----------------------------------------------------------------------------
 *
 * csnlog.c
 *		Track global commit sequence numbers of finished transactions
 *
 * Implementation of cross-node transaction isolation relies on commit sequence
 * number (CSN) based visibility rules.  This module provides SLRU to store
 * CSN for each transaction.  This mapping need to be kept only for xid's
 * greater then oldestXid, but that can require arbitrary large amounts of
 * memory in case of long-lived transactions.  Because of same lifetime and
 * persistancy requirements this module is quite similar to subtrans.c
 *
 * If we switch database from CSN-based snapshot to xid-based snapshot then,
 * nothing wrong. But if we switch xid-based snapshot to CSN-based snapshot
 * it should decide a new xid which begin csn-based check. It can not be
 * oldestActiveXID because of prepared transaction.
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL  Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/transam/csnlog.c
 *
 *-----------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/csnlog.h"
#include "access/slru.h"
#include "access/subtrans.h"
#include "access/transam.h"
#include "miscadmin.h"
#include "pg_trace.h"
#include "utils/snapmgr.h"

bool enable_csn_snapshot;

/*
 * Defines for CSNLog page sizes.  A page is the same BLCKSZ as is used
 * everywhere else in Postgres.
 *
 * Note: because TransactionIds are 32 bits and wrap around at 0xFFFFFFFF,
 * CSNLog page numbering also wraps around at
 * 0xFFFFFFFF/GLOBAL_CSN_LOG_XACTS_PER_PAGE, and CSNLog segment numbering at
 * 0xFFFFFFFF/CLOG_XACTS_PER_PAGE/SLRU_PAGES_PER_SEGMENT.  We need take no
 * explicit notice of that fact in this module, except when comparing segment
 * and page numbers in TruncateCSNLog (see CSNLogPagePrecedes).
 */

/* We store the commit CSN_t for each xid */
#define GCSNLOG_XACTS_PER_PAGE (BLCKSZ / sizeof(CSN_t))

#define TransactionIdToPage(xid)	((xid) / (TransactionId) GCSNLOG_XACTS_PER_PAGE)
#define TransactionIdToPgIndex(xid) ((xid) % (TransactionId) GCSNLOG_XACTS_PER_PAGE)

/*
 * Link to shared-memory data structures for CLOG control
 */
static SlruCtlData CSNLogCtlData;
#define CsnlogCtl (&CSNLogCtlData)

static int	ZeroCSNLogPage(int pageno, bool write_xlog);
static void ZeroTruncateCSNLogPage(int pageno, bool write_xlog);
static bool CSNLogPagePrecedes(int page1, int page2);
static void CSNLogSetPageStatus(TransactionId xid, int nsubxids,
									  TransactionId *subxids,
									  CSN_t csn, int pageno);
static void CSNLogSetCSNInSlot(TransactionId xid, CSN_t csn,
									  int slotno);
static void WriteCSNXlogRec(TransactionId xid, int nsubxids,
							   TransactionId *subxids, CSN_t csn);
static void WriteZeroCSNPageXlogRec(int pageno);
static void WriteTruncateCSNXlogRec(int pageno);

/*
 * CSNLogSetCSN
 *
 * Record CSN of transaction and its subtransaction tree.
 *
 * xid is a single xid to set status for. This will typically be the top level
 * transactionid for a top level commit or abort. It can also be a
 * subtransaction when we record transaction aborts.
 *
 * subxids is an array of xids of length nsubxids, representing subtransactions
 * in the tree of xid. In various cases nsubxids may be zero.
 *
 * csn is the commit sequence number of the transaction. It should be
 * AbortedCSN for abort cases.
 */
void
CSNLogSetCSN(TransactionId xid, int nsubxids,
				   TransactionId *subxids, CSN_t csn,
				   bool write_xlog)
{
	int			pageno;
	int			i = 0;
	int			offset = 0;

	/* Callers of CSNLogSetCSN() must check GUC params */
	Assert(enable_csn_snapshot);

	Assert(TransactionIdIsValid(xid));

	pageno = TransactionIdToPage(xid);		/* get page of parent */

	if(write_xlog)
		WriteCSNXlogRec(xid, nsubxids, subxids, csn);

	for (;;)
	{
		int			num_on_page = 0;

		while (i < nsubxids && TransactionIdToPage(subxids[i]) == pageno)
		{
			num_on_page++;
			i++;
		}

		CSNLogSetPageStatus(xid,
							num_on_page, subxids + offset,
							csn, pageno);
		if (i >= nsubxids)
			break;

		offset = i;
		pageno = TransactionIdToPage(subxids[offset]);
		xid = InvalidTransactionId;
	}
}

/*
 * Record the final state of transaction entries in the csn log for
 * all entries on a single page.  Atomic only on this page.
 *
 * Otherwise API is same as TransactionIdSetTreeStatus()
 */
static void
CSNLogSetPageStatus(TransactionId xid, int nsubxids,
						   TransactionId *subxids,
						   CSN_t csn, int pageno)
{
	int			slotno;
	int			i;

	LWLockAcquire(CSNLogControlLock, LW_EXCLUSIVE);

	slotno = SimpleLruReadPage(CsnlogCtl, pageno, true, xid);

	/* Subtransactions first, if needed ... */
	for (i = 0; i < nsubxids; i++)
	{
		Assert(CsnlogCtl->shared->page_number[slotno] == TransactionIdToPage(subxids[i]));
		CSNLogSetCSNInSlot(subxids[i],	csn, slotno);
	}

	/* ... then the main transaction */
	if (TransactionIdIsValid(xid))
		CSNLogSetCSNInSlot(xid, csn, slotno);

	CsnlogCtl->shared->page_dirty[slotno] = true;

	LWLockRelease(CSNLogControlLock);
}

/*
 * Sets the commit status of a single transaction.
 */
static void
CSNLogSetCSNInSlot(TransactionId xid, CSN_t csn, int slotno)
{
	int			entryno = TransactionIdToPgIndex(xid);
	CSN_t *ptr;

	Assert(LWLockHeldByMe(CSNLogControlLock));

	ptr = (CSN_t *) (CsnlogCtl->shared->page_buffer[slotno] + entryno * sizeof(XLogRecPtr));

	*ptr = csn;
}

/*
 * Interrogate the state of a transaction in the log.
 *
 * NB: this is a low-level routine and is NOT the preferred entry point
 * for most uses; TransactionIdGetCSN() in global_snapshot.c is the
 * intended caller.
 */
CSN_t
CSNLogGetCSN(TransactionId xid)
{
	int			pageno = TransactionIdToPage(xid);
	int			entryno = TransactionIdToPgIndex(xid);
	int			slotno;
	CSN_t *ptr;
	CSN_t	csn;

	/* Callers of CSNLogGetCSN() must check GUC params */
	Assert(enable_csn_snapshot);

	/* lock is acquired by SimpleLruReadPage_ReadOnly */

	slotno = SimpleLruReadPage_ReadOnly(CsnlogCtl, pageno, xid);
	ptr = (CSN_t *) (CsnlogCtl->shared->page_buffer[slotno] + entryno * sizeof(XLogRecPtr));
	csn = *ptr;

	LWLockRelease(CSNLogControlLock);

	return csn;
}

/*
 * Number of shared CSNLog buffers.
 */
static Size
CSNLogShmemBuffers(void)
{
	return Min(32, Max(4, NBuffers / 512));
}

/*
 * Reserve shared memory for CsnlogCtl.
 */
Size
CSNLogShmemSize(void)
{
	if (!enable_csn_snapshot)
		return 0;

	return SimpleLruShmemSize(CSNLogShmemBuffers(), 0);
}

/*
 * Initialization of shared memory for CSNLog.
 */
void
CSNLogShmemInit(void)
{
	if (!enable_csn_snapshot)
		return;

	CsnlogCtl->PagePrecedes = CSNLogPagePrecedes;
	SimpleLruInit(CsnlogCtl, "CSNLog Ctl", CSNLogShmemBuffers(), 0,
				  CSNLogControlLock, "pg_csn", LWTRANCHE_GLOBAL_CSN_LOG_BUFFERS);
}

/*
 * This func must be called ONCE on system install.  It creates the initial
 * CSNLog segment.  The pg_csn directory is assumed to have been
 * created by initdb, and CSNLogShmemInit must have been called already.
 */
void
BootStrapCSNLog(void)
{
	int			slotno;

	if (!enable_csn_snapshot)
		return;

	LWLockAcquire(CSNLogControlLock, LW_EXCLUSIVE);

	/* Create and zero the first page of the commit log */
	slotno = ZeroCSNLogPage(0, false);

	/* Make sure it's written out */
	SimpleLruWritePage(CsnlogCtl, slotno);
	Assert(!CsnlogCtl->shared->page_dirty[slotno]);

	LWLockRelease(CSNLogControlLock);
}

/*
 * This must be called ONCE during postmaster or standalone-backend startup,
 * after StartupXLOG has initialized ShmemVariableCache->nextXid.
 *
 * oldestActiveXID is the oldest XID of any prepared transaction, or nextXid
 * if there are none.
 */
void
StartupCSNLog(TransactionId oldestActiveXID)
{
	int			startPage;
	int			endPage;

	if (!enable_csn_snapshot)
		return;

	/*
	 * Since we don't expect pg_csn to be valid across crashes, we
	 * initialize the currently-active page(s) to zeroes during startup.
	 * Whenever we advance into a new page, ExtendCSNLog will likewise
	 * zero the new page without regard to whatever was previously on disk.
	 */
	LWLockAcquire(CSNLogControlLock, LW_EXCLUSIVE);

	startPage = TransactionIdToPage(oldestActiveXID);
	endPage = TransactionIdToPage(XidFromFullTransactionId(
											ShmemVariableCache->nextFullXid));

	while (startPage != endPage)
	{
		(void) ZeroCSNLogPage(startPage, false);
		startPage++;
		/* must account for wraparound */
		if (startPage > TransactionIdToPage(MaxTransactionId))
			startPage = 0;
	}

	(void) ZeroCSNLogPage(startPage, false);

	LWLockRelease(CSNLogControlLock);
}

/*
 * Initialize (or reinitialize) a page of CSNLog to zeroes.
 *
 * The page is not actually written, just set up in shared memory.
 * The slot number of the new page is returned.
 *
 * Control lock must be held at entry, and will be held at exit.
 */
static int
ZeroCSNLogPage(int pageno, bool write_xlog)
{
	Assert(LWLockHeldByMe(CSNLogControlLock));

	if(write_xlog)
		WriteZeroCSNPageXlogRec(pageno);

	return SimpleLruZeroPage(CsnlogCtl, pageno);
}

/*
 * This must be called ONCE during postmaster or standalone-backend shutdown
 */
void
ShutdownCSNLog(void)
{
	if (!enable_csn_snapshot)
		return;

	/*
	 * Flush dirty CSNLog pages to disk.
	 *
	 * This is not actually necessary from a correctness point of view. We do
	 * it merely as a debugging aid.
	 */
	TRACE_POSTGRESQL_GLOBALCSNLOG_CHECKPOINT_START(false);
	SimpleLruFlush(CsnlogCtl, false);
	TRACE_POSTGRESQL_GLOBALCSNLOG_CHECKPOINT_DONE(false);
}

/*
 * Perform a checkpoint --- either during shutdown, or on-the-fly
 */
void
CheckPointCSNLog(void)
{
	if (!enable_csn_snapshot)
		return;

	/*
	 * Flush dirty CSNLog pages to disk.
	 *
	 * This is not actually necessary from a correctness point of view. We do
	 * it merely to improve the odds that writing of dirty pages is done by
	 * the checkpoint process and not by backends.
	 */
	TRACE_POSTGRESQL_GLOBALCSNLOG_CHECKPOINT_START(true);
	SimpleLruFlush(CsnlogCtl, true);
	TRACE_POSTGRESQL_GLOBALCSNLOG_CHECKPOINT_DONE(true);
}

/*
 * Make sure that CSNLog has room for a newly-allocated XID.
 *
 * NB: this is called while holding XidGenLock.  We want it to be very fast
 * most of the time; even when it's not so fast, no actual I/O need happen
 * unless we're forced to write out a dirty clog or xlog page to make room
 * in shared memory.
 */
void
ExtendCSNLog(TransactionId newestXact)
{
	int			pageno;

	if (!enable_csn_snapshot)
		return;

	/*
	 * No work except at first XID of a page.  But beware: just after
	 * wraparound, the first XID of page zero is FirstNormalTransactionId.
	 */
	if (TransactionIdToPgIndex(newestXact) != 0 &&
		!TransactionIdEquals(newestXact, FirstNormalTransactionId))
		return;

	pageno = TransactionIdToPage(newestXact);

	LWLockAcquire(CSNLogControlLock, LW_EXCLUSIVE);

	/* Zero the page and make an XLOG entry about it */
	ZeroCSNLogPage(pageno, !InRecovery);

	LWLockRelease(CSNLogControlLock);
}

static void
ZeroTruncateCSNLogPage(int pageno, bool write_xlog)
{
	if(write_xlog)
		WriteTruncateCSNXlogRec(pageno);
	SimpleLruTruncate(CsnlogCtl, pageno);
}

/*
 * Remove all CSNLog segments before the one holding the passed
 * transaction ID.
 *
 * This is normally called during checkpoint, with oldestXact being the
 * oldest TransactionXmin of any running transaction.
 */
void
TruncateCSNLog(TransactionId oldestXact)
{
	int			cutoffPage;

	if (!enable_csn_snapshot)
		return;

	/*
	 * The cutoff point is the start of the segment containing oldestXact. We
	 * pass the *page* containing oldestXact to SimpleLruTruncate. We step
	 * back one transaction to avoid passing a cutoff page that hasn't been
	 * created yet in the rare case that oldestXact would be the first item on
	 * a page and oldestXact == next XID.  In that case, if we didn't subtract
	 * one, we'd trigger SimpleLruTruncate's wraparound detection.
	 */
	TransactionIdRetreat(oldestXact);
	cutoffPage = TransactionIdToPage(oldestXact);

	ZeroTruncateCSNLogPage(cutoffPage, true);
}

/*
 * Decide which of two CSNLog page numbers is "older" for truncation
 * purposes.
 *
 * We need to use comparison of TransactionIds here in order to do the right
 * thing with wraparound XID arithmetic.  However, if we are asked about
 * page number zero, we don't want to hand InvalidTransactionId to
 * TransactionIdPrecedes: it'll get weird about permanent xact IDs.  So,
 * offset both xids by FirstNormalTransactionId to avoid that.
 */
static bool
CSNLogPagePrecedes(int page1, int page2)
{
	TransactionId xid1;
	TransactionId xid2;

	xid1 = ((TransactionId) page1) * GCSNLOG_XACTS_PER_PAGE;
	xid1 += FirstNormalTransactionId;
	xid2 = ((TransactionId) page2) * GCSNLOG_XACTS_PER_PAGE;
	xid2 += FirstNormalTransactionId;

	return TransactionIdPrecedes(xid1, xid2);
}

void
WriteAssignCSNXlogRec(CSN_t csn)
{
	CSN_t log_csn = 0;

	if(csn <= get_last_log_wal_csn())
		return;

	log_csn = CSNAddByNanosec(csn, 0);
	set_last_log_wal_csn(log_csn);

	XLogBeginInsert();
	XLogRegisterData((char *) (&log_csn), sizeof(CSN_t));
	XLogInsert(RM_CSNLOG_ID, XLOG_CSN_ASSIGNMENT);
}

static void
WriteCSNXlogRec(TransactionId xid, int nsubxids,
				TransactionId *subxids, CSN_t csn)
{
	xl_csn_set 	xlrec;
	XLogRecPtr		recptr;

	xlrec.xtop = xid;
	xlrec.nsubxacts = nsubxids;
	xlrec.csn = csn;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, MinSizeOfCSN_tSet);
	XLogRegisterData((char *) subxids, nsubxids * sizeof(TransactionId));
	recptr = XLogInsert(RM_CSNLOG_ID, XLOG_CSN_SETXIDCSN);
	XLogFlush(recptr);
}

/*
 * Write a ZEROPAGE xlog record
 */
static void
WriteZeroCSNPageXlogRec(int pageno)
{
	XLogBeginInsert();
	XLogRegisterData((char *) (&pageno), sizeof(int));
	(void) XLogInsert(RM_CSNLOG_ID, XLOG_CSN_ZEROPAGE);
}

/*
 * Write a TRUNCATE xlog record
 */
static void
WriteTruncateCSNXlogRec(int pageno)
{
	XLogRecPtr	recptr;
	return;
	XLogBeginInsert();
	XLogRegisterData((char *) (&pageno), sizeof(int));
	recptr = XLogInsert(RM_CSNLOG_ID, XLOG_CSN_TRUNCATE);
	XLogFlush(recptr);
}


void
csnlog_redo(XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	/* Backup blocks are not used in csnlog records */
	Assert(!XLogRecHasAnyBlockRefs(record));

	if (info == XLOG_CSN_ASSIGNMENT)
	{
		CSN_t csn;

		memcpy(&csn, XLogRecGetData(record), sizeof(CSN_t));
		LWLockAcquire(CSNLogControlLock, LW_EXCLUSIVE);
		set_last_max_csn(csn);
		LWLockRelease(CSNLogControlLock);

	}
	else if (info == XLOG_CSN_SETXIDCSN)
	{
		xl_csn_set *xlrec = (xl_csn_set *) XLogRecGetData(record);
		CSNLogSetCSN(xlrec->xtop, xlrec->nsubxacts, xlrec->xsub, xlrec->csn, false);
	}
	else if (info == XLOG_CSN_ZEROPAGE)
	{
		int			pageno;
		int			slotno;

		memcpy(&pageno, XLogRecGetData(record), sizeof(int));
		LWLockAcquire(CSNLogControlLock, LW_EXCLUSIVE);
		slotno = ZeroCSNLogPage(pageno, false);
		SimpleLruWritePage(CsnlogCtl, slotno);
		LWLockRelease(CSNLogControlLock);
		Assert(!CsnlogCtl->shared->page_dirty[slotno]);

	}
	else if (info == XLOG_CSN_TRUNCATE)
	{
		int			pageno;

		memcpy(&pageno, XLogRecGetData(record), sizeof(int));
		CsnlogCtl->shared->latest_page_number = pageno;
		ZeroTruncateCSNLogPage(pageno, false);
	}
	else
		elog(PANIC, "csnlog_redo: unknown op code %u", info);
}
