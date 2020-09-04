/*-------------------------------------------------------------------------
 *
 * csn_snapshot.h
 *	  Support for cross-node snapshot isolation.
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/csn_snapshot.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CSN_SNAPSHOT_H
#define CSN_SNAPSHOT_H

#include "port/atomics.h"
#include "storage/lock.h"
#include "utils/snapshot.h"
#include "utils/guc.h"

/*
 * snapshot.h is used in frontend code so atomic variant of SnapshotCSN type
 * is defined here.
 */
typedef pg_atomic_uint64 CSN_atomic;

#define InProgressCSN	 	UINT64CONST(0x0)
#define AbortedCSN	 		UINT64CONST(0x1)
#define FrozenCSN		 	UINT64CONST(0x2)
#define InDoubtCSN	 		UINT64CONST(0x3)
#define UnclearCSN	 		UINT64CONST(0x4)
#define FirstNormalCSN 		UINT64CONST(0x5)

#define CSNIsInProgress(csn)	((csn) == InProgressCSN)
#define CSNIsAborted(csn)		((csn) == AbortedCSN)
#define CSNIsFrozen(csn)		((csn) == FrozenCSN)
#define CSNIsInDoubt(csn)		((csn) == InDoubtCSN)
#define CSNIsUnclear(csn)		((csn) == UnclearCSN)
#define CSNIsNormal(csn)		((csn) >= FirstNormalCSN)


extern int csn_snapshot_defer_time;
extern int csn_time_shift;


extern Size CSNSnapshotShmemSize(void);
extern void CSNSnapshotShmemInit(void);
extern void CSNSnapshotStartup(TransactionId oldestActiveXID);

extern void CSNSnapshotMapXmin(SnapshotCSN snapshot_csn);
extern TransactionId CSNSnapshotToXmin(SnapshotCSN snapshot_csn);

extern SnapshotCSN GenerateCSN(bool locked);

extern bool XidInvisibleInCSNSnapshot(TransactionId xid, Snapshot snapshot);

extern CSN TransactionIdGetCSN(TransactionId xid);

extern void CSNSnapshotAbort(PGPROC *proc, TransactionId xid, int nsubxids,
								TransactionId *subxids);
extern void CSNSnapshotPrecommit(PGPROC *proc, TransactionId xid, int nsubxids,
									TransactionId *subxids);
extern void CSNSnapshotCommit(PGPROC *proc, TransactionId xid, int nsubxids,
									TransactionId *subxids);
extern void CSNSnapshotAssignCurrent(SnapshotCSN snapshot_csn);
extern SnapshotCSN CSNSnapshotPrepareCurrent(void);
extern void CSNSnapshotSync(SnapshotCSN remote_csn);

#endif							/* CSN_SNAPSHOT_H */
