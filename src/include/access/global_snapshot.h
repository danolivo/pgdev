/*-------------------------------------------------------------------------
 *
 * global_snapshot.h
 *	  Support for cross-node snapshot isolation.
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/global_snapshot.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef GLOBAL_SNAPSHOT_H
#define GLOBAL_SNAPSHOT_H

#include "port/atomics.h"
#include "storage/lock.h"
#include "utils/snapshot.h"
#include "utils/guc.h"

/*
 * snapshot.h is used in frontend code so atomic variant of CSN_t type
 * is defined here.
 */
typedef pg_atomic_uint64 GlobalCSN_atomic;

#define InProgressGlobalCSN	 UINT64CONST(0x0)
#define AbortedGlobalCSN	 UINT64CONST(0x1)
#define FrozenGlobalCSN		 UINT64CONST(0x2)
#define InDoubtGlobalCSN	 UINT64CONST(0x3)
#define FirstNormalGlobalCSN UINT64CONST(0x4)

#define GlobalCSNIsInProgress(csn)	((csn) == InProgressGlobalCSN)
#define GlobalCSNIsAborted(csn)		((csn) == AbortedGlobalCSN)
#define GlobalCSNIsFrozen(csn)		((csn) == FrozenGlobalCSN)
#define GlobalCSNIsInDoubt(csn)		((csn) == InDoubtGlobalCSN)
#define GlobalCSNIsNormal(csn)		((csn) >= FirstNormalGlobalCSN)


extern int global_snapshot_defer_time;


extern Size GlobalSnapshotShmemSize(void);
extern void GlobalSnapshotShmemInit(void);
extern void GlobalSnapshotStartup(TransactionId oldestActiveXID);

extern void GlobalSnapshotMapXmin(CSN_t snapshot_global_csn);
extern TransactionId GlobalSnapshotToXmin(CSN_t snapshot_global_csn);

extern CSN_t GlobalSnapshotGenerate(bool locked);

extern bool XidInvisibleInGlobalSnapshot(TransactionId xid, Snapshot snapshot);

extern void GlobalSnapshotSync(CSN_t remote_gcsn);

extern CSN_t TransactionIdGetGlobalCSN(TransactionId xid);

extern CSN_t GlobalSnapshotPrepareGlobal(const char *gid);
extern void GlobalSnapshotAssignCsnGlobal(const char *gid,
										  CSN_t global_csn);

extern CSN_t GlobalSnapshotPrepareCurrent(void);
extern void GlobalSnapshotAssignCsnCurrent(CSN_t global_csn);

extern void GlobalSnapshotAbort(PGPROC *proc, TransactionId xid, int nsubxids,
								TransactionId *subxids);
extern void GlobalSnapshotPrecommit(PGPROC *proc, TransactionId xid, int nsubxids,
									TransactionId *subxids);
extern void GlobalSnapshotCommit(PGPROC *proc, TransactionId xid, int nsubxids,
									TransactionId *subxids);

#endif							/* GLOBAL_SNAPSHOT_H */
