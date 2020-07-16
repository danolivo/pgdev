/*
 * global_csn_log.h
 *
 * Commit-Sequence-Number log.
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/global_csn_log.h
 */
#ifndef CSNLOG_H
#define CSNLOG_H

#include "access/xlog.h"
#include "utils/snapshot.h"


/* XLOG stuff */
#define XLOG_CSN_ASSIGNMENT	(0x00)
#define XLOG_CSN_SETXIDCSN	(0x10)
#define XLOG_CSN_ZEROPAGE	(0x20)
#define XLOG_CSN_TRUNCATE	(0x30)

#define MinSizeOfCSN_tSet offsetof(xl_csn_set, xsub)
#define	CSNAddByNanosec(csn,second) (csn + second * 1000000000L)

typedef struct xl_csn_set
{
	CSN_t			csn;
	TransactionId	xtop; /* XID's top-level XID */
	int				nsubxacts; /* number of subtransaction XIDs */
	TransactionId	xsub[FLEXIBLE_ARRAY_MEMBER];	/* assigned subxids */
} xl_csn_set;

extern void GlobalCSNLogSetCSN(TransactionId xid, int nsubxids,
							   TransactionId *subxids, CSN_t csn,
							   bool write_xlog);
extern CSN_t GlobalCSNLogGetCSN(TransactionId xid);

extern Size GlobalCSNLogShmemSize(void);
extern void GlobalCSNLogShmemInit(void);
extern void BootStrapGlobalCSNLog(void);
extern void StartupGlobalCSNLog(TransactionId oldestActiveXID);
extern void ShutdownGlobalCSNLog(void);
extern void CheckPointGlobalCSNLog(void);
extern void ExtendGlobalCSNLog(TransactionId newestXact);
extern void TruncateGlobalCSNLog(TransactionId oldestXact);

extern void csnlog_redo(XLogReaderState *record);
extern void csnlog_desc(StringInfo buf, XLogReaderState *record);
extern const char *csnlog_identify(uint8 info);
extern void WriteAssignCSNXlogRec(CSN_t xidcsn);
extern void set_last_max_csn(CSN_t xidcsn);
extern void set_last_log_wal_csn(CSN_t xidcsn);
extern CSN_t get_last_log_wal_csn(void);
extern void set_xmin_for_csn(void);

#endif   /* CSNLOG_H */
