/*
 * bgheap.h
 *
 *  Created on: 27.06.2018
 *      Author: andrey
 */

#ifndef SRC_INCLUDE_ACCESS_BGHEAP_H_
#define SRC_INCLUDE_ACCESS_BGHEAP_H_

#include "storage/block.h"
#include "storage/off.h"
//#include "storage/relfilenode.h"

#ifdef EXEC_BACKEND
extern void HeapCleanerLauncherIAm(void);
extern void HeapCleanerWorkerIAm(void);
extern void HeapCleanerLauncherMain(int argc, char *argv[]) pg_attribute_noreturn();
extern void HeapCleanerWorkerMain(int argc, char *argv[]) pg_attribute_noreturn();
#endif

extern int heapcleaner_max_workers;


extern void AtEOXact_BGHeap_tables(bool isCommit);
extern bool IsHeapCleanerLauncherProcess(void);
extern bool IsHeapCleanerWorkerProcess(void);

extern void HeapCleanerInit(void);
extern int StartHeapCleanerLauncher(void);
extern int StartHeapCleanerWorker(void);

extern void HeapCleanerSend(Relation relation, BlockNumber blkno);

extern void HeapCleanerShmemInit(void);
extern Size HeapCleanerShmemSize(void);

#endif /* SRC_INCLUDE_ACCESS_BGHEAP_H_ */
