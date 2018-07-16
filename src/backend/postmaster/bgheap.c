/*
 * relcleaner.c
 *
 *  Created on: 27.06.2018
 *      Author: andrey
 */

#include <unistd.h>

#include "postgres.h"

#include "fmgr.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/heapam_xlog.h"
#include "access/htup_details.h"
#include "access/nbtree.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/pg_class.h"
#include "catalog/pg_database.h"
#include "catalog/pg_namespace.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "commands/vacuum.h"
#include "common/ip.h"
#include "executor/executor.h"
#include "libpq/pqsignal.h"
#include "postmaster/bgheap.h"
#include "postmaster/bgworker.h"
#include "postmaster/fork_process.h"
#include "postmaster/postmaster.h"
#include "storage/bufmgr.h"
#include "storage/condition_variable.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lmgr.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "storage/smgr.h"
#include "tcop/tcopprot.h"
#include "utils/guc.h"
#include "utils/ps_status.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/relcache.h"
//#include "utils/relfilenodemap.h"
#include "utils/resowner.h"
#include "utils/syscache.h"
#include "utils/timeout.h"

#define WORK_ITEMS_MAX	(256)

typedef struct CleanerMessage
{
	Oid	relid;
	Oid	dbNode;
	Oid	spcNode;
	int	blkno;
} CleanerMessage;

typedef struct WorkerInfoData
{
	dlist_node	links;
	Oid			dbOid;
	Oid			spcOid;
	TimestampTz launchtime;
	int			pid;
	CleanerMessage	buffer[WORK_ITEMS_MAX];
	int			nitems;
	LWLock		WorkItemLock;
} WorkerInfoData;

typedef struct WorkerInfoData *WorkerInfo;

#define NUM_WORKITEMS	256

typedef struct
{
	dlist_head	freeWorkers;
	dlist_head	runningWorkers;
	WorkerInfo	startingWorker;
} HeapCleanerShmemStruct;

typedef struct WorkWaitingList
{
	dlist_node		links;
	CleanerMessage	msg;
} WorkWaitingList;

static bool am_heapcleaner_launcher = false;
static bool am_heapcleaner_worker = false;

int heapcleaner_max_workers = 10;

static WorkerInfo MyWorkerInfo = NULL;

static HeapCleanerShmemStruct *HeapCleanerShmem;

static dlist_head DatabaseList = DLIST_STATIC_INIT(DatabaseList);
static MemoryContext DatabaseListCxt = NULL;

/* Signal handling */
static volatile sig_atomic_t got_SIGHUP = false;
static volatile sig_atomic_t got_SIGTERM = false;
static volatile sig_atomic_t got_SIGUSR2 = false;

NON_EXEC_STATIC pgsocket RelCleanerSock = PGINVALID_SOCKET;
static struct sockaddr_storage pgStatAddr;

static int TrancheId;
dlist_head	WaitingList;

#ifdef EXEC_BACKEND
static pid_t hclauncher_forkexec(void);
static pid_t hcworker_forkexec(void);
#endif

NON_EXEC_STATIC void HeapCleanerLauncherMain(int argc, char *argv[]) pg_attribute_noreturn();
NON_EXEC_STATIC void HeapCleanerWorkerMain(int argc, char *argv[]) pg_attribute_noreturn();

static void index_cleanup(Oid spcNode, Oid relNode, BlockNumber blkno);
static void launch_worker(Oid dbNode);
static WorkerInfo look_for_worker(Oid dbNode);
static void main_launcher_loop(void);
static void main_worker_loop(void);

static CleanerMessage pop_waiting_list(void);
static void push_waiting_list(CleanerMessage *msg);

static void SIGHUP_Handler(SIGNAL_ARGS);
static void SIGTERM_Handler(SIGNAL_ARGS);
static void SIGUSR2_Handler(SIGNAL_ARGS);

static void _init_socket(pgsocket *sock);

#ifdef EXEC_BACKEND
/*
 * forkexec routine for the autovacuum launcher process.
 *
 * Format up the arglist, then fork and exec.
 */
static pid_t
hclauncher_forkexec(void)
{
	char	   *av[10];
	int			ac = 0;

	av[ac++] = "postgres";
	av[ac++] = "--forkhclauncher";
	av[ac++] = NULL;			/* filled in by postmaster_forkexec */
	av[ac] = NULL;

	Assert(ac < lengthof(av));

	return postmaster_forkexec(ac, av);
}

/*
 * We need this set from the outside, before InitProcess is called
 */
void
HeapCleanerLauncherIAm(void)
{
	am_heapcleaner_launcher = true;
}

static pid_t
hcworker_forkexec(void)
{
	char	   *av[10];
	int			ac = 0;

	av[ac++] = "postgres";
	av[ac++] = "--forkhcworker";
	av[ac++] = NULL;			/* filled in by postmaster_forkexec */
	av[ac] = NULL;

	Assert(ac < lengthof(av));

	return postmaster_forkexec(ac, av);
}

/*
 * We need this set from the outside, before InitProcess is called
 */
void
HeapCleanerWorkerIAm(void)
{
	am_heapcleaner_worker = true;
}
#endif

static void
_init_socket(pgsocket *sock)
{
	struct addrinfo *addrs = NULL,
			*addr, hints;
	ACCEPT_TYPE_ARG3 alen;
	int				ret;

	hints.ai_flags = AI_PASSIVE;
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_DGRAM;
	hints.ai_protocol = 0;
	hints.ai_addrlen = 0;
	hints.ai_addr = NULL;
	hints.ai_canonname = NULL;
	hints.ai_next = NULL;
	ret = pg_getaddrinfo_all("localhost", NULL, &hints, &addrs);
	Assert(ret == 0);

	for (addr = addrs; addr; addr = addr->ai_next)
	{
		if (addr->ai_family == AF_UNIX)
			continue;

		if ((*sock = socket(addr->ai_family, SOCK_DGRAM, 0)) == PGINVALID_SOCKET)
		{
			ereport(LOG,
					(errcode_for_socket_access(),
					errmsg("could not create socket for relation cleaner: %m")));
			continue;
		}

		if (bind(*sock, addr->ai_addr, addr->ai_addrlen) < 0)
		{
			ereport(LOG,
					(errcode_for_socket_access(),
					 errmsg("could not bind socket for relation cleaner: %m")));
			closesocket(*sock);
			*sock = PGINVALID_SOCKET;
			continue;
		}

		alen = sizeof(pgStatAddr);
		if (getsockname(*sock, (struct sockaddr *) &pgStatAddr, &alen) < 0)
		{
			ereport(LOG,
					(errcode_for_socket_access(),
					 errmsg("could not get address of socket for statistics collector: %m")));
			closesocket(*sock);
			*sock = PGINVALID_SOCKET;
			continue;
		}

		if (connect(*sock, (struct sockaddr *) &pgStatAddr, alen) < 0)
		{
			ereport(LOG,
					(errcode_for_socket_access(),
					 errmsg("could not connect socket for statistics collector: %m")));
			closesocket(*sock);
			*sock = PGINVALID_SOCKET;
			continue;
		}
		break;
	}
	if (!pg_set_noblock(*sock))
	{
		ereport(LOG,
				(errcode_for_socket_access(),
				 errmsg("could not set statistics collector socket to nonblocking mode: %m")));
		Assert(0);
	}
	pg_freeaddrinfo_all(hints.ai_family, addrs);
}

static void
FreeWorkerInfo(int code, Datum arg)
{
	if (MyWorkerInfo != NULL)
	{
		LWLockAcquire(HeapCleanerLock, LW_EXCLUSIVE);

		dlist_delete(&MyWorkerInfo->links);
		MyWorkerInfo->dbOid = InvalidOid;
		MyWorkerInfo->launchtime = 0;
		dlist_push_head(&HeapCleanerShmem->freeWorkers,
						&MyWorkerInfo->links);
		/* not mine anymore */
		MyWorkerInfo = NULL;

		LWLockRelease(HeapCleanerLock);
	}
	else
		elog(ERROR, "---> MyWorkerInfo is NULL");
}

void
HeapCleanerInit()
{
	_init_socket(&RelCleanerSock);
}

NON_EXEC_STATIC void
HeapCleanerLauncherMain(int argc, char *argv[])
{
	sigjmp_buf		local_sigjmp_buf;
	MemoryContext	bgheap_context;

	am_heapcleaner_launcher = true;

	/*
	 * Identify myself via ps
	 */
	init_ps_display(pgstat_get_backend_desc(B_BG_HEAPCLNR_LAUNCHER), "", "", "");

//	elog(LOG, "Launcher of background heap cleaners started");

	SetProcessingMode(InitProcessing);

	pqsignal(SIGHUP, SIGHUP_Handler);	/* set flag to read config file */
	pqsignal(SIGINT, StatementCancelHandler);
	pqsignal(SIGTERM, SIGTERM_Handler);
	pqsignal(SIGQUIT, quickdie); /* hard crash time */
	InitializeTimeouts();
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	pqsignal(SIGUSR2, SIG_IGN);
	pqsignal(SIGFPE, FloatExceptionHandler);
	pqsignal(SIGCHLD, SIG_DFL);

	BaseInit();

#ifndef EXEC_BACKEND
	InitProcess();
#endif

	InitPostgres(NULL, InvalidOid, NULL, InvalidOid, NULL, false);

	SetProcessingMode(NormalProcessing);

	/*
	 * Create a resource owner to keep track of our resources (currently only
	 * buffer pins).
	 */
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "Background cleaner");

	bgheap_context = AllocSetContextCreate(TopMemoryContext,
												"Background cleaner",
												ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(bgheap_context);

	if (sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		/* Since not using PG_TRY, must reset error stack by hand */
		error_context_stack = NULL;

		/* Prevent interrupts while cleaning up */
		HOLD_INTERRUPTS();

		/* Forget any pending QueryCancel or timeout request */
		disable_all_timeouts(false);
		/* Report the error to the server log */
		EmitErrorReport();

		/*
		 * These operations are really just a minimal subset of
		 * AbortTransaction().  We don't have very many resources to worry
		 * about in bgfreezer, but we do have LWLocks, buffers, and temp files.
		 */
		LWLockReleaseAll();
		pgstat_report_wait_end();
		AbortBufferIO();
		UnlockBuffers();
		if (CurrentResourceOwner)
			/* buffer pins are released here: */
			ResourceOwnerRelease(CurrentResourceOwner,
									RESOURCE_RELEASE_BEFORE_LOCKS,
									false, true);
		/* we needn't bother with the other ResourceOwnerRelease phases */
		AtEOXact_Buffers(false);
		AtEOXact_SMgr();
		AtEOXact_Files(false);
		AtEOXact_HashTables(false);

		/*
		 * Now return to normal top-level context and clear ErrorContext for
		 * next time.
		 */
		MemoryContextSwitchTo(bgheap_context);
		FlushErrorState();

		/* Flush any leaked data in the top-level context */
		MemoryContextResetAndDeleteChildren(bgheap_context);

		/* don't leave dangling pointers to freed memory */
		DatabaseListCxt = NULL;
		dlist_init(&DatabaseList);

		/*
		 * Make sure pgstat also considers our stat data as gone.  Note: we
		 * mustn't use autovac_refresh_stats here.
		 */
		pgstat_clear_snapshot();

		/* Now we can allow interrupts again */
		RESUME_INTERRUPTS();

		if (got_SIGTERM)
			proc_exit(0);
		/*
		 * Sleep at least 1 second after any error.  A write error is likely
		 * to be repeated, and we don't want to be filling the error logs as
		 * fast as we can.
		 */
		pg_usleep(1000000L);
	}

	/* We can now handle ereport(ERROR) */
	PG_exception_stack = &local_sigjmp_buf;

	/*
	 * Unblock signals (they were blocked when the postmaster forked us)
	 */
	PG_SETMASK(&UnBlockSig);

	main_launcher_loop();

	proc_exit(0);
}

void
HeapCleanerShmemInit(void)
{
	bool		found;

	HeapCleanerShmem = (HeapCleanerShmemStruct *)
		ShmemInitStruct("HeapCleaner Data",
						HeapCleanerShmemSize(),
						&found);

	if (!IsUnderPostmaster)
	{
		WorkerInfo	worker;
		int			i;

		Assert(!found);

		dlist_init(&HeapCleanerShmem->freeWorkers);
		dlist_init(&HeapCleanerShmem->runningWorkers);

		worker = (WorkerInfo) ((char *) HeapCleanerShmem +
							   MAXALIGN(sizeof(HeapCleanerShmemStruct)));

		/* initialize the WorkerInfo free list */
		for (i = 0; i < heapcleaner_max_workers; i++)
			dlist_push_head(&HeapCleanerShmem->freeWorkers,
							&worker[i].links);
	}
	else
		Assert(found);
	HeapCleanerShmem->startingWorker = NULL;

	TrancheId = LWLockNewTrancheId();
	LWLockRegisterTranche(TrancheId, "heapcleaner");
}

Size
HeapCleanerShmemSize(void)
{
	Size		size;

	/*
	 * Need the fixed struct and the array of WorkerInfoData.
	 */
	size = sizeof(HeapCleanerShmemStruct);
	size = MAXALIGN(size);
	size = add_size(size, mul_size(heapcleaner_max_workers,
								   sizeof(WorkerInfoData)));
	return size;
}

void
HeapCleanerSend(Relation relation, BlockNumber blkno)
{
	int		rc;
	CleanerMessage msg;

	if (RecoveryInProgress())
			return;

	if (RelCleanerSock == PGINVALID_SOCKET)
		return;

	msg.relid = RelationGetRelid(relation);
	msg.dbNode = relation->rd_node.dbNode;
	msg.spcNode = relation->rd_node.spcNode;
	msg.blkno = blkno;

	/* We'll retry after EINTR, but ignore all other failures */
	do
	{
		rc = send(RelCleanerSock, &msg, sizeof(CleanerMessage), 0);
	} while (rc < 0 && errno == EINTR);
}

NON_EXEC_STATIC void
HeapCleanerWorkerMain(int argc, char *argv[])
{
	sigjmp_buf	local_sigjmp_buf;

	am_heapcleaner_worker = true;

	/* Identify myself via ps */
	init_ps_display(pgstat_get_backend_desc(B_BG_HEAPCLNR_WORKER), "", "", "");

	SetProcessingMode(InitProcessing);

	pqsignal(SIGHUP, SIGHUP_Handler);
	pqsignal(SIGINT, StatementCancelHandler);
	pqsignal(SIGTERM, SIGTERM_Handler);
	pqsignal(SIGQUIT, quickdie);
	InitializeTimeouts();		/* establishes SIGALRM handler */

	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	pqsignal(SIGUSR2, SIGUSR2_Handler);
	pqsignal(SIGFPE, FloatExceptionHandler);
	pqsignal(SIGCHLD, SIG_DFL);

	/* Early initialization */
	BaseInit();

#ifndef EXEC_BACKEND
	InitProcess();
#endif

	if (sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		/* Prevents interrupts while cleaning up */
		HOLD_INTERRUPTS();

		/* Report the error to the server log */
		EmitErrorReport();

		proc_exit(0);
	}

	/* We can now handle ereport(ERROR) */
	PG_exception_stack = &local_sigjmp_buf;

	PG_SETMASK(&UnBlockSig);

	/* Initialization steps */
	LWLockAcquire(HeapCleanerLock, LW_EXCLUSIVE);
	if (HeapCleanerShmem->startingWorker != NULL)
	{
		MyWorkerInfo = HeapCleanerShmem->startingWorker;
		MyWorkerInfo->pid = getpid();
		HeapCleanerShmem->startingWorker = NULL;

		/* insert into the running list */
		dlist_push_head(&HeapCleanerShmem->runningWorkers, &MyWorkerInfo->links);

		on_shmem_exit(FreeWorkerInfo, 0);
		elog(LOG, "Worker started. pid=%d", MyWorkerInfo->pid);
	}
	else
		elog(ERROR, "No Starting worker!");
	LWLockRelease(HeapCleanerLock);

	if (OidIsValid(MyWorkerInfo->dbOid))
	{
		char		dbname[NAMEDATALEN];

		InitPostgres(NULL, MyWorkerInfo->dbOid, NULL, InvalidOid, dbname, false);
		SetProcessingMode(NormalProcessing);
		set_ps_display(dbname, false);
		elog(LOG, "Worker initialized with dbid=%d!", MyWorkerInfo->dbOid);
		main_worker_loop();
	}
	else
		elog(ERROR, "dbid not valid!");
	elog(LOG, "Worker EXIT");
	proc_exit(0);
}

static void
index_cleanup(Oid spcNode, Oid relid, BlockNumber blkno)
{
	Relation	heapRelation;
	Relation   *IndexRelations;
	int			nindexes;
	int			irnum;
	ItemPointerData	dead_tuples[MaxOffsetNumber];
	int			num_dead_tuples = 0;
	Buffer		buffer;
	OffsetNumber	offnum;
	Page page;
	ItemId lp;
	bool needLock;
	BlockNumber nblocks;
//	Oid relid;
	LOCKMODE	lmode = AccessExclusiveLock; /* ShareUpdateExclusiveLock; */
	LockRelId	onerelid;
	int tnum;
	Oid toast_relid;
	bool found_non_nbtree = false;
	Oid			save_userid;
	int			save_sec_context;
	int			save_nestlevel;

	if (RecoveryInProgress())
		return;

	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());

	CHECK_FOR_INTERRUPTS();

	if (ConditionalLockRelationOid(relid, lmode))
		heapRelation = try_relation_open(relid, AccessExclusiveLock/*NoLock*/);
	else
	{
//		elog(LOG, "--- heapRelation not locked:: %d %d", relid, blkno);
		heapRelation = NULL;
	}

	if (!heapRelation)
	{
//		elog(LOG, "--- heapRelation not exists:: %d %d", relid, blkno);
		PopActiveSnapshot();
		CommitTransactionCommand();
		return;
	}/* else
		elog(LOG, "--- heapRelation SUCCESS: %d %d", relid, blkno);
*/
	if (!(pg_class_ownercheck(RelationGetRelid(heapRelation), GetUserId()) ||
		  (pg_database_ownercheck(MyDatabaseId, GetUserId()) && !heapRelation->rd_rel->relisshared)))
	{
		if (heapRelation->rd_rel->relisshared)
			ereport(WARNING,
					(errmsg("skipping \"%s\" --- only superuser can vacuum it",
							RelationGetRelationName(heapRelation))));
		else if (heapRelation->rd_rel->relnamespace == PG_CATALOG_NAMESPACE)
			ereport(WARNING,
					(errmsg("skipping \"%s\" --- only superuser or database owner can vacuum it",
							RelationGetRelationName(heapRelation))));
		else
			ereport(WARNING,
					(errmsg("skipping \"%s\" --- only table or database owner can vacuum it",
							RelationGetRelationName(heapRelation))));
		relation_close(heapRelation, lmode);
		PopActiveSnapshot();
		CommitTransactionCommand();
		return;
	}

	if (heapRelation->rd_rel->relkind != RELKIND_RELATION &&
			heapRelation->rd_rel->relkind != RELKIND_MATVIEW &&
			heapRelation->rd_rel->relkind != RELKIND_TOASTVALUE &&
			heapRelation->rd_rel->relkind != RELKIND_PARTITIONED_TABLE)
	{
		ereport(WARNING,
				(errmsg("skipping \"%s\" --- cannot vacuum non-tables or special system tables",
						RelationGetRelationName(heapRelation))));
		relation_close(heapRelation, lmode);
		PopActiveSnapshot();
		CommitTransactionCommand();
		return;
	}
	if (RELATION_IS_OTHER_TEMP(heapRelation))
	{
		relation_close(heapRelation, lmode);
		elog(LOG, "--- S problem");
		PopActiveSnapshot();
		CommitTransactionCommand();
		return;
	}

	if (heapRelation->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
	{
		relation_close(heapRelation, lmode);
		PopActiveSnapshot();
		CommitTransactionCommand();
		return;
	}

	onerelid = heapRelation->rd_lockInfo.lockRelId;
	LockRelationIdForSession(&onerelid, lmode);
//	CheckTableNotInUse(heapRelation, "Heap Cleaner");

	needLock = !RELATION_IS_LOCAL(heapRelation);
	if (needLock)
		LockRelationForExtension(heapRelation, ExclusiveLock);
	nblocks = RelationGetNumberOfBlocks(heapRelation);
	if (needLock)
		UnlockRelationForExtension(heapRelation, ExclusiveLock);

	if (blkno > nblocks)
	{
		relation_close(heapRelation, lmode);
		UnlockRelationIdForSession(&onerelid, lmode);
		PopActiveSnapshot();
		CommitTransactionCommand();
		return;
	}

	GetUserIdAndSecContext(&save_userid, &save_sec_context);
	SetUserIdAndSecContext(heapRelation->rd_rel->relowner,
						   save_sec_context | SECURITY_RESTRICTED_OPERATION);
	save_nestlevel = NewGUCNestLevel();

	/* Create TID list */
	buffer = ReadBufferExtended(heapRelation, MAIN_FORKNUM, blkno, RBM_NORMAL, NULL);
	LockBuffer(buffer, BUFFER_LOCK_SHARE);
	page = (Page) BufferGetPage(buffer);
	for (offnum = FirstOffsetNumber; offnum < PageGetMaxOffsetNumber(page); offnum = OffsetNumberNext(offnum))
	{
		lp = PageGetItemId(page, offnum);
		if (ItemIdIsDead(lp) && ItemIdHasStorage(lp))
		{
			ItemPointerSet(&(dead_tuples[num_dead_tuples]),blkno, offnum);
			num_dead_tuples++;
		}
	}
	vac_open_indexes(heapRelation, RowExclusiveLock, &nindexes, &IndexRelations);

	/* Iterate across all index relations */
	for (irnum = 0; irnum < nindexes; irnum++)
	{
		if (IndexRelations[irnum]->rd_amroutine->amtargetdelete == NULL)
		{
			found_non_nbtree = true;
			continue;
		}

		quick_vacuum_index(IndexRelations[irnum], heapRelation,
						    dead_tuples,
							num_dead_tuples);

	}

	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
	if (!found_non_nbtree && ConditionalLockBufferForCleanup(buffer))
	{
		OffsetNumber	unusable[MaxOffsetNumber];
		int				nunusable = 0;

		START_CRIT_SECTION();

		/* Release DEAD heap tuples storage */
		for (tnum = 0; tnum < num_dead_tuples; tnum++)
		{
			OffsetNumber	offnum = ItemPointerGetOffsetNumber(&dead_tuples[tnum]);
			ItemId			lp = PageGetItemId(page, offnum);

			ItemIdSetUnused(lp);
			unusable[nunusable++] = offnum;
		}

		if (nunusable > 0)
		{
			XLogRecPtr	recptr;

			((PageHeader) page)->pd_prune_xid = InvalidTransactionId;
			PageRepairFragmentation(page);
			PageClearFull(page);
			MarkBufferDirty(buffer);

			recptr = log_heap_clean(heapRelation, buffer,
								NULL, 0,
								NULL, 0,
								unusable, nunusable,
								InvalidTransactionId);

			PageSetLSN(BufferGetPage(buffer), recptr);
		}

		END_CRIT_SECTION();

		UnlockReleaseBuffer(buffer);
	} else
		ReleaseBuffer(buffer);

	vac_close_indexes(nindexes, IndexRelations, NoLock);

	/* Roll back any GUC changes executed by index functions */
	AtEOXact_GUC(false, save_nestlevel);

	/* Restore userid and security context */
	SetUserIdAndSecContext(save_userid, save_sec_context);

	toast_relid = heapRelation->rd_rel->reltoastrelid;
	relation_close(heapRelation, NoLock);
	PopActiveSnapshot();
	CommitTransactionCommand();

	if (toast_relid != InvalidOid)
		index_cleanup(spcNode, toast_relid, blkno);

	UnlockRelationIdForSession(&onerelid, lmode);
}

bool
IsHeapCleanerLauncherProcess(void)
{
	return am_heapcleaner_launcher;
}

bool
IsHeapCleanerWorkerProcess(void)
{
	return am_heapcleaner_worker;
}

static void
launch_worker(Oid dbNode)
{
	WorkerInfo worker;
	dlist_node *wptr;

	LWLockAcquire(HeapCleanerLock, LW_EXCLUSIVE);
	wptr = dlist_pop_head_node(&HeapCleanerShmem->freeWorkers);
	worker = dlist_container(WorkerInfoData, links, wptr);
	worker->dbOid = dbNode;
	worker->launchtime = GetCurrentTimestamp();
	worker->nitems = 0;
	HeapCleanerShmem->startingWorker = worker;
	LWLockInitialize(&worker->WorkItemLock, TrancheId);

	SendPostmasterSignal(PMSIGNAL_START_HEAPCLNR_WORKER);
	LWLockRelease(HeapCleanerLock);
}

static WorkerInfo
look_for_worker(Oid dbNode)
{
	dlist_node	*node;
	WorkerInfo worker = NULL;

	LWLockAcquire(HeapCleanerLock, LW_EXCLUSIVE);
	if (!dlist_is_empty(&HeapCleanerShmem->runningWorkers))
		for (node = dlist_head_node(&HeapCleanerShmem->runningWorkers);
			 ;
			 node = dlist_next_node(&HeapCleanerShmem->runningWorkers, node))
		{
			if (((WorkerInfo)node)->dbOid == dbNode)
			{
				worker = (WorkerInfo) node;
				break;
			}
			if (!dlist_has_next(&HeapCleanerShmem->runningWorkers, node))
				break;
		}

	LWLockRelease(HeapCleanerLock);
	return worker;
}

static void
main_launcher_loop()
{
	int workers = 0;
	dlist_init(&WaitingList);

	if (RelCleanerSock == PGINVALID_SOCKET)
		elog(ERROR, "---RelCleanerSock == PGINVALID_SOCKET");

	while (!got_SIGTERM)
	{
		int				rc;
		int				len;
		CleanerMessage	msg;
		WorkerInfo		startingWorker;
		long			timeout;

		/* Process system signals */
		if (got_SIGHUP)
		{
			got_SIGHUP = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		ResetLatch(MyLatch);

		LWLockAcquire(HeapCleanerLock, LW_EXCLUSIVE);
		startingWorker = HeapCleanerShmem->startingWorker;
		LWLockRelease(HeapCleanerLock);

		/* Receive a data from backends */
		len = recv(RelCleanerSock, &msg, sizeof(CleanerMessage), 0);
		if ((len <= 0) && !dlist_is_empty(&WaitingList))
		{
			msg = pop_waiting_list();
			len = sizeof(CleanerMessage);
		}

		if (len > 0)
		{
			WorkerInfo worker;
//elog(LOG, "Launcher got a message: dbNode=%d relNode=%d spcNode=%d blkno=%d",msg.dbNode, msg.relid, msg.spcNode, msg.blkno);
			/* A message is received */
			if (len != sizeof(CleanerMessage))
				elog(ERROR, "Cleaner message size not consistent: %d. expected: %lu", len, sizeof(CleanerMessage));

			worker = look_for_worker(msg.dbNode);
			if ((worker != NULL) && (worker->nitems < WORK_ITEMS_MAX))
			{
				LWLockAcquire(&worker->WorkItemLock, LW_EXCLUSIVE);
				memcpy(&worker->buffer[worker->nitems], &msg, sizeof(CleanerMessage));
				worker->nitems++;
				LWLockRelease(&worker->WorkItemLock);
				kill(worker->pid, SIGUSR2);

				worker->launchtime = GetCurrentTimestamp();
			}
			else
			{
				push_waiting_list(&msg);
				if (startingWorker == NULL)
				{
					workers++;
					elog(LOG, "Launcher creates new worker, dbNode=%d from %d", msg.dbNode, workers);
					if (workers >= heapcleaner_max_workers)
						elog(ERROR, "Too many workers: %d", workers);
					launch_worker(msg.dbNode);
				}
			}
		}

		if (dlist_is_empty(&WaitingList))
		{
			timeout = -1L;
			/* Wait data or signals */
			rc = WaitLatchOrSocket(MyLatch,
							WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_SOCKET_READABLE,
							RelCleanerSock, timeout,
							WAIT_EVENT_BGHEAP_MAIN);
		}
		else
		{
			pg_usleep(1);
//			elog(LOG, "Launcher jump");
			continue;
		}
//		elog(LOG, "Launcher after wait");
		/* Emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
		{
			elog(LOG, "Heap Launcher exit with 1");
			proc_exit(1);
		}
	}
	{
		FILE *f = fopen("/home/andrey/test.log", "a+");
		fprintf(f, "Heap Launcher exit with 0\n");
		fclose(f);
	}
	elog(LOG, "Heap Launcher exit with 0");
	proc_exit(0);
}

static void
main_worker_loop(void)
{
	CleanerMessage	lbuf[WORK_ITEMS_MAX];
	int			nitems;

	while (!got_SIGTERM)
	{
		int	rc;

		if (got_SIGHUP)
		{
			got_SIGHUP = false;
			ProcessConfigFile(PGC_SIGHUP);
		}
		if (got_SIGUSR2)
			/* It is needed only for wakeup worker */
			got_SIGUSR2 = false;

		/* Task buffer is not empty */
		LWLockAcquire(&MyWorkerInfo->WorkItemLock, LW_SHARED);
		if (MyWorkerInfo->nitems > 0)
		{
			nitems = MyWorkerInfo->nitems;
			memcpy(lbuf, MyWorkerInfo->buffer, nitems*sizeof(CleanerMessage));
			MyWorkerInfo->nitems = 0;
		}
		LWLockRelease(&MyWorkerInfo->WorkItemLock);

		if (nitems > 0)
		{
			PG_TRY();
			{
				for (; nitems > 0; nitems--)
					index_cleanup(lbuf[nitems-1].spcNode, lbuf[nitems-1].relid, lbuf[nitems-1].blkno);

				QueryCancelPending = false;
			}
			PG_CATCH();
			{
				{
						FILE *f = fopen("/home/andrey/test.log", "a+");
						fprintf(f, "---> CATCH!\n");
						fclose(f);
				}
				HOLD_INTERRUPTS();
				EmitErrorReport();
				AbortOutOfAnyTransaction();
				FlushErrorState();

				/* restart our transaction for the following operations */
//				StartTransactionCommand();
				RESUME_INTERRUPTS();
			}
			PG_END_TRY();
//			CommitTransactionCommand();
		}

		/* Wait data or signals */
		rc = WaitLatch(MyLatch,
				WL_LATCH_SET /*| WL_TIMEOUT */| WL_POSTMASTER_DEATH,
				-1L,
				WAIT_EVENT_BGHEAP_MAIN);

		ResetLatch(MyLatch);

		/* Emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);
	}

	elog(LOG, "HEAP Cleaner (worker) exit with 0");
	proc_exit(0);
}

static CleanerMessage
pop_waiting_list(void)
{
	dlist_node *wptr;
	WorkWaitingList	*elem;

	Assert(!dlist_is_empty(&WaitingList));
	wptr = dlist_pop_head_node(&WaitingList);
	elem = dlist_container(WorkWaitingList, links, wptr);
	return elem->msg;
}

static void
push_waiting_list(CleanerMessage *msg)
{
	WorkWaitingList *elem = palloc(sizeof(WorkWaitingList));

	memcpy(&elem->msg, msg, sizeof(CleanerMessage));
	dlist_push_head(&WaitingList, &elem->links);
}

static void
SIGHUP_Handler(SIGNAL_ARGS)
{
	int	save_errno = errno;
elog(LOG, "--> catch sighup!");
	got_SIGHUP = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

/*
 * SIGTERM_Handler
 */
static void
SIGTERM_Handler(SIGNAL_ARGS)
{
	int save_errno = errno;

	got_SIGTERM = true;

	SetLatch(MyLatch);

	errno = save_errno;
}

static void
SIGUSR2_Handler(SIGNAL_ARGS)
{
	int save_errno = errno;

	got_SIGUSR2 = true;

	SetLatch(MyLatch);

	errno = save_errno;
}

/*
 * Main entry point for autovacuum launcher process, to be called from the
 * postmaster.
 */
int
StartHeapCleanerLauncher(void)
{
	pid_t		HeapCleanerLauncherPID;

#ifdef EXEC_BACKEND
	switch ((HeapCleanerLauncherPID = bghclauncher_forkexec()))
#else
	switch ((HeapCleanerLauncherPID = fork_process()))
#endif
	{
		case -1:
			ereport(LOG,
					(errmsg("could not fork heap cleaner launcher process: %m")));
			return 0;

#ifndef EXEC_BACKEND
		case 0:
			/* in postmaster child ... */
			InitPostmasterChild();

			/* Close the postmaster's sockets */
			ClosePostmasterPorts(false);

			HeapCleanerLauncherMain(0, NULL);
			break;
#endif
		default:
			return (int) HeapCleanerLauncherPID;
	}

	/* shouldn't get here */
	return 0;
}

int
StartHeapCleanerWorker(void)
{
	pid_t		worker_pid;

#ifdef EXEC_BACKEND
	switch ((worker_pid = hcworker_forkexec()))
#else
	switch ((worker_pid = fork_process()))
#endif
	{
		case -1:
			ereport(LOG,
					(errmsg("could not fork autovacuum worker process: %m")));
			return 0;

#ifndef EXEC_BACKEND
		case 0:
			/* in postmaster child ... */
			InitPostmasterChild();

			/* Close the postmaster's sockets */
			ClosePostmasterPorts(false);

			HeapCleanerWorkerMain(0, NULL);
			break;
#endif
		default:
			return (int) worker_pid;
	}

	/* shouldn't get here */
	return 0;
}
