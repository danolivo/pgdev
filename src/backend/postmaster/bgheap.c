/*
 * bgheap.c
 *
 * PostgreSQL integrated cleaner of HEAP and INDEX relations
 *
 * Made in autovacuum analogy. Uses 'Target' strategy for clean relations,
 * without full scan.
 * The cleaner to consist of one Launcher and Workers.
 * One worker corresponds to one database.
 * Launcher receives message from a backend {dbOid; relOid; blkno} by
 * socket and translate it to a worker by shared memory buffer. Worker receive
 * message and cleanup block of heap relation and its index relations.
 *
 * Copyright (c) 1996-2018, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/bgheap.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <unistd.h>
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/heapam_xlog.h"
#include "access/htup_details.h"
#include "access/nbtree.h"
#include "access/visibilitymap.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/pg_database.h"
#include "catalog/pg_namespace.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "commands/progress.h"
#include "commands/vacuum.h"
#include "common/ip.h"
#include "executor/executor.h"
#include "libpq/pqsignal.h"
#include "postmaster/bgheap.h"
#include "postmaster/fork_process.h"
#include "postmaster/postmaster.h"
#include "storage/condition_variable.h"
#include "storage/fd.h"
#include "storage/freespace.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "storage/pmsignal.h"
#include "storage/procarray.h"
#include "storage/smgr.h"
#include "tcop/tcopprot.h"
#include "utils/ps_status.h"
#include "utils/lsyscache.h"
#include "utils/resowner.h"
#include "utils/shash.h"
#include "utils/syscache.h"
#include "utils/timeout.h"

/*
 * Maximum number of task items in storage at a backend side before shipping to a
 * background heap cleaner
 */
#define BACKEND_DIRTY_ITEMS_MAX			(20)

/*
 * Maximum number of task items in waiting list at a Launcher side.
 * It is necessary to make it sufficiently large. If a number of arrived
 * messages exceed this, we ignore the remains and increase counter of missed
 * items.
 */
#define WAITING_MESSAGES_MAX_NUM		(100)

/*
 * Maximum number of dirty blocks which can keep a worker for each relation
 */
#define WORKER_DIRTYBLOCKS_MAX_NUM		(1000)

/* Maximum number of slots for dirty relations */
#define WORKER_RELATIONS_MAX_NUM		(100)

/* Maximum number of task items in a launcher/worker shared buffer */
#define WORKER_TASK_ITEMS_MAX			(1000)

/*
 * Maximum time interval which worker can idle without a task (ms)
 */
#define WORKER_IDLE_TIME_DURATION_MAX	(5000)

/* Minimal info for cleanup a block of heap relation and its index relations */
typedef struct CleanerMessage
{
	Oid				dbNode;		/* Database ID */
	Oid				relid;		/* Relation ID */
	int				blkno;		/* Block number */
	TransactionId	xid;
	uint16			hits;
} CleanerMessage;

typedef struct WorkerTask
{
	BlockNumber		blkno;
	TransactionId	lastXid;
	uint16			hits;
} WorkerTask;

/*
 * Shared memory data to control and two-way communication with worker
 */
typedef struct WorkerInfoData
{
	dlist_node		links;
	Oid				dbOid;					/* Database ID of the worker */
	TimestampTz 	launchtime;				/* To define a time of last worker activity */
	int				pid;					/* Used for system signals passing */
	CleanerMessage	buffer[WORKER_TASK_ITEMS_MAX];	/* Array of work items */
	int				nitems;					/* Number of work items in buffer */
	LWLock			WorkItemLock;			/* Locker for safe buffer access */
	int				id;						/* Used for Internal launcher buffers management */
} WorkerInfoData;

typedef struct WorkerInfoData *WorkerInfo;

/*
 * Shared memory lists.
 * Launcher get an element from freeWorkers list and init startingWorker value.
 * Worker set startingWorker to NULL value after startup and add himself
 * to runningWorkers list.
 */
typedef struct HeapCleanerShmemStruct
{
	dlist_head	freeWorkers;
	dlist_head	runningWorkers;
	WorkerInfo	startingWorker;
} HeapCleanerShmemStruct;

typedef struct DirtyRelation
{
	Oid			relid;
	SHTAB		*items;
} DirtyRelation;

/*
 * Structure for debug purposes only
 */
typedef struct stat_struct
{
	/* - hash key - */
	Oid			dbOid;
	Oid			reloid;
	BlockNumber	blkno;

	/* --- DATA --- */
	int		hits;
	int 	tuples_removed;
	int		cleaning_cycles;
} stat_struct;

static PSHTAB	worker_stat = NULL;
static int		worker_zero_cleaned_blocks_num = 0;
static int		worker_zero_cleaned_hits_num = 0;
static int		worker_not_found_blocks_num = 0;
static int		worker_not_found_blocks_hits = 0;
static int		worker_total_hits_received = 0;
static int		worker_waiting_lists_hits = 0;
static int		worker_tuple_dead_but_HOT = 0;
static int		worker_tuple_recently_dead = 0;
static int		launcher_total_received_hits = 0;
static uint32	MissedBlocksNum = 0;

static void launcher_stat_print(void);

static MemoryContext BGHeapMemCxt = NULL;

/*
 * Table of database relations.
 * For each relation we support a waiting list of dirty blocks.
 */
static PSHTAB PrivateRelationsTable = NULL;

static bool am_heapcleaner_launcher = false;
static bool am_heapcleaner_worker = false;

int heapcleaner_max_workers = 10;
static WorkerInfo MyWorkerInfo = NULL;
static HeapCleanerShmemStruct *HeapCleanerShmem;
static dlist_head DatabaseList = DLIST_STATIC_INIT(DatabaseList);
static MemoryContext DatabaseListCxt = NULL;

/*
 * Hash table for collection of dirty blocks after heap_page_prune() action
 * at a Backend side.
 */
PSHTAB	dblocks = NULL;

/* Signal handling */
static volatile sig_atomic_t got_SIGHUP = false;
static volatile sig_atomic_t got_SIGTERM = false;
static volatile sig_atomic_t got_SIGUSR2 = false;

NON_EXEC_STATIC pgsocket HeapCleanerSock = PGINVALID_SOCKET;
static struct sockaddr_storage HeapCleanerSockAddr;

static int TrancheId;

/*
 * Hash table for waiting data: one for each active worker and one for all messages
 * intended to not-running workers. Reasons:
 * 1. Worker in startup process.
 * 2. Another worker in startup and we can't launch new worker.
 * 3. We have not free slots for new workers.
 */
PSHTAB	*wTab;

/*
 * Parameters of each hash table
 */
SHTABCTL wTabCtl;

#ifdef EXEC_BACKEND
static pid_t hclauncher_forkexec(void);
static pid_t hcworker_forkexec(void);
#endif

static uint64 CleanerMessageHashFunc(void *key, uint64 size, uint64 base);
static PSHTAB cleanup_relations(DirtyRelation *res, SHTAB *AuxiliaryList, bool got_SIGTERM);
static bool DefaultCompareFunc(void* bucket1, void* bucket2);
NON_EXEC_STATIC void HeapCleanerLauncherMain(int argc, char *argv[]) pg_attribute_noreturn();
NON_EXEC_STATIC void HeapCleanerWorkerMain(int argc, char *argv[]) pg_attribute_noreturn();
static bool isEqualMsgs(void *arg1, void *arg2);

static void launch_worker(Oid dbNode);
static WorkerInfo look_for_worker(Oid dbNode);
static void main_launcher_loop(void);
static void main_worker_loop(void);

static void SIGHUP_Handler(SIGNAL_ARGS);
static void SIGTERM_Handler(SIGNAL_ARGS);
static void SIGUSR2_Handler(SIGNAL_ARGS);
static void backend_send_dirty_blocks(void);

/* Functions for debug purposes */
static void stat_collector_print(void);

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

/*
 * Send dirty blocks from a collector's table to launcher at the end of transaction
 */
void
AtEOXact_BGHeap_tables(bool isCommit)
{
	MemoryContext oldMemCxt;

	if (BGHeapMemCxt == NULL)
		BGHeapMemCxt = AllocSetContextCreate(BGHeapMemCxt, "bgheap",
											 ALLOCSET_DEFAULT_SIZES);
	oldMemCxt = MemoryContextSwitchTo(BGHeapMemCxt);

	backend_send_dirty_blocks();

	MemoryContextSwitchTo(oldMemCxt);
}

static void
save_to_list(PSHTAB AuxiliaryList, WorkerTask *item)
{
	bool found;

	WorkerTask *new_item = (WorkerTask *)
							SHASH_Search(AuxiliaryList,
							(void *) &(item->blkno),
							HASH_ENTER, &found);

	Assert((new_item != NULL) && (!found));
	new_item->hits = item->hits;
	new_item->lastXid = item->lastXid;
}

/*
 * Main logic of HEAP and index relations cleaning
 */
static PSHTAB
cleanup_relations(DirtyRelation *res, PSHTAB AuxiliaryList, bool got_SIGTERM)
{
	Relation		heapRelation;
	Relation   		*IndexRelations;
	int				nindexes;
	LOCKMODE		lockmode = AccessShareLock;
	WorkerTask		*item;

	Assert(res != NULL);
	Assert(res->items != NULL);
	Assert(AuxiliaryList != NULL);
	Assert(SHASH_Entries(AuxiliaryList) == 0);

	if (SHASH_Entries(res->items) == 0)
		return AuxiliaryList;

	if (RecoveryInProgress())
	{
		SHASH_Clean(res->items);
		return AuxiliaryList;
	}

	CHECK_FOR_INTERRUPTS();

	StartTransactionCommand();

	/*
	 * At this point relation availability is not guaranteed.
	 * Make safe test to check this.
	 */
	heapRelation = try_relation_open(res->relid, lockmode);

	if (!heapRelation)
	{
		elog(LOG, "[%d] Cleanup: UnSuccessful opening. Relation deleted? dbOid=%d", res->relid, MyWorkerInfo->dbOid);
		CommitTransactionCommand();
		SHASH_Clean(res->items);
		return AuxiliaryList;
	}

	/*
	 * Check relation type similarly vacuum
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
		relation_close(heapRelation, lockmode);
		CommitTransactionCommand();
		SHASH_Clean(res->items);
		return AuxiliaryList;
	}

	/* Open and lock index relations correspond to the heap relation */
	vac_open_indexes(heapRelation, RowExclusiveLock, &nindexes, &IndexRelations);

	/* Main cleanup cycle */
	for (SHASH_SeqReset(res->items);
		 (item = (WorkerTask *) SHASH_SeqNext(res->items)) != NULL; )
	{
		BlockNumber		nblocks;
		bool			needLock;
		Buffer			buffer;
		ItemPointerData	dead_tuples[MaxOffsetNumber];
		int				num_dead_tuples = 0;
		OffsetNumber	offnum;
		int				irnum;
		Page 			page;
		ItemId 			lp;
		int				tnum;
		bool			found_non_nbtree = false;

		Assert(item->hits > 0);

		if (!got_SIGTERM &&
			(res->items->Header.ElementsMaxNum/SHASH_Entries(res->items) > 3) &&
			(item->lastXid > GetOldestXmin(NULL, PROCARRAY_FLAGS_VACUUM)) &&
			(item->hits < 10))
		{
			/*
			 * Skip block cleaning and save it to a waiting list
			 * for the next iteration.
			 */
			save_to_list(AuxiliaryList, item);
			continue;
		}

		needLock = !RELATION_IS_LOCAL(heapRelation);
		if (needLock)
			LockRelationForExtension(heapRelation, ExclusiveLock);
		nblocks = RelationGetNumberOfBlocks(heapRelation);
		if (needLock)
			UnlockRelationForExtension(heapRelation, ExclusiveLock);

		if (item->blkno >= nblocks)
		{
			/*
			 * Block was deleted early.
			 * Skip cleaning and drop block from waiting list.
			 * Accumulate some stats before continue.
			 */
			worker_not_found_blocks_num++;
			worker_not_found_blocks_hits += item->hits;
			continue;
		}

		/* Create TID list */
		if (!got_SIGTERM)
			buffer = ReadBufferExtended(heapRelation, MAIN_FORKNUM, item->blkno, RBM_NORMAL_NO_READ, NULL);
		else
			buffer = ReadBuffer(heapRelation, item->blkno);

		if (BufferIsInvalid(buffer))
		{
			/*
			 * Buffer was already evicted from shared buffers
			 */
			Assert(!got_SIGTERM);
			save_to_list(AuxiliaryList, item);
			continue;
		}

		page = BufferGetPage(buffer);
		if (!ConditionalLockBuffer(buffer))
		{
			/* Can't lock buffer. */
			ReleaseBuffer(buffer);
			save_to_list(AuxiliaryList, item);
			continue;
		}

		if (!IsBufferDirty(buffer))
		{
			/* Skip block if it is not dirty */
			UnlockReleaseBuffer(buffer);
			continue;
		}

		/* Collect dead tuples TID's */
		for (offnum = FirstOffsetNumber;
			 offnum < PageGetMaxOffsetNumber(page);
			 offnum = OffsetNumberNext(offnum))
		{
			lp = PageGetItemId(page, offnum);
			if (ItemIdIsDead(lp) /*&& ItemIdHasStorage(lp)*/)
			{
				ItemPointerSet(&(dead_tuples[num_dead_tuples]), item->blkno, offnum);
				num_dead_tuples++;
			}
		}
		LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

		if (num_dead_tuples == 0)
		{
			/*
			 * Block is clear. Accumulate some stats and continue.
			 */
			worker_zero_cleaned_blocks_num++;
			worker_zero_cleaned_hits_num += item->hits;
			ReleaseBuffer(buffer);
			continue;
		}

		/* Iterate across all index relations */
		for (irnum = 0; irnum < nindexes; irnum++)
		{
			if (IndexRelations[irnum]->rd_amroutine->amtargetdelete == NULL)
			{
				/*
				 * Can clean only btree indexes now.
				 */
				found_non_nbtree = true;
				continue;
			}

			quick_vacuum_index(IndexRelations[irnum], heapRelation,
							   dead_tuples,
							   num_dead_tuples);
		}

		/*
		 * If heap relation has not only b-tree indexes, can't clean heap block.
		 */
		if (!found_non_nbtree)
		{
			OffsetNumber	unusable[MaxOffsetNumber];
			int				nunusable = 0;
			Size			freespace;

			if (!ConditionalLockBufferForCleanup(buffer))
			{
				ReleaseBuffer(buffer);
				save_to_list(AuxiliaryList, item);
				continue;
			}

			START_CRIT_SECTION();

			/* Release DEAD heap tuples storage */
			for (tnum = 0; tnum < num_dead_tuples; tnum++)
			{
				OffsetNumber	offnum = ItemPointerGetOffsetNumber(&dead_tuples[tnum]);
				ItemId			lp = PageGetItemId(page, offnum);

				Assert(ItemIdIsDead(lp));
				ItemIdSetUnused(lp);
				unusable[nunusable++] = offnum;
			}

			if (nunusable > 0)
			{
				PageRepairFragmentation(page);
				PageClearFull(page);
				MarkBufferDirty(buffer);

				if (RelationNeedsWAL(heapRelation))
				{
					XLogRecPtr	recptr = log_heap_clean(heapRelation, buffer,
														NULL, 0,
														NULL, 0,
														unusable, nunusable,
														InvalidTransactionId);

					PageSetLSN(BufferGetPage(buffer), recptr);
				}
			}

			END_CRIT_SECTION();
			freespace = PageGetHeapFreeSpace(page);

			UnlockReleaseBuffer(buffer);
			RecordPageWithFreeSpace(heapRelation, item->blkno, freespace);
			pgstat_update_heap_dead_tuples(heapRelation, nunusable);
		}
		/*
		 * ToDo: that we will do with TOAST relation?
		 */
	}

	vac_close_indexes(nindexes, IndexRelations, NoLock);
	relation_close(heapRelation, lockmode);
	CommitTransactionCommand();

	{
		SHTAB	*tmp = res->items;
		SHASH_Clean(tmp);
		res->items = AuxiliaryList;
		return tmp;
	}
}

static bool
DefaultCompareFunc(void* bucket1, void* bucket2)
{
	WorkerTask	*item1 = (WorkerTask *) bucket1;
	WorkerTask	*item2 = (WorkerTask *) bucket2;

	if (item1->blkno == item2->blkno)
		return true;
	else
		return false;
}

/*
 * Free any launcher resources before close process
 */
static void
FreeLauncherInfo(int code, Datum arg)
{
	int counter;

	for (counter = 0; counter < heapcleaner_max_workers+1; counter++)
		SHASH_Destroy(wTab[counter]);
	pfree(wTab);
}

/*
 * Return a WorkerInfo to the free list
 */
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

/*
 * Initialize communication with backends
 */
void
HeapCleanerInit()
{
	struct addrinfo *addrs = NULL,
			*addr, hints;
	ACCEPT_TYPE_ARG3 alen;
	int				ret;
	fd_set		rset;
	struct timeval tv;
	char		test_byte;
	int			sel_res;
	int			tries = 0;

#define TESTBYTEVAL ((char) 199)

	/*
	 * Create the UDP socket for sending and receiving statistic messages
	 */
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
#ifdef HAVE_UNIX_SOCKETS
		/* Ignore AF_UNIX sockets, if any are returned. */
		if (addr->ai_family == AF_UNIX)
			continue;
#endif

		if (++tries > 1)
			ereport(LOG,
					(errmsg("trying another address for the statistics collector")));

		if ((HeapCleanerSock = socket(addr->ai_family, SOCK_DGRAM, 0)) == PGINVALID_SOCKET)
		{
			ereport(LOG,
					(errcode_for_socket_access(),
					errmsg("could not create socket for relation cleaner: %m")));
			continue;
		}

		if (bind(HeapCleanerSock, addr->ai_addr, addr->ai_addrlen) < 0)
		{
			ereport(LOG,
					(errcode_for_socket_access(),
					 errmsg("could not bind socket for relation cleaner: %m")));
			closesocket(HeapCleanerSock);
			HeapCleanerSock = PGINVALID_SOCKET;
			continue;
		}

		alen = sizeof(HeapCleanerSockAddr);
		if (getsockname(HeapCleanerSock, (struct sockaddr *) &HeapCleanerSockAddr, &alen) < 0)
		{
			ereport(LOG,
					(errcode_for_socket_access(),
					 errmsg("could not get address of socket for statistics collector: %m")));
			closesocket(HeapCleanerSock);
			HeapCleanerSock = PGINVALID_SOCKET;
			continue;
		}

		if (connect(HeapCleanerSock, (struct sockaddr *) &HeapCleanerSockAddr, alen) < 0)
		{
			ereport(LOG,
					(errcode_for_socket_access(),
					 errmsg("could not connect socket for statistics collector: %m")));
			closesocket(HeapCleanerSock);
			HeapCleanerSock = PGINVALID_SOCKET;
			continue;
		}
		/*
		 * Try to send and receive a one-byte test message on the socket. This
		 * is to catch situations where the socket can be created but will not
		 * actually pass data (for instance, because kernel packet filtering
		 * rules prevent it).
		 */
		test_byte = TESTBYTEVAL;

retry1:
		if (send(HeapCleanerSock, &test_byte, 1, 0) != 1)
		{
			if (errno == EINTR)
				goto retry1;	/* if interrupted, just retry */
			ereport(LOG,
					(errcode_for_socket_access(),
					 errmsg("could not send test message on socket for statistics collector: %m")));
			closesocket(HeapCleanerSock);
			HeapCleanerSock = PGINVALID_SOCKET;
			continue;
		}

		/*
		 * There could possibly be a little delay before the message can be
		 * received.  We arbitrarily allow up to half a second before deciding
		 * it's broken.
		 */
		for (;;)				/* need a loop to handle EINTR */
		{
			FD_ZERO(&rset);
			FD_SET(HeapCleanerSock, &rset);

			tv.tv_sec = 0;
			tv.tv_usec = 500000;
			sel_res = select(HeapCleanerSock + 1, &rset, NULL, NULL, &tv);
			if (sel_res >= 0 || errno != EINTR)
				break;
		}
		if (sel_res < 0)
		{
			ereport(LOG,
					(errcode_for_socket_access(),
					 errmsg("select() failed in statistics collector: %m")));
			closesocket(HeapCleanerSock);
			HeapCleanerSock = PGINVALID_SOCKET;
			continue;
		}
		if (sel_res == 0 || !FD_ISSET(HeapCleanerSock, &rset))
		{
			/*
			 * This is the case we actually think is likely, so take pains to
			 * give a specific message for it.
			 *
			 * errno will not be set meaningfully here, so don't use it.
			 */
			ereport(LOG,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("test message did not get through on socket for statistics collector")));
			closesocket(HeapCleanerSock);
			HeapCleanerSock = PGINVALID_SOCKET;
			continue;
		}

		test_byte++;			/* just make sure variable is changed */

retry2:
		if (recv(HeapCleanerSock, &test_byte, 1, 0) != 1)
		{
			if (errno == EINTR)
				goto retry2;	/* if interrupted, just retry */
			ereport(LOG,
					(errcode_for_socket_access(),
					 errmsg("could not receive test message on socket for statistics collector: %m")));
			closesocket(HeapCleanerSock);
			HeapCleanerSock = PGINVALID_SOCKET;
			continue;
		}

		if (test_byte != TESTBYTEVAL)	/* strictly paranoia ... */
		{
			ereport(LOG,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("incorrect test message transmission on socket for statistics collector")));
			closesocket(HeapCleanerSock);
			HeapCleanerSock = PGINVALID_SOCKET;
			continue;
		}

		/* If we get here, we have a working socket */

		break;
	}

	/* Did we find a working address? */
	if (!addr || HeapCleanerSock == PGINVALID_SOCKET)
		goto startup_failed;

	/*
	 * Set the socket to non-blocking IO.  This ensures that if the collector
	 * falls behind, statistics messages will be discarded; backends won't
	 * block waiting to send messages to the collector.
	 */
	if (!pg_set_noblock(HeapCleanerSock))
	{
		ereport(LOG,
				(errcode_for_socket_access(),
				 errmsg("could not set statistics collector socket to nonblocking mode: %m")));
		goto startup_failed;
	}
	pg_freeaddrinfo_all(hints.ai_family, addrs);

	return;

startup_failed:
	ereport(LOG,
			(errmsg("disabling heap cleaner for lack of working socket")));

	if (addrs)
		pg_freeaddrinfo_all(hints.ai_family, addrs);

	if (HeapCleanerSock != PGINVALID_SOCKET)
		closesocket(HeapCleanerSock);
	HeapCleanerSock = PGINVALID_SOCKET;
}

/*
 * Start and initialization logic of a launcher
 */
NON_EXEC_STATIC void
HeapCleanerLauncherMain(int argc, char *argv[])
{
	sigjmp_buf		local_sigjmp_buf;
	MemoryContext	bgheap_context;
	int				counter;

	am_heapcleaner_launcher = true;

	/*
	 * Identify myself via ps
	 */
	init_ps_display(pgstat_get_backend_desc(B_BG_HEAPCLNR_LAUNCHER), "", "", "");

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

	wTabCtl.FillFactor = 0.75;
	wTabCtl.ElementsMaxNum = WAITING_MESSAGES_MAX_NUM;
	wTabCtl.ElementSize = sizeof(CleanerMessage);
	wTabCtl.KeySize = 2 * sizeof(Oid) + sizeof(BlockNumber);
	wTabCtl.HashFunc = CleanerMessageHashFunc;
	wTabCtl.CompFunc = isEqualMsgs;

	Assert(heapcleaner_max_workers == 10);
	wTab = palloc0((heapcleaner_max_workers+1)*sizeof(PSHTAB));
	for (counter = 0; counter < heapcleaner_max_workers+1; counter++)
		wTab[counter] = SHASH_Create(wTabCtl);
	on_shmem_exit(FreeLauncherInfo, 0);
elog(LOG, "-> Launcher Started!");
	main_launcher_loop();

	proc_exit(0);
}

static bool
backend_store_dirty_block(CleanerMessage *msg)
{
	bool			found;
	CleanerMessage	*rec;

	/* Initialize collector's hash table */
	if (dblocks == NULL)
	{
		SHTABCTL	ctl;

		ctl.ElementsMaxNum = BACKEND_DIRTY_ITEMS_MAX;
		ctl.FillFactor = 0.75;
		ctl.ElementSize = sizeof(CleanerMessage);
		ctl.KeySize = 2 * sizeof(Oid) + sizeof(BlockNumber);
		ctl.HashFunc = CleanerMessageHashFunc;
		ctl.CompFunc = isEqualMsgs;
		dblocks = SHASH_Create(ctl);
	}

	/* Search for new position or for duplicates */
	rec = (CleanerMessage *) SHASH_Search(dblocks, (void *) msg, SHASH_ENTER, &found);

	if (!rec)
		return true;

	if (!found)
		rec->hits = 0;

	rec->xid = msg->xid;
	rec->hits++;
	return false;
}

/*
 * Send a package of dirty blocks from a Backend to the Cleaner.
 * Form the package by passing across the table. During this pass table is
 * cleaned. Sign of clean row is (dbNode == 0) condition.
 */
static void
backend_send_dirty_blocks(void)
{
	int				rc;
	CleanerMessage	*rec;
	CleanerMessage	data[BACKEND_DIRTY_ITEMS_MAX];
	int				nitems = 0;

	if (dblocks == NULL)
		return;

	if (SHASH_Entries(dblocks) == 0)
		return;

	Assert(SHASH_Entries(dblocks) <= BACKEND_DIRTY_ITEMS_MAX);

	for (SHASH_SeqReset(dblocks);
		(rec = SHASH_SeqNext(dblocks)) != NULL; )
	{
		Assert(rec->hits > 0);
		Assert(nitems < BACKEND_DIRTY_ITEMS_MAX);
		data[nitems++] = *rec;
	}
	SHASH_Clean(dblocks);

	/* Send data to launcher.
	 * We'll retry after EINTR, but ignore all other failures
	 */
	do
	{
		rc = send(HeapCleanerSock, data, nitems*sizeof(CleanerMessage), 0);
	} while (rc < 0 && errno == EINTR);
}

/*
 * Send a dirty block of relation to the Cleaner.
 * Before shipping to the Cleaner dirty blocks collects in a simple hash
 * table. Duplicates are discarded.
 */
void
HeapCleanerSend(Relation relation, BlockNumber blkno)
{
	CleanerMessage msg;
	MemoryContext	oldMemCxt;

	if (BGHeapMemCxt == NULL)
		BGHeapMemCxt = AllocSetContextCreate(BGHeapMemCxt, "bgheap",
											 ALLOCSET_DEFAULT_SIZES);

	if (RecoveryInProgress())
			return;

	if (HeapCleanerSock == PGINVALID_SOCKET)
		return;

	/*
	 * ToDo: Cleaning of system relations make instability in regression tests
	 * output order
	 */
	if (IsSystemRelation(relation))
		return;

	/*
	 * Check the relation type.
	 * Not all relations can be cleaned now (similarly vacuum).
	 */
	if (relation->rd_rel->relkind != RELKIND_RELATION &&
		relation->rd_rel->relkind != RELKIND_MATVIEW &&
		relation->rd_rel->relkind != RELKIND_TOASTVALUE &&
		relation->rd_rel->relkind != RELKIND_PARTITIONED_TABLE)
	{
		ereport(WARNING,
				(errmsg("skipping \"%s\" --- cannot clean non-tables or special system tables",
						RelationGetRelationName(relation))));
		return;
	}

	if (RELATION_IS_OTHER_TEMP(relation))
		return;

	if (relation->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
		return;

	oldMemCxt = MemoryContextSwitchTo(BGHeapMemCxt);

	msg.dbNode = relation->rd_node.dbNode;
	msg.relid = RelationGetRelid(relation);
	msg.blkno = blkno;
	msg.xid = GetCurrentTransactionIdIfAny();

	if (backend_store_dirty_block(&msg))
		backend_send_dirty_blocks();

	MemoryContextSwitchTo(oldMemCxt);
}

/*
 * Start and initialization logic of a worker
 */
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
	pqsignal(SIGUSR2, SIGUSR2_Handler); /* Signal: Buffer contains a message */
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
	}
	else
		elog(ERROR, "No Starting worker!");
	LWLockRelease(HeapCleanerLock);

	if (OidIsValid(MyWorkerInfo->dbOid))
	{
		char		dbname[NAMEDATALEN];
		SHTABCTL	pr_ctl;

		pgstat_report_heapcleaner(MyWorkerInfo->dbOid);
		InitPostgres(NULL, MyWorkerInfo->dbOid, NULL, InvalidOid, dbname, false);
		SetProcessingMode(NormalProcessing);
		set_ps_display(dbname, false);

		pr_ctl.ElementsMaxNum = 1000;
		pr_ctl.FillFactor = 0.5;
		pr_ctl.HashFunc = DefaultHashValueFunc;
		pr_ctl.CompFunc = DefaultCompareFunc;
		pr_ctl.ElementSize = sizeof(DirtyRelation);
		pr_ctl.KeySize = sizeof(int32);

		PrivateRelationsTable = SHASH_Create(pr_ctl);

		if (worker_stat == NULL)
		{
			SHTABCTL ctl;

			ctl.ElementsMaxNum = 100000;
			ctl.FillFactor = 0.5;
			ctl.HashFunc = CleanerMessageHashFunc;
			ctl.CompFunc = DefaultCompareFunc;
			ctl.ElementSize = sizeof(stat_struct);
			ctl.KeySize = 2 * sizeof(Oid) + sizeof(BlockNumber);

			worker_stat = SHASH_Create(ctl);
		}

		/* Add tracking info to pgstat */
		pgstat_progress_start_command(PROGRESS_COMMAND_CLEANER, MyWorkerInfo->dbOid);

		main_worker_loop();
	}
	else
		elog(ERROR, "dbid %d not valid!", MyWorkerInfo->dbOid);

	proc_exit(0);
}

/*
 * Compare messages
 */
static bool
isEqualMsgs(void *arg1, void *arg2)
{
	CleanerMessage *msg1 = arg1;
	CleanerMessage *msg2 = arg2;

	Assert(arg1 != NULL);
	Assert(arg2 != NULL);

	if (msg1->blkno != msg2->blkno)
		return false;
	if (msg1->relid != msg2->relid)
		return false;
	if (msg1->dbNode != msg2->dbNode)
		return false;

	return true;
}

/*
 * IsHeapCleaner functions
 *		Return whether this is either a launcher heap cleaner process or
 *		a worker process.
 */
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

/*
 * Send signal to Postmaster for launch new worker instance.
 * Eject one node from freeWorkers list and assign to startingWorker
 * HeapCleanerLock must be exclusive-locked
 */
static void
launch_worker(Oid dbNode)
{
	WorkerInfo worker;
	dlist_node *wptr;

	if (dlist_is_empty(&HeapCleanerShmem->freeWorkers))
		elog(ERROR, "NO a free slot for background cleaner worker!");

	wptr = dlist_pop_head_node(&HeapCleanerShmem->freeWorkers);
	worker = dlist_container(WorkerInfoData, links, wptr);
	worker->dbOid = dbNode;
	worker->launchtime = GetCurrentTimestamp();
	worker->nitems = 0;
	HeapCleanerShmem->startingWorker = worker;

	SendPostmasterSignal(PMSIGNAL_START_HEAPCLNR_WORKER);
}

/*
 * Return worker that initialized for dbNode database or NULL
 * Caller need to acquire share lock on HeapCleanerLock
 */
static WorkerInfo
look_for_worker(Oid dbNode)
{
	dlist_node	*node;
	WorkerInfo worker = NULL;

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

	return worker;
}

/*
 * Entry point of a launcher behavior logic
 */
static void
main_launcher_loop()
{
	uint32	TmpMissedBlocksNumber = MissedBlocksNum;
	bool	found;
	long	timeout = -1L;

	Assert(HeapCleanerSock != PGINVALID_SOCKET);

	/*
	 * Do work until not catch a SIGTERM signal and all blocks
	 * not shipped to workers.
	 * timeout <= 0 tells us that the launcher does not have any work right now,
	 * and it will sleep until the socket message or the system signal.
	 */
	while (!got_SIGTERM || (timeout > 0))
	{
		int				rc;
		int				len;
		CleanerMessage	*msg;
		CleanerMessage	table[BACKEND_DIRTY_ITEMS_MAX];
		WorkerInfo		startingWorker;
		WorkerInfo		worker;
		dlist_node		*node;

		timeout = -1L;

		if (TmpMissedBlocksNumber != MissedBlocksNum)
		{
			elog(LOG, "Missed blocks: cur=%d delta=%d", MissedBlocksNum, MissedBlocksNum-TmpMissedBlocksNumber);
			TmpMissedBlocksNumber = MissedBlocksNum;
		}
		/* Process system signals */
		if (got_SIGHUP)
		{
			got_SIGHUP = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		ResetLatch(MyLatch);

		/* At First, receive a message from backend */
		len = recv(HeapCleanerSock, table, BACKEND_DIRTY_ITEMS_MAX*sizeof(CleanerMessage), 0);

		LWLockAcquire(HeapCleanerLock, LW_EXCLUSIVE);
		startingWorker = HeapCleanerShmem->startingWorker;

		/*
		 * Message was received from socket.
		 */
		if (len > 0)
		{
			CleanerMessage *mptr;

			if (len%sizeof(CleanerMessage) != 0)
				elog(ERROR, "INCORRECT Message size!");

			/*
			 * Push all messages received from a backend to general
			 * hash table with waiting messages.
			 */
			for (mptr = table; ((char *)mptr-(char *)table) < len; mptr++)
			{
				launcher_total_received_hits += mptr->hits;
				worker = look_for_worker(mptr->dbNode);
				if (worker)
					msg = (CleanerMessage *) SHASH_Search(wTab[worker->id], mptr, SHASH_ENTER, &found);
				else
					msg = (CleanerMessage *) SHASH_Search(wTab[heapcleaner_max_workers], mptr, SHASH_ENTER, &found);

				if (msg == NULL)
					/*
					 * Hash table is FULL and we can't insert a message
					 */
					MissedBlocksNum++;
				else if (found)
				{
					msg->hits++;
					msg->xid = (msg->xid > mptr->xid) ? msg->xid : mptr->xid;
				}
				else
					memcpy(msg, mptr, sizeof(CleanerMessage));
			}
		}

		/*
		 * Main waiting list parsing.
		 */
		if (SHASH_Entries(wTab[heapcleaner_max_workers]) > 0)
		{
			bool startWorker = (startingWorker != NULL);

			/*
			 * Pass across general waiting list.
			 * Try to send tasks to active workers.
			 */
			for (SHASH_SeqReset(wTab[heapcleaner_max_workers]);
				(msg = (CleanerMessage *) SHASH_SeqNext(wTab[heapcleaner_max_workers])) != NULL; )
			{
				worker = look_for_worker(msg->dbNode);

				if (worker)
				{
					CleanerMessage *temp_msg = (CleanerMessage *) SHASH_Search(wTab[worker->id], (void *) msg, SHASH_ENTER, &found);
					if (temp_msg != NULL)
						if (found)
						{
							temp_msg->hits += msg->hits;
							temp_msg->xid = (msg->xid > temp_msg->xid) ? msg->xid : temp_msg->xid;
						}
						else
							memcpy(temp_msg, msg, sizeof(CleanerMessage));
					else
						MissedBlocksNum++;

					/*
					 * Message gone to worker. delete from main waiting list.
					 */
					temp_msg = (CleanerMessage *) SHASH_Search(wTab[heapcleaner_max_workers], (void *) msg, SHASH_REMOVE, NULL);
					Assert(temp_msg != NULL);

					continue;
				}
				else if (!startWorker)
				{
					/* Start new worker */
					launch_worker(msg->dbNode);
					startWorker = true;
				}
			}
		}

		/*
		 * Check: if we can't process all incoming messages, we need too small
		 * timeout, check latches and go to next iteration.
		 */
		if (SHASH_Entries(wTab[heapcleaner_max_workers]) > 0)
			timeout = 1L;

		/*
		 * See waiting lists of active workers and try to send messages.
		 */
		if (!dlist_is_empty(&HeapCleanerShmem->runningWorkers))
		{
			for (node = dlist_head_node(&HeapCleanerShmem->runningWorkers);
				 ;
				 node = dlist_next_node(&HeapCleanerShmem->runningWorkers, node))
			{
				worker = (WorkerInfo) node;

				if (SHASH_Entries(wTab[worker->id]) == 0)
				{
					/* Check worker idle time */
					if (TimestampDifferenceExceeds(worker->launchtime, GetCurrentTimestamp(), WORKER_IDLE_TIME_DURATION_MAX))
					{
						LWLockAcquire(&worker->WorkItemLock, LW_EXCLUSIVE);

						/*
						 * Check: may be worker has tasks but is too lazy
						 */
						if (worker->nitems == 0)
						{
							/* Shutdown the idle worker */
							kill(worker->pid, SIGTERM);
							dlist_delete(&worker->links);
							dlist_push_head(&HeapCleanerShmem->freeWorkers, &worker->links);

							if (!dlist_is_empty(&HeapCleanerShmem->runningWorkers))
								node = dlist_head_node(&HeapCleanerShmem->runningWorkers);
							else
							{
								LWLockRelease(&worker->WorkItemLock);
								break;
							}
						}
						LWLockRelease(&worker->WorkItemLock);
					}
					else if (!dlist_has_next(&HeapCleanerShmem->runningWorkers, node))
						break;
					else
						continue;
				}

				/* Put list of potentially dirty blocks to the worker shared buffer */
				LWLockAcquire(&worker->WorkItemLock, LW_EXCLUSIVE);

				SHASH_SeqReset(wTab[worker->id]);
				while (((msg = (CleanerMessage *) SHASH_SeqNext(wTab[worker->id])) != NULL) &&
						(worker->nitems < WORKER_TASK_ITEMS_MAX))
				{
					void *temp_msg;

					memcpy(&worker->buffer[worker->nitems++], msg, sizeof(CleanerMessage));
					temp_msg = SHASH_Search(wTab[worker->id], (void *) msg, SHASH_REMOVE, NULL);
					Assert(temp_msg != NULL);
					Assert(worker->buffer[worker->nitems-1].hits > 0);
				}

				worker->launchtime = GetCurrentTimestamp();
				LWLockRelease(&worker->WorkItemLock);

				/* Worker, you have a task! */
				kill(worker->pid, SIGUSR2);

				/*
				 * If launcher has any tasks, It check signals quickly
				 * and to work on further.
				 */
				if (SHASH_Entries(wTab[worker->id]) > 0)
					timeout = 5L;

				if (!dlist_has_next(&HeapCleanerShmem->runningWorkers, node))
					break;
			}

			/*
			 * Launcher have'nt any immediate tasks and
			 * need to manage idle workers only
			 */
			if (timeout < 0)
				/* We only need to wait idle workers */
				timeout = 100L;
		}
		LWLockRelease(HeapCleanerLock);

		if (!got_SIGTERM)
		{
			int wakeEvents = WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_SOCKET_READABLE;

			if (timeout > 0)
				wakeEvents |=  WL_TIMEOUT;
			/* Wait data or signals */
			rc = WaitLatchOrSocket(MyLatch,
								   wakeEvents,
								   HeapCleanerSock, timeout,
								   WAIT_EVENT_BGHEAP_MAIN);
		}

		/* Emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
		{
			elog(LOG, "Heap Launcher exit with 1");
			proc_exit(1);
		}
	}

	launcher_stat_print();
	elog(LOG, "Heap Launcher exit with 0");
	proc_exit(0);
}

static void
launcher_stat_print(void)
{
	FILE *f = fopen("/home/andrey/stat.txt", "a+");
	fprintf(f, "TOTAL hits received: %d\n", launcher_total_received_hits);
	fclose(f);
}

/*
 * Entry point of a worker behavior logic
 */
static void
main_worker_loop(void)
{
	CleanerMessage	task_item[WORKER_TASK_ITEMS_MAX];
	DirtyRelation	*dirty_relation[WORKER_RELATIONS_MAX_NUM];
	PSHTAB			FreeDirtyBlocksList;
	uint32			TmpMissedBlocksNumber = 0;
	long			timeout = -1L;
	int				dirty_relations_num = 0;
	int				task_items_num = 0;
	SHTABCTL		shctl;

	shctl.FillFactor = 0.75;
	shctl.ElementsMaxNum = WORKER_DIRTYBLOCKS_MAX_NUM;
	shctl.ElementSize = sizeof(WorkerTask);
	shctl.KeySize = sizeof(BlockNumber);
	shctl.HashFunc = DefaultHashValueFunc;
	shctl.CompFunc = DefaultCompareFunc;

	FreeDirtyBlocksList = SHASH_Create(shctl);

	while (!got_SIGTERM || (timeout > 0))
	{
		int	rc;

		timeout = -1L;

		if (got_SIGHUP)
		{
			got_SIGHUP = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		if (got_SIGUSR2)
			/* It is needed only for wakeup worker */
			got_SIGUSR2 = false;

		if (TmpMissedBlocksNumber != MissedBlocksNum)
		{
			elog(LOG, "WORKER Missed blocks: cur=%d delta=%d", MissedBlocksNum, MissedBlocksNum-TmpMissedBlocksNumber);
			TmpMissedBlocksNumber = MissedBlocksNum;
		}

		/*
		 * Move task items from shared buffer to local and release it for
		 * new data.
		 * It is introduced in accordance with the idea that shared memory
		 * buffer is smaller than internal process buffer.
		 */
		LWLockAcquire(&MyWorkerInfo->WorkItemLock, LW_SHARED);
		if (MyWorkerInfo->nitems > 0)
		{
			task_items_num = MyWorkerInfo->nitems;
			memcpy(task_item, MyWorkerInfo->buffer, task_items_num*sizeof(CleanerMessage));
			MyWorkerInfo->nitems = 0;
		}
		LWLockRelease(&MyWorkerInfo->WorkItemLock);

		/*
		 * If launcher receive some task items it need to distribute between
		 * workers and waiting lists.
		 */
		if (task_items_num > 0)
		{
			DirtyRelation	*hashent;
			bool			found;
			int				i;

			/* Pass across items and sort by relation */
			for (i = 0; i < task_items_num; i++)
			{
				WorkerTask	*item;

				worker_total_hits_received += task_item[i].hits;
				hashent = SHASH_Search(PrivateRelationsTable,
								  (void *) &(task_item[i].relid), HASH_FIND, NULL);

				if (hashent == NULL)
				{
					elog(LOG, "Add new relation: %d", task_item[i].relid);
					if (dirty_relations_num == WORKER_RELATIONS_MAX_NUM)
					{
						int	j;
						int min =  SHASH_Entries(dirty_relation[0]->items);
						int pos = 0;

						/*
						 * All slots for dirty relations are busy.
						 * Search for slot with smaller number of dirty blocks
						 * and replace it.
						 */
						elog(LOG, "All dirty relation slots are busy at worker process. Start min=%d", min);
						for (j = 1; j < dirty_relations_num; j++)
						{
							int nitems = SHASH_Entries(dirty_relation[j]->items);
							if (nitems < min)
							{
								min = nitems;
								pos = j;
							}
						}

						/* Save stats about not cleaned blocks */
						MissedBlocksNum += SHASH_Entries(dirty_relation[pos]->items);

						elog(LOG, "Remove slot %d with id=%d nitems=%lu. Set new relation id=%d",pos, dirty_relation[pos]->relid, SHASH_Entries(dirty_relation[pos]->items), task_item[i].relid);
						hashent = SHASH_Search(PrivateRelationsTable,
														  (void *) &(dirty_relation[pos]->relid), HASH_REMOVE, &found);

						Assert(found);
						Assert(hashent != NULL);
						SHASH_Destroy(hashent->items);

						/*
						 * Fill empty position by data from last position
						 */
						dirty_relation[pos] = dirty_relation[--dirty_relations_num];
					}

					/* Insert new relid to hash table */
					hashent = SHASH_Search(PrivateRelationsTable, (void *) &(task_item[i].relid), HASH_ENTER, &found);
					Assert(!found);
					Assert(hashent != NULL);

					/*
					 * At a new entry we create and init list of 'dirty' blocks
					 */
					hashent->items = SHASH_Create(shctl);
					dirty_relation[dirty_relations_num++] = hashent;
					pgstat_progress_update_param(PROGRESS_CLEANER_RELATIONS, SHASH_Entries(PrivateRelationsTable));
				}

				/*
				 * Relation entry found or create.
				 * Now, add an item to a waiting list.
				 */
				item = (WorkerTask *) SHASH_Search(hashent->items, (void *) &(task_item[i].blkno), HASH_ENTER, &found);
				if (item == NULL)
					MissedBlocksNum++;
				else
				{
					if (!found)
					{
						item->hits = 0;
						item->lastXid = InvalidTransactionId;
					}
					worker_waiting_lists_hits += task_item[i].hits;
					item->hits += task_item[i].hits;
					Assert(item->hits > 0);
					item->lastXid = (item->lastXid > task_item[i].xid) ? item->lastXid : task_item[i].xid;
				}
			}
			task_items_num = 0;
		}

		PG_TRY();
		{
			int relcounter;
			int64 stat_tot_item = 0;

			/* Pass along dirty relations and try to clean it */
			for (relcounter = 0; relcounter < dirty_relations_num; relcounter++)
			{
				if (SHASH_Entries(FreeDirtyBlocksList) > 0)
					SHASH_Clean(FreeDirtyBlocksList);

				FreeDirtyBlocksList = cleanup_relations(dirty_relation[relcounter], FreeDirtyBlocksList, got_SIGTERM);

				/*
				 * Some blocks from the list may blocked by a backend.
				 * Deferred its for next cleanup attempt.
				 */
				if ((stat_tot_item += SHASH_Entries(dirty_relation[relcounter]->items)) > 0)
					timeout = 10L;
			}

			pgstat_progress_update_param(PROGRESS_CLEANER_TOTAL_ITEMS, stat_tot_item);
			QueryCancelPending = false;
		}
		PG_CATCH();
		{
			HOLD_INTERRUPTS();
			EmitErrorReport();
			AbortOutOfAnyTransaction();
			FlushErrorState();

			RESUME_INTERRUPTS();
		}
		PG_END_TRY();

		if (!got_SIGTERM)
		{
			int	wakeEvents = WL_LATCH_SET | WL_POSTMASTER_DEATH;
			if (timeout > 0)
				wakeEvents |= WL_TIMEOUT;
			rc = WaitLatch(MyLatch, wakeEvents, timeout, WAIT_EVENT_BGHEAP_MAIN);
		}

		ResetLatch(MyLatch);

		/* Emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);
	}

	pgstat_progress_end_command();
	stat_collector_print();
	proc_exit(0);
}

/*
 * SIGHUP_Handler
 */
static void
SIGHUP_Handler(SIGNAL_ARGS)
{
	int	save_errno = errno;

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

/*
 * SIGUSR2_Handler
 * Wake up a worker to read some messages from launcher
 */
static void
SIGUSR2_Handler(SIGNAL_ARGS)
{
	int save_errno = errno;

	got_SIGUSR2 = true;

	SetLatch(MyLatch);

	errno = save_errno;
}

/*
 * Main entry point for background heap cleaner (launcher) process, to be called from the
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
					(errmsg("could not fork background heap cleaner (launcher) process: %m")));
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

/*
 * Main entry point for background heap cleaner (worker) process, to be called from the
 * postmaster.
 */
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
					(errmsg("could not fork background heap cleaner (worker) process: %m")));
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

/*
 * HeapCleanerShmemInit
 *		Allocate and initialize heapcleaner-related shared memory
 */
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
		{
			worker[i].id = i;
			LWLockInitialize(&worker[i].WorkItemLock, TrancheId);
			dlist_push_head(&HeapCleanerShmem->freeWorkers,
							&worker[i].links);
		}
	}
	else
		Assert(found);
	HeapCleanerShmem->startingWorker = NULL;

	TrancheId = LWLockNewTrancheId();
	LWLockRegisterTranche(TrancheId, "heapcleaner");
}

/*
 * HeapCleanerShmemSiz
 *		Compute space needed for heap cleaner-related shared memory
 */
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

static uint64
CleanerMessageHashFunc(void *key, uint64 size, uint64 base)
{
	uint64			sum = 0;
	CleanerMessage	*msg = (CleanerMessage *) key;

	Assert(key != NULL);
	Assert(size == 2 * sizeof(Oid) + sizeof(BlockNumber));
	Assert(base > 0);

	sum += DefaultHashValueFunc(&msg->dbNode, sizeof(Oid), base);
	sum += DefaultHashValueFunc(&msg->relid, sizeof(Oid), base);
	sum += DefaultHashValueFunc(&msg->blkno, sizeof(BlockNumber), base);

	return sum%base;
}

static void
stat_collector_print(void)
{
	stat_struct	*rec;
	FILE *f = fopen("/home/andrey/stat.txt", "a+");
	int	total_removed_tuples = 0;
	int	total_hits = 0;
	int total_blocks_cleaned = 0;

	fprintf(f, "\n\n--------- WORKER ----------------\n");

	for (SHASH_SeqReset(worker_stat);
		(rec = SHASH_SeqNext(worker_stat)) != NULL; )
	{
		Assert(rec->hits > 0);

		total_hits += rec->hits;
		total_blocks_cleaned += rec->cleaning_cycles;
		total_removed_tuples += rec->tuples_removed;
	}

	fprintf(f, "worker_total_hits_received: %d\n", worker_total_hits_received);
	fprintf(f, "WORKER total waiting_lists_hits: %d\n", worker_waiting_lists_hits);
	fprintf(f, "TOTAL BLOCKS Cleaned: %lu (%d with cycles)\n", SHASH_Entries(worker_stat), total_blocks_cleaned);
	fprintf(f, "TOTAL HITS/REMOVED => %d/%d (%6.2f percent)\n", total_hits, total_removed_tuples, 100.*(float)total_removed_tuples/(float)total_hits);
	fprintf(f, "vainly cleaned blocks: %d (%d hits)\n", worker_zero_cleaned_blocks_num, worker_zero_cleaned_hits_num);
	fprintf(f, "worker_tuple_dead_but_HOT=%d, worker_tuple_recently_dead=%d\n", worker_tuple_dead_but_HOT, worker_tuple_recently_dead);
	fprintf(f, "BLOCKS NOT FOUND: %d (%d hits)\n", worker_not_found_blocks_num, worker_not_found_blocks_hits);

	fprintf(f, "IN-Memory waiting list: %lu relations\n", SHASH_Entries(PrivateRelationsTable));

	if (SHASH_Entries(PrivateRelationsTable) > 0)
	{
		DirtyRelation	*list;

		/* Get stat about blocks in waiting lists */
		for (SHASH_SeqReset(PrivateRelationsTable);
			(list = SHASH_SeqNext(PrivateRelationsTable)) != NULL; )
		{
			WorkerTask	*item;
			int			total_inmem_blocks = 0;
			int			total_inmem_hints = 0;

			fprintf(f, "WORKER: Relation: %d, pos: %lu\n", list->relid, PrivateRelationsTable->SeqScanCurElem);
			for (SHASH_SeqReset(list->items);
				(item = SHASH_SeqNext(list->items)) != NULL; )
			{
				total_inmem_blocks++;
				total_inmem_hints += item->hits;
			}
			fprintf(f, "WORKER: total_inmem_blocks=%d, total_inmem_hints=%d, MissedBlocksNum: %d\n",
					total_inmem_blocks, total_inmem_hints, MissedBlocksNum);
		}
	}
	fclose(f);
}
