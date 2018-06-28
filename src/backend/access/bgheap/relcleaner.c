/*
 * relcleaner.c
 *
 *  Created on: 27.06.2018
 *      Author: andrey
 */

#include <unistd.h>

#include "postgres.h"

#include "fmgr.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "access/bgheap.h"
#include "access/htup_details.h"
#include "common/ip.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "utils/ps_status.h"
#include "utils/lsyscache.h"
#include "utils/relcache.h"
#include "utils/relfilenodemap.h"
#include "utils/syscache.h"

/* Essential for shared libs! */
PG_MODULE_MAGIC;

/* Entry point of library loading */
void _PG_init(void);

/* Signal handling */
static volatile sig_atomic_t got_sigterm = false;

NON_EXEC_STATIC pgsocket RelCleanerSock = PGINVALID_SOCKET;
static struct sockaddr_storage pgStatAddr;

static pgsocket relcleaner_init(void);
/* Main loop of process */
void RelCleaner_main(Datum main_arg) pg_attribute_noreturn();

/*
 * RelCleaner_sigterm
 *
 * SIGTERM handler.
 */
static void
RelCleaner_sigterm(SIGNAL_ARGS)
{
	int save_errno = errno;
	got_sigterm = true;
	if (MyProc)
		SetLatch(&MyProc->procLatch);
	errno = save_errno;
}

#define MESSAGE_SIZE_MAX	(sizeof(RelFileNode)+sizeof(BlockNumber)+sizeof(int)+MaxHeapTuplesPerPage*sizeof(OffsetNumber))

typedef struct CleanerMessage
{
	RelFileNode		relnode;
	int				blkno;
	int				noff;
	OffsetNumber	off[MaxHeapTuplesPerPage];
} CleanerMessage;

void
relcleaner_send(RelFileNode relnode, int blkno, OffsetNumber* off, int noff)
{
	int		rc;
	CleanerMessage msg;

	if (RelCleanerSock == PGINVALID_SOCKET)
		return;
	msg.relnode = relnode;
	msg.blkno = blkno;
	msg.noff = noff;
	memcpy(&(msg.off), (char *)&off, noff*sizeof(OffsetNumber));

	Assert(RelCleanerSock != PGINVALID_SOCKET);
	/* We'll retry after EINTR, but ignore all other failures */
	do
	{
		rc = send(RelCleanerSock, &msg, sizeof(CleanerMessage), 0);
	} while (rc < 0 && errno == EINTR);

	if (rc < 0)
		elog(LOG, "could not send to relation cleaner: %m");
	else
		elog(LOG, "send to relation cleaner: %m");
//	sleep(10);
}

static pgsocket
relcleaner_init(void)
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

		if ((RelCleanerSock = socket(addr->ai_family, SOCK_DGRAM, 0)) == PGINVALID_SOCKET)
		{
			ereport(LOG,
					(errcode_for_socket_access(),
					errmsg("could not create socket for relation cleaner: %m")));
			continue;
		}

		if (bind(RelCleanerSock, addr->ai_addr, addr->ai_addrlen) < 0)
		{
			ereport(LOG,
					(errcode_for_socket_access(),
					 errmsg("could not bind socket for relation cleaner: %m")));
			closesocket(RelCleanerSock);
			RelCleanerSock = PGINVALID_SOCKET;
			continue;
		}

		alen = sizeof(pgStatAddr);
		if (getsockname(RelCleanerSock, (struct sockaddr *) &pgStatAddr, &alen) < 0)
		{
			ereport(LOG,
					(errcode_for_socket_access(),
					 errmsg("could not get address of socket for statistics collector: %m")));
			closesocket(RelCleanerSock);
			RelCleanerSock = PGINVALID_SOCKET;
			continue;
		}

		if (connect(RelCleanerSock, (struct sockaddr *) &pgStatAddr, alen) < 0)
		{
			ereport(LOG,
					(errcode_for_socket_access(),
					 errmsg("could not connect socket for statistics collector: %m")));
			closesocket(RelCleanerSock);
			RelCleanerSock = PGINVALID_SOCKET;
			continue;
		}
		break;
	}
	if (!pg_set_noblock(RelCleanerSock))
	{
		ereport(LOG,
				(errcode_for_socket_access(),
				 errmsg("could not set statistics collector socket to nonblocking mode: %m")));
		Assert(0);
	}
	pg_freeaddrinfo_all(hints.ai_family, addrs);
	{
		FILE* f = fopen("/home/andrey/test.log", "a+");
		fprintf(f, "Socket was Initialized! %d\n", (int)RelCleanerSock);
		fclose(f);
	}
elog(LOG, "B");
	return RelCleanerSock;
}

/*
 * RelCleaner_main
 *
 * Main loop processing.
 */
void
RelCleaner_main(Datum main_arg)
{
	RelCleanerSock = (pgsocket)DatumGetInt32(main_arg);
	/* Set up the sigterm signal before unblocking them */
	pqsignal(SIGTERM, RelCleaner_sigterm);
	elog(LOG, "IN: RelCleanerSock=%d", (int)RelCleanerSock);
	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();
//	RelCleanerSock = relcleaner_init();

	while (!got_sigterm)
	{
		int				rc;
		int				len;
		CleanerMessage	msg;

		Assert(RelCleanerSock != PGINVALID_SOCKET);
		ResetLatch(MyLatch);

		len = recv(RelCleanerSock, &msg, sizeof(CleanerMessage), 0);

		if (len > 0)
		{
//			Assert(len == sizeof(CleanerMessage));

			elog(LOG, "Relation Cleaner-2! len=%d", len);
			//index_cleanup(&msg);
		} else {
			elog(LOG, "Relation Cleaner-3!");
		}

		/* Wait data or signals */
		rc = WaitLatchOrSocket(MyLatch,
				WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_SOCKET_READABLE,
				RelCleanerSock, -1L,
				PG_WAIT_EXTENSION);

		/* Emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		elog(LOG, "Relation Cleaner!"); /* Say Hello to the world */
	}
	proc_exit(0);
}

/*
 * _PG_init
 *
 * Load point of library.
 */
void
_PG_init(void)
{
	BackgroundWorker worker;

	RelCleanerSock = relcleaner_init();
	/* Register the worker processes */
	MemSet(&worker, 0, sizeof(BackgroundWorker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	snprintf(worker.bgw_library_name, BGW_MAXLEN, "relcleaner");
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "RelCleaner_main");
	snprintf(worker.bgw_name, BGW_MAXLEN, "HEAP Relation cleaner");
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	elog(LOG, "OUT: A: %d", RelCleanerSock);
	worker.bgw_main_arg = Int32GetDatum((int)RelCleanerSock);
	worker.bgw_notify_pid = 0;
	RegisterBackgroundWorker(&worker);
}
