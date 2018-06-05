#include "postgres.h"

#include <unistd.h>

#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/autovacuum.h"
#include "postmaster/fork_process.h"
#include "postmaster/postmaster.h"
#include "postmaster/relcleaner.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "utils/ps_status.h"

#ifdef EXEC_BACKEND
static pid_t rcleaner_forkexec(void);
#endif

NON_EXEC_STATIC void RelCleanerMain(int argc, char *argv[]) pg_attribute_noreturn();

static void main_loop(void);

int
rcleaner_start(void)
{
	pid_t		RCleanerPid;

#ifdef EXEC_BACKEND
	switch ((RCleanerPid = rcleaner_forkexec()))
#else
	switch ((RCleanerPid = fork_process()))
#endif
	{
		case -1:
			ereport(LOG,
					(errmsg("could not fork relation cleaner: %m")));
			return 0;

#ifndef EXEC_BACKEND
		case 0:
			/* in postmaster child ... */
			InitPostmasterChild();

			/* Close the postmaster's sockets */
			ClosePostmasterPorts(false);

			RelCleanerMain(0, NULL);
			break;
#endif

		default:
			return (int) RCleanerPid;
	}

	/* shouldn't get here */
	return 0;
}

#ifdef EXEC_BACKEND

/*
 * pgarch_forkexec() -
 *
 * Format up the arglist for, then fork and exec, archive process
 */
static pid_t
rcleaner_forkexec(void)
{
	char	   *av[10];
	int			ac = 0;

	av[ac++] = "postgres";

	av[ac++] = "--forkrcleaner";

	av[ac++] = NULL;			/* filled in by postmaster_forkexec */

	av[ac] = NULL;
	Assert(ac < lengthof(av));

	return postmaster_forkexec(ac, av);
}
#endif							/* EXEC_BACKEND */

NON_EXEC_STATIC void
RelCleanerMain(int argc, char *argv[])
{
		{
			FILE* f = fopen("/home/andrey/test.log", "a+");
			fprintf(f, "Start\n");
			fclose(f);
		}
	pqsignal(SIGHUP, SIG_IGN);
	pqsignal(SIGINT, SIG_IGN);
	pqsignal(SIGTERM, SIG_IGN);
	pqsignal(SIGQUIT, SIG_IGN);
	pqsignal(SIGALRM, SIG_IGN);
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, SIG_IGN);
	pqsignal(SIGUSR2, SIG_IGN);
	pqsignal(SIGCHLD, SIG_DFL);
	pqsignal(SIGTTIN, SIG_DFL);
	pqsignal(SIGTTOU, SIG_DFL);
	pqsignal(SIGCONT, SIG_DFL);
	pqsignal(SIGWINCH, SIG_DFL);
	PG_SETMASK(&UnBlockSig);
	/*
	 * Identify myself via ps
	 */
	init_ps_display("archiver", "", "", "");

//	ProcGlobal->relcleanerLatch = &MyProc->procLatch;

	main_loop();

	exit(0);
}

static void main_loop(void)
{
	int rc;

	for (;;)
	{
		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   1000L /* convert to ms */ ,
					   WAIT_EVENT_RELCLEANER_MAIN);
		ResetLatch(MyLatch);
		/*
		 * Emergency bailout if postmaster has died.  This is to avoid the
		 * necessity for manual cleanup of all postmaster children.
		 */
		if (rc & WL_POSTMASTER_DEATH)
			exit(1);
		
		/* WAL has a records with DEAD tuples. We need:
		 * 1. Scan WAL from lastScannedPosition to the end.
		 * 2. Got Messages with deleted tuples.
		 * 3. Generate scankeys, itids
		 * 4. Remove itids
		 * 5. Set UNUSED heap tuples
		 * 6. Write to WAL an info
		 */
	}
	proc_exit(0);
}
