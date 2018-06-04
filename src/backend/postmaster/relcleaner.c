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
static pid_t rclauncher_forkexec(void);
#endif
static void RelCleaner_MainLoop(void);

#ifdef EXEC_BACKEND
static pid_t
rclauncher_forkexec(void)
{
	char	   *av[10];
	int			ac = 0;

	av[ac++] = "postgres";
	av[ac++] = "--forkrclauncher";
	av[ac++] = NULL;			/* filled in by postmaster_forkexec */
	av[ac] = NULL;

	return postmaster_forkexec(ac, av);
}

#endif

int relcleaner_start(void)
{
	pid_t		RelCleanerPid;

#ifdef EXEC_BACKEND
	switch ((RelCleanerPid = rclauncher_forkexec()))
#else
	switch ((RelCleanerPid = fork_process()))
#endif
	{
		case -1:
			ereport(LOG,
					(errmsg("could not fork relcleaner: %m")));
			return 0;

#ifndef EXEC_BACKEND
		case 0:
			/* in postmaster child ... */
			InitPostmasterChild();

			/* Close the postmaster's sockets */
			ClosePostmasterPorts(false);

			RelCleanerMain();
			break;
#endif

		default:
			return (int) RelCleanerPid;
	}

	/*
	 * Advertise our latch that backends can use to wake us up while we're
	 * sleeping.
	 */
	ProcGlobal->relcleanerLatch = &MyProc->procLatch;
	/* shouldn't get here */
	return 0;
}

void RelCleanerMain(void)
{
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

	RelCleaner_MainLoop();

	exit(0);
}

static void
RelCleaner_MainLoop(void)
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
		{
		FILE* f = fopen("/home/andrey/test.log", "a+");
		fprintf(f, "NOP-2\n");
		printf("NOP1\n");
		fclose(f);
		sleep(1);
		}
	}
	proc_exit(0);
}
