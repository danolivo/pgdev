/*-------------------------------------------------------------------------
 *
 * pg_heaptrack.c
 *		Turn on heaptrack chunk profiling for the current backend at run time.
 *
 * A backend built with -DUSE_HEAPTRACK carries heaptrack's recording engine
 * compiled directly into it, and routes every palloc/pfree/repalloc and every
 * memory-context reset through it -- but only while the core-owned switch
 * pg_heaptrack_active is set.  The flag is false by default, so the
 * instrumentation is inert in a normally-started server, costing an un-profiled
 * backend only one branch per allocation.
 *
 * This module is the activation surface.  It does not supply the engine (the
 * core does); _PG_init() calls heaptrack_init() once -- the only engine call
 * that must happen exactly once per process, and _PG_init() runs exactly once
 * -- and opens the gate.  pg_heaptrack_start()/stop() then just toggle
 * recording with heaptrack_resume()/pause(), each an idempotent atomic store,
 * so no "already started/stopped" bookkeeping is needed.  Activation is
 * per-backend and needs no LD_PRELOAD and no debugger attach.  Because we use
 * heaptrack_init() rather than heaptrack's preload/inject malloc interposers,
 * only the chunk allocations are recorded -- the underlying malloc'd blocks are
 * not, so there is no double counting.
 *
 * The engine control API comes from heaptrack's own libheaptrack.h; the engine
 * symbols, and pg_heaptrack_active, live in the backend, so this module only
 * links against a USE_HEAPTRACK server.  Built against a server without it -- or
 * on a platform where heaptrack's engine does not compile, i.e. anything but
 * Linux/FreeBSD -- it fails at link time with unresolved symbols, which is the
 * intended "this server cannot profile" signal.  There is deliberately no
 * run-time platform check.
 *
 * Usage:
 *		LOAD 'pg_heaptrack';						-- this session only
 *		-- or session_preload_libraries = 'pg_heaptrack' for every backend
 *
 * The profile is written to <PGDATA>/heaptrack.postgres.<pid>; interpret it
 * with heaptrack_interpret and view with heaptrack_gui / heaptrack_print.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		contrib/pg_heaptrack/pg_heaptrack.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "miscadmin.h"
#include "storage/ipc.h"

#include "libheaptrack.h"		/* heaptrack engine control API (compiled in) */

PG_MODULE_MAGIC;

void		_PG_init(void);

/* Core-owned master switch (src/backend/utils/mmgr/mcxt_instrument.c). */
extern PGDLLIMPORT bool pg_heaptrack_active;

PG_FUNCTION_INFO_V1(pg_heaptrack_start);
PG_FUNCTION_INFO_V1(pg_heaptrack_stop);
PG_FUNCTION_INFO_V1(pg_heaptrack_is_active);

/*
 * pg_heaptrack_at_proc_exit
 *		Close the gate and let heaptrack flush its data file.
 *
 * heaptrack also installs its own atexit() flush; calling heaptrack_stop() here
 * just makes the flush happen at a defined point during backend shutdown.  The
 * engine guards against the two paths cleaning up twice.
 */
static void
pg_heaptrack_at_proc_exit(int code, Datum arg)
{
	pg_heaptrack_active = false;
	heaptrack_stop();
}

Datum
pg_heaptrack_start(PG_FUNCTION_ARGS)
{
	/* heaptrack_resume() is an idempotent atomic store, so no guard is needed. */
	heaptrack_resume();
	pg_heaptrack_active = true;

	PG_RETURN_VOID();
}

Datum
pg_heaptrack_stop(PG_FUNCTION_ARGS)
{
	/* Close the gate first, so nothing races the pause; both are idempotent. */
	pg_heaptrack_active = false;
	heaptrack_pause();

	PG_RETURN_VOID();
}

Datum
pg_heaptrack_is_active(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(pg_heaptrack_active);
}

void
_PG_init(void)
{
	char		path[MAXPGPATH];

	/*
	 * Loading in the postmaster (shared_preload_libraries) would initialise the
	 * engine before any backend forks.  We want strictly per-backend profiling,
	 * so refuse it; use LOAD or session_preload_libraries instead, so we run in
	 * an already-forked backend.
	 */
	if (process_shared_preload_libraries_in_progress)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("pg_heaptrack cannot be loaded via shared_preload_libraries"),
				 errhint("Use LOAD or session_preload_libraries so the module loads after fork.")));

	/* One data file per backend, in the data directory. */
	snprintf(path, sizeof(path), "%s/heaptrack.postgres.%d",
			 DataDir, (int) MyProcPid);

	/* The engine initialises exactly once per process, here. */
	heaptrack_init(path, NULL, NULL, NULL);
	on_proc_exit(pg_heaptrack_at_proc_exit, (Datum) 0);

	/* Recording is live from load. */
	pg_heaptrack_active = true;

	ereport(LOG,
			(errmsg("pg_heaptrack: recording chunk-level heap profile to \"%s\"",
					path)));
}
