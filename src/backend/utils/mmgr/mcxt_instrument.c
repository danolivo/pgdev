/*-------------------------------------------------------------------------
 *
 * mcxt_instrument.c
 *	  Allocation-instrumentation hooks for external heap profilers.
 *
 * Master switch for heaptrack chunk profiling.  The recording engine is
 * compiled into the backend (USE_HEAPTRACK build) but stays dormant until the
 * pg_heaptrack module (contrib/pg_heaptrack) calls heaptrack_init().  That same
 * module flips this flag around the live region.  The MemoryContext annotation
 * macros (see memdebug.h) and the context-reset chunk walkers in
 * aset.c/bump.c/generation.c/slab.c gate their work on it, so the
 * instrumentation is strictly per-backend: only a backend that has started the
 * profiler records anything, and the cost in every other backend is a single
 * predicted-false branch per palloc -- and, crucially, no chunk-header walk on
 * context reset.
 *
 * Keeping the flag here (rather than in the module) lets the hot allocator
 * paths test a core symbol directly; "LOAD 'pg_heaptrack'" then works in an
 * already-running backend, since the engine is activated at run time rather
 * than bound by the dynamic linker at process start (as heaptrack's own weak
 * symbols would require).
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/utils/mmgr/mcxt_instrument.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_HEAPTRACK

bool		pg_heaptrack_active = false;

#endif							/* USE_HEAPTRACK */
