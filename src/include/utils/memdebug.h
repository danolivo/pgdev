/*-------------------------------------------------------------------------
 *
 * memdebug.h
 *	  Memory debugging support.
 *
 * Currently, this file either wraps <valgrind/memcheck.h>, maps the
 * Valgrind client request macros we use onto heaptrack's API, or
 * substitutes empty definitions for them.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/memdebug.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MEMDEBUG_H
#define MEMDEBUG_H

#if defined(USE_VALGRIND) && defined(USE_HEAPTRACK)
#error "USE_VALGRIND and USE_HEAPTRACK are mutually exclusive"
#endif

#ifdef USE_VALGRIND
#include <valgrind/memcheck.h>
#elif defined(USE_HEAPTRACK)

/*
 * Map the Valgrind mempool client requests onto heaptrack, so that heaptrack
 * can attribute palloc/pfree traffic to individual chunks instead of whole
 * memory context blocks.
 *
 * The heaptrack recording engine is compiled directly into the backend when
 * building with USE_HEAPTRACK (see src/backend/utils/mmgr/Makefile and
 * contrib/heaptrack), so heaptrack_malloc()/free()/realloc() are ordinary
 * in-binary symbols.  The engine stays dormant until the pg_heaptrack
 * extension (contrib/pg_heaptrack) calls heaptrack_init().  We additionally
 * gate every report on pg_heaptrack_active, a core-owned switch the extension
 * flips: an un-profiled backend then pays only one predicted-not-taken branch,
 * and -- more importantly -- the per-reset chunk walk in the allocators stays
 * out of hot paths until the engine is actually recording.  A USE_HEAPTRACK
 * core build needs no heaptrack header on the include path.
 *
 * The memory (un)definedness requests have no heaptrack equivalent and are
 * no-ops, as are the pool create/destroy/trim requests.
 */
extern void heaptrack_malloc(void *ptr, size_t size);
extern void heaptrack_free(void *ptr);
extern void heaptrack_realloc(void *ptr_in, size_t size, void *ptr_out);

extern PGDLLIMPORT bool pg_heaptrack_active;

#define heaptrack_report_alloc(ptr, size) \
	do { if (pg_heaptrack_active) heaptrack_malloc((ptr), (size)); } while (0)
#define heaptrack_report_free(ptr) \
	do { if (pg_heaptrack_active) heaptrack_free((ptr)); } while (0)
#define heaptrack_report_realloc(ptr_in, size, ptr_out) \
	do { if (pg_heaptrack_active) heaptrack_realloc((ptr_in), (size), (ptr_out)); } while (0)

#define VALGRIND_CHECK_MEM_IS_DEFINED(addr, size)			do {} while (0)
#define VALGRIND_CREATE_MEMPOOL(context, redzones, zeroed)	do {} while (0)
#define VALGRIND_DESTROY_MEMPOOL(context)					do {} while (0)
#define VALGRIND_MAKE_MEM_DEFINED(addr, size)				do {} while (0)
#define VALGRIND_MAKE_MEM_NOACCESS(addr, size)				do {} while (0)
#define VALGRIND_MAKE_MEM_UNDEFINED(addr, size)				do {} while (0)
#define VALGRIND_MEMPOOL_TRIM(context, addr, size)			do {} while (0)

/*
 * heaptrack matches allocations and frees by exact pointer in a single
 * global map, and it already tracks the underlying malloc'd blocks via its
 * interposed malloc/free hooks.  The mempool requests made by the
 * block-oriented allocators (aset.c, generation.c, slab.c, bump.c) operate
 * on block and context-header pointers and would collide with that
 * tracking: a duplicate allocation record for the same pointer overwrites
 * the earlier one in heaptrack's map, orphaning its size as a permanent
 * "leak".  Those files therefore define
 * HEAPTRACK_SUPPRESS_BLOCK_LEVEL_REQUESTS before their includes, turning
 * the requests into no-ops here.  The chunk-pointer requests made by
 * mcxt.c and alignedalloc.c remain live.
 */
#ifdef HEAPTRACK_SUPPRESS_BLOCK_LEVEL_REQUESTS
#define VALGRIND_MEMPOOL_ALLOC(context, addr, size)			do {} while (0)
#define VALGRIND_MEMPOOL_FREE(context, addr)				do {} while (0)
#define VALGRIND_MEMPOOL_CHANGE(context, optr, nptr, size)	do {} while (0)

/*
 * Confirm to the requesting file that its define was seen in time.  The
 * allocators #error if this is missing after their includes, catching the
 * day this header gets included (through some other header) before their
 * HEAPTRACK_SUPPRESS_BLOCK_LEVEL_REQUESTS define could take effect.
 */
#define HEAPTRACK_BLOCK_LEVEL_REQUESTS_SUPPRESSED
#else
#define VALGRIND_MEMPOOL_ALLOC(context, addr, size) \
	do { heaptrack_report_alloc(addr, size); } while (0)
#define VALGRIND_MEMPOOL_FREE(context, addr) \
	do { heaptrack_report_free(addr); } while (0)
#define VALGRIND_MEMPOOL_CHANGE(context, optr, nptr, size) \
	do { heaptrack_report_realloc(optr, size, nptr); } while (0)
#endif							/* HEAPTRACK_SUPPRESS_BLOCK_LEVEL_REQUESTS */

/*
 * heaptrack has no notion of destroying or trimming a pool, so chunks
 * released wholesale by a memory context reset or delete would appear
 * still allocated in the profile.  To compensate, the context allocators
 * walk their blocks while discarding them and report the chunks they
 * carry; see the HEAPTRACK_MEMPOOL_FREE macros in aset.c, generation.c,
 * slab.c and bump.c (whose chunks grow headers under USE_HEAPTRACK for
 * exactly this purpose).  One gap remains: palloc_aligned() chunks are
 * tracked under their aligned pointer, which cannot be recovered from the
 * block layout, so they appear as leaked after a context reset or delete.
 */

/*
 * In MEMORY_CONTEXT_CHECKING builds the chunk headers record whether a
 * chunk was already pfree'd, letting the reset walkers report exactly the
 * still-allocated set.  Without it, freed chunks get reported again;
 * that is harmless, as heaptrack ignores frees of pointers it does not
 * track.
 */
#ifdef MEMORY_CONTEXT_CHECKING
#define HEAPTRACK_CHUNK_IS_LIVE(chunk) \
	((chunk)->requested_size != InvalidAllocSize)
#else
#define HEAPTRACK_CHUNK_IS_LIVE(chunk)	true
#endif

#else
#define VALGRIND_CHECK_MEM_IS_DEFINED(addr, size)			do {} while (0)
#define VALGRIND_CREATE_MEMPOOL(context, redzones, zeroed)	do {} while (0)
#define VALGRIND_DESTROY_MEMPOOL(context)					do {} while (0)
#define VALGRIND_MAKE_MEM_DEFINED(addr, size)				do {} while (0)
#define VALGRIND_MAKE_MEM_NOACCESS(addr, size)				do {} while (0)
#define VALGRIND_MAKE_MEM_UNDEFINED(addr, size)				do {} while (0)
#define VALGRIND_MEMPOOL_ALLOC(context, addr, size)			do {} while (0)
#define VALGRIND_MEMPOOL_FREE(context, addr)				do {} while (0)
#define VALGRIND_MEMPOOL_CHANGE(context, optr, nptr, size)	do {} while (0)
#define VALGRIND_MEMPOOL_TRIM(context, addr, size)			do {} while (0)
#endif


#ifdef CLOBBER_FREED_MEMORY

/* Wipe freed memory for debugging purposes */
static inline void
wipe_mem(void *ptr, size_t size)
{
	VALGRIND_MAKE_MEM_UNDEFINED(ptr, size);
	memset(ptr, 0x7F, size);
	VALGRIND_MAKE_MEM_NOACCESS(ptr, size);
}

#endif							/* CLOBBER_FREED_MEMORY */

#ifdef MEMORY_CONTEXT_CHECKING

static inline void
set_sentinel(void *base, Size offset)
{
	char	   *ptr = (char *) base + offset;

	VALGRIND_MAKE_MEM_UNDEFINED(ptr, 1);
	*ptr = 0x7E;
	VALGRIND_MAKE_MEM_NOACCESS(ptr, 1);
}

static inline bool
sentinel_ok(const void *base, Size offset)
{
	const char *ptr = (const char *) base + offset;
	bool		ret;

	VALGRIND_MAKE_MEM_DEFINED(ptr, 1);
	ret = *ptr == 0x7E;
	VALGRIND_MAKE_MEM_NOACCESS(ptr, 1);

	return ret;
}

#endif							/* MEMORY_CONTEXT_CHECKING */

#ifdef RANDOMIZE_ALLOCATED_MEMORY

void		randomize_mem(char *ptr, size_t size);

#endif							/* RANDOMIZE_ALLOCATED_MEMORY */


#endif							/* MEMDEBUG_H */
