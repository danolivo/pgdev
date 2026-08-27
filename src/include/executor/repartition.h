/*-------------------------------------------------------------------------
 *
 * repartition.h
 *	  Shared state for the parallel Repartition executor node.
 *
 * Not included by execnodes.h, which only forward-declares the structs below.
 * That keeps barrier.h, sharedfileset.h and sharedtuplestore.h out of most of
 * the tree.
 *
 * src/include/executor/repartition.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef REPARTITION_H
#define REPARTITION_H

#include "portability/instr_time.h"
#include "port/atomics.h"
#include "storage/barrier.h"
#include "storage/sharedfileset.h"
#include "utils/sharedtuplestore.h"

/* Per-participant statistics, for EXPLAIN and for calibrating the cost model. */
typedef struct RepartitionInstrumentation
{
	int			nclaimed;		/* partitions this participant drained */
	int64		ntuples_written;
	int64		ntuples_read;
	int64		bytes_payload;	/* sum of t_len; with temp_blks gives the
								 * chunk-alignment inflation factor */
	instr_time	time_sink;		/* cost of the write phase */
	instr_time	time_barrier;	/* idle at the barrier == skew */
	instr_time	time_drain;		/* cost of the read phase */
} RepartitionInstrumentation;

typedef struct SharedRepartitionInfo
{
	int			num_workers;
	RepartitionInstrumentation instrument[FLEXIBLE_ARRAY_MEMBER];
} SharedRepartitionInfo;

/*
 * Stamped into the head of the allocation.  Catches a stale or mistyped
 * shm_toc lookup before the result is dereferenced as something else.
 */
#define REPARTITION_MAGIC		((uint32) 0x52505254)	/* "RPRT" */

/*
 * A redzone precedes every area inside the allocation, and one more closes it.
 *
 * It has to be reserved deliberately: relying on alignment padding to provide
 * it does not work, because the areas here are all naturally MAXALIGNed --
 * K * sizeof(pg_atomic_uint64) always is, and so is the fixed struct -- and
 * measurement says the incidental padding is zero bytes at exactly the two
 * boundaries that matter.  Without this, a length bug in one area writes into
 * the head of the next with nothing in between to notice.
 *
 * Neither valgrind nor AddressSanitizer sees such a write on its own: to both
 * of them the whole allocation is one live object from the first byte to the
 * last.  Marking the redzones NOACCESS (a no-op unless built with
 * USE_VALGRIND) is what gives valgrind something to catch, and the byte
 * pattern is what an assert-enabled build without valgrind can check by hand.
 */
#define REPARTITION_REDZONE			MAXALIGN(sizeof(uint64))
#define REPARTITION_REDZONE_BYTE	0x5A

/*
 * Where everything lives inside the single allocation, decided once in
 * repartition_layout() and never recomputed.  Recomputing an offset at the
 * point of use is how two functions that must agree stop agreeing, so the
 * accessors below read these fields instead and bounds-check themselves
 * against alloc_size.
 *
 *	 [ ParallelRepartitionState ][ rz ][ counts[K] ][ rz ]
 *	 [ SharedTuplestore #0 ][ rz ] ... [ SharedTuplestore #K-1 ][ rz ]
 *	 [ SharedRepartitionInfo -- only when instrumenting ][ rz ]
 *
 * Every area start is MAXALIGNed, which is what makes the int64s and the
 * atomics inside them addressable everywhere: MAXIMUM_ALIGNOF is by definition
 * the strictest alignment any C type on the platform needs.  Do not replace
 * these with sizeof-based arithmetic at the point of use.
 */
typedef struct RepartitionLayout
{
	Size		alloc_size;		/* total bytes handed out by shm_toc_allocate */
	Size		counts_offset;	/* per-partition tuple counters */
	Size		sts_offset;		/* first SharedTuplestore */
	Size		sts_size;		/* bytes one SharedTuplestore occupies */
	Size		sts_stride;		/* bytes from one to the next, redzone included */
	Size		instrument_offset;	/* 0 when not instrumenting */
	Size		instrument_size;	/* 0 when not instrumenting */
} RepartitionLayout;

/*
 * Shared state for one Repartition node.  A single shm_toc entry, keyed by
 * plan_node_id, holds all of it: shm_toc_insert() does not detect duplicate
 * keys and shm_toc_lookup() returns the first match, so a second entry for the
 * instrumentation would be unreachable.
 */
typedef struct ParallelRepartitionState
{
	uint32		magic;			/* REPARTITION_MAGIC */
	int			npartitions;	/* K; power of two */
	int			nparticipants;	/* pcxt->nworkers + 1; sizes each STS */
	int			ninstrument;	/* participants with a shared counter slot */

	RepartitionLayout layout;

	/*
	 * Hands out partitions during the drain phase.  A plain fetch-add, no
	 * modulo: each partition is read exactly once, by exactly one participant.
	 * (Parallel hash join uses a modulo here because its batches are
	 * revisited; ours are not.)
	 */
	pg_atomic_uint32 distributor;

	/*
	 * Checksum of the drain order, published by whichever participant computes
	 * it first and checked by the others.  See repartition_order_partitions().
	 * Zero means "nobody has published one yet".
	 */
	pg_atomic_uint32 order_checksum;

	/*
	 * Conservation of tuples.  Maintained and checked only in assert-enabled
	 * builds -- the counter that feeds assert_read is incremented per tuple,
	 * and that is not a cost to impose on a production drain loop for a check
	 * that has never fired.  The fields stay in the struct unconditionally so
	 * that the layout does not depend on the build.
	 *
	 * Two checks, because they cover different things.  Whoever drains the
	 * last partition can compare the totals: every write precedes the barrier
	 * and every read has been added by then, so written must equal read.  A
	 * query that stops early -- LIMIT, a cursor, an error above us -- never
	 * reaches that point, which is precisely where a silent truncation would
	 * be least visible, so every partition also checks the weaker
	 * read <= written as it finishes.
	 */
	pg_atomic_uint64 assert_written;
	pg_atomic_uint64 assert_read;
	pg_atomic_uint32 assert_drained;

	/*
	 * Separates the sink phase from the drain phase.  Slots are reserved by
	 * the leader before any worker exists; see ExecRepartitionInitializeDSM().
	 */
	Barrier		sink_barrier;

	SharedFileSet fileset;
} ParallelRepartitionState;

/*
 * Bounds-checked address of one area inside the allocation.
 *
 * The asserts are the whole point of routing every access through here: an
 * offset that has drifted, or a length that has outgrown its area, is caught
 * at the first dereference instead of quietly landing in the next area and
 * showing up as a corrupted tuplestore an hour later.
 */
static inline char *
RepartitionArea(ParallelRepartitionState *pstate, Size offset, Size len)
{
	Assert(pstate->magic == REPARTITION_MAGIC);
	Assert(offset >= MAXALIGN(sizeof(ParallelRepartitionState)));
	Assert(offset == MAXALIGN(offset));
	Assert(len > 0);
	/* the allocation ends with a redzone, which is nobody's area */
	Assert(offset + len <= pstate->layout.alloc_size - REPARTITION_REDZONE);

	return (char *) pstate + offset;
}

static inline pg_atomic_uint64 *
RepartitionPartTuples(ParallelRepartitionState *pstate)
{
	pg_atomic_uint64 *counts;

	counts = (pg_atomic_uint64 *)
		RepartitionArea(pstate, pstate->layout.counts_offset,
						pstate->npartitions * sizeof(pg_atomic_uint64));

	/*
	 * pg_atomic_init_u64() asserts this itself where 64-bit atomics are
	 * native, but not where they are simulated with a spinlock -- and the
	 * simulated build is exactly the one where a misaligned uint64 would
	 * still be a SIGBUS on a strict-alignment machine.
	 */
	AssertPointerAlignment(counts, 8);

	return counts;
}

/*
 * sts_estimate() returns an unaligned size and SharedTuplestoreParticipant
 * contains an LWLock, so the stride must be MAXALIGNed -- the same reason
 * EstimateParallelHashJoinBatch() does it.
 */
static inline SharedTuplestore *
RepartitionSTS(ParallelRepartitionState *pstate, int n)
{
	Assert(n >= 0 && n < pstate->npartitions);
	Assert(pstate->layout.sts_size == MAXALIGN(sts_estimate(pstate->nparticipants)));
	Assert(pstate->layout.sts_stride == pstate->layout.sts_size + REPARTITION_REDZONE);

	return (SharedTuplestore *)
		RepartitionArea(pstate,
						pstate->layout.sts_offset +
						pstate->layout.sts_stride * (Size) n,
						pstate->layout.sts_size);
}

/* Size of the instrumentation area for n participants' worth of counters. */
static inline Size
RepartitionSharedInfoSize(int nworkers)
{
	Assert(nworkers >= 0);
	return add_size(offsetof(SharedRepartitionInfo, instrument),
					mul_size(nworkers, sizeof(RepartitionInstrumentation)));
}

static inline SharedRepartitionInfo *
RepartitionSharedInfo(ParallelRepartitionState *pstate)
{
	SharedRepartitionInfo *si;

	Assert(pstate->magic == REPARTITION_MAGIC);
	if (pstate->layout.instrument_offset == 0)
		return NULL;

	si = (SharedRepartitionInfo *)
		RepartitionArea(pstate, pstate->layout.instrument_offset,
						pstate->layout.instrument_size);

	/*
	 * num_workers is written once by the leader before any worker exists, so
	 * reading it here is not a race.  Compare it against what the layout was
	 * built for rather than against nparticipants - 1: the two happen to be
	 * equal today, but only because of how ExecRepartitionInitializeDSM()
	 * chooses ninstrument, and an assertion that encodes a coincidence stops
	 * catching anything the moment the coincidence ends.
	 */
	Assert(si->num_workers == pstate->ninstrument);
	Assert(pstate->layout.instrument_size ==
		   RepartitionSharedInfoSize(pstate->ninstrument));

	return si;
}

/*
 * Offsets of the areas inside the allocation, in order, with alloc_size last.
 * A redzone of REPARTITION_REDZONE bytes sits immediately before each of them.
 *
 * starts[] must have room for npartitions + 3 entries: the counters, the K
 * tuplestores, the instrumentation when there is any, and alloc_size.
 */
static inline int
RepartitionAreaStarts(ParallelRepartitionState *pstate, Size *starts)
{
	int			n = 0;
	int			i;

	Assert(pstate->magic == REPARTITION_MAGIC);

	starts[n++] = pstate->layout.counts_offset;
	for (i = 0; i < pstate->npartitions; i++)
		starts[n++] = pstate->layout.sts_offset +
			pstate->layout.sts_stride * (Size) i;
	if (pstate->layout.instrument_offset != 0)
		starts[n++] = pstate->layout.instrument_offset;
	starts[n++] = pstate->layout.alloc_size;

	Assert(n == pstate->npartitions + 2 + (pstate->layout.instrument_offset != 0));
	return n;
}

#endif							/* REPARTITION_H */
