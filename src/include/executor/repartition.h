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
 * Shared state for one Repartition node.  A single shm_toc entry, keyed by
 * plan_node_id, holds all of it: shm_toc_insert() does not detect duplicate
 * keys and shm_toc_lookup() returns the first match, so a second entry for the
 * instrumentation would be unreachable.  Layout:
 *
 *	 [ ParallelRepartitionState                 ]
 *	 [ pg_atomic_uint64 part_tuples[npartitions] ]
 *	 [ SharedTuplestore x npartitions            ]
 *	 [ SharedRepartitionInfo   -- only when instrumenting ]
 */
typedef struct ParallelRepartitionState
{
	int			npartitions;	/* K; power of two */
	int			nparticipants;	/* pcxt->nworkers + 1; sizes each STS */
	Size		instrument_offset;	/* 0 when not instrumenting */

	/*
	 * Hands out partitions during the drain phase.  A plain fetch-add, no
	 * modulo: each partition is read exactly once, by exactly one participant.
	 * (Parallel hash join uses a modulo here because its batches are
	 * revisited; ours are not.)
	 */
	pg_atomic_uint32 distributor;

	/*
	 * Separates the sink phase from the drain phase.  Slots are reserved by
	 * the leader before any worker exists; see ExecRepartitionInitializeDSM().
	 */
	Barrier		sink_barrier;

	SharedFileSet fileset;
} ParallelRepartitionState;

static inline pg_atomic_uint64 *
RepartitionPartTuples(ParallelRepartitionState *pstate)
{
	return (pg_atomic_uint64 *) ((char *) pstate +
								 MAXALIGN(sizeof(ParallelRepartitionState)));
}

/*
 * sts_estimate() returns an unaligned size and SharedTuplestoreParticipant
 * contains an LWLock, so the stride must be MAXALIGNed -- the same reason
 * EstimateParallelHashJoinBatch() does it.
 */
static inline SharedTuplestore *
RepartitionSTS(ParallelRepartitionState *pstate, int n)
{
	char	   *base = (char *) pstate +
		MAXALIGN(sizeof(ParallelRepartitionState)) +
		MAXALIGN(pstate->npartitions * sizeof(pg_atomic_uint64));

	return (SharedTuplestore *)
		(base + MAXALIGN(sts_estimate(pstate->nparticipants)) * n);
}

static inline SharedRepartitionInfo *
RepartitionSharedInfo(ParallelRepartitionState *pstate)
{
	if (pstate->instrument_offset == 0)
		return NULL;
	return (SharedRepartitionInfo *)
		((char *) pstate + pstate->instrument_offset);
}

#endif							/* REPARTITION_H */
