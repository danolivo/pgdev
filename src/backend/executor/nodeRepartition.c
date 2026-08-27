/*-------------------------------------------------------------------------
 *
 * nodeRepartition.c
 *	  Redistribute tuples among parallel participants by hash.
 *
 * The node guarantees that every tuple whose partition-key columns are equal
 * (under the hash opfamily's equality operator) is handed to the same
 * participant.  That is exactly the precondition a Finalize Aggregate needs in
 * order to run below Gather instead of single-threaded in the leader.
 *
 * Execution has two phases separated by a barrier:
 *
 *	 RS_SINK	pull the child dry, hash each tuple's key columns, and write
 *				the tuple into one of npartitions SharedTuplestores.  Nothing
 *				is returned upwards during this phase.
 *
 *	 RS_DRAIN	claim whole partitions with an atomic counter and stream them
 *				upwards.  No participant ever waits in this phase: a partition
 *				that somebody else took is simply skipped.
 *
 * Deadlock avoidance.  The rule the tree already lives by (see the header
 * comment in nodeHashjoin.c) is: never wait at a barrier after emitting a
 * tuple.  This node satisfies it trivially -- it emits nothing before the
 * barrier and waits on nothing after it.  A barrier does not create a pairwise
 * "A waits for B" edge the way a bounded queue does; its release condition is
 * over the set of participants, every one of which is, by the above, either
 * running towards the barrier or gone.  That is why a materialising exchange
 * is used here rather than a pipelined one.
 *
 * That argument assumes the leader reaches this node at all.  Under Gather it
 * does: gather_readnext() polls the worker queues without blocking whenever
 * the leader participates, so the leader always falls through to executing the
 * plan locally.  Under Gather Merge it holds for a narrower reason -- the
 * first pass of gather_merge_init() reads every source, the leader's own
 * included, in nowait mode, and only then blocks on the worker queues.  If
 * that order is ever reversed, every participant here waits on the barrier for
 * a leader that is waiting on their tuple queues.  There is a matching comment
 * in gather_merge_init(); this dependency is not expressible as an assertion.
 *
 * Hash independence.  TupleHashTableHash() already ends with murmurhash32(),
 * and the Agg above us uses the low bits of that value for its simplehash
 * bucket index and the high bits for its spill partition number.  If this node
 * partitioned on the same value, every tuple in one partition would share the
 * bits nodeAgg.c spills on and the finalize aggregate's spilling would
 * degenerate.  So we build our own value: a different per-column combiner
 * (hash_combine, not rotate-xor), a nonzero salt, and a 64-bit multiply-shift
 * finaliser instead of murmurhash32.
 *
 * src/backend/executor/nodeRepartition.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/parallel.h"
#include "common/hashfn.h"
#include "executor/executor.h"
#include "executor/nodeRepartition.h"
#include "executor/repartition.h"
#include "miscadmin.h"
#include "utils/injection_point.h"
#include "utils/memutils.h"
#include "utils/wait_event.h"

/*
 * Odd 64-bit constant (golden ratio) for the multiply-shift finaliser, and a
 * salt so that even a single-column key does not reduce to the same input
 * murmurhash32() sees inside TupleHashTableHash().
 */
#define REPARTITION_MULT		UINT64CONST(0x9E3779B97F4A7C15)
#define REPARTITION_SALT		((uint32) 0x5bf03635)

struct ParallelRepartitionState;

static void repartition_sink(RepartitionState *node);
static void repartition_end_read(RepartitionState *node, bool drained);


/*
 * Partition number for the tuple in slot.
 *
 * Deliberately unlike TupleHashTableHash(): different combiner, nonzero seed,
 * different finaliser.  Correctness only requires that equal keys map to equal
 * values, which holds because we use the same hash functions and collations
 * the Agg above uses.
 */
static inline int
repartition_partition_of(RepartitionState *node, TupleTableSlot *slot)
{
	uint32		hashkey = REPARTITION_SALT;
	uint64		mixed;
	int			i;

	for (i = 0; i < node->rs_numCols; i++)
	{
		AttrNumber	att = node->rs_hashColIdx[i];
		Datum		attr;
		bool		isNull;

		attr = slot_getattr(slot, att, &isNull);
		if (!isNull)
		{
			uint32		hkey;

			hkey = DatumGetUInt32(FunctionCall1Coll(&node->rs_hashfunctions[i],
													node->rs_collations[i],
													attr));
			hashkey = hash_combine(hashkey, hkey);
		}
		else
		{
			/*
			 * Fold the column position in even for NULL, so that (NULL,'a')
			 * and ('a',NULL) do not collide.  Purely a quality matter; either
			 * way equal keys still land together.
			 */
			hashkey = hash_combine(hashkey, (uint32) i + 1);
		}
	}

	mixed = (uint64) hashkey * REPARTITION_MULT;
	hashkey = (uint32) (mixed >> 32) ^ (uint32) mixed;

	/*
	 * With a single partition rs_part_shift is 32, and shifting a uint32 by
	 * its own width is undefined -- on x86 it is a no-op, which would put
	 * tuples in nonexistent partitions.  nodeAgg.c guards the same case in
	 * hashagg_spill_init().
	 */
	if (node->rs_part_shift == 32)
		return 0;

	return (int) (hashkey >> node->rs_part_shift);
}

/*
 * Claim the next unread partition, or -1 when they are all taken.
 */
static inline int
repartition_claim_next(RepartitionState *node)
{
	uint32		idx;

	idx = pg_atomic_fetch_add_u32(&node->rs_shared->distributor, 1);

	/*
	 * Each participant stops asking as soon as it is told there is nothing
	 * left, so the counter can overshoot K by at most one per participant.
	 * Anything beyond that means somebody is claiming after the drain, or the
	 * counter was not reset for a rescan.
	 */
	Assert(idx < (uint32) (node->rs_npartitions + node->rs_shared->nparticipants));

	if (idx >= (uint32) node->rs_npartitions)
		return -1;

	Assert(node->rs_order != NULL);
	Assert(node->rs_order[idx] >= 0 && node->rs_order[idx] < node->rs_npartitions);
	return node->rs_order[idx];
}

/*
 * Pull the child dry, routing every tuple to its partition.
 *
 * This is an ordinary pull loop, the same shape as MultiExecHash() and
 * agg_fill_hash_table().  Writing into a store another participant will read
 * wakes nobody and inverts nobody's control flow; the reader pulls when it
 * gets there.  Nothing about this node requires a push executor.
 */
static void
repartition_sink(RepartitionState *node)
{
	PlanState  *outerNode = outerPlanState(node);
	pg_atomic_uint64 *shared_counts;
	int64	   *counts;
	int64		nwritten = 0;
	int64		payload = 0;
	int			i;

	Assert(node->rs_shared != NULL);
	Assert(node->rs_accessors != NULL);
	Assert(node->rs_phase == RS_SINK);
	Assert(node->rs_attached);
	Assert(node->rs_curpart == -1);
	Assert(node->rs_npartitions == node->rs_shared->npartitions);

	shared_counts = RepartitionPartTuples(node->rs_shared);
	counts = palloc0(node->rs_npartitions * sizeof(int64));


	for (;;)
	{
		TupleTableSlot *slot;
		MinimalTuple tuple;
		bool		shouldFree;
		int			partno;

		slot = ExecProcNode(outerNode);
		if (TupIsNull(slot))
			break;

		partno = repartition_partition_of(node, slot);
		Assert(partno >= 0 && partno < node->rs_npartitions);
		Assert(node->rs_accessors[partno] != NULL);

		tuple = ExecFetchSlotMinimalTuple(slot, &shouldFree);
		Assert(tuple->t_len >= SizeofMinimalTupleHeader);
		sts_puttuple(node->rs_accessors[partno], NULL, tuple);
		counts[partno]++;
		nwritten++;
		payload += tuple->t_len;
		if (shouldFree)
			pfree(tuple);

		/*
		 * "repartition-sink-done" below only covers a clean end of the write
		 * phase.  The interesting failures -- temp_file_limit, ENOSPC -- all
		 * happen here, with some partitions written and the barrier not yet
		 * reached, so give a test a way to stand exactly there.
		 */
		if (nwritten == 1000)
			INJECTION_POINT("repartition-sink-midway", NULL);
	}

	INJECTION_POINT("repartition-sink-done", NULL);

	/*
	 * Publish the per-partition counts once, not per tuple: the drain phase
	 * uses them to hand out the biggest partitions first (§ ExecRepartition).
	 */
	for (i = 0; i < node->rs_npartitions; i++)
	{
		Assert(counts[i] >= 0);
		if (counts[i] != 0)
			pg_atomic_fetch_add_u64(&shared_counts[i], (uint64) counts[i]);
	}
	pfree(counts);

#ifdef USE_ASSERT_CHECKING
	/* one add, for the conservation checks in repartition_end_read() */
	pg_atomic_fetch_add_u64(&node->rs_shared->assert_written, (uint64) nwritten);
#endif

	if (node->rs_instrument)
	{
		node->rs_instrument->ntuples_written += nwritten;
		node->rs_instrument->bytes_payload += payload;
	}
}

/*
 * Checksum of a drain order.  Never returns 0, which the shared slot uses to
 * mean "not published yet".
 */
static uint32
repartition_order_checksum(const int *order, int k)
{
	uint32		sum = 1;
	int			i;

	for (i = 0; i < k; i++)
		sum = hash_combine(sum, (uint32) order[i]);

	return sum == 0 ? 1 : sum;
}

/*
 * Order the partitions by descending size.
 *
 * Handing them out in index order lets the participant that happens to take
 * the last, largest partition finish long after everyone else.  Sizes are
 * known once the barrier has been passed, so sort by them: this is longest
 * processing time first, whose makespan is within 4/3 - 1/(3P) of optimal,
 * against 2 - 1/P for an arbitrary order.
 *
 * Every participant computes this independently from the same shared array,
 * so the orders agree without any further synchronisation -- provided the
 * comparison really is a total order and really is fed the same input.  If it
 * ever is not, the partitions are handed out by an index into disagreeing
 * arrays: some are drained twice and others not at all, and the query returns
 * a wrong answer with no error, no assertion and consistent-looking
 * instrumentation (everything written is still read, and K partitions are
 * still claimed).  That is the worst failure mode this node has, so the orders
 * are compared rather than assumed: the first participant to arrive publishes
 * a checksum, the rest check theirs against it.
 */
static void
repartition_order_partitions(RepartitionState *node)
{
	pg_atomic_uint64 *counts;
	int			k = node->rs_npartitions;
	uint64	   *sizes;
	int			i;

	/* only ever reached on the parallel path, after the barrier */
	Assert(node->rs_shared != NULL);
	counts = RepartitionPartTuples(node->rs_shared);

	if (node->rs_order != NULL)
		pfree(node->rs_order);
	node->rs_order = palloc(k * sizeof(int));
	sizes = palloc(k * sizeof(uint64));
	for (i = 0; i < k; i++)
	{
		node->rs_order[i] = i;
		sizes[i] = pg_atomic_read_u64(&counts[i]);
	}

	/*
	 * Insertion sort; k is at most REPARTITION_MAX_PARTITIONS.  The comparison
	 * is strict, so partitions of equal size keep their index order: that,
	 * plus identical input, is what makes the result identical everywhere.
	 * Do not relax it into >= without reading the comment above first.
	 */
	for (i = 1; i < k; i++)
	{
		int			part = node->rs_order[i];
		uint64		sz = sizes[part];
		int			j = i - 1;

		while (j >= 0 && sizes[node->rs_order[j]] < sz)
		{
			node->rs_order[j + 1] = node->rs_order[j];
			j--;
		}
		node->rs_order[j + 1] = part;
	}
	pfree(sizes);

#ifdef USE_ASSERT_CHECKING
	{
		/*
		 * The order is a permutation, and the drain hands partitions out by
		 * index into it: a repeated or missing entry is a partition read twice
		 * and one never read, which is silent.
		 */
		bool	   *seen = palloc0(k * sizeof(bool));

		for (i = 0; i < k; i++)
		{
			Assert(node->rs_order[i] >= 0 && node->rs_order[i] < k);
			Assert(!seen[node->rs_order[i]]);
			seen[node->rs_order[i]] = true;
		}
		pfree(seen);
	}
#endif

	/* Publish, or check against, the order everyone else computed. */
	{
		uint32		mine = repartition_order_checksum(node->rs_order, k);
		uint32		published = 0;

		if (!pg_atomic_compare_exchange_u32(&node->rs_shared->order_checksum,
											&published, mine) &&
			published != mine)
			elog(ERROR, "repartition participants disagree on partition order");
	}
}

static void
repartition_end_read(RepartitionState *node, bool drained)
{
	if (node->rs_curpart >= 0)
	{
		SharedTuplestoreAccessor *acc = node->rs_accessors[node->rs_curpart];

		sts_end_parallel_scan(acc);

		/*
		 * We are the only reader this partition will ever have, so once it is
		 * drained nobody needs its files.  Freeing them here turns the peak
		 * temporary-space requirement from "the whole exchange" into "the
		 * whole exchange minus what has been processed".  Not on the early-out
		 * path: there the query is being torn down and DSM detach will clean
		 * up anyway.
		 */
		if (drained)
			sts_delete_files(acc);

#ifdef USE_ASSERT_CHECKING
		if (drained)
		{
			ParallelRepartitionState *pstate = node->rs_shared;
			uint64		written;
			uint64		nread;
			uint32		ndrained;

			nread = pg_atomic_fetch_add_u64(&pstate->assert_read,
											(uint64) node->rs_nread_part) +
				(uint64) node->rs_nread_part;
			written = pg_atomic_read_u64(&pstate->assert_written);
			ndrained = pg_atomic_fetch_add_u32(&pstate->assert_drained, 1) + 1;
			Assert(ndrained <= (uint32) pstate->npartitions);

			/*
			 * The weak check, on every partition: reading more than was
			 * written is wrong however the query ends.  This is the only one a
			 * query that stops early ever gets to run, and stopping early is
			 * where a truncated exchange would be hardest to notice.
			 *
			 * written is complete from the moment the barrier releases, so
			 * reading it here needs no synchronisation of its own.
			 */
			if (nread > written)
				elog(PANIC, "repartition returned " UINT64_FORMAT " tuples but only " UINT64_FORMAT " were written",
					 nread, written);

			/*
			 * The strong check, for whoever finishes the last partition: by
			 * then every read has been added, so the two must be equal.
			 */
			if (ndrained == (uint32) pstate->npartitions && nread != written)
				elog(PANIC, "repartition exchanged " UINT64_FORMAT " tuples but returned " UINT64_FORMAT,
					 written, nread);
		}
#endif

		node->rs_nread_part = 0;
		node->rs_curpart = -1;
		if (node->rs_instrument)
			node->rs_instrument->nclaimed++;
	}
}

static TupleTableSlot *
ExecRepartition(PlanState *pstate)
{
	RepartitionState *node = castNode(RepartitionState, pstate);
	int			i;

	CHECK_FOR_INTERRUPTS();

	/*
	 * No DSM segment means no other participant exists, so C1 holds trivially
	 * and there is nothing to redistribute.  We still copy through our own
	 * slot so that the result slot ops never change between calls -- the Agg
	 * above compiles its deform and grouping expressions once, against
	 * whatever ExecGetResultSlotOps() reported at init time.
	 */
	if (node->rs_shared == NULL)
	{
		TupleTableSlot *slot = ExecProcNode(outerPlanState(node));

		if (TupIsNull(slot))
			return NULL;
		return ExecCopySlot(node->rs_slot, slot);
	}

	switch (node->rs_phase)
	{
		case RS_SINK:
			{
				instr_time	start,
							mid,
							end;

				/*
				 * Fires in workers only, before this participant does anything
				 * with the exchange, so that a test can hold every worker here
				 * while the leader runs ahead.  The leader must block at the
				 * barrier.  Were the barrier to count only the participants
				 * that had reached the node -- the obvious design, and the one
				 * parallel hash join can afford because its late participant
				 * owes the build nothing -- the leader would pass alone, drain
				 * every partition and emit, and everything the workers wrote
				 * afterwards would be silently lost.
				 */
				if (IsParallelWorker())
					INJECTION_POINT("repartition-worker-sink-start", NULL);

				INJECTION_POINT("repartition-sink-start", NULL);

				/*
				 * The barrier counts one slot per *requested* worker; the
				 * slots of the workers that never started are given back by
				 * ExecRepartitionPostLaunch(), which Gather and Gather Merge
				 * call for us.  If that call is ever lost -- a third launch
				 * site, a reordering in ExecGather(), a back-patch -- the
				 * barrier can no longer be released and every participant
				 * waits on RepartitionSink forever: no error, no timeout, and
				 * nothing in the log.  Refuse to enter the wait instead.
				 *
				 * Only the leader can check this, and only when it takes part
				 * in the scan: it is the one that runs the fixup, and it is
				 * guaranteed to run it before it first executes this node.
				 */
				if (!IsParallelWorker() && !node->rs_post_launch_seen)
					elog(ERROR, "repartition node reached without post-launch fixup");

				/*
				 * ExecReScanRepartition() puts us back into RS_SINK, but the
				 * shared state -- the distributor, the barrier, the files --
				 * is reset by ExecRepartitionReInitializeDSM(), which
				 * ExecParallelReinitialize() drives separately.  A rescan that
				 * did not go through it would write into stores that have
				 * already been read and deleted: no error, and a silently
				 * short answer.  The planner cannot build such a plan today
				 * (a parallel-aware node cannot be the inner side of a nested
				 * loop, and the path is only ever built for an upper rel), so
				 * this is a tripwire for the day one of those changes.
				 */
				if ((int32) (node->rs_rescan_count - node->rs_reinit_count) > 0)
					elog(ERROR, "repartition node rescanned without reinitialising its shared state");

				/*
				 * From here until we are through the barrier, leaving without
				 * arriving would let the others past it early and truncate the
				 * exchange without a word.  ExecShutdownRepartition() asserts
				 * on this flag for exactly that reason.
				 */
				node->rs_sink_started = true;

				INSTR_TIME_SET_CURRENT(start);
				repartition_sink(node);

				for (i = 0; i < node->rs_npartitions; i++)
					sts_end_write(node->rs_accessors[i]);

				INSTR_TIME_SET_CURRENT(mid);

				/*
				 * Safe to wait: we have emitted nothing, so no participant
				 * can be blocked writing into a full tuple queue while we sit
				 * here.
				 */
				INJECTION_POINT("repartition-before-barrier", NULL);

				BarrierArriveAndWait(&node->rs_shared->sink_barrier,
									 WAIT_EVENT_REPARTITION_SINK);

				node->rs_sink_started = false;
				INJECTION_POINT("repartition-after-barrier", NULL);

				/*
				 * Everyone who was going to write has written.  If the fixup
				 * had been skipped we could not have got here at all, so this
				 * only documents the invariant for the leader's sake.
				 */
				Assert(IsParallelWorker() || node->rs_post_launch_seen);

				BarrierDetach(&node->rs_shared->sink_barrier);
				node->rs_attached = false;

				INSTR_TIME_SET_CURRENT(end);
				if (node->rs_instrument)
				{
					INSTR_TIME_ACCUM_DIFF(node->rs_instrument->time_sink,
										  mid, start);
					INSTR_TIME_ACCUM_DIFF(node->rs_instrument->time_barrier,
										  end, mid);
				}
				INSTR_TIME_SET_CURRENT(node->rs_drain_start);
			}

			repartition_order_partitions(node);
			node->rs_phase = RS_DRAIN;
			node->rs_curpart = -1;
			/* FALLTHROUGH */

		case RS_DRAIN:
			for (;;)
			{
				MinimalTuple tuple;

				if (node->rs_curpart < 0)
				{
					int			partno = repartition_claim_next(node);

					if (partno < 0)
					{
						node->rs_phase = RS_DONE;
						if (node->rs_instrument)
						{
							instr_time	now;

							INSTR_TIME_SET_CURRENT(now);
							INSTR_TIME_ACCUM_DIFF(node->rs_instrument->time_drain,
												  now, node->rs_drain_start);
						}
						return NULL;
					}
					node->rs_curpart = partno;
					node->rs_nread_part = 0;
					Assert(node->rs_accessors[partno] != NULL);
					INJECTION_POINT("repartition-drain-claim", NULL);
					sts_begin_parallel_scan(node->rs_accessors[partno]);
				}

				tuple = sts_parallel_scan_next(node->rs_accessors[node->rs_curpart],
											   NULL);
				if (tuple != NULL)
				{
					/*
					 * The tuple points into the accessor's read buffer, which
					 * the next call overwrites.  Fine for Agg, which copies
					 * into its hash table; any future consumer must be told.
					 */
#ifdef USE_ASSERT_CHECKING
					node->rs_nread_part++;
#endif
					if (node->rs_instrument)
						node->rs_instrument->ntuples_read++;
					return ExecStoreMinimalTuple(tuple, node->rs_slot, false);
				}

				repartition_end_read(node, true);
				CHECK_FOR_INTERRUPTS();
			}

		case RS_DONE:
			return NULL;
	}

	return NULL;				/* keep compiler quiet */
}

RepartitionState *
ExecInitRepartition(Repartition *node, EState *estate, int eflags)
{
	RepartitionState *rstate;
	Oid		   *eqfuncoids;

	/* Backward scan is impossible; the node reorders its input. */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	rstate = makeNode(RepartitionState);
	rstate->ps.plan = (Plan *) node;
	rstate->ps.state = estate;
	rstate->ps.ExecProcNode = ExecRepartition;

	rstate->rs_numCols = node->numCols;
	rstate->rs_hashColIdx = node->hashColIdx;
	rstate->rs_collations = node->collations;
	rstate->rs_npartitions = node->npartitions;
	{
		int			bits = 0;

		while ((1 << bits) < node->npartitions)
			bits++;
		rstate->rs_part_shift = 32 - bits;

		/*
		 * bits == 0 gives a shift of 32, which is undefined on a uint32 and
		 * is why repartition_partition_of() special-cases it.
		 */
		Assert(bits >= 0 && bits <= 6);
		Assert((1 << bits) == node->npartitions);
	}
	Assert(node->npartitions > 0 &&
		   node->npartitions <= REPARTITION_MAX_PARTITIONS);
	Assert(node->npartitions == pg_nextpower2_32(node->npartitions));
	Assert(node->numCols > 0);
	Assert(node->plan.parallel_aware);

	rstate->rs_phase = RS_SINK;
	rstate->rs_curpart = -1;
	rstate->rs_attached = false;
	rstate->rs_post_launch_seen = false;
	rstate->rs_sink_started = false;
	rstate->rs_nread_part = 0;
	rstate->rs_reinit_count = 0;
	rstate->rs_rescan_count = 0;
	rstate->rs_shared = NULL;
	rstate->rs_accessors = NULL;
	rstate->rs_order = NULL;
	rstate->rs_shared_info = NULL;
	rstate->rs_instrument = NULL;

	outerPlanState(rstate) = ExecInitNode(outerPlan(node), estate, eflags);

	/*
	 * The node does not project and its targetlist is a verbatim copy of the
	 * child's (see create_repartition_plan), so the result slot mirrors the
	 * child's descriptor.  Minimal-tuple ops, because that is what the drain
	 * phase produces.
	 */
	rstate->ps.ps_ProjInfo = NULL;
	ExecInitResultTupleSlotTL(&rstate->ps, &TTSOpsMinimalTuple);
	rstate->rs_slot = rstate->ps.ps_ResultTupleSlot;

	execTuplesHashPrepare(node->numCols, node->hashOperators,
						  &eqfuncoids, &rstate->rs_hashfunctions);
	pfree(eqfuncoids);

	/*
	 * Accessors and their 32kB write chunks are allocated by sts_initialize()
	 * / sts_attach() in whatever context is current at the time, so give them
	 * one we can reset independently of the per-query context.  Parallel hash
	 * join does the same with its spillCxt.
	 */
	rstate->rs_spillCxt = AllocSetContextCreate(estate->es_query_cxt,
												"RepartitionSpillContext",
												ALLOCSET_DEFAULT_SIZES);

	return rstate;
}

void
ExecEndRepartition(RepartitionState *node)
{
	ExecShutdownRepartition(node);
	ExecEndNode(outerPlanState(node));
}

void
ExecReScanRepartition(RepartitionState *node)
{
	/*
	 * Only local state here.  Shared state is reset by
	 * ExecRepartitionReInitializeDSM(), which ExecParallelReinitialize()
	 * drives from ExecReScanGather() -- the node is parallel_aware, so
	 * ExecReScanGather() does not call ExecReScan() on us directly.
	 */
	repartition_end_read(node, false);
	node->rs_phase = RS_SINK;
	node->rs_curpart = -1;
	node->rs_rescan_count++;

	if (node->ps.lefttree->chgParam == NULL)
		ExecReScan(node->ps.lefttree);
}

void
ExecShutdownRepartition(RepartitionState *node)
{
	if (node->rs_shared == NULL)
		return;

	/*
	 * Called from ExecShutdownNode(), which walks the tree post-order, so this
	 * runs before ExecShutdownGather() stops the workers.  That means we must
	 * touch nothing shared beyond our own barrier slot and our own accessors:
	 * the other participants are still running.
	 */
	/*
	 * Detaching from the barrier is only safe once this participant has either
	 * arrived at it or written nothing.  Detaching in between reduces the
	 * party and lets everyone else through early, and what they then read is a
	 * partially written exchange -- a short answer with no error anywhere.  No
	 * caller does this today: ExecShutdownNode() runs after the plan has
	 * stopped producing tuples, and this node produces none before the
	 * barrier.  That is a property of the callers, not of this code, so check
	 * it here rather than trust it.
	 */
	Assert(!node->rs_sink_started);

	/*
	 * No guard check here, tempting as it is: this function runs twice, once
	 * from ExecShutdownNode() while the segment is still mapped and again from
	 * ExecEndRepartition() after ExecParallelCleanup() has destroyed the
	 * parallel context, and by then rs_shared points into unmapped memory.
	 * The prototype gets away with reading it only because rs_attached is
	 * false by that point.  The guards are checked instead at every point that
	 * is provably inside the parallel region: post-launch, worker attach,
	 * re-initialise, the end of the drain, and instrumentation retrieval,
	 * which ExecParallelCleanup() calls before the detach.
	 */
	repartition_end_read(node, false);

	if (node->rs_attached)
	{
		BarrierDetach(&node->rs_shared->sink_barrier);
		node->rs_attached = false;
	}
}

/* ----------------------------------------------------------------
 *						Parallel Execution Support
 * ----------------------------------------------------------------
 */

/*
 * Size of the single DSM allocation this node needs, and, in *instrument_offset,
 * where inside it the instrumentation array begins.
 *
 * The offset is returned rather than recomputed by a second function because
 * the two have to agree exactly: a divergence would put the workers'
 * instrumentation on top of the last tuplestore's control data, which no test
 * would notice until something else started reading it.  It is stored in the
 * struct as well, so that workers never recompute it at all.
 *
 * *instrument_offset is set to 0 when ninstrument is 0.
 */
/*
 * repartition_layout
 *		Decide where everything lives inside the single DSM allocation.
 *
 * Called once by the leader, for the estimate and again for the real thing;
 * the result is then stored in the shared state and never recomputed.  See
 * RepartitionLayout in repartition.h for the picture.
 *
 */
static void
repartition_layout(int npartitions, int nparticipants, int ninstrument,
				   RepartitionLayout *layout)
{
	Size		size;

	Assert(npartitions > 0 && npartitions <= REPARTITION_MAX_PARTITIONS);
	Assert(npartitions == pg_nextpower2_32(npartitions));
	Assert(nparticipants > 0);
	Assert(ninstrument == 0 || ninstrument == nparticipants - 1);
	Assert(layout != NULL);

	size = MAXALIGN(sizeof(ParallelRepartitionState));

	layout->counts_offset = size;
	size = add_size(size, MAXALIGN(mul_size(npartitions,
											sizeof(pg_atomic_uint64))));

	layout->sts_size = MAXALIGN(sts_estimate(nparticipants));
	layout->sts_stride = layout->sts_size;
	layout->sts_offset = size;
	size = add_size(size, mul_size(layout->sts_stride, npartitions));

	if (ninstrument > 0)
	{
		layout->instrument_offset = size;
		layout->instrument_size = RepartitionSharedInfoSize(ninstrument);
		size = add_size(size, MAXALIGN(layout->instrument_size));
	}
	else
	{
		layout->instrument_offset = 0;
		layout->instrument_size = 0;
	}

	layout->alloc_size = size;

	Assert(layout->counts_offset == MAXALIGN(layout->counts_offset));
	Assert(layout->sts_offset == MAXALIGN(layout->sts_offset));
	Assert(layout->instrument_offset == MAXALIGN(layout->instrument_offset));
	Assert(layout->alloc_size == MAXALIGN(layout->alloc_size));
	Assert(layout->alloc_size < MaxAllocSize);
}

void
ExecRepartitionEstimate(RepartitionState *node, ParallelContext *pcxt)
{
	RepartitionLayout layout;

	repartition_layout(node->rs_npartitions, pcxt->nworkers + 1,
					   node->ps.instrument ? pcxt->nworkers : 0, &layout);
	shm_toc_estimate_chunk(&pcxt->estimator, layout.alloc_size);
	shm_toc_estimate_keys(&pcxt->estimator, 1);
}

void
ExecRepartitionInitializeDSM(RepartitionState *node, ParallelContext *pcxt)
{
	ParallelRepartitionState *pstate;
	RepartitionLayout layout;
	MemoryContext oldcxt;
	int			nparticipants = pcxt->nworkers + 1;
	int			ninstrument;
	int			i;

	Assert(node->rs_shared == NULL);		/* once per execution, before launch */
	Assert(node->rs_npartitions > 0 &&
		   node->rs_npartitions <= REPARTITION_MAX_PARTITIONS);
	Assert(node->rs_npartitions == pg_nextpower2_32(node->rs_npartitions));
	Assert(!IsParallelWorker());

	/*
	 * Must be first.  With no real segment pcxt->toc is still valid (it is
	 * built over backend-private memory), so shm_toc_allocate() would happily
	 * succeed, but SharedFileSetInit() would not register its on_dsm_detach
	 * callback and the temporary files would never be cleaned up.
	 */
	if (pcxt->seg == NULL)
		return;

	ninstrument = node->ps.instrument ? pcxt->nworkers : 0;
	repartition_layout(node->rs_npartitions, nparticipants, ninstrument,
					   &layout);
	pstate = shm_toc_allocate(pcxt->toc, layout.alloc_size);
	memset(pstate, 0, layout.alloc_size);
	shm_toc_insert(pcxt->toc, node->ps.plan->plan_node_id, pstate);

	/*
	 * magic first: every accessor asserts on it, so anything that reads this
	 * area before this point is reading a zeroed struct and will say so.
	 */
	pstate->magic = REPARTITION_MAGIC;
	pstate->npartitions = node->rs_npartitions;
	pstate->nparticipants = nparticipants;
	pstate->ninstrument = ninstrument;
	pstate->layout = layout;
	pg_atomic_init_u32(&pstate->distributor, 0);
	pg_atomic_init_u32(&pstate->order_checksum, 0);
	pg_atomic_init_u64(&pstate->assert_written, 0);
	pg_atomic_init_u64(&pstate->assert_read, 0);
	pg_atomic_init_u32(&pstate->assert_drained, 0);
	for (i = 0; i < node->rs_npartitions; i++)
		pg_atomic_init_u64(&RepartitionPartTuples(pstate)[i], 0);

	if (ninstrument > 0)
	{
		/*
		 * Set num_workers through the raw offset: RepartitionSharedInfo()
		 * asserts on the value we are about to write.
		 */
		SharedRepartitionInfo *si = (SharedRepartitionInfo *)
			((char *) pstate + pstate->layout.instrument_offset);

		si->num_workers = ninstrument;
		Assert(RepartitionSharedInfo(pstate) == si);
	}

	/*
	 * The barrier must already account for every participant before any of
	 * them can arrive, otherwise a participant that reaches the node late
	 * would write its tuples into partitions that have already been read and
	 * handed upwards.  That is the crucial difference from parallel hash
	 * join, whose dynamic build_barrier is safe precisely because a late
	 * participant there has nothing to contribute to the build.
	 *
	 * We cannot use a static-party barrier: BarrierDetach() asserts
	 * !static_party, and we need detach both to cancel the slots of workers
	 * that fail to launch and to leave the barrier afterwards.  So attach one
	 * slot per requested worker plus one for the leader, here, before
	 * LaunchParallelWorkers() has run.  No worker exists yet, so nothing can
	 * arrive while we do it.  ExecRepartitionPostLaunch() then gives back the
	 * slots that turned out to be unnecessary.
	 */
	BarrierInit(&pstate->sink_barrier, 0);
	for (i = 0; i < nparticipants; i++)
		BarrierAttach(&pstate->sink_barrier);

	SharedFileSetInit(&pstate->fileset, pcxt->seg);

	oldcxt = MemoryContextSwitchTo(node->rs_spillCxt);
	node->rs_accessors = palloc(node->rs_npartitions *
								sizeof(SharedTuplestoreAccessor *));
	for (i = 0; i < node->rs_npartitions; i++)
	{
		char		name[32];

		snprintf(name, sizeof(name), "rp%d.%d",
				 node->ps.plan->plan_node_id, i);
		node->rs_accessors[i] = sts_initialize(RepartitionSTS(pstate, i),
											   nparticipants,
											   ParallelWorkerNumber + 1,
											   0,	/* no per-tuple metadata */
											   SHARED_TUPLESTORE_SINGLE_PASS |
											   SHARED_TUPLESTORE_SINGLE_READER,
											   &pstate->fileset,
											   name);
	}
	MemoryContextSwitchTo(oldcxt);

	node->rs_shared = pstate;
	node->rs_attached = true;
	node->rs_post_launch_seen = false;
	node->rs_sink_started = false;

	/*
	 * The leader's own counters stay in backend-local memory, as they do for
	 * Hash; only workers write into the shared array.
	 */
	if (node->ps.instrument)
		node->rs_instrument = palloc0(sizeof(RepartitionInstrumentation));
}

/*
 * Called by Gather/Gather Merge right after LaunchParallelWorkers(), before
 * the leader starts executing the subplan.
 *
 * Gives back one barrier slot per worker that failed to launch, plus the
 * leader's own slot when the leader will not execute the subplan.
 *
 * Safe against a worker that is already running and arriving at the barrier.
 * Write p for the participant count and a for the number arrived.  Throughout
 * this loop p >= p_final = nworkers_launched + (leader ? 1 : 0), and until the
 * leader arrives a <= nworkers_launched.  When the leader participates,
 * a <= nworkers_launched < p_final <= p, so the barrier cannot advance.  When
 * it does not, a == p is reachable only at p == p_final with every launched
 * worker arrived -- which is exactly when the barrier should advance.
 */
void
ExecRepartitionPostLaunch(RepartitionState *node, ParallelContext *pcxt,
						  bool leader_participates)
{
	int			ncancel;
	int			i;

	if (node->rs_shared == NULL)
		return;

	Assert(!IsParallelWorker());
	Assert(pcxt->nworkers_launched >= 0 &&
		   pcxt->nworkers_launched <= pcxt->nworkers);
	Assert(node->rs_shared->nparticipants == pcxt->nworkers + 1);
	Assert(!node->rs_post_launch_seen);		/* exactly once per launch */

	ncancel = pcxt->nworkers - pcxt->nworkers_launched;
	if (!leader_participates)
	{
		ncancel++;
		node->rs_attached = false;
	}

	for (i = 0; i < ncancel; i++)
		BarrierDetach(&node->rs_shared->sink_barrier);

	node->rs_post_launch_seen = true;
}

void
ExecRepartitionReInitializeDSM(RepartitionState *node, ParallelContext *pcxt)
{
	ParallelRepartitionState *pstate = node->rs_shared;
	MemoryContext oldcxt;
	int			i;

	if (pstate == NULL)
		return;

	Assert(!IsParallelWorker());

	/*
	 * sts_reinitialize() is not enough and is in fact unused in the tree: it
	 * resets read_page only, leaving the data in the files and npages
	 * untouched, so a second pass would append to the first and emit
	 * duplicates.  Throw the files away and build the stores again, which is
	 * what ExecHashJoinReInitializeDSM() does.
	 */
	repartition_end_read(node, false);

	SharedFileSetDeleteAll(&pstate->fileset);

	/*
	 * The accessors and their buffers all live in rs_spillCxt, and we are
	 * about to build a new set: reset the context rather than abandon the old
	 * ones in it.  They are worth K * (accessor + STS_CHUNK_PAGES * BLCKSZ)
	 * per rescan, which at K = 64 is megabytes per iteration.  Parallel hash
	 * join does the same with its own spillCxt.  Must come after
	 * repartition_end_read(), which still reads rs_accessors.
	 */
	MemoryContextReset(node->rs_spillCxt);
	node->rs_accessors = NULL;

	oldcxt = MemoryContextSwitchTo(node->rs_spillCxt);
	node->rs_accessors = palloc(node->rs_npartitions *
								sizeof(SharedTuplestoreAccessor *));
	for (i = 0; i < node->rs_npartitions; i++)
	{
		char		name[32];

		snprintf(name, sizeof(name), "rp%d.%d",
				 node->ps.plan->plan_node_id, i);
		node->rs_accessors[i] = sts_initialize(RepartitionSTS(pstate, i),
											   pstate->nparticipants,
											   ParallelWorkerNumber + 1,
											   0,
											   SHARED_TUPLESTORE_SINGLE_PASS |
											   SHARED_TUPLESTORE_SINGLE_READER,
											   &pstate->fileset,
											   name);
	}
	MemoryContextSwitchTo(oldcxt);

	pg_atomic_write_u32(&pstate->distributor, 0);
	pg_atomic_write_u32(&pstate->order_checksum, 0);
	pg_atomic_write_u64(&pstate->assert_written, 0);
	pg_atomic_write_u64(&pstate->assert_read, 0);
	pg_atomic_write_u32(&pstate->assert_drained, 0);
	for (i = 0; i < node->rs_npartitions; i++)
		pg_atomic_write_u64(&RepartitionPartTuples(pstate)[i], 0);
	BarrierInit(&pstate->sink_barrier, 0);
	for (i = 0; i < pstate->nparticipants; i++)
		BarrierAttach(&pstate->sink_barrier);
	node->rs_attached = true;

	/* the workers are launched again, so the fixup has to run again */
	node->rs_post_launch_seen = false;
	node->rs_sink_started = false;
	node->rs_reinit_count++;

}

void
ExecRepartitionInitializeWorker(RepartitionState *node,
								ParallelWorkerContext *pwcxt)
{
	ParallelRepartitionState *pstate;
	MemoryContext oldcxt;
	int			i;

	pstate = shm_toc_lookup(pwcxt->toc, node->ps.plan->plan_node_id, false);

	/*
	 * The plan and the shared state are two sources of truth for K: this node
	 * takes the partition count from the shared state below, but
	 * repartition_partition_of() shifts by rs_part_shift, which was computed
	 * from the plan in ExecInitRepartition().  They agree because the plan is
	 * the same everywhere -- but if they ever stop agreeing, tuples land in
	 * rs_accessors[] past its end, and that is a heap corruption in a worker
	 * with no proximate cause.
	 */
	Assert(pstate->magic == REPARTITION_MAGIC);
	Assert(pstate->npartitions == node->rs_npartitions);
	Assert(pstate->nparticipants > ParallelWorkerNumber + 1);
	Assert(IsParallelWorker());

	/* a relaunched worker re-attaches; do not leak the previous set */
	MemoryContextReset(node->rs_spillCxt);
	node->rs_accessors = NULL;

	oldcxt = MemoryContextSwitchTo(node->rs_spillCxt);
	node->rs_accessors = palloc(pstate->npartitions *
								sizeof(SharedTuplestoreAccessor *));
	for (i = 0; i < pstate->npartitions; i++)
		node->rs_accessors[i] = sts_attach(RepartitionSTS(pstate, i),
										   ParallelWorkerNumber + 1,
										   &pstate->fileset);
	MemoryContextSwitchTo(oldcxt);

	node->rs_shared = pstate;

	if (node->ps.instrument)
	{
		SharedRepartitionInfo *si = RepartitionSharedInfo(pstate);

		if (si != NULL && ParallelWorkerNumber < si->num_workers)
			node->rs_instrument = &si->instrument[ParallelWorkerNumber];
	}

	/*
	 * Do not attach: the leader reserved one slot per requested worker in
	 * ExecRepartitionInitializeDSM(), before any worker existed.  We own one
	 * of those, and give it back when we arrive or shut down.
	 */
	node->rs_attached = true;
}

/*
 * Copy the workers' counters out of DSM before it goes away.  Called from
 * ExecParallelCleanup(), i.e. after WaitForParallelWorkersToFinish().
 */
void
ExecRepartitionRetrieveInstrumentation(RepartitionState *node)
{
	SharedRepartitionInfo *si;
	Size		size;

	if (node->rs_shared == NULL)
		return;

	Assert(!IsParallelWorker());

	si = RepartitionSharedInfo(node->rs_shared);
	if (si == NULL)
		return;

	size = RepartitionSharedInfoSize(si->num_workers);
	node->rs_shared_info = palloc(size);
	memcpy(node->rs_shared_info, si, size);
}
