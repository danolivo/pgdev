/*-------------------------------------------------------------------------
 *
 * nodeAgg.c
 *	  Routines to handle aggregate nodes.
 *
 *	  ExecAgg normally evaluates each aggregate in the following steps:
 *
 *		 transvalue = initcond
 *		 foreach input_tuple do
 *			transvalue = transfunc(transvalue, input_value(s))
 *		 result = finalfunc(transvalue, direct_argument(s))
 *
 *	  If a finalfunc is not supplied then the result is just the ending
 *	  value of transvalue.
 *
 *	  Other behaviors can be selected by the "aggsplit" mode, which exists
 *	  to support partial aggregation.  It is possible to:
 *	  * Skip running the finalfunc, so that the output is always the
 *	  final transvalue state.
 *	  * Substitute the combinefunc for the transfunc, so that transvalue
 *	  states (propagated up from a child partial-aggregation step) are merged
 *	  rather than processing raw input rows.  (The statements below about
 *	  the transfunc apply equally to the combinefunc, when it's selected.)
 *	  * Apply the serializefunc to the output values (this only makes sense
 *	  when skipping the finalfunc, since the serializefunc works on the
 *	  transvalue data type).
 *	  * Apply the deserializefunc to the input values (this only makes sense
 *	  when using the combinefunc, for similar reasons).
 *	  It is the planner's responsibility to connect up Agg nodes using these
 *	  alternate behaviors in a way that makes sense, with partial aggregation
 *	  results being fed to nodes that expect them.
 *
 *	  If a normal aggregate call specifies DISTINCT or ORDER BY, we sort the
 *	  input tuples and eliminate duplicates (if required) before performing
 *	  the above-depicted process.  (However, we don't do that for ordered-set
 *	  aggregates; their "ORDER BY" inputs are ordinary aggregate arguments
 *	  so far as this module is concerned.)	Note that partial aggregation
 *	  is not supported in these cases, since we couldn't ensure global
 *	  ordering or distinctness of the inputs.
 *
 *	  If transfunc is marked "strict" in pg_proc and initcond is NULL,
 *	  then the first non-NULL input_value is assigned directly to transvalue,
 *	  and transfunc isn't applied until the second non-NULL input_value.
 *	  The agg's first input type and transtype must be the same in this case!
 *
 *	  If transfunc is marked "strict" then NULL input_values are skipped,
 *	  keeping the previous transvalue.  If transfunc is not strict then it
 *	  is called for every input tuple and must deal with NULL initcond
 *	  or NULL input_values for itself.
 *
 *	  If finalfunc is marked "strict" then it is not called when the
 *	  ending transvalue is NULL, instead a NULL result is created
 *	  automatically (this is just the usual handling of strict functions,
 *	  of course).  A non-strict finalfunc can make its own choice of
 *	  what to return for a NULL ending transvalue.
 *
 *	  Ordered-set aggregates are treated specially in one other way: we
 *	  evaluate any "direct" arguments and pass them to the finalfunc along
 *	  with the transition value.
 *
 *	  A finalfunc can have additional arguments beyond the transvalue and
 *	  any "direct" arguments, corresponding to the input arguments of the
 *	  aggregate.  These are always just passed as NULL.  Such arguments may be
 *	  needed to allow resolution of a polymorphic aggregate's result type.
 *
 *	  We compute aggregate input expressions and run the transition functions
 *	  in a temporary econtext (aggstate->tmpcontext).  This is reset at least
 *	  once per input tuple, so when the transvalue datatype is
 *	  pass-by-reference, we have to be careful to copy it into a longer-lived
 *	  memory context, and free the prior value to avoid memory leakage.  We
 *	  store transvalues in another set of econtexts, aggstate->aggcontexts
 *	  (one per grouping set, see below), which are also used for the hashtable
 *	  structures in AGG_HASHED mode.  These econtexts are rescanned, not just
 *	  reset, at group boundaries so that aggregate transition functions can
 *	  register shutdown callbacks via AggRegisterCallback.
 *
 *	  The node's regular econtext (aggstate->ss.ps.ps_ExprContext) is used to
 *	  run finalize functions and compute the output tuple; this context can be
 *	  reset once per output tuple.
 *
 *	  The executor's AggState node is passed as the fmgr "context" value in
 *	  all transfunc and finalfunc calls.  It is not recommended that the
 *	  transition functions look at the AggState node directly, but they can
 *	  use AggCheckCallContext() to verify that they are being called by
 *	  nodeAgg.c (and not as ordinary SQL functions).  The main reason a
 *	  transition function might want to know this is so that it can avoid
 *	  palloc'ing a fixed-size pass-by-ref transition value on every call:
 *	  it can instead just scribble on and return its left input.  Ordinarily
 *	  it is completely forbidden for functions to modify pass-by-ref inputs,
 *	  but in the aggregate case we know the left input is either the initial
 *	  transition value or a previous function result, and in either case its
 *	  value need not be preserved.  See int8inc() for an example.  Notice that
 *	  the EEOP_AGG_PLAIN_TRANS step is coded to avoid a data copy step when
 *	  the previous transition value pointer is returned.  It is also possible
 *	  to avoid repeated data copying when the transition value is an expanded
 *	  object: to do that, the transition function must take care to return
 *	  an expanded object that is in a child context of the memory context
 *	  returned by AggCheckCallContext().  Also, some transition functions want
 *	  to store working state in addition to the nominal transition value; they
 *	  can use the memory context returned by AggCheckCallContext() to do that.
 *
 *	  Note: AggCheckCallContext() is available as of PostgreSQL 9.0.  The
 *	  AggState is available as context in earlier releases (back to 8.1),
 *	  but direct examination of the node is needed to use it before 9.0.
 *
 *	  As of 9.4, aggregate transition functions can also use AggGetAggref()
 *	  to get hold of the Aggref expression node for their aggregate call.
 *	  This is mainly intended for ordered-set aggregates, which are not
 *	  supported as window functions.  (A regular aggregate function would
 *	  need some fallback logic to use this, since there's no Aggref node
 *	  for a window function.)
 *
 *	  Grouping sets:
 *
 *	  A list of grouping sets which is structurally equivalent to a ROLLUP
 *	  clause (e.g. (a,b,c), (a,b), (a)) can be processed in a single pass over
 *	  ordered data.  We do this by keeping a separate set of transition values
 *	  for each grouping set being concurrently processed; for each input tuple
 *	  we update them all, and on group boundaries we reset those states
 *	  (starting at the front of the list) whose grouping values have changed
 *	  (the list of grouping sets is ordered from most specific to least
 *	  specific).
 *
 *	  Where more complex grouping sets are used, we break them down into
 *	  "phases", where each phase has a different sort order (except phase 0
 *	  which is reserved for hashing).  During each phase but the last, the
 *	  input tuples are additionally stored in a tuplesort which is keyed to the
 *	  next phase's sort order; during each phase but the first, the input
 *	  tuples are drawn from the previously sorted data.  (The sorting of the
 *	  data for the first phase is handled by the planner, as it might be
 *	  satisfied by underlying nodes.)
 *
 *	  Hashing can be mixed with sorted grouping.  To do this, we have an
 *	  AGG_MIXED strategy that populates the hashtables during the first sorted
 *	  phase, and switches to reading them out after completing all sort phases.
 *	  We can also support AGG_HASHED with multiple hash tables and no sorting
 *	  at all.
 *
 *	  From the perspective of aggregate transition and final functions, the
 *	  only issue regarding grouping sets is this: a single call site (flinfo)
 *	  of an aggregate function may be used for updating several different
 *	  transition values in turn. So the function must not cache in the flinfo
 *	  anything which logically belongs as part of the transition value (most
 *	  importantly, the memory context in which the transition value exists).
 *	  The support API functions (AggCheckCallContext, AggRegisterCallback) are
 *	  sensitive to the grouping set for which the aggregate function is
 *	  currently being called.
 *
 *	  Plan structure:
 *
 *	  What we get from the planner is actually one "real" Agg node which is
 *	  part of the plan tree proper, but which optionally has an additional list
 *	  of Agg nodes hung off the side via the "chain" field.  This is because an
 *	  Agg node happens to be a convenient representation of all the data we
 *	  need for grouping sets.
 *
 *	  For many purposes, we treat the "real" node as if it were just the first
 *	  node in the chain.  The chain must be ordered such that hashed entries
 *	  come before sorted/plain entries; the real node is marked AGG_MIXED if
 *	  there are both types present (in which case the real node describes one
 *	  of the hashed groupings, other AGG_HASHED nodes may optionally follow in
 *	  the chain, followed in turn by AGG_SORTED or (one) AGG_PLAIN node).  If
 *	  the real node is marked AGG_HASHED or AGG_SORTED, then all the chained
 *	  nodes must be of the same type; if it is AGG_PLAIN, there can be no
 *	  chained nodes.
 *
 *	  We collect all hashed nodes into a single "phase", numbered 0, and create
 *	  a sorted phase (numbered 1..n) for each AGG_SORTED or AGG_PLAIN node.
 *	  Phase 0 is allocated even if there are no hashes, but remains unused in
 *	  that case.
 *
 *	  AGG_HASHED nodes actually refer to only a single grouping set each,
 *	  because for each hashed grouping we need a separate grpColIdx and
 *	  numGroups estimate.  AGG_SORTED nodes represent a "rollup", a list of
 *	  grouping sets that share a sort order.  Each AGG_SORTED node other than
 *	  the first one has an associated Sort node which describes the sort order
 *	  to be used; the first sorted node takes its input from the outer subtree,
 *	  which the planner has already arranged to provide ordered data.
 *
 *	  Memory and ExprContext usage:
 *
 *	  Because we're accumulating aggregate values across input rows, we need to
 *	  use more memory contexts than just simple input/output tuple contexts.
 *	  In fact, for a rollup, we need a separate context for each grouping set
 *	  so that we can reset the inner (finer-grained) aggregates on their group
 *	  boundaries while continuing to accumulate values for outer
 *	  (coarser-grained) groupings.  On top of this, we might be simultaneously
 *	  populating hashtables; however, we only need one context for all the
 *	  hashtables.
 *
 *	  So we create an array, aggcontexts, with an ExprContext for each grouping
 *	  set in the largest rollup that we're going to process, and use the
 *	  per-tuple memory context of those ExprContexts to store the aggregate
 *	  transition values.  hashcontext is the single context created to support
 *	  all hash tables.
 *
 *	  Spilling To Disk
 *
 *	  When performing hash aggregation, if the hash table memory exceeds the
 *	  limit (see hash_agg_check_limits()), we enter "spill mode". In spill
 *	  mode, we advance the transition states only for groups already in the
 *	  hash table. For tuples that would need to create a new hash table
 *	  entries (and initialize new transition states), we instead spill them to
 *	  disk to be processed later. The tuples are spilled in a partitioned
 *	  manner, so that subsequent batches are smaller and less likely to exceed
 *	  hash_mem (if a batch does exceed hash_mem, it must be spilled
 *	  recursively).
 *
 *	  Spilled data is written to logical tapes. These provide better control
 *	  over memory usage, disk space, and the number of files than if we were
 *	  to use a BufFile for each spill.  We don't know the number of tapes needed
 *	  at the start of the algorithm (because it can recurse), so a tape set is
 *	  allocated at the beginning, and individual tapes are created as needed.
 *	  As a particular tape is read, logtape.c recycles its disk space. When a
 *	  tape is read to completion, it is destroyed entirely.
 *
 *	  Tapes' buffers can take up substantial memory when many tapes are open at
 *	  once. We only need one tape open at a time in read mode (using a buffer
 *	  that's a multiple of BLCKSZ); but we need one tape open in write mode (each
 *	  requiring a buffer of size BLCKSZ) for each partition.
 *
 *	  Note that it's possible for transition states to start small but then
 *	  grow very large; for instance in the case of ARRAY_AGG. In such cases,
 *	  it's still possible to significantly exceed hash_mem. We try to avoid
 *	  this situation by estimating what will fit in the available memory, and
 *	  imposing a limit on the number of groups separately from the amount of
 *	  memory consumed.
 *
 *    Transition / Combine function invocation:
 *
 *    For performance reasons transition functions, including combine
 *    functions, aren't invoked one-by-one from nodeAgg.c after computing
 *    arguments using the expression evaluation engine. Instead
 *    ExecBuildAggTrans() builds one large expression that does both argument
 *    evaluation and transition function invocation. That avoids performance
 *    issues due to repeated uses of expression evaluation, complications due
 *    to filter expressions having to be evaluated early, and allows to JIT
 *    the entire expression into one native function.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeAgg.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "access/parallel.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "common/hashfn.h"
#include "executor/execExpr.h"
#include "executor/executor.h"
#include "executor/instrument.h"
#include "executor/nodeAgg.h"
#include "lib/hyperloglog.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/optimizer.h"
#include "parser/parse_agg.h"
#include "parser/parse_coerce.h"
#include "port/pg_bitutils.h"
#include "storage/barrier.h"
#include "storage/lwlock.h"
#include "storage/sharedfileset.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/dsa.h"
#include "utils/wait_event.h"
#include "utils/expandeddatum.h"
#include "utils/injection_point.h"
#include "utils/logtape.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/memutils_memorychunk.h"
#include "utils/sharedtuplestore.h"
#include "utils/syscache.h"
#include "utils/tuplesort.h"

/*
 * Control how many partitions are created when spilling HashAgg to
 * disk.
 *
 * HASHAGG_PARTITION_FACTOR is multiplied by the estimated number of
 * partitions needed such that each partition will fit in memory. The factor
 * is set higher than one because there's not a high cost to having a few too
 * many partitions, and it makes it less likely that a partition will need to
 * be spilled recursively. Another benefit of having more, smaller partitions
 * is that small hash tables may perform better than large ones due to memory
 * caching effects.
 *
 * We also specify a min and max number of partitions per spill. Too few might
 * mean a lot of wasted I/O from repeated spilling of the same tuples. Too
 * many will result in lots of memory wasted buffering the spill files (which
 * could instead be spent on a larger hash table).
 */
#define HASHAGG_PARTITION_FACTOR 1.50
#define HASHAGG_MIN_PARTITIONS 4
#define HASHAGG_MAX_PARTITIONS 1024

/*
 * For reading from tapes, the buffer size must be a multiple of
 * BLCKSZ. Larger values help when reading from multiple tapes concurrently,
 * but that doesn't happen in HashAgg, so we simply use BLCKSZ. Writing to a
 * tape always uses a buffer of size BLCKSZ.
 */
#define HASHAGG_READ_BUFFER_SIZE BLCKSZ
#define HASHAGG_WRITE_BUFFER_SIZE BLCKSZ

/*
 * HyperLogLog is used for estimating the cardinality of the spilled tuples in
 * a given partition. 5 bits corresponds to a size of about 32 bytes and a
 * worst-case error of around 18%. That's effective enough to choose a
 * reasonable number of partitions when recursing.
 */
#define HASHAGG_HLL_BIT_WIDTH 5

/*
 * Assume the palloc overhead always uses sizeof(MemoryChunk) bytes.
 */
#define CHUNKHDRSZ sizeof(MemoryChunk)

/*
 * Represents partitioned spill data for a single hashtable. Contains the
 * necessary information to route tuples to the correct partition, and to
 * transform the spilled data into new batches.
 *
 * The high bits are used for partition selection (when recursing, we ignore
 * the bits that have already been used for partition selection at an earlier
 * level).
 */
typedef struct HashAggSpill
{
	int			npartitions;	/* number of partitions */
	LogicalTape **partitions;	/* spill partition tapes */
	int64	   *ntuples;		/* number of tuples in each partition */
	uint32		mask;			/* mask to find partition from hash value */
	int			shift;			/* after masking, shift by this amount */
	hyperLogLogState *hll_card; /* cardinality estimate for contents */
} HashAggSpill;

/*
 * Represents work to be done for one pass of hash aggregation (with only one
 * grouping set).
 *
 * Also tracks the bits of the hash already used for partition selection by
 * earlier iterations, so that this batch can use new bits. If all bits have
 * already been used, no partitioning will be done (any spilled data will go
 * to a single output tape).
 */
typedef struct HashAggBatch
{
	int			setno;			/* grouping set */
	int			used_bits;		/* number of bits of hash already used */
	LogicalTape *input_tape;	/* input partition tape */
	int64		input_tuples;	/* number of tuples in this batch */
	double		input_card;		/* estimated group cardinality */
} HashAggBatch;

/* used to find referenced colnos */
typedef struct FindColsContext
{
	bool		is_aggref;		/* is under an aggref */
	Bitmapset  *aggregated;		/* column references under an aggref */
	Bitmapset  *unaggregated;	/* other column references */
} FindColsContext;

/*
 * Parallel shared hash aggregation (Parallel Hash Aggregate, v0 prototype).
 *
 * All participants (workers plus possibly the leader) cooperatively build a
 * single hash table in dynamic shared memory, then cooperatively emit the
 * finalized groups.  This removes the serial Finalize HashAggregate stage of
 * the conventional Partial -> Gather -> Finalize plan shape: no partial
 * states ever need to be merged, because every input row's transition call
 * mutates the one shared per-group state, under a per-bucket-stripe LWLock
 * (strategy 1 of the accompanying specification).
 *
 * v0 restrictions (documented, checked by the planner):
 *	- every aggregate's transition type must be pass-by-value (a by-ref
 *	  transition state allocated in one process's memory cannot be
 *	  dereferenced in another process);
 *	- no grouping sets, no DISTINCT/ORDER BY aggregates;
 *	- the table is sized once from the planner estimate and never grows;
 *	  underestimates only lengthen chains, they do not affect correctness;
 *	- no disk spilling: hash_mem is not enforced for the shared table;
 *	- rescan is not supported.
 *
 * The shared bucket array holds dsa_pointers to chains of SharedAggEntry.
 * All chain reads and mutations during the build phase happen under the
 * stripe LWLock covering the entry's bucket, so no atomics are required.
 * The emission phase begins only after every participant has arrived at the
 * build barrier, at which point the table is immutable and may be read
 * without locks.
 */

/* Offset added to plan_node_id for the shared-build shm_toc entry */
#define PARALLEL_KEY_AGG_SHARED_OFFSET	UINT64CONST(0xB000000000000000)

/*
 * Number of LWLock stripes protecting the bucket array during build, and
 * buckets claimed at a time by one participant during emission.  Both are
 * unbenchmarked initial guesses: 256 stripes keeps the probability of false
 * stripe sharing below ~4% for 8 concurrent participants, and 64-bucket
 * scan chunks amortize the atomic fetch-add without starving participants
 * at the tail.  Revisit with contention benchmarks.
 */
#define SHARED_AGG_NUM_STRIPES		256
#define SHARED_AGG_SCAN_CHUNK		64

/* Cap on the shared bucket array (16M buckets = 128MB of pointers) */
#define SHARED_AGG_MAX_BUCKETS		(UINT64CONST(1) << 24)

/*
 * Number of spill partitions (single level, no recursive repartitioning).
 * Partition selection uses the top hash bits, disjoint from the bucket
 * number's low bits.  Also an unbenchmarked initial guess.
 */
#define SHARED_AGG_SPILL_BITS		5
#define SHARED_AGG_SPILL_PARTITIONS (1 << SHARED_AGG_SPILL_BITS)
#define SharedAggSpillPartition(hash) \
	((hash) >> (32 - SHARED_AGG_SPILL_BITS))

/* Build-barrier phases */
#define PHA_BUILD_ELECT				0
#define PHA_BUILD_ALLOCATE			1
#define PHA_BUILD_RUN				2
#define PHA_BUILD_DONE				3

/* Sentinel for SharedAggBuildState.cur_batch: all batches processed */
#define PHA_BATCHES_DONE			PG_UINT32_MAX

/*
 * How transition states are maintained in the shared table.
 *
 * Pass-by-value states are ordinary AggStatePerGroupData whose transValue
 * is the value itself, mutated in place by advance_transition_function().
 *
 * Plain pass-by-reference states (varlena such as bytea/text/numeric, or
 * fixed-length by-ref such as interval) are stored as self-contained blobs
 * in the DSA area: the slot's transValue then holds a dsa_pointer rather
 * than a local pointer.  shared_agg_advance_byref() translates it to a
 * local address around each transition call and copies the result back
 * into DSA only when the transition function returns a different pointer;
 * a function that modifies its state in place (the common pattern for
 * custom bytea-state aggregates, sanctioned by AggCheckCallContext) is
 * thereby mutating the shared blob directly, under the stripe lock, with
 * no copy at all.  Values are detoasted and expanded datums flattened
 * before storing, so a blob is always plain contiguous bytes that any
 * participant may map.
 *
 * The 'internal' pseudo-type is deliberately unsupported: it is a
 * by-value pointer to an opaque struct with interior process-local
 * pointers, which no byte-copy can make shareable.  (A variant that kept
 * such states in serialized form, converting around every transition
 * call, was prototyped and removed: per-row serialize/deserialize under
 * the stripe lock costs more than it saves.  Aggregates wanting the
 * shared table should provide a flat transition representation instead —
 * see contrib/shared_numeric_agg.)  The planner never admits them; the
 * check in shared_agg_init_support() is a cross-check.
 */

/*
 * Executor-local per-transition support for the shared path: argument and
 * filter expressions (the shared path cannot use the compiled evaltrans
 * program, whose by-ref handling would store process-local pointers into
 * the shared table).
 */
typedef struct SharedAggTransInfo
{
	bool		blob_stored;	/* transValue holds a dsa_pointer */
	ExprState **argstates;		/* transition argument expressions */
	ExprState **argstates_spill;	/* ditto, compiled for minimal-tuple
									 * slots (spill batch reads) */
	int			numargs;
	ExprState  *filterstate;	/* FILTER clause, or NULL */
	ExprState  *filterstate_spill;	/* ditto, for spill batch reads */

	/*
	 * Per-row staging area, filled by shared_agg_prepare_inputs() BEFORE
	 * the stripe lock is taken, consumed by shared_agg_apply() under the
	 * lock.  Keeping expression evaluation and detoasting outside the lock
	 * is essential: the lock serializes 1/256th of the bucket space, and
	 * must protect only the state mutation.
	 */
	bool		pending_skip;	/* row filtered out */
	Datum	   *pending_args;	/* evaluated argument values */
	bool	   *pending_nulls;
} SharedAggTransInfo;

/*
 * One entry of the shared hash table.  Immediately followed (MAXALIGNed) by
 * an AggStatePerGroupData[numtrans] array and then by the group-key
 * MinimalTuple bytes.  For by-ref transition types the pergroup transValue
 * holds a dsa_pointer to the state blob, not a local pointer.
 */
typedef struct SharedAggEntry
{
	dsa_pointer next;			/* next entry in bucket chain */
	uint32		hash;			/* group key hash, same IV in all workers */
	uint32		tuplen;			/* length of key MinimalTuple */
} SharedAggEntry;

#define SharedAggEntryStates(entry) \
	((AggStatePerGroup) ((char *) (entry) + MAXALIGN(sizeof(SharedAggEntry))))
#define SharedAggEntryTuple(entry, numtrans) \
	((MinimalTuple) ((char *) (entry) + MAXALIGN(sizeof(SharedAggEntry)) + \
					 MAXALIGN((numtrans) * sizeof(AggStatePerGroupData))))

/*
 * Shared-memory coordination state, allocated in the fixed DSM segment
 * (shm_toc) by the leader; the variable-size table itself lives in the
 * per-query dsa_area.
 */
typedef struct SharedAggBuildState
{
	Barrier		build_barrier;	/* synchronizes build vs. emission */
	Barrier		scan_barrier;	/* synchronizes spill-batch cycles */
	int			nparticipants;	/* leader + planned workers */
	uint64		nbuckets;		/* size of shared bucket array (power of 2) */
	dsa_pointer buckets;		/* -> dsa_pointer[nbuckets], zero-initialized */
	pg_atomic_uint64 nentries;	/* number of groups created */
	pg_atomic_uint64 scan_cursor;	/* next bucket to claim for emission */

	/* spill support */
	uint64		mem_limit;		/* combined budget for the shared table */
	pg_atomic_uint64 mem_used;	/* DSA bytes allocated for the table */
	pg_atomic_uint32 spill_mode;	/* nonzero: stop creating new groups */
	pg_atomic_uint64 nspilled;	/* total tuples written to spill files */
	pg_atomic_uint64 npart_spilled[SHARED_AGG_SPILL_PARTITIONS];	/* per partition */

	/*
	 * cur_batch: 0 = in-memory phase, 1..NPARTITIONS = spill batches,
	 * PHA_BATCHES_DONE = all done.  Advanced under batch_lock by whichever
	 * participant gets there first after the emit-done barrier wait.  Do
	 * NOT derive that responsibility from BarrierArriveAndWait()'s return
	 * value: a detaching participant (the leader bowing out of batch
	 * processing) advances the barrier phase without any waiter being told
	 * it arrived last, so a barrier-return "election" can be lost entirely,
	 * leaving cur_batch stuck.
	 */
	uint32		cur_batch;
	LWLock		batch_lock;		/* protects cur_batch transitions and the
								 * entry chunk list */
	dsa_pointer chunk_head;		/* list of entry chunks, for wholesale free */

	SharedFileSet fileset;		/* backing files for spill partitions */

	/*
	 * Padded to a cache line each: adjacent unpadded LWLocks share lines,
	 * so participants contending on DIFFERENT stripes would still bounce
	 * the same cache line between cores (false sharing).
	 */
	LWLockPadded stripe_locks[SHARED_AGG_NUM_STRIPES];

	/*
	 * Followed in the same shm_toc chunk by SHARED_AGG_SPILL_PARTITIONS
	 * SharedTuplestores, each of size sts_estimate(nparticipants).
	 */
} SharedAggBuildState;

/*
 * A group's key hashes to exactly one bucket, and a bucket maps to exactly
 * one stripe: this is the entire duplicate-prevention argument.  Two
 * participants inserting the same new group necessarily contend on the same
 * stripe lock, so the second finds the first's entry.
 */
#define SharedAggStripeLock(shstate, bucketno) \
	(&(shstate)->stripe_locks[(bucketno) % SHARED_AGG_NUM_STRIPES].lock)

/*
 * Entries are carved out of per-participant chunks rather than allocated
 * individually: dsa_allocate takes area-global locks, and per-entry
 * allocation was measured as a contention point (the same reason Parallel
 * Hash Join uses HASH_CHUNK-based allocation).  A participant owns its
 * current chunk exclusively, so carving needs no locks; each new chunk is
 * pushed onto a shared list (under batch_lock, once per chunk) so that a
 * batch reset can free entry memory wholesale instead of per entry.
 * dsa_pointer arithmetic within one allocation is valid — nodeHash.c's
 * "chunk_shared + offset" pattern is the precedent.
 */
#define SHARED_AGG_CHUNK_SIZE	(32 * 1024)

typedef struct SharedAggChunk
{
	dsa_pointer next;			/* next chunk in shared list */
} SharedAggChunk;

#define SharedAggChunkHeaderSize	MAXALIGN(sizeof(SharedAggChunk))

#define SharedAggPartitionSts(shstate, i) \
	((SharedTuplestore *) ((char *) (shstate) + \
						   MAXALIGN(sizeof(SharedAggBuildState)) + \
						   (i) * MAXALIGN(sts_estimate((shstate)->nparticipants))))

static void select_current_set(AggState *aggstate, int setno, bool is_hash);
static void initialize_phase(AggState *aggstate, int newphase);
static TupleTableSlot *fetch_input_tuple(AggState *aggstate);
static void initialize_aggregates(AggState *aggstate,
								  AggStatePerGroup *pergroups,
								  int numReset);
static void advance_transition_function(AggState *aggstate,
										AggStatePerTrans pertrans,
										AggStatePerGroup pergroupstate);
static void advance_aggregates(AggState *aggstate);
static void process_ordered_aggregate_single(AggState *aggstate,
											 AggStatePerTrans pertrans,
											 AggStatePerGroup pergroupstate);
static void process_ordered_aggregate_multi(AggState *aggstate,
											AggStatePerTrans pertrans,
											AggStatePerGroup pergroupstate);
static void finalize_aggregate(AggState *aggstate,
							   AggStatePerAgg peragg,
							   AggStatePerGroup pergroupstate,
							   Datum *resultVal, bool *resultIsNull);
static void finalize_partialaggregate(AggState *aggstate,
									  AggStatePerAgg peragg,
									  AggStatePerGroup pergroupstate,
									  Datum *resultVal, bool *resultIsNull);
static inline void prepare_hash_slot(AggStatePerHash perhash,
									 TupleTableSlot *inputslot,
									 TupleTableSlot *hashslot);
static void prepare_projection_slot(AggState *aggstate,
									TupleTableSlot *slot,
									int currentSet);
static void finalize_aggregates(AggState *aggstate,
								AggStatePerAgg peraggs,
								AggStatePerGroup pergroup);
static TupleTableSlot *project_aggregates(AggState *aggstate);
static void find_cols(AggState *aggstate, Bitmapset **aggregated,
					  Bitmapset **unaggregated);
static bool find_cols_walker(Node *node, FindColsContext *context);
static void build_hash_tables(AggState *aggstate);
static void build_hash_table(AggState *aggstate, int setno, double nbuckets);
static void hashagg_recompile_expressions(AggState *aggstate, bool minslot,
										  bool nullcheck);
static void hash_create_memory(AggState *aggstate);
static double hash_choose_num_buckets(double hashentrysize,
									  double ngroups, Size memory);
static int	hash_choose_num_partitions(double input_groups,
									   double hashentrysize,
									   int used_bits,
									   int *log2_npartitions);
static void initialize_hash_entry(AggState *aggstate,
								  TupleHashTable hashtable,
								  TupleHashEntry entry);
static void lookup_hash_entries(AggState *aggstate);
static TupleTableSlot *agg_retrieve_direct(AggState *aggstate);
static void agg_fill_hash_table(AggState *aggstate);
static bool agg_refill_hash_table(AggState *aggstate);
static TupleTableSlot *agg_retrieve_hash_table(AggState *aggstate);
static TupleTableSlot *agg_retrieve_hash_table_in_memory(AggState *aggstate);
static void hash_agg_check_limits(AggState *aggstate);
static void hash_agg_enter_spill_mode(AggState *aggstate);
static void hash_agg_update_metrics(AggState *aggstate, bool from_tape,
									int npartitions);
static void hashagg_finish_initial_spills(AggState *aggstate);
static void hashagg_reset_spill_state(AggState *aggstate);
static HashAggBatch *hashagg_batch_new(LogicalTape *input_tape, int setno,
									   int64 input_tuples, double input_card,
									   int used_bits);
static MinimalTuple hashagg_batch_read(HashAggBatch *batch, uint32 *hashp);
static void hashagg_spill_init(HashAggSpill *spill, LogicalTapeSet *tapeset,
							   int used_bits, double input_groups,
							   double hashentrysize);
static Size hashagg_spill_tuple(AggState *aggstate, HashAggSpill *spill,
								TupleTableSlot *inputslot, uint32 hash);
static void hashagg_spill_finish(AggState *aggstate, HashAggSpill *spill,
								 int setno);
static void shared_agg_init_support(AggState *aggstate);
static void shared_agg_prepare_inputs(AggState *aggstate, bool from_spill);
static dsa_pointer shared_agg_store_varlena(AggState *aggstate, dsa_area *area,
											struct varlena *val);
static dsa_pointer shared_agg_store_datum(AggState *aggstate, dsa_area *area,
										  AggStatePerTrans pertrans, Datum value);
static void shared_agg_advance_byref(AggState *aggstate, dsa_area *area,
									 AggStatePerTrans pertrans,
									 SharedAggTransInfo *info,
									 AggStatePerGroup pergroupstate);
static void shared_agg_apply(AggState *aggstate, dsa_area *area,
							 AggStatePerGroup states);
static void shared_agg_insert(AggState *aggstate, dsa_area *area,
							  TupleTableSlot *hashslot, uint32 hash,
							  bool allow_spill);
static void shared_agg_reset_table(AggState *aggstate, dsa_area *area);
static void shared_agg_refill(AggState *aggstate, dsa_area *area, int batchno);
static void agg_fill_shared_hash_table(AggState *aggstate);
static TupleTableSlot *agg_retrieve_shared_hash_table(AggState *aggstate);
static Datum GetAggInitVal(Datum textInitVal, Oid transtype);
static void build_pertrans_for_aggref(AggStatePerTrans pertrans,
									  AggState *aggstate, EState *estate,
									  Aggref *aggref, Oid transfn_oid,
									  Oid aggtranstype, Oid aggserialfn,
									  Oid aggdeserialfn, Datum initValue,
									  bool initValueIsNull, Oid *inputTypes,
									  int numArguments);


/*
 * Select the current grouping set; affects current_set and
 * curaggcontext.
 */
static void
select_current_set(AggState *aggstate, int setno, bool is_hash)
{
	/*
	 * When changing this, also adapt ExecAggPlainTransByVal() and
	 * ExecAggPlainTransByRef().
	 */
	if (is_hash)
		aggstate->curaggcontext = aggstate->hashcontext;
	else
		aggstate->curaggcontext = aggstate->aggcontexts[setno];

	aggstate->current_set = setno;
}

/*
 * Switch to phase "newphase", which must either be 0 or 1 (to reset) or
 * current_phase + 1. Juggle the tuplesorts accordingly.
 *
 * Phase 0 is for hashing, which we currently handle last in the AGG_MIXED
 * case, so when entering phase 0, all we need to do is drop open sorts.
 */
static void
initialize_phase(AggState *aggstate, int newphase)
{
	Assert(newphase <= 1 || newphase == aggstate->current_phase + 1);

	/*
	 * Whatever the previous state, we're now done with whatever input
	 * tuplesort was in use.
	 */
	if (aggstate->sort_in)
	{
		tuplesort_end(aggstate->sort_in);
		aggstate->sort_in = NULL;
	}

	if (newphase <= 1)
	{
		/*
		 * Discard any existing output tuplesort.
		 */
		if (aggstate->sort_out)
		{
			tuplesort_end(aggstate->sort_out);
			aggstate->sort_out = NULL;
		}
	}
	else
	{
		/*
		 * The old output tuplesort becomes the new input one, and this is the
		 * right time to actually sort it.
		 */
		aggstate->sort_in = aggstate->sort_out;
		aggstate->sort_out = NULL;
		Assert(aggstate->sort_in);
		tuplesort_performsort(aggstate->sort_in);
	}

	/*
	 * If this isn't the last phase, we need to sort appropriately for the
	 * next phase in sequence.
	 */
	if (newphase > 0 && newphase < aggstate->numphases - 1)
	{
		Sort	   *sortnode = aggstate->phases[newphase + 1].sortnode;
		PlanState  *outerNode = outerPlanState(aggstate);
		TupleDesc	tupDesc = ExecGetResultType(outerNode);

		aggstate->sort_out = tuplesort_begin_heap(tupDesc,
												  sortnode->numCols,
												  sortnode->sortColIdx,
												  sortnode->sortOperators,
												  sortnode->collations,
												  sortnode->nullsFirst,
												  work_mem,
												  NULL, TUPLESORT_NONE);
	}

	aggstate->current_phase = newphase;
	aggstate->phase = &aggstate->phases[newphase];
}

/*
 * Fetch a tuple from either the outer plan (for phase 1) or from the sorter
 * populated by the previous phase.  Copy it to the sorter for the next phase
 * if any.
 *
 * Callers cannot rely on memory for tuple in returned slot remaining valid
 * past any subsequently fetched tuple.
 */
static TupleTableSlot *
fetch_input_tuple(AggState *aggstate)
{
	TupleTableSlot *slot;

	if (aggstate->sort_in)
	{
		/* make sure we check for interrupts in either path through here */
		CHECK_FOR_INTERRUPTS();
		if (!tuplesort_gettupleslot(aggstate->sort_in, true, false,
									aggstate->sort_slot, NULL))
			return NULL;
		slot = aggstate->sort_slot;
	}
	else
		slot = ExecProcNode(outerPlanState(aggstate));

	if (!TupIsNull(slot) && aggstate->sort_out)
		tuplesort_puttupleslot(aggstate->sort_out, slot);

	return slot;
}

/*
 * (Re)Initialize an individual aggregate.
 *
 * This function handles only one grouping set, already set in
 * aggstate->current_set.
 *
 * When called, CurrentMemoryContext should be the per-query context.
 */
static void
initialize_aggregate(AggState *aggstate, AggStatePerTrans pertrans,
					 AggStatePerGroup pergroupstate)
{
	/*
	 * Start a fresh sort operation for each DISTINCT/ORDER BY aggregate.
	 */
	if (pertrans->aggsortrequired)
	{
		/*
		 * In case of rescan, maybe there could be an uncompleted sort
		 * operation?  Clean it up if so.
		 */
		if (pertrans->sortstates[aggstate->current_set])
			tuplesort_end(pertrans->sortstates[aggstate->current_set]);


		/*
		 * We use a plain Datum sorter when there's a single input column;
		 * otherwise sort the full tuple.  (See comments for
		 * process_ordered_aggregate_single.)
		 */
		if (pertrans->numInputs == 1)
		{
			Form_pg_attribute attr = TupleDescAttr(pertrans->sortdesc, 0);

			pertrans->sortstates[aggstate->current_set] =
				tuplesort_begin_datum(attr->atttypid,
									  pertrans->sortOperators[0],
									  pertrans->sortCollations[0],
									  pertrans->sortNullsFirst[0],
									  work_mem, NULL, TUPLESORT_NONE);
		}
		else
			pertrans->sortstates[aggstate->current_set] =
				tuplesort_begin_heap(pertrans->sortdesc,
									 pertrans->numSortCols,
									 pertrans->sortColIdx,
									 pertrans->sortOperators,
									 pertrans->sortCollations,
									 pertrans->sortNullsFirst,
									 work_mem, NULL, TUPLESORT_NONE);
	}

	/*
	 * (Re)set transValue to the initial value.
	 *
	 * Note that when the initial value is pass-by-ref, we must copy it (into
	 * the aggcontext) since we will pfree the transValue later.
	 */
	if (pertrans->initValueIsNull)
		pergroupstate->transValue = pertrans->initValue;
	else
	{
		MemoryContext oldContext;

		oldContext = MemoryContextSwitchTo(aggstate->curaggcontext->ecxt_per_tuple_memory);
		pergroupstate->transValue = datumCopy(pertrans->initValue,
											  pertrans->transtypeByVal,
											  pertrans->transtypeLen);
		MemoryContextSwitchTo(oldContext);
	}
	pergroupstate->transValueIsNull = pertrans->initValueIsNull;

	/*
	 * If the initial value for the transition state doesn't exist in the
	 * pg_aggregate table then we will let the first non-NULL value returned
	 * from the outer procNode become the initial value. (This is useful for
	 * aggregates like max() and min().) The noTransValue flag signals that we
	 * still need to do this.
	 */
	pergroupstate->noTransValue = pertrans->initValueIsNull;
}

/*
 * Initialize all aggregate transition states for a new group of input values.
 *
 * If there are multiple grouping sets, we initialize only the first numReset
 * of them (the grouping sets are ordered so that the most specific one, which
 * is reset most often, is first). As a convenience, if numReset is 0, we
 * reinitialize all sets.
 *
 * NB: This cannot be used for hash aggregates, as for those the grouping set
 * number has to be specified from further up.
 *
 * When called, CurrentMemoryContext should be the per-query context.
 */
static void
initialize_aggregates(AggState *aggstate,
					  AggStatePerGroup *pergroups,
					  int numReset)
{
	int			transno;
	int			numGroupingSets = Max(aggstate->phase->numsets, 1);
	int			setno = 0;
	int			numTrans = aggstate->numtrans;
	AggStatePerTrans transstates = aggstate->pertrans;

	if (numReset == 0)
		numReset = numGroupingSets;

	for (setno = 0; setno < numReset; setno++)
	{
		AggStatePerGroup pergroup = pergroups[setno];

		select_current_set(aggstate, setno, false);

		for (transno = 0; transno < numTrans; transno++)
		{
			AggStatePerTrans pertrans = &transstates[transno];
			AggStatePerGroup pergroupstate = &pergroup[transno];

			initialize_aggregate(aggstate, pertrans, pergroupstate);
		}
	}
}

/*
 * Given new input value(s), advance the transition function of one aggregate
 * state within one grouping set only (already set in aggstate->current_set)
 *
 * The new values (and null flags) have been preloaded into argument positions
 * 1 and up in pertrans->transfn_fcinfo, so that we needn't copy them again to
 * pass to the transition function.  We also expect that the static fields of
 * the fcinfo are already initialized; that was done by ExecInitAgg().
 *
 * It doesn't matter which memory context this is called in.
 */
static void
advance_transition_function(AggState *aggstate,
							AggStatePerTrans pertrans,
							AggStatePerGroup pergroupstate)
{
	FunctionCallInfo fcinfo = pertrans->transfn_fcinfo;
	MemoryContext oldContext;
	Datum		newVal;

	if (pertrans->transfn.fn_strict)
	{
		/*
		 * For a strict transfn, nothing happens when there's a NULL input; we
		 * just keep the prior transValue.
		 */
		int			numTransInputs = pertrans->numTransInputs;
		int			i;

		for (i = 1; i <= numTransInputs; i++)
		{
			if (fcinfo->args[i].isnull)
				return;
		}
		if (pergroupstate->noTransValue)
		{
			/*
			 * transValue has not been initialized. This is the first non-NULL
			 * input value. We use it as the initial value for transValue. (We
			 * already checked that the agg's input type is binary-compatible
			 * with its transtype, so straight copy here is OK.)
			 *
			 * We must copy the datum into aggcontext if it is pass-by-ref. We
			 * do not need to pfree the old transValue, since it's NULL.
			 */
			oldContext = MemoryContextSwitchTo(aggstate->curaggcontext->ecxt_per_tuple_memory);
			pergroupstate->transValue = datumCopy(fcinfo->args[1].value,
												  pertrans->transtypeByVal,
												  pertrans->transtypeLen);
			pergroupstate->transValueIsNull = false;
			pergroupstate->noTransValue = false;
			MemoryContextSwitchTo(oldContext);
			return;
		}
		if (pergroupstate->transValueIsNull)
		{
			/*
			 * Don't call a strict function with NULL inputs.  Note it is
			 * possible to get here despite the above tests, if the transfn is
			 * strict *and* returned a NULL on a prior cycle. If that happens
			 * we will propagate the NULL all the way to the end.
			 */
			return;
		}
	}

	/* We run the transition functions in per-input-tuple memory context */
	oldContext = MemoryContextSwitchTo(aggstate->tmpcontext->ecxt_per_tuple_memory);

	/* set up aggstate->curpertrans for AggGetAggref() */
	aggstate->curpertrans = pertrans;

	/*
	 * OK to call the transition function
	 */
	fcinfo->args[0].value = pergroupstate->transValue;
	fcinfo->args[0].isnull = pergroupstate->transValueIsNull;
	fcinfo->isnull = false;		/* just in case transfn doesn't set it */

	newVal = FunctionCallInvoke(fcinfo);

	aggstate->curpertrans = NULL;

	/*
	 * If pass-by-ref datatype, must copy the new value into aggcontext and
	 * free the prior transValue.  But if transfn returned a pointer to its
	 * first input, we don't need to do anything.
	 *
	 * It's safe to compare newVal with pergroup->transValue without regard
	 * for either being NULL, because ExecAggCopyTransValue takes care to set
	 * transValue to 0 when NULL. Otherwise we could end up accidentally not
	 * reparenting, when the transValue has the same numerical value as
	 * newValue, despite being NULL.  This is a somewhat hot path, making it
	 * undesirable to instead solve this with another branch for the common
	 * case of the transition function returning its (modified) input
	 * argument.
	 */
	if (!pertrans->transtypeByVal &&
		DatumGetPointer(newVal) != DatumGetPointer(pergroupstate->transValue))
		newVal = ExecAggCopyTransValue(aggstate, pertrans,
									   newVal, fcinfo->isnull,
									   pergroupstate->transValue,
									   pergroupstate->transValueIsNull);

	pergroupstate->transValue = newVal;
	pergroupstate->transValueIsNull = fcinfo->isnull;

	MemoryContextSwitchTo(oldContext);
}

/*
 * Advance each aggregate transition state for one input tuple.  The input
 * tuple has been stored in tmpcontext->ecxt_outertuple, so that it is
 * accessible to ExecEvalExpr.
 *
 * We have two sets of transition states to handle: one for sorted aggregation
 * and one for hashed; we do them both here, to avoid multiple evaluation of
 * the inputs.
 *
 * When called, CurrentMemoryContext should be the per-query context.
 */
static void
advance_aggregates(AggState *aggstate)
{
	ExecEvalExprNoReturnSwitchContext(aggstate->phase->evaltrans,
									  aggstate->tmpcontext);
}

/*
 * Run the transition function for a DISTINCT or ORDER BY aggregate
 * with only one input.  This is called after we have completed
 * entering all the input values into the sort object.  We complete the
 * sort, read out the values in sorted order, and run the transition
 * function on each value (applying DISTINCT if appropriate).
 *
 * Note that the strictness of the transition function was checked when
 * entering the values into the sort, so we don't check it again here;
 * we just apply standard SQL DISTINCT logic.
 *
 * The one-input case is handled separately from the multi-input case
 * for performance reasons: for single by-value inputs, such as the
 * common case of count(distinct id), the tuplesort_getdatum code path
 * is around 300% faster.  (The speedup for by-reference types is less
 * but still noticeable.)
 *
 * This function handles only one grouping set (already set in
 * aggstate->current_set).
 *
 * When called, CurrentMemoryContext should be the per-query context.
 */
static void
process_ordered_aggregate_single(AggState *aggstate,
								 AggStatePerTrans pertrans,
								 AggStatePerGroup pergroupstate)
{
	Datum		oldVal = (Datum) 0;
	bool		oldIsNull = true;
	bool		haveOldVal = false;
	MemoryContext workcontext = aggstate->tmpcontext->ecxt_per_tuple_memory;
	MemoryContext oldContext;
	bool		isDistinct = (pertrans->numDistinctCols > 0);
	Datum		newAbbrevVal = (Datum) 0;
	Datum		oldAbbrevVal = (Datum) 0;
	FunctionCallInfo fcinfo = pertrans->transfn_fcinfo;
	Datum	   *newVal;
	bool	   *isNull;

	Assert(pertrans->numDistinctCols < 2);

	tuplesort_performsort(pertrans->sortstates[aggstate->current_set]);

	/* Load the column into argument 1 (arg 0 will be transition value) */
	newVal = &fcinfo->args[1].value;
	isNull = &fcinfo->args[1].isnull;

	/*
	 * Note: if input type is pass-by-ref, the datums returned by the sort are
	 * freshly palloc'd in the per-query context, so we must be careful to
	 * pfree them when they are no longer needed.
	 */

	while (tuplesort_getdatum(pertrans->sortstates[aggstate->current_set],
							  true, false, newVal, isNull, &newAbbrevVal))
	{
		/*
		 * Clear and select the working context for evaluation of the equality
		 * function and transition function.
		 */
		MemoryContextReset(workcontext);
		oldContext = MemoryContextSwitchTo(workcontext);

		/*
		 * If DISTINCT mode, and not distinct from prior, skip it.
		 */
		if (isDistinct &&
			haveOldVal &&
			((oldIsNull && *isNull) ||
			 (!oldIsNull && !*isNull &&
			  oldAbbrevVal == newAbbrevVal &&
			  DatumGetBool(FunctionCall2Coll(&pertrans->equalfnOne,
											 pertrans->aggCollation,
											 oldVal, *newVal)))))
		{
			MemoryContextSwitchTo(oldContext);
			continue;
		}
		else
		{
			advance_transition_function(aggstate, pertrans, pergroupstate);

			MemoryContextSwitchTo(oldContext);

			/*
			 * Forget the old value, if any, and remember the new one for
			 * subsequent equality checks.
			 */
			if (!pertrans->inputtypeByVal)
			{
				if (!oldIsNull)
					pfree(DatumGetPointer(oldVal));
				if (!*isNull)
					oldVal = datumCopy(*newVal, pertrans->inputtypeByVal,
									   pertrans->inputtypeLen);
			}
			else
				oldVal = *newVal;
			oldAbbrevVal = newAbbrevVal;
			oldIsNull = *isNull;
			haveOldVal = true;
		}
	}

	if (!oldIsNull && !pertrans->inputtypeByVal)
		pfree(DatumGetPointer(oldVal));

	tuplesort_end(pertrans->sortstates[aggstate->current_set]);
	pertrans->sortstates[aggstate->current_set] = NULL;
}

/*
 * Run the transition function for a DISTINCT or ORDER BY aggregate
 * with more than one input.  This is called after we have completed
 * entering all the input values into the sort object.  We complete the
 * sort, read out the values in sorted order, and run the transition
 * function on each value (applying DISTINCT if appropriate).
 *
 * This function handles only one grouping set (already set in
 * aggstate->current_set).
 *
 * When called, CurrentMemoryContext should be the per-query context.
 */
static void
process_ordered_aggregate_multi(AggState *aggstate,
								AggStatePerTrans pertrans,
								AggStatePerGroup pergroupstate)
{
	ExprContext *tmpcontext = aggstate->tmpcontext;
	FunctionCallInfo fcinfo = pertrans->transfn_fcinfo;
	TupleTableSlot *slot1 = pertrans->sortslot;
	TupleTableSlot *slot2 = pertrans->uniqslot;
	int			numTransInputs = pertrans->numTransInputs;
	int			numDistinctCols = pertrans->numDistinctCols;
	Datum		newAbbrevVal = (Datum) 0;
	Datum		oldAbbrevVal = (Datum) 0;
	bool		haveOldValue = false;
	TupleTableSlot *save = aggstate->tmpcontext->ecxt_outertuple;
	int			i;

	tuplesort_performsort(pertrans->sortstates[aggstate->current_set]);

	ExecClearTuple(slot1);
	if (slot2)
		ExecClearTuple(slot2);

	while (tuplesort_gettupleslot(pertrans->sortstates[aggstate->current_set],
								  true, true, slot1, &newAbbrevVal))
	{
		CHECK_FOR_INTERRUPTS();

		tmpcontext->ecxt_outertuple = slot1;
		tmpcontext->ecxt_innertuple = slot2;

		if (numDistinctCols == 0 ||
			!haveOldValue ||
			newAbbrevVal != oldAbbrevVal ||
			!ExecQual(pertrans->equalfnMulti, tmpcontext))
		{
			/*
			 * Extract the first numTransInputs columns as datums to pass to
			 * the transfn.
			 */
			slot_getsomeattrs(slot1, numTransInputs);

			/* Load values into fcinfo */
			/* Start from 1, since the 0th arg will be the transition value */
			for (i = 0; i < numTransInputs; i++)
			{
				fcinfo->args[i + 1].value = slot1->tts_values[i];
				fcinfo->args[i + 1].isnull = slot1->tts_isnull[i];
			}

			advance_transition_function(aggstate, pertrans, pergroupstate);

			if (numDistinctCols > 0)
			{
				/* swap the slot pointers to retain the current tuple */
				TupleTableSlot *tmpslot = slot2;

				slot2 = slot1;
				slot1 = tmpslot;
				/* avoid ExecQual() calls by reusing abbreviated keys */
				oldAbbrevVal = newAbbrevVal;
				haveOldValue = true;
			}
		}

		/* Reset context each time */
		ResetExprContext(tmpcontext);

		ExecClearTuple(slot1);
	}

	if (slot2)
		ExecClearTuple(slot2);

	tuplesort_end(pertrans->sortstates[aggstate->current_set]);
	pertrans->sortstates[aggstate->current_set] = NULL;

	/* restore previous slot, potentially in use for grouping sets */
	tmpcontext->ecxt_outertuple = save;
}

/*
 * Compute the final value of one aggregate.
 *
 * This function handles only one grouping set (already set in
 * aggstate->current_set).
 *
 * The finalfn will be run, and the result delivered, in the
 * output-tuple context; caller's CurrentMemoryContext does not matter.
 * (But note that in some cases, such as when there is no finalfn, the
 * result might be a pointer to or into the agg's transition value.)
 *
 * The finalfn uses the state as set in the transno.  This also might be
 * being used by another aggregate function, so it's important that we do
 * nothing destructive here.  Moreover, the aggregate's final value might
 * get used in multiple places, so we mustn't return a R/W expanded datum.
 */
static void
finalize_aggregate(AggState *aggstate,
				   AggStatePerAgg peragg,
				   AggStatePerGroup pergroupstate,
				   Datum *resultVal, bool *resultIsNull)
{
	LOCAL_FCINFO(fcinfo, FUNC_MAX_ARGS);
	bool		anynull = false;
	MemoryContext oldContext;
	int			i;
	ListCell   *lc;
	AggStatePerTrans pertrans = &aggstate->pertrans[peragg->transno];

	oldContext = MemoryContextSwitchTo(aggstate->ss.ps.ps_ExprContext->ecxt_per_tuple_memory);

	/*
	 * Evaluate any direct arguments.  We do this even if there's no finalfn
	 * (which is unlikely anyway), so that side-effects happen as expected.
	 * The direct arguments go into arg positions 1 and up, leaving position 0
	 * for the transition state value.
	 */
	i = 1;
	foreach(lc, peragg->aggdirectargs)
	{
		ExprState  *expr = (ExprState *) lfirst(lc);

		fcinfo->args[i].value = ExecEvalExpr(expr,
											 aggstate->ss.ps.ps_ExprContext,
											 &fcinfo->args[i].isnull);
		anynull |= fcinfo->args[i].isnull;
		i++;
	}

	/*
	 * Apply the agg's finalfn if one is provided, else return transValue.
	 */
	if (OidIsValid(peragg->finalfn_oid))
	{
		int			numFinalArgs = peragg->numFinalArgs;

		/* set up aggstate->curperagg for AggGetAggref() */
		aggstate->curperagg = peragg;

		InitFunctionCallInfoData(*fcinfo, &peragg->finalfn,
								 numFinalArgs,
								 pertrans->aggCollation,
								 (Node *) aggstate, NULL);

		/* Fill in the transition state value */
		fcinfo->args[0].value =
			MakeExpandedObjectReadOnly(pergroupstate->transValue,
									   pergroupstate->transValueIsNull,
									   pertrans->transtypeLen);
		fcinfo->args[0].isnull = pergroupstate->transValueIsNull;
		anynull |= pergroupstate->transValueIsNull;

		/* Fill any remaining argument positions with nulls */
		for (; i < numFinalArgs; i++)
		{
			fcinfo->args[i].value = (Datum) 0;
			fcinfo->args[i].isnull = true;
			anynull = true;
		}

		if (fcinfo->flinfo->fn_strict && anynull)
		{
			/* don't call a strict function with NULL inputs */
			*resultVal = (Datum) 0;
			*resultIsNull = true;
		}
		else
		{
			Datum		result;

			result = FunctionCallInvoke(fcinfo);
			*resultIsNull = fcinfo->isnull;
			*resultVal = MakeExpandedObjectReadOnly(result,
													fcinfo->isnull,
													peragg->resulttypeLen);
		}
		aggstate->curperagg = NULL;
	}
	else
	{
		*resultVal =
			MakeExpandedObjectReadOnly(pergroupstate->transValue,
									   pergroupstate->transValueIsNull,
									   pertrans->transtypeLen);
		*resultIsNull = pergroupstate->transValueIsNull;
	}

	MemoryContextSwitchTo(oldContext);
}

/*
 * Compute the output value of one partial aggregate.
 *
 * The serialization function will be run, and the result delivered, in the
 * output-tuple context; caller's CurrentMemoryContext does not matter.
 */
static void
finalize_partialaggregate(AggState *aggstate,
						  AggStatePerAgg peragg,
						  AggStatePerGroup pergroupstate,
						  Datum *resultVal, bool *resultIsNull)
{
	AggStatePerTrans pertrans = &aggstate->pertrans[peragg->transno];
	MemoryContext oldContext;

	oldContext = MemoryContextSwitchTo(aggstate->ss.ps.ps_ExprContext->ecxt_per_tuple_memory);

	/*
	 * serialfn_oid will be set if we must serialize the transvalue before
	 * returning it
	 */
	if (OidIsValid(pertrans->serialfn_oid))
	{
		/* Don't call a strict serialization function with NULL input. */
		if (pertrans->serialfn.fn_strict && pergroupstate->transValueIsNull)
		{
			*resultVal = (Datum) 0;
			*resultIsNull = true;
		}
		else
		{
			FunctionCallInfo fcinfo = pertrans->serialfn_fcinfo;
			Datum		result;

			fcinfo->args[0].value =
				MakeExpandedObjectReadOnly(pergroupstate->transValue,
										   pergroupstate->transValueIsNull,
										   pertrans->transtypeLen);
			fcinfo->args[0].isnull = pergroupstate->transValueIsNull;
			fcinfo->isnull = false;

			result = FunctionCallInvoke(fcinfo);
			*resultIsNull = fcinfo->isnull;
			*resultVal = MakeExpandedObjectReadOnly(result,
													fcinfo->isnull,
													peragg->resulttypeLen);
		}
	}
	else
	{
		*resultVal =
			MakeExpandedObjectReadOnly(pergroupstate->transValue,
									   pergroupstate->transValueIsNull,
									   pertrans->transtypeLen);
		*resultIsNull = pergroupstate->transValueIsNull;
	}

	MemoryContextSwitchTo(oldContext);
}

/*
 * Extract the attributes that make up the grouping key into the
 * hashslot. This is necessary to compute the hash or perform a lookup.
 */
static inline void
prepare_hash_slot(AggStatePerHash perhash,
				  TupleTableSlot *inputslot,
				  TupleTableSlot *hashslot)
{
	int			i;

	/* transfer just the needed columns into hashslot */
	slot_getsomeattrs(inputslot, perhash->largestGrpColIdx);
	ExecClearTuple(hashslot);

	for (i = 0; i < perhash->numhashGrpCols; i++)
	{
		int			varNumber = perhash->hashGrpColIdxInput[i] - 1;

		hashslot->tts_values[i] = inputslot->tts_values[varNumber];
		hashslot->tts_isnull[i] = inputslot->tts_isnull[varNumber];
	}
	ExecStoreVirtualTuple(hashslot);
}

/*
 * Prepare to finalize and project based on the specified representative tuple
 * slot and grouping set.
 *
 * In the specified tuple slot, force to null all attributes that should be
 * read as null in the context of the current grouping set.  Also stash the
 * current group bitmap where GroupingExpr can get at it.
 *
 * This relies on three conditions:
 *
 * 1) Nothing is ever going to try and extract the whole tuple from this slot,
 * only reference it in evaluations, which will only access individual
 * attributes.
 *
 * 2) No system columns are going to need to be nulled. (If a system column is
 * referenced in a group clause, it is actually projected in the outer plan
 * tlist.)
 *
 * 3) Within a given phase, we never need to recover the value of an attribute
 * once it has been set to null.
 *
 * Poking into the slot this way is a bit ugly, but the consensus is that the
 * alternative was worse.
 */
static void
prepare_projection_slot(AggState *aggstate, TupleTableSlot *slot, int currentSet)
{
	if (aggstate->phase->grouped_cols)
	{
		Bitmapset  *grouped_cols = aggstate->phase->grouped_cols[currentSet];

		aggstate->grouped_cols = grouped_cols;

		if (TTS_EMPTY(slot))
		{
			/*
			 * Force all values to be NULL if working on an empty input tuple
			 * (i.e. an empty grouping set for which no input rows were
			 * supplied).
			 */
			ExecStoreAllNullTuple(slot);
		}
		else if (aggstate->all_grouped_cols)
		{
			ListCell   *lc;

			/* all_grouped_cols is arranged in desc order */
			slot_getsomeattrs(slot, linitial_int(aggstate->all_grouped_cols));

			foreach(lc, aggstate->all_grouped_cols)
			{
				int			attnum = lfirst_int(lc);

				if (!bms_is_member(attnum, grouped_cols))
					slot->tts_isnull[attnum - 1] = true;
			}
		}
	}
}

/*
 * Compute the final value of all aggregates for one group.
 *
 * This function handles only one grouping set at a time, which the caller must
 * have selected.  It's also the caller's responsibility to adjust the supplied
 * pergroup parameter to point to the current set's transvalues.
 *
 * Results are stored in the output econtext aggvalues/aggnulls.
 */
static void
finalize_aggregates(AggState *aggstate,
					AggStatePerAgg peraggs,
					AggStatePerGroup pergroup)
{
	ExprContext *econtext = aggstate->ss.ps.ps_ExprContext;
	Datum	   *aggvalues = econtext->ecxt_aggvalues;
	bool	   *aggnulls = econtext->ecxt_aggnulls;
	int			aggno;

	/*
	 * If there were any DISTINCT and/or ORDER BY aggregates, sort their
	 * inputs and run the transition functions.
	 */
	for (int transno = 0; transno < aggstate->numtrans; transno++)
	{
		AggStatePerTrans pertrans = &aggstate->pertrans[transno];
		AggStatePerGroup pergroupstate;

		pergroupstate = &pergroup[transno];

		if (pertrans->aggsortrequired)
		{
			Assert(aggstate->aggstrategy != AGG_HASHED &&
				   aggstate->aggstrategy != AGG_MIXED);

			if (pertrans->numInputs == 1)
				process_ordered_aggregate_single(aggstate,
												 pertrans,
												 pergroupstate);
			else
				process_ordered_aggregate_multi(aggstate,
												pertrans,
												pergroupstate);
		}
		else if (pertrans->numDistinctCols > 0 && pertrans->haslast)
		{
			pertrans->haslast = false;

			if (pertrans->numDistinctCols == 1)
			{
				if (!pertrans->inputtypeByVal && !pertrans->lastisnull)
					pfree(DatumGetPointer(pertrans->lastdatum));

				pertrans->lastisnull = false;
				pertrans->lastdatum = (Datum) 0;
			}
			else
				ExecClearTuple(pertrans->uniqslot);
		}
	}

	/*
	 * Run the final functions.
	 */
	for (aggno = 0; aggno < aggstate->numaggs; aggno++)
	{
		AggStatePerAgg peragg = &peraggs[aggno];
		int			transno = peragg->transno;
		AggStatePerGroup pergroupstate;

		pergroupstate = &pergroup[transno];

		if (DO_AGGSPLIT_SKIPFINAL(aggstate->aggsplit))
			finalize_partialaggregate(aggstate, peragg, pergroupstate,
									  &aggvalues[aggno], &aggnulls[aggno]);
		else
			finalize_aggregate(aggstate, peragg, pergroupstate,
							   &aggvalues[aggno], &aggnulls[aggno]);
	}
}

/*
 * Project the result of a group (whose aggs have already been calculated by
 * finalize_aggregates). Returns the result slot, or NULL if no row is
 * projected (suppressed by qual).
 */
static TupleTableSlot *
project_aggregates(AggState *aggstate)
{
	ExprContext *econtext = aggstate->ss.ps.ps_ExprContext;

	/*
	 * Check the qual (HAVING clause); if the group does not match, ignore it.
	 */
	if (ExecQual(aggstate->ss.ps.qual, econtext))
	{
		/*
		 * Form and return projection tuple using the aggregate results and
		 * the representative input tuple.
		 */
		return ExecProject(aggstate->ss.ps.ps_ProjInfo);
	}
	else
		InstrCountFiltered1(aggstate, 1);

	return NULL;
}

/*
 * Find input-tuple columns that are needed, dividing them into
 * aggregated and unaggregated sets.
 */
static void
find_cols(AggState *aggstate, Bitmapset **aggregated, Bitmapset **unaggregated)
{
	Agg		   *agg = (Agg *) aggstate->ss.ps.plan;
	FindColsContext context;

	context.is_aggref = false;
	context.aggregated = NULL;
	context.unaggregated = NULL;

	/* Examine tlist and quals */
	(void) find_cols_walker((Node *) agg->plan.targetlist, &context);
	(void) find_cols_walker((Node *) agg->plan.qual, &context);

	/* In some cases, grouping columns will not appear in the tlist */
	for (int i = 0; i < agg->numCols; i++)
		context.unaggregated = bms_add_member(context.unaggregated,
											  agg->grpColIdx[i]);

	*aggregated = context.aggregated;
	*unaggregated = context.unaggregated;
}

static bool
find_cols_walker(Node *node, FindColsContext *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Var))
	{
		Var		   *var = (Var *) node;

		/* setrefs.c should have set the varno to OUTER_VAR */
		Assert(var->varno == OUTER_VAR);
		Assert(var->varlevelsup == 0);
		if (context->is_aggref)
			context->aggregated = bms_add_member(context->aggregated,
												 var->varattno);
		else
			context->unaggregated = bms_add_member(context->unaggregated,
												   var->varattno);
		return false;
	}
	if (IsA(node, Aggref))
	{
		Assert(!context->is_aggref);
		context->is_aggref = true;
		expression_tree_walker(node, find_cols_walker, context);
		context->is_aggref = false;
		return false;
	}
	return expression_tree_walker(node, find_cols_walker, context);
}

/*
 * (Re-)initialize the hash table(s) to empty.
 *
 * To implement hashed aggregation, we need a hashtable that stores a
 * representative tuple and an array of AggStatePerGroup structs for each
 * distinct set of GROUP BY column values.  We compute the hash key from the
 * GROUP BY columns.  The per-group data is allocated in initialize_hash_entry(),
 * for each entry.
 *
 * We have a separate hashtable and associated perhash data structure for each
 * grouping set for which we're doing hashing.
 *
 * The contents of the hash tables live in the aggstate's hash_tuplescxt
 * memory context (there is only one of these for all tables together, since
 * they are all reset at the same time).
 */
static void
build_hash_tables(AggState *aggstate)
{
	int			setno;

	for (setno = 0; setno < aggstate->num_hashes; ++setno)
	{
		AggStatePerHash perhash = &aggstate->perhash[setno];
		double		nbuckets;
		Size		memory;

		if (perhash->hashtable != NULL)
		{
			ResetTupleHashTable(perhash->hashtable);
			continue;
		}

		memory = aggstate->hash_mem_limit / aggstate->num_hashes;

		/* choose reasonable number of buckets per hashtable */
		nbuckets = hash_choose_num_buckets(aggstate->hashentrysize,
										   perhash->aggnode->numGroups,
										   memory);

#ifdef USE_INJECTION_POINTS
		if (IS_INJECTION_POINT_ATTACHED("hash-aggregate-oversize-table"))
		{
			nbuckets = memory / TupleHashEntrySize();
			INJECTION_POINT_CACHED("hash-aggregate-oversize-table", NULL);
		}
#endif

		build_hash_table(aggstate, setno, nbuckets);
	}

	aggstate->hash_ngroups_current = 0;
}

/*
 * Build a single hashtable for this grouping set.
 */
static void
build_hash_table(AggState *aggstate, int setno, double nbuckets)
{
	AggStatePerHash perhash = &aggstate->perhash[setno];
	MemoryContext metacxt = aggstate->hash_metacxt;
	MemoryContext tuplescxt = aggstate->hash_tuplescxt;
	MemoryContext tmpcxt = aggstate->tmpcontext->ecxt_per_tuple_memory;
	Size		additionalsize;

	Assert(aggstate->aggstrategy == AGG_HASHED ||
		   aggstate->aggstrategy == AGG_MIXED);

	/*
	 * Used to make sure initial hash table allocation does not exceed
	 * hash_mem. Note that the estimate does not include space for
	 * pass-by-reference transition data values, nor for the representative
	 * tuple of each group.
	 */
	additionalsize = aggstate->numtrans * sizeof(AggStatePerGroupData);

	perhash->hashtable = BuildTupleHashTable(&aggstate->ss.ps,
											 perhash->hashslot->tts_tupleDescriptor,
											 perhash->hashslot->tts_ops,
											 perhash->numCols,
											 perhash->hashGrpColIdxHash,
											 perhash->eqfuncoids,
											 perhash->hashfunctions,
											 perhash->aggnode->grpCollations,
											 nbuckets,
											 additionalsize,
											 metacxt,
											 tuplescxt,
											 tmpcxt,
											 DO_AGGSPLIT_SKIPFINAL(aggstate->aggsplit));
}

/*
 * Compute columns that actually need to be stored in hashtable entries.  The
 * incoming tuples from the child plan node will contain grouping columns,
 * other columns referenced in our targetlist and qual, columns used to
 * compute the aggregate functions, and perhaps just junk columns we don't use
 * at all.  Only columns of the first two types need to be stored in the
 * hashtable, and getting rid of the others can make the table entries
 * significantly smaller.  The hashtable only contains the relevant columns,
 * and is packed/unpacked in lookup_hash_entries() / agg_retrieve_hash_table()
 * into the format of the normal input descriptor.
 *
 * Additional columns, in addition to the columns grouped by, come from two
 * sources: Firstly functionally dependent columns that we don't need to group
 * by themselves, and secondly ctids for row-marks.
 *
 * To eliminate duplicates, we build a bitmapset of the needed columns, and
 * then build an array of the columns included in the hashtable. We might
 * still have duplicates if the passed-in grpColIdx has them, which can happen
 * in edge cases from semijoins/distinct; these can't always be removed,
 * because it's not certain that the duplicate cols will be using the same
 * hash function.
 *
 * Note that the array is preserved over ExecReScanAgg, so we allocate it in
 * the per-query context (unlike the hash table itself).
 */
static void
find_hash_columns(AggState *aggstate)
{
	Bitmapset  *base_colnos;
	Bitmapset  *aggregated_colnos;
	TupleDesc	scanDesc = aggstate->ss.ss_ScanTupleSlot->tts_tupleDescriptor;
	List	   *outerTlist = outerPlanState(aggstate)->plan->targetlist;
	int			numHashes = aggstate->num_hashes;
	EState	   *estate = aggstate->ss.ps.state;
	int			j;

	/* Find Vars that will be needed in tlist and qual */
	find_cols(aggstate, &aggregated_colnos, &base_colnos);
	aggstate->colnos_needed = bms_union(base_colnos, aggregated_colnos);
	aggstate->max_colno_needed = 0;
	aggstate->all_cols_needed = true;

	for (int i = 0; i < scanDesc->natts; i++)
	{
		int			colno = i + 1;

		if (bms_is_member(colno, aggstate->colnos_needed))
			aggstate->max_colno_needed = colno;
		else
			aggstate->all_cols_needed = false;
	}

	for (j = 0; j < numHashes; ++j)
	{
		AggStatePerHash perhash = &aggstate->perhash[j];
		Bitmapset  *colnos = bms_copy(base_colnos);
		AttrNumber *grpColIdx = perhash->aggnode->grpColIdx;
		List	   *hashTlist = NIL;
		TupleDesc	hashDesc;
		int			maxCols;
		int			i;

		perhash->largestGrpColIdx = 0;

		/*
		 * If we're doing grouping sets, then some Vars might be referenced in
		 * tlist/qual for the benefit of other grouping sets, but not needed
		 * when hashing; i.e. prepare_projection_slot will null them out, so
		 * there'd be no point storing them.  Use prepare_projection_slot's
		 * logic to determine which.
		 */
		if (aggstate->phases[0].grouped_cols)
		{
			Bitmapset  *grouped_cols = aggstate->phases[0].grouped_cols[j];
			ListCell   *lc;

			foreach(lc, aggstate->all_grouped_cols)
			{
				int			attnum = lfirst_int(lc);

				if (!bms_is_member(attnum, grouped_cols))
					colnos = bms_del_member(colnos, attnum);
			}
		}

		/*
		 * Compute maximum number of input columns accounting for possible
		 * duplications in the grpColIdx array, which can happen in some edge
		 * cases where HashAggregate was generated as part of a semijoin or a
		 * DISTINCT.
		 */
		maxCols = bms_num_members(colnos) + perhash->numCols;

		perhash->hashGrpColIdxInput =
			palloc(maxCols * sizeof(AttrNumber));
		perhash->hashGrpColIdxHash =
			palloc(perhash->numCols * sizeof(AttrNumber));

		/* Add all the grouping columns to colnos */
		for (i = 0; i < perhash->numCols; i++)
			colnos = bms_add_member(colnos, grpColIdx[i]);

		/*
		 * First build mapping for columns directly hashed. These are the
		 * first, because they'll be accessed when computing hash values and
		 * comparing tuples for exact matches. We also build simple mapping
		 * for execGrouping, so it knows where to find the to-be-hashed /
		 * compared columns in the input.
		 */
		for (i = 0; i < perhash->numCols; i++)
		{
			perhash->hashGrpColIdxInput[i] = grpColIdx[i];
			perhash->hashGrpColIdxHash[i] = i + 1;
			perhash->numhashGrpCols++;
			/* delete already mapped columns */
			colnos = bms_del_member(colnos, grpColIdx[i]);
		}

		/* and add the remaining columns */
		i = -1;
		while ((i = bms_next_member(colnos, i)) >= 0)
		{
			perhash->hashGrpColIdxInput[perhash->numhashGrpCols] = i;
			perhash->numhashGrpCols++;
		}

		/* and build a tuple descriptor for the hashtable */
		for (i = 0; i < perhash->numhashGrpCols; i++)
		{
			int			varNumber = perhash->hashGrpColIdxInput[i] - 1;

			hashTlist = lappend(hashTlist, list_nth(outerTlist, varNumber));
			perhash->largestGrpColIdx =
				Max(varNumber + 1, perhash->largestGrpColIdx);
		}

		hashDesc = ExecTypeFromTL(hashTlist);

		execTuplesHashPrepare(perhash->numCols,
							  perhash->aggnode->grpOperators,
							  &perhash->eqfuncoids,
							  &perhash->hashfunctions);
		perhash->hashslot =
			ExecAllocTableSlot(&estate->es_tupleTable, hashDesc,
							   &TTSOpsMinimalTuple, 0);

		list_free(hashTlist);
		bms_free(colnos);
	}

	bms_free(base_colnos);
}

/*
 * Estimate per-hash-table-entry overhead.
 */
Size
hash_agg_entry_size(int numTrans, Size tupleWidth, Size transitionSpace)
{
	Size		tupleChunkSize;
	Size		pergroupChunkSize;
	Size		transitionChunkSize;
	Size		tupleSize = (MAXALIGN(SizeofMinimalTupleHeader) +
							 tupleWidth);
	Size		pergroupSize = numTrans * sizeof(AggStatePerGroupData);

	/*
	 * Entries use the Bump allocator, so the chunk sizes are the same as the
	 * requested sizes.
	 */
	tupleChunkSize = MAXALIGN(tupleSize);
	pergroupChunkSize = pergroupSize;

	/*
	 * Transition values use AllocSet, which has a chunk header and also uses
	 * power-of-two allocations.
	 */
	if (transitionSpace > 0)
		transitionChunkSize = CHUNKHDRSZ + pg_nextpower2_size_t(transitionSpace);
	else
		transitionChunkSize = 0;

	return
		TupleHashEntrySize() +
		tupleChunkSize +
		pergroupChunkSize +
		transitionChunkSize;
}

/*
 * hashagg_recompile_expressions()
 *
 * Identifies the right phase, compiles the right expression given the
 * arguments, and then sets phase->evalfunc to that expression.
 *
 * Different versions of the compiled expression are needed depending on
 * whether hash aggregation has spilled or not, and whether it's reading from
 * the outer plan or a tape. Before spilling to disk, the expression reads
 * from the outer plan and does not need to perform a NULL check. After
 * HashAgg begins to spill, new groups will not be created in the hash table,
 * and the AggStatePerGroup array may be NULL; therefore we need to add a null
 * pointer check to the expression. Then, when reading spilled data from a
 * tape, we change the outer slot type to be a fixed minimal tuple slot.
 *
 * It would be wasteful to recompile every time, so cache the compiled
 * expressions in the AggStatePerPhase, and reuse when appropriate.
 */
static void
hashagg_recompile_expressions(AggState *aggstate, bool minslot, bool nullcheck)
{
	AggStatePerPhase phase;
	int			i = minslot ? 1 : 0;
	int			j = nullcheck ? 1 : 0;

	Assert(aggstate->aggstrategy == AGG_HASHED ||
		   aggstate->aggstrategy == AGG_MIXED);

	if (aggstate->aggstrategy == AGG_HASHED)
		phase = &aggstate->phases[0];
	else						/* AGG_MIXED */
		phase = &aggstate->phases[1];

	if (phase->evaltrans_cache[i][j] == NULL)
	{
		const TupleTableSlotOps *outerops = aggstate->ss.ps.outerops;
		bool		outerfixed = aggstate->ss.ps.outeropsfixed;
		bool		dohash = true;
		bool		dosort = false;

		/*
		 * If minslot is true, that means we are processing a spilled batch
		 * (inside agg_refill_hash_table()), and we must not advance the
		 * sorted grouping sets.
		 */
		if (aggstate->aggstrategy == AGG_MIXED && !minslot)
			dosort = true;

		/* temporarily change the outerops while compiling the expression */
		if (minslot)
		{
			aggstate->ss.ps.outerops = &TTSOpsMinimalTuple;
			aggstate->ss.ps.outeropsfixed = true;
		}

		phase->evaltrans_cache[i][j] = ExecBuildAggTrans(aggstate, phase,
														 dosort, dohash,
														 nullcheck);

		/* change back */
		aggstate->ss.ps.outerops = outerops;
		aggstate->ss.ps.outeropsfixed = outerfixed;
	}

	phase->evaltrans = phase->evaltrans_cache[i][j];
}

/*
 * Set limits that trigger spilling to avoid exceeding hash_mem. Consider the
 * number of partitions we expect to create (if we do spill).
 *
 * There are two limits: a memory limit, and also an ngroups limit. The
 * ngroups limit becomes important when we expect transition values to grow
 * substantially larger than the initial value.
 */
void
hash_agg_set_limits(double hashentrysize, double input_groups, int used_bits,
					Size *mem_limit, uint64 *ngroups_limit,
					int *num_partitions)
{
	int			npartitions;
	Size		partition_mem;
	Size		hash_mem_limit = get_hash_memory_limit();

	/* if not expected to spill, use all of hash_mem */
	if (input_groups * hashentrysize <= hash_mem_limit)
	{
		if (num_partitions != NULL)
			*num_partitions = 0;
		*mem_limit = hash_mem_limit;
		*ngroups_limit = hash_mem_limit / hashentrysize;
		return;
	}

	/*
	 * Calculate expected memory requirements for spilling, which is the size
	 * of the buffers needed for all the tapes that need to be open at once.
	 * Then, subtract that from the memory available for holding hash tables.
	 */
	npartitions = hash_choose_num_partitions(input_groups,
											 hashentrysize,
											 used_bits,
											 NULL);
	if (num_partitions != NULL)
		*num_partitions = npartitions;

	partition_mem =
		HASHAGG_READ_BUFFER_SIZE +
		HASHAGG_WRITE_BUFFER_SIZE * npartitions;

	/*
	 * Don't set the limit below 3/4 of hash_mem. In that case, we are at the
	 * minimum number of partitions, so we aren't going to dramatically exceed
	 * work mem anyway.
	 */
	if (hash_mem_limit > 4 * partition_mem)
		*mem_limit = hash_mem_limit - partition_mem;
	else
		*mem_limit = hash_mem_limit * 0.75;

	if (*mem_limit > hashentrysize)
		*ngroups_limit = *mem_limit / hashentrysize;
	else
		*ngroups_limit = 1;
}

/*
 * hash_agg_check_limits
 *
 * After adding a new group to the hash table, check whether we need to enter
 * spill mode. Allocations may happen without adding new groups (for instance,
 * if the transition state size grows), so this check is imperfect.
 */
static void
hash_agg_check_limits(AggState *aggstate)
{
	uint64		ngroups = aggstate->hash_ngroups_current;
	Size		meta_mem = MemoryContextMemAllocated(aggstate->hash_metacxt,
													 true);
	Size		entry_mem = MemoryContextMemAllocated(aggstate->hash_tuplescxt,
													  true);
	Size		tval_mem = MemoryContextMemAllocated(aggstate->hashcontext->ecxt_per_tuple_memory,
													 true);
	Size		total_mem = meta_mem + entry_mem + tval_mem;
	bool		do_spill = false;

#ifdef USE_INJECTION_POINTS
	if (ngroups >= 1000)
	{
		if (IS_INJECTION_POINT_ATTACHED("hash-aggregate-spill-1000"))
		{
			do_spill = true;
			INJECTION_POINT_CACHED("hash-aggregate-spill-1000", NULL);
		}
	}
#endif

	/*
	 * Don't spill unless there's at least one group in the hash table so we
	 * can be sure to make progress even in edge cases.
	 */
	if (aggstate->hash_ngroups_current > 0 &&
		(total_mem > aggstate->hash_mem_limit ||
		 ngroups > aggstate->hash_ngroups_limit))
	{
		do_spill = true;
	}

	if (do_spill)
		hash_agg_enter_spill_mode(aggstate);
}

/*
 * Enter "spill mode", meaning that no new groups are added to any of the hash
 * tables. Tuples that would create a new group are instead spilled, and
 * processed later.
 */
static void
hash_agg_enter_spill_mode(AggState *aggstate)
{
	INJECTION_POINT("hash-aggregate-enter-spill-mode", NULL);
	aggstate->hash_spill_mode = true;
	hashagg_recompile_expressions(aggstate, aggstate->table_filled, true);

	if (!aggstate->hash_ever_spilled)
	{
		Assert(aggstate->hash_tapeset == NULL);
		Assert(aggstate->hash_spills == NULL);

		aggstate->hash_ever_spilled = true;

		aggstate->hash_tapeset = LogicalTapeSetCreate(true, NULL, -1);

		aggstate->hash_spills = palloc_array(HashAggSpill, aggstate->num_hashes);

		for (int setno = 0; setno < aggstate->num_hashes; setno++)
		{
			AggStatePerHash perhash = &aggstate->perhash[setno];
			HashAggSpill *spill = &aggstate->hash_spills[setno];

			hashagg_spill_init(spill, aggstate->hash_tapeset, 0,
							   perhash->aggnode->numGroups,
							   aggstate->hashentrysize);
		}
	}
}

/*
 * Update metrics after filling the hash table.
 *
 * If reading from the outer plan, from_tape should be false; if reading from
 * another tape, from_tape should be true.
 */
static void
hash_agg_update_metrics(AggState *aggstate, bool from_tape, int npartitions)
{
	Size		meta_mem;
	Size		entry_mem;
	Size		hashkey_mem;
	Size		buffer_mem;
	Size		total_mem;

	if (aggstate->aggstrategy != AGG_MIXED &&
		aggstate->aggstrategy != AGG_HASHED)
		return;

	/* memory for the hash table itself */
	meta_mem = MemoryContextMemAllocated(aggstate->hash_metacxt, true);

	/* memory for hash entries */
	entry_mem = MemoryContextMemAllocated(aggstate->hash_tuplescxt, true);

	/* memory for byref transition states */
	hashkey_mem = MemoryContextMemAllocated(aggstate->hashcontext->ecxt_per_tuple_memory, true);

	/* memory for read/write tape buffers, if spilled */
	buffer_mem = npartitions * HASHAGG_WRITE_BUFFER_SIZE;
	if (from_tape)
		buffer_mem += HASHAGG_READ_BUFFER_SIZE;

	/* update peak mem */
	total_mem = meta_mem + entry_mem + hashkey_mem + buffer_mem;
	if (total_mem > aggstate->hash_mem_peak)
		aggstate->hash_mem_peak = total_mem;

	/* update disk usage */
	if (aggstate->hash_tapeset != NULL)
	{
		uint64		disk_used = LogicalTapeSetBlocks(aggstate->hash_tapeset) * (BLCKSZ / 1024);

		if (aggstate->hash_disk_used < disk_used)
			aggstate->hash_disk_used = disk_used;
	}

	/* update hashentrysize estimate based on contents */
	if (aggstate->hash_ngroups_current > 0)
	{
		aggstate->hashentrysize =
			TupleHashEntrySize() +
			(hashkey_mem / (double) aggstate->hash_ngroups_current);
	}
}

/*
 * Create memory contexts used for hash aggregation.
 */
static void
hash_create_memory(AggState *aggstate)
{
	Size		maxBlockSize = ALLOCSET_DEFAULT_MAXSIZE;

	/*
	 * The hashcontext's per-tuple memory will be used for byref transition
	 * values and returned by AggCheckCallContext().
	 */
	aggstate->hashcontext = CreateWorkExprContext(aggstate->ss.ps.state);

	/*
	 * The meta context will be used for the bucket array of
	 * TupleHashEntryData (or arrays, in the case of grouping sets). As the
	 * hash table grows, the bucket array will double in size and the old one
	 * will be freed, so an AllocSet is appropriate. For large bucket arrays,
	 * the large allocation path will be used, so it's not worth worrying
	 * about wasting space due to power-of-two allocations.
	 */
	aggstate->hash_metacxt = AllocSetContextCreate(aggstate->ss.ps.state->es_query_cxt,
												   "HashAgg meta context",
												   ALLOCSET_DEFAULT_SIZES);

	/*
	 * The hash entries themselves, which include the grouping key
	 * (firstTuple) and pergroup data, are stored in the table context. The
	 * bump allocator can be used because the entries are not freed until the
	 * entire hash table is reset. The bump allocator is faster for
	 * allocations and avoids wasting space on the chunk header or
	 * power-of-two allocations.
	 *
	 * Like CreateWorkExprContext(), use smaller sizings for smaller work_mem,
	 * to avoid large jumps in memory usage.
	 */

	/*
	 * Like CreateWorkExprContext(), use smaller sizings for smaller work_mem,
	 * to avoid large jumps in memory usage.
	 */
	maxBlockSize = pg_prevpower2_size_t(work_mem * (Size) 1024 / 16);

	/* But no bigger than ALLOCSET_DEFAULT_MAXSIZE */
	maxBlockSize = Min(maxBlockSize, ALLOCSET_DEFAULT_MAXSIZE);

	/* and no smaller than ALLOCSET_DEFAULT_INITSIZE */
	maxBlockSize = Max(maxBlockSize, ALLOCSET_DEFAULT_INITSIZE);

	aggstate->hash_tuplescxt = BumpContextCreate(aggstate->ss.ps.state->es_query_cxt,
												 "HashAgg hashed tuples",
												 ALLOCSET_DEFAULT_MINSIZE,
												 ALLOCSET_DEFAULT_INITSIZE,
												 maxBlockSize);

}

/*
 * Choose a reasonable number of buckets for the initial hash table size.
 */
static double
hash_choose_num_buckets(double hashentrysize, double ngroups, Size memory)
{
	double		max_nbuckets;
	double		nbuckets = ngroups;

	max_nbuckets = memory / hashentrysize;

	/*
	 * Underestimating is better than overestimating. Too many buckets crowd
	 * out space for group keys and transition state values.
	 */
	max_nbuckets /= 2;

	if (nbuckets > max_nbuckets)
		nbuckets = max_nbuckets;

	/*
	 * BuildTupleHashTable will clamp any obviously-insane result, so we don't
	 * need to be too careful here.
	 */
	return nbuckets;
}

/*
 * Determine the number of partitions to create when spilling, which will
 * always be a power of two. If log2_npartitions is non-NULL, set
 * *log2_npartitions to the log2() of the number of partitions.
 */
static int
hash_choose_num_partitions(double input_groups, double hashentrysize,
						   int used_bits, int *log2_npartitions)
{
	Size		hash_mem_limit = get_hash_memory_limit();
	double		partition_limit;
	double		mem_wanted;
	double		dpartitions;
	int			npartitions;
	int			partition_bits;

	/*
	 * Avoid creating so many partitions that the memory requirements of the
	 * open partition files are greater than 1/4 of hash_mem.
	 */
	partition_limit =
		(hash_mem_limit * 0.25 - HASHAGG_READ_BUFFER_SIZE) /
		HASHAGG_WRITE_BUFFER_SIZE;

	mem_wanted = HASHAGG_PARTITION_FACTOR * input_groups * hashentrysize;

	/* make enough partitions so that each one is likely to fit in memory */
	dpartitions = 1 + (mem_wanted / hash_mem_limit);

	if (dpartitions > partition_limit)
		dpartitions = partition_limit;

	if (dpartitions < HASHAGG_MIN_PARTITIONS)
		dpartitions = HASHAGG_MIN_PARTITIONS;
	if (dpartitions > HASHAGG_MAX_PARTITIONS)
		dpartitions = HASHAGG_MAX_PARTITIONS;

	/* HASHAGG_MAX_PARTITIONS limit makes this safe */
	npartitions = (int) dpartitions;

	/* ceil(log2(npartitions)) */
	partition_bits = pg_ceil_log2_32(npartitions);

	/* make sure that we don't exhaust the hash bits */
	if (partition_bits + used_bits >= 32)
		partition_bits = 32 - used_bits;

	if (log2_npartitions != NULL)
		*log2_npartitions = partition_bits;

	/* number of partitions will be a power of two */
	npartitions = 1 << partition_bits;

	return npartitions;
}

/*
 * Initialize a freshly-created TupleHashEntry.
 */
static void
initialize_hash_entry(AggState *aggstate, TupleHashTable hashtable,
					  TupleHashEntry entry)
{
	AggStatePerGroup pergroup;
	int			transno;

	aggstate->hash_ngroups_current++;
	hash_agg_check_limits(aggstate);

	/* no need to allocate or initialize per-group state */
	if (aggstate->numtrans == 0)
		return;

	pergroup = (AggStatePerGroup) TupleHashEntryGetAdditional(hashtable, entry);

	/*
	 * Initialize aggregates for new tuple group, lookup_hash_entries()
	 * already has selected the relevant grouping set.
	 */
	for (transno = 0; transno < aggstate->numtrans; transno++)
	{
		AggStatePerTrans pertrans = &aggstate->pertrans[transno];
		AggStatePerGroup pergroupstate = &pergroup[transno];

		initialize_aggregate(aggstate, pertrans, pergroupstate);
	}
}

/*
 * Look up hash entries for the current tuple in all hashed grouping sets.
 *
 * Some entries may be left NULL if we are in "spill mode". The same tuple
 * will belong to different groups for each grouping set, so may match a group
 * already in memory for one set and match a group not in memory for another
 * set. When in "spill mode", the tuple will be spilled for each grouping set
 * where it doesn't match a group in memory.
 *
 * NB: It's possible to spill the same tuple for several different grouping
 * sets. This may seem wasteful, but it's actually a trade-off: if we spill
 * the tuple multiple times for multiple grouping sets, it can be partitioned
 * for each grouping set, making the refilling of the hash table very
 * efficient.
 */
static void
lookup_hash_entries(AggState *aggstate)
{
	AggStatePerGroup *pergroup = aggstate->hash_pergroup;
	TupleTableSlot *outerslot = aggstate->tmpcontext->ecxt_outertuple;
	int			setno;

	for (setno = 0; setno < aggstate->num_hashes; setno++)
	{
		AggStatePerHash perhash = &aggstate->perhash[setno];
		TupleHashTable hashtable = perhash->hashtable;
		TupleTableSlot *hashslot = perhash->hashslot;
		TupleHashEntry entry;
		uint32		hash;
		bool		isnew = false;
		bool	   *p_isnew;

		/* if hash table already spilled, don't create new entries */
		p_isnew = aggstate->hash_spill_mode ? NULL : &isnew;

		select_current_set(aggstate, setno, true);
		prepare_hash_slot(perhash,
						  outerslot,
						  hashslot);

		entry = LookupTupleHashEntry(hashtable, hashslot,
									 p_isnew, &hash);

		if (entry != NULL)
		{
			if (isnew)
				initialize_hash_entry(aggstate, hashtable, entry);
			pergroup[setno] = TupleHashEntryGetAdditional(hashtable, entry);
		}
		else
		{
			HashAggSpill *spill = &aggstate->hash_spills[setno];
			TupleTableSlot *slot = aggstate->tmpcontext->ecxt_outertuple;

			if (spill->partitions == NULL)
				hashagg_spill_init(spill, aggstate->hash_tapeset, 0,
								   perhash->aggnode->numGroups,
								   aggstate->hashentrysize);

			hashagg_spill_tuple(aggstate, spill, slot, hash);
			pergroup[setno] = NULL;
		}
	}
}

/*
 * ExecAgg -
 *
 *	  ExecAgg receives tuples from its outer subplan and aggregates over
 *	  the appropriate attribute for each aggregate function use (Aggref
 *	  node) appearing in the targetlist or qual of the node.  The number
 *	  of tuples to aggregate over depends on whether grouped or plain
 *	  aggregation is selected.  In grouped aggregation, we produce a result
 *	  row for each group; in plain aggregation there's a single result row
 *	  for the whole query.  In either case, the value of each aggregate is
 *	  stored in the expression context to be used when ExecProject evaluates
 *	  the result tuple.
 */
static TupleTableSlot *
ExecAgg(PlanState *pstate)
{
	AggState   *node = castNode(AggState, pstate);
	TupleTableSlot *result = NULL;

	CHECK_FOR_INTERRUPTS();

	if (!node->agg_done)
	{
		/* Dispatch based on strategy */
		switch (node->phase->aggstrategy)
		{
			case AGG_HASHED:
				if (node->shared_mode)
				{
					if (!node->table_filled)
						agg_fill_shared_hash_table(node);
					result = agg_retrieve_shared_hash_table(node);
					break;
				}
				if (!node->table_filled)
					agg_fill_hash_table(node);
				pg_fallthrough;
			case AGG_MIXED:
				result = agg_retrieve_hash_table(node);
				break;
			case AGG_PLAIN:
			case AGG_SORTED:
				result = agg_retrieve_direct(node);
				break;
		}

		if (!TupIsNull(result))
			return result;
	}

	return NULL;
}

/*
 * ExecAgg for non-hashed case
 */
static TupleTableSlot *
agg_retrieve_direct(AggState *aggstate)
{
	Agg		   *node = aggstate->phase->aggnode;
	ExprContext *econtext;
	ExprContext *tmpcontext;
	AggStatePerAgg peragg;
	AggStatePerGroup *pergroups;
	TupleTableSlot *outerslot;
	TupleTableSlot *firstSlot;
	TupleTableSlot *result;
	bool		hasGroupingSets = aggstate->phase->numsets > 0;
	int			numGroupingSets = Max(aggstate->phase->numsets, 1);
	int			currentSet;
	int			nextSetSize;
	int			numReset;
	int			i;

	/*
	 * get state info from node
	 *
	 * econtext is the per-output-tuple expression context
	 *
	 * tmpcontext is the per-input-tuple expression context
	 */
	econtext = aggstate->ss.ps.ps_ExprContext;
	tmpcontext = aggstate->tmpcontext;

	peragg = aggstate->peragg;
	pergroups = aggstate->pergroups;
	firstSlot = aggstate->ss.ss_ScanTupleSlot;

	/*
	 * We loop retrieving groups until we find one matching
	 * aggstate->ss.ps.qual
	 *
	 * For grouping sets, we have the invariant that aggstate->projected_set
	 * is either -1 (initial call) or the index (starting from 0) in
	 * gset_lengths for the group we just completed (either by projecting a
	 * row or by discarding it in the qual).
	 */
	while (!aggstate->agg_done)
	{
		/*
		 * Clear the per-output-tuple context for each group, as well as
		 * aggcontext (which contains any pass-by-ref transvalues of the old
		 * group).  Some aggregate functions store working state in child
		 * contexts; those now get reset automatically without us needing to
		 * do anything special.
		 *
		 * We use ReScanExprContext not just ResetExprContext because we want
		 * any registered shutdown callbacks to be called.  That allows
		 * aggregate functions to ensure they've cleaned up any non-memory
		 * resources.
		 */
		ReScanExprContext(econtext);

		/*
		 * Determine how many grouping sets need to be reset at this boundary.
		 */
		if (aggstate->projected_set >= 0 &&
			aggstate->projected_set < numGroupingSets)
			numReset = aggstate->projected_set + 1;
		else
			numReset = numGroupingSets;

		/*
		 * numReset can change on a phase boundary, but that's OK; we want to
		 * reset the contexts used in _this_ phase, and later, after possibly
		 * changing phase, initialize the right number of aggregates for the
		 * _new_ phase.
		 */

		for (i = 0; i < numReset; i++)
		{
			ReScanExprContext(aggstate->aggcontexts[i]);
		}

		/*
		 * Check if input is complete and there are no more groups to project
		 * in this phase; move to next phase or mark as done.
		 */
		if (aggstate->input_done == true &&
			aggstate->projected_set >= (numGroupingSets - 1))
		{
			if (aggstate->current_phase < aggstate->numphases - 1)
			{
				initialize_phase(aggstate, aggstate->current_phase + 1);
				aggstate->input_done = false;
				aggstate->projected_set = -1;
				numGroupingSets = Max(aggstate->phase->numsets, 1);
				node = aggstate->phase->aggnode;
				numReset = numGroupingSets;
			}
			else if (aggstate->aggstrategy == AGG_MIXED)
			{
				/*
				 * Mixed mode; we've output all the grouped stuff and have
				 * full hashtables, so switch to outputting those.
				 */
				initialize_phase(aggstate, 0);
				aggstate->table_filled = true;
				ResetTupleHashIterator(aggstate->perhash[0].hashtable,
									   &aggstate->perhash[0].hashiter);
				select_current_set(aggstate, 0, true);
				return agg_retrieve_hash_table(aggstate);
			}
			else
			{
				aggstate->agg_done = true;
				break;
			}
		}

		/*
		 * Get the number of columns in the next grouping set after the last
		 * projected one (if any). This is the number of columns to compare to
		 * see if we reached the boundary of that set too.
		 */
		if (aggstate->projected_set >= 0 &&
			aggstate->projected_set < (numGroupingSets - 1))
			nextSetSize = aggstate->phase->gset_lengths[aggstate->projected_set + 1];
		else
			nextSetSize = 0;

		/*----------
		 * If a subgroup for the current grouping set is present, project it.
		 *
		 * We have a new group if:
		 *	- we're out of input but haven't projected all grouping sets
		 *	  (checked above)
		 * OR
		 *	  - we already projected a row that wasn't from the last grouping
		 *		set
		 *	  AND
		 *	  - the next grouping set has at least one grouping column (since
		 *		empty grouping sets project only once input is exhausted)
		 *	  AND
		 *	  - the previous and pending rows differ on the grouping columns
		 *		of the next grouping set
		 *----------
		 */
		tmpcontext->ecxt_innertuple = econtext->ecxt_outertuple;
		if (aggstate->input_done ||
			(node->aggstrategy != AGG_PLAIN &&
			 aggstate->projected_set != -1 &&
			 aggstate->projected_set < (numGroupingSets - 1) &&
			 nextSetSize > 0 &&
			 !ExecQualAndReset(aggstate->phase->eqfunctions[nextSetSize - 1],
							   tmpcontext)))
		{
			aggstate->projected_set += 1;

			Assert(aggstate->projected_set < numGroupingSets);
			Assert(nextSetSize > 0 || aggstate->input_done);
		}
		else
		{
			/*
			 * We no longer care what group we just projected, the next
			 * projection will always be the first (or only) grouping set
			 * (unless the input proves to be empty).
			 */
			aggstate->projected_set = 0;

			/*
			 * If we don't already have the first tuple of the new group,
			 * fetch it from the outer plan.
			 */
			if (aggstate->grp_firstTuple == NULL)
			{
				outerslot = fetch_input_tuple(aggstate);
				if (!TupIsNull(outerslot))
				{
					/*
					 * Make a copy of the first input tuple; we will use this
					 * for comparisons (in group mode) and for projection.
					 */
					aggstate->grp_firstTuple = ExecCopySlotHeapTuple(outerslot);
				}
				else
				{
					/* outer plan produced no tuples at all */
					if (hasGroupingSets)
					{
						/*
						 * If there was no input at all, we need to project
						 * rows only if there are grouping sets of size 0.
						 * Note that this implies that there can't be any
						 * references to ungrouped Vars, which would otherwise
						 * cause issues with the empty output slot.
						 *
						 * XXX: This is no longer true, we currently deal with
						 * this in finalize_aggregates().
						 */
						aggstate->input_done = true;

						while (aggstate->phase->gset_lengths[aggstate->projected_set] > 0)
						{
							aggstate->projected_set += 1;
							if (aggstate->projected_set >= numGroupingSets)
							{
								/*
								 * We can't set agg_done here because we might
								 * have more phases to do, even though the
								 * input is empty. So we need to restart the
								 * whole outer loop.
								 */
								break;
							}
						}

						if (aggstate->projected_set >= numGroupingSets)
							continue;
					}
					else
					{
						aggstate->agg_done = true;
						/* If we are grouping, we should produce no tuples too */
						if (node->aggstrategy != AGG_PLAIN)
							return NULL;
					}
				}
			}

			/*
			 * Initialize working state for a new input tuple group.
			 */
			initialize_aggregates(aggstate, pergroups, numReset);

			if (aggstate->grp_firstTuple != NULL)
			{
				/*
				 * Store the copied first input tuple in the tuple table slot
				 * reserved for it.  The tuple will be deleted when it is
				 * cleared from the slot.
				 */
				ExecForceStoreHeapTuple(aggstate->grp_firstTuple,
										firstSlot, true);
				aggstate->grp_firstTuple = NULL;	/* don't keep two pointers */

				/* set up for first advance_aggregates call */
				tmpcontext->ecxt_outertuple = firstSlot;

				/*
				 * Process each outer-plan tuple, and then fetch the next one,
				 * until we exhaust the outer plan or cross a group boundary.
				 */
				for (;;)
				{
					/*
					 * During phase 1 only of a mixed agg, we need to update
					 * hashtables as well in advance_aggregates.
					 */
					if (aggstate->aggstrategy == AGG_MIXED &&
						aggstate->current_phase == 1)
					{
						lookup_hash_entries(aggstate);
					}

					/* Advance the aggregates (or combine functions) */
					advance_aggregates(aggstate);

					/* Reset per-input-tuple context after each tuple */
					ResetExprContext(tmpcontext);

					outerslot = fetch_input_tuple(aggstate);
					if (TupIsNull(outerslot))
					{
						/* no more outer-plan tuples available */

						/* if we built hash tables, finalize any spills */
						if (aggstate->aggstrategy == AGG_MIXED &&
							aggstate->current_phase == 1)
							hashagg_finish_initial_spills(aggstate);

						if (hasGroupingSets)
						{
							aggstate->input_done = true;
							break;
						}
						else
						{
							aggstate->agg_done = true;
							break;
						}
					}
					/* set up for next advance_aggregates call */
					tmpcontext->ecxt_outertuple = outerslot;

					/*
					 * If we are grouping, check whether we've crossed a group
					 * boundary.
					 */
					if (node->aggstrategy != AGG_PLAIN && node->numCols > 0)
					{
						tmpcontext->ecxt_innertuple = firstSlot;
						if (!ExecQual(aggstate->phase->eqfunctions[node->numCols - 1],
									  tmpcontext))
						{
							aggstate->grp_firstTuple = ExecCopySlotHeapTuple(outerslot);
							break;
						}
					}
				}
			}

			/*
			 * Use the representative input tuple for any references to
			 * non-aggregated input columns in aggregate direct args, the node
			 * qual, and the tlist.  (If we are not grouping, and there are no
			 * input rows at all, we will come here with an empty firstSlot
			 * ... but if not grouping, there can't be any references to
			 * non-aggregated input columns, so no problem.)
			 */
			econtext->ecxt_outertuple = firstSlot;
		}

		Assert(aggstate->projected_set >= 0);

		currentSet = aggstate->projected_set;

		prepare_projection_slot(aggstate, econtext->ecxt_outertuple, currentSet);

		select_current_set(aggstate, currentSet, false);

		finalize_aggregates(aggstate,
							peragg,
							pergroups[currentSet]);

		/*
		 * If there's no row to project right now, we must continue rather
		 * than returning a null since there might be more groups.
		 */
		result = project_aggregates(aggstate);
		if (result)
			return result;
	}

	/* No more groups */
	return NULL;
}

/*
 * ExecAgg for hashed case: read input and build hash table
 */
static void
agg_fill_hash_table(AggState *aggstate)
{
	TupleTableSlot *outerslot;
	ExprContext *tmpcontext = aggstate->tmpcontext;

	/*
	 * Process each outer-plan tuple, and then fetch the next one, until we
	 * exhaust the outer plan.
	 */
	for (;;)
	{
		outerslot = fetch_input_tuple(aggstate);
		if (TupIsNull(outerslot))
			break;

		/* set up for lookup_hash_entries and advance_aggregates */
		tmpcontext->ecxt_outertuple = outerslot;

		/* Find or build hashtable entries */
		lookup_hash_entries(aggstate);

		/* Advance the aggregates (or combine functions) */
		advance_aggregates(aggstate);

		/*
		 * Reset per-input-tuple context after each tuple, but note that the
		 * hash lookups do this too
		 */
		ResetExprContext(aggstate->tmpcontext);
	}

	/* finalize spills, if any */
	hashagg_finish_initial_spills(aggstate);

	aggstate->table_filled = true;
	/* Initialize to walk the first hash table */
	select_current_set(aggstate, 0, true);
	ResetTupleHashIterator(aggstate->perhash[0].hashtable,
						   &aggstate->perhash[0].hashiter);
}

/*
 * If any data was spilled during hash aggregation, reset the hash table and
 * reprocess one batch of spilled data. After reprocessing a batch, the hash
 * table will again contain data, ready to be consumed by
 * agg_retrieve_hash_table_in_memory().
 *
 * Should only be called after all in memory hash table entries have been
 * finalized and emitted.
 *
 * Return false when input is exhausted and there's no more work to be done;
 * otherwise return true.
 */
static bool
agg_refill_hash_table(AggState *aggstate)
{
	HashAggBatch *batch;
	AggStatePerHash perhash;
	HashAggSpill spill;
	LogicalTapeSet *tapeset = aggstate->hash_tapeset;
	bool		spill_initialized = false;

	if (aggstate->hash_batches == NIL)
		return false;

	/* hash_batches is a stack, with the top item at the end of the list */
	batch = llast(aggstate->hash_batches);
	aggstate->hash_batches = list_delete_last(aggstate->hash_batches);

	hash_agg_set_limits(aggstate->hashentrysize, batch->input_card,
						batch->used_bits, &aggstate->hash_mem_limit,
						&aggstate->hash_ngroups_limit, NULL);

	/*
	 * Each batch only processes one grouping set; set the rest to NULL so
	 * that advance_aggregates() knows to ignore them. We don't touch
	 * pergroups for sorted grouping sets here, because they will be needed if
	 * we rescan later. The expressions for sorted grouping sets will not be
	 * evaluated after we recompile anyway.
	 */
	MemSet(aggstate->hash_pergroup, 0,
		   sizeof(AggStatePerGroup) * aggstate->num_hashes);

	/* free memory and reset hash tables */
	ReScanExprContext(aggstate->hashcontext);
	for (int setno = 0; setno < aggstate->num_hashes; setno++)
		ResetTupleHashTable(aggstate->perhash[setno].hashtable);

	aggstate->hash_ngroups_current = 0;

	/*
	 * In AGG_MIXED mode, hash aggregation happens in phase 1 and the output
	 * happens in phase 0. So, we switch to phase 1 when processing a batch,
	 * and back to phase 0 after the batch is done.
	 */
	Assert(aggstate->current_phase == 0);
	if (aggstate->phase->aggstrategy == AGG_MIXED)
	{
		aggstate->current_phase = 1;
		aggstate->phase = &aggstate->phases[aggstate->current_phase];
	}

	select_current_set(aggstate, batch->setno, true);

	perhash = &aggstate->perhash[aggstate->current_set];

	/*
	 * Spilled tuples are always read back as MinimalTuples, which may be
	 * different from the outer plan, so recompile the aggregate expressions.
	 *
	 * We still need the NULL check, because we are only processing one
	 * grouping set at a time and the rest will be NULL.
	 */
	hashagg_recompile_expressions(aggstate, true, true);

	INJECTION_POINT("hash-aggregate-process-batch", NULL);
	for (;;)
	{
		TupleTableSlot *spillslot = aggstate->hash_spill_rslot;
		TupleTableSlot *hashslot = perhash->hashslot;
		TupleHashTable hashtable = perhash->hashtable;
		TupleHashEntry entry;
		MinimalTuple tuple;
		uint32		hash;
		bool		isnew = false;
		bool	   *p_isnew = aggstate->hash_spill_mode ? NULL : &isnew;

		CHECK_FOR_INTERRUPTS();

		tuple = hashagg_batch_read(batch, &hash);
		if (tuple == NULL)
			break;

		ExecStoreMinimalTuple(tuple, spillslot, true);
		aggstate->tmpcontext->ecxt_outertuple = spillslot;

		prepare_hash_slot(perhash,
						  aggstate->tmpcontext->ecxt_outertuple,
						  hashslot);
		entry = LookupTupleHashEntryHash(hashtable, hashslot,
										 p_isnew, hash);

		if (entry != NULL)
		{
			if (isnew)
				initialize_hash_entry(aggstate, hashtable, entry);
			aggstate->hash_pergroup[batch->setno] = TupleHashEntryGetAdditional(hashtable, entry);
			advance_aggregates(aggstate);
		}
		else
		{
			if (!spill_initialized)
			{
				/*
				 * Avoid initializing the spill until we actually need it so
				 * that we don't assign tapes that will never be used.
				 */
				spill_initialized = true;
				hashagg_spill_init(&spill, tapeset, batch->used_bits,
								   batch->input_card, aggstate->hashentrysize);
			}
			/* no memory for a new group, spill */
			hashagg_spill_tuple(aggstate, &spill, spillslot, hash);

			aggstate->hash_pergroup[batch->setno] = NULL;
		}

		/*
		 * Reset per-input-tuple context after each tuple, but note that the
		 * hash lookups do this too
		 */
		ResetExprContext(aggstate->tmpcontext);
	}

	LogicalTapeClose(batch->input_tape);

	/* change back to phase 0 */
	aggstate->current_phase = 0;
	aggstate->phase = &aggstate->phases[aggstate->current_phase];

	if (spill_initialized)
	{
		hashagg_spill_finish(aggstate, &spill, batch->setno);
		hash_agg_update_metrics(aggstate, true, spill.npartitions);
	}
	else
		hash_agg_update_metrics(aggstate, true, 0);

	aggstate->hash_spill_mode = false;

	/* prepare to walk the first hash table */
	select_current_set(aggstate, batch->setno, true);
	ResetTupleHashIterator(aggstate->perhash[batch->setno].hashtable,
						   &aggstate->perhash[batch->setno].hashiter);

	pfree(batch);

	return true;
}

/*
 * ExecAgg for hashed case: retrieving groups from hash table
 *
 * After exhausting in-memory tuples, also try refilling the hash table using
 * previously-spilled tuples. Only returns NULL after all in-memory and
 * spilled tuples are exhausted.
 */
static TupleTableSlot *
agg_retrieve_hash_table(AggState *aggstate)
{
	TupleTableSlot *result = NULL;

	while (result == NULL)
	{
		result = agg_retrieve_hash_table_in_memory(aggstate);
		if (result == NULL)
		{
			if (!agg_refill_hash_table(aggstate))
			{
				aggstate->agg_done = true;
				break;
			}
		}
	}

	return result;
}

/*
 * Retrieve the groups from the in-memory hash tables without considering any
 * spilled tuples.
 */
static TupleTableSlot *
agg_retrieve_hash_table_in_memory(AggState *aggstate)
{
	ExprContext *econtext;
	AggStatePerAgg peragg;
	AggStatePerGroup pergroup;
	TupleHashEntry entry;
	TupleTableSlot *firstSlot;
	TupleTableSlot *result;
	AggStatePerHash perhash;

	/*
	 * get state info from node.
	 *
	 * econtext is the per-output-tuple expression context.
	 */
	econtext = aggstate->ss.ps.ps_ExprContext;
	peragg = aggstate->peragg;
	firstSlot = aggstate->ss.ss_ScanTupleSlot;

	/*
	 * Note that perhash (and therefore anything accessed through it) can
	 * change inside the loop, as we change between grouping sets.
	 */
	perhash = &aggstate->perhash[aggstate->current_set];

	/*
	 * We loop retrieving groups until we find one satisfying
	 * aggstate->ss.ps.qual
	 */
	for (;;)
	{
		TupleTableSlot *hashslot = perhash->hashslot;
		TupleHashTable hashtable = perhash->hashtable;
		int			i;

		CHECK_FOR_INTERRUPTS();

		/*
		 * Find the next entry in the hash table
		 */
		entry = ScanTupleHashTable(hashtable, &perhash->hashiter);
		if (entry == NULL)
		{
			int			nextset = aggstate->current_set + 1;

			if (nextset < aggstate->num_hashes)
			{
				/*
				 * Switch to next grouping set, reinitialize, and restart the
				 * loop.
				 */
				select_current_set(aggstate, nextset, true);

				perhash = &aggstate->perhash[aggstate->current_set];

				ResetTupleHashIterator(perhash->hashtable, &perhash->hashiter);

				continue;
			}
			else
			{
				return NULL;
			}
		}

		/*
		 * Clear the per-output-tuple context for each group
		 *
		 * We intentionally don't use ReScanExprContext here; if any aggs have
		 * registered shutdown callbacks, they mustn't be called yet, since we
		 * might not be done with that agg.
		 */
		ResetExprContext(econtext);

		/*
		 * Transform representative tuple back into one with the right
		 * columns.
		 */
		ExecStoreMinimalTuple(TupleHashEntryGetTuple(entry), hashslot, false);
		slot_getallattrs(hashslot);

		ExecClearTuple(firstSlot);
		memset(firstSlot->tts_isnull, true,
			   firstSlot->tts_tupleDescriptor->natts * sizeof(bool));

		for (i = 0; i < perhash->numhashGrpCols; i++)
		{
			int			varNumber = perhash->hashGrpColIdxInput[i] - 1;

			firstSlot->tts_values[varNumber] = hashslot->tts_values[i];
			firstSlot->tts_isnull[varNumber] = hashslot->tts_isnull[i];
		}
		ExecStoreVirtualTuple(firstSlot);

		pergroup = (AggStatePerGroup) TupleHashEntryGetAdditional(hashtable, entry);

		/*
		 * Use the representative input tuple for any references to
		 * non-aggregated input columns in the qual and tlist.
		 */
		econtext->ecxt_outertuple = firstSlot;

		prepare_projection_slot(aggstate,
								econtext->ecxt_outertuple,
								aggstate->current_set);

		finalize_aggregates(aggstate, peragg, pergroup);

		result = project_aggregates(aggstate);
		if (result)
			return result;
	}

	/* No more groups */
	return NULL;
}

/*
 * Set up executor-local support for parallel shared hash aggregation:
 * a deterministic hash expression (zero IV, so every participant computes
 * identical hashes), a group-key equality expression comparing the condensed
 * input key against a stored MinimalTuple, a slot to read stored keys, a
 * private ExprContext so hash/equality evaluation cannot clobber the slots
 * the transition arguments read from tmpcontext, and per-transition advance
 * support (the shared path does not use the compiled evaltrans program).
 */
static void
shared_agg_init_support(AggState *aggstate)
{
	AggStatePerHash perhash = &aggstate->perhash[0];
	EState	   *estate = aggstate->ss.ps.state;
	TupleDesc	keyDesc = perhash->hashslot->tts_tupleDescriptor;
	int			transno;

	/* planner only generates this for plain GROUP BY */
	Assert(aggstate->num_hashes == 1);

	aggstate->shared_keyslot =
		ExecInitExtraTupleSlot(estate, CreateTupleDescCopy(keyDesc),
							   &TTSOpsMinimalTuple);
	aggstate->shared_exprcontext = CreateExprContext(estate);

	aggstate->shared_hashexpr =
		ExecBuildHash32FromAttrs(keyDesc,
								 perhash->hashslot->tts_ops,
								 perhash->hashfunctions,
								 perhash->aggnode->grpCollations,
								 perhash->numCols,
								 perhash->hashGrpColIdxHash,
								 &aggstate->ss.ps,
								 0);

	/* INNER = condensed input key (hashslot), OUTER = stored key tuple */
	aggstate->shared_eqexpr =
		ExecBuildGroupingEqual(keyDesc, keyDesc,
							   perhash->hashslot->tts_ops,
							   &TTSOpsMinimalTuple,
							   perhash->numCols,
							   perhash->hashGrpColIdxHash,
							   perhash->eqfuncoids,
							   perhash->aggnode->grpCollations,
							   &aggstate->ss.ps);

	aggstate->shared_scan_entry = InvalidDsaPointer;

	/*
	 * Classify every transition and build its argument/filter expressions.
	 * The planner has already vetted eligibility; anything neither by-value
	 * nor whitelisted here is a planner/executor disagreement.
	 */
	aggstate->shared_transinfo =
		palloc0(aggstate->numtrans * sizeof(SharedAggTransInfo));
	aggstate->shared_scratch_pergroup =
		palloc0(aggstate->numtrans * sizeof(AggStatePerGroupData));

	for (transno = 0; transno < aggstate->numtrans; transno++)
	{
		AggStatePerTrans pertrans = &aggstate->pertrans[transno];
		Aggref	   *aggref = pertrans->aggref;
		SharedAggTransInfo *info = &aggstate->shared_transinfo[transno];
		const TupleTableSlotOps *save_outerops;
		bool		save_outeropsfixed;
		ListCell   *arg;
		int			i;

		/*
		 * Cross-check the planner's eligibility decision.  Note that
		 * testing transtypeByVal alone would NOT suffice: the 'internal'
		 * pseudo-type is itself pass-by-value (a pointer-sized Datum),
		 * while the pointed-to struct is process-local and must never be
		 * stored in the shared table.
		 */
		if (pertrans->aggtranstype == INTERNALOID)
			elog(ERROR, "aggregate %u is not supported by shared hash aggregation",
				 aggref->aggfnoid);

		info->blob_stored = !pertrans->transtypeByVal;

		info->numargs = list_length(aggref->args);
		info->argstates = palloc0(info->numargs * sizeof(ExprState *));
		info->argstates_spill = palloc0(info->numargs * sizeof(ExprState *));
		info->pending_args = palloc0(info->numargs * sizeof(Datum));
		info->pending_nulls = palloc0(info->numargs * sizeof(bool));
		i = 0;
		foreach(arg, aggref->args)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(arg);

			info->argstates[i++] = ExecInitExpr(tle->expr, &aggstate->ss.ps);
		}

		if (aggref->aggfilter)
			info->filterstate = ExecInitExpr(aggref->aggfilter,
											 &aggstate->ss.ps);

		/*
		 * Build second variants compiled for minimal-tuple outer slots, for
		 * reading spilled batches — the same trick as
		 * hashagg_recompile_expressions().
		 */
		save_outerops = aggstate->ss.ps.outerops;
		save_outeropsfixed = aggstate->ss.ps.outeropsfixed;
		aggstate->ss.ps.outerops = &TTSOpsMinimalTuple;
		aggstate->ss.ps.outeropsfixed = true;

		i = 0;
		foreach(arg, aggref->args)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(arg);

			info->argstates_spill[i++] = ExecInitExpr(tle->expr,
													  &aggstate->ss.ps);
		}
		if (aggref->aggfilter)
			info->filterstate_spill = ExecInitExpr(aggref->aggfilter,
												   &aggstate->ss.ps);

		aggstate->ss.ps.outerops = save_outerops;
		aggstate->ss.ps.outeropsfixed = save_outeropsfixed;
	}
}

/*
 * Evaluate FILTER clauses, transition arguments, and (for the numeric fast
 * path) the rescaling of the input into each transition's pending staging
 * area.  This runs with NO lock held: everything here is a pure function of
 * the input tuple (tmpcontext->ecxt_outertuple), and can involve arbitrary
 * expression evaluation, detoasting and numeric arithmetic that must not
 * serialize other participants.
 *
 * from_spill selects the expression variants compiled for minimal-tuple
 * outer slots (spill batch reads).
 */
static void
shared_agg_prepare_inputs(AggState *aggstate, bool from_spill)
{
	ExprContext *tmpcontext = aggstate->tmpcontext;
	int			transno;

	for (transno = 0; transno < aggstate->numtrans; transno++)
	{
		SharedAggTransInfo *info = &aggstate->shared_transinfo[transno];
		ExprState **argstates = from_spill ? info->argstates_spill :
			info->argstates;
		ExprState  *filterstate = from_spill ? info->filterstate_spill :
			info->filterstate;
		int			i;

		info->pending_skip = false;

		if (filterstate)
		{
			Datum		res;
			bool		resnull;

			res = ExecEvalExprSwitchContext(filterstate, tmpcontext,
											&resnull);
			if (resnull || !DatumGetBool(res))
			{
				info->pending_skip = true;
				continue;
			}
		}

		for (i = 0; i < info->numargs; i++)
			info->pending_args[i] =
				ExecEvalExprSwitchContext(argstates[i], tmpcontext,
										  &info->pending_nulls[i]);
	}
}

/*
 * Allocate space for one hash table entry, carving it from the
 * participant's current chunk (allocating and publishing a fresh chunk
 * when needed).  Oversized entries get a dedicated chunk.  Returns the
 * local address and sets *entryp_out to the corresponding dsa_pointer.
 */
static SharedAggEntry *
shared_agg_alloc_entry(AggState *aggstate, dsa_area *area, Size len,
					   dsa_pointer *entryp_out)
{
	SharedAggBuildState *shstate = aggstate->shared_build;
	dsa_pointer entryp;

	Assert(len >= sizeof(SharedAggEntry));

	len = MAXALIGN(len);

	/*
	 * Note: this runs with the caller's bucket stripe lock held, so a
	 * chunk refill (dsa_allocate + a brief batch_lock acquisition, once
	 * per ~32KB of entries) briefly extends that lock's hold time.
	 * Releasing and re-acquiring around the refill would require
	 * re-walking the bucket chain (the group might appear concurrently);
	 * not worth it for an amortized-rare event.
	 */

	if (!DsaPointerIsValid((dsa_pointer) aggstate->shared_alloc_chunk) ||
		aggstate->shared_alloc_used + len > aggstate->shared_alloc_size)
	{
		Size		chunksz;
		dsa_pointer chunkp;
		SharedAggChunk *chunk;

		chunksz = Max((Size) SHARED_AGG_CHUNK_SIZE,
					  SharedAggChunkHeaderSize + len);
		chunkp = dsa_allocate(area, chunksz);
		chunk = (SharedAggChunk *) dsa_get_address(area, chunkp);

		/* publish for wholesale freeing at batch reset */
		LWLockAcquire(&shstate->batch_lock, LW_EXCLUSIVE);
		chunk->next = shstate->chunk_head;
		shstate->chunk_head = chunkp;
		LWLockRelease(&shstate->batch_lock);

		pg_atomic_add_fetch_u64(&shstate->mem_used, chunksz);

		aggstate->shared_alloc_chunk = chunkp;
		aggstate->shared_alloc_used = SharedAggChunkHeaderSize;
		aggstate->shared_alloc_size = chunksz;
	}

	entryp = (dsa_pointer) aggstate->shared_alloc_chunk +
		aggstate->shared_alloc_used;
	aggstate->shared_alloc_used += len;

	*entryp_out = entryp;
	return (SharedAggEntry *) dsa_get_address(area, entryp);
}

/*
 * Store a plain varlena into the DSA area and return its dsa_pointer.
 * Memory accounting only ever adds: blobs replaced during aggregation are
 * freed but not subtracted, which can only make spilling kick in earlier —
 * the safe direction.
 */
static dsa_pointer
shared_agg_store_varlena(AggState *aggstate, dsa_area *area,
						 struct varlena *val)
{
	SharedAggBuildState *shstate = aggstate->shared_build;
	Size		sz = VARSIZE_ANY(val);
	dsa_pointer sp;

	sp = dsa_allocate(area, sz);
	memcpy(dsa_get_address(area, sp), val, sz);
	pg_atomic_add_fetch_u64(&shstate->mem_used, sz);
	return sp;
}

/*
 * Try to replace the state blob at *state_dp with the new (flat, local)
 * representation IN PLACE.  Steady-state aggregation tends to produce
 * same-sized states row after row (serialized numeric accumulators,
 * fixed-scale sums, all fixed-length types), and overwriting in place
 * avoids a dsa_allocate + dsa_free round trip per row — dsa_allocate takes
 * area-global locks, so this matters under concurrency.  Returns true if
 * the blob was overwritten; false if the caller must allocate anew.
 */
static bool
shared_agg_overwrite_blob(dsa_area *area, dsa_pointer state_dp,
						  const void *newdata, Size newsz)
{
	void	   *old = dsa_get_address(area, state_dp);

	if (VARSIZE_ANY(old) != newsz)
		return false;
	memcpy(old, newdata, newsz);
	return true;
}

/*
 * Copy a transition value into the DSA area as a self-contained blob,
 * detoasting varlenas and flattening expanded datums, and return its
 * dsa_pointer.
 */
static dsa_pointer
shared_agg_store_datum(AggState *aggstate, dsa_area *area,
					   AggStatePerTrans pertrans, Datum value)
{
	SharedAggBuildState *shstate = aggstate->shared_build;
	dsa_pointer sp;
	Size		sz;

	if (pertrans->transtypeLen > 0)
	{
		/* fixed-length by-reference type (e.g. interval) */
		sz = (Size) pertrans->transtypeLen;
		sp = dsa_allocate(area, sz);
		memcpy(dsa_get_address(area, sp), DatumGetPointer(value), sz);
	}
	else if (pertrans->transtypeLen == -2)
	{
		/* cstring */
		sz = strlen(DatumGetCString(value)) + 1;
		sp = dsa_allocate(area, sz);
		memcpy(dsa_get_address(area, sp), DatumGetPointer(value), sz);
	}
	else if (VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(value)))
	{
		/* expanded datum: flatten directly into the shared blob */
		ExpandedObjectHeader *eoh = DatumGetEOHP(value);

		sz = EOH_get_flat_size(eoh);
		sp = dsa_allocate(area, sz);
		EOH_flatten_into(eoh, dsa_get_address(area, sp), sz);
	}
	else
	{
		/* varlena: detoast (into per-tuple memory) and copy flat bytes */
		return shared_agg_store_varlena(aggstate, area,
										pg_detoast_datum((struct varlena *) DatumGetPointer(value)));
	}

	pg_atomic_add_fetch_u64(&shstate->mem_used, sz);
	return sp;
}

/*
 * Advance one by-ref transition state stored in the DSA area.  Caller holds
 * the stripe LWLock and has staged the argument values.
 *
 * This mirrors advance_transition_function(), with the copy semantics
 * replaced: the state Datum in the shared slot is a dsa_pointer, mapped to
 * a local address around the call; a new result is copied into DSA and the
 * old blob freed.  A transition function that returns its own input
 * unchanged (min/max) or scribbles on it in place (custom bytea states,
 * sanctioned via AggCheckCallContext) touches the shared blob directly and
 * requires no copy.
 *
 * Note: transition functions that allocate their result in the aggcontext
 * (as AggCheckCallContext invites) leak that local copy until end of query
 * in shared mode, since we immediately copy it into DSA; the aggcontext is
 * not reset between groups here.  Bounded by input size per row, but worth
 * remembering when profiling memory.
 */
static void
shared_agg_advance_byref(AggState *aggstate, dsa_area *area,
						 AggStatePerTrans pertrans,
						 SharedAggTransInfo *info,
						 AggStatePerGroup pergroupstate)
{
	FunctionCallInfo fcinfo = pertrans->transfn_fcinfo;
	MemoryContext oldContext;
	Datum		newVal;
	void	   *oldstate = NULL;
	int			i;

	for (i = 0; i < info->numargs; i++)
	{
		fcinfo->args[i + 1].value = info->pending_args[i];
		fcinfo->args[i + 1].isnull = info->pending_nulls[i];
	}

	if (pertrans->transfn.fn_strict)
	{
		for (i = 1; i <= pertrans->numTransInputs; i++)
		{
			if (fcinfo->args[i].isnull)
				return;
		}
		if (pergroupstate->noTransValue)
		{
			/*
			 * First non-NULL input becomes the state (the agg's input type
			 * is binary-compatible with its transtype, as for the regular
			 * path).
			 */
			pergroupstate->transValue =
				(Datum) shared_agg_store_datum(aggstate, area, pertrans,
											   fcinfo->args[1].value);
			pergroupstate->transValueIsNull = false;
			pergroupstate->noTransValue = false;
			return;
		}
		if (pergroupstate->transValueIsNull)
			return;
	}

	oldContext = MemoryContextSwitchTo(aggstate->tmpcontext->ecxt_per_tuple_memory);

	/* set up aggstate->curpertrans for AggGetAggref() */
	aggstate->curpertrans = pertrans;

	if (!pergroupstate->transValueIsNull)
	{
		oldstate = dsa_get_address(area, (dsa_pointer) pergroupstate->transValue);
		fcinfo->args[0].value = PointerGetDatum(oldstate);
		fcinfo->args[0].isnull = false;
	}
	else
	{
		fcinfo->args[0].value = (Datum) 0;
		fcinfo->args[0].isnull = true;
	}
	fcinfo->isnull = false;

	newVal = FunctionCallInvoke(fcinfo);

	aggstate->curpertrans = NULL;

	if (fcinfo->isnull)
	{
		if (!pergroupstate->transValueIsNull)
			dsa_free(area, (dsa_pointer) pergroupstate->transValue);
		pergroupstate->transValue = (Datum) 0;
		pergroupstate->transValueIsNull = true;
	}
	else if (DatumGetPointer(newVal) != oldstate)
	{
		/*
		 * Prefer overwriting the existing blob in place: fixed-length
		 * states are always the same size, and varlena states usually
		 * stabilize (fixed-scale sums, extrema of similar length),
		 * avoiding per-row dsa_allocate/dsa_free traffic.
		 */
		if (!pergroupstate->transValueIsNull && pertrans->transtypeLen > 0)
		{
			memcpy(dsa_get_address(area, (dsa_pointer) pergroupstate->transValue),
				   DatumGetPointer(newVal), (Size) pertrans->transtypeLen);
		}
		else
		{
			struct varlena *flat = NULL;

			if (!pergroupstate->transValueIsNull &&
				pertrans->transtypeLen == -1 &&
				!VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(newVal)))
				flat = pg_detoast_datum((struct varlena *) DatumGetPointer(newVal));

			if (flat != NULL &&
				shared_agg_overwrite_blob(area,
										  (dsa_pointer) pergroupstate->transValue,
										  flat, VARSIZE_ANY(flat)))
			{
				/* overwritten in place */
			}
			else
			{
				dsa_pointer newsp;

				newsp = shared_agg_store_datum(aggstate, area, pertrans,
											   newVal);
				if (!pergroupstate->transValueIsNull)
					dsa_free(area, (dsa_pointer) pergroupstate->transValue);
				pergroupstate->transValue = (Datum) newsp;
				pergroupstate->transValueIsNull = false;
			}
		}
	}
	/* else: state was modified (or kept) in place in shared memory */

	MemoryContextSwitchTo(oldContext);
}

/*
 * Apply the staged inputs to one shared entry's transition states.  Caller
 * holds the entry's stripe LWLock; keep this minimal — no expression
 * evaluation and no detoasting of inputs (both happened pre-lock), though
 * by-ref transition functions do run here by design.
 */
static void
shared_agg_apply(AggState *aggstate, dsa_area *area, AggStatePerGroup states)
{
	int			transno;

	for (transno = 0; transno < aggstate->numtrans; transno++)
	{
		AggStatePerTrans pertrans = &aggstate->pertrans[transno];
		SharedAggTransInfo *info = &aggstate->shared_transinfo[transno];
		AggStatePerGroup pergroupstate = &states[transno];

		if (info->pending_skip)
			continue;

		if (pertrans->transtypeByVal)
		{
			FunctionCallInfo fcinfo = pertrans->transfn_fcinfo;
			int			i;

			for (i = 0; i < info->numargs; i++)
			{
				fcinfo->args[i + 1].value = info->pending_args[i];
				fcinfo->args[i + 1].isnull = info->pending_nulls[i];
			}
			advance_transition_function(aggstate, pertrans, pergroupstate);
		}
		else
			shared_agg_advance_byref(aggstate, area, pertrans, info,
									 pergroupstate);
	}
}

/*
 * Find-or-create the group for the key currently in hashslot, and apply the
 * staged transition inputs to it.  If the group is absent, memory is over
 * budget, and allow_spill is true, the full input tuple is written to the
 * appropriate spill partition instead.
 *
 * Caller must already have run shared_agg_prepare_inputs().
 */
static void
shared_agg_insert(AggState *aggstate, dsa_area *area,
				  TupleTableSlot *hashslot, uint32 hash, bool allow_spill)
{
	SharedAggBuildState *shstate = aggstate->shared_build;
	ExprContext *tmpcontext = aggstate->tmpcontext;
	ExprContext *hashcxt = aggstate->shared_exprcontext;
	TupleTableSlot *keyslot = aggstate->shared_keyslot;
	dsa_pointer *buckets = (dsa_pointer *)
		dsa_get_address(area, shstate->buckets);
	uint64		mask = shstate->nbuckets - 1;
	uint64		bucketno = hash & mask;
	LWLock	   *lock = SharedAggStripeLock(shstate, bucketno);
	dsa_pointer entryp;
	SharedAggEntry *entry = NULL;

	LWLockAcquire(lock, LW_EXCLUSIVE);

	/* walk the chain looking for our group */
	entryp = buckets[bucketno];
	while (DsaPointerIsValid(entryp))
	{
		SharedAggEntry *cand = (SharedAggEntry *)
			dsa_get_address(area, entryp);

		if (cand->hash == hash)
		{
			ExecStoreMinimalTuple(SharedAggEntryTuple(cand, aggstate->numtrans),
								  keyslot, false);
			hashcxt->ecxt_innertuple = hashslot;
			hashcxt->ecxt_outertuple = keyslot;
			if (ExecQualAndReset(aggstate->shared_eqexpr, hashcxt))
			{
				entry = cand;
				break;
			}
		}
		entryp = cand->next;
	}

	if (entry == NULL)
	{
		MinimalTuple mt;
		Size		alloclen;
		dsa_pointer newp;
		SharedAggEntry *newentry;
		AggStatePerGroup states;
		MemoryContext oldcxt;
		int			transno;
		uint64		used;

		/*
		 * Over budget?  Route the whole input tuple to its spill partition
		 * instead of growing the table.  Existing groups keep advancing in
		 * place above, so memory use is stable in spill mode — the same
		 * semantics as the private hashagg's hash_spill_mode.
		 */
		if (allow_spill &&
			pg_atomic_read_u32(&shstate->spill_mode) != 0)
		{
			uint32		partno = SharedAggSpillPartition(hash);

			LWLockRelease(lock);

			/*
			 * Note that shared_agg_prepare_inputs() already ran for this
			 * row and its work is discarded here, to be redone when the
			 * batch is reloaded.  Preparing after the probe would avoid
			 * that, but would put expression evaluation back under the
			 * stripe lock; spilled rows are the rarer case, so we accept
			 * the double evaluation.
			 */
			oldcxt = MemoryContextSwitchTo(tmpcontext->ecxt_per_tuple_memory);
			mt = ExecCopySlotMinimalTuple(tmpcontext->ecxt_outertuple);
			MemoryContextSwitchTo(oldcxt);

			sts_puttuple(aggstate->shared_spill_acc[partno], &hash, mt);
			pg_atomic_fetch_add_u64(&shstate->nspilled, 1);
			pg_atomic_fetch_add_u64(&shstate->npart_spilled[partno], 1);
			return;
		}

		oldcxt = MemoryContextSwitchTo(tmpcontext->ecxt_per_tuple_memory);
		mt = ExecCopySlotMinimalTuple(hashslot);
		MemoryContextSwitchTo(oldcxt);

		alloclen = MAXALIGN(sizeof(SharedAggEntry)) +
			MAXALIGN(aggstate->numtrans * sizeof(AggStatePerGroupData)) +
			mt->t_len;
		newentry = shared_agg_alloc_entry(aggstate, area, alloclen, &newp);
		newentry->hash = hash;
		newentry->tuplen = mt->t_len;
		memcpy(SharedAggEntryTuple(newentry, aggstate->numtrans),
			   mt, mt->t_len);

		states = SharedAggEntryStates(newentry);
		memset(states, 0,
			   aggstate->numtrans * sizeof(AggStatePerGroupData));
		for (transno = 0; transno < aggstate->numtrans; transno++)
		{
			AggStatePerTrans pertrans = &aggstate->pertrans[transno];
			AggStatePerGroup pergroupstate = &states[transno];

			if (pertrans->transtypeByVal || pertrans->initValueIsNull)
				initialize_aggregate(aggstate, pertrans, pergroupstate);
			else
			{
				/* non-NULL by-ref initcond: copy the blob into DSA */
				pergroupstate->transValue =
					(Datum) shared_agg_store_datum(aggstate, area, pertrans,
												   pertrans->initValue);
				pergroupstate->transValueIsNull = false;
				pergroupstate->noTransValue = false;
			}
		}

		/* link at chain head; all mutators hold the stripe lock */
		newentry->next = buckets[bucketno];
		buckets[bucketno] = newp;
		pg_atomic_fetch_add_u64(&shstate->nentries, 1);

		/*
		 * Entry memory is accounted at chunk granularity inside
		 * shared_agg_alloc_entry(); here just check the budget and flip
		 * into spill mode when exceeded.
		 */
		used = pg_atomic_read_u64(&shstate->mem_used);
		if (allow_spill && used > shstate->mem_limit)
			pg_atomic_write_u32(&shstate->spill_mode, 1);

		entry = newentry;
	}

	/*
	 * Apply the staged inputs, still under the stripe lock.  This is why
	 * only LWLocks are acceptable here: a transition function can
	 * ereport(ERROR), and LWLocks are released by transaction abort,
	 * whereas a spinlock would stay wedged.
	 */
	shared_agg_apply(aggstate, area, SharedAggEntryStates(entry));

	LWLockRelease(lock);
}

/*
 * Free the table's contents and zero the bucket array, preparing the
 * shared table for the next spill batch.  Runs in the one participant
 * that claimed the batch transition; nobody else touches the table
 * concurrently (they are waiting at the scan barrier).
 *
 * Entry memory is freed wholesale by walking the chunk list.  A per-entry
 * bucket-chain walk is needed only when some transition keeps its state
 * in a separately allocated blob.
 */
static void
shared_agg_reset_table(AggState *aggstate, dsa_area *area)
{
	SharedAggBuildState *shstate = aggstate->shared_build;
	dsa_pointer *buckets = (dsa_pointer *)
		dsa_get_address(area, shstate->buckets);
	dsa_pointer chunkp;
	bool		have_blobs = false;
	int			transno;
	uint64		i;

	for (transno = 0; transno < aggstate->numtrans; transno++)
		have_blobs |= aggstate->shared_transinfo[transno].blob_stored;

	if (have_blobs)
	{
		for (i = 0; i < shstate->nbuckets; i++)
		{
			dsa_pointer entryp = buckets[i];

			while (DsaPointerIsValid(entryp))
			{
				SharedAggEntry *entry = (SharedAggEntry *)
					dsa_get_address(area, entryp);
				AggStatePerGroup states = SharedAggEntryStates(entry);

				for (transno = 0; transno < aggstate->numtrans; transno++)
				{
					if (aggstate->shared_transinfo[transno].blob_stored &&
						!states[transno].transValueIsNull)
						dsa_free(area,
								 (dsa_pointer) states[transno].transValue);
				}
				entryp = entry->next;
			}
		}
	}

	/* free all entry chunks wholesale */
	chunkp = shstate->chunk_head;
	while (DsaPointerIsValid(chunkp))
	{
		SharedAggChunk *chunk = (SharedAggChunk *)
			dsa_get_address(area, chunkp);
		dsa_pointer next = chunk->next;

		dsa_free(area, chunkp);
		chunkp = next;
	}
	shstate->chunk_head = InvalidDsaPointer;

	memset(buckets, 0, shstate->nbuckets * sizeof(dsa_pointer));

	pg_atomic_write_u64(&shstate->nentries, 0);
	pg_atomic_write_u64(&shstate->scan_cursor, 0);
	pg_atomic_write_u64(&shstate->mem_used,
						shstate->nbuckets * sizeof(dsa_pointer));
}

/*
 * Re-aggregate one spill partition into the (empty) shared table.  All
 * remaining participants call this concurrently; sts_parallel_scan_next
 * hands out disjoint tuples.
 *
 * Note: batches do not re-spill.  If a single partition's groups exceed the
 * budget, the table simply grows past it (single-level partitioning, no
 * recursion) — documented prototype limitation.
 */
static void
shared_agg_refill(AggState *aggstate, dsa_area *area, int batchno)
{
	SharedTuplestoreAccessor *acc = aggstate->shared_spill_acc[batchno];
	AggStatePerHash perhash = &aggstate->perhash[0];
	ExprContext *tmpcontext = aggstate->tmpcontext;
	TupleTableSlot *spillslot = aggstate->hash_spill_rslot;
	TupleTableSlot *hashslot = perhash->hashslot;

	/* curaggcontext must point at the hash context, as during the build */
	select_current_set(aggstate, 0, true);

	sts_begin_parallel_scan(acc);
	for (;;)
	{
		MinimalTuple mt;
		uint32		hash;

		CHECK_FOR_INTERRUPTS();

		mt = sts_parallel_scan_next(acc, &hash);
		if (mt == NULL)
			break;

		ExecStoreMinimalTuple(mt, spillslot, false);
		tmpcontext->ecxt_outertuple = spillslot;

		prepare_hash_slot(perhash, spillslot, hashslot);
		shared_agg_prepare_inputs(aggstate, true);
		shared_agg_insert(aggstate, area, hashslot, hash, false);

		ResetExprContext(aggstate->shared_exprcontext);
		ResetExprContext(tmpcontext);
	}
	sts_end_parallel_scan(acc);
}

/*
 * Cooperatively build the shared hash table.
 *
 * Every participant attaches to the build barrier, one of them is elected to
 * allocate the bucket array, then all of them consume their share of the
 * (parallel-aware) outer plan, doing find-or-create-then-advance on shared
 * entries under per-stripe LWLocks.  Nobody proceeds to emission until every
 * attached participant has exhausted its input, so the table is complete and
 * immutable once this function returns.
 */
static void
agg_fill_shared_hash_table(AggState *aggstate)
{
	SharedAggBuildState *shstate = aggstate->shared_build;
	dsa_area   *area = aggstate->ss.ps.state->es_query_dsa;
	Barrier    *build_barrier;
	int			phase;
	AggStatePerHash perhash = &aggstate->perhash[0];
	ExprContext *tmpcontext = aggstate->tmpcontext;
	ExprContext *hashcxt;

	if (shstate == NULL || area == NULL)
		elog(ERROR, "parallel shared hash aggregation is not initialized");

	hashcxt = aggstate->shared_exprcontext;
	build_barrier = &shstate->build_barrier;
	phase = BarrierAttach(build_barrier);
	aggstate->shared_attached = true;

	/*
	 * Elect one participant to size and allocate the bucket array.  The
	 * table is sized once from the planner's group estimate; in this
	 * prototype it never grows, so an underestimate merely lengthens
	 * chains.  We reuse two Parallel Hash Join wait events for the
	 * allocation handshake rather than defining near-duplicates.
	 *
	 * This barrier-return election is safe only because NOTHING detaches
	 * from the build barrier before the run phase completes (late
	 * attachers bail out below without ever arriving).  A detach-driven
	 * phase advance releases waiters with no one told they arrived last,
	 * losing the election — see the batch_lock protocol in
	 * agg_retrieve_shared_hash_table() for the detach-tolerant pattern.
	 */
	if (phase == PHA_BUILD_ELECT)
	{
		if (BarrierArriveAndWait(build_barrier, WAIT_EVENT_HASH_BUILD_ELECT))
		{
			uint64		nbuckets;
			double		dgroups = perhash->aggnode->numGroups;

			nbuckets = pg_nextpower2_64((uint64) Max(dgroups / 0.75, 1024));
			nbuckets = Min(nbuckets, SHARED_AGG_MAX_BUCKETS);
			shstate->nbuckets = nbuckets;
			shstate->buckets =
				dsa_allocate_extended(area,
									  nbuckets * sizeof(dsa_pointer),
									  DSA_ALLOC_ZERO | DSA_ALLOC_HUGE);
			pg_atomic_add_fetch_u64(&shstate->mem_used,
									nbuckets * sizeof(dsa_pointer));
		}
		BarrierArriveAndWait(build_barrier, WAIT_EVENT_HASH_BUILD_ALLOCATE);
		phase = PHA_BUILD_RUN;
	}
	else if (phase == PHA_BUILD_ALLOCATE)
	{
		BarrierArriveAndWait(build_barrier, WAIT_EVENT_HASH_BUILD_ALLOCATE);
		phase = PHA_BUILD_RUN;
	}

	if (phase == PHA_BUILD_RUN)
	{
		TupleTableSlot *hashslot = perhash->hashslot;

		select_current_set(aggstate, 0, true);

		for (;;)
		{
			TupleTableSlot *outerslot;
			uint32		hash;
			bool		isnull;

			outerslot = fetch_input_tuple(aggstate);
			if (TupIsNull(outerslot))
				break;
			tmpcontext->ecxt_outertuple = outerslot;

			/* condense the grouping columns into hashslot */
			prepare_hash_slot(perhash, outerslot, hashslot);

			/* deterministic hash of the condensed key */
			hashcxt->ecxt_innertuple = hashslot;
			hash = DatumGetUInt32(ExecEvalExprSwitchContext(aggstate->shared_hashexpr,
															hashcxt,
															&isnull));

			/*
			 * Evaluate filters, arguments and fast-path rescaling BEFORE
			 * taking any lock, then do the locked find-or-create-and-apply
			 * (or spill).
			 */
			shared_agg_prepare_inputs(aggstate, false);
			shared_agg_insert(aggstate, area, hashslot, hash, true);

			ResetExprContext(hashcxt);
			ResetExprContext(tmpcontext);
		}

		/*
		 * Flush our spill writes (harmless if we never wrote), and attach
		 * to the scan barrier while every feeding participant is still
		 * co-present.  This ordering is load-bearing: scan-barrier
		 * membership is thereby fixed for all feeders before any emission
		 * begins and can only shrink afterwards (leader bow-out, batch
		 * completion), never grow — the batch-cycle waits rely on that.
		 */
		{
			int			partno;

			for (partno = 0; partno < SHARED_AGG_SPILL_PARTITIONS; partno++)
				sts_end_write(aggstate->shared_spill_acc[partno]);
		}
		BarrierAttach(&shstate->scan_barrier);
		aggstate->shared_scan_attached = true;

		/* wait for all attached participants to finish feeding the table */
		BarrierArriveAndWait(build_barrier, WAIT_EVENT_PARALLEL_HASH_AGG_BUILD);

		BarrierDetach(build_barrier);
		aggstate->shared_attached = false;

		aggstate->table_filled = true;
		aggstate->shared_scan_bucket = 0;
		aggstate->shared_scan_chunk_end = 0;
		aggstate->shared_scan_entry = InvalidDsaPointer;
		return;
	}

	/*
	 * Late attacher (phase >= PHA_BUILD_DONE): the build is already over
	 * and other participants may be arbitrarily deep into emission or
	 * batch cycles, whose barrier phases we cannot join coherently.
	 * Contribute nothing; the other participants cover all groups.
	 */
	BarrierDetach(build_barrier);
	aggstate->shared_attached = false;
	aggstate->shared_scan_attached = false;

	aggstate->table_filled = true;
	aggstate->agg_done = true;
}

/*
 * Cooperatively emit finalized groups from the completed shared hash table.
 *
 * Participants claim disjoint chunks of the bucket array via an atomic
 * fetch-add cursor, so each group is finalized and emitted by exactly one
 * participant and no leader-side merge exists at all.  The table is
 * immutable by now, so no locks are taken.
 */
static TupleTableSlot *
agg_retrieve_shared_hash_table(AggState *aggstate)
{
	ExprContext *econtext = aggstate->ss.ps.ps_ExprContext;
	AggStatePerAgg peragg = aggstate->peragg;
	TupleTableSlot *firstSlot = aggstate->ss.ss_ScanTupleSlot;
	AggStatePerHash perhash = &aggstate->perhash[0];
	SharedAggBuildState *shstate = aggstate->shared_build;
	dsa_area   *area = aggstate->ss.ps.state->es_query_dsa;
	dsa_pointer *buckets;

	if (aggstate->agg_done)
		return NULL;

	buckets = (dsa_pointer *) dsa_get_address(area, shstate->buckets);

	for (;;)
	{
		SharedAggEntry *entry;
		AggStatePerGroup states;
		TupleTableSlot *hashslot = perhash->hashslot;
		TupleTableSlot *result;
		AggStatePerGroup scratch = aggstate->shared_scratch_pergroup;
		int			i;
		int			transno;

		CHECK_FOR_INTERRUPTS();

		/* advance: current chain, then next bucket, then claim a new chunk */
		while (!DsaPointerIsValid(aggstate->shared_scan_entry))
		{
			if (aggstate->shared_scan_bucket >= aggstate->shared_scan_chunk_end)
			{
				uint64		claim;
				uint32		mybatch;
				uint32		newbatch;
				bool		do_reset;

				claim = pg_atomic_fetch_add_u64(&shstate->scan_cursor,
												SHARED_AGG_SCAN_CHUNK);
				if (claim >= shstate->nbuckets)
				{
					/*
					 * Current table content exhausted.  If nothing was
					 * spilled, we are done; detaching from the scan
					 * barrier never blocks.  (All spill writes happened
					 * before the build barrier, so nspilled is stable.)
					 */
					if (pg_atomic_read_u64(&shstate->nspilled) == 0 ||
						!aggstate->shared_scan_attached)
					{
						if (aggstate->shared_scan_attached)
						{
							BarrierDetach(&shstate->scan_barrier);
							aggstate->shared_scan_attached = false;
						}
						aggstate->agg_done = true;
						return NULL;
					}

					/*
					 * Spill batches remain.  The leader must not take part
					 * in batch cycles when workers exist: workers emit
					 * into tuple queues that only the leader drains, so a
					 * leader blocked at this barrier while a worker is
					 * blocked on a full queue would deadlock.  Bow out and
					 * go back to draining queues; the workers process all
					 * batches among themselves.  With no workers there are
					 * no tuple queues, so the leader cycles alone safely.
					 *
					 * Note this can advance the barrier phase for waiting
					 * workers without any of them "arriving last" — which
					 * is exactly why batch advancement below is claimed
					 * under batch_lock rather than derived from the
					 * barrier's return value.
					 */
					if (!IsParallelWorker() &&
						BarrierParticipants(&shstate->scan_barrier) > 1)
					{
						BarrierDetach(&shstate->scan_barrier);
						aggstate->shared_scan_attached = false;
						aggstate->agg_done = true;
						return NULL;
					}

					/*
					 * Snapshot the currently-loaded batch before waiting;
					 * it cannot change during emission, and it is the
					 * generation token for the advancement claim below.
					 *
					 * Also drop our current entry chunk: the claimant is
					 * about to free all chunks wholesale, so any local
					 * carve-out pointer would dangle.
					 */
					mybatch = shstate->cur_batch;
					aggstate->shared_alloc_chunk = InvalidDsaPointer;
					aggstate->shared_alloc_used = 0;
					aggstate->shared_alloc_size = 0;

					/* wait for everyone to finish emitting this table */
					BarrierArriveAndWait(&shstate->scan_barrier,
										 WAIT_EVENT_PARALLEL_HASH_AGG_EMIT);

					/*
					 * First through the lock finds cur_batch unchanged and
					 * claims the advancement; everyone else sees the new
					 * value and skips.  Empty partitions are skipped here
					 * in one step, avoiding pointless reset/refill cycles.
					 * The lock is held only for the decision — the O(n)
					 * table reset happens outside it, while the others sit
					 * in the interruptible barrier wait below.
					 */
					do_reset = false;
					LWLockAcquire(&shstate->batch_lock, LW_EXCLUSIVE);
					if (shstate->cur_batch == mybatch)
					{
						newbatch = mybatch + 1;
						while (newbatch <= SHARED_AGG_SPILL_PARTITIONS &&
							   pg_atomic_read_u64(&shstate->npart_spilled[newbatch - 1]) == 0)
							newbatch++;

						if (newbatch > SHARED_AGG_SPILL_PARTITIONS)
							shstate->cur_batch = PHA_BATCHES_DONE;
						else
						{
							shstate->cur_batch = newbatch;
							do_reset = true;
						}
					}
					LWLockRelease(&shstate->batch_lock);

					if (do_reset)
						shared_agg_reset_table(aggstate, area);

					/*
					 * Wait for the reset to complete.  cur_batch is stable
					 * after this point: it was written before the claimant
					 * arrived here, and the barrier orders that write
					 * before our read.
					 */
					BarrierArriveAndWait(&shstate->scan_barrier,
										 WAIT_EVENT_PARALLEL_HASH_AGG_BATCH);

					newbatch = shstate->cur_batch;
					if (newbatch == PHA_BATCHES_DONE)
					{
						BarrierDetach(&shstate->scan_barrier);
						aggstate->shared_scan_attached = false;
						aggstate->agg_done = true;
						return NULL;
					}

					/* cooperatively re-aggregate the spill partition */
					shared_agg_refill(aggstate, area, (int) (newbatch - 1));

					/* wait until the batch is fully built everywhere */
					BarrierArriveAndWait(&shstate->scan_barrier,
										 WAIT_EVENT_PARALLEL_HASH_AGG_BATCH);

					/* restart emission over the refilled table */
					aggstate->shared_scan_bucket = 0;
					aggstate->shared_scan_chunk_end = 0;
					aggstate->shared_scan_entry = InvalidDsaPointer;
					continue;
				}
				aggstate->shared_scan_bucket = claim;
				aggstate->shared_scan_chunk_end =
					Min(claim + SHARED_AGG_SCAN_CHUNK, shstate->nbuckets);
			}
			aggstate->shared_scan_entry =
				buckets[aggstate->shared_scan_bucket++];
		}

		entry = (SharedAggEntry *)
			dsa_get_address(area, aggstate->shared_scan_entry);
		aggstate->shared_scan_entry = entry->next;

		ResetExprContext(econtext);

		/*
		 * Transform the representative tuple back into one with the right
		 * columns, exactly as agg_retrieve_hash_table_in_memory() does.
		 */
		ExecStoreMinimalTuple(SharedAggEntryTuple(entry, aggstate->numtrans),
							  hashslot, false);
		slot_getallattrs(hashslot);

		ExecClearTuple(firstSlot);
		memset(firstSlot->tts_isnull, true,
			   firstSlot->tts_tupleDescriptor->natts * sizeof(bool));
		for (i = 0; i < perhash->numhashGrpCols; i++)
		{
			int			varNumber = perhash->hashGrpColIdxInput[i] - 1;

			firstSlot->tts_values[varNumber] = hashslot->tts_values[i];
			firstSlot->tts_isnull[varNumber] = hashslot->tts_isnull[i];
		}
		ExecStoreVirtualTuple(firstSlot);

		states = SharedAggEntryStates(entry);

		/*
		 * finalize_aggregates() wants a local AggStatePerGroupData array
		 * with process-local pointers.  Copy the states into scratch,
		 * translating by-ref dsa_pointers into mapped addresses; the
		 * final functions treat the state as read-only, so handing them
		 * the shared blob directly is safe.
		 */
		for (transno = 0; transno < aggstate->numtrans; transno++)
		{
			SharedAggTransInfo *info = &aggstate->shared_transinfo[transno];

			scratch[transno] = states[transno];
			if (info->blob_stored && !scratch[transno].transValueIsNull)
				scratch[transno].transValue =
					PointerGetDatum(dsa_get_address(area,
													(dsa_pointer) states[transno].transValue));
		}

		econtext->ecxt_outertuple = firstSlot;
		prepare_projection_slot(aggstate, econtext->ecxt_outertuple,
								aggstate->current_set);
		finalize_aggregates(aggstate, peragg, scratch);

		result = project_aggregates(aggstate);
		if (result)
			return result;
	}
}

/*
 * hashagg_spill_init
 *
 * Called after we determined that spilling is necessary. Chooses the number
 * of partitions to create, and initializes them.
 */
static void
hashagg_spill_init(HashAggSpill *spill, LogicalTapeSet *tapeset, int used_bits,
				   double input_groups, double hashentrysize)
{
	int			npartitions;
	int			partition_bits;

	npartitions = hash_choose_num_partitions(input_groups, hashentrysize,
											 used_bits, &partition_bits);

#ifdef USE_INJECTION_POINTS
	if (IS_INJECTION_POINT_ATTACHED("hash-aggregate-single-partition"))
	{
		npartitions = 1;
		partition_bits = 0;
		INJECTION_POINT_CACHED("hash-aggregate-single-partition", NULL);
	}
#endif

	spill->partitions = palloc0_array(LogicalTape *, npartitions);
	spill->ntuples = palloc0_array(int64, npartitions);
	spill->hll_card = palloc0_array(hyperLogLogState, npartitions);

	for (int i = 0; i < npartitions; i++)
		spill->partitions[i] = LogicalTapeCreate(tapeset);

	spill->shift = 32 - used_bits - partition_bits;
	if (spill->shift < 32)
		spill->mask = (npartitions - 1) << spill->shift;
	else
		spill->mask = 0;
	spill->npartitions = npartitions;

	for (int i = 0; i < npartitions; i++)
		initHyperLogLog(&spill->hll_card[i], HASHAGG_HLL_BIT_WIDTH);
}

/*
 * hashagg_spill_tuple
 *
 * No room for new groups in the hash table. Save for later in the appropriate
 * partition.
 */
static Size
hashagg_spill_tuple(AggState *aggstate, HashAggSpill *spill,
					TupleTableSlot *inputslot, uint32 hash)
{
	TupleTableSlot *spillslot;
	int			partition;
	MinimalTuple tuple;
	LogicalTape *tape;
	int			total_written = 0;
	bool		shouldFree;

	Assert(spill->partitions != NULL);

	/* spill only attributes that we actually need */
	if (!aggstate->all_cols_needed)
	{
		spillslot = aggstate->hash_spill_wslot;
		slot_getsomeattrs(inputslot, aggstate->max_colno_needed);
		ExecClearTuple(spillslot);
		for (int i = 0; i < spillslot->tts_tupleDescriptor->natts; i++)
		{
			if (bms_is_member(i + 1, aggstate->colnos_needed))
			{
				spillslot->tts_values[i] = inputslot->tts_values[i];
				spillslot->tts_isnull[i] = inputslot->tts_isnull[i];
			}
			else
				spillslot->tts_isnull[i] = true;
		}
		ExecStoreVirtualTuple(spillslot);
	}
	else
		spillslot = inputslot;

	tuple = ExecFetchSlotMinimalTuple(spillslot, &shouldFree);

	if (spill->shift < 32)
		partition = (hash & spill->mask) >> spill->shift;
	else
		partition = 0;

	spill->ntuples[partition]++;

	/*
	 * All hash values destined for a given partition have some bits in
	 * common, which causes bad HLL cardinality estimates. Hash the hash to
	 * get a more uniform distribution.
	 */
	addHyperLogLog(&spill->hll_card[partition], hash_bytes_uint32(hash));

	tape = spill->partitions[partition];

	LogicalTapeWrite(tape, &hash, sizeof(uint32));
	total_written += sizeof(uint32);

	LogicalTapeWrite(tape, tuple, tuple->t_len);
	total_written += tuple->t_len;

	if (shouldFree)
		pfree(tuple);

	return total_written;
}

/*
 * hashagg_batch_new
 *
 * Construct a HashAggBatch item, which represents one iteration of HashAgg to
 * be done.
 */
static HashAggBatch *
hashagg_batch_new(LogicalTape *input_tape, int setno,
				  int64 input_tuples, double input_card, int used_bits)
{
	HashAggBatch *batch = palloc0_object(HashAggBatch);

	batch->setno = setno;
	batch->used_bits = used_bits;
	batch->input_tape = input_tape;
	batch->input_tuples = input_tuples;
	batch->input_card = input_card;

	return batch;
}

/*
 * hashagg_batch_read
 * 		read the next tuple from a batch's tape.  Return NULL if no more.
 */
static MinimalTuple
hashagg_batch_read(HashAggBatch *batch, uint32 *hashp)
{
	LogicalTape *tape = batch->input_tape;
	MinimalTuple tuple;
	uint32		t_len;
	size_t		nread;
	uint32		hash;

	nread = LogicalTapeRead(tape, &hash, sizeof(uint32));
	if (nread == 0)
		return NULL;
	if (nread != sizeof(uint32))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg_internal("unexpected EOF for tape %p: requested %zu bytes, read %zu bytes",
								 tape, sizeof(uint32), nread)));
	if (hashp != NULL)
		*hashp = hash;

	nread = LogicalTapeRead(tape, &t_len, sizeof(t_len));
	if (nread != sizeof(uint32))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg_internal("unexpected EOF for tape %p: requested %zu bytes, read %zu bytes",
								 tape, sizeof(uint32), nread)));

	tuple = (MinimalTuple) palloc(t_len);
	tuple->t_len = t_len;

	nread = LogicalTapeRead(tape,
							(char *) tuple + sizeof(uint32),
							t_len - sizeof(uint32));
	if (nread != t_len - sizeof(uint32))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg_internal("unexpected EOF for tape %p: requested %zu bytes, read %zu bytes",
								 tape, t_len - sizeof(uint32), nread)));

	return tuple;
}

/*
 * hashagg_finish_initial_spills
 *
 * After a HashAggBatch has been processed, it may have spilled tuples to
 * disk. If so, turn the spilled partitions into new batches that must later
 * be executed.
 */
static void
hashagg_finish_initial_spills(AggState *aggstate)
{
	int			setno;
	int			total_npartitions = 0;

	if (aggstate->hash_spills != NULL)
	{
		for (setno = 0; setno < aggstate->num_hashes; setno++)
		{
			HashAggSpill *spill = &aggstate->hash_spills[setno];

			total_npartitions += spill->npartitions;
			hashagg_spill_finish(aggstate, spill, setno);
		}

		/*
		 * We're not processing tuples from outer plan any more; only
		 * processing batches of spilled tuples. The initial spill structures
		 * are no longer needed.
		 */
		pfree(aggstate->hash_spills);
		aggstate->hash_spills = NULL;
	}

	hash_agg_update_metrics(aggstate, false, total_npartitions);
	aggstate->hash_spill_mode = false;
}

/*
 * hashagg_spill_finish
 *
 * Transform spill partitions into new batches.
 */
static void
hashagg_spill_finish(AggState *aggstate, HashAggSpill *spill, int setno)
{
	int			i;
	int			used_bits = 32 - spill->shift;

	if (spill->npartitions == 0)
		return;					/* didn't spill */

	for (i = 0; i < spill->npartitions; i++)
	{
		LogicalTape *tape = spill->partitions[i];
		HashAggBatch *new_batch;
		double		cardinality;

		/* if the partition is empty, don't create a new batch of work */
		if (spill->ntuples[i] == 0)
			continue;

		cardinality = estimateHyperLogLog(&spill->hll_card[i]);
		freeHyperLogLog(&spill->hll_card[i]);

		/* rewinding frees the buffer while not in use */
		LogicalTapeRewindForRead(tape, HASHAGG_READ_BUFFER_SIZE);

		new_batch = hashagg_batch_new(tape, setno,
									  spill->ntuples[i], cardinality,
									  used_bits);
		aggstate->hash_batches = lappend(aggstate->hash_batches, new_batch);
		aggstate->hash_batches_used++;
	}

	pfree(spill->ntuples);
	pfree(spill->hll_card);
	pfree(spill->partitions);
}

/*
 * Free resources related to a spilled HashAgg.
 */
static void
hashagg_reset_spill_state(AggState *aggstate)
{
	/* free spills from initial pass */
	if (aggstate->hash_spills != NULL)
	{
		int			setno;

		for (setno = 0; setno < aggstate->num_hashes; setno++)
		{
			HashAggSpill *spill = &aggstate->hash_spills[setno];

			pfree(spill->ntuples);
			pfree(spill->partitions);
		}
		pfree(aggstate->hash_spills);
		aggstate->hash_spills = NULL;
	}

	/* free batches */
	list_free_deep(aggstate->hash_batches);
	aggstate->hash_batches = NIL;

	/* close tape set */
	if (aggstate->hash_tapeset != NULL)
	{
		LogicalTapeSetClose(aggstate->hash_tapeset);
		aggstate->hash_tapeset = NULL;
	}
}


/* -----------------
 * ExecInitAgg
 *
 *	Creates the run-time information for the agg node produced by the
 *	planner and initializes its outer subtree.
 *
 * -----------------
 */
AggState *
ExecInitAgg(Agg *node, EState *estate, int eflags)
{
	AggState   *aggstate;
	AggStatePerAgg peraggs;
	AggStatePerTrans pertransstates;
	AggStatePerGroup *pergroups;
	Plan	   *outerPlan;
	ExprContext *econtext;
	TupleDesc	scanDesc;
	int			max_aggno;
	int			max_transno;
	int			numaggrefs;
	int			numaggs;
	int			numtrans;
	int			phase;
	int			phaseidx;
	ListCell   *l;
	Bitmapset  *all_grouped_cols = NULL;
	int			numGroupingSets = 1;
	int			numPhases;
	int			numHashes;
	int			i = 0;
	int			j = 0;
	bool		use_hashing = (node->aggstrategy == AGG_HASHED ||
							   node->aggstrategy == AGG_MIXED);

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	/*
	 * create state structure
	 */
	aggstate = makeNode(AggState);
	aggstate->ss.ps.plan = (Plan *) node;
	aggstate->ss.ps.state = estate;
	aggstate->ss.ps.ExecProcNode = ExecAgg;

	aggstate->aggs = NIL;
	aggstate->numaggs = 0;
	aggstate->numtrans = 0;
	aggstate->aggstrategy = node->aggstrategy;
	aggstate->aggsplit = node->aggsplit;
	aggstate->maxsets = 0;
	aggstate->projected_set = -1;
	aggstate->current_set = 0;
	aggstate->peragg = NULL;
	aggstate->pertrans = NULL;
	aggstate->curperagg = NULL;
	aggstate->curpertrans = NULL;
	aggstate->input_done = false;
	aggstate->agg_done = false;
	aggstate->pergroups = NULL;
	aggstate->grp_firstTuple = NULL;
	aggstate->sort_in = NULL;
	aggstate->sort_out = NULL;

	/*
	 * phases[0] always exists, but is dummy in sorted/plain mode
	 */
	numPhases = (use_hashing ? 1 : 2);
	numHashes = (use_hashing ? 1 : 0);

	/*
	 * Calculate the maximum number of grouping sets in any phase; this
	 * determines the size of some allocations.  Also calculate the number of
	 * phases, since all hashed/mixed nodes contribute to only a single phase.
	 */
	if (node->groupingSets)
	{
		numGroupingSets = list_length(node->groupingSets);

		foreach(l, node->chain)
		{
			Agg		   *agg = lfirst(l);

			numGroupingSets = Max(numGroupingSets,
								  list_length(agg->groupingSets));

			/*
			 * additional AGG_HASHED aggs become part of phase 0, but all
			 * others add an extra phase.
			 */
			if (agg->aggstrategy != AGG_HASHED)
				++numPhases;
			else
				++numHashes;
		}
	}

	aggstate->maxsets = numGroupingSets;
	aggstate->numphases = numPhases;

	aggstate->aggcontexts = palloc0_array(ExprContext *, numGroupingSets);

	/*
	 * Create expression contexts.  We need three or more, one for
	 * per-input-tuple processing, one for per-output-tuple processing, one
	 * for all the hashtables, and one for each grouping set.  The per-tuple
	 * memory context of the per-grouping-set ExprContexts (aggcontexts)
	 * replaces the standalone memory context formerly used to hold transition
	 * values.  We cheat a little by using ExecAssignExprContext() to build
	 * all of them.
	 *
	 * NOTE: the details of what is stored in aggcontexts and what is stored
	 * in the regular per-query memory context are driven by a simple
	 * decision: we want to reset the aggcontext at group boundaries (if not
	 * hashing) and in ExecReScanAgg to recover no-longer-wanted space.
	 */
	ExecAssignExprContext(estate, &aggstate->ss.ps);
	aggstate->tmpcontext = aggstate->ss.ps.ps_ExprContext;

	for (i = 0; i < numGroupingSets; ++i)
	{
		ExecAssignExprContext(estate, &aggstate->ss.ps);
		aggstate->aggcontexts[i] = aggstate->ss.ps.ps_ExprContext;
	}

	if (use_hashing)
		hash_create_memory(aggstate);

	ExecAssignExprContext(estate, &aggstate->ss.ps);

	/*
	 * Initialize child nodes.
	 *
	 * If we are doing a hashed aggregation then the child plan does not need
	 * to handle REWIND efficiently; see ExecReScanAgg.
	 */
	if (node->aggstrategy == AGG_HASHED)
		eflags &= ~EXEC_FLAG_REWIND;
	outerPlan = outerPlan(node);
	outerPlanState(aggstate) = ExecInitNode(outerPlan, estate, eflags);

	/*
	 * initialize source tuple type.
	 */
	aggstate->ss.ps.outerops =
		ExecGetResultSlotOps(outerPlanState(&aggstate->ss),
							 &aggstate->ss.ps.outeropsfixed);
	aggstate->ss.ps.outeropsset = true;

	ExecCreateScanSlotFromOuterPlan(estate, &aggstate->ss,
									aggstate->ss.ps.outerops);
	scanDesc = aggstate->ss.ss_ScanTupleSlot->tts_tupleDescriptor;

	/*
	 * If there are more than two phases (including a potential dummy phase
	 * 0), input will be resorted using tuplesort. Need a slot for that.
	 */
	if (numPhases > 2)
	{
		aggstate->sort_slot = ExecInitExtraTupleSlot(estate, scanDesc,
													 &TTSOpsMinimalTuple);

		/*
		 * The output of the tuplesort, and the output from the outer child
		 * might not use the same type of slot. In most cases the child will
		 * be a Sort, and thus return a TTSOpsMinimalTuple type slot - but the
		 * input can also be presorted due an index, in which case it could be
		 * a different type of slot.
		 *
		 * XXX: For efficiency it would be good to instead/additionally
		 * generate expressions with corresponding settings of outerops* for
		 * the individual phases - deforming is often a bottleneck for
		 * aggregations with lots of rows per group. If there's multiple
		 * sorts, we know that all but the first use TTSOpsMinimalTuple (via
		 * the nodeAgg.c internal tuplesort).
		 */
		if (aggstate->ss.ps.outeropsfixed &&
			aggstate->ss.ps.outerops != &TTSOpsMinimalTuple)
			aggstate->ss.ps.outeropsfixed = false;
	}

	/*
	 * Initialize result type, slot and projection.
	 */
	ExecInitResultTupleSlotTL(&aggstate->ss.ps, &TTSOpsVirtual);
	ExecAssignProjectionInfo(&aggstate->ss.ps, NULL);

	/*
	 * initialize child expressions
	 *
	 * We expect the parser to have checked that no aggs contain other agg
	 * calls in their arguments (and just to be sure, we verify it again while
	 * initializing the plan node).  This would make no sense under SQL
	 * semantics, and it's forbidden by the spec.  Because it is true, we
	 * don't need to worry about evaluating the aggs in any particular order.
	 *
	 * Note: execExpr.c finds Aggrefs for us, and adds them to aggstate->aggs.
	 * Aggrefs in the qual are found here; Aggrefs in the targetlist are found
	 * during ExecAssignProjectionInfo, above.
	 */
	aggstate->ss.ps.qual =
		ExecInitQual(node->plan.qual, (PlanState *) aggstate);

	/*
	 * We should now have found all Aggrefs in the targetlist and quals.
	 */
	numaggrefs = list_length(aggstate->aggs);
	max_aggno = -1;
	max_transno = -1;
	foreach(l, aggstate->aggs)
	{
		Aggref	   *aggref = (Aggref *) lfirst(l);

		max_aggno = Max(max_aggno, aggref->aggno);
		max_transno = Max(max_transno, aggref->aggtransno);
	}
	aggstate->numaggs = numaggs = max_aggno + 1;
	aggstate->numtrans = numtrans = max_transno + 1;

	/*
	 * For each phase, prepare grouping set data and fmgr lookup data for
	 * compare functions.  Accumulate all_grouped_cols in passing.
	 */
	aggstate->phases = palloc0_array(AggStatePerPhaseData, numPhases);

	aggstate->num_hashes = numHashes;
	if (numHashes)
	{
		aggstate->perhash = palloc0_array(AggStatePerHashData, numHashes);
		aggstate->phases[0].numsets = 0;
		aggstate->phases[0].gset_lengths = palloc_array(int, numHashes);
		aggstate->phases[0].grouped_cols = palloc_array(Bitmapset *, numHashes);
	}

	phase = 0;
	for (phaseidx = 0; phaseidx <= list_length(node->chain); ++phaseidx)
	{
		Agg		   *aggnode;
		Sort	   *sortnode;

		if (phaseidx > 0)
		{
			aggnode = list_nth_node(Agg, node->chain, phaseidx - 1);
			sortnode = castNode(Sort, outerPlan(aggnode));
		}
		else
		{
			aggnode = node;
			sortnode = NULL;
		}

		Assert(phase <= 1 || sortnode);

		if (aggnode->aggstrategy == AGG_HASHED
			|| aggnode->aggstrategy == AGG_MIXED)
		{
			AggStatePerPhase phasedata = &aggstate->phases[0];
			AggStatePerHash perhash;
			Bitmapset  *cols = NULL;

			Assert(phase == 0);
			i = phasedata->numsets++;
			perhash = &aggstate->perhash[i];

			/* phase 0 always points to the "real" Agg in the hash case */
			phasedata->aggnode = node;
			phasedata->aggstrategy = node->aggstrategy;

			/* but the actual Agg node representing this hash is saved here */
			perhash->aggnode = aggnode;

			phasedata->gset_lengths[i] = perhash->numCols = aggnode->numCols;

			for (j = 0; j < aggnode->numCols; ++j)
				cols = bms_add_member(cols, aggnode->grpColIdx[j]);

			phasedata->grouped_cols[i] = cols;

			all_grouped_cols = bms_add_members(all_grouped_cols, cols);
			continue;
		}
		else
		{
			AggStatePerPhase phasedata = &aggstate->phases[++phase];
			int			num_sets;

			phasedata->numsets = num_sets = list_length(aggnode->groupingSets);

			if (num_sets)
			{
				phasedata->gset_lengths = palloc(num_sets * sizeof(int));
				phasedata->grouped_cols = palloc(num_sets * sizeof(Bitmapset *));

				i = 0;
				foreach(l, aggnode->groupingSets)
				{
					int			current_length = list_length(lfirst(l));
					Bitmapset  *cols = NULL;

					/* planner forces this to be correct */
					for (j = 0; j < current_length; ++j)
						cols = bms_add_member(cols, aggnode->grpColIdx[j]);

					phasedata->grouped_cols[i] = cols;
					phasedata->gset_lengths[i] = current_length;

					++i;
				}

				all_grouped_cols = bms_add_members(all_grouped_cols,
												   phasedata->grouped_cols[0]);
			}
			else
			{
				Assert(phaseidx == 0);

				phasedata->gset_lengths = NULL;
				phasedata->grouped_cols = NULL;
			}

			/*
			 * If we are grouping, precompute fmgr lookup data for inner loop.
			 */
			if (aggnode->aggstrategy == AGG_SORTED)
			{
				/*
				 * Build a separate function for each subset of columns that
				 * need to be compared.
				 */
				phasedata->eqfunctions = palloc0_array(ExprState *, aggnode->numCols);

				/* for each grouping set */
				for (int k = 0; k < phasedata->numsets; k++)
				{
					int			length = phasedata->gset_lengths[k];

					/* nothing to do for empty grouping set */
					if (length == 0)
						continue;

					/* if we already had one of this length, it'll do */
					if (phasedata->eqfunctions[length - 1] != NULL)
						continue;

					phasedata->eqfunctions[length - 1] =
						execTuplesMatchPrepare(scanDesc,
											   length,
											   aggnode->grpColIdx,
											   aggnode->grpOperators,
											   aggnode->grpCollations,
											   (PlanState *) aggstate);
				}

				/* and for all grouped columns, unless already computed */
				if (aggnode->numCols > 0 &&
					phasedata->eqfunctions[aggnode->numCols - 1] == NULL)
				{
					phasedata->eqfunctions[aggnode->numCols - 1] =
						execTuplesMatchPrepare(scanDesc,
											   aggnode->numCols,
											   aggnode->grpColIdx,
											   aggnode->grpOperators,
											   aggnode->grpCollations,
											   (PlanState *) aggstate);
				}
			}

			phasedata->aggnode = aggnode;
			phasedata->aggstrategy = aggnode->aggstrategy;
			phasedata->sortnode = sortnode;
		}
	}

	/*
	 * Convert all_grouped_cols to a descending-order list.
	 */
	i = -1;
	while ((i = bms_next_member(all_grouped_cols, i)) >= 0)
		aggstate->all_grouped_cols = lcons_int(i, aggstate->all_grouped_cols);

	/*
	 * Set up aggregate-result storage in the output expr context, and also
	 * allocate my private per-agg working storage
	 */
	econtext = aggstate->ss.ps.ps_ExprContext;
	econtext->ecxt_aggvalues = palloc0_array(Datum, numaggs);
	econtext->ecxt_aggnulls = palloc0_array(bool, numaggs);

	peraggs = palloc0_array(AggStatePerAggData, numaggs);
	pertransstates = palloc0_array(AggStatePerTransData, numtrans);

	aggstate->peragg = peraggs;
	aggstate->pertrans = pertransstates;


	aggstate->all_pergroups = palloc0_array(AggStatePerGroup, numGroupingSets + numHashes);
	pergroups = aggstate->all_pergroups;

	if (node->aggstrategy != AGG_HASHED)
	{
		for (i = 0; i < numGroupingSets; i++)
		{
			pergroups[i] = palloc0_array(AggStatePerGroupData, numaggs);
		}

		aggstate->pergroups = pergroups;
		pergroups += numGroupingSets;
	}

	/*
	 * Hashing can only appear in the initial phase.
	 */
	if (use_hashing)
	{
		Plan	   *outerplan = outerPlan(node);
		double		totalGroups = 0;

		aggstate->hash_spill_rslot = ExecInitExtraTupleSlot(estate, scanDesc,
															&TTSOpsMinimalTuple);
		aggstate->hash_spill_wslot = ExecInitExtraTupleSlot(estate, scanDesc,
															&TTSOpsVirtual);

		/* this is an array of pointers, not structures */
		aggstate->hash_pergroup = pergroups;

		aggstate->hashentrysize = hash_agg_entry_size(aggstate->numtrans,
													  outerplan->plan_width,
													  node->transitionSpace);

		/*
		 * Consider all of the grouping sets together when setting the limits
		 * and estimating the number of partitions. This can be inaccurate
		 * when there is more than one grouping set, but should still be
		 * reasonable.
		 */
		for (int k = 0; k < aggstate->num_hashes; k++)
			totalGroups += aggstate->perhash[k].aggnode->numGroups;

		hash_agg_set_limits(aggstate->hashentrysize, totalGroups, 0,
							&aggstate->hash_mem_limit,
							&aggstate->hash_ngroups_limit,
							&aggstate->hash_planned_partitions);
		find_hash_columns(aggstate);

		/*
		 * A parallel-aware hashed Agg cooperates on one hash table in shared
		 * memory instead of building a private one.  Its executor-local
		 * support is set up at the end of this function, once the pertrans
		 * array has actually been filled in.
		 */
		aggstate->shared_mode = (node->plan.parallel_aware &&
								 node->aggstrategy == AGG_HASHED);

		/* Skip massive memory allocation if we are just doing EXPLAIN */
		if (!(eflags & EXEC_FLAG_EXPLAIN_ONLY) && !aggstate->shared_mode)
			build_hash_tables(aggstate);

		aggstate->table_filled = false;

		/* Initialize this to 1, meaning nothing spilled, yet */
		aggstate->hash_batches_used = 1;
	}

	/*
	 * Initialize current phase-dependent values to initial phase. The initial
	 * phase is 1 (first sort pass) for all strategies that use sorting (if
	 * hashing is being done too, then phase 0 is processed last); but if only
	 * hashing is being done, then phase 0 is all there is.
	 */
	if (node->aggstrategy == AGG_HASHED)
	{
		aggstate->current_phase = 0;
		initialize_phase(aggstate, 0);
		select_current_set(aggstate, 0, true);
	}
	else
	{
		aggstate->current_phase = 1;
		initialize_phase(aggstate, 1);
		select_current_set(aggstate, 0, false);
	}

	/*
	 * Perform lookups of aggregate function info, and initialize the
	 * unchanging fields of the per-agg and per-trans data.
	 */
	foreach(l, aggstate->aggs)
	{
		Aggref	   *aggref = lfirst(l);
		AggStatePerAgg peragg;
		AggStatePerTrans pertrans;
		Oid			aggTransFnInputTypes[FUNC_MAX_ARGS];
		int			numAggTransFnArgs;
		int			numDirectArgs;
		HeapTuple	aggTuple;
		Form_pg_aggregate aggform;
		AclResult	aclresult;
		Oid			finalfn_oid;
		Oid			serialfn_oid,
					deserialfn_oid;
		Oid			aggOwner;
		Expr	   *finalfnexpr;
		Oid			aggtranstype;

		/* Planner should have assigned aggregate to correct level */
		Assert(aggref->agglevelsup == 0);
		/* ... and the split mode should match */
		Assert(aggref->aggsplit == aggstate->aggsplit);

		peragg = &peraggs[aggref->aggno];

		/* Check if we initialized the state for this aggregate already. */
		if (peragg->aggref != NULL)
			continue;

		peragg->aggref = aggref;
		peragg->transno = aggref->aggtransno;

		/* Fetch the pg_aggregate row */
		aggTuple = SearchSysCache1(AGGFNOID,
								   ObjectIdGetDatum(aggref->aggfnoid));
		if (!HeapTupleIsValid(aggTuple))
			elog(ERROR, "cache lookup failed for aggregate %u",
				 aggref->aggfnoid);
		aggform = (Form_pg_aggregate) GETSTRUCT(aggTuple);

		/* Check permission to call aggregate function */
		aclresult = object_aclcheck(ProcedureRelationId, aggref->aggfnoid, GetUserId(),
									ACL_EXECUTE);
		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult, OBJECT_AGGREGATE,
						   get_func_name(aggref->aggfnoid));
		InvokeFunctionExecuteHook(aggref->aggfnoid);

		/* planner recorded transition state type in the Aggref itself */
		aggtranstype = aggref->aggtranstype;
		Assert(OidIsValid(aggtranstype));

		/* Final function only required if we're finalizing the aggregates */
		if (DO_AGGSPLIT_SKIPFINAL(aggstate->aggsplit))
			peragg->finalfn_oid = finalfn_oid = InvalidOid;
		else
			peragg->finalfn_oid = finalfn_oid = aggform->aggfinalfn;

		serialfn_oid = InvalidOid;
		deserialfn_oid = InvalidOid;

		/*
		 * Check if serialization/deserialization is required.  We only do it
		 * for aggregates that have transtype INTERNAL.
		 */
		if (aggtranstype == INTERNALOID)
		{
			/*
			 * The planner should only have generated a serialize agg node if
			 * every aggregate with an INTERNAL state has a serialization
			 * function.  Verify that.
			 */
			if (DO_AGGSPLIT_SERIALIZE(aggstate->aggsplit))
			{
				/* serialization only valid when not running finalfn */
				Assert(DO_AGGSPLIT_SKIPFINAL(aggstate->aggsplit));

				if (!OidIsValid(aggform->aggserialfn))
					elog(ERROR, "serialfunc not provided for serialization aggregation");
				serialfn_oid = aggform->aggserialfn;
			}

			/* Likewise for deserialization functions */
			if (DO_AGGSPLIT_DESERIALIZE(aggstate->aggsplit))
			{
				/* deserialization only valid when combining states */
				Assert(DO_AGGSPLIT_COMBINE(aggstate->aggsplit));

				if (!OidIsValid(aggform->aggdeserialfn))
					elog(ERROR, "deserialfunc not provided for deserialization aggregation");
				deserialfn_oid = aggform->aggdeserialfn;
			}

		}

		/* Check that aggregate owner has permission to call component fns */
		{
			HeapTuple	procTuple;

			procTuple = SearchSysCache1(PROCOID,
										ObjectIdGetDatum(aggref->aggfnoid));
			if (!HeapTupleIsValid(procTuple))
				elog(ERROR, "cache lookup failed for function %u",
					 aggref->aggfnoid);
			aggOwner = ((Form_pg_proc) GETSTRUCT(procTuple))->proowner;
			ReleaseSysCache(procTuple);

			if (OidIsValid(finalfn_oid))
			{
				aclresult = object_aclcheck(ProcedureRelationId, finalfn_oid, aggOwner,
											ACL_EXECUTE);
				if (aclresult != ACLCHECK_OK)
					aclcheck_error(aclresult, OBJECT_FUNCTION,
								   get_func_name(finalfn_oid));
				InvokeFunctionExecuteHook(finalfn_oid);
			}
			if (OidIsValid(serialfn_oid))
			{
				aclresult = object_aclcheck(ProcedureRelationId, serialfn_oid, aggOwner,
											ACL_EXECUTE);
				if (aclresult != ACLCHECK_OK)
					aclcheck_error(aclresult, OBJECT_FUNCTION,
								   get_func_name(serialfn_oid));
				InvokeFunctionExecuteHook(serialfn_oid);
			}
			if (OidIsValid(deserialfn_oid))
			{
				aclresult = object_aclcheck(ProcedureRelationId, deserialfn_oid, aggOwner,
											ACL_EXECUTE);
				if (aclresult != ACLCHECK_OK)
					aclcheck_error(aclresult, OBJECT_FUNCTION,
								   get_func_name(deserialfn_oid));
				InvokeFunctionExecuteHook(deserialfn_oid);
			}
		}

		/*
		 * Get actual datatypes of the (nominal) aggregate inputs.  These
		 * could be different from the agg's declared input types, when the
		 * agg accepts ANY or a polymorphic type.
		 */
		numAggTransFnArgs = get_aggregate_argtypes(aggref,
												   aggTransFnInputTypes);

		/* Count the "direct" arguments, if any */
		numDirectArgs = list_length(aggref->aggdirectargs);

		/* Detect how many arguments to pass to the finalfn */
		if (aggform->aggfinalextra)
			peragg->numFinalArgs = numAggTransFnArgs + 1;
		else
			peragg->numFinalArgs = numDirectArgs + 1;

		/* Initialize any direct-argument expressions */
		peragg->aggdirectargs = ExecInitExprList(aggref->aggdirectargs,
												 (PlanState *) aggstate);

		/*
		 * build expression trees using actual argument & result types for the
		 * finalfn, if it exists and is required.
		 */
		if (OidIsValid(finalfn_oid))
		{
			build_aggregate_finalfn_expr(aggTransFnInputTypes,
										 peragg->numFinalArgs,
										 aggtranstype,
										 aggref->aggtype,
										 aggref->inputcollid,
										 finalfn_oid,
										 &finalfnexpr);
			fmgr_info(finalfn_oid, &peragg->finalfn);
			fmgr_info_set_expr((Node *) finalfnexpr, &peragg->finalfn);
		}

		/* get info about the output value's datatype */
		get_typlenbyval(aggref->aggtype,
						&peragg->resulttypeLen,
						&peragg->resulttypeByVal);

		/*
		 * Build working state for invoking the transition function, if we
		 * haven't done it already.
		 */
		pertrans = &pertransstates[aggref->aggtransno];
		if (pertrans->aggref == NULL)
		{
			Datum		textInitVal;
			Datum		initValue;
			bool		initValueIsNull;
			Oid			transfn_oid;

			/*
			 * If this aggregation is performing state combines, then instead
			 * of using the transition function, we'll use the combine
			 * function.
			 */
			if (DO_AGGSPLIT_COMBINE(aggstate->aggsplit))
			{
				transfn_oid = aggform->aggcombinefn;

				/* If not set then the planner messed up */
				if (!OidIsValid(transfn_oid))
					elog(ERROR, "combinefn not set for aggregate function");
			}
			else
				transfn_oid = aggform->aggtransfn;

			aclresult = object_aclcheck(ProcedureRelationId, transfn_oid, aggOwner, ACL_EXECUTE);
			if (aclresult != ACLCHECK_OK)
				aclcheck_error(aclresult, OBJECT_FUNCTION,
							   get_func_name(transfn_oid));
			InvokeFunctionExecuteHook(transfn_oid);

			/*
			 * initval is potentially null, so don't try to access it as a
			 * struct field. Must do it the hard way with SysCacheGetAttr.
			 */
			textInitVal = SysCacheGetAttr(AGGFNOID, aggTuple,
										  Anum_pg_aggregate_agginitval,
										  &initValueIsNull);
			if (initValueIsNull)
				initValue = (Datum) 0;
			else
				initValue = GetAggInitVal(textInitVal, aggtranstype);

			if (DO_AGGSPLIT_COMBINE(aggstate->aggsplit))
			{
				Oid			combineFnInputTypes[] = {aggtranstype,
				aggtranstype};

				/*
				 * When combining there's only one input, the to-be-combined
				 * transition value.  The transition value is not counted
				 * here.
				 */
				pertrans->numTransInputs = 1;

				/* aggcombinefn always has two arguments of aggtranstype */
				build_pertrans_for_aggref(pertrans, aggstate, estate,
										  aggref, transfn_oid, aggtranstype,
										  serialfn_oid, deserialfn_oid,
										  initValue, initValueIsNull,
										  combineFnInputTypes, 2);

				/*
				 * Ensure that a combine function to combine INTERNAL states
				 * is not strict. This should have been checked during CREATE
				 * AGGREGATE, but the strict property could have been changed
				 * since then.
				 */
				if (pertrans->transfn.fn_strict && aggtranstype == INTERNALOID)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
							 errmsg("combine function with transition type %s must not be declared STRICT",
									format_type_be(aggtranstype))));
			}
			else
			{
				/* Detect how many arguments to pass to the transfn */
				if (AGGKIND_IS_ORDERED_SET(aggref->aggkind))
					pertrans->numTransInputs = list_length(aggref->args);
				else
					pertrans->numTransInputs = numAggTransFnArgs;

				build_pertrans_for_aggref(pertrans, aggstate, estate,
										  aggref, transfn_oid, aggtranstype,
										  serialfn_oid, deserialfn_oid,
										  initValue, initValueIsNull,
										  aggTransFnInputTypes,
										  numAggTransFnArgs);

				/*
				 * If the transfn is strict and the initval is NULL, make sure
				 * input type and transtype are the same (or at least
				 * binary-compatible), so that it's OK to use the first
				 * aggregated input value as the initial transValue.  This
				 * should have been checked at agg definition time, but we
				 * must check again in case the transfn's strictness property
				 * has been changed.
				 */
				if (pertrans->transfn.fn_strict && pertrans->initValueIsNull)
				{
					if (numAggTransFnArgs <= numDirectArgs ||
						!IsBinaryCoercible(aggTransFnInputTypes[numDirectArgs],
										   aggtranstype))
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
								 errmsg("aggregate %u needs to have compatible input type and transition type",
										aggref->aggfnoid)));
				}
			}
		}
		else
			pertrans->aggshared = true;
		ReleaseSysCache(aggTuple);
	}

	/*
	 * Last, check whether any more aggregates got added onto the node while
	 * we processed the expressions for the aggregate arguments (including not
	 * only the regular arguments and FILTER expressions handled immediately
	 * above, but any direct arguments we might've handled earlier).  If so,
	 * we have nested aggregate functions, which is semantically nonsensical,
	 * so complain.  (This should have been caught by the parser, so we don't
	 * need to work hard on a helpful error message; but we defend against it
	 * here anyway, just to be sure.)
	 */
	if (numaggrefs != list_length(aggstate->aggs))
		ereport(ERROR,
				(errcode(ERRCODE_GROUPING_ERROR),
				 errmsg("aggregate function calls cannot be nested")));

	/*
	 * Build expressions doing all the transition work at once. We build a
	 * different one for each phase, as the number of transition function
	 * invocation can differ between phases. Note this'll work both for
	 * transition and combination functions (although there'll only be one
	 * phase in the latter case).
	 */
	for (phaseidx = 0; phaseidx < aggstate->numphases; phaseidx++)
	{
		AggStatePerPhase phase = &aggstate->phases[phaseidx];
		bool		dohash = false;
		bool		dosort = false;

		/* phase 0 doesn't necessarily exist */
		if (!phase->aggnode)
			continue;

		if (aggstate->aggstrategy == AGG_MIXED && phaseidx == 1)
		{
			/*
			 * Phase one, and only phase one, in a mixed agg performs both
			 * sorting and aggregation.
			 */
			dohash = true;
			dosort = true;
		}
		else if (aggstate->aggstrategy == AGG_MIXED && phaseidx == 0)
		{
			/*
			 * No need to compute a transition function for an AGG_MIXED phase
			 * 0 - the contents of the hashtables will have been computed
			 * during phase 1.
			 */
			continue;
		}
		else if (phase->aggstrategy == AGG_PLAIN ||
				 phase->aggstrategy == AGG_SORTED)
		{
			dohash = false;
			dosort = true;
		}
		else if (phase->aggstrategy == AGG_HASHED)
		{
			dohash = true;
			dosort = false;
		}
		else
			Assert(false);

		phase->evaltrans = ExecBuildAggTrans(aggstate, phase, dosort, dohash,
											 false);

		/* cache compiled expression for outer slot without NULL check */
		phase->evaltrans_cache[0][0] = phase->evaltrans;
	}

	/*
	 * Set up parallel shared hash aggregation support.  This must happen
	 * here, after the loop above has populated every pertrans entry: the
	 * per-transition classification reads pertrans->transtypeByVal and
	 * pertrans->aggref, which are all zeroes/NULL until then.
	 */
	if (aggstate->shared_mode)
		shared_agg_init_support(aggstate);

	return aggstate;
}

/*
 * Build the state needed to calculate a state value for an aggregate.
 *
 * This initializes all the fields in 'pertrans'. 'aggref' is the aggregate
 * to initialize the state for. 'transfn_oid', 'aggtranstype', and the rest
 * of the arguments could be calculated from 'aggref', but the caller has
 * calculated them already, so might as well pass them.
 *
 * 'transfn_oid' may be either the Oid of the aggtransfn or the aggcombinefn.
 */
static void
build_pertrans_for_aggref(AggStatePerTrans pertrans,
						  AggState *aggstate, EState *estate,
						  Aggref *aggref,
						  Oid transfn_oid, Oid aggtranstype,
						  Oid aggserialfn, Oid aggdeserialfn,
						  Datum initValue, bool initValueIsNull,
						  Oid *inputTypes, int numArguments)
{
	int			numGroupingSets = Max(aggstate->maxsets, 1);
	Expr	   *transfnexpr;
	int			numTransArgs;
	Expr	   *serialfnexpr = NULL;
	Expr	   *deserialfnexpr = NULL;
	ListCell   *lc;
	int			numInputs;
	int			numDirectArgs;
	List	   *sortlist;
	int			numSortCols;
	int			numDistinctCols;
	int			i;

	/* Begin filling in the pertrans data */
	pertrans->aggref = aggref;
	pertrans->aggshared = false;
	pertrans->aggCollation = aggref->inputcollid;
	pertrans->transfn_oid = transfn_oid;
	pertrans->serialfn_oid = aggserialfn;
	pertrans->deserialfn_oid = aggdeserialfn;
	pertrans->initValue = initValue;
	pertrans->initValueIsNull = initValueIsNull;

	/* Count the "direct" arguments, if any */
	numDirectArgs = list_length(aggref->aggdirectargs);

	/* Count the number of aggregated input columns */
	pertrans->numInputs = numInputs = list_length(aggref->args);

	pertrans->aggtranstype = aggtranstype;

	/* account for the current transition state */
	numTransArgs = pertrans->numTransInputs + 1;

	/*
	 * Set up infrastructure for calling the transfn.  Note that invtransfn is
	 * not needed here.
	 */
	build_aggregate_transfn_expr(inputTypes,
								 numArguments,
								 numDirectArgs,
								 aggref->aggvariadic,
								 aggtranstype,
								 aggref->inputcollid,
								 transfn_oid,
								 InvalidOid,
								 &transfnexpr,
								 NULL);

	fmgr_info(transfn_oid, &pertrans->transfn);
	fmgr_info_set_expr((Node *) transfnexpr, &pertrans->transfn);

	pertrans->transfn_fcinfo =
		(FunctionCallInfo) palloc(SizeForFunctionCallInfo(numTransArgs));
	InitFunctionCallInfoData(*pertrans->transfn_fcinfo,
							 &pertrans->transfn,
							 numTransArgs,
							 pertrans->aggCollation,
							 (Node *) aggstate, NULL);

	/* get info about the state value's datatype */
	get_typlenbyval(aggtranstype,
					&pertrans->transtypeLen,
					&pertrans->transtypeByVal);

	if (OidIsValid(aggserialfn))
	{
		build_aggregate_serialfn_expr(aggserialfn,
									  &serialfnexpr);
		fmgr_info(aggserialfn, &pertrans->serialfn);
		fmgr_info_set_expr((Node *) serialfnexpr, &pertrans->serialfn);

		pertrans->serialfn_fcinfo =
			(FunctionCallInfo) palloc(SizeForFunctionCallInfo(1));
		InitFunctionCallInfoData(*pertrans->serialfn_fcinfo,
								 &pertrans->serialfn,
								 1,
								 InvalidOid,
								 (Node *) aggstate, NULL);
	}

	if (OidIsValid(aggdeserialfn))
	{
		build_aggregate_deserialfn_expr(aggdeserialfn,
										&deserialfnexpr);
		fmgr_info(aggdeserialfn, &pertrans->deserialfn);
		fmgr_info_set_expr((Node *) deserialfnexpr, &pertrans->deserialfn);

		pertrans->deserialfn_fcinfo =
			(FunctionCallInfo) palloc(SizeForFunctionCallInfo(2));
		InitFunctionCallInfoData(*pertrans->deserialfn_fcinfo,
								 &pertrans->deserialfn,
								 2,
								 InvalidOid,
								 (Node *) aggstate, NULL);
	}

	/*
	 * If we're doing either DISTINCT or ORDER BY for a plain agg, then we
	 * have a list of SortGroupClause nodes; fish out the data in them and
	 * stick them into arrays.  We ignore ORDER BY for an ordered-set agg,
	 * however; the agg's transfn and finalfn are responsible for that.
	 *
	 * When the planner has set the aggpresorted flag, the input to the
	 * aggregate is already correctly sorted.  For ORDER BY aggregates we can
	 * simply treat these as normal aggregates.  For presorted DISTINCT
	 * aggregates an extra step must be added to remove duplicate consecutive
	 * inputs.
	 *
	 * Note that by construction, if there is a DISTINCT clause then the ORDER
	 * BY clause is a prefix of it (see transformDistinctClause).
	 */
	if (AGGKIND_IS_ORDERED_SET(aggref->aggkind))
	{
		sortlist = NIL;
		numSortCols = numDistinctCols = 0;
		pertrans->aggsortrequired = false;
	}
	else if (aggref->aggpresorted && aggref->aggdistinct == NIL)
	{
		sortlist = NIL;
		numSortCols = numDistinctCols = 0;
		pertrans->aggsortrequired = false;
	}
	else if (aggref->aggdistinct)
	{
		sortlist = aggref->aggdistinct;
		numSortCols = numDistinctCols = list_length(sortlist);
		Assert(numSortCols >= list_length(aggref->aggorder));
		pertrans->aggsortrequired = !aggref->aggpresorted;
	}
	else
	{
		sortlist = aggref->aggorder;
		numSortCols = list_length(sortlist);
		numDistinctCols = 0;
		pertrans->aggsortrequired = (numSortCols > 0);
	}

	pertrans->numSortCols = numSortCols;
	pertrans->numDistinctCols = numDistinctCols;

	/*
	 * If we have either sorting or filtering to do, create a tupledesc and
	 * slot corresponding to the aggregated inputs (including sort
	 * expressions) of the agg.
	 */
	if (numSortCols > 0 || aggref->aggfilter)
	{
		pertrans->sortdesc = ExecTypeFromTL(aggref->args);
		pertrans->sortslot =
			ExecInitExtraTupleSlot(estate, pertrans->sortdesc,
								   &TTSOpsMinimalTuple);
	}

	if (numSortCols > 0)
	{
		/*
		 * We don't implement DISTINCT or ORDER BY aggs in the HASHED case
		 * (yet)
		 */
		Assert(aggstate->aggstrategy != AGG_HASHED && aggstate->aggstrategy != AGG_MIXED);

		/* ORDER BY aggregates are not supported with partial aggregation */
		Assert(!DO_AGGSPLIT_COMBINE(aggstate->aggsplit));

		/* If we have only one input, we need its len/byval info. */
		if (numInputs == 1)
		{
			get_typlenbyval(inputTypes[numDirectArgs],
							&pertrans->inputtypeLen,
							&pertrans->inputtypeByVal);
		}
		else if (numDistinctCols > 0)
		{
			/* we will need an extra slot to store prior values */
			pertrans->uniqslot =
				ExecInitExtraTupleSlot(estate, pertrans->sortdesc,
									   &TTSOpsMinimalTuple);
		}

		/* Extract the sort information for use later */
		pertrans->sortColIdx =
			(AttrNumber *) palloc(numSortCols * sizeof(AttrNumber));
		pertrans->sortOperators =
			(Oid *) palloc(numSortCols * sizeof(Oid));
		pertrans->sortCollations =
			(Oid *) palloc(numSortCols * sizeof(Oid));
		pertrans->sortNullsFirst =
			(bool *) palloc(numSortCols * sizeof(bool));

		i = 0;
		foreach(lc, sortlist)
		{
			SortGroupClause *sortcl = (SortGroupClause *) lfirst(lc);
			TargetEntry *tle = get_sortgroupclause_tle(sortcl, aggref->args);

			/* the parser should have made sure of this */
			Assert(OidIsValid(sortcl->sortop));

			pertrans->sortColIdx[i] = tle->resno;
			pertrans->sortOperators[i] = sortcl->sortop;
			pertrans->sortCollations[i] = exprCollation((Node *) tle->expr);
			pertrans->sortNullsFirst[i] = sortcl->nulls_first;
			i++;
		}
		Assert(i == numSortCols);
	}

	if (aggref->aggdistinct)
	{
		Oid		   *ops;

		Assert(numArguments > 0);
		Assert(list_length(aggref->aggdistinct) == numDistinctCols);

		ops = palloc(numDistinctCols * sizeof(Oid));

		i = 0;
		foreach(lc, aggref->aggdistinct)
			ops[i++] = ((SortGroupClause *) lfirst(lc))->eqop;

		/* lookup / build the necessary comparators */
		if (numDistinctCols == 1)
			fmgr_info(get_opcode(ops[0]), &pertrans->equalfnOne);
		else
			pertrans->equalfnMulti =
				execTuplesMatchPrepare(pertrans->sortdesc,
									   numDistinctCols,
									   pertrans->sortColIdx,
									   ops,
									   pertrans->sortCollations,
									   &aggstate->ss.ps);
		pfree(ops);
	}

	pertrans->sortstates = palloc0_array(Tuplesortstate *, numGroupingSets);
}


static Datum
GetAggInitVal(Datum textInitVal, Oid transtype)
{
	Oid			typinput,
				typioparam;
	char	   *strInitVal;
	Datum		initVal;

	getTypeInputInfo(transtype, &typinput, &typioparam);
	strInitVal = TextDatumGetCString(textInitVal);
	initVal = OidInputFunctionCall(typinput, strInitVal,
								   typioparam, -1);
	pfree(strInitVal);
	return initVal;
}

void
ExecEndAgg(AggState *node)
{
	PlanState  *outerPlan;
	int			transno;
	int			numGroupingSets = Max(node->maxsets, 1);
	int			setno;

	/*
	 * When ending a parallel worker, copy the statistics gathered by the
	 * worker back into shared memory so that it can be picked up by the main
	 * process to report in EXPLAIN ANALYZE.
	 */
	if (node->shared_info && IsParallelWorker())
	{
		AggregateInstrumentation *si;

		Assert(ParallelWorkerNumber < node->shared_info->num_workers);
		si = &node->shared_info->sinstrument[ParallelWorkerNumber];
		si->hash_batches_used = node->hash_batches_used;
		si->hash_disk_used = node->hash_disk_used;
		si->hash_mem_peak = node->hash_mem_peak;
	}

	/* Make sure we have closed any open tuplesorts */

	if (node->sort_in)
		tuplesort_end(node->sort_in);
	if (node->sort_out)
		tuplesort_end(node->sort_out);

	hashagg_reset_spill_state(node);

	/* Release hash tables too */
	if (node->hash_metacxt != NULL)
	{
		MemoryContextDelete(node->hash_metacxt);
		node->hash_metacxt = NULL;
	}
	if (node->hash_tuplescxt != NULL)
	{
		MemoryContextDelete(node->hash_tuplescxt);
		node->hash_tuplescxt = NULL;
	}

	for (transno = 0; transno < node->numtrans; transno++)
	{
		AggStatePerTrans pertrans = &node->pertrans[transno];

		for (setno = 0; setno < numGroupingSets; setno++)
		{
			if (pertrans->sortstates[setno])
				tuplesort_end(pertrans->sortstates[setno]);
		}
	}

	/* And ensure any agg shutdown callbacks have been called */
	for (setno = 0; setno < numGroupingSets; setno++)
		ReScanExprContext(node->aggcontexts[setno]);
	if (node->hashcontext)
		ReScanExprContext(node->hashcontext);

	outerPlan = outerPlanState(node);
	ExecEndNode(outerPlan);
}

void
ExecReScanAgg(AggState *node)
{
	ExprContext *econtext = node->ss.ps.ps_ExprContext;
	PlanState  *outerPlan = outerPlanState(node);
	Agg		   *aggnode = (Agg *) node->ss.ps.plan;
	int			transno;
	int			numGroupingSets = Max(node->maxsets, 1);
	int			setno;

	node->agg_done = false;

	/*
	 * Parallel shared hash aggregation: reset local state; the shared
	 * coordination state is reset separately by ExecAggReInitializeDSM
	 * before workers are relaunched.  The old shared table is leaked in
	 * the per-query DSA until end of query (v0 prototype limitation).
	 */
	if (node->shared_mode)
	{
		node->table_filled = false;
		node->shared_attached = false;
		node->shared_scan_attached = false;
		node->shared_scan_bucket = 0;
		node->shared_scan_chunk_end = 0;
		node->shared_scan_entry = InvalidDsaPointer;
		node->shared_alloc_chunk = InvalidDsaPointer;
		node->shared_alloc_used = 0;
		node->shared_alloc_size = 0;

		if (outerPlan->chgParam == NULL)
			ExecReScan(outerPlan);
		return;
	}

	if (node->aggstrategy == AGG_HASHED)
	{
		/*
		 * In the hashed case, if we haven't yet built the hash table then we
		 * can just return; nothing done yet, so nothing to undo. If subnode's
		 * chgParam is not NULL then it will be re-scanned by ExecProcNode,
		 * else no reason to re-scan it at all.
		 */
		if (!node->table_filled)
			return;

		/*
		 * If we do have the hash table, and it never spilled, and the subplan
		 * does not have any parameter changes, and none of our own parameter
		 * changes affect input expressions of the aggregated functions, then
		 * we can just rescan the existing hash table; no need to build it
		 * again.
		 */
		if (outerPlan->chgParam == NULL && !node->hash_ever_spilled &&
			!bms_overlap(node->ss.ps.chgParam, aggnode->aggParams))
		{
			ResetTupleHashIterator(node->perhash[0].hashtable,
								   &node->perhash[0].hashiter);
			select_current_set(node, 0, true);
			return;
		}
	}

	/* Make sure we have closed any open tuplesorts */
	for (transno = 0; transno < node->numtrans; transno++)
	{
		for (setno = 0; setno < numGroupingSets; setno++)
		{
			AggStatePerTrans pertrans = &node->pertrans[transno];

			if (pertrans->sortstates[setno])
			{
				tuplesort_end(pertrans->sortstates[setno]);
				pertrans->sortstates[setno] = NULL;
			}
		}
	}

	/*
	 * We don't need to ReScanExprContext the output tuple context here;
	 * ExecReScan already did it. But we do need to reset our per-grouping-set
	 * contexts, which may have transvalues stored in them. (We use rescan
	 * rather than just reset because transfns may have registered callbacks
	 * that need to be run now.) For the AGG_HASHED case, see below.
	 */

	for (setno = 0; setno < numGroupingSets; setno++)
	{
		ReScanExprContext(node->aggcontexts[setno]);
	}

	/* Release first tuple of group, if we have made a copy */
	if (node->grp_firstTuple != NULL)
	{
		heap_freetuple(node->grp_firstTuple);
		node->grp_firstTuple = NULL;
	}
	ExecClearTuple(node->ss.ss_ScanTupleSlot);

	/* Forget current agg values */
	MemSet(econtext->ecxt_aggvalues, 0, sizeof(Datum) * node->numaggs);
	MemSet(econtext->ecxt_aggnulls, 0, sizeof(bool) * node->numaggs);

	/*
	 * With AGG_HASHED/MIXED, the hash table is allocated in a sub-context of
	 * the hashcontext. This used to be an issue, but now, resetting a context
	 * automatically deletes sub-contexts too.
	 */
	if (node->aggstrategy == AGG_HASHED || node->aggstrategy == AGG_MIXED)
	{
		hashagg_reset_spill_state(node);

		node->hash_ever_spilled = false;
		node->hash_spill_mode = false;
		node->hash_ngroups_current = 0;

		ReScanExprContext(node->hashcontext);
		/* Rebuild empty hash table(s) */
		build_hash_tables(node);
		node->table_filled = false;
		/* iterator will be reset when the table is filled */

		hashagg_recompile_expressions(node, false, false);
	}

	if (node->aggstrategy != AGG_HASHED)
	{
		/*
		 * Reset the per-group state (in particular, mark transvalues null)
		 */
		for (setno = 0; setno < numGroupingSets; setno++)
		{
			MemSet(node->pergroups[setno], 0,
				   sizeof(AggStatePerGroupData) * node->numaggs);
		}

		/* reset to phase 1 */
		initialize_phase(node, 1);

		node->input_done = false;
		node->projected_set = -1;
	}

	if (outerPlan->chgParam == NULL)
		ExecReScan(outerPlan);
}


/***********************************************************************
 * API exposed to aggregate functions
 ***********************************************************************/


/*
 * AggCheckCallContext - test if a SQL function is being called as an aggregate
 *
 * The transition and/or final functions of an aggregate may want to verify
 * that they are being called as aggregates, rather than as plain SQL
 * functions.  They should use this function to do so.  The return value
 * is nonzero if being called as an aggregate, or zero if not.  (Specific
 * nonzero values are AGG_CONTEXT_AGGREGATE or AGG_CONTEXT_WINDOW, but more
 * values could conceivably appear in future.)
 *
 * If aggcontext isn't NULL, the function also stores at *aggcontext the
 * identity of the memory context that aggregate transition values are being
 * stored in.  Note that the same aggregate call site (flinfo) may be called
 * interleaved on different transition values in different contexts, so it's
 * not kosher to cache aggcontext under fn_extra.  It is, however, kosher to
 * cache it in the transvalue itself (for internal-type transvalues).
 */
int
AggCheckCallContext(FunctionCallInfo fcinfo, MemoryContext *aggcontext)
{
	if (fcinfo->context && IsA(fcinfo->context, AggState))
	{
		if (aggcontext)
		{
			AggState   *aggstate = ((AggState *) fcinfo->context);
			ExprContext *cxt = aggstate->curaggcontext;

			*aggcontext = cxt->ecxt_per_tuple_memory;
		}
		return AGG_CONTEXT_AGGREGATE;
	}
	if (fcinfo->context && IsA(fcinfo->context, WindowAggState))
	{
		if (aggcontext)
			*aggcontext = ((WindowAggState *) fcinfo->context)->curaggcontext;
		return AGG_CONTEXT_WINDOW;
	}

	/* this is just to prevent "uninitialized variable" warnings */
	if (aggcontext)
		*aggcontext = NULL;
	return 0;
}

/*
 * AggGetAggref - allow an aggregate support function to get its Aggref
 *
 * If the function is being called as an aggregate support function,
 * return the Aggref node for the aggregate call.  Otherwise, return NULL.
 *
 * Aggregates sharing the same inputs and transition functions can get
 * merged into a single transition calculation.  If the transition function
 * calls AggGetAggref, it will get some one of the Aggrefs for which it is
 * executing.  It must therefore not pay attention to the Aggref fields that
 * relate to the final function, as those are indeterminate.  But if a final
 * function calls AggGetAggref, it will get a precise result.
 *
 * Note that if an aggregate is being used as a window function, this will
 * return NULL.  We could provide a similar function to return the relevant
 * WindowFunc node in such cases, but it's not needed yet.
 */
Aggref *
AggGetAggref(FunctionCallInfo fcinfo)
{
	if (fcinfo->context && IsA(fcinfo->context, AggState))
	{
		AggState   *aggstate = (AggState *) fcinfo->context;
		AggStatePerAgg curperagg;
		AggStatePerTrans curpertrans;

		/* check curperagg (valid when in a final function) */
		curperagg = aggstate->curperagg;

		if (curperagg)
			return curperagg->aggref;

		/* check curpertrans (valid when in a transition function) */
		curpertrans = aggstate->curpertrans;

		if (curpertrans)
			return curpertrans->aggref;
	}
	return NULL;
}

/*
 * AggGetTempMemoryContext - fetch short-term memory context for aggregates
 *
 * This is useful in agg final functions; the context returned is one that
 * the final function can safely reset as desired.  This isn't useful for
 * transition functions, since the context returned MAY (we don't promise)
 * be the same as the context those are called in.
 *
 * As above, this is currently not useful for aggs called as window functions.
 */
MemoryContext
AggGetTempMemoryContext(FunctionCallInfo fcinfo)
{
	if (fcinfo->context && IsA(fcinfo->context, AggState))
	{
		AggState   *aggstate = (AggState *) fcinfo->context;

		return aggstate->tmpcontext->ecxt_per_tuple_memory;
	}
	return NULL;
}

/*
 * AggStateIsShared - find out whether transition state is shared
 *
 * If the function is being called as an aggregate support function,
 * return true if the aggregate's transition state is shared across
 * multiple aggregates, false if it is not.
 *
 * Returns true if not called as an aggregate support function.
 * This is intended as a conservative answer, ie "no you'd better not
 * scribble on your input".  In particular, will return true if the
 * aggregate is being used as a window function, which is a scenario
 * in which changing the transition state is a bad idea.  We might
 * want to refine the behavior for the window case in future.
 */
bool
AggStateIsShared(FunctionCallInfo fcinfo)
{
	if (fcinfo->context && IsA(fcinfo->context, AggState))
	{
		AggState   *aggstate = (AggState *) fcinfo->context;
		AggStatePerAgg curperagg;
		AggStatePerTrans curpertrans;

		/* check curperagg (valid when in a final function) */
		curperagg = aggstate->curperagg;

		if (curperagg)
			return aggstate->pertrans[curperagg->transno].aggshared;

		/* check curpertrans (valid when in a transition function) */
		curpertrans = aggstate->curpertrans;

		if (curpertrans)
			return curpertrans->aggshared;
	}
	return true;
}

/*
 * AggRegisterCallback - register a cleanup callback for an aggregate
 *
 * This is useful for aggs to register shutdown callbacks, which will ensure
 * that non-memory resources are freed.  The callback will occur just before
 * the associated aggcontext (as returned by AggCheckCallContext) is reset,
 * either between groups or as a result of rescanning the query.  The callback
 * will NOT be called on error paths.  The typical use-case is for freeing of
 * tuplestores or tuplesorts maintained in aggcontext, or pins held by slots
 * created by the agg functions.  (The callback will not be called until after
 * the result of the finalfn is no longer needed, so it's safe for the finalfn
 * to return data that will be freed by the callback.)
 *
 * As above, this is currently not useful for aggs called as window functions.
 */
void
AggRegisterCallback(FunctionCallInfo fcinfo,
					ExprContextCallbackFunction func,
					Datum arg)
{
	if (fcinfo->context && IsA(fcinfo->context, AggState))
	{
		AggState   *aggstate = (AggState *) fcinfo->context;
		ExprContext *cxt = aggstate->curaggcontext;

		RegisterExprContextCallback(cxt, func, arg);

		return;
	}
	elog(ERROR, "aggregate function cannot register a callback in this context");
}


/* ----------------------------------------------------------------
 *						Parallel Query Support
 * ----------------------------------------------------------------
 */

 /* ----------------------------------------------------------------
  *		ExecAggEstimate
  *
  *		Estimate space required to propagate aggregate statistics.
  * ----------------------------------------------------------------
  */
void
ExecAggEstimate(AggState *node, ParallelContext *pcxt)
{
	Size		size;

	/* shared-build coordination state for Parallel Hash Aggregate */
	if (node->shared_mode && pcxt->nworkers > 0)
	{
		Size		shsize;

		shsize = add_size(MAXALIGN(sizeof(SharedAggBuildState)),
						  mul_size(SHARED_AGG_SPILL_PARTITIONS,
								   MAXALIGN(sts_estimate(pcxt->nworkers + 1))));
		shm_toc_estimate_chunk(&pcxt->estimator, shsize);
		shm_toc_estimate_keys(&pcxt->estimator, 1);
	}

	/* don't need this if not instrumenting or no workers */
	if (!node->ss.ps.instrument || pcxt->nworkers == 0)
		return;

	size = mul_size(pcxt->nworkers, sizeof(AggregateInstrumentation));
	size = add_size(size, offsetof(SharedAggInfo, sinstrument));
	shm_toc_estimate_chunk(&pcxt->estimator, size);
	shm_toc_estimate_keys(&pcxt->estimator, 1);
}

/* ----------------------------------------------------------------
 *		ExecAggInitializeDSM
 *
 *		Initialize DSM space for aggregate statistics.
 * ----------------------------------------------------------------
 */
void
ExecAggInitializeDSM(AggState *node, ParallelContext *pcxt)
{
	Size		size;

	/* shared-build coordination state for Parallel Hash Aggregate */
	if (node->shared_mode && pcxt->nworkers > 0)
	{
		SharedAggBuildState *shstate;
		Size		shsize;
		int			nparticipants = pcxt->nworkers + 1;
		int			i;

		shsize = add_size(MAXALIGN(sizeof(SharedAggBuildState)),
						  mul_size(SHARED_AGG_SPILL_PARTITIONS,
								   MAXALIGN(sts_estimate(nparticipants))));
		shstate = (SharedAggBuildState *)
			shm_toc_allocate(pcxt->toc, shsize);
		BarrierInit(&shstate->build_barrier, 0);
		BarrierInit(&shstate->scan_barrier, 0);
		shstate->nparticipants = nparticipants;
		shstate->nbuckets = 0;
		shstate->buckets = InvalidDsaPointer;
		pg_atomic_init_u64(&shstate->nentries, 0);
		pg_atomic_init_u64(&shstate->scan_cursor, 0);
		pg_atomic_init_u64(&shstate->mem_used, 0);
		pg_atomic_init_u32(&shstate->spill_mode, 0);
		pg_atomic_init_u64(&shstate->nspilled, 0);
		for (i = 0; i < SHARED_AGG_SPILL_PARTITIONS; i++)
			pg_atomic_init_u64(&shstate->npart_spilled[i], 0);
		shstate->cur_batch = 0;
		LWLockInitialize(&shstate->batch_lock, LWTRANCHE_PARALLEL_HASH_AGG);
		shstate->chunk_head = InvalidDsaPointer;
		shstate->mem_limit =
			mul_size(get_hash_memory_limit(), nparticipants);
		for (i = 0; i < SHARED_AGG_NUM_STRIPES; i++)
			LWLockInitialize(&shstate->stripe_locks[i].lock,
							 LWTRANCHE_PARALLEL_HASH_AGG);

		/* set up the spill partitions' shared tuplestores */
		SharedFileSetInit(&shstate->fileset, pcxt->seg);
		node->shared_spill_acc =
			palloc(SHARED_AGG_SPILL_PARTITIONS *
				   sizeof(SharedTuplestoreAccessor *));
		for (i = 0; i < SHARED_AGG_SPILL_PARTITIONS; i++)
		{
			char		name[MAXPGPATH];

			snprintf(name, MAXPGPATH, "pha%d.%d",
					 node->ss.ps.plan->plan_node_id, i);
			node->shared_spill_acc[i] =
				sts_initialize(SharedAggPartitionSts(shstate, i),
							   nparticipants,
							   ParallelWorkerNumber + 1,
							   sizeof(uint32),
							   SHARED_TUPLESTORE_SINGLE_PASS,
							   &shstate->fileset,
							   name);
		}

		shm_toc_insert(pcxt->toc,
					   node->ss.ps.plan->plan_node_id +
					   PARALLEL_KEY_AGG_SHARED_OFFSET,
					   shstate);
		node->shared_build = shstate;
	}

	/* don't need this if not instrumenting or no workers */
	if (!node->ss.ps.instrument || pcxt->nworkers == 0)
		return;

	size = offsetof(SharedAggInfo, sinstrument)
		+ pcxt->nworkers * sizeof(AggregateInstrumentation);
	node->shared_info = shm_toc_allocate(pcxt->toc, size);
	/* ensure any unfilled slots will contain zeroes */
	memset(node->shared_info, 0, size);
	node->shared_info->num_workers = pcxt->nworkers;
	shm_toc_insert(pcxt->toc, node->ss.ps.plan->plan_node_id,
				   node->shared_info);
}

/* ----------------------------------------------------------------
 *		ExecAggReInitializeDSM
 *
 *		Reset shared-build state before relaunching workers for a rescan.
 *		The previous bucket array and entries are intentionally leaked in
 *		the per-query DSA until end of query (v0 prototype limitation).
 * ----------------------------------------------------------------
 */
void
ExecAggReInitializeDSM(AggState *node, ParallelContext *pcxt)
{
	SharedAggBuildState *shstate = node->shared_build;

	if (shstate == NULL)
		return;

	/*
	 * Reinitializing the shared tuplestores after a spilling run would
	 * require recreating their backing files; not supported yet.
	 */
	if (pg_atomic_read_u64(&shstate->nspilled) > 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot rescan Parallel Hash Aggregate after it has spilled to disk")));

	BarrierInit(&shstate->build_barrier, 0);
	BarrierInit(&shstate->scan_barrier, 0);
	shstate->nbuckets = 0;
	shstate->buckets = InvalidDsaPointer;
	shstate->cur_batch = 0;
	shstate->chunk_head = InvalidDsaPointer;
	pg_atomic_write_u64(&shstate->nentries, 0);
	pg_atomic_write_u64(&shstate->scan_cursor, 0);
	pg_atomic_write_u64(&shstate->mem_used, 0);
	pg_atomic_write_u32(&shstate->spill_mode, 0);
	for (int i = 0; i < SHARED_AGG_SPILL_PARTITIONS; i++)
		pg_atomic_write_u64(&shstate->npart_spilled[i], 0);
}

/* ----------------------------------------------------------------
 *		ExecAggInitializeWorker
 *
 *		Attach worker to DSM space for aggregate statistics.
 * ----------------------------------------------------------------
 */
void
ExecAggInitializeWorker(AggState *node, ParallelWorkerContext *pwcxt)
{
	if (node->shared_mode)
	{
		SharedAggBuildState *shstate;
		int			i;

		shstate = (SharedAggBuildState *)
			shm_toc_lookup(pwcxt->toc,
						   node->ss.ps.plan->plan_node_id +
						   PARALLEL_KEY_AGG_SHARED_OFFSET,
						   false);
		node->shared_build = shstate;

		SharedFileSetAttach(&shstate->fileset, pwcxt->seg);
		node->shared_spill_acc =
			palloc(SHARED_AGG_SPILL_PARTITIONS *
				   sizeof(SharedTuplestoreAccessor *));
		for (i = 0; i < SHARED_AGG_SPILL_PARTITIONS; i++)
			node->shared_spill_acc[i] =
				sts_attach(SharedAggPartitionSts(shstate, i),
						   ParallelWorkerNumber + 1,
						   &shstate->fileset);
	}

	node->shared_info =
		shm_toc_lookup(pwcxt->toc, node->ss.ps.plan->plan_node_id, true);
}

/* ----------------------------------------------------------------
 *		ExecAggRetrieveInstrumentation
 *
 *		Transfer aggregate statistics from DSM to private memory.
 * ----------------------------------------------------------------
 */
void
ExecAggRetrieveInstrumentation(AggState *node)
{
	Size		size;
	SharedAggInfo *si;

	/*
	 * Copy shared-build spill statistics into backend-local fields while
	 * the DSM segment is still attached; EXPLAIN runs after it is gone.
	 */
	if (node->shared_build != NULL)
	{
		SharedAggBuildState *shstate = node->shared_build;
		int			i;

		node->shared_nspilled = pg_atomic_read_u64(&shstate->nspilled);
		node->shared_nbatches = 0;
		for (i = 0; i < SHARED_AGG_SPILL_PARTITIONS; i++)
		{
			if (pg_atomic_read_u64(&shstate->npart_spilled[i]) > 0)
				node->shared_nbatches++;
		}
	}

	if (node->shared_info == NULL)
		return;

	size = offsetof(SharedAggInfo, sinstrument)
		+ node->shared_info->num_workers * sizeof(AggregateInstrumentation);
	si = palloc(size);
	memcpy(si, node->shared_info, size);
	node->shared_info = si;
}
