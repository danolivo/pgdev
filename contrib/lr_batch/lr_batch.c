/*-------------------------------------------------------------------------
 *
 * lr_batch.c
 *		Batch remote INSERTs in the logical replication apply worker.
 *
 * The module plugs into the apply worker through three hooks:
 *
 * logicalrep_insert_hook takes over a remote INSERT once the worker has
 * converted it into a slot.  If the subscription and the target relation
 * are eligible, the tuple is checked the same way ExecSimpleRelationInsert()
 * would check it (stored generated columns, NOT NULL, CHECK and partition
 * constraints) and appended to an in-memory buffer for its relation.  As in
 * COPY (CopyMultiInsertInfo), a transaction keeps one buffer per relation,
 * up to LR_BATCH_MAX_BUFFERS of them, and each buffer is flushed with
 * table_multi_insert() plus per-tuple ExecInsertIndexTuples().
 *
 * Within a transaction, rows of different relations can therefore reach the
 * heap in a different order than they were sent.  Nothing can observe that:
 * eligible tables have no triggers that fire, foreign-key triggers do not
 * fire in the apply worker, and all buffers are flushed before any other
 * message is handled and before any INSERT takes the per-row path.
 *
 * logicalrep_message_hook is the flush barrier: every message other than an
 * INSERT -- UPDATE, DELETE, TRUNCATE, RELATION, TYPE, COMMIT, PREPARE, ... --
 * flushes the buffers and releases them before the message is handled, so
 * that later changes and the commit see the inserted rows.
 *
 * A flush can be triggered by any message, so it sets up its own execution
 * context: it runs as the table owner unless the subscription has
 * run_as_owner = true (index expressions and partial-index predicates are
 * evaluated there), pushes a snapshot if none is active, and increments the
 * command counter afterwards, as end_replication_step() does.
 *
 * Not every commit is preceded by a message: a streamed transaction that the
 * leader spooled to a file (streaming = on, or parallel apply falling back
 * to serialization) is replayed through apply_dispatch() and then committed
 * directly.  The transaction callback therefore flushes at pre-commit and
 * pre-prepare as well.  Inserting there is fine: deferred triggers have
 * already fired by then, and eligible tables have no triggers that fire.
 * Replay happens in the top-level transaction, since the changes of aborted
 * subtransactions are cut out of the spool file before it is replayed.
 *
 * The buffer lives in a child of TopTransactionContext and its relation and
 * tuple descriptor references are owned by the transaction's resource
 * owner, so an aborted transaction cleans it up without our help; the
 * transaction callback only forgets the pointer.
 *
 * A parallel apply worker applies a streamed transaction chunk by chunk and
 * opens a savepoint for each new subtransaction between two changes, not at
 * a message boundary.  logicalrep_savepoint_hook is called right before
 * that, and flushes the buffers, so that every row is inserted in the same
 * subtransaction as the per-row path would insert it.  Rolling back to a
 * savepoint is driven by a STREAM ABORT message, which goes through the
 * barrier.  A subtransaction callback checks the invariant: no subtransaction
 * may start while tuples are buffered.
 *
 * Limitations:
 *
 * - A unique violation against a pre-existing subscriber row is reported
 *   as an insert_exists (or multiple_unique_conflicts) conflict and counted
 *   in pg_stat_subscription_stats, as in the per-row path.  The worker's own
 *   error context names the message that triggered the flush (e.g. COMMIT)
 *   rather than the INSERT and its relation, so we add a context line of our
 *   own naming the relation.  The finish LSN needed for ALTER SUBSCRIPTION
 *   ... SKIP is still reported.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/lr_batch/lr_batch.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/index.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_class.h"
#include "catalog/pg_index.h"
#include "catalog/pg_subscription.h"
#include "catalog/pg_trigger.h"
#include "commands/trigger.h"
#include "executor/executor.h"
#include "executor/nodeModifyTable.h"
#include "miscadmin.h"
#include "parser/parse_relation.h"
#include "replication/conflict.h"
#include "replication/logicalproto.h"
#include "replication/logicalrelation.h"
#include "replication/worker_internal.h"
#include "utils/acl.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/rls.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/usercontext.h"
#include "utils/varlena.h"

PG_MODULE_MAGIC_EXT(
					.name = "lr_batch",
					.version = PG_VERSION
);

/* GUC variables */
static char *lr_batch_subscriptions = NULL;
static int	lr_batch_max_tuples = 1000;
static int	lr_batch_max_bytes = 8192;	/* kB */

/* Saved hook values */
static logicalrep_insert_hook_type prev_insert_hook = NULL;
static logicalrep_message_hook_type prev_message_hook = NULL;
static logicalrep_savepoint_hook_type prev_savepoint_hook = NULL;

/*
 * Whether batching is enabled for the subscription of this worker.  The
 * answer is cached per subscription OID and recomputed when
 * lr_batch.subscriptions changes.  Renaming the subscription restarts the
 * worker.
 */
static bool enabled_valid = false;
static Oid	enabled_subid = InvalidOid;
static bool enabled_value = false;

/* Per-relation eligibility cache, invalidated by relcache callbacks. */
typedef struct LRBatchRelEntry
{
	Oid			relid;			/* hash key */
	bool		valid;
	bool		eligible;
} LRBatchRelEntry;

static HTAB *rel_hash = NULL;

/* A buffer of tuples for one relation. */
typedef struct LRBatchBuffer
{
	MemoryContext mcxt;			/* child of TopTransactionContext; holds
								 * everything below */
	MemoryContext tuple_mcxt;	/* child of mcxt; buffered tuples, reset at
								 * every flush */
	Oid			relid;
	Oid			relowner;
	char	   *nspname;		/* for the error context */
	Relation	rel;			/* our own reference, released at destroy */
	TupleDesc	slot_desc;		/* unpinned copy of the relation's descriptor
								 * for the buffered slots, see buffer_init */
	EState	   *estate;			/* for index insertion */
	ResultRelInfo *relinfo;		/* with indexes open */
	TupleTableSlot **slots;
	int			nslots;			/* number of buffered tuples */
	int			nslots_made;	/* number of slots created so far */
	int			slots_len;		/* allocated length of slots[] */
	Size		nbytes;			/* size of the buffered tuples */
} LRBatchBuffer;

/*
 * The buffers of the current transaction, in creation order.  They are all
 * flushed when the byte limit is reached, and released at every barrier.
 */
#define LR_BATCH_MAX_BUFFERS	32

static LRBatchBuffer *buffers[LR_BATCH_MAX_BUFFERS];
static int	nbuffers = 0;
static LRBatchBuffer *last_buffer = NULL;	/* most recently used */
static Size total_bytes = 0;	/* size of all buffered tuples */

void		_PG_init(void);

static bool lr_batch_insert_hook(LogicalRepRelMapEntry *rel,
								 ResultRelInfo *relinfo,
								 EState *estate,
								 TupleTableSlot *remoteslot);
static void lr_batch_message_hook(LogicalRepMsgType action);
static void lr_batch_savepoint_hook(void);
static void lr_batch_subxact_callback(SubXactEvent event,
									  SubTransactionId mySubid,
									  SubTransactionId parentSubid,
									  void *arg);
static void lr_batch_xact_callback(XactEvent event, void *arg);
static void lr_batch_relcache_callback(Datum arg, Oid relid);
static bool check_subscriptions(char **newval, void **extra, GucSource source);
static void assign_subscriptions(const char *newval, void *extra);

static bool batching_enabled(void);
static bool relation_is_eligible(Relation rel);
static bool compute_relation_eligibility(Relation rel);
static LRBatchBuffer *find_buffer(Oid relid);
static LRBatchBuffer *buffer_init(LogicalRepRelMapEntry *rel);
static void buffer_add(LRBatchBuffer *buf, TupleTableSlot *remoteslot);
static void buffer_flush(LRBatchBuffer *buf);
static void buffer_destroy(LRBatchBuffer *buf);
static void flush_all(void);
static void flush_and_destroy_all(void);
static void report_insert_conflict(LRBatchBuffer *buf, TupleTableSlot *slot,
								   List *recheckIndexes);


/*
 * Module load callback
 */
void
_PG_init(void)
{
	DefineCustomStringVariable("lr_batch.subscriptions",
							   "Subscriptions whose apply workers batch remote INSERTs.",
							   "A comma-separated list of subscription names, or * for all subscriptions.",
							   &lr_batch_subscriptions,
							   "",
							   PGC_SIGHUP,
							   GUC_LIST_INPUT,
							   check_subscriptions,
							   assign_subscriptions,
							   NULL);

	DefineCustomIntVariable("lr_batch.max_tuples",
							"Maximum number of tuples buffered before a flush.",
							NULL,
							&lr_batch_max_tuples,
							1000,
							1,
							100000,
							PGC_SIGHUP,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("lr_batch.max_bytes",
							"Maximum size of the tuples buffered before a flush.",
							NULL,
							&lr_batch_max_bytes,
							8192,
							64,
							MAX_KILOBYTES,
							PGC_SIGHUP,
							GUC_UNIT_KB,
							NULL,
							NULL,
							NULL);

	MarkGUCPrefixReserved("lr_batch");

	prev_insert_hook = logicalrep_insert_hook;
	logicalrep_insert_hook = lr_batch_insert_hook;
	prev_message_hook = logicalrep_message_hook;
	logicalrep_message_hook = lr_batch_message_hook;
	prev_savepoint_hook = logicalrep_savepoint_hook;
	logicalrep_savepoint_hook = lr_batch_savepoint_hook;

	RegisterXactCallback(lr_batch_xact_callback, NULL);
	RegisterSubXactCallback(lr_batch_subxact_callback, NULL);
	CacheRegisterRelcacheCallback(lr_batch_relcache_callback, (Datum) 0);
}

/*
 * GUC check hook for lr_batch.subscriptions: the value must be a valid
 * list of identifiers.
 */
static bool
check_subscriptions(char **newval, void **extra, GucSource source)
{
	char	   *rawstring;
	List	   *elemlist;
	bool		ok;

	rawstring = pstrdup(*newval);
	ok = SplitIdentifierString(rawstring, ',', &elemlist);
	if (!ok)
		GUC_check_errdetail("List syntax is invalid.");
	list_free(elemlist);
	pfree(rawstring);

	return ok;
}

static void
assign_subscriptions(const char *newval, void *extra)
{
	enabled_valid = false;
}

/*
 * Is batching enabled for the subscription this worker applies?
 */
static bool
batching_enabled(void)
{
	char	   *rawstring;
	List	   *elemlist;
	ListCell   *lc;
	bool		result = false;

	if (MySubscription == NULL)
		return false;

	if (enabled_valid && enabled_subid == MySubscription->oid)
		return enabled_value;

	rawstring = pstrdup(lr_batch_subscriptions ? lr_batch_subscriptions : "");
	if (SplitIdentifierString(rawstring, ',', &elemlist))
	{
		foreach(lc, elemlist)
		{
			const char *name = (const char *) lfirst(lc);

			if (strcmp(name, "*") == 0 ||
				strcmp(name, MySubscription->name) == 0)
			{
				result = true;
				break;
			}
		}
	}
	list_free(elemlist);
	pfree(rawstring);

	enabled_subid = MySubscription->oid;
	enabled_value = result;
	enabled_valid = true;

	return result;
}

/*
 * Relcache invalidation callback: forget cached eligibility.
 */
static void
lr_batch_relcache_callback(Datum arg, Oid relid)
{
	LRBatchRelEntry *entry;

	if (rel_hash == NULL)
		return;

	if (OidIsValid(relid))
	{
		entry = hash_search(rel_hash, &relid, HASH_FIND, NULL);
		if (entry)
			entry->valid = false;
	}
	else
	{
		HASH_SEQ_STATUS status;

		hash_seq_init(&status, rel_hash);
		while ((entry = hash_seq_search(&status)) != NULL)
			entry->valid = false;
	}
}

static bool
relation_is_eligible(Relation rel)
{
	Oid			relid = RelationGetRelid(rel);
	LRBatchRelEntry *entry;
	bool		found;

	if (rel_hash == NULL)
	{
		HASHCTL		ctl;

		ctl.keysize = sizeof(Oid);
		ctl.entrysize = sizeof(LRBatchRelEntry);
		rel_hash = hash_create("lr_batch relations", 64, &ctl,
							   HASH_ELEM | HASH_BLOBS);
	}

	entry = hash_search(rel_hash, &relid, HASH_ENTER, &found);
	if (!found || !entry->valid)
	{
		/*
		 * Mark the entry valid before computing, so that an invalidation
		 * arriving during the syscache lookups below marks it invalid again.
		 */
		entry->valid = true;
		entry->eligible = compute_relation_eligibility(rel);
	}

	return entry->eligible;
}

/*
 * Decide whether INSERTs into rel can be batched.
 *
 * Everything the per-row path does per tuple before the insertion (stored
 * generated columns, NOT NULL, CHECK and partition constraints) is done by
 * the insert hook as well, so those are no reason to refuse.  We refuse:
 *
 * - anything but a plain table (partitioned tables use tuple routing);
 * - tables with INSERT triggers that fire in the current
 *   session_replication_role (in the apply worker: ENABLE REPLICA and
 *   ENABLE ALWAYS triggers; internal foreign-key triggers do not fire);
 * - tables with row-level security, so that the per-row path raises its
 *   usual error;
 * - exclusion constraints and deferrable unique constraints, which need the
 *   recheck machinery, and indexes that are not valid, ready and live.
 */
static bool
compute_relation_eligibility(Relation rel)
{
	List	   *indexlist;
	ListCell   *lc;
	bool		result = true;

	if (rel->rd_rel->relkind != RELKIND_RELATION)
		return false;

	if (rel->rd_rel->relrowsecurity || rel->rd_rel->relforcerowsecurity)
		return false;

	if (rel->trigdesc != NULL)
	{
		for (int i = 0; i < rel->trigdesc->numtriggers; i++)
		{
			Trigger    *trig = &rel->trigdesc->triggers[i];
			bool		fires;

			if (!TRIGGER_FOR_INSERT(trig->tgtype))
				continue;

			if (SessionReplicationRole == SESSION_REPLICATION_ROLE_REPLICA)
				fires = (trig->tgenabled == TRIGGER_FIRES_ON_REPLICA ||
						 trig->tgenabled == TRIGGER_FIRES_ALWAYS);
			else
				fires = (trig->tgenabled == TRIGGER_FIRES_ON_ORIGIN ||
						 trig->tgenabled == TRIGGER_FIRES_ALWAYS);

			if (fires)
				return false;
		}
	}

	indexlist = RelationGetIndexList(rel);
	foreach(lc, indexlist)
	{
		Oid			indexoid = lfirst_oid(lc);
		HeapTuple	tup;
		Form_pg_index index;

		tup = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexoid));
		if (!HeapTupleIsValid(tup))
		{
			result = false;
			break;
		}
		index = (Form_pg_index) GETSTRUCT(tup);

		if (!index->indisvalid || !index->indisready || !index->indislive ||
			index->indisexclusion ||
			(index->indisunique && !index->indimmediate))
			result = false;

		ReleaseSysCache(tup);

		if (!result)
			break;
	}
	list_free(indexlist);

	return result;
}

/*
 * logicalrep_insert_hook
 */
static bool
lr_batch_insert_hook(LogicalRepRelMapEntry *rel,
					 ResultRelInfo *relinfo,
					 EState *estate,
					 TupleTableSlot *remoteslot)
{
	Relation	localrel = rel->localrel;
	LRBatchBuffer *buf = find_buffer(RelationGetRelid(localrel));

	/*
	 * While a buffer for this relation exists we hold a lock that blocks any
	 * DDL which could change its eligibility, so skip the checks.
	 */
	if (buf == NULL)
	{
		if (!batching_enabled() || !relation_is_eligible(localrel))
		{
			/*
			 * The per-row path is going to insert this tuple.  Flush first:
			 * triggers or index functions on this relation may look at the
			 * rows buffered for other ones.
			 */
			flush_and_destroy_all();

			if (prev_insert_hook)
				return prev_insert_hook(rel, relinfo, estate, remoteslot);
			return false;
		}

		if (nbuffers == LR_BATCH_MAX_BUFFERS)
			flush_and_destroy_all();
	}

	/*
	 * Do what ExecSimpleRelationInsert() does before inserting.  We use the
	 * worker's per-message relinfo and estate: they come with a range table,
	 * which the error reporting code needs.
	 */
	if (localrel->rd_att->constr &&
		localrel->rd_att->constr->has_generated_stored)
		ExecComputeStoredGenerated(relinfo, estate, remoteslot, CMD_INSERT);
	if (localrel->rd_att->constr)
		ExecConstraints(relinfo, remoteslot, estate);
	if (localrel->rd_rel->relispartition)
		ExecPartitionCheck(relinfo, remoteslot, estate, true);

	if (buf == NULL)
		buf = buffer_init(rel);

	buffer_add(buf, remoteslot);

	return true;
}

/*
 * logicalrep_message_hook: the flush barrier.
 */
static void
lr_batch_message_hook(LogicalRepMsgType action)
{
	if (action != LOGICAL_REP_MSG_INSERT)
		flush_and_destroy_all();

	if (prev_message_hook)
		prev_message_hook(action);
}

/*
 * logicalrep_savepoint_hook: a parallel apply worker is about to open a
 * subtransaction.  Everything buffered so far belongs to the current level.
 */
static void
lr_batch_savepoint_hook(void)
{
	flush_and_destroy_all();

	if (prev_savepoint_hook)
		prev_savepoint_hook();
}

static void
lr_batch_subxact_callback(SubXactEvent event, SubTransactionId mySubid,
						  SubTransactionId parentSubid, void *arg)
{
	switch (event)
	{
		case SUBXACT_EVENT_START_SUB:

			/*
			 * The buffered rows would be inserted in the new subtransaction
			 * instead of the one they were applied in.  Every known path
			 * calls a hook first; fail rather than misplace rows if some
			 * other path does not.
			 */
			if (nbuffers > 0)
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("lr_batch: subtransaction started with %d relations buffered",
								nbuffers)));
			break;

		case SUBXACT_EVENT_ABORT_SUB:

			/*
			 * Given the check above, any buffer still here was created in the
			 * aborting subtransaction: its rows are rolled back with it, and
			 * its relation references are released by its resource owner.
			 */
			nbuffers = 0;
			last_buffer = NULL;
			total_bytes = 0;
			break;

		default:
			break;
	}
}

static void
lr_batch_xact_callback(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_PRE_COMMIT:
		case XACT_EVENT_PARALLEL_PRE_COMMIT:
		case XACT_EVENT_PRE_PREPARE:
			/* Commit of a spooled streamed transaction; see the header. */
			flush_and_destroy_all();
			break;

		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:

			/*
			 * The buffers' memory is a child of TopTransactionContext and
			 * their relation references belong to the transaction's resource
			 * owner; abort releases both.
			 */
			nbuffers = 0;
			last_buffer = NULL;
			total_bytes = 0;
			break;

		default:
			break;
	}
}

static LRBatchBuffer *
find_buffer(Oid relid)
{
	if (last_buffer != NULL && last_buffer->relid == relid)
		return last_buffer;

	for (int i = 0; i < nbuffers; i++)
	{
		if (buffers[i]->relid == relid)
		{
			last_buffer = buffers[i];
			return last_buffer;
		}
	}
	return NULL;
}

/*
 * Create the buffer for rel.
 *
 * If anything here fails, the transaction aborts, which releases what has
 * been allocated so far; the buffer is registered only at the end.
 */
static LRBatchBuffer *
buffer_init(LogicalRepRelMapEntry *rel)
{
	MemoryContext mcxt;
	MemoryContext oldctx;
	LRBatchBuffer *buf;

	Assert(nbuffers < LR_BATCH_MAX_BUFFERS);
	Assert(IsTransactionState());

	mcxt = AllocSetContextCreate(TopTransactionContext,
								 "lr_batch buffer",
								 ALLOCSET_DEFAULT_SIZES);
	oldctx = MemoryContextSwitchTo(mcxt);

	buf = palloc0(sizeof(LRBatchBuffer));
	buf->mcxt = mcxt;
	buf->tuple_mcxt = AllocSetContextCreate(mcxt,
											"lr_batch tuples",
											ALLOCSET_DEFAULT_SIZES);
	buf->relid = RelationGetRelid(rel->localrel);
	buf->relowner = rel->localrel->rd_rel->relowner;
	buf->nspname = get_namespace_name(RelationGetNamespace(rel->localrel));

	/*
	 * Take our own reference: the worker closes its LogicalRepRelMapEntry at
	 * the end of every message, and the buffer outlives that.  The lock is
	 * already held, so this only bumps the local lock count.
	 */
	buf->rel = table_open(buf->relid, RowExclusiveLock);

	buf->estate = CreateExecutorState();

	/*
	 * A one-entry range table, as in create_edata_for_relation():
	 * ReportApplyConflict() looks up the RTE permission info when it builds
	 * the row description for its message.
	 */
	{
		RangeTblEntry *rte;
		List	   *perminfos = NIL;

		rte = makeNode(RangeTblEntry);
		rte->rtekind = RTE_RELATION;
		rte->relid = buf->relid;
		rte->relkind = buf->rel->rd_rel->relkind;
		rte->rellockmode = AccessShareLock;
		addRTEPermissionInfo(&perminfos, rte);
		ExecInitRangeTable(buf->estate, list_make1(rte), perminfos,
						   bms_make_singleton(1));
	}

	buf->relinfo = makeNode(ResultRelInfo);
	InitResultRelInfo(buf->relinfo, buf->rel, 1, NULL, 0);
	ExecOpenIndices(buf->relinfo, false);

	/*
	 * As in apply_handle_insert_internal(): the immediate unique indexes are
	 * checked with UNIQUE_CHECK_PARTIAL, and a violation is reported as a
	 * conflict.  Build the extra IndexInfo data that locating the
	 * conflicting row needs once, here.
	 */
	InitConflictIndexes(buf->relinfo);
	for (int i = 0; i < buf->relinfo->ri_NumIndices; i++)
	{
		Relation	indexrel = buf->relinfo->ri_IndexRelationDescs[i];

		if (list_member_oid(buf->relinfo->ri_onConflictArbiterIndexes,
							RelationGetRelid(indexrel)))
			BuildSpeculativeIndexInfo(indexrel,
									  buf->relinfo->ri_IndexRelationInfo[i]);
	}

	/*
	 * The buffered slots get a private copy of the tuple descriptor.  The
	 * relcache descriptor is reference-counted, so every slot made from it
	 * would pin it in the resource owner, and N pins of the same descriptor
	 * all land in one probe chain of the resource owner's hash: filling a
	 * buffer of N slots costs O(N^2).  A copy is not reference-counted.
	 */
	buf->slot_desc = CreateTupleDescCopy(RelationGetDescr(buf->rel));

	buf->slots_len = Min(64, lr_batch_max_tuples);
	buf->slots = palloc(sizeof(TupleTableSlot *) * buf->slots_len);

	MemoryContextSwitchTo(oldctx);

	buffers[nbuffers++] = buf;
	last_buffer = buf;
	return buf;
}

/*
 * Append a copy of remoteslot to the buffer; flush if it is full.
 *
 * The copy is formed once, as a heap tuple owned by the slot, so that
 * table_multi_insert() can use it directly without materializing it again.
 */
static void
buffer_add(LRBatchBuffer *buf, TupleTableSlot *remoteslot)
{
	MemoryContext oldctx;
	TupleTableSlot *dst;
	HeapTuple	tuple;

	Assert(buf != NULL);

	if (buf->nslots == buf->slots_len)
	{
		buf->slots_len *= 2;
		buf->slots = repalloc(buf->slots,
							  sizeof(TupleTableSlot *) * buf->slots_len);
	}

	if (buf->nslots == buf->nslots_made)
	{
		oldctx = MemoryContextSwitchTo(buf->mcxt);
		buf->slots[buf->nslots_made++] =
			MakeSingleTupleTableSlot(buf->slot_desc, &TTSOpsHeapTuple);
		MemoryContextSwitchTo(oldctx);
	}
	dst = buf->slots[buf->nslots];

	oldctx = MemoryContextSwitchTo(buf->tuple_mcxt);
	tuple = ExecCopySlotHeapTuple(remoteslot);
	MemoryContextSwitchTo(oldctx);
	ExecStoreHeapTuple(tuple, dst, true);

	buf->nslots++;
	buf->nbytes += tuple->t_len;
	total_bytes += tuple->t_len;

	/* lr_batch.max_bytes bounds the memory of all buffers together. */
	if (total_bytes >= (Size) lr_batch_max_bytes * 1024)
		flush_all();
	else if (buf->nslots >= lr_batch_max_tuples)
		buffer_flush(buf);
}

static void
buffer_error_callback(void *arg)
{
	LRBatchBuffer *buf = (LRBatchBuffer *) arg;

	errcontext("applying %d batched INSERTs to relation \"%s.%s\"",
			   buf->nslots, buf->nspname, RelationGetRelationName(buf->rel));
}

/*
 * Insert the buffered tuples and empty the buffer.  The buffer itself stays.
 */
static void
buffer_flush(LRBatchBuffer *buf)
{
	bool		run_as_owner;
	UserContext ucxt;
	bool		pushed_snapshot = false;
	AclResult	aclresult;
	BulkInsertState bistate;
	ErrorContextCallback errcallback;

	if (buf == NULL || buf->nslots == 0)
		return;

	Assert(IsTransactionState());

	errcallback.callback = buffer_error_callback;
	errcallback.arg = buf;
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;

	/*
	 * Make sure that any user-supplied code runs as the table owner, unless
	 * the user has opted out of that behavior -- as apply_handle_insert()
	 * does.  When we are called from the insert hook we may already be
	 * running as this or another table's owner; nesting is fine.
	 */
	run_as_owner = MySubscription->runasowner;
	if (!run_as_owner)
		SwitchToUntrustedUser(buf->relowner, &ucxt);

	if (!ActiveSnapshotSet())
	{
		PushActiveSnapshot(GetTransactionSnapshot());
		pushed_snapshot = true;
	}

	/* The checks the per-row path does, once per batch. */
	aclresult = pg_class_aclcheck(buf->relid, GetUserId(), ACL_INSERT);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult,
					   get_relkind_objtype(buf->rel->rd_rel->relkind),
					   RelationGetRelationName(buf->rel));
	CheckCmdReplicaIdentity(buf->rel, CMD_INSERT);

	bistate = GetBulkInsertState();
	table_multi_insert(buf->rel, buf->slots, buf->nslots,
					   GetCurrentCommandId(true), 0, bistate);
	FreeBulkInsertState(bistate);

	if (buf->relinfo->ri_NumIndices > 0)
	{
		List	   *conflictindexes = buf->relinfo->ri_onConflictArbiterIndexes;

		for (int i = 0; i < buf->nslots; i++)
		{
			List	   *recheck;
			bool		conflict = false;

			CHECK_FOR_INTERRUPTS();

			/* Index expressions are evaluated in per-tuple memory. */
			ResetPerTupleExprContext(buf->estate);

			recheck = ExecInsertIndexTuples(buf->relinfo, buf->slots[i],
											buf->estate, false,
											conflictindexes != NIL,
											&conflict, conflictindexes,
											false);
			if (conflict)
				report_insert_conflict(buf, buf->slots[i], recheck);
			list_free(recheck);
		}
	}

	elog(DEBUG1, "lr_batch: inserted %d tuples into relation \"%s\"",
		 buf->nslots, RelationGetRelationName(buf->rel));

	if (pushed_snapshot)
		PopActiveSnapshot();

	if (!run_as_owner)
		RestoreUserContext(&ucxt);

	error_context_stack = errcallback.previous;

	/*
	 * Make the rows visible to the rest of the remote transaction.  Without
	 * this, an UPDATE or DELETE of a just-flushed row would find it through
	 * a dirty snapshot and then fail to lock it, its cmin being equal to the
	 * current command ID.
	 */
	CommandCounterIncrement();

	for (int i = 0; i < buf->nslots; i++)
		ExecClearTuple(buf->slots[i]);
	MemoryContextReset(buf->tuple_mcxt);

	buf->nslots = 0;
	total_bytes -= buf->nbytes;
	buf->nbytes = 0;
}

/*
 * Report the unique-index conflict of a just-inserted tuple and raise an
 * error, like CheckAndReportConflict() and FindConflictTuple() do for the
 * per-row path.
 */
static void
report_insert_conflict(LRBatchBuffer *buf, TupleTableSlot *slot,
					   List *recheckIndexes)
{
	List	   *conflicttuples = NIL;

	foreach_oid(uniqueidx, buf->relinfo->ri_onConflictArbiterIndexes)
	{
		ItemPointerData conflictTid;
		TupleTableSlot *conflictslot = NULL;
		TM_FailureData tmfd;
		TM_Result	res;

		if (!list_member_oid(recheckIndexes, uniqueidx))
			continue;

retry:
		if (ExecCheckIndexConstraints(buf->relinfo, slot, buf->estate,
									  &conflictTid, &slot->tts_tid,
									  list_make1_oid(uniqueidx)))
		{
			if (conflictslot)
				ExecDropSingleTupleTableSlot(conflictslot);
			continue;
		}

		if (conflictslot == NULL)
			conflictslot = table_slot_create(buf->rel, NULL);

		PushActiveSnapshot(GetLatestSnapshot());
		res = table_tuple_lock(buf->rel, &conflictTid, GetActiveSnapshot(),
							   conflictslot, GetCurrentCommandId(false),
							   LockTupleShare, LockWaitBlock, 0, &tmfd);
		PopActiveSnapshot();

		switch (res)
		{
			case TM_Ok:
				break;
			case TM_Updated:
			case TM_Deleted:
				/* Concurrently changed; look again. */
				goto retry;
			case TM_Invisible:
				elog(ERROR, "attempted to lock invisible tuple");
				break;
			default:
				elog(ERROR, "unexpected table_tuple_lock status: %u", res);
				break;
		}

		{
			ConflictTupleInfo *conflicttuple = palloc0_object(ConflictTupleInfo);

			conflicttuple->slot = conflictslot;
			conflicttuple->indexoid = uniqueidx;
			GetTupleTransactionInfo(conflictslot, &conflicttuple->xmin,
									&conflicttuple->origin, &conflicttuple->ts);
			conflicttuples = lappend(conflicttuples, conflicttuple);
		}
	}

	if (conflicttuples)
		ReportApplyConflict(buf->estate, buf->relinfo, ERROR,
							list_length(conflicttuples) > 1 ?
							CT_MULTIPLE_UNIQUE_CONFLICTS : CT_INSERT_EXISTS,
							NULL, slot, conflicttuples);
}

/*
 * Release a buffer.  It must be empty.
 */
static void
buffer_destroy(LRBatchBuffer *buf)
{
	int			pos;

	Assert(buf->nslots == 0);

	for (pos = 0; pos < nbuffers; pos++)
		if (buffers[pos] == buf)
			break;
	Assert(pos < nbuffers);
	memmove(&buffers[pos], &buffers[pos + 1],
			sizeof(LRBatchBuffer *) * (nbuffers - pos - 1));
	nbuffers--;
	if (last_buffer == buf)
		last_buffer = NULL;

	/* The slots hold no descriptor pins; dropping them just frees memory. */
	for (int i = 0; i < buf->nslots_made; i++)
		ExecDropSingleTupleTableSlot(buf->slots[i]);

	ExecCloseIndices(buf->relinfo);
	FreeExecutorState(buf->estate);

	/* Keep the lock until the end of the transaction. */
	table_close(buf->rel, NoLock);

	MemoryContextDelete(buf->mcxt);
}

/* Flush all buffers, in creation order, and keep them. */
static void
flush_all(void)
{
	for (int i = 0; i < nbuffers; i++)
		buffer_flush(buffers[i]);
}

/* Flush all buffers and release them. */
static void
flush_and_destroy_all(void)
{
	if (nbuffers == 0)
		return;

	flush_all();
	while (nbuffers > 0)
		buffer_destroy(buffers[nbuffers - 1]);
	Assert(total_bytes == 0);
}
