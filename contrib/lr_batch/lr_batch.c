/*-------------------------------------------------------------------------
 *
 * lr_batch.c
 *		Batch remote INSERTs in the logical replication apply worker.
 *
 * The module plugs into the apply worker through two hooks:
 *
 * logicalrep_insert_hook takes over a remote INSERT once the worker has
 * converted it into a slot.  If the subscription and the target relation
 * are eligible, the tuple is checked the same way ExecSimpleRelationInsert()
 * would check it (stored generated columns, NOT NULL, CHECK and partition
 * constraints) and appended to an in-memory buffer.  Consecutive INSERTs
 * into the same relation accumulate in the buffer, which is flushed with
 * table_multi_insert() plus per-tuple ExecInsertIndexTuples().
 *
 * logicalrep_message_hook is the flush barrier: every message other than an
 * INSERT -- UPDATE, DELETE, TRUNCATE, RELATION, TYPE, COMMIT, PREPARE, ... --
 * flushes the buffer and releases it before the message is handled, so that
 * later changes and the commit see the inserted rows.
 *
 * A flush can be triggered by any message, so it sets up its own execution
 * context: it runs as the table owner unless the subscription has
 * run_as_owner = true (index expressions and partial-index predicates are
 * evaluated there), pushes a snapshot if none is active, and increments the
 * command counter afterwards, as end_replication_step() does.
 *
 * The buffer lives in a child of TopTransactionContext and its relation and
 * tuple descriptor references are owned by the transaction's resource
 * owner, so an aborted transaction cleans it up without our help; the
 * transaction callback only forgets the pointer.  At pre-commit a non-empty
 * buffer is a bug (a commit path that bypassed the barrier), and we raise an
 * error rather than lose rows.
 *
 * Limitations:
 *
 * - Batching is disabled for subscriptions with streaming enabled, and in
 *   parallel apply workers: a streamed transaction replayed from a spool
 *   file is committed without a COMMIT message passing through the barrier.
 *
 * - A unique violation against a pre-existing subscriber row is reported
 *   as an insert_exists (or multiple_unique_conflicts) conflict and counted
 *   in pg_stat_subscription_stats, as in the per-row path, but the error
 *   context names the message that triggered the flush rather than the
 *   INSERT itself.
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

/*
 * Whether batching is enabled for the subscription of this worker.  The
 * answer is cached per subscription OID and recomputed when
 * lr_batch.subscriptions changes.  Any subscription option that affects it
 * (name, streaming) restarts the worker when changed.
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

/* The one active buffer, or NULL. */
typedef struct LRBatchBuffer
{
	MemoryContext mcxt;			/* child of TopTransactionContext; holds
								 * everything below */
	MemoryContext tuple_mcxt;	/* child of mcxt; buffered tuples, reset at
								 * every flush */
	Oid			relid;
	Oid			relowner;
	Relation	rel;			/* our own reference, released at destroy */
	EState	   *estate;			/* for index insertion */
	ResultRelInfo *relinfo;		/* with indexes open */
	TupleTableSlot **slots;
	int			nslots;			/* number of buffered tuples */
	int			nslots_made;	/* number of slots created so far */
	int			slots_len;		/* allocated length of slots[] */
	Size		nbytes;			/* size of the buffered tuples */
} LRBatchBuffer;

static LRBatchBuffer *buffer = NULL;

void		_PG_init(void);

static bool lr_batch_insert_hook(LogicalRepRelMapEntry *rel,
								 ResultRelInfo *relinfo,
								 EState *estate,
								 TupleTableSlot *remoteslot);
static void lr_batch_message_hook(LogicalRepMsgType action);
static void lr_batch_xact_callback(XactEvent event, void *arg);
static void lr_batch_relcache_callback(Datum arg, Oid relid);
static bool check_subscriptions(char **newval, void **extra, GucSource source);
static void assign_subscriptions(const char *newval, void *extra);

static bool batching_enabled(void);
static bool relation_is_eligible(Relation rel);
static bool compute_relation_eligibility(Relation rel);
static void buffer_init(LogicalRepRelMapEntry *rel);
static void buffer_add(TupleTableSlot *remoteslot);
static void buffer_flush(void);
static void buffer_destroy(void);
static void buffer_flush_and_destroy(void);
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

	RegisterXactCallback(lr_batch_xact_callback, NULL);
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

	if (result && MySubscription->stream != LOGICALREP_STREAM_OFF)
	{
		ereport(LOG,
				(errmsg("lr_batch: batching is disabled for subscription \"%s\" because streaming is enabled",
						MySubscription->name),
				 errhint("Set streaming = off for the subscription to enable batching.")));
		result = false;
	}

	/* Parallel apply workers only exist for streamed transactions. */
	if (am_parallel_apply_worker())
		result = false;

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
	Oid			relid = RelationGetRelid(localrel);

	/*
	 * While a buffer for this relation exists we hold a lock that blocks
	 * any DDL which could change its eligibility, so skip the checks.
	 */
	if (buffer == NULL || buffer->relid != relid)
	{
		if (!batching_enabled() || !relation_is_eligible(localrel))
		{
			/*
			 * The per-row path is going to insert this tuple.  Flush first:
			 * triggers or index functions on this relation may look at the
			 * rows buffered for another one.
			 */
			buffer_flush_and_destroy();

			if (prev_insert_hook)
				return prev_insert_hook(rel, relinfo, estate, remoteslot);
			return false;
		}

		/* Relation switch */
		buffer_flush_and_destroy();
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

	if (buffer == NULL)
		buffer_init(rel);

	buffer_add(remoteslot);

	return true;
}

/*
 * logicalrep_message_hook: the flush barrier.
 */
static void
lr_batch_message_hook(LogicalRepMsgType action)
{
	if (action != LOGICAL_REP_MSG_INSERT)
		buffer_flush_and_destroy();

	if (prev_message_hook)
		prev_message_hook(action);
}

static void
lr_batch_xact_callback(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_PRE_COMMIT:
		case XACT_EVENT_PARALLEL_PRE_COMMIT:
		case XACT_EVENT_PRE_PREPARE:
			if (buffer != NULL)
			{
				if (buffer->nslots > 0)
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("lr_batch: %d buffered tuples for relation \"%s\" were not flushed before commit",
									buffer->nslots,
									RelationGetRelationName(buffer->rel))));
				buffer_destroy();
			}
			break;

		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:

			/*
			 * The buffer's memory is a child of TopTransactionContext and its
			 * relation and tupdesc references belong to the transaction's
			 * resource owner; abort releases both.
			 */
			buffer = NULL;
			break;

		default:
			break;
	}
}

/*
 * Create the buffer for rel.
 *
 * If anything here fails, the transaction aborts, which releases what has
 * been allocated so far; buffer is set only at the end.
 */
static void
buffer_init(LogicalRepRelMapEntry *rel)
{
	MemoryContext mcxt;
	MemoryContext oldctx;
	LRBatchBuffer *buf;

	Assert(buffer == NULL);
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

	buf->slots_len = Min(64, lr_batch_max_tuples);
	buf->slots = palloc(sizeof(TupleTableSlot *) * buf->slots_len);

	MemoryContextSwitchTo(oldctx);

	buffer = buf;
}

/*
 * Append a copy of remoteslot to the buffer; flush if it is full.
 *
 * The copy is formed once, as a heap tuple owned by the slot, so that
 * table_multi_insert() can use it directly without materializing it again.
 */
static void
buffer_add(TupleTableSlot *remoteslot)
{
	LRBatchBuffer *buf = buffer;
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
			MakeSingleTupleTableSlot(RelationGetDescr(buf->rel),
									 &TTSOpsHeapTuple);
		MemoryContextSwitchTo(oldctx);
	}
	dst = buf->slots[buf->nslots];

	oldctx = MemoryContextSwitchTo(buf->tuple_mcxt);
	tuple = ExecCopySlotHeapTuple(remoteslot);
	MemoryContextSwitchTo(oldctx);
	ExecStoreHeapTuple(tuple, dst, true);

	buf->nslots++;
	buf->nbytes += tuple->t_len;

	if (buf->nslots >= lr_batch_max_tuples ||
		buf->nbytes >= (Size) lr_batch_max_bytes * 1024)
		buffer_flush();
}

/*
 * Insert the buffered tuples and empty the buffer.  The buffer itself stays.
 */
static void
buffer_flush(void)
{
	LRBatchBuffer *buf = buffer;
	bool		run_as_owner;
	UserContext ucxt;
	bool		pushed_snapshot = false;
	AclResult	aclresult;
	BulkInsertState bistate;

	if (buf == NULL || buf->nslots == 0)
		return;

	Assert(IsTransactionState());

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
 * Release the buffer.  It must be empty.
 */
static void
buffer_destroy(void)
{
	LRBatchBuffer *buf = buffer;

	if (buf == NULL)
		return;

	Assert(buf->nslots == 0);

	/* Drop the slots first, to release their tupdesc references. */
	for (int i = 0; i < buf->nslots_made; i++)
		ExecDropSingleTupleTableSlot(buf->slots[i]);

	ExecCloseIndices(buf->relinfo);
	FreeExecutorState(buf->estate);

	/* Keep the lock until the end of the transaction. */
	table_close(buf->rel, NoLock);

	buffer = NULL;
	MemoryContextDelete(buf->mcxt);
}

static void
buffer_flush_and_destroy(void)
{
	if (buffer == NULL)
		return;

	buffer_flush();
	buffer_destroy();
}
