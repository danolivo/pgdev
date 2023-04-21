/*-------------------------------------------------------------------------
 *
 * replan.c
 *		...
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *		src/backend/tcop/replan.c
 *
 * NOTES
 *		...

 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/hash.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/optimizer.h"
#include "tcop/replan.h"
#include "tcop/tcopprot.h"
#include "utils/snapmgr.h"

int QueryInadequateExecutionTime = -1;
bool ShowNodeHash = false;

PlannedStmt *
try_replan(PlannedStmt *src_stmt)
{
	PlannedStmt		   *stmt;
	ReplanningStuff	   *node = (ReplanningStuff *) src_stmt->replan;
	Query			   *query = copyObject(node->querytree);
	char			   *query_string = node->query_string;
	int					cursorOptions = node->cursorOptions;

	/* Attach hash table with cardinalities learned */
//	query->replanning = (char *) node;

	/* Copy before the push to save original snapshot for usage on a next iteration */
	PushActiveSnapshot(CopySnapshot(node->snapshot));
	stmt = pg_plan_query(query, query_string, cursorOptions, node->boundParams);
	PopActiveSnapshot();
	return stmt;
}
static PlannedStmt *cur_stmt = NULL;
void
check_replan_trigger(PlanState *node)
{
	Assert(node->instrument);
	Assert(!INSTR_TIME_IS_ZERO(node->instrument->counter));
//elog(WARNING, "trigger %.1ld", node->state->es_processed);
	if (node->state->es_processed > 0)
		return;

	if (INSTR_TIME_GET_MILLISEC(node->instrument->counter) >= QueryInadequateExecutionTime)
	{
		cur_stmt = node->state->es_plannedstmt;
		ereport(ERROR,
		(errcode(ERRCODE_INADEQUATE_QUERY_EXECUTION_TIME),
					 errmsg("ERRCODE_INADEQUATE_QUERY_EXECUTION_TIME")));
	}
}

/*
 * TODO: think about case with parallel workers.
 */
static bool
learnOnPlanState(PlanState *p, void *context)
{
	HTAB		   *htab = (HTAB *) context;
	uint64			nodeid = p->plan->nodeid;
	double			cardinality = -1;
	LearningData   *entry;
	bool			found;

	Assert(p->instrument);
	Assert(nodeid != UINT64_MAX);

	planstate_tree_walker(p, learnOnPlanState, context);

	if (nodeid == 0)
		return false;

	InstrEndLoop(p->instrument);
	entry = hash_search(htab, &nodeid, HASH_ENTER, &found);
	if (!found)
		/*
		 * New record initialize with negative constant. Should we initialize it
		 * with p->plan->plan_rows somehow?
		 */
		entry->cardinality = -1;

	if (!p->instrument->running && TupIsNull(p->ps_ResultTupleSlot) &&
		p->instrument->nloops > 0.)
	{
		/* This node has finished its job. We have real number of tuples */
		if (p->chgParam == NULL)
		{
			/*
			 * Even when this node were rescanned we would get the same
			 * cardinality. Save this value.
			 */
			cardinality = p->instrument->ntuples / p->instrument->nloops;
		}
		else if (p->instrument->ntuples / p->instrument->nloops > p->plan->plan_rows)
		{
			/* Save more reliable cardinality */
			cardinality = p->instrument->ntuples / p->instrument->nloops;
		}
	}
	else
	{
		/* Node still in use */
		if (p->instrument->ntuples / p->instrument->nloops > p->plan->plan_rows)
		{
			/* Save more reliable cardinality */
			cardinality = p->instrument->ntuples / p->instrument->nloops;
		}
	}

	if (cardinality > 0)
	{
		entry->cardinality = clamp_row_est(cardinality);
	}
//	elog(WARNING, "Learn node (%lu, %.1lf) (%.1lf, %.1lf, %.1lf)", entry->key.key, entry->cardinality,
//		p->plan->plan_rows, p->instrument->ntuples, p->instrument->nloops);

	return false;
}

bool
learn_partially_executed_state(PlannedStmt *stmt)
{
	ReplanningStuff	   *replan;
	PlanState		   *state;

	stmt = cur_stmt;
	Assert(stmt->replan);
//elog(WARNING, "ABC");
	replan = (ReplanningStuff *) stmt->replan;
	state = replan->queryDesc->planstate;

	if (replan->data == NULL)
	{
		HTAB			   *htab;
		HASHCTL				hash_ctl;
		MemoryContext		oldmemctx;

		oldmemctx = MemoryContextSwitchTo(replan->mctx);

		/* Create the hashtable proper */
		MemSet(&hash_ctl, 0, sizeof(hash_ctl));
		hash_ctl.keysize = sizeof(LearningDataKey);
		hash_ctl.entrysize = sizeof(LearningData);
		hash_ctl.hcxt = replan->mctx;

		htab = hash_create("Replanning HTAB", 32, &hash_ctl,
							HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
		replan->data = (void *) htab;
		MemoryContextSwitchTo(oldmemctx);
	}

	(void ) learnOnPlanState(state, (void *) replan->data);
	return true;
}

static int
uint8cmp(const void *a, const void *b)
{
	uint64		*a1 = (uint64 *) a;
	uint64		*b1 = (uint64 *) b;

	if (*a1 > *b1)
		return 1;
	else if (*a1 == *b1)
		return 0;
	else
		return -1;
}

static uint64
extract_clauses(List *rinfos, List *reloids, List *subsigns)
{
	ListCell   *lc;
	int			nhashes = list_length(rinfos) + list_length(reloids) + list_length(subsigns);
	uint64	   *hashes = palloc0(nhashes * sizeof(*hashes));
	int			i = 0;
	uint64		res;

	foreach(lc, rinfos)
	{
		RestrictInfo   *rinfo = lfirst_node(RestrictInfo, lc);
		char		   *str;

		/*
		 * Unfortunately, we can't use sort of query jumbling here. Maybe it's
		 * a time to invent it?
		 * XXX: Don't mind orclause here because it is the same, but with a
		 * RestrictInfo node added on each OR expression.
		 */
		str = (char *) nodeToString(rinfo->clause);
		hashes[i++] = hash_bytes_extended((const unsigned char *) str,
										  strlen(str), 0);
		pfree(str);
	}
	qsort(hashes, i, sizeof(*hashes), uint8cmp);

	/* Add reloids to the list. Don't need to sort it with current prerequisities */
	foreach(lc, reloids)
	{
		Oid reloid = lfirst_oid(lc);

		hashes[i++] = (uint64) reloid; /* can hash this value, but not really needed so far */
	}

	/* Add hashes of child rels. shouldn't be sorted to feel changes of outer and inner */
	foreach(lc, subsigns)
	{
		uint64 *value = (uint64 *) lfirst(lc);

		hashes[i++] = *value; /* can hash this value, but not really needed so far */
	}
	res = hash_bytes_extended((const unsigned char *) hashes,
							   nhashes * sizeof(*hashes), 0);
	/* UINT64_MAX means uninitialized, 0 - inserted in vitro, without planning. */
	if (res == UINT64_MAX)
		return 1;
	if (res == 0)
		return 2;
	return res;
}

static List *
get_relids_list(PlannerInfo *root, RelOptInfo *rel)
{
	int	index = rel->relid;
	List   *relids = NIL;

	if (index > 0)
		return list_make1_oid(root->simple_rte_array[index]->relid);

	index = -1;
	while ((index = bms_next_member(rel->relids, index)) >= 0)
	{
		Oid value;
		Assert(index > 0);

		if (OidIsValid(root->simple_rte_array[index]->relid))
			value = root->simple_rte_array[index]->relid;
		else
		{
			/* Not a table source. Use the same logic as the JumbleRangeTable
			 * routine.
			 */
			switch (root->simple_rte_array[index]->rtekind)
			{
			case RTE_SUBQUERY:
				value = root->simple_rel_array[index]->hash;
				break;
			case RTE_JOIN:
				value = root->simple_rte_array[index]->jointype;
				break;
			case RTE_FUNCTION:
			{
				List *l = root->simple_rte_array[index]->functions;
				char *functions = nodeToString(l);

				value = (Oid) hash_bytes((const unsigned char *) functions, strlen(functions));
				pfree(functions);
			}
				break;
			case RTE_TABLEFUNC:
			{
				TableFunc *l = root->simple_rte_array[index]->tablefunc;
				char *tablefunc = nodeToString(l);

				value = (Oid) hash_bytes((const unsigned char *) tablefunc, strlen(tablefunc));
				pfree(tablefunc);
			}
				break;
			case RTE_VALUES:
			{
				List *l = root->simple_rte_array[index]->values_lists;
				char *values_lists = nodeToString(l);

				value = (Oid) hash_bytes((const unsigned char *) values_lists, strlen(values_lists));
				pfree(values_lists);
			}
				break;
			case RTE_CTE:
			{
				char *ctename = root->simple_rte_array[index]->ctename;

				Assert(ctename && strlen(ctename) > 0);
				value = (Oid) hash_bytes((const unsigned char *) ctename, strlen(ctename));
			}
				break;
			case RTE_NAMEDTUPLESTORE:
			{
				char *enrname = root->simple_rte_array[index]->enrname;

				Assert(enrname && strlen(enrname) > 0);
				value = (Oid) hash_bytes((const unsigned char *) enrname, strlen(enrname));
			}
				break;
			case RTE_RESULT:
				value = 1;
				break;
			default:
				elog(PANIC, "Unexpected RangeTblEntry: %d", root->simple_rte_array[index]->rtekind);
			}
		}
		Assert(OidIsValid(value));
		relids = lappend_oid(relids, value);
	}

	return relids;
}

static void
find_subsequent_rels(Path *path, RelOptInfo *prel, List **rels)
{

	Assert(path != NULL && rels != NULL && prel != NULL);

	if (prel != path->parent)
	{
		/* In accordance with my current knowledge it shouldn't happen */
//		Assert(path->parent->hash != UINT64_MAX);

		*rels = lappend(*rels, (void *) &path->parent->hash);
		return;
	}

	/* Get signatures of underlying RelOptInfos */
	switch (nodeTag(path))
	{
		case T_Path:
			switch (path->pathtype)
			{
				/* It is a leaf node. Don't have underlying rels */
				case T_SeqScan:
				case T_SampleScan:
				case T_FunctionScan:
				case T_TableFuncScan:
				case T_ValuesScan:
				case T_CteScan:
				case T_NamedTuplestoreScan:
				case T_Result:
				case T_WorkTableScan:
					break;
				default:
					elog(PANIC, "Unknown node");
			}
			break;
		case T_IndexPath:
		case T_BitmapHeapPath:
			/* Have underlying bitmapqual paths, but as a part of the same rel */
		case T_BitmapAndPath:
		case T_BitmapOrPath:
		case T_TidPath:
		case T_TidRangePath:
			break;
		case T_SubqueryScanPath:
		{
			Path *subpath = ((SubqueryScanPath *) path)->subpath;
			find_subsequent_rels(subpath, prel, rels);
		}
			break;
		case T_ForeignPath:
		{
			Path *subpath = ((ForeignPath *) path)->fdw_outerpath;
			if (subpath)
				find_subsequent_rels(subpath, prel, rels);
		}
			break;
		case T_CustomPath:
		{
			List *subpaths = ((CustomPath *) path)->custom_paths;
			if (subpaths != NIL)
			{
				ListCell *lc;

				foreach (lc, subpaths)
				{
					Path *subpath = (Path *) lfirst(lc);
					find_subsequent_rels(subpath, prel, rels);
				}
			}

		}
			break;
		/* JOINs */
		case T_NestPath:
		case T_MergePath:
		case T_HashPath:
		{
			find_subsequent_rels(((JoinPath *) path)->innerjoinpath, prel, rels);
			find_subsequent_rels(((JoinPath *) path)->outerjoinpath, prel, rels);
		}
			break;
		case T_AppendPath:
		{
			List *subpaths = ((AppendPath *) path)->subpaths;
			if (subpaths != NIL)
			{
				ListCell *lc;

				foreach (lc, subpaths)
				{
					Path *subpath = (Path *) lfirst(lc);
					find_subsequent_rels(subpath, prel, rels);
				}
			}

		}
			break;
		case T_MergeAppendPath:
		{
			List *subpaths = ((MergeAppendPath *) path)->subpaths;
			if (subpaths != NIL)
			{
				ListCell *lc;

				foreach (lc, subpaths)
				{
					Path *subpath = (Path *) lfirst(lc);
					find_subsequent_rels(subpath, prel, rels);
				}
			}

		}
			break;
		case T_GroupResultPath:
			break;
		case T_MaterialPath:
			find_subsequent_rels(((MaterialPath *) path)->subpath, prel, rels);
			break;
		case T_MemoizePath:
			find_subsequent_rels(((MemoizePath *) path)->subpath, prel, rels);
			break;
		case T_UniquePath:
			find_subsequent_rels(((UniquePath *) path)->subpath, prel, rels);
			break;
		case T_GatherPath:
			find_subsequent_rels(((GatherPath *) path)->subpath, prel, rels);
			break;
		case T_GatherMergePath:
			find_subsequent_rels(((GatherMergePath *) path)->subpath, prel, rels);
			break;
		case T_ProjectionPath:
			find_subsequent_rels(((ProjectionPath *) path)->subpath, prel, rels);
			break;
		case T_ProjectSetPath:
			find_subsequent_rels(((ProjectSetPath *) path)->subpath, prel, rels);
			break;
		case T_SortPath:
		case T_IncrementalSortPath:
			find_subsequent_rels(((SortPath *) path)->subpath, prel, rels);
			break;
		case T_GroupPath:
			find_subsequent_rels(((GroupPath *) path)->subpath, prel, rels);
			break;
		case T_UpperUniquePath:
			find_subsequent_rels(((UpperUniquePath *) path)->subpath, prel, rels);
			break;
		case T_AggPath:
			find_subsequent_rels(((AggPath *) path)->subpath, prel, rels);
			break;
		case T_GroupingSetsPath:
			find_subsequent_rels(((GroupingSetsPath *) path)->subpath, prel, rels);
			break;
		case T_MinMaxAggPath:
			break;
		case T_WindowAggPath:
			find_subsequent_rels(((WindowAggPath *) path)->subpath, prel, rels);
			break;
		case T_SetOpPath:
			find_subsequent_rels(((SetOpPath *) path)->subpath, prel, rels);
			break;
		case T_RecursiveUnionPath:
			find_subsequent_rels(((RecursiveUnionPath *) path)->leftpath, prel, rels);
			find_subsequent_rels(((RecursiveUnionPath *) path)->rightpath, prel, rels);
			break;
		case T_LockRowsPath:
			find_subsequent_rels(((LockRowsPath *) path)->subpath, prel, rels);
			break;
		case T_ModifyTablePath:
			find_subsequent_rels(((ModifyTablePath *) path)->subpath, prel, rels);
			break;
		case T_LimitPath:
			find_subsequent_rels(((LimitPath *) path)->subpath, prel, rels);
			break;
		default:
			elog(PANIC, "Unknown node");
	}
}

/*
 * TODO: implement subplans
 */
uint64
generate_signature(PlannerInfo *root, RelOptInfo *rel)
{
	uint64	signature = -1;
	List   *relids;
	List   *subsigns = NIL;
	List   *rinfos = NIL;

	if (rel->reloptkind == RELOPT_UPPER_REL)
		return 0;

	/* Make a node signature */
	relids = get_relids_list(root, rel);
	rinfos = list_concat(rinfos, rel->baserestrictinfo);
	rinfos = list_concat(rinfos, rel->joininfo);

	if (rel->cheapest_total_path)
		find_subsequent_rels(rel->cheapest_total_path, rel, &subsigns);

	signature = extract_clauses(rinfos, relids, subsigns);

	/*
	 * Two or more connected path nodes implement this RelOptInfo.
	 * TODO: in the case of bushy path tree here it will be asserted.
	 */
//	Assert(rel->hash == UINT64_MAX || rel->hash == signature);

	return (signature != 0) ? signature : 1;
}


/* *****************************************************************************
 *
 * Cardinality Estimation Hooks
 *
 * ****************************************************************************/

double
replan_rows_estimate(PlannerInfo *root, RelOptInfo *rel)
{
	ReplanningStuff	   *replan = (ReplanningStuff *) root->parse->replanning;
	HTAB			   *htab;
	LearningData	   *entry;
	LearningDataKey		key;
	bool				found;

	if (replan == NULL)
		return -2;

	htab = (HTAB *) replan->data;
	if (htab == NULL)
		return -3;

	key.key = generate_signature(root, rel);

	entry = hash_search(htab, &key, HASH_FIND, &found);

	if (!found)
		return -1;

	/* Save hash value into the RelOptInfo as a sign that we've made a decision */
	rel->hash = key.key;
	return entry->cardinality;
}
