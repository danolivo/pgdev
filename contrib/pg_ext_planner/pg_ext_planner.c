/*--------------------------------------------------------------------------------
 * pg_ext_planner.c
 *		A library to provide core Postgres with unconventional paths that may
 *		be more effective.
 *
 * Memoize Subplan
 *
 * Pass through the path tree at the end of optimisation stage and find
 * any not transformed subplans.
 * Insert into the head of such (parameterised) subplan Memoize node to reduce
 * number of the subplan evaluations if predicted that the incoming parameter
 * set may have multiple duplicates.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  contrib/pg_ext_planner/pg_ext_planner.c
 *--------------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_operator.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/cost.h" /* enable_memoize */
#include "optimizer/optimizer.h"
#include "optimizer/paths.h"
#include "optimizer/pathnode.h"
#include "optimizer/planner.h"
#include "optimizer/restrictinfo.h"
#include "parser/parse_collate.h"
#include "parser/parse_coerce.h"
#include "parser/parse_oper.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/numeric.h"
#include "utils/syscache.h"
#include "math.h"

#define	EXTENSION_NAME	"pg_ext_planner"

PG_MODULE_MAGIC_EXT(
					.name = EXTENSION_NAME,
					.version = "0.1.0"
);

void		_PG_init(void);

static bool enable_memoize_subplan = true;
static bool force_memoize_subplan = false;

static create_upper_paths_hook_type upper_paths_hook_next = NULL;

static void upper_paths_hook(PlannerInfo *root, UpperRelationKind stage,
							 RelOptInfo *input_rel, RelOptInfo *output_rel,
							 void *extra);

#include "utils/selfuncs.h"
#include "utils/typcache.h"
#include "optimizer/planmain.h"
#include "miscadmin.h"
#include "executor/nodeMemoize.h"

typedef struct PathWalkerCtx
{
	PlannerInfo *root;
	double		calls;
}			PathWalkerCtx;

static bool subplan_hunter_path_walker(Node *node, void *context);


#define WALK(n) walker((Node *) (n), context)
#define LIST_WALK(l) path_tree_walker((Node *) (l), walker, context)

static bool
path_tree_walker(Node *node, tree_walker_callback walker, void *context)

{
	ListCell   *temp;

	if (node == NULL)
		return false;

	switch (nodeTag(node))
	{
		case T_List:
			foreach(temp, (List *) node)
			{
				if (WALK(lfirst(temp)))
					return true;
			}
			break;
		case T_Path:
		case T_IndexPath:
		case T_TidPath:
		case T_TidRangePath:
		case T_GroupResultPath:
#if PG_VERSION_NUM >= 170000
		case T_JsonTablePath:
#endif
			/* End point */
			break;
		case T_BitmapHeapPath:
			{
				BitmapHeapPath *path = (BitmapHeapPath *) node;

				if (LIST_WALK(path->bitmapqual))
					return true;
				break;
			}
		case T_BitmapAndPath:
			{
				BitmapAndPath *path = (BitmapAndPath *) node;

				if (LIST_WALK(path->bitmapquals))
					return true;
				break;
			}
		case T_BitmapOrPath:
			{
				BitmapOrPath *path = (BitmapOrPath *) node;

				if (LIST_WALK(path->bitmapquals))
					return true;
				break;
			}
		case T_CustomPath:
			{
				CustomPath *path = (CustomPath *) node;

				if (LIST_WALK(path->custom_paths))
					return true;
				break;
			}
		case T_AppendPath:
			{
				AppendPath *path = (AppendPath *) node;

				if (LIST_WALK(path->subpaths))
					return true;
				break;
			}
		case T_MergeAppendPath:
			{
				MergeAppendPath *path = (MergeAppendPath *) node;

				if (LIST_WALK(path->subpaths))
					return true;
				break;
			}
		case T_SubqueryScanPath:
			{
				SubqueryScanPath *path = (SubqueryScanPath *) node;

				if (WALK(path->subpath))
					return true;
				break;
			}
		case T_GroupPath:
			{
				GroupPath  *path = (GroupPath *) node;

				if (WALK(path->subpath))
					return true;
				break;
			}
		case T_LimitPath:
			{
				LimitPath  *path = (LimitPath *) node;

				if (WALK(path->subpath))
					return true;
				break;
			}
		case T_AggPath:
			{
				AggPath    *path = (AggPath *) node;

				if (WALK(path->subpath))
					return true;
				break;
			}
		case T_MemoizePath:
			{
				MemoizePath *path = (MemoizePath *) node;

				if (WALK(path->subpath))
					return true;
				break;
			}
		case T_SortPath:
			{
				SortPath   *path = (SortPath *) node;

				if (WALK(path->subpath))
					return true;
				break;
			}
		case T_IncrementalSortPath:
			{
				IncrementalSortPath *path = (IncrementalSortPath *) node;

				if (WALK(path->spath.subpath))
					return true;
				break;
			}
		case T_MaterialPath:
			{
				MaterialPath *path = (MaterialPath *) node;

				if (WALK(path->subpath))
					return true;
				break;
			}
		case T_LockRowsPath:
			{
				LockRowsPath *path = (LockRowsPath *) node;

				if (WALK(path->subpath))
					return true;
				break;
			}
		case T_ModifyTablePath:
			{
				ModifyTablePath *path = (ModifyTablePath *) node;

				if (WALK(path->subpath))
					return true;
				break;
			}
		case T_ForeignPath:
			{
				ForeignPath *path = (ForeignPath *) node;

				if (WALK(path->fdw_outerpath))
					return true;
				break;
			}
		case T_UniquePath:
			{
				UniquePath *path = (UniquePath *) node;

				if (WALK(path->subpath))
					return true;
				break;
			}
		case T_GatherPath:
			{
				GatherPath *path = (GatherPath *) node;

				if (WALK(path->subpath))
					return true;
				break;
			}
		case T_GatherMergePath:
			{
				GatherMergePath *path = (GatherMergePath *) node;

				if (WALK(path->subpath))
					return true;
				break;
			}
		case T_ProjectionPath:
			{
				ProjectionPath *path = (ProjectionPath *) node;

				if (WALK(path->subpath))
					return true;
				break;
			}
		case T_ProjectSetPath:
			{
				ProjectSetPath *path = (ProjectSetPath *) node;

				if (WALK(path->subpath))
					return true;
				break;
			}
		case T_UpperUniquePath:
			{
				UpperUniquePath *path = (UpperUniquePath *) node;

				if (WALK(path->subpath))
					return true;
				break;
			}
		case T_GroupingSetsPath:
			{
				GroupingSetsPath *path = (GroupingSetsPath *) node;

				if (WALK(path->subpath))
					return true;
				break;
			}
		case T_MinMaxAggPath:
			{
				MinMaxAggPath *path = (MinMaxAggPath *) node;
				ListCell   *lc;

				foreach(lc, path->mmaggregates)
				{
					MinMaxAggInfo *mminfo = (MinMaxAggInfo *) lfirst(lc);

					if (WALK(mminfo->path))
						return true;
				}
				break;
			}
		case T_WindowAggPath:
			{
				WindowAggPath *path = (WindowAggPath *) node;

				if (WALK(path->subpath))
					return true;
				break;
			}
		case T_RecursiveUnionPath:
			{
				RecursiveUnionPath *path = (RecursiveUnionPath *) node;

				if (WALK(path->leftpath))
					return true;
				if (WALK(path->rightpath))
					return true;
				break;
			}
		case T_SetOpPath:
			{
				SetOpPath  *path = (SetOpPath *) node;
#if PG_VERSION_NUM < 180000
				if (WALK(path->subpath))
					return true;
#else
				if (WALK(path->leftpath))
					return true;
				if (WALK(path->rightpath))
					return true;
#endif
				break;
			}
		case T_NestPath:
			{
				NestPath   *path = (NestPath *) node;

				if (WALK(path->jpath.innerjoinpath))
					return true;
				if (WALK(path->jpath.outerjoinpath))
					return true;
				break;
			}
		case T_MergePath:
			{
				MergePath  *path = (MergePath *) node;

				if (WALK(path->jpath.innerjoinpath))
					return true;
				if (WALK(path->jpath.outerjoinpath))
					return true;
				break;
			}
		case T_HashPath:
			{
				HashPath   *path = (HashPath *) node;

				if (WALK(path->jpath.innerjoinpath))
					return true;
				if (WALK(path->jpath.outerjoinpath))
					return true;
				break;
			}
		default:
			elog(ERROR, "unrecognized node type: %d",
				 (int) nodeTag(node));
			break;
	}
	return false;
}

#undef LIST_WALK
#undef WALK

static double
relation_byte_size(double tuples, int width)
{
	return tuples * (MAXALIGN(width) + MAXALIGN(SizeofHeapTupleHeader));
}

/*
 * get_expr_width
 *		Estimate the width of the given expr attempting to use the width
 *		cached in a Var's owning RelOptInfo, else fallback on the type's
 *		average width when unable to or when the given Node is not a Var.
 */
static int32
get_expr_width(const Node *expr)
{
	int32		width;

	width = get_typavgwidth(exprType(expr), exprTypmod(expr));
	Assert(width > 0);
	return width;
}

/*
 * cost_memoize_rescan
 *	  Determines the estimated cost of rescanning a Memoize node.
 *
 * In order to estimate this, we must gain knowledge of how often we expect to
 * be called and how many distinct sets of parameters we are likely to be
 * called with. If we expect a good cache hit ratio, then we can set our
 * costs to account for that hit ratio, plus a little bit of cost for the
 * caching itself.  Caching will not work out well if we expect to be called
 * with too many distinct parameter values.  The worst-case here is that we
 * never see any parameter value twice, in which case we'd never get a cache
 * hit and caching would be a complete waste of effort.
 */
static void
cost_memoize_rescan(PlannerInfo *root, MemoizePath *mpath,
					List *exprs,
					Cost *rescan_startup_cost, Cost *rescan_total_cost)
{
	EstimationInfo estinfo;
	ListCell   *lc;
	Cost		input_startup_cost = mpath->subpath->startup_cost;
	Cost		input_total_cost = mpath->subpath->total_cost;
	double		tuples = mpath->subpath->rows;
	double		calls = mpath->calls;
	int			width = mpath->subpath->pathtarget->width;

	double		hash_mem_bytes;
	double		est_entry_bytes;
	double		est_cache_entries;
	double		ndistinct = 0;
	double		evict_ratio;
	double		hit_ratio;
	Cost		startup_cost;
	Cost		total_cost;

	/* available cache space */
	hash_mem_bytes = get_hash_memory_limit();

	/*
	 * Set the number of bytes each cache entry should consume in the cache.
	 * To provide us with better estimations on how many cache entries we can
	 * store at once, we make a call to the executor here to ask it what
	 * memory overheads there are for a single cache entry.
	 */
	est_entry_bytes = relation_byte_size(tuples, width) +
		ExecEstimateCacheEntryOverheadBytes(tuples);

	/* include the estimated width for the cache keys */
	foreach(lc, mpath->param_exprs)
		est_entry_bytes += get_expr_width((Node *) lfirst(lc));

	/* estimate on the upper limit of cache entries we can hold at once */
	est_cache_entries = floor(hash_mem_bytes / est_entry_bytes);

	/* estimate on the distinct number of parameter values */
	ndistinct = estimate_num_groups(root, exprs, calls, NULL,
									&estinfo);

	/* TODO: rethink this place */
	if ((estinfo.flags & SELFLAG_USED_DEFAULT) != 0)
		ndistinct = calls;

	/*
	 * Since we've already estimated the maximum number of entries we can
	 * store at once and know the estimated number of distinct values we'll be
	 * called with, we'll take this opportunity to set the path's est_entries.
	 * This will ultimately determine the hash table size that the executor
	 * will use.  If we leave this at zero, the executor will just choose the
	 * size itself.  Really this is not the right place to do this, but it's
	 * convenient since everything is already calculated.
	 */
	mpath->est_entries = Min(Min(ndistinct, est_cache_entries),
							 PG_UINT32_MAX);

	/*
	 * When the number of distinct parameter values is above the amount we can
	 * store in the cache, then we'll have to evict some entries from the
	 * cache.  This is not free. Here we estimate how often we'll incur the
	 * cost of that eviction.
	 */
	evict_ratio = 1.0 - Min(est_cache_entries, ndistinct) / ndistinct;

	/*
	 * In order to estimate how costly a single scan will be, we need to
	 * attempt to estimate what the cache hit ratio will be.  To do that we
	 * must look at how many scans are estimated in total for this node and
	 * how many of those scans we expect to get a cache hit.
	 */
	hit_ratio = ((calls - ndistinct) / calls) *
		(est_cache_entries / Max(ndistinct, est_cache_entries));

	Assert(hit_ratio >= 0 && hit_ratio <= 1.0);

	/*
	 * Set the total_cost accounting for the expected cache hit ratio.  We
	 * also add on a cpu_operator_cost to account for a cache lookup. This
	 * will happen regardless of whether it's a cache hit or not.
	 */
	total_cost = input_total_cost * (1.0 - hit_ratio) + cpu_operator_cost;

	/* Now adjust the total cost to account for cache evictions */

	/* Charge a cpu_tuple_cost for evicting the actual cache entry */
	total_cost += cpu_tuple_cost * evict_ratio;

	/*
	 * Charge a 10th of cpu_operator_cost to evict every tuple in that entry.
	 * The per-tuple eviction is really just a pfree, so charging a whole
	 * cpu_operator_cost seems a little excessive.
	 */
	total_cost += cpu_operator_cost / 10.0 * evict_ratio * tuples;

	/*
	 * Now adjust for storing things in the cache, since that's not free
	 * either.  Everything must go in the cache.  We don't proportion this
	 * over any ratio, just apply it once for the scan.  We charge a
	 * cpu_tuple_cost for the creation of the cache entry and also a
	 * cpu_operator_cost for each tuple we expect to cache.
	 */
	total_cost += cpu_tuple_cost + cpu_operator_cost * tuples;

	/*
	 * Getting the first row must be also be proportioned according to the
	 * expected cache hit ratio.
	 */
	startup_cost = input_startup_cost * (1.0 - hit_ratio);

	/*
	 * Additionally we charge a cpu_tuple_cost to account for cache lookups,
	 * which we'll do regardless of whether it was a cache hit or not.
	 */
	startup_cost += cpu_tuple_cost;

	*rescan_startup_cost = startup_cost;
	*rescan_total_cost = total_cost;
}

/*
 * Just a simplified copy from of the core code. It looks like a quite stable
 * code. We need to revise it if only the MemoizePath code will be changed.
 */
static bool
paraminfo_get_equal_hashops(List *ph_lateral_vars, List **param_exprs,
							List **operators, bool *binary_mode)

{
	ListCell   *lc;

	*param_exprs = NIL;
	*operators = NIL;
	*binary_mode = false;

	foreach(lc, ph_lateral_vars)
	{
		Node	   *expr = (Node *) lfirst(lc);
		TypeCacheEntry *typentry;

		/* Reject if there are any volatile functions in lateral vars */
		if (contain_volatile_functions(expr))
		{
			list_free(*operators);
			list_free(*param_exprs);
			return false;
		}

		typentry = lookup_type_cache(exprType(expr),
									 TYPECACHE_HASH_PROC | TYPECACHE_EQ_OPR);

		/* can't use memoize without a valid hash proc and equals operator */
		if (!OidIsValid(typentry->hash_proc) || !OidIsValid(typentry->eq_opr))
		{
			list_free(*operators);
			list_free(*param_exprs);
			return false;
		}

		*operators = lappend_oid(*operators, typentry->eq_opr);
		*param_exprs = lappend(*param_exprs, expr);

		*binary_mode = true;
	}

	/* We're okay to use memoize */
	return true;
}

/*
 * Recursively pass through the root's subqueries and detect grouping set.
 * If it is found, imemdiately return.
 */
static bool
contains_grouping_sets(const PlannerInfo *root)
{
	int i;

	if (root->parse->groupingSets != NIL)
		return true;

	for (i = 1; i < root->simple_rel_array_size; i++)
	{
		RelOptInfo *rel = root->simple_rel_array[i];

		if (!rel || rel->rtekind != RTE_SUBQUERY || rel->subroot == NULL)
			continue;

		if (contains_grouping_sets(rel->subroot))
			return true;
	}

	return false;
}

/*
 * Detect a SubPlan node and try to memoize it.
 *
 * NOTE: We intentionally don't walk recursively into the SubPlan's plan tree
 * because have simplified the feature with referencing only immediate parent.
 */
static bool
memoize_subplan_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, SubPlan))
	{
		SubPlan    *splan = (SubPlan *) node;
		PathWalkerCtx *ctx = (PathWalkerCtx *) context;
		MemoizePath *mpath;
		PlannerInfo *root = ctx->root;
		PlannerInfo *subroot;
		Path	   *subpath;
		List	   *param_exprs = NIL;
		List	   *hash_operators = NIL;
		bool		binary_mode = false;
		Plan	   *plan;
		ListCell   *lc,
				   *lc1,
				   *lc2;
		List	   *params = NIL;
		List	   *saved_minmax_aggs = NIL;
		PlannerInfo *proot;
		RelOptInfo *final_rel;

		subroot = list_nth(root->glob->subroots, splan->plan_id - 1);
		final_rel = fetch_upper_rel(subroot, UPPERREL_FINAL, NULL);
		subpath = final_rel->cheapest_total_path;

		/*
		 * Our implementation has a fundamental flaw: a plan creation may cause
		 * some changes in the root fields. Repeated calls may cause
		 * inconsistency.
		 * For now, we detected only one issue: minmax aggregates add InitPlan
		 * into the root->glob lists.
		 * Having no idea how to design it as an extension without repeating
		 * plan creation, I just skip the memoisation of such plans. We need to
		 * execute create_plan beforehand because the Memoize node should be
		 * estimated and inserted when the upper plan is already at the end of
		 * planning. It might be designed through the AlternativeSubplan
		 * machinery, but setrefs will access statistical slots in this case.
		 */
		if (subroot->minmax_aggs != NIL)
			return false;

		if (IsA(subpath, MemoizePath))
			/* We have already planned it and decided to insert Memoize, skip */
			return false;

		/*
		 * Limit the feature a little bit: Allow references to the imemdiate
		 * parent only. It simplifies the code drastically and may be OK as a
		 * first approximation.
		 */

		if (splan->args == NIL)
			return false;

		/*
		 * Pass through the parent root tree and detect any references to
		 * upper query levels, higher than immediate parent.
		 */
		for (proot = subroot->parent_root; proot != NULL;
			 proot = proot->parent_root)
		{
			if (proot->plan_params != NIL)
				return false;
		}

		/*
		 * Don't memoize subplan if it (or its subqueries) contain grouping sets
		 * They affect PlannerInfo::grouping_map field during the plan creation
		 * and may cause issues.
		 */
		if (contains_grouping_sets(subroot))
			return false;

		/*
		 * Convert references to immediate parent already materialised in the
		 * SubPlan structure into a parameters list.
		 */
		forboth(lc1, splan->args, lc2, splan->parParam)
		{
			Param	   *param;
			Node	   *arg = (Node *) lfirst(lc1);

			param = makeNode(Param);
			param->paramkind = PARAM_EXEC;
			param->paramid = lfirst_int(lc2);
			param->paramtype = exprType(arg);
			param->paramtypmod = exprTypmod(arg);
			param->paramcollid = exprCollation(arg);
			param->location = exprLocation(arg);
			params = lappend(params, param);
		}

		if (!paraminfo_get_equal_hashops(params, &param_exprs,
										 &hash_operators, &binary_mode))
			return false;

		/* Build a memoized plan */

		mpath = create_memoize_path(subroot, subpath->parent, subpath,
									param_exprs, hash_operators, false,
									binary_mode, ctx->calls);

		saved_minmax_aggs = subroot->minmax_aggs;
		subroot->minmax_aggs = NIL;
		subroot->grouping_map = NULL;

		plan = create_plan(subroot, (Path *) mpath);

		/* TODO: cover by assertion checking */
		if (list_difference_ptr(saved_minmax_aggs, subroot->minmax_aggs) != NIL)
			elog(PANIC, "Planning with memoization of the subplan does not correspond the original one");

		if (!force_memoize_subplan)
		{
			Cost		rescan_startup_cost;
			Cost		rescan_total_cost;
			Cost		curcost;

			cost_memoize_rescan(subroot->parent_root, mpath, splan->args,
								&rescan_startup_cost, &rescan_total_cost);

			/* Now, we have to do something close to fix_alternative_subplan */
			curcost = splan->startup_cost + ctx->calls * splan->per_call_cost;

			elog(DEBUG5, "SubPlan Memoization costing: calls = %.0lf, current cost: %.4lf, memoized cost: %.4lf",
				 mpath->calls, curcost, rescan_startup_cost + ctx->calls * rescan_total_cost);

			if (curcost < rescan_startup_cost + ctx->calls * rescan_total_cost)
				/* Refuse memoising, go forth */
				return false;
		}

		/*
		 * To be consistent, replace path and plan nodes in the
		 * subplan-related lists. Free previous data to nuke any possibly lost
		 * references.
		 */
		lc = list_nth_cell(root->glob->subplans, splan->plan_id - 1);
		pfree(lc->ptr_value);
		lc->ptr_value = plan;

		final_rel->cheapest_total_path = subpath;
#if PG_VERSION_NUM >= 170000
		lc = list_nth_cell(root->glob->subpaths, splan->plan_id - 1);
		lc->ptr_value = mpath;
#endif

		/* Let's check next subplan */
		return false;
	}
	else if (IsA(node, Query))
		Assert(0);
	else if (IsA(node, RestrictInfo))
	{
		RestrictInfo *rinfo = (RestrictInfo *) node;

		return memoize_subplan_walker((Node *) rinfo->clause, context);
	}

	return expression_tree_walker(node, memoize_subplan_walker, context);
}

/*
 * According to the Path type, probe expressions to detect a SubPlan.
 *
 * It should set the calls field into correct value
 */
static bool
subplan_hunter_path_walker(Node *node, void *context)
{
	Path	   *pathnode = (Path *) node;

	PathWalkerCtx *ctx = (PathWalkerCtx *) context;

	if (node == NULL)
		return false;

	/* TODO: rethink the whole idea of calls calculations */
	ctx->calls = pathnode->parent->tuples;

	switch (nodeTag(pathnode))
	{
		case T_Path:
		case T_BitmapHeapPath:
			{
				Path	   *path = (Path *) node;

				memoize_subplan_walker((Node *) path->parent->baserestrictinfo,
									   context);
				memoize_subplan_walker((Node *) path->parent->reltarget->exprs,
									   context);
				return false;
			}
		case T_NestPath:
		case T_HashPath:
		case T_MergePath:
			{
				JoinPath   *path = (JoinPath *) node;

				memoize_subplan_walker((Node *) path->joinrestrictinfo,
									   context);
				memoize_subplan_walker((Node *) path->path.pathtarget->exprs,
									   context);

				/* Pass the tree further */
				break;
			}
		case T_IndexPath:
			{
				IndexPath  *ipath = (IndexPath *) node;
				IndexOptInfo *index = ((IndexPath *) node)->indexinfo;

				memoize_subplan_walker((Node *) index->indrestrictinfo,
									   context);
				memoize_subplan_walker((Node *) ipath->path.parent->reltarget->exprs,
									   context);
				return false;
			}
		case T_GroupPath:
			{
				GroupPath  *path = (GroupPath *) node;

				memoize_subplan_walker((Node *) path->path.pathtarget->exprs,
									   context);
				memoize_subplan_walker((Node *) path->qual, context);

				/* XXX: Do we need to pass throughout the grouping clauses? */

				break;
			}
		case T_AggPath:
			{
				AggPath    *path = (AggPath *) node;

				memoize_subplan_walker((Node *) path->path.pathtarget->exprs,
									   context);
				memoize_subplan_walker((Node *) path->qual, context);

				/* XXX: Do we need to pass throughout the grouping clauses? */

				break;
			}
		case T_AppendPath:
			/* Don't have its own clauses */
		default:
			break;
	}

	return path_tree_walker(node, subplan_hunter_path_walker, context);
}

/*
 * It would be ideal to pass the path tree only once when this hooks is called
 * at the top query level. But in this case parameters detection procedure seems
 * much more complicated. May we implement it in the future?
 */
static void
upper_paths_hook(PlannerInfo *root, UpperRelationKind stage,
				 RelOptInfo *input_rel, RelOptInfo *output_rel, void *extra)
{
	PathWalkerCtx ctx;

	if (upper_paths_hook_next)
		(*upper_paths_hook_next) (root, UPPERREL_FINAL,
								  input_rel, output_rel, &extra);

	/*
	 * No need to do anything if memoisation is disabled in the core or in the
	 * extension, or we just don't have any SubLinks there.
	 */
	if (!enable_memoize || !enable_memoize_subplan || stage != UPPERREL_FINAL ||
		!root->parse->hasSubLinks)
		return;

	/* Now, do our job */
	set_cheapest(output_rel);

	ctx.root = root;
	subplan_hunter_path_walker((Node *) output_rel->cheapest_total_path,
							   (void *) &ctx);
}

void
_PG_init(void)
{
	DefineCustomBoolVariable(EXTENSION_NAME ".memoize_subplan",
							 "Enable/disable caching results of correlated subplans",
							 "It is similar to how NestLoop JOIN caches its inner to avoid unnecessary rescanning",
							 &enable_memoize_subplan,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	/*
	 * Memoization seems too complicated. It needs a lot of testing for
	 * multiple corner cases. To simplify the process invent a GUC which will
	 * insert the node into any possible SubPlan. So, make it disabled by
	 * default and allow only DBA to enable it.
	 */
	DefineCustomBoolVariable(EXTENSION_NAME ".force_memoize_subplan",
							 "Force insertion of a Memoize node into correlated SubPlan",
							 "Ignore costing. It is needed mostly for debugging",
							 &force_memoize_subplan,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	MarkGUCPrefixReserved(EXTENSION_NAME);

	upper_paths_hook_next = create_upper_paths_hook;
	create_upper_paths_hook = upper_paths_hook;
}
