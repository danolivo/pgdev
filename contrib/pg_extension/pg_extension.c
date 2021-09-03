#include "postgres.h"

#include "executor/executor.h"
#include "nodes/extensible.h"
#include "optimizer/cost.h"
#include "optimizer/paths.h"
#include "optimizer/pathnode.h"

PG_MODULE_MAGIC;

static set_join_pathlist_hook_type prev_set_join_pathlist_hook = NULL;


void _PG_init(void);
static void try_joinswitcher(PlannerInfo *root, RelOptInfo *joinrel,
							 RelOptInfo *outerrel, RelOptInfo *innerrel,
							 JoinType jointype, JoinPathExtraData *extra);
static inline bool clause_sides_match_join(RestrictInfo *rinfo,
										   RelOptInfo *outerrel,
										   RelOptInfo *innerrel);
static void BeginJoinSwitcher(CustomScanState *node,
							   EState *estate, int eflags);
static TupleTableSlot *ExecJoinSwitcher(CustomScanState *node);
static void EndJoinSwitcher(CustomScanState *node);
static void ExplainJoinSwitcher(CustomScanState *node, List *ancestors,
								 ExplainState *es);
static void ReScanJoinSwitcher(CustomScanState *node);
static Node *CreateJoinSwitcherState(CustomScan *node);
static Plan *PlanJoinSwitcherPath(PlannerInfo *root, RelOptInfo *rel,
								  struct CustomPath *best_path, List *tlist,
								  List *clauses, List *custom_plans);
static CustomPath *create_joinswitcher_path(PlannerInfo *root,
											RelOptInfo *joinrel,
											JoinPathExtraData *extra,
											List *restrict_clauses,
											Relids required_outer,
											NestPath *nl_path,
											HashPath *hj_path);

void
_PG_init(void)
{
	prev_set_join_pathlist_hook = set_join_pathlist_hook;
	set_join_pathlist_hook = try_joinswitcher;

	elog(LOG, "Template extension was initialized.");
}

/*
 * Paths parameterized by the parent can be considered to be parameterized by
 * any of its child.
 */
#define PATH_PARAM_BY_PARENT(path, rel)	\
	((path)->param_info && bms_overlap(PATH_REQ_OUTER(path),	\
									   (rel)->top_parent_relids))
#define PATH_PARAM_BY_REL_SELF(path, rel)  \
	((path)->param_info && bms_overlap(PATH_REQ_OUTER(path), (rel)->relids))

#define PATH_PARAM_BY_REL(path, rel)	\
	(PATH_PARAM_BY_REL_SELF(path, rel) || PATH_PARAM_BY_PARENT(path, rel))

static void
try_joinswitcher(PlannerInfo *root,
				 RelOptInfo *joinrel,
				 RelOptInfo *outerrel,
				 RelOptInfo *innerrel,
				 JoinType jointype,
				 JoinPathExtraData *extra)
{
	JoinCostWorkspace workspace;
	Relids cheapest_required_outer;
	Path *cheapest_inner_path = NULL;
	NestPath *nl_path = makeNode(NestPath);
	HashPath *hj_path = NULL;
	List *hashclauses;
	ListCell *lc;
	ListCell *lc2;

	/*
	 * Some extension intercept this hook earlier. Allow it to do a work
	 * before us.
	 */
	if (prev_set_join_pathlist_hook)
		(*prev_set_join_pathlist_hook)(root, joinrel, outerrel, innerrel,
									   jointype, extra);

	if (jointype != JOIN_INNER)
		return;

	hashclauses = NIL;
	foreach(lc, extra->restrictlist)
	{
		RestrictInfo *restrictinfo = (RestrictInfo *) lfirst(lc);

		/*
		 * If processing an outer join, only use its own join clauses for
		 * hashing.  For inner joins we need not be so picky.
		 */
		if (IS_OUTER_JOIN(jointype) &&
			RINFO_IS_PUSHED_DOWN(restrictinfo, joinrel->relids))
			continue;

		if (!restrictinfo->can_join ||
			restrictinfo->hashjoinoperator == InvalidOid)
			continue;			/* not hashjoinable */

		/*
		 * Check if clause has the form "outer op inner" or "inner op outer".
		 */
		if (!clause_sides_match_join(restrictinfo, outerrel, innerrel))
			continue;			/* no good for these input relations */

		hashclauses = lappend(hashclauses, restrictinfo);
	}

	if (!hashclauses)
		return;

	/* look for the most optimal combination of NL/HJ paths */
	workspace.total_cost = 0.;
	foreach(lc, joinrel->pathlist)
	{
		Path *path = (Path *) lfirst(lc);
		Path *outer_path;

		if (!IsA(path, NestPath))
			continue;

		outer_path = ((NestPath *) path)->jpath.outerjoinpath;

		foreach(lc2, innerrel->pathlist)
		{
			Relids required_outer;
			JoinCostWorkspace cwspace;
			Path *inner_path = (Path *) lfirst(lc2);

			if (PATH_PARAM_BY_REL(outer_path, innerrel) ||
			PATH_PARAM_BY_REL(inner_path, outerrel))
				continue;

			/*
			 * Check to see if proposed path is still parameterized, and reject if the
			 * parameterization wouldn't be sensible.
			 */
			required_outer = calc_non_nestloop_required_outer(outer_path,
															  inner_path);
			if (required_outer &&
				!bms_overlap(required_outer, extra->param_source_rels))
				continue;

			initial_cost_hashjoin(root, &cwspace, jointype, hashclauses,
								  outer_path, inner_path, extra, false);

			if (workspace.total_cost == 0. ||
				workspace.total_cost > cwspace.total_cost)
			{
				cheapest_required_outer = required_outer;
				cheapest_inner_path = inner_path;
				memcpy(nl_path, path, sizeof(NestPath));
				workspace = cwspace;
			}
		}
	}

	/* XXX: Maybe we need to call add_path_precheck here? */
	if (!nl_path || !cheapest_inner_path ||
		bms_equal(cheapest_required_outer, PATH_REQ_OUTER(cheapest_inner_path)))
		/* Combination not found. */
		return;

	hj_path = create_hashjoin_path(root, joinrel, jointype, &workspace, extra,
								   ((NestPath *) nl_path)->jpath.outerjoinpath,
								   cheapest_inner_path, false,
								   extra->restrictlist, cheapest_required_outer,
								   hashclauses);
	if (hj_path == NULL)
		return;

	/*
	 * Create a join switcher path node. Add HJ и NL paths as subpaths of this custom
	 * node.
	 */
	add_path(joinrel, (Path *)
		create_joinswitcher_path(root, joinrel, extra, extra->restrictlist,
						 cheapest_required_outer, nl_path, hj_path));
}

/*
 * Copy of the core routine.
 */
static inline bool
clause_sides_match_join(RestrictInfo *rinfo, RelOptInfo *outerrel,
						RelOptInfo *innerrel)
{
	if (bms_is_subset(rinfo->left_relids, outerrel->relids) &&
		bms_is_subset(rinfo->right_relids, innerrel->relids))
	{
		/* lefthand side is outer */
		rinfo->outer_is_left = true;
		return true;
	}
	else if (bms_is_subset(rinfo->left_relids, innerrel->relids) &&
			 bms_is_subset(rinfo->right_relids, outerrel->relids))
	{
		/* righthand side is outer */
		rinfo->outer_is_left = false;
		return true;
	}
	return false;				/* no good for these input relations */
}

static CustomPathMethods pathmethods = {
	.CustomName = "JoinSwitcherPath",
	.PlanCustomPath = PlanJoinSwitcherPath,
	.ReparameterizeCustomPathByChild = NULL
};

static CustomScanMethods planmethods = {
	.CustomName = "JoinSwitcherPlan",
	.CreateCustomScanState = CreateJoinSwitcherState
};

static CustomExecMethods execmethods = {
	.CustomName = "JoinSwitcherExec",
	.BeginCustomScan = BeginJoinSwitcher,
	.ExecCustomScan = ExecJoinSwitcher,
	.EndCustomScan = EndJoinSwitcher,
	.ReScanCustomScan = ReScanJoinSwitcher,
	.MarkPosCustomScan = NULL,
	.RestrPosCustomScan = NULL,
	.EstimateDSMCustomScan = NULL,
	.InitializeDSMCustomScan = NULL,
	.ReInitializeDSMCustomScan = NULL,
	.InitializeWorkerCustomScan = NULL,
	.ShutdownCustomScan = NULL,
	.ExplainCustomScan = ExplainJoinSwitcher
};

static void
BeginJoinSwitcher(struct CustomScanState *node, struct EState *estate, int eflags)
{
	CustomScan	*cscan = (CustomScan *) node->ss.ps.plan;
	Plan *scan_plan;
	PlanState	*planState;

	scan_plan = linitial(cscan->custom_plans);
	planState = (PlanState *) ExecInitNode(scan_plan, estate, eflags);
	node->custom_ps = lappend(node->custom_ps, planState);
}

static TupleTableSlot *
ExecJoinSwitcher(CustomScanState *node)
{
	ScanState	*subPlanState = linitial(node->custom_ps);
	return ExecProcNode(&subPlanState->ps);
}

static void
EndJoinSwitcher(CustomScanState *node)
{
	Assert(list_length(node->custom_ps) == 1);
	ExecEndNode(linitial(node->custom_ps));
}

static void
ExplainJoinSwitcher(CustomScanState *node, List *ancestors, ExplainState *es)
{
}

static void
ReScanJoinSwitcher(CustomScanState *node)
{
	PlanState *subPlan = (PlanState *) linitial(node->custom_ps);

	if (subPlan->chgParam == NULL)
		ExecReScan(subPlan);
}
static Node *
CreateJoinSwitcherState(CustomScan *node)
{
	CustomScanState *state;

	state = (CustomScanState *) palloc0(sizeof(CustomScanState));
	NodeSetTag(state, T_CustomScanState);

	state->flags = node->flags;
	state->methods = &execmethods;

	return (Node *) state;
}

static CustomScan *
makeJoinSwitcher(List *custom_plans, List *tlist)
{
	CustomScan	*node = makeNode(CustomScan);
	Plan		*plan = &node->scan.plan;
	List *child_tlist;

	plan->qual = NIL;
	plan->lefttree = NULL;
	plan->righttree = NULL;
	plan->targetlist = tlist;

	/* Setup methods and child plan */
	node->methods = &planmethods;
	node->scan.scanrelid = 0;
	node->custom_plans = custom_plans;
	child_tlist = ((Plan *)linitial(node->custom_plans))->targetlist;
	node->custom_scan_tlist = child_tlist;
	node->custom_exprs = NIL;
	node->custom_private = NIL;

	return node;
}

static Plan *
PlanJoinSwitcherPath(PlannerInfo *root, RelOptInfo *rel,
					 struct CustomPath *best_path, List *tlist, List *clauses,
					 List *custom_plans)
{
	CustomScan *JoinSwitcher = makeNode(CustomScan);

	JoinSwitcher = makeJoinSwitcher(custom_plans, tlist);
	return &JoinSwitcher->scan.plan;
}

static CustomPath *
create_joinswitcher_path(PlannerInfo *root, RelOptInfo *joinrel,
						 JoinPathExtraData *extra,
						 List *restrict_clauses,
						 Relids required_outer,
						 NestPath *nl_path, HashPath *hj_path)
{
	CustomPath *JoinSwitcher = makeNode(CustomPath);

	/* initialize basic path structures. */
	JoinSwitcher->path.pathtype = T_CustomScan;
	JoinSwitcher->path.parent = joinrel;
	JoinSwitcher->path.pathtarget = joinrel->reltarget;
	JoinSwitcher->path.parallel_safe = false;
	JoinSwitcher->path.parallel_aware = false;
	JoinSwitcher->path.parallel_workers = 0;
	JoinSwitcher->path.pathkeys = NIL;
	JoinSwitcher->path.param_info =
		get_joinrel_parampathinfo(root,
								  joinrel,
								  nl_path->jpath.outerjoinpath,
								  nl_path->jpath.innerjoinpath,
								  extra->sjinfo,
								  required_outer,
								  &restrict_clauses);

	/* Set specific fields of the custom path. */
	JoinSwitcher->flags = 0;
	JoinSwitcher->custom_private = NIL;
	JoinSwitcher->custom_paths = list_make1(nl_path/*, hj_path*/);
	JoinSwitcher->methods = &pathmethods;

	/* Cost estimations. */
	JoinSwitcher->path.startup_cost = nl_path->jpath.path.startup_cost;
	JoinSwitcher->path.total_cost = nl_path->jpath.path.total_cost - 0.8 * nl_path->jpath.path.total_cost;
	JoinSwitcher->path.rows = nl_path->jpath.path.rows;

	return JoinSwitcher;
}
