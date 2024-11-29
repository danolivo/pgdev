/*-------------------------------------------------------------------------
 *
 * planmain.c
 *	  Routines to plan a single query
 *
 * What's in a name, anyway?  The top-level entry point of the planner/
 * optimizer is over in planner.c, not here as you might think from the
 * file name.  But this is the main code for planning a basic join operation,
 * shorn of features like subselects, inheritance, aggregates, grouping,
 * and so on.  (Those are the things planner.c deals with.)
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/plan/planmain.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "nodes/nodeFuncs.h"
#include "optimizer/appendinfo.h"
#include "optimizer/clauses.h"
#include "optimizer/optimizer.h"
#include "optimizer/orclauses.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/placeholder.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"

static void extract_non_equivalence_filters(PlannerInfo *root);

/*
 * query_planner
 *	  Generate a path (that is, a simplified plan) for a basic query,
 *	  which may involve joins but not any fancier features.
 *
 * Since query_planner does not handle the toplevel processing (grouping,
 * sorting, etc) it cannot select the best path by itself.  Instead, it
 * returns the RelOptInfo for the top level of joining, and the caller
 * (grouping_planner) can choose among the surviving paths for the rel.
 *
 * root describes the query to plan
 * qp_callback is a function to compute query_pathkeys once it's safe to do so
 * qp_extra is optional extra data to pass to qp_callback
 *
 * Note: the PlannerInfo node also includes a query_pathkeys field, which
 * tells query_planner the sort order that is desired in the final output
 * plan.  This value is *not* available at call time, but is computed by
 * qp_callback once we have completed merging the query's equivalence classes.
 * (We cannot construct canonical pathkeys until that's done.)
 */
RelOptInfo *
query_planner(PlannerInfo *root,
			  query_pathkeys_callback qp_callback, void *qp_extra)
{
	Query	   *parse = root->parse;
	List	   *joinlist;
	RelOptInfo *final_rel;

	/*
	 * Init planner lists to empty.
	 *
	 * NOTE: append_rel_list was set up by subquery_planner, so do not touch
	 * here.
	 */
	root->join_rel_list = NIL;
	root->join_rel_hash = NULL;
	root->join_rel_level = NULL;
	root->join_cur_level = 0;
	root->canon_pathkeys = NIL;
	root->left_join_clauses = NIL;
	root->right_join_clauses = NIL;
	root->full_join_clauses = NIL;
	root->join_info_list = NIL;
	root->placeholder_list = NIL;
	root->placeholder_array = NULL;
	root->placeholder_array_size = 0;
	root->fkey_list = NIL;
	root->initial_rels = NIL;

	/*
	 * Set up arrays for accessing base relations and AppendRelInfos.
	 */
	setup_simple_rel_arrays(root);

	/*
	 * In the trivial case where the jointree is a single RTE_RESULT relation,
	 * bypass all the rest of this function and just make a RelOptInfo and its
	 * one access path.  This is worth optimizing because it applies for
	 * common cases like "SELECT expression" and "INSERT ... VALUES()".
	 */
	Assert(parse->jointree->fromlist != NIL);
	if (list_length(parse->jointree->fromlist) == 1)
	{
		Node	   *jtnode = (Node *) linitial(parse->jointree->fromlist);

		if (IsA(jtnode, RangeTblRef))
		{
			int			varno = ((RangeTblRef *) jtnode)->rtindex;
			RangeTblEntry *rte = root->simple_rte_array[varno];

			Assert(rte != NULL);
			if (rte->rtekind == RTE_RESULT)
			{
				/* Make the RelOptInfo for it directly */
				final_rel = build_simple_rel(root, varno, NULL);

				/*
				 * If query allows parallelism in general, check whether the
				 * quals are parallel-restricted.  (We need not check
				 * final_rel->reltarget because it's empty at this point.
				 * Anything parallel-restricted in the query tlist will be
				 * dealt with later.)  We should always do this in a subquery,
				 * since it might be useful to use the subquery in parallel
				 * paths in the parent level.  At top level this is normally
				 * not worth the cycles, because a Result-only plan would
				 * never be interesting to parallelize.  However, if
				 * debug_parallel_query is on, then we want to execute the
				 * Result in a parallel worker if possible, so we must check.
				 */
				if (root->glob->parallelModeOK &&
					(root->query_level > 1 ||
					 debug_parallel_query != DEBUG_PARALLEL_OFF))
					final_rel->consider_parallel =
						is_parallel_safe(root, parse->jointree->quals);

				/*
				 * The only path for it is a trivial Result path.  We cheat a
				 * bit here by using a GroupResultPath, because that way we
				 * can just jam the quals into it without preprocessing them.
				 * (But, if you hold your head at the right angle, a FROM-less
				 * SELECT is a kind of degenerate-grouping case, so it's not
				 * that much of a cheat.)
				 */
				add_path(final_rel, (Path *)
						 create_group_result_path(root, final_rel,
												  final_rel->reltarget,
												  (List *) parse->jointree->quals));

				/* Select cheapest path (pretty easy in this case...) */
				set_cheapest(final_rel);

				/*
				 * We don't need to run generate_base_implied_equalities, but
				 * we do need to pretend that EC merging is complete.
				 */
				root->ec_merging_done = true;

				/*
				 * We still are required to call qp_callback, in case it's
				 * something like "SELECT 2+2 ORDER BY 1".
				 */
				(*qp_callback) (root, qp_extra);

				return final_rel;
			}
		}
	}

	/*
	 * Construct RelOptInfo nodes for all base relations used in the query.
	 * Appendrel member relations ("other rels") will be added later.
	 *
	 * Note: the reason we find the baserels by searching the jointree, rather
	 * than scanning the rangetable, is that the rangetable may contain RTEs
	 * for rels not actively part of the query, for example views.  We don't
	 * want to make RelOptInfos for them.
	 */
	add_base_rels_to_query(root, (Node *) parse->jointree);

	/*
	 * Examine the targetlist and join tree, adding entries to baserel
	 * targetlists for all referenced Vars, and generating PlaceHolderInfo
	 * entries for all referenced PlaceHolderVars.  Restrict and join clauses
	 * are added to appropriate lists belonging to the mentioned relations. We
	 * also build EquivalenceClasses for provably equivalent expressions. The
	 * SpecialJoinInfo list is also built to hold information about join order
	 * restrictions.  Finally, we form a target joinlist for make_one_rel() to
	 * work from.
	 */
	build_base_rel_tlists(root, root->processed_tlist);

	find_placeholders_in_jointree(root);

	find_lateral_references(root);

	joinlist = deconstruct_jointree(root);

	/*
	 * Reconsider any postponed outer-join quals now that we have built up
	 * equivalence classes.  (This could result in further additions or
	 * mergings of classes.)
	 */
	reconsider_outer_join_clauses(root);

	/*
	 * If we formed any equivalence classes, generate additional restriction
	 * clauses as appropriate.  (Implied join clauses are formed on-the-fly
	 * later.)
	 */
	generate_base_implied_equalities(root);

	/*
	 * We have completed merging equivalence sets, so it's now possible to
	 * generate pathkeys in canonical form; so compute query_pathkeys and
	 * other pathkeys fields in PlannerInfo.
	 */
	(*qp_callback) (root, qp_extra);

	/*
	 * Examine any "placeholder" expressions generated during subquery pullup.
	 * Make sure that the Vars they need are marked as needed at the relevant
	 * join level.  This must be done before join removal because it might
	 * cause Vars or placeholders to be needed above a join when they weren't
	 * so marked before.
	 */
	fix_placeholder_input_needed_levels(root);

	/*
	 * Remove any useless outer joins.  Ideally this would be done during
	 * jointree preprocessing, but the necessary information isn't available
	 * until we've built baserel data structures and classified qual clauses.
	 */
	joinlist = remove_useless_joins(root, joinlist);

	/*
	 * Also, reduce any semijoins with unique inner rels to plain inner joins.
	 * Likewise, this can't be done until now for lack of needed info.
	 */
	reduce_unique_semijoins(root);

	/*
	 * Now distribute "placeholders" to base rels as needed.  This has to be
	 * done after join removal because removal could change whether a
	 * placeholder is evaluable at a base rel.
	 */
	add_placeholders_to_base_rels(root);

	/*
	 * Construct the lateral reference sets now that we have finalized
	 * PlaceHolderVar eval levels.
	 */
	create_lateral_join_info(root);

	/*
	 * Match foreign keys to equivalence classes and join quals.  This must be
	 * done after finalizing equivalence classes, and it's useful to wait till
	 * after join removal so that we can skip processing foreign keys
	 * involving removed relations.
	 */
	match_foreign_keys_to_quals(root);

	/*
	 * Look for join OR clauses that we can extract single-relation
	 * restriction OR clauses from.
	 */
	extract_restriction_or_clauses(root);

	/*
	 * Look for additional clauses which can be pushed to another side of a join.
	 * It adds to the clauselist 'A.x=B.x AND A.x<10' one more clause 'B.x<10'.
	 */
	extract_non_equivalence_filters(root);

	/*
	 * Now expand appendrels by adding "otherrels" for their children.  We
	 * delay this to the end so that we have as much information as possible
	 * available for each baserel, including all restriction clauses.  That
	 * let us prune away partitions that don't satisfy a restriction clause.
	 * Also note that some information such as lateral_relids is propagated
	 * from baserels to otherrels here, so we must have computed it already.
	 */
	add_other_rels_to_query(root);

	/*
	 * Distribute any UPDATE/DELETE/MERGE row identity variables to the target
	 * relations.  This can't be done till we've finished expansion of
	 * appendrels.
	 */
	distribute_row_identity_vars(root);

	/*
	 * Ready to do the primary planning.
	 */
	final_rel = make_one_rel(root, joinlist);

	/* Check that we got at least one usable path */
	if (!final_rel || !final_rel->cheapest_total_path ||
		final_rel->cheapest_total_path->param_info != NULL)
		elog(ERROR, "failed to construct the join relation");

	return final_rel;
}

static List *
extract_base_clauses(PlannerInfo *root, Node *expr, int relid)
{
	List	   *clauses = NIL;
	RelOptInfo *rel = find_base_rel(root, relid);

	foreach_node(RestrictInfo, rinfo, rel->baserestrictinfo)
	{
		Node	   *expr2 = NULL;

		if (rinfo->is_clone || rinfo->has_clone || rinfo->pseudoconstant ||
			rinfo->security_level > 0 || rinfo->has_volatile ||
			rinfo->mergeopfamilies != NIL ||
			!bms_is_empty(rinfo->incompatible_relids) ||
			!bms_is_empty(rinfo->outer_relids))
			continue;

		if (IsA(rinfo->clause, OpExpr))
		{
			if (bms_is_empty(rinfo->left_relids))
				expr2 =  get_rightop(rinfo->clause);
			else if (bms_is_empty(rinfo->right_relids))
				expr2 =  get_leftop(rinfo->clause);
			else
				continue;
		}
		else if (IsA(rinfo->clause, ScalarArrayOpExpr))
		{
			ScalarArrayOpExpr *saop = (ScalarArrayOpExpr *) rinfo->clause;

			expr2 = (Node*) linitial(saop->args);
		}
		else
			continue;

		if (expr2 && IsA(expr2, RelabelType))
				expr2 = (Node *) ((RelabelType *) expr2)->arg;
		if (expr && IsA(expr, RelabelType))
				expr = (Node *) ((RelabelType *) expr)->arg;

		if (!equal(expr2, expr))
			continue;

		clauses = lappend(clauses, rinfo);
	}
	return clauses;
}

/*
 * Add extra filters derived from other base relations.
 *
 * Using Equivalence Classes try to find clauses which can be used to filter
 * more tuples.
 */
static void
generate_extra_filters(PlannerInfo *root, RelOptInfo *rel)
{
	int		relid = rel->relid;
	ListCell *lc;

	Assert(rel->relid > 0);

	/*
	 * Take all the ECs where this relation may have potential match
	 */
	foreach_node(EquivalenceClass, ec, root->eq_classes)
	{
		List			   *exprs = NIL;
		List			   *others = NIL;
		List			   *others_expr = NIL;

		/* EC must describe a join clause */
		if (!bms_is_member(relid, ec->ec_relids) ||
			bms_num_members(ec->ec_relids) == 1)
			continue;

		foreach_node(EquivalenceMember, em, ec->ec_members)
		{
			int	em_relid = 0;

			if (em->em_is_child ||
				!bms_get_singleton_member(em->em_relids, &em_relid))
				continue;

			Assert(em_relid > 0);

			if (em_relid != relid)
			{
				List *bclauses = extract_base_clauses(
										root, (Node *) em->em_expr, em_relid);

				others = lappend(others, bclauses);
				others_expr = lappend(others_expr, em);
			}
			else
				exprs = list_concat(exprs, extract_base_clauses(
											root, (Node *) em->em_expr, relid));
		}

		if (exprs == NIL || others == NIL)
			continue;
		Assert(list_length(others) == list_length(others_expr));

		foreach_node(RestrictInfo, rinfo, exprs)
		{
			int	i;
			void *tmp_arg_ptr;
			void **arg = NULL;

			if (IsA(rinfo->clause, OpExpr))
			{
				OpExpr *opexpr = (OpExpr *) rinfo->clause;

				/* Temporary replace a var in the inner clause by the outer */
				i = bms_is_empty(rinfo->left_relids) ? 1 : 0;
				arg = &opexpr->args->elements[i].ptr_value;
			}
			else if (IsA(rinfo->clause, ScalarArrayOpExpr))
			{
				ScalarArrayOpExpr *saop = (ScalarArrayOpExpr *) rinfo->clause;
				arg = &saop->args->elements[0].ptr_value;
			}
			else
				Assert(0);

			tmp_arg_ptr = *arg;

			foreach (lc, others_expr)
			{
				EquivalenceMember *em = lfirst_node(EquivalenceMember, lc);
				List *others_lst =
						(List *) list_nth(others, foreach_current_index(lc));
				bool duplicated = false;

				Assert(bms_num_members(em->em_relids) == 1);

				*arg = (void *) em->em_expr;

				foreach_node(RestrictInfo, rinfo2, others_lst)
				{
					Assert(IsA(rinfo2->clause, OpExpr) ||
						   IsA(rinfo2->clause, ScalarArrayOpExpr));

					/* XXX: remember commuting case! */
					if (equal(rinfo->clause, rinfo2->clause))
					{
						duplicated = true;
						others_lst = foreach_delete_current(others_lst, rinfo2);
						break;
					}
				}

				if (!duplicated)
				{
					RestrictInfo *restrictinfo;

					/* The clause can be attached to the outer side */
					restrictinfo = make_restrictinfo(root,
										 (Expr *) copyObject(rinfo->clause),
										 rinfo->is_pushed_down,
										 false, false, false, 0,
										 em->em_relids,
										 NULL, NULL);
					distribute_restrictinfo_to_rels(root, restrictinfo);
				}

				*arg = tmp_arg_ptr;
			}
		}
	}
}

static void
extract_non_equivalence_filters(PlannerInfo *root)
{
	Index		rti;

	for (rti = 1; rti < root->simple_rel_array_size; rti++)
	{
		RelOptInfo *rel = root->simple_rel_array[rti];

		/* there may be empty slots corresponding to non-baserel RTEs */
		if (rel == NULL)
			continue;

		Assert(rel->relid == rti);	/* sanity check on array */

		/* ignore RTEs that are "other rels" */
		if (rel->reloptkind != RELOPT_BASEREL)
			continue;

		/*
		 * XXX: If needed, restrict it by the only partitioned case,
		 * see rel->inh
		 */

		generate_extra_filters(root, rel);
	}
}
