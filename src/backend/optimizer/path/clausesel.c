/*-------------------------------------------------------------------------
 *
 * clausesel.c
 *	  Routines to compute clause selectivities
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/path/clausesel.c
 *
 *-------------------------------------------------------------------------
 */
#include <math.h>
#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "catalog/pg_collation.h"
#include "common/pg_prng.h"
#include "commands/vacuum.h"
#include "funcapi.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/plancat.h"
#include "statistics/statistics.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/selfuncs.h"

#include "parser/parsetree.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

#define EXHAUSTIVE_IN_SELECTIVITY_THRESHOLD (default_statistics_target/4)
#define RANGE_IN_SELECTIVITY_THRESHOLD (default_statistics_target/20)
#define MULTICOLUMN_STATISTIC_FALLBACK_DISTINCT_THRESHOLD (default_statistics_target)

/*
 * Data structure for accumulating info about possible range-query
 * clause pairs in clauselist_selectivity.
 */
typedef struct RangeQueryClause
{
	struct RangeQueryClause *next;	/* next in linked list */
	Node	   *var;			/* The common variable of the clauses */
	bool		have_lobound;	/* found a low-bound clause yet? */
	bool		have_hibound;	/* found a high-bound clause yet? */
	Selectivity lobound;		/* Selectivity of a var > something clause */
	Selectivity hibound;		/* Selectivity of a var < something clause */
} RangeQueryClause;

static void addRangeClause(RangeQueryClause **rqlist, Node *clause,
						   bool varonleft, bool isLTsel, Selectivity s2);
static RelOptInfo *find_single_rel_for_clauses(PlannerInfo *root,
											   List *clauses);
static Selectivity clauselist_selectivity_or(PlannerInfo *root,
											 List *clauses,
											 int varRelid,
											 JoinType jointype,
											 SpecialJoinInfo *sjinfo,
											 bool use_extended_stats);

static bool treat_as_join_clause(PlannerInfo *root, Node *clause, RestrictInfo *rinfo,
					 int varRelid, SpecialJoinInfo *sjinfo);

typedef enum CorrelationKind {
	CKRestrict = 0,
	CKIndepend,		/* unknown correlation */
	CKLikelySelf,	/* Seems, should be close to be correlated, like agg with
					   self join */
	CKSelf,			/* 100% correlation because of self join */
	CKMul			/* product of all CKLikelySelf * CKSelf */
} CorrelationKind;
static CorrelationKind get_correlation_kind(PlannerInfo *root, int varRelid,
											OpExpr* expr);

/*
 *  Get variabe node. Returns null if node is not a Var node.
 */
static inline Var*
get_var(Node* node)
{
	if (IsA(node, RelabelType))
		node = (Node *) ((RelabelType *) node)->arg;

	return IsA(node, Var) ? (Var*)node : NULL;
}

/*
 * Locate compound index which can be used for multicolumn clauses/join.
 */
static IndexOptInfo*
locate_inner_multicolumn_index(PlannerInfo *root, Index varno, List* vars,
						 int n_clauses,
						 int **permutation, List **missed_vars, int* n_keys)
{
	ListCell		*ilist;
	RelOptInfo		*rel = find_base_rel(root, varno);
	IndexOptInfo	*index_opt = NULL;
	List			*missed_vars_opt = NIL;
	int				*permutation_opt = NULL;
	int				n_index_cols_opt = 0;
	bool			used[INDEX_MAX_KEYS];
	int				posvars[INDEX_MAX_KEYS];

	*n_keys = 0;
	*missed_vars = NIL;

	Assert(list_length(vars) >= 1);
	Assert(list_length(vars) <= n_clauses);

	foreach(ilist, rel->indexlist)
	{
		IndexOptInfo	*index = (IndexOptInfo *) lfirst(ilist);
		ListCell		*vlist;
		int				i, n_index_cols = 0;
		List			*missed = NIL;
		int				*perm = NULL;
		int				last_idx = 0;

		memset(used, 0, sizeof(used));
		perm = palloc(n_clauses * sizeof(*perm));
		for(i=0; i<n_clauses; i++)
			perm[i] = -1;

		i = 0;
		foreach (vlist, vars)
		{
			Var* var = lfirst(vlist);
			int pos;

			for (pos = 0; pos < index->nkeycolumns; pos++)
			{
				if (index->indexkeys[pos] == var->varattno)
				{
					if (used[pos])
						missed = lappend(missed, var);
					else
					{
						used[pos] = true;
						posvars[pos] = i;
						perm[i] = pos;
						n_index_cols++;
						last_idx = Max(last_idx, pos);
						break;
					}
				}
			}

			/* var isn't found in index columns */
			if (pos == index->nkeycolumns && !list_member_ptr(missed, var))
				missed = lappend(missed, var);

			i += 1;
		}

		if (n_index_cols == 0)
			continue;

		/* check that found columns are first columns in index */
		if (index->nkeycolumns != n_index_cols)
		{
			int old_n_index_cols = n_index_cols;

			for (i = 0; i <= last_idx; i++)
			{
				if (n_index_cols != old_n_index_cols)
				{
					/*
					 * We will use only first n_index_cols columns instead of
					 * found old_n_index_cols, so, all other columns should be
					 * added to missed list
					 */
					if (used[i])
					{
						Var *var = list_nth(vars, posvars[i]);

						missed = lappend(missed, var);
					}
				}
				else if (!used[i])
				{
					if (i==0)
						/* there isn't useful prefix */
						goto TryNextIndex;

					/* we will use only first i columns, save as new n_index_cols */
					n_index_cols = i;
				}
			}
		}

		/* found exact match vars - index, immediately return */
		if (vlist == NULL && list_length(missed) == 0 && n_index_cols == index->nkeycolumns)
		{
			*permutation = perm;
			*n_keys = n_index_cols;
			return index;
		}

		/* save partially matched index */
		if (index_opt == NULL ||
			n_index_cols > n_index_cols_opt ||
			(n_index_cols == n_index_cols_opt && index->nkeycolumns < index_opt->nkeycolumns))
		{
			index_opt = index;
			missed_vars_opt = missed;
			if (permutation_opt)
				pfree(permutation_opt);
			permutation_opt = perm;
			perm = NULL;
			n_index_cols_opt = n_index_cols;
		}
TryNextIndex:
		if (perm)
			pfree(perm);
	}

	if (index_opt)
	{
		*missed_vars = list_concat_unique(*missed_vars, missed_vars_opt);
		*permutation = permutation_opt;
		*n_keys = n_index_cols_opt;
	}

	return index_opt;
}

/*
 * verify that used vars are leading columns
 */
static bool
check_leading_vars_index(IndexOptInfo *index, int n_vars,
						 bool used[INDEX_MAX_KEYS])
{
	int	i;

	if (index->nkeycolumns == n_vars)
		return true;

	for(i=0; i<n_vars; i++)
		if (used[i] == false)
			return false;

	return true;
}


/*
 * Locate index which exactly match joins vars
 */
static IndexOptInfo*
locate_outer_multicolumn_index(PlannerInfo *root, Index varno, List* vars,
							   int *permutation)
{
	ListCell   *ilist;
	RelOptInfo* rel = find_base_rel(root, varno);
	int n_vars = list_length(vars);
	bool	used[INDEX_MAX_KEYS];
	IndexOptInfo *index_opt = NULL;

	Assert(n_vars >= 1);

	foreach(ilist, rel->indexlist)
	{
		IndexOptInfo	*index = (IndexOptInfo *) lfirst(ilist);
		ListCell		*vlist;
		int				i;

		if (index->nkeycolumns < n_vars)
			continue;

		memset(used, 0, sizeof(used));

		i = 0;
		foreach (vlist, vars)
		{
			Var* var = lfirst(vlist);

			if (permutation[i] < 0 ||
				index->nkeycolumns <= permutation[i] ||
				index->indexkeys[permutation[i]] != var->varattno)
				break;

			used[i] = true;
			i += 1;
		}

		if (vlist == NULL && check_leading_vars_index(index, n_vars, used))
		{
			if (index->nkeycolumns == n_vars)
				/* found exact match vars - index, immediately return */
				return index;
			else if (index_opt == NULL ||
					 index_opt->nkeycolumns > index->nkeycolumns)
				/* found better candidate - store it */
				index_opt = index;
		}
	}

	return index_opt;
}

typedef struct InArrayClause
{
	ArrayType*	array;
	Datum*		elems;
	bool*		nulls;
	int			index;
	int			n_elems;
	int			curr_elem;
} InArrayClause;

typedef struct TupleIterator
{
	Datum	values	[INDEX_MAX_KEYS];
	bool	isnull[INDEX_MAX_KEYS];
	int		n_variants;
	int		i_variant;
	int		*permutation;
	List	*in_clauses;
	bool	isExhaustive;
} TupleIterator;

static void
initTupleIterator(TupleIterator *it, List *consts, int *permutation,
				  List *in_clauses)
{
	ListCell	*l;
	int			i;
	double		n_variants = 1;

	it->n_variants = 1;
	it->permutation = permutation;
	it->in_clauses = in_clauses;
	it->isExhaustive = false;
	for(i = 0; i < INDEX_MAX_KEYS; i++)
		it->isnull[i] = true;

	i = 0;
	foreach (l, consts)
	{
		Const* c = (Const*) lfirst(l);
		int j = permutation[i++];

		if (j<0)
			continue;
		it->values[j] = c->constvalue;
		it->isnull[j] = c->constisnull;
	}

	foreach (l, in_clauses)
	{
		InArrayClause*	iac = (InArrayClause*) lfirst(l);
		int16			elmlen;
		bool			elmbyval;
		char			elmalign;

		get_typlenbyvalalign(iac->array->elemtype,
							 &elmlen, &elmbyval, &elmalign);
		deconstruct_array(iac->array, iac->array->elemtype,
						  elmlen, elmbyval, elmalign,
						  &iac->elems, &iac->nulls, &iac->n_elems);
		iac->curr_elem = 0;
		n_variants *= (double)iac->n_elems;
	}

	if (n_variants > EXHAUSTIVE_IN_SELECTIVITY_THRESHOLD)
	{
		it->isExhaustive = true;
		it->n_variants = EXHAUSTIVE_IN_SELECTIVITY_THRESHOLD;
	}
	else
		it->n_variants = n_variants;

	it->i_variant = it->n_variants;
}

static void
resetTupleIterator(TupleIterator *it)
{
	ListCell	*l;

	it->i_variant = it->n_variants;

	foreach (l, it->in_clauses)
	{
		InArrayClause*  iac = (InArrayClause*) lfirst(l);

		iac->curr_elem = 0;
	}
}

static bool
getTupleIterator(TupleIterator *it)
{
	ListCell	*l;
	int			carry = 1;

	if (it->i_variant == 0)
		return false;

	it->i_variant--;

	foreach (l, it->in_clauses)
	{
		InArrayClause* iac = (InArrayClause*) lfirst(l);
		int j = it->permutation[iac->index];

		if (j<0)
			continue;

		if (it->isExhaustive)
		{
			/* use random subset of IN list(s) */
			iac->curr_elem = pg_prng_uint64(&pg_global_prng_state) % iac->n_elems;
		}
		else if ((iac->curr_elem += carry) >= iac->n_elems)
		{
			iac->curr_elem = 0;
			carry = 1;
		}
		else
			carry = 0;

		it->values[j] = iac->elems[iac->curr_elem];
		it->isnull[j] = iac->nulls[iac->curr_elem];
	}

	return true;
}

static double
get_numdistinct(PlannerInfo *root, IndexOptInfo* index, int n_keys)
{
	double numdistinct = 1.0;
	ListCell	*lc;
	int			i = 0;

	foreach(lc, index->indextlist)
	{
		TargetEntry *tle = lfirst(lc);
		VariableStatData	vardata;
		bool				isdefault;

		examine_variable(root, (Node*)tle->expr, 0, &vardata);

		numdistinct *= get_variable_numdistinct(&vardata, &isdefault);

		ReleaseVariableStats(vardata);

		if (++i >= n_keys)
			break;
	}

	if (numdistinct > index->tuples)
		numdistinct = index->tuples;

	return numdistinct;
}

static Selectivity
estimate_selectivity_by_index(PlannerInfo *root, IndexOptInfo* index,
							  VariableStatData *vardata,
							  List *consts, List** missed_vars, int *permutation,
							  List *in_clauses, int n_keys,
							  bool *usedEqSel,
							  bool *isWeakSelectivity)
{
	TupleIterator	it;
	Selectivity		sum = 0.0;
	TypeCacheEntry	*typentry;
	Datum			constant;
	int				nBins;
	double			nDistinct = 0.0;

	*isWeakSelectivity = false;

	if (n_keys < index->nkeycolumns )
	{
		double	nd;
		bool				isdefault;

		nDistinct = get_numdistinct(root, index, n_keys);
		nd = get_variable_numdistinct(vardata, &isdefault);

		if (isdefault == false && nDistinct > nd)
			nDistinct = sqrt(nDistinct * nd);
	}

	/*
	 * Assume that two compound types are coherent, so we can use equality
	 * function from one type to compare it with other type. Use >= and <= range
	 * definition.
	 */
	typentry = lookup_type_cache(vardata->atttype,
								 TYPECACHE_EQ_OPR | TYPECACHE_TUPDESC);
	initTupleIterator(&it, consts, permutation, in_clauses);

	/*
	 * Try to  simplify calculations: if all variants matches to small amount of
	 * bins histogram the we don't need to check tuples separately, it's enough
	 * to checck min and max tuples and compute selecivity by range of bins
	 */

	if (n_keys != index->nkeycolumns &&
		it.n_variants > RANGE_IN_SELECTIVITY_THRESHOLD)
	{
		Datum	constantMax = 0,
				constantMin = 0;
		FmgrInfo		opprocLT, opprocGT;

		fmgr_info(F_RECORD_GT, &opprocGT);
		fmgr_info(F_RECORD_LT, &opprocLT);

		/*
		 * Find min and max tuples
		 */
		while(getTupleIterator(&it))
		{
			/* we check cache invalidation message */
			if (typentry->tupDesc == NULL)
				typentry = lookup_type_cache(vardata->atttype,
											 TYPECACHE_EQ_OPR | TYPECACHE_TUPDESC);
			constant = HeapTupleGetDatum(heap_form_tuple(typentry->tupDesc,
														 it.values, it.isnull));

			if (constantMax == 0 ||
				DatumGetBool(FunctionCall2Coll(&opprocGT,
											   DEFAULT_COLLATION_OID,
											   constant, constantMax)))
			{
				constantMax = constant;
				if (constantMin != 0)
					continue;
			}
			if (constantMin == 0 ||
				DatumGetBool(FunctionCall2Coll(&opprocLT,
											   DEFAULT_COLLATION_OID,
											   constant, constantMin)))
			{
				constantMin = constant;
			}
		}

		sum = prefix_record_histogram_selectivity(vardata,
												  constantMin, constantMax,
												  n_keys, nDistinct,
												  &nBins);

		if (sum > 0 && (nBins == it.n_variants || nBins <=2))
			/*
			 * conclude that all  tuples are in the same, rather small, range of
			 * bins
			 */
			goto finish;

		/*
		 * let try tuples one by one
		 */
		sum = 0.0;
		resetTupleIterator(&it);
	}

	while(getTupleIterator(&it))
	{
		Selectivity	s;

		/* we check cache invalidation message */
		if (typentry->tupDesc == NULL)
			typentry = lookup_type_cache(vardata->atttype,
										 TYPECACHE_EQ_OPR | TYPECACHE_TUPDESC);
		constant = HeapTupleGetDatum(heap_form_tuple(typentry->tupDesc,
													 it.values, it.isnull));

		if (n_keys != index->nkeycolumns)
		{
			s = prefix_record_histogram_selectivity(vardata,
													constant, constant,
													n_keys,
													nDistinct,
													&nBins);

			if (nBins == 0 && n_keys < index->nkeycolumns && nDistinct > MULTICOLUMN_STATISTIC_FALLBACK_DISTINCT_THRESHOLD)
				*isWeakSelectivity = true;

			if (s < 0)
			{
				/*
				 * There is no histogram, fallback to single available option
				 */
				s = eqconst_selectivity(typentry->eq_opr, DEFAULT_COLLATION_OID, vardata,
											 constant, false, true, false,
											 n_keys);

				if (usedEqSel)
					*usedEqSel = true;
			}
		}
		else
		{
			s = eqconst_selectivity(typentry->eq_opr, DEFAULT_COLLATION_OID, vardata,
									constant, false, true, false,
									-1);
		}

		sum += s - s*sum;
	}

finish:
	if (it.isExhaustive)
		sum *= ((double)(it.n_variants))/EXHAUSTIVE_IN_SELECTIVITY_THRESHOLD;

	return sum;
}

typedef struct ClauseVarPair
{
	Var		*var;
	int		idx;
} ClauseVarPair;

static void
appendCVP(List **cvp, Var *var, int idx)
{
	ClauseVarPair	*e;

	e = palloc(sizeof(*e));
	e->var = var;
	e->idx = idx;

	*cvp = lappend(*cvp, e);
}

static int
findCVP(List* cvp, Var* var)
{
	ListCell* lc;
	foreach(lc, cvp)
	{
		ClauseVarPair* c = (ClauseVarPair*)lfirst(lc);
		if (c->var == var)
			return c->idx;
	}
	return -1;
}

static bool
initVarData(IndexOptInfo *index, VariableStatData *vardata)
{
	Relation	indexRel = index_open(index->indexoid, AccessShareLock);

	if (!indexRel->rd_rel->reltype)
	{
		index_close(indexRel, AccessShareLock);

		return false;
	}

	memset(vardata, 0, sizeof(*vardata));
	vardata->isunique = index->unique;
	vardata->atttype = indexRel->rd_rel->reltype;
	vardata->rel = index->rel;
	vardata->acl_ok = true;
	vardata->statsTuple = SearchSysCache3(STATRELATTINH,
										  ObjectIdGetDatum(index->indexoid),
										  Int16GetDatum(1),
										  BoolGetDatum(false));
	vardata->freefunc = ReleaseSysCache;

	index_close(indexRel, AccessShareLock);

	if (!HeapTupleIsValid(vardata->statsTuple))
	{
		ReleaseVariableStats(*vardata);
		return false;
	}

	vardata->sslots = index->sslots;

	return true;
}

static int
markEstimatedColumns(Bitmapset **estimatedclauses, List	*pairs,
					 List	*vars, List	*missed_vars)
{
	ListCell	*l;
	int			n_estimated = 0;

	foreach(l, vars)
	{
		Var* var = (Var *) lfirst(l);
		ListCell	*ll;

		if (list_member_ptr(missed_vars, var))
			continue;

		foreach(ll, pairs)
		{
			ClauseVarPair *cvp=(ClauseVarPair*)lfirst(ll);

			if (cvp->var == var)
			{
				*estimatedclauses = bms_add_member(*estimatedclauses, cvp->idx);
				n_estimated += 1;
				break;
			}
		}

		Assert(ll != NULL);
	}

	return n_estimated;
}

#define SET_VARNOS(vn) do {										\
	if ((vn) != 0)												\
	{															\
		if (data[0].varno == 0)									\
			data[0].varno = (vn);								\
		else if (data[1].varno == 0 && data[0].varno != (vn))	\
			data[1].varno = (vn);								\
	}															\
} while(0)

#define GET_RELBY_NO(vn)	\
((data[0].varno == (vn) && (vn) != 0) ? &data[0] : ((data[1].varno == (vn) && (vn) != 0) ? &data[1] : NULL))

#define SET_CURDATA(vn)	((cur = GET_RELBY_NO(vn)) != NULL)

static bool
hasSAOPRestriction(List *clauses, Bitmapset *estimatedclauses)
{
	ListCell	*l;
	int		i = -1;

	foreach(l, clauses)
	{
		Node* clause = (Node *) lfirst(l);
		RestrictInfo	*rinfo = NULL;

		i++;
		if (bms_is_member(i, estimatedclauses))
			continue;

		if (IsA(clause, RestrictInfo))
		{
			rinfo = (RestrictInfo *) clause;
			if (!rinfo->orclause)
				clause = (Node*)rinfo->clause;
		}

		if (IsA(clause, ScalarArrayOpExpr))
			return true;
	}

	return false;
}

/*
 * Check if clauses represent multicolumn join with compound indexes available
 * for both side of comparison of indexed columns of one relation with constant
 * values. If so, calculates selectivity of compound type comparison and returns
 * true.
 */
static bool
use_multicolumn_statistic(PlannerInfo *root, List *clauses, int varRelid,
						  JoinType jointype, SpecialJoinInfo *sjinfo,
						  Selectivity* restrict_selectivity, Selectivity *join_selectivity,
						  Bitmapset	**estimatedclauses, CorrelationKind
						  *correlationKind)
{
	ListCell			*l;
	List*				var_clause_map = NIL;
	List*				missed_vars = NIL;
	int					i;
	int					*permutation = NULL;
	int					n_estimated = 0;
	int					n_keys;
	TypeCacheEntry		*typentry;

	struct	{
		Index				varno;

		List				*restrictionColumns;
		List				*restrictionConsts;
		List				*in_clauses;
		List				*ineqRestrictionClauses;

		List				*joinColumns;

		IndexOptInfo		*index;
		VariableStatData	vardata;
	} data[2], *cur;

	if (list_length(clauses) < 1)
		return false;

	/*
	 * For simple queries default estimator is good enough, but multicolumn
	 * statistic could be too expensive because of search and decompress a lot
	 * of stat data (histogramm of multicolumn indexes).
	 */
	if (root->join_rel_list == NIL &&
		root->simple_rel_array_size <= 2 /* 0th is always empty */ &&
		/* list_length(clauses) < 4 && */
		hasSAOPRestriction(clauses, *estimatedclauses) == false)
		return false;

	*correlationKind = CKIndepend;
	memset(data, 0, sizeof(data));

	i=-1;
	foreach(l, clauses)
	{
		Node* clause = (Node *) lfirst(l);
		RestrictInfo* rinfo = NULL;
		OpExpr	   *opclause = NULL;

		i++;

		/* do not use already estimated clauses */
		if (bms_is_member(i, *estimatedclauses))
			continue;

		if (IsA(clause, RestrictInfo))
		{
			rinfo = (RestrictInfo *) clause;
			if (!rinfo->orclause)
				clause = (Node*)rinfo->clause;
		}
		if (IsA(clause, OpExpr))
			opclause = (OpExpr*)clause;

		if (IsA(clause, Var)) /* boolean variable */
		{
			Var* var1 = (Var*)clause;

			SET_VARNOS(var1->varno);
			if (SET_CURDATA(var1->varno))
			{
				cur->restrictionColumns = lappend(cur->restrictionColumns, var1);
				appendCVP(&var_clause_map, var1, i);
				cur->restrictionConsts = lappend(cur->restrictionConsts,
												 makeBoolConst(true, false));
			}
		}
		else if (IsA(clause, BoolExpr) && ((BoolExpr*)clause)->boolop == NOT_EXPR) /* (NOT bool_expr) */
		{
			Node* arg1 = (Node*) linitial( ((BoolExpr*)clause)->args);
			Var* var1 = get_var(arg1);

			if (var1 == NULL)
				continue;

			SET_VARNOS(var1->varno);
			if (SET_CURDATA(var1->varno))
			{
				cur->restrictionColumns = lappend(cur->restrictionColumns, var1);
				appendCVP(&var_clause_map, var1, i);
				cur->restrictionConsts = lappend(cur->restrictionConsts,
												 makeBoolConst(false, false));
			}
		}
		else if (IsA(clause, ScalarArrayOpExpr))
		{
			ScalarArrayOpExpr* in = (ScalarArrayOpExpr*)clause;
			Var* var1;
			Node* arg2;
			InArrayClause* iac;

			var1 = get_var((Node*)linitial(in->args));
			arg2 = (Node*) lsecond(in->args);

			if (!in->useOr
				|| list_length(in->args) != 2
				|| get_oprrest(in->opno) != F_EQSEL
				|| var1 == NULL
				|| !IsA(arg2, Const))
			{
				continue;
			}

			SET_VARNOS(var1->varno);
			if (SET_CURDATA(var1->varno))
			{
				cur->restrictionColumns = lappend(cur->restrictionColumns, var1);
				appendCVP(&var_clause_map, var1, i);
				cur->restrictionConsts = lappend(cur->restrictionConsts, arg2);

				iac = (InArrayClause*)palloc(sizeof(InArrayClause));
				iac->array = (ArrayType*)DatumGetPointer(((Const*)arg2)->constvalue);
				iac->index = list_length(cur->restrictionConsts) - 1;

				cur->in_clauses = lappend(cur->in_clauses, iac);
			}
		}
		else if (opclause
				 && list_length(opclause->args) == 2)
		{
			int oprrest = get_oprrest(opclause->opno);
			Node* arg1 = (Node*) linitial(opclause->args);
			Node* arg2 = (Node*) lsecond(opclause->args);
			Var* var1 = get_var(arg1);
			Var* var2 = get_var(arg2);

			if (oprrest == F_EQSEL && treat_as_join_clause(root, (Node*)opclause, NULL, varRelid, sjinfo))
			{
				if (var1 == NULL || var2 == NULL || var1->vartype != var2->vartype)
					continue;

				SET_VARNOS(var1->varno);
				SET_VARNOS(var2->varno);

				if (var1->varno == data[0].varno && var2->varno == data[1].varno)
				{
					data[0].joinColumns = lappend(data[0].joinColumns, var1);
					appendCVP(&var_clause_map, var1, i);
					data[1].joinColumns = lappend(data[1].joinColumns, var2);
					appendCVP(&var_clause_map, var2, i);
				}
				else if (var1->varno == data[1].varno && var2->varno == data[0].varno)
				{
					data[0].joinColumns = lappend(data[0].joinColumns, var2);
					appendCVP(&var_clause_map, var2, i);
					data[1].joinColumns = lappend(data[1].joinColumns, var1);
					appendCVP(&var_clause_map, var1, i);
				}
			}
			else /* Estimate selectivity for a restriction clause. */
			{
				/*
				 * Give up if it is not equality comparison of variable with
				 * constant or some other clause is treated as join condition
				 */
				if (((var1 == NULL) == (var2 == NULL)))
					continue;

				if (var1 == NULL)
				{
					/* swap var1 and var2 */
					var1 = var2;
					arg2 = arg1;
				}

				SET_VARNOS(var1->varno);

				if (SET_CURDATA(var1->varno))
				{
					if ((rinfo && is_pseudo_constant_clause_relids(arg2, rinfo->right_relids))
						|| (!rinfo && NumRelids(root, clause) == 1 && is_pseudo_constant_clause(arg2)))
					{
						/* Restriction clause with a pseudoconstant . */
						Node* const_val = estimate_expression_value(root, arg2);

						if (IsA(const_val, Const))
						{
							switch (oprrest)
							{
								case F_EQSEL:
									cur->restrictionColumns =
										lappend(cur->restrictionColumns, var1);
									cur->restrictionConsts =
										lappend(cur->restrictionConsts, const_val);
									appendCVP(&var_clause_map, var1, i);
									break;
								case F_SCALARGTSEL:
								case F_SCALARGESEL:
								case F_SCALARLTSEL:
								case F_SCALARLESEL:
									/*
									 * We do not consider range predicates now,
									 * but we can mark them as estimated
									 * if their variables are covered by index.
									 */
									appendCVP(&var_clause_map, var1, i);
									cur->ineqRestrictionClauses =
										lappend(cur->ineqRestrictionClauses, var1);
									break;
								default:
									break;
							}
						}
					}

				}
			}
		}
		/* else just skip clause to work with it later in caller */
	}

	*restrict_selectivity = 1.0;
	*join_selectivity = 1.0;

	/*
	 * First, try to estimate selectivity by restrictions
	 */
	for(i=0; i<lengthof(data); i++)
	{
		cur = &data[i];

		/* compute restriction clauses if applicable */
		if (cur->varno == 0 || list_length(cur->restrictionColumns) < 1)
			continue;

		cur->index = locate_inner_multicolumn_index(
					root, cur->varno, cur->restrictionColumns,
					list_length(clauses), &permutation, &missed_vars, &n_keys);

		if (cur->index && n_keys > 1 &&
			initVarData(cur->index, &cur->vardata))
		{
			bool	usedEqSel= false;
			double s;
			bool usedWeakSelectivity;

			s = estimate_selectivity_by_index(
									root, cur->index, &cur->vardata,
									cur->restrictionConsts, &missed_vars, permutation,
									cur->in_clauses, n_keys, &usedEqSel, &usedWeakSelectivity);

			if (usedWeakSelectivity)
			{
				double simpleSelectivity = 1.0;
				foreach(l, cur->restrictionColumns)
				{
					Node* clause;
					int   clauseIndex;
					double clauseSelectivity;
					
					clauseIndex = findCVP(var_clause_map, (Var*)lfirst(l));
					if (clauseIndex < 0)
					{
						simpleSelectivity = -1;
						break;
					}

					clause = (Node*)list_nth(clauses, clauseIndex);

					clauseSelectivity = clause_selectivity_ext(root, clause, varRelid, jointype, sjinfo, false);
					
					simpleSelectivity *= clauseSelectivity;
				}

				if (simpleSelectivity > 0 && simpleSelectivity < s)
				{
					s = simpleSelectivity;
				}
			}

			*restrict_selectivity *= s;

			ReleaseVariableStats(cur->vardata);

			/*
			 * mark inequality clauses as used, see estimate_selectivity_by_index()
			 */
			if (usedEqSel)
			{
				foreach(l, cur->ineqRestrictionClauses)
				{
					Var* var = (Var *) lfirst(l);

					/*
					 * Note, restrictionColumns will contains extra columns !
					 */
					for(i=0; i<cur->index->nkeycolumns; i++)
						if (cur->index->indexkeys[i] == var->varattno)
							cur->restrictionColumns =
								lappend(cur->restrictionColumns, var);
				}
			}

			n_estimated +=
				markEstimatedColumns(estimatedclauses, var_clause_map,
									 cur->restrictionColumns, missed_vars);
		}

		if (permutation)
		{
			pfree(permutation);
			permutation = NULL;
		}
	}

	/* Deal with join clauses, if possible */
	if (list_length(data[0].joinColumns) < 1)
		goto cleanup;

	data[0].index = locate_inner_multicolumn_index(
						root,
						data[0].varno, data[0].joinColumns,
						list_length(clauses), &permutation, &missed_vars, &n_keys);

	if (!data[0].index || n_keys < 1)
		goto cleanup;

	Assert(permutation != NULL);
	Assert(data[1].varno != 0);
	Assert(list_length(data[0].joinColumns) == list_length(data[1].joinColumns));

	data[1].index = locate_outer_multicolumn_index(
										root,
										data[1].varno, data[1].joinColumns,
										permutation);

	if (!data[1].index)
		goto cleanup;

	if (!initVarData(data[0].index, &data[0].vardata))
		goto cleanup;

	if (!initVarData(data[1].index, &data[1].vardata))
	{
		ReleaseVariableStats(data[0].vardata);
		goto cleanup;
	}

	typentry = lookup_type_cache(data[0].vardata.atttype, TYPECACHE_EQ_OPR);
	*join_selectivity *= eqjoin_selectivity(root, typentry->eq_opr,
											DEFAULT_COLLATION_OID,
											&data[0].vardata, &data[1].vardata,
											sjinfo, n_keys);

	/* for self join */
	if (data[0].index->indexoid == data[1].index->indexoid)
		*correlationKind = CKSelf;
	else
	{
		RangeTblEntry *lrte = planner_rt_fetch(data[0].index->rel->relid, root),
					  *rrte = planner_rt_fetch(data[1].index->rel->relid, root);

		if (lrte->relid == rrte->relid)
			*correlationKind = CKSelf;
	}

	for (i = 0; i < lengthof(data); i++)
		ReleaseVariableStats(data[i].vardata);

	n_estimated +=
				markEstimatedColumns(estimatedclauses, var_clause_map,
									 data[0].joinColumns, missed_vars);

cleanup:
	if (permutation)
		pfree(permutation);

	return n_estimated != 0;
}

/****************************************************************************
 *		ROUTINES TO COMPUTE SELECTIVITIES
 ****************************************************************************/

/*
 * clauselist_selectivity -
 *	  Compute the selectivity of an implicitly-ANDed list of boolean
 *	  expression clauses.  The list can be empty, in which case 1.0
 *	  must be returned.  List elements may be either RestrictInfos
 *	  or bare expression clauses --- the former is preferred since
 *	  it allows caching of results.
 *
 * See clause_selectivity() for the meaning of the additional parameters.
 *
 * The basic approach is to apply extended statistics first, on as many
 * clauses as possible, in order to capture cross-column dependencies etc.
 * The remaining clauses are then estimated by taking the product of their
 * selectivities, but that's only right if they have independent
 * probabilities, and in reality they are often NOT independent even if they
 * only refer to a single column.  So, we want to be smarter where we can.
 *
 * We also recognize "range queries", such as "x > 34 AND x < 42".  Clauses
 * are recognized as possible range query components if they are restriction
 * opclauses whose operators have scalarltsel or a related function as their
 * restriction selectivity estimator.  We pair up clauses of this form that
 * refer to the same variable.  An unpairable clause of this kind is simply
 * multiplied into the selectivity product in the normal way.  But when we
 * find a pair, we know that the selectivities represent the relative
 * positions of the low and high bounds within the column's range, so instead
 * of figuring the selectivity as hisel * losel, we can figure it as hisel +
 * losel - 1.  (To visualize this, see that hisel is the fraction of the range
 * below the high bound, while losel is the fraction above the low bound; so
 * hisel can be interpreted directly as a 0..1 value but we need to convert
 * losel to 1-losel before interpreting it as a value.  Then the available
 * range is 1-losel to hisel.  However, this calculation double-excludes
 * nulls, so really we need hisel + losel + null_frac - 1.)
 *
 * If either selectivity is exactly DEFAULT_INEQ_SEL, we forget this equation
 * and instead use DEFAULT_RANGE_INEQ_SEL.  The same applies if the equation
 * yields an impossible (negative) result.
 *
 * A free side-effect is that we can recognize redundant inequalities such
 * as "x < 4 AND x < 5"; only the tighter constraint will be counted.
 *
 * Of course this is all very dependent on the behavior of the inequality
 * selectivity functions; perhaps some day we can generalize the approach.
 */

static void
appendSelectivityRes(Selectivity s[5], Selectivity sel, CorrelationKind ck)
{
	switch(ck)
	{
		case CKRestrict:
			s[ck] *= sel;
			break;
		case CKSelf:
		case CKLikelySelf:
			s[CKMul] *= sel;
			if (s[ck] > sel)
				s[ck] = sel;
			/* FALLTHROUGH */
		case CKIndepend:
			s[CKIndepend] *= sel;
			break;
		default:
			elog(ERROR, "unknown selectivity kind: %d", ck);
	}
}

static Selectivity
finalizeSelectivityRes(Selectivity s[5])
{
	Selectivity	sel;

	sel = s[CKRestrict] * s[CKIndepend];

	if (s[CKIndepend] != s[CKMul])
	{
		/* we have both independ and correlated - fallback */
		sel *= s[CKMul];
	}
	else
	{
		/* we have only correlated join clauses */
		if (s[CKLikelySelf] != 1.0 && sel < s[CKLikelySelf])
			sel = sel + (s[CKLikelySelf] - sel) * 0.25;

		if (s[CKSelf] != 1.0 && sel < s[CKSelf])
			sel = sel + (s[CKSelf] - sel) * 1.0;
	}

	return sel;
}

Selectivity
clauselist_selectivity(PlannerInfo *root,
					   List *clauses,
					   int varRelid,
					   JoinType jointype,
					   SpecialJoinInfo *sjinfo)
{
	return clauselist_selectivity_ext(root, clauses, varRelid,
									  jointype, sjinfo, true);
}

/*
 * clauselist_selectivity_ext -
 *	  Extended version of clauselist_selectivity().  If "use_extended_stats"
 *	  is false, all extended statistics will be ignored, and only per-column
 *	  statistics will be used.
 */
Selectivity
clauselist_selectivity_ext(PlannerInfo *root,
						   List *clauses,
						   int varRelid,
						   JoinType jointype,
						   SpecialJoinInfo *sjinfo,
						   bool use_extended_stats)
{
	Selectivity s[5 /* per CorrelationKind */]  = {1.0, 1.0, 1.0, 1.0, 1.0};
	Selectivity s2 = 1.0, s3 = 1.0;
	RelOptInfo *rel;
	Bitmapset  *estimatedclauses = NULL;
	RangeQueryClause *rqlist = NULL;
	ListCell   *l;
	int			listidx;
	CorrelationKind ck;

	/*
	 * If there's exactly one clause, just go directly to
	 * clause_selectivity_ext(). None of what we might do below is relevant.
	 */
	if (list_length(clauses) == 1)
		return clause_selectivity_ext(root, (Node *) linitial(clauses),
									  varRelid, jointype, sjinfo,
									  use_extended_stats);

	/*
	 * Determine if these clauses reference a single relation.  If so, and if
	 * it has extended statistics, try to apply those.
	 */
	rel = find_single_rel_for_clauses(root, clauses);
	if (use_extended_stats && rel && rel->rtekind == RTE_RELATION && rel->statlist != NIL)
	{
		/*
		 * Estimate as many clauses as possible using extended statistics.
		 *
		 * 'estimatedclauses' is populated with the 0-based list position
		 * index of clauses estimated here, and that should be ignored below.
		 */
		s2 = statext_clauselist_selectivity(root, clauses, varRelid,
											jointype, sjinfo, rel,
											&estimatedclauses, false);
		appendSelectivityRes(s, s2, CKRestrict);
	}

	/*
	 * Check if join conjuncts corresponds to some compound indexes on left and
	 * right joined relations or indexed columns of one relation is compared
	 * with constant values. In this case selectivity of join can be calculated
	 * based on statistic of this compound index.
	 */
	while(use_multicolumn_statistic(root, clauses, varRelid, jointype, sjinfo,
									&s2, &s3, &estimatedclauses, &ck))
	{
		appendSelectivityRes(s, s2, CKRestrict);
		appendSelectivityRes(s, s3, ck);
	}

	/*
	 * Apply normal selectivity estimates for remaining clauses. We'll be
	 * careful to skip any clauses which were already estimated above.
	 *
	 * Anything that doesn't look like a potential rangequery clause gets
	 * multiplied into s1 and forgotten. Anything that does gets inserted into
	 * an rqlist entry.
	 */
	listidx = -1;
	foreach(l, clauses)
	{
		Node	   *clause = (Node *) lfirst(l);
		RestrictInfo *rinfo;

		listidx++;

		/*
		 * Skip this clause if it's already been estimated by some other
		 * statistics above.
		 */
		if (bms_is_member(listidx, estimatedclauses))
			continue;

		/* Compute the selectivity of this clause in isolation */
		s2 = clause_selectivity_ext(root, clause, varRelid, jointype, sjinfo,
									use_extended_stats);

		/*
		 * Check for being passed a RestrictInfo.
		 *
		 * If it's a pseudoconstant RestrictInfo, then s2 is either 1.0 or
		 * 0.0; just use that rather than looking for range pairs.
		 */
		if (IsA(clause, RestrictInfo))
		{
			rinfo = (RestrictInfo *) clause;
			if (rinfo->pseudoconstant)
			{
				appendSelectivityRes(s, s2, CKRestrict);
				continue;
			}
			clause = (Node *) rinfo->clause;
		}
		else
			rinfo = NULL;

		/*
		 * See if it looks like a restriction clause with a pseudoconstant on
		 * one side.  (Anything more complicated than that might not behave in
		 * the simple way we are expecting.)  Most of the tests here can be
		 * done more efficiently with rinfo than without.
		 */
		ck = treat_as_join_clause(root, clause, rinfo, varRelid, sjinfo) ?
				CKIndepend : CKRestrict;
		if (is_opclause(clause) && list_length(((OpExpr *) clause)->args) == 2)
		{
			OpExpr	   *expr = (OpExpr *) clause;
			bool		varonleft = true;
			bool		ok;

			if (ck == CKIndepend)
				ck = get_correlation_kind(root, varRelid, expr);

			if (rinfo)
			{
				ok = (rinfo->num_base_rels == 1) &&
					(is_pseudo_constant_clause_relids(lsecond(expr->args),
													  rinfo->right_relids) ||
					 (varonleft = false,
					  is_pseudo_constant_clause_relids(linitial(expr->args),
													   rinfo->left_relids)));
			}
			else
			{
				ok = (NumRelids(root, clause) == 1) &&
					(is_pseudo_constant_clause(lsecond(expr->args)) ||
					 (varonleft = false,
					  is_pseudo_constant_clause(linitial(expr->args))));
			}

			if (ok)
			{
				/*
				 * If it's not a "<"/"<="/">"/">=" operator, just merge the
				 * selectivity in generically.  But if it's the right oprrest,
				 * add the clause to rqlist for later processing.
				 */
				switch (get_oprrest(expr->opno))
				{
					case F_SCALARLTSEL:
					case F_SCALARLESEL:
						addRangeClause(&rqlist, clause,
									   varonleft, true, s2);
						break;
					case F_SCALARGTSEL:
					case F_SCALARGESEL:
						addRangeClause(&rqlist, clause,
									   varonleft, false, s2);
						break;
					default:
						/* Just merge the selectivity in generically */
						appendSelectivityRes(s, s2, ck);
						break;
				}
				continue;		/* drop to loop bottom */
			}
		}

		/* Not the right form, so treat it generically. */
		appendSelectivityRes(s, s2, ck);
	}

	/*
	 * Now scan the rangequery pair list.
	 */
	while (rqlist != NULL)
	{
		RangeQueryClause *rqnext;

		if (rqlist->have_lobound && rqlist->have_hibound)
		{
			/* Successfully matched a pair of range clauses */

			/*
			 * Exact equality to the default value probably means the
			 * selectivity function punted.  This is not airtight but should
			 * be good enough.
			 */
			if (rqlist->hibound == DEFAULT_INEQ_SEL ||
				rqlist->lobound == DEFAULT_INEQ_SEL)
			{
				s2 = DEFAULT_RANGE_INEQ_SEL;
			}
			else
			{
				s2 = rqlist->hibound + rqlist->lobound - 1.0;

				/* Adjust for double-exclusion of NULLs */
				s2 += nulltestsel(root, IS_NULL, rqlist->var,
								  varRelid, jointype, sjinfo);

				/*
				 * A zero or slightly negative s2 should be converted into a
				 * small positive value; we probably are dealing with a very
				 * tight range and got a bogus result due to roundoff errors.
				 * However, if s2 is very negative, then we probably have
				 * default selectivity estimates on one or both sides of the
				 * range that we failed to recognize above for some reason.
				 */
				if (s2 <= 0.0)
				{
					if (s2 < -0.01)
					{
						/*
						 * No data available --- use a default estimate that
						 * is small, but not real small.
						 */
						s2 = DEFAULT_RANGE_INEQ_SEL;
					}
					else
					{
						/*
						 * It's just roundoff error; use a small positive
						 * value
						 */
						s2 = 1.0e-10;
					}
				}
			}
			/* Merge in the selectivity of the pair of clauses */
			appendSelectivityRes(s, s2, CKRestrict);
		}
		else
		{
			/* Only found one of a pair, merge it in generically */
			appendSelectivityRes(s, (rqlist->have_lobound) ? rqlist->lobound :
								 rqlist->hibound, CKRestrict);
		}
		/* release storage and advance */
		rqnext = rqlist->next;
		pfree(rqlist);
		rqlist = rqnext;
	}

	return finalizeSelectivityRes(s);
}

/*
 * clauselist_selectivity_or -
 *	  Compute the selectivity of an implicitly-ORed list of boolean
 *	  expression clauses.  The list can be empty, in which case 0.0
 *	  must be returned.  List elements may be either RestrictInfos
 *	  or bare expression clauses --- the former is preferred since
 *	  it allows caching of results.
 *
 * See clause_selectivity() for the meaning of the additional parameters.
 *
 * The basic approach is to apply extended statistics first, on as many
 * clauses as possible, in order to capture cross-column dependencies etc.
 * The remaining clauses are then estimated as if they were independent.
 */
static Selectivity
clauselist_selectivity_or(PlannerInfo *root,
						  List *clauses,
						  int varRelid,
						  JoinType jointype,
						  SpecialJoinInfo *sjinfo,
						  bool use_extended_stats)
{
	Selectivity s1 = 0.0;
	RelOptInfo *rel;
	Bitmapset  *estimatedclauses = NULL;
	ListCell   *lc;
	int			listidx;

	/*
	 * Determine if these clauses reference a single relation.  If so, and if
	 * it has extended statistics, try to apply those.
	 */
	rel = find_single_rel_for_clauses(root, clauses);
	if (use_extended_stats && rel && rel->rtekind == RTE_RELATION && rel->statlist != NIL)
	{
		/*
		 * Estimate as many clauses as possible using extended statistics.
		 *
		 * 'estimatedclauses' is populated with the 0-based list position
		 * index of clauses estimated here, and that should be ignored below.
		 */
		s1 = statext_clauselist_selectivity(root, clauses, varRelid,
											jointype, sjinfo, rel,
											&estimatedclauses, true);
	}

	/*
	 * Estimate the remaining clauses as if they were independent.
	 *
	 * Selectivities for an OR clause are computed as s1+s2 - s1*s2 to account
	 * for the probable overlap of selected tuple sets.
	 *
	 * XXX is this too conservative?
	 */
	listidx = -1;
	foreach(lc, clauses)
	{
		Selectivity s2;

		listidx++;

		/*
		 * Skip this clause if it's already been estimated by some other
		 * statistics above.
		 */
		if (bms_is_member(listidx, estimatedclauses))
			continue;

		s2 = clause_selectivity_ext(root, (Node *) lfirst(lc), varRelid,
									jointype, sjinfo, use_extended_stats);

		s1 = s1 + s2 - s1 * s2;
	}

	return s1;
}

/*
 * addRangeClause --- add a new range clause for clauselist_selectivity
 *
 * Here is where we try to match up pairs of range-query clauses
 */
static void
addRangeClause(RangeQueryClause **rqlist, Node *clause,
			   bool varonleft, bool isLTsel, Selectivity s2)
{
	RangeQueryClause *rqelem;
	Node	   *var;
	bool		is_lobound;

	if (varonleft)
	{
		var = get_leftop((Expr *) clause);
		is_lobound = !isLTsel;	/* x < something is high bound */
	}
	else
	{
		var = get_rightop((Expr *) clause);
		is_lobound = isLTsel;	/* something < x is low bound */
	}

	for (rqelem = *rqlist; rqelem; rqelem = rqelem->next)
	{
		/*
		 * We use full equal() here because the "var" might be a function of
		 * one or more attributes of the same relation...
		 */
		if (!equal(var, rqelem->var))
			continue;
		/* Found the right group to put this clause in */
		if (is_lobound)
		{
			if (!rqelem->have_lobound)
			{
				rqelem->have_lobound = true;
				rqelem->lobound = s2;
			}
			else
			{

				/*------
				 * We have found two similar clauses, such as
				 * x < y AND x <= z.
				 * Keep only the more restrictive one.
				 *------
				 */
				if (rqelem->lobound > s2)
					rqelem->lobound = s2;
			}
		}
		else
		{
			if (!rqelem->have_hibound)
			{
				rqelem->have_hibound = true;
				rqelem->hibound = s2;
			}
			else
			{

				/*------
				 * We have found two similar clauses, such as
				 * x > y AND x >= z.
				 * Keep only the more restrictive one.
				 *------
				 */
				if (rqelem->hibound > s2)
					rqelem->hibound = s2;
			}
		}
		return;
	}

	/* No matching var found, so make a new clause-pair data structure */
	rqelem = (RangeQueryClause *) palloc(sizeof(RangeQueryClause));
	rqelem->var = var;
	if (is_lobound)
	{
		rqelem->have_lobound = true;
		rqelem->have_hibound = false;
		rqelem->lobound = s2;
	}
	else
	{
		rqelem->have_lobound = false;
		rqelem->have_hibound = true;
		rqelem->hibound = s2;
	}
	rqelem->next = *rqlist;
	*rqlist = rqelem;
}

/*
 * find_single_rel_for_clauses
 *		Examine each clause in 'clauses' and determine if all clauses
 *		reference only a single relation.  If so return that relation,
 *		otherwise return NULL.
 */
static RelOptInfo *
find_single_rel_for_clauses(PlannerInfo *root, List *clauses)
{
	int			lastrelid = 0;
	ListCell   *l;

	foreach(l, clauses)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(l);
		int			relid;

		/*
		 * If we have a list of bare clauses rather than RestrictInfos, we
		 * could pull out their relids the hard way with pull_varnos().
		 * However, currently the extended-stats machinery won't do anything
		 * with non-RestrictInfo clauses anyway, so there's no point in
		 * spending extra cycles; just fail if that's what we have.
		 *
		 * An exception to that rule is if we have a bare BoolExpr AND clause.
		 * We treat this as a special case because the restrictinfo machinery
		 * doesn't build RestrictInfos on top of AND clauses.
		 */
		if (is_andclause(rinfo))
		{
			RelOptInfo *rel;

			rel = find_single_rel_for_clauses(root,
											  ((BoolExpr *) rinfo)->args);

			if (rel == NULL)
				return NULL;
			if (lastrelid == 0)
				lastrelid = rel->relid;
			else if (rel->relid != lastrelid)
				return NULL;

			continue;
		}

		if (!IsA(rinfo, RestrictInfo))
			return NULL;

		if (bms_is_empty(rinfo->clause_relids))
			continue;			/* we can ignore variable-free clauses */
		if (!bms_get_singleton_member(rinfo->clause_relids, &relid))
			return NULL;		/* multiple relations in this clause */
		if (lastrelid == 0)
			lastrelid = relid;	/* first clause referencing a relation */
		else if (relid != lastrelid)
			return NULL;		/* relation not same as last one */
	}

	if (lastrelid != 0)
		return find_base_rel(root, lastrelid);

	return NULL;				/* no clauses */
}

/*
 * treat_as_join_clause -
 *	  Decide whether an operator clause is to be handled by the
 *	  restriction or join estimator.  Subroutine for clause_selectivity().
 */
static inline bool
treat_as_join_clause(PlannerInfo *root, Node *clause, RestrictInfo *rinfo,
					 int varRelid, SpecialJoinInfo *sjinfo)
{
	if (varRelid != 0)
	{
		/*
		 * Caller is forcing restriction mode (eg, because we are examining an
		 * inner indexscan qual).
		 */
		return false;
	}
	else if (sjinfo == NULL)
	{
		/*
		 * It must be a restriction clause, since it's being evaluated at a
		 * scan node.
		 */
		return false;
	}
	else
	{
		/*
		 * Otherwise, it's a join if there's more than one base relation used.
		 * We can optimize this calculation if an rinfo was passed.
		 *
		 * XXX	Since we know the clause is being evaluated at a join, the
		 * only way it could be single-relation is if it was delayed by outer
		 * joins.  We intentionally count only baserels here, not OJs that
		 * might be present in rinfo->clause_relids, so that we direct such
		 * cases to the restriction qual estimators not join estimators.
		 * Eventually some notice should be taken of the possibility of
		 * injected nulls, but we'll likely want to do that in the restriction
		 * estimators rather than starting to treat such cases as join quals.
		 */
		if (rinfo)
			return (rinfo->num_base_rels > 1);
		else
			return (NumRelids(root, clause) > 1);
	}
}

typedef struct RangeTblEntryContext {
	RangeTblEntry	*rte;
	int				count;
} RangeTblEntryContext;

static bool
find_rte_walker(Node *node, RangeTblEntryContext *context)
{
	if (node == NULL)
		return false;

	if (context->count > 1)
		return true; /* skip rest */

	if (IsA(node, RangeTblEntry)) {
		RangeTblEntry	*rte = (RangeTblEntry*)node;

		if (rte->rtekind == RTE_RELATION)
		{
			if (context->count == 0)
			{
				context->count++;
				context->rte=rte;
			}
			else if (rte->relid != context->rte->relid)
			{
				context->count++;
				return true; /* more that one relation in subtree */
			}
		}
		else if (!(rte->rtekind == RTE_SUBQUERY || rte->rtekind == RTE_JOIN ||
				   rte->rtekind == RTE_CTE))
		{
			context->count++;
			return true; /* more that one relation in subtree */
		}

		return false; /* allow range_table_walker to continue */
	}

	if (IsA(node, Query))
		return query_tree_walker((Query *) node, find_rte_walker,
								 (void *) context, QTW_EXAMINE_RTES_BEFORE);

	return expression_tree_walker(node, find_rte_walker, (void *) context);
}

static RangeTblEntry*
find_single_rte(RangeTblEntry *node)
{
	RangeTblEntryContext	context;

	context.rte = NULL;
	context.count = 0;

	(void)range_table_walker(list_make1(node),
							 find_rte_walker,
							 (void *) &context, QTW_EXAMINE_RTES_BEFORE);

	return context.count == 1 ? context.rte : NULL;
}

#define IsSameRelationRTE(a, b)	( \
	(a)->rtekind == (b)->rtekind && \
	(a)->rtekind == RTE_RELATION && \
	(a)->relid == (b)->relid \
)


/*
 * Any self join or join with aggregation over the same table
 */

static CorrelationKind
get_correlation_kind(PlannerInfo *root, int varRelid, OpExpr* expr)
{
	Node	*left_arg, *right_arg;
	Relids	left_varnos, right_varnos;
	int		left_varno, right_varno;
	RangeTblEntry	*left_rte, *right_rte;

	if (varRelid != 0)
		/* We consider only case of joins, not restriction mode */
		return CKIndepend;

	/* Check if it is equality comparison */
	if (get_oprrest(expr->opno) != F_EQSEL)
		return CKIndepend;

	left_arg = linitial(expr->args);
	right_arg = lsecond(expr->args);

	/*
	 * Check if it is join of two different relations
	 */
	left_varnos = pull_varnos(root, left_arg);
	right_varnos = pull_varnos(root, right_arg);
	if (!bms_get_singleton_member(left_varnos, &left_varno) ||
		!bms_get_singleton_member(right_varnos, &right_varno) ||
		left_varno == right_varno)
		return CKIndepend;

	left_rte = planner_rt_fetch(left_varno, root);
	right_rte = planner_rt_fetch(right_varno, root);

	if (IsSameRelationRTE(left_rte, right_rte))
	{
		Var *lvar = get_var(left_arg),
			*rvar = get_var(right_arg);

		/* self join detected, check if it simple a=b clause */
		if (lvar == NULL || rvar == NULL)
			return CKLikelySelf;
		return (lvar->varattno == rvar->varattno) ?
											CKSelf : CKLikelySelf;
	}

	if ((left_rte = find_single_rte(left_rte)) == NULL)
		return CKIndepend;
	if ((right_rte = find_single_rte(right_rte)) == NULL)
		return CKIndepend;

	if (IsSameRelationRTE(left_rte, right_rte))
	{
		/* self join detected, but over some transformation which cannot be
		 * flatten */
		return CKLikelySelf;
	}

	return CKIndepend;
}

/*
 * clause_selectivity -
 *	  Compute the selectivity of a general boolean expression clause.
 *
 * The clause can be either a RestrictInfo or a plain expression.  If it's
 * a RestrictInfo, we try to cache the selectivity for possible re-use,
 * so passing RestrictInfos is preferred.
 *
 * varRelid is either 0 or a rangetable index.
 *
 * When varRelid is not 0, only variables belonging to that relation are
 * considered in computing selectivity; other vars are treated as constants
 * of unknown values.  This is appropriate for estimating the selectivity of
 * a join clause that is being used as a restriction clause in a scan of a
 * nestloop join's inner relation --- varRelid should then be the ID of the
 * inner relation.
 *
 * When varRelid is 0, all variables are treated as variables.  This
 * is appropriate for ordinary join clauses and restriction clauses.
 *
 * jointype is the join type, if the clause is a join clause.  Pass JOIN_INNER
 * if the clause isn't a join clause.
 *
 * sjinfo is NULL for a non-join clause, otherwise it provides additional
 * context information about the join being performed.  There are some
 * special cases:
 *	1. For a special (not INNER) join, sjinfo is always a member of
 *	   root->join_info_list.
 *	2. For an INNER join, sjinfo is just a transient struct, and only the
 *	   relids and jointype fields in it can be trusted.
 * It is possible for jointype to be different from sjinfo->jointype.
 * This indicates we are considering a variant join: either with
 * the LHS and RHS switched, or with one input unique-ified.
 *
 * Note: when passing nonzero varRelid, it's normally appropriate to set
 * jointype == JOIN_INNER, sjinfo == NULL, even if the clause is really a
 * join clause; because we aren't treating it as a join clause.
 */
Selectivity
clause_selectivity(PlannerInfo *root,
				   Node *clause,
				   int varRelid,
				   JoinType jointype,
				   SpecialJoinInfo *sjinfo)
{
	return clause_selectivity_ext(root, clause, varRelid,
								  jointype, sjinfo, true);
}

/*
 * clause_selectivity_ext -
 *	  Extended version of clause_selectivity().  If "use_extended_stats" is
 *	  false, all extended statistics will be ignored, and only per-column
 *	  statistics will be used.
 */
Selectivity
clause_selectivity_ext(PlannerInfo *root,
					   Node *clause,
					   int varRelid,
					   JoinType jointype,
					   SpecialJoinInfo *sjinfo,
					   bool use_extended_stats)
{
	Selectivity s1 = 0.5;		/* default for any unhandled clause type */
	RestrictInfo *rinfo = NULL;
	bool		cacheable = false;

	if (clause == NULL)			/* can this still happen? */
		return s1;

	if (IsA(clause, RestrictInfo))
	{
		rinfo = (RestrictInfo *) clause;

		/*
		 * If the clause is marked pseudoconstant, then it will be used as a
		 * gating qual and should not affect selectivity estimates; hence
		 * return 1.0.  The only exception is that a constant FALSE may be
		 * taken as having selectivity 0.0, since it will surely mean no rows
		 * out of the plan.  This case is simple enough that we need not
		 * bother caching the result.
		 */
		if (rinfo->pseudoconstant)
		{
			if (!IsA(rinfo->clause, Const))
				return (Selectivity) 1.0;
		}

		/*
		 * If possible, cache the result of the selectivity calculation for
		 * the clause.  We can cache if varRelid is zero or the clause
		 * contains only vars of that relid --- otherwise varRelid will affect
		 * the result, so mustn't cache.  Outer join quals might be examined
		 * with either their join's actual jointype or JOIN_INNER, so we need
		 * two cache variables to remember both cases.  Note: we assume the
		 * result won't change if we are switching the input relations or
		 * considering a unique-ified case, so we only need one cache variable
		 * for all non-JOIN_INNER cases.
		 */
		if (varRelid == 0 ||
			rinfo->num_base_rels == 0 ||
			(rinfo->num_base_rels == 1 &&
			 bms_is_member(varRelid, rinfo->clause_relids)))
		{
			/* Cacheable --- do we already have the result? */
			if (jointype == JOIN_INNER)
			{
				if (rinfo->norm_selec >= 0)
					return rinfo->norm_selec;
			}
			else
			{
				if (rinfo->outer_selec >= 0)
					return rinfo->outer_selec;
			}
			cacheable = true;
		}

		/*
		 * Proceed with examination of contained clause.  If the clause is an
		 * OR-clause, we want to look at the variant with sub-RestrictInfos,
		 * so that per-subclause selectivities can be cached.
		 */
		if (rinfo->orclause)
			clause = (Node *) rinfo->orclause;
		else
			clause = (Node *) rinfo->clause;
	}

	if (IsA(clause, Var))
	{
		Var		   *var = (Var *) clause;

		/*
		 * We probably shouldn't ever see an uplevel Var here, but if we do,
		 * return the default selectivity...
		 */
		if (var->varlevelsup == 0 &&
			(varRelid == 0 || varRelid == (int) var->varno))
		{
			/* Use the restriction selectivity function for a bool Var */
			s1 = boolvarsel(root, (Node *) var, varRelid);
		}
	}
	else if (IsA(clause, Const))
	{
		/* bool constant is pretty easy... */
		Const	   *con = (Const *) clause;

		s1 = con->constisnull ? 0.0 :
			DatumGetBool(con->constvalue) ? 1.0 : 0.0;
	}
	else if (IsA(clause, Param))
	{
		/* see if we can replace the Param */
		Node	   *subst = estimate_expression_value(root, clause);

		if (IsA(subst, Const))
		{
			/* bool constant is pretty easy... */
			Const	   *con = (Const *) subst;

			s1 = con->constisnull ? 0.0 :
				DatumGetBool(con->constvalue) ? 1.0 : 0.0;
		}
		else
		{
			/* XXX any way to do better than default? */
		}
	}
	else if (is_notclause(clause))
	{
		/* inverse of the selectivity of the underlying clause */
		s1 = 1.0 - clause_selectivity_ext(root,
										  (Node *) get_notclausearg((Expr *) clause),
										  varRelid,
										  jointype,
										  sjinfo,
										  use_extended_stats);
	}
	else if (is_andclause(clause))
	{
		/* share code with clauselist_selectivity() */
		s1 = clauselist_selectivity_ext(root,
										((BoolExpr *) clause)->args,
										varRelid,
										jointype,
										sjinfo,
										use_extended_stats);
	}
	else if (is_orclause(clause))
	{
		/*
		 * Almost the same thing as clauselist_selectivity, but with the
		 * clauses connected by OR.
		 */
		s1 = clauselist_selectivity_or(root,
									   ((BoolExpr *) clause)->args,
									   varRelid,
									   jointype,
									   sjinfo,
									   use_extended_stats);
	}
	else if (is_opclause(clause) || IsA(clause, DistinctExpr))
	{
		OpExpr	   *opclause = (OpExpr *) clause;
		Oid			opno = opclause->opno;

		if (treat_as_join_clause(root, clause, rinfo, varRelid, sjinfo))
		{
			/* Estimate selectivity for a join clause. */
			s1 = join_selectivity(root, opno,
								  opclause->args,
								  opclause->inputcollid,
								  jointype,
								  sjinfo);
		}
		else
		{
			/* Estimate selectivity for a restriction clause. */
			s1 = restriction_selectivity(root, opno,
										 opclause->args,
										 opclause->inputcollid,
										 varRelid);
		}

		/*
		 * DistinctExpr has the same representation as OpExpr, but the
		 * contained operator is "=" not "<>", so we must negate the result.
		 * This estimation method doesn't give the right behavior for nulls,
		 * but it's better than doing nothing.
		 */
		if (IsA(clause, DistinctExpr))
			s1 = 1.0 - s1;
	}
	else if (is_funcclause(clause))
	{
		FuncExpr   *funcclause = (FuncExpr *) clause;

		/* Try to get an estimate from the support function, if any */
		s1 = function_selectivity(root,
								  funcclause->funcid,
								  funcclause->args,
								  funcclause->inputcollid,
								  treat_as_join_clause(root, clause, rinfo,
													   varRelid, sjinfo),
								  varRelid,
								  jointype,
								  sjinfo);
	}
	else if (IsA(clause, ScalarArrayOpExpr))
	{
		/* Use node specific selectivity calculation function */
		s1 = scalararraysel(root,
							(ScalarArrayOpExpr *) clause,
							treat_as_join_clause(root, clause, rinfo,
												 varRelid, sjinfo),
							varRelid,
							jointype,
							sjinfo);
	}
	else if (IsA(clause, RowCompareExpr))
	{
		/* Use node specific selectivity calculation function */
		s1 = rowcomparesel(root,
						   (RowCompareExpr *) clause,
						   varRelid,
						   jointype,
						   sjinfo);
	}
	else if (IsA(clause, NullTest))
	{
		/* Use node specific selectivity calculation function */
		s1 = nulltestsel(root,
						 ((NullTest *) clause)->nulltesttype,
						 (Node *) ((NullTest *) clause)->arg,
						 varRelid,
						 jointype,
						 sjinfo);
	}
	else if (IsA(clause, BooleanTest))
	{
		/* Use node specific selectivity calculation function */
		s1 = booltestsel(root,
						 ((BooleanTest *) clause)->booltesttype,
						 (Node *) ((BooleanTest *) clause)->arg,
						 varRelid,
						 jointype,
						 sjinfo);
	}
	else if (IsA(clause, CurrentOfExpr))
	{
		/* CURRENT OF selects at most one row of its table */
		CurrentOfExpr *cexpr = (CurrentOfExpr *) clause;
		RelOptInfo *crel = find_base_rel(root, cexpr->cvarno);

		if (crel->tuples > 0)
			s1 = 1.0 / crel->tuples;
	}
	else if (IsA(clause, RelabelType))
	{
		/* Not sure this case is needed, but it can't hurt */
		s1 = clause_selectivity_ext(root,
									(Node *) ((RelabelType *) clause)->arg,
									varRelid,
									jointype,
									sjinfo,
									use_extended_stats);
	}
	else if (IsA(clause, CoerceToDomain))
	{
		/* Not sure this case is needed, but it can't hurt */
		s1 = clause_selectivity_ext(root,
									(Node *) ((CoerceToDomain *) clause)->arg,
									varRelid,
									jointype,
									sjinfo,
									use_extended_stats);
	}
	else
	{
		/*
		 * For anything else, see if we can consider it as a boolean variable.
		 * This only works if it's an immutable expression in Vars of a single
		 * relation; but there's no point in us checking that here because
		 * boolvarsel() will do it internally, and return a suitable default
		 * selectivity if not.
		 */
		s1 = boolvarsel(root, clause, varRelid);
	}

	/* Cache the result if possible */
	if (cacheable)
	{
		if (jointype == JOIN_INNER)
			rinfo->norm_selec = s1;
		else
			rinfo->outer_selec = s1;
	}

#ifdef SELECTIVITY_DEBUG
	elog(DEBUG4, "clause_selectivity: s1 %f", s1);
#endif							/* SELECTIVITY_DEBUG */

	return s1;
}
