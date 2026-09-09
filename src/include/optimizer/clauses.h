/*-------------------------------------------------------------------------
 *
 * clauses.h
 *	  prototypes for clauses.c.
 *
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/optimizer/clauses.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLAUSES_H
#define CLAUSES_H

#include "nodes/pathnodes.h"

typedef struct
{
	int			numWindowFuncs; /* total number of WindowFuncs found */
	Index		maxWinRef;		/* windowFuncs[] is indexed 0 .. maxWinRef */
	List	  **windowFuncs;	/* lists of WindowFuncs for each winref */
} WindowFuncLists;

/*
 * Callback used by expression_has_grouping_conflict below.  Given a Var, the
 * callback returns the equality operator that the relevant grouping mechanism
 * (GROUP BY, DISTINCT, DISTINCT ON, window PARTITION BY, or set operation)
 * uses for the column the Var references, or InvalidOid if the Var does not
 * participate in that grouping.  Returning InvalidOid signals "not a grouping
 * column" to both the opfamily and collation checks.
 */
typedef Oid (*grouping_eqop_callback) (Var *var, void *context);

/*
 * Hook for plugins to take over simplification of an Aggref at plan time.
 *
 * eval_const_expressions_mutator() calls this, if set, for every Aggref it
 * meets, immediately after simplifying the Aggref's own arguments.  The hook
 * gets the current PlannerInfo and the Aggref itself; it must not modify the
 * Aggref in place (build a copy if it wants to return a changed one -- see
 * copyObject()).  Returning NULL leaves the Aggref alone; returning a
 * non-NULL Node substitutes that node in its place, exactly like any other
 * eval_const_expressions_mutator() rewrite.
 *
 * There is no catalog-driven dispatch here -- unlike a plain function's
 * SupportRequestSimplify, which core looks up per-function via
 * pg_proc.prosupport, this hook is a single global entry point that a
 * loaded module installs in its _PG_init() and that fires for every
 * Aggref in every query; the hook function itself has to recognise which
 * aggregate (if any) it wants to touch, from aggref->aggfnoid.
 */
typedef Node *(*agg_simplify_hook_type) (PlannerInfo *root, Aggref *aggref);
extern PGDLLIMPORT agg_simplify_hook_type agg_simplify_hook;

extern bool contain_agg_clause(Node *clause);

extern bool contain_window_function(Node *clause);
extern WindowFuncLists *find_window_functions(Node *clause, Index maxWinRef);

extern double expression_returns_set_rows(PlannerInfo *root, Node *clause);

extern bool contain_subplans(Node *clause);

extern char max_parallel_hazard(Query *parse);
extern bool is_parallel_safe(PlannerInfo *root, Node *node);
extern bool contain_nonstrict_functions(Node *clause);
extern bool contain_exec_param(Node *clause, List *param_ids);
extern bool contain_leaked_vars(Node *clause);

extern Relids find_nonnullable_rels(Node *clause);
extern List *find_nonnullable_vars(Node *clause);
extern List *find_forced_null_vars(Node *node);
extern Var *find_forced_null_var(Node *node);

extern bool is_pseudo_constant_clause(Node *clause);
extern bool is_pseudo_constant_clause_relids(Node *clause, Relids relids);

extern int	NumRelids(PlannerInfo *root, Node *clause);

extern void CommuteOpExpr(OpExpr *clause);

extern Query *inline_set_returning_function(PlannerInfo *root,
											RangeTblEntry *rte);

extern Bitmapset *pull_paramids(Expr *expr);

extern bool expression_has_grouping_conflict(Node *expr,
											 grouping_eqop_callback get_eqop,
											 void *context);

#endif							/* CLAUSES_H */
