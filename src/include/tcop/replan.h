/*-------------------------------------------------------------------------
 *
 * replan.h
 *	  ...
 *
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/tcop/replan.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef REPLAN_H
#define REPLAN_H

#include "executor/executor.h"
#include "nodes/pathnodes.h"
#include "utils/palloc.h"


typedef struct LearningDataKey
{
	uint64 key;
} LearningDataKey;

typedef struct LearningData
{
	LearningDataKey key;
	double			cardinality;
} LearningData;

typedef struct ReplanningStuff
{
	Query		   *querytree;
	char		   *query_string;
	int				cursorOptions;
	ParamListInfo	boundParams;
	Snapshot		snapshot;
	QueryDesc	   *queryDesc;
	void		   *data;
	MemoryContext	mctx;
} ReplanningStuff;


extern PGDLLIMPORT int QueryInadequateExecutionTime;

extern bool learn_partially_executed_state(PlannedStmt *stmt);
extern PlannedStmt *try_replan(PlannedStmt *src_stmt);
extern void check_replan_trigger(PlanState *node);
extern uint64 generate_signature(PlannerInfo *root, RelOptInfo *rel);
extern double replan_rows_estimate(PlannerInfo *root, RelOptInfo *rel);

#endif							/* REPLAN_H */
