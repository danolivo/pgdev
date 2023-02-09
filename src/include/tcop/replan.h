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


typedef struct ReplanningStuff
{
	Query		   *querytree;
	char		   *query_string;
	int				cursorOptions;
	ParamListInfo	boundParams;
	Snapshot		snapshot;
} ReplanningStuff;


extern PGDLLIMPORT int QueryInadequateExecutionTime;

extern PlannedStmt *try_replan(PlannedStmt *src_stmt);
extern void check_replan_trigger(PlanState *node);

#endif							/* REPLAN_H */
