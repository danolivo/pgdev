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

#include "tcop/replan.h"
#include "tcop/tcopprot.h"
#include "utils/snapmgr.h"

int QueryInadequateExecutionTime = 0;

PlannedStmt *
try_replan(PlannedStmt *src_stmt)
{
	PlannedStmt		   *stmt;
	ReplanningStuff	   *node = (ReplanningStuff *) src_stmt->replan;
	Query			   *query = copyObject(node->querytree);
	char			   *query_string = node->query_string;
	int					cursorOptions = node->cursorOptions;

	PushActiveSnapshot(node->snapshot);
	stmt = pg_plan_query(query, query_string, cursorOptions, node->boundParams);
	PopActiveSnapshot();
	return stmt;
}

void
check_replan_trigger(PlanState *node)
{
	Assert(node->instrument);
	Assert(!INSTR_TIME_IS_ZERO(node->instrument->counter));

	if (node->state->es_processed > 0)
		return;

	if (INSTR_TIME_GET_MILLISEC(node->instrument->counter) >= QueryInadequateExecutionTime)
		ereport(ERROR,
		(errcode(ERRCODE_INADEQUATE_QUERY_EXECUTION_TIME),
					 errmsg("ERRCODE_INADEQUATE_QUERY_EXECUTION_TIME")));
}

/* Use ERRCODE_INADEQUATE_QUERY_EXECUTION_TIME */
