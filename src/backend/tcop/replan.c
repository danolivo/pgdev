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

ReplanContext replanCtx;
int QueryInadequateExecutionTime = 0;

void
trigger_replanning(PlanState *node)
{
	Assert(node->instrument);
	Assert(!INSTR_TIME_IS_ZERO(node->instrument->counter));

	if (INSTR_TIME_GET_MILLISEC(node->instrument->counter) >= QueryInadequateExecutionTime)
		ereport(ERROR,
		(errcode(ERRCODE_INADEQUATE_QUERY_EXECUTION_TIME),
					 errmsg("ERRCODE_INADEQUATE_QUERY_EXECUTION_TIME")));
}

/* Use ERRCODE_INADEQUATE_QUERY_EXECUTION_TIME */
