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

extern ReplanContext replanCtx;

extern PGDLLIMPORT int QueryInadequateExecutionTime;


extern void trigger_replanning(PlanState *node);

#endif							/* REPLAN_H */
