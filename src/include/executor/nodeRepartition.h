/*-------------------------------------------------------------------------
 *
 * nodeRepartition.h
 *
 * src/include/executor/nodeRepartition.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODEREPARTITION_H
#define NODEREPARTITION_H

#include "access/parallel.h"
#include "nodes/execnodes.h"

extern RepartitionState *ExecInitRepartition(Repartition *node, EState *estate,
											 int eflags);
extern void ExecEndRepartition(RepartitionState *node);
extern void ExecReScanRepartition(RepartitionState *node);
extern void ExecShutdownRepartition(RepartitionState *node);

extern void ExecRepartitionEstimate(RepartitionState *node,
									ParallelContext *pcxt);
extern void ExecRepartitionInitializeDSM(RepartitionState *node,
										 ParallelContext *pcxt);
extern void ExecRepartitionReInitializeDSM(RepartitionState *node,
										   ParallelContext *pcxt);
extern void ExecRepartitionInitializeWorker(RepartitionState *node,
											ParallelWorkerContext *pwcxt);
extern void ExecRepartitionRetrieveInstrumentation(RepartitionState *node);
extern void ExecRepartitionPostLaunch(RepartitionState *node,
									  ParallelContext *pcxt,
									  bool leader_participates);

#endif							/* NODEREPARTITION_H */
