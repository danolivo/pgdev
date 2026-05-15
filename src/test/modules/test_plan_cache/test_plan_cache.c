/*-------------------------------------------------------------------------
 *
 * test_plan_cache.c
 *	  Test harness for the cursor_options vs plan_cache_mode precedence
 *	  rule in choose_custom_plan().
 *
 * Exposes a single SQL-callable function that prepares a parameterised
 * statement through SPI_prepare_cursor with caller-supplied cursor option
 * hints, asks the plan cache for a plan bound to the supplied parameter
 * value, and reports whether the chosen plan was generic.  The regression
 * script uses it to pin the precedence rule across all combinations of
 * plan_cache_mode and CURSOR_OPT_GENERIC_PLAN / CURSOR_OPT_CUSTOM_PLAN.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/modules/test_plan_cache/test_plan_cache.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "executor/spi_priv.h"
#include "fmgr.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "utils/builtins.h"
#include "utils/plancache.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(test_plan_cache_is_generic);

/*
 * test_plan_cache_is_generic(
 *     query text,
 *     request_generic bool,
 *     request_custom bool,
 *     param int) returns bool
 *
 * Prepares the given single-parameter int4 query through SPI_prepare_cursor.
 * The two boolean arguments select which CURSOR_OPT_*_PLAN bits, if any,
 * are set on the prepared statement; the function then asks the plan cache
 * for a plan bound to the supplied parameter value and returns whether the
 * chosen plan was generic.
 */
Datum
test_plan_cache_is_generic(PG_FUNCTION_ARGS)
{
	text	   *query_text = PG_GETARG_TEXT_PP(0);
	bool		request_generic = PG_GETARG_BOOL(1);
	bool		request_custom = PG_GETARG_BOOL(2);
	int32		param = PG_GETARG_INT32(3);
	char	   *query_string;
	int			cursor_options = 0;
	Oid			argtypes[1] = {INT4OID};
	SPIPlanPtr	spiplan;
	CachedPlanSource *plansource;
	ParamListInfo paramLI;
	CachedPlan *cplan;
	int64		num_custom_before;
	bool		is_generic;

	if (request_generic)
		cursor_options |= CURSOR_OPT_GENERIC_PLAN;
	if (request_custom)
		cursor_options |= CURSOR_OPT_CUSTOM_PLAN;

	query_string = text_to_cstring(query_text);

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	spiplan = SPI_prepare_cursor(query_string, 1, argtypes, cursor_options);
	if (spiplan == NULL)
		elog(ERROR, "SPI_prepare_cursor failed: %s",
			 SPI_result_code_string(SPI_result));

	/*
	 * SPI_prepare_cursor returns a plan whose plancache_list holds one
	 * CachedPlanSource per parsetree.  We require exactly one statement so
	 * that the precedence observation is unambiguous.
	 */
	if (list_length(spiplan->plancache_list) != 1)
		elog(ERROR, "test_plan_cache requires a single-statement query");
	plansource = (CachedPlanSource *) linitial(spiplan->plancache_list);

	/*
	 * Build a one-element ParamListInfo so that choose_custom_plan() reaches
	 * the cursor_options / plan_cache_mode arms; a NULL boundParams would
	 * short-circuit to the generic plan before we got there.
	 */
	paramLI = makeParamList(1);
	paramLI->params[0].value = Int32GetDatum(param);
	paramLI->params[0].isnull = false;
	paramLI->params[0].pflags = PARAM_FLAG_CONST;
	paramLI->params[0].ptype = INT4OID;

	/*
	 * The SPI plan is unsaved, so we acquire and release the cached plan
	 * with a NULL resource owner, matching SPI_plan_get_cached_plan()'s
	 * pattern for the unsaved case.
	 *
	 * GetCachedPlan() increments exactly one of num_generic_plans /
	 * num_custom_plans depending on the choice it made; observing which
	 * counter moved tells us the plan kind chosen for this execution.
	 */
	Assert(!spiplan->saved);
	num_custom_before = plansource->num_custom_plans;
	cplan = GetCachedPlan(plansource, paramLI, NULL, NULL);
	is_generic = (plansource->num_custom_plans == num_custom_before);
	ReleaseCachedPlan(cplan, NULL);

	SPI_finish();

	PG_RETURN_BOOL(is_generic);
}
