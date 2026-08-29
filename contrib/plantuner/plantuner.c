/*
 * Copyright (c) 2009 Teodor Sigaev <teodor@sigaev.ru>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *		notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *		notice, this list of conditions and the following disclaimer in the
 *		documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the author nor the names of any co-contributors
 *		may be used to endorse or promote products derived from this software
 *		without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY CONTRIBUTORS ``AS IS'' AND ANY EXPRESS
 * OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL CONTRIBUTORS BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
 * GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER
 * IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
 * OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN
 * IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include <postgres.h>

#include <fmgr.h>
#include <miscadmin.h>
#include <access/heapam.h>
#include <access/xact.h>
#include <catalog/namespace.h>
#include <catalog/pg_class.h>
#if PG_VERSION_NUM >= 160000
#include <nodes/miscnodes.h>
#endif
#include <nodes/pg_list.h>
#include <optimizer/plancat.h>
#include <storage/bufmgr.h>
#include <utils/builtins.h>
#include <utils/guc.h>
#include <utils/lsyscache.h>
#include <utils/rel.h>
#if PG_VERSION_NUM >= 100000
#include <utils/regproc.h>
#include <utils/varlena.h>
#endif

PG_MODULE_MAGIC;

#if PG_VERSION_NUM >= 130000
#define heap_open(r, l)					table_open(r, l)
#define heap_close(r, l)				table_close(r, l)
#endif

static int	nDisabledIndexes = 0;
static Oid	*disabledIndexes = NULL;
static char *disableIndexesOutStr = "";

static int	nEnabledIndexes = 0;
static Oid	*enabledIndexes = NULL;
static char *enableIndexesOutStr = "";

static int	nOnlyIndexes = 0;
static Oid	*onlyIndexes = NULL;
static char *onlyIndexesOutStr = "";

static get_relation_info_hook_type	prevHook = NULL;
static bool	fix_empty_table = false;

static bool	plantuner_enable_inited = false;
static bool	plantuner_only_inited = false;
static bool	plantuner_disable_inited = false;

typedef enum IndexListKind {
	EnabledKind,
	DisabledKind,
	OnlyKind
} IndexListKind;

static const char *
indexesAssign(const char * newval, bool doit, GucSource source,
			  IndexListKind kind)
{
	char		*rawname;
	List		*namelist;
	ListCell	*l;
	Oid			*newOids = NULL;
	int			nOids = 0,
				i = 0;

	rawname = pstrdup(newval);

	if (!SplitIdentifierString(rawname, ',', &namelist))
		goto cleanup;

	/*
	 * follow work could be done only in normal processing because of
	 * accsess to system catalog
	 */
	if (MyProcNumber == INVALID_PROC_NUMBER || !IsUnderPostmaster ||
		!IsTransactionState())
	{
		/* reset init state */
		switch(kind)
		{
			case EnabledKind:
				plantuner_enable_inited = false;
				break;
			case DisabledKind:
				plantuner_disable_inited = false;
				break;
			case OnlyKind:
				plantuner_only_inited = false;
				break;
			default:
				elog(ERROR, "wrong kind");
		}

		return newval;
	}

	if (doit)
	{
		nOids = list_length(namelist);
		newOids = malloc(sizeof(Oid) * (nOids+1));
		if (!newOids)
			elog(ERROR,"could not allocate %d bytes",
				 (int)(sizeof(Oid) * (nOids+1)));
	}

	switch(kind)
	{
		case EnabledKind:
			plantuner_enable_inited = true;
			break;
		case DisabledKind:
			plantuner_disable_inited = true;
			break;
		case OnlyKind:
			plantuner_only_inited = true;
			break;
		default:
			elog(ERROR, "wrong kind");
	}

	foreach(l, namelist)
	{
		char	*curname = (char *) lfirst(l);
#if PG_VERSION_NUM >= 90200
		List	*cur_namelist;
		Oid		indexOid;

#if PG_VERSION_NUM >= 160000
		ErrorSaveContext escontext = {T_ErrorSaveContext};

		cur_namelist = stringToQualifiedNameList(curname, (Node *) &escontext);

		/* bad name list syntax */
		if (cur_namelist == NIL)
			continue;
#else
		cur_namelist = stringToQualifiedNameList(curname);
#endif

		indexOid = RangeVarGetRelid(makeRangeVarFromNameList(cur_namelist),
									NoLock, true);
#else
		Oid		indexOid = RangeVarGetRelid(
				makeRangeVarFromNameList(stringToQualifiedNameList(curname)),
											true);
#endif

		if (indexOid == InvalidOid)
		{
#if PG_VERSION_NUM >= 90100
			if (doit == false)
#endif
				elog(WARNING,"'%s' does not exist", curname);
			continue;
		}
		else if ( get_rel_relkind(indexOid) != RELKIND_INDEX )
		{
#if PG_VERSION_NUM >= 90100
			if (doit == false)
#endif
				elog(WARNING,"'%s' is not an index", curname);
			continue;
		}
		else if (doit)
		{
			newOids[i++] = indexOid;
		}
	}

	if (doit)
	{
		switch(kind)
		{
			case EnabledKind:
				nEnabledIndexes = i;
				if (enabledIndexes)
					free(enabledIndexes);
				enabledIndexes = newOids;
				break;
			case DisabledKind:
				nDisabledIndexes = i;
				if (disabledIndexes)
					free(disabledIndexes);
				disabledIndexes = newOids;
				break;
			case OnlyKind:
				nOnlyIndexes = i;
				if (onlyIndexes)
					free(onlyIndexes);
				onlyIndexes = newOids;
				break;
			default:
				elog(ERROR, "wrong kind");
		}
	}

	pfree(rawname);
	list_free(namelist);

	return newval;

cleanup:
	if (newOids)
		free(newOids);
	pfree(rawname);
	list_free(namelist);
	return NULL;
}

static const char *
assignDisabledIndexes(const char * newval, bool doit, GucSource source)
{
	return indexesAssign(newval, doit, source, DisabledKind);
}

static const char *
assignEnabledIndexes(const char * newval, bool doit, GucSource source)
{
	return indexesAssign(newval, doit, source, EnabledKind);
}

static const char *
assignOnlyIndexes(const char * newval, bool doit, GucSource source)
{
	return indexesAssign(newval, doit, source, OnlyKind);
}

static void
lateInit()
{
	if (!plantuner_only_inited)
		indexesAssign(onlyIndexesOutStr, true, PGC_S_USER, OnlyKind);
	if (!plantuner_enable_inited)
		indexesAssign(enableIndexesOutStr, true, PGC_S_USER, EnabledKind);
	if (!plantuner_disable_inited)
		indexesAssign(disableIndexesOutStr, true, PGC_S_USER, DisabledKind);
}

#if PG_VERSION_NUM >= 90100

static bool
checkOnlyIndexes(char **newval, void **extra, GucSource source)
{
	char *val;

	val = (char*)indexesAssign(*newval, false, source, OnlyKind);

	if (val)
	{
		*newval = val;
		return true;
	}

	return false;
}

static bool
checkDisabledIndexes(char **newval, void **extra, GucSource source)
{
	char *val;

	val = (char*)indexesAssign(*newval, false, source, DisabledKind);

	if (val)
	{
		*newval = val;
		return true;
	}

	return false;
}

static bool
checkEnabledIndexes(char **newval, void **extra, GucSource source)
{
	char *val;

	val = (char*)indexesAssign(*newval, false, source, EnabledKind);

	if (val)
	{
		*newval = val;
		return true;
	}

	return false;
}

static void
assignDisabledIndexesNew(const char *newval, void *extra)
{
	assignDisabledIndexes(newval, true, PGC_S_USER /* doesn't matter */);
}

static void
assignEnabledIndexesNew(const char *newval, void *extra)
{
	assignEnabledIndexes(newval, true, PGC_S_USER /* doesn't matter */);
}

static void
assignOnlyIndexesNew(const char *newval, void *extra)
{
	assignOnlyIndexes(newval, true, PGC_S_USER /* doesn't matter */);
}

#endif

static void
indexFilter(PlannerInfo *root, Oid relationObjectId, bool inhparent,
			RelOptInfo *rel)
{
	int i;

	lateInit();

	if (nOnlyIndexes > 0)
	{
		ListCell	*l;

restart1:
		foreach(l, rel->indexlist)
		{
			IndexOptInfo	*info = (IndexOptInfo*)lfirst(l);
			bool			remove = true;

			for(i=0; remove && i<nOnlyIndexes; i++)
				if (onlyIndexes[i] == info->indexoid)
					remove = false;

			if (remove)
			{
				rel->indexlist = list_delete_ptr(rel->indexlist, info);
				goto restart1;
			}
		}

		return;
	}

	for(i=0; i<nDisabledIndexes; i++)
	{
		ListCell   *l;

		foreach(l, rel->indexlist)
		{
			IndexOptInfo	*info = (IndexOptInfo*)lfirst(l);

			if (disabledIndexes[i] == info->indexoid)
			{
				int j;

				for(j=0; j<nEnabledIndexes; j++)
					if (enabledIndexes[j] == info->indexoid)
						break;

				if (j >= nEnabledIndexes)
					rel->indexlist = list_delete_ptr(rel->indexlist, info);

				break;
			}
		}
	}
}

static void
execPlantuner(PlannerInfo *root, Oid relationObjectId, bool inhparent,
			  RelOptInfo *rel)
{
	Relation	relation;

	relation = heap_open(relationObjectId, NoLock);
	if (relation->rd_rel->relkind == RELKIND_RELATION)
	{
		if (fix_empty_table && RelationGetNumberOfBlocks(relation) == 0)
		{
			/*
			 * estimate_rel_size() could be too pessimistic for particular
			 * workload
			 */
			rel->pages = 1.0;
			rel->tuples = 0.0;
		}

		indexFilter(root, relationObjectId, inhparent, rel);
	}
	heap_close(relation, NoLock);

	/*
	 * Call next hook if it exists
	 */
	if (prevHook)
		prevHook(root, relationObjectId, inhparent, rel);
}

static const char*
IndexFilterShow(Oid* indexes, int nIndexes)
{
	char	*val, *ptr;
	int		i,
			len;

	lateInit();

	len = 1 /* \0 */ + nIndexes * (2 * NAMEDATALEN + 2 /* ', ' */ + 1 /* . */);
	ptr = val = palloc(len);

	*ptr =(char)'\0';
	for(i=0; i<nIndexes; i++)
	{
		char	*relname = get_rel_name(indexes[i]);
		Oid		nspOid = get_rel_namespace(indexes[i]);
		char	*nspname = get_namespace_name(nspOid);

		if ( relname == NULL || nspOid == InvalidOid || nspname == NULL )
			continue;

		ptr += snprintf(ptr, len - (ptr - val), "%s%s.%s",
												(i==0) ? "" : ", ",
												nspname,
												relname);
	}

	return val;
}

static const char*
disabledIndexFilterShow(void)
{
	return IndexFilterShow(disabledIndexes, nDisabledIndexes);
}

static const char*
enabledIndexFilterShow(void)
{
	return IndexFilterShow(enabledIndexes, nEnabledIndexes);
}

static const char*
onlyIndexFilterShow(void)
{
	return IndexFilterShow(onlyIndexes, nOnlyIndexes);
}

void _PG_init(void);
void
_PG_init(void)
{
	DefineCustomStringVariable(
		"plantuner.forbid_index",
		"List of forbidden indexes (deprecated)",
		"Listed indexes will not be used in queries (deprecated, use plantuner.disable_index)",
		&disableIndexesOutStr,
		"",
		PGC_USERSET,
		0,
#if PG_VERSION_NUM >= 90100
		checkDisabledIndexes,
		assignDisabledIndexesNew,
#else
		assignDisabledIndexes,
#endif
		disabledIndexFilterShow
	);

	DefineCustomStringVariable(
		"plantuner.disable_index",
		"List of disabled indexes",
		"Listed indexes will not be used in queries",
		&disableIndexesOutStr,
		"",
		PGC_USERSET,
		0,
#if PG_VERSION_NUM >= 90100
		checkDisabledIndexes,
		assignDisabledIndexesNew,
#else
		assignDisabledIndexes,
#endif
		disabledIndexFilterShow
	);

	DefineCustomStringVariable(
		"plantuner.enable_index",
		"List of enabled indexes (overload plantuner.disable_index)",
		"Listed indexes which could be used in queries even they are listed in plantuner.disable_index",
		&enableIndexesOutStr,
		"",
		PGC_USERSET,
		0,
#if PG_VERSION_NUM >= 90100
		checkEnabledIndexes,
		assignEnabledIndexesNew,
#else
		assignEnabledIndexes,
#endif
		enabledIndexFilterShow
	);

	DefineCustomStringVariable(
		"plantuner.only_index",
		"List of explicitly enabled indexes (overload plantuner.disable_index and plantuner.enable_index)",
		"Only indexes in this list are allowed",
		&onlyIndexesOutStr,
		"",
		PGC_USERSET,
		0,
#if PG_VERSION_NUM >= 90100
		checkOnlyIndexes,
		assignOnlyIndexesNew,
#else
		assignOnlyIndexes,
#endif
		onlyIndexFilterShow
	);

	DefineCustomBoolVariable(
		"plantuner.fix_empty_table",
		"Sets to zero estimations for empty tables",
		"Sets to zero estimations for empty or newly created tables",
		&fix_empty_table,
#if PG_VERSION_NUM >= 80400
		fix_empty_table,
#endif
		PGC_USERSET,
#if PG_VERSION_NUM >= 80400
		GUC_NOT_IN_SAMPLE,
#if PG_VERSION_NUM >= 90100
		NULL,
#endif
#endif
		NULL,
		NULL
	);

	if (get_relation_info_hook != execPlantuner )
	{
		prevHook = get_relation_info_hook;
		get_relation_info_hook = execPlantuner;
	}
}
