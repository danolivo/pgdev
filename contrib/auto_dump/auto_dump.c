#include "c.h"
#include "postgres.h"

#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <math.h>

#include "access/heapam.h"
#include "access/skey.h"
#include "access/table.h"
#include "access/tableam.h"
#include "catalog/namespace.h"
#include "catalog/pg_class_d.h"
#include "catalog/pg_proc_d.h"
#include "commands/copy.h"
#include "commands/explain.h"
#include "common/file_perm.h"
#include "common/keywords.h"
#include "executor/instrument.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "storage/ipc.h"
#include "storage/lockdefs.h"
#include "storage/proc.h"
#include "storage/procnumber.h"
#include "tcop/pquery.h"
#include "utils/backend_status.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/fmgrprotos.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/regproc.h"
#include "utils/snapmgr.h"
#include "commands/explain_state.h"
#include "commands/explain_format.h"
#include "utils/syscache.h"
#include "utils/acl.h"
#include "catalog/pg_authid_d.h"


enum{
	MODE_OFF=0,
	MODE_IMMEDIATE=1,
	MODE_CURRENT=2,
	MODE_NEXT=3,
};

enum{
	DATA_FORMAT_INSERT=0,
	DATA_FORMAT_COPY_FILE=1,
	DATA_FORMAT_COPY_STDIN=2,
};

static const struct config_enum_entry data_format_options[] = {
	{"insert", DATA_FORMAT_INSERT, false},
	{"copy-file", DATA_FORMAT_COPY_FILE, false},
	{"copy-stdin", DATA_FORMAT_COPY_STDIN, false},
	{NULL, 0, false}
};

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(auto_dump_immediate);
Datum auto_dump_immediate(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(auto_dump_current);
Datum auto_dump_current(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(auto_dump_next);
Datum auto_dump_next(PG_FUNCTION_ARGS);

static bool  dump_enable                         = false;
static bool  dump_all_temp_tables                = false;
static bool  dump_temporary_tables               = true;
static bool  dump_persistent_tables              = false;
static int   data_format                         = DATA_FORMAT_COPY_STDIN;
static bool  dump_data                           = true;
static bool  dump_indexes                        = true;
static bool  dump_query                          = true;
static bool  dump_create                         = true;
static bool  dump_plan                           = true;
static bool  dump_readme                         = true;
static bool  dump_on_cancel                      = false;
static bool  dump_on_bad_plan                    = false;
static char* dump_on_query_string                = "";
static int   dump_on_duration                    = -1;
static int   dump_plan_count_threshold           = 0;
static int   dump_plan_percent_threshold         = 0;
static char* output_directory                    = "";
static char* query_output_directory;
static int   nesting_level;
static bool  query_dumped;
static bool  plan_dumped;
static bool  plan_analysis_dumped;
static char volatile* backend_dump_mode;

static ExecutorStart_hook_type     prev_ExecutorStart;
static ProcessInterrupts_hook_type prev_ProcessInterrupts;
static ExecutorEnd_hook_type       prev_ExecutorEnd;
static ExecutorRun_hook_type       prev_ExecutorRun;
static ExecutorFinish_hook_type    prev_ExecutorFinish;
static shmem_startup_hook_type     prev_shmem_startup_hook;
static shmem_request_hook_type     prev_shmem_request_hook;

typedef struct FieldInfo{

	HeapTuple         tuple;
	Form_pg_attribute form;
	AttrNumber        attnum;
	FmgrInfo          outfunc;
	bool              first;
	char             *attname;
} FieldInfo;

#define IS_MODE_ACTIVE() (MyProcNumber != INVALID_PROC_NUMBER && backend_dump_mode && ((backend_dump_mode[MyProcNumber] == MODE_IMMEDIATE) || (backend_dump_mode[MyProcNumber] == MODE_CURRENT)))



/*
 * Checks, if plan is "bad" based on difference between expected number of rows
 * and actual number of rows. Plan in considered bad if both relative and absolute
 * threshold are reached (or disabled).
 */
static bool
IsBadPlan(PlanState *planstate)
{
	double expect_rows;
	double actual_rows;
	ListCell* lc;
	int c;

	if (!planstate)
		return false;

	if (planstate->instrument)
	{
		InstrEndLoop(planstate->instrument);

		expect_rows = planstate->plan->plan_rows;
		actual_rows = planstate->instrument->ntuples;

		if (planstate->instrument->nloops > 0)
			actual_rows /= planstate->instrument->nloops;

		if (
		    (fabs(actual_rows - expect_rows) > dump_plan_count_threshold) &&
		    (expect_rows <= 0 || fabs(actual_rows-expect_rows)/expect_rows*100 > dump_plan_percent_threshold))
		{
			ereport(DEBUG5,
					(errmsg("auto_dump hit bad plan threshold: expected=%.0f actual=%.0f", expect_rows, actual_rows),
					errhidestmt(true),
					errhidecontext(true)));
			return true;
		}
	}

	if (IsBadPlan(outerPlanState(planstate)))
		return true;

	if (IsBadPlan(innerPlanState(planstate)))
		return true;
	
	foreach(lc, planstate->initPlan)
	{
		if(IsBadPlan(((SubPlanState*)lfirst(lc))->planstate))
			return true;
	}

	foreach(lc, planstate->subPlan)
	{
		if(IsBadPlan(((SubPlanState*)lfirst(lc))->planstate))
			return true;
	}

	switch (nodeTag(planstate->plan))
	{
		case T_Append:
			for(c=0; c < ((AppendState *) planstate)->as_nplans; c++)
			{
				if (IsBadPlan(((AppendState *) planstate)->appendplans[c]))
					return true;
			}
			break;

		case T_MergeAppend:
			for(c=0; c < ((MergeAppendState *) planstate)->ms_nplans; c++)
			{
				if (IsBadPlan(((MergeAppendState *) planstate)->mergeplans[c]))
					return true;
			}
			break;

		case T_BitmapAnd:
			for(c=0; c < ((BitmapAndState *) planstate)->nplans; c++)
			{
				if (IsBadPlan(((BitmapAndState *) planstate)->bitmapplans[c]))
					return true;
			}
			break;

		case T_BitmapOr:
			for(c=0; c < ((BitmapOrState *) planstate)->nplans ; c++)
			{
				if (IsBadPlan(((BitmapOrState *) planstate)->bitmapplans[c]))
					return true;
			}
			break;

		case T_SubqueryScan:
			if(IsBadPlan(((SubqueryScanState *) planstate)->subplan))
				return true;
			break;

		case T_CustomScan:
			foreach(lc, ((CustomScanState *) planstate)->custom_ps)
			{
				if(IsBadPlan((PlanState*)lfirst(lc)))
					return true;
			}
			break;

		default:
			break;
	}

	return false;
}


static bool
IsQueryMatch(const char* query, const char* pattern)
{
	while (*query)
	{
		char const* q = query;
		char const* p = pattern;
		bool match = true;

		while (*p)
		{
			if (*q == '\r' || *q == '\n')
			{
				if (!isspace(*p))
				{
					match = false;
					break;
				}

				while (isspace(*p))
					p++;

				while (isspace(*q))
					q++;
			}
			else if (*q == *p)
			{
				q++;
				p++;
			}
			else
			{
				match = false;
				break;
			}
		}

		if (match)
			return true;

		query++;
	}

	return false;
}


static bool
PrepareDump()
{
	time_t     t;
	struct tm tm;
	static int counter = 0;

	/* Do nothing if not output directory specified */
	if (!*output_directory)
		return false;

	t  = time(NULL);
	tm = *localtime(&t);

	query_output_directory = psprintf("%s/%d-%04d_%02d_%02d_%02d_%02d_%02d_%02d/", make_absolute_path(output_directory),
						 MyProcPid, tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour,
						 tm.tm_min, tm.tm_sec, (counter++) % 100 );
	if (pg_mkdir_p(query_output_directory, PG_DIR_MODE_OWNER) != 0)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			errmsg("could not create directory \"%s\"", query_output_directory)));

	return true;
}

static void
SavePlan(QueryDesc* queryDesc, char const* name, bool analyze)
{
	FILE*         f;
	mode_t        oumask;
	MemoryContext oldcxt;
	struct ExplainState* es;

	oldcxt = MemoryContextSwitchTo(queryDesc->estate->es_query_cxt);

	es = NewExplainState();
	es->analyze = analyze;
	es->verbose = analyze;
	es->buffers = analyze;
	es->wal = analyze;
	es->timing = analyze;
	es->summary = analyze;
	es->format = EXPLAIN_FORMAT_TEXT;
	es->settings = analyze;

	ExplainBeginOutput(es);
	ExplainQueryText(es, queryDesc);
	ExplainQueryParameters(es, queryDesc->params, -1);
	ExplainPrintPlan(es, queryDesc);
	if (es->analyze)
		ExplainPrintTriggers(es, queryDesc);
	if (es->costs)
		ExplainPrintJITSummary(es, queryDesc);
	ExplainEndOutput(es);

	/* Remove last line break */
	if (es->str->len > 0 && es->str->data[es->str->len - 1] == '\n')
		es->str->data[--es->str->len] = '\0';

	oumask = umask((mode_t) ((~(S_IRUSR | S_IWUSR)) & (S_IRWXU | S_IRWXG | S_IRWXO)));
	f = fopen(psprintf("%s/%s.txt", query_output_directory, name), "wb");
	umask(oumask);
	fputs(es->str->data, f);
	fclose(f);

	MemoryContextSwitchTo(oldcxt);
}


static void
FlushStringInfoToFile(StringInfo buf, FILE* f)
{
	if (buf->len)
		fwrite(buf->data, buf->len, 1, f);
	resetStringInfo(buf);
}

static FILE*
CreateDumpFile(char const* name)
{
	char* filePath = psprintf("%s/%s", query_output_directory, name);
	FILE* f = fopen(filePath, "wb");
	if (!f)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("unable to create file \"%s\"", filePath)));
	pfree(filePath);
	return f;
}

static char*
QuotedIdentifier(char const* s)
{
	bool need_quotes = false;
	StringInfoData buf;
	size_t remaining;
	const char *cp;
	int encoding = PG_UTF8;

	for (cp=s; *cp; cp++)
	{
		if (!((*cp >= 'a' && *cp <= 'z') || (*cp == '_') || (*cp >= '0' && *cp <= '9' && cp != s)))
		{
			need_quotes = true;
			break;
		}
	}

	if (!need_quotes)
	{
		int kwnum = ScanKeywordLookup(s, &ScanKeywords);
		if (kwnum >= 0 && ScanKeywordCategories[kwnum] != UNRESERVED_KEYWORD)
			need_quotes = true;
	}

	if (!need_quotes)
		return pstrdup(s);

		
	initStringInfo(&buf);
	appendStringInfoChar(&buf, '"');

	remaining = strlen(s);
	cp =s;
	while (remaining > 0)
	{
		int charlen;

		if (!IS_HIGHBIT_SET(*cp))
		{
			if (*cp == '"')
				appendStringInfoChar(&buf, *cp);
			appendStringInfoChar(&buf, *cp);
			cp++;
			remaining--;
			continue;
		}

		charlen = pg_encoding_mblen(encoding, cp);

		if (remaining >= charlen &&
			pg_encoding_verifymbchar(encoding, cp, charlen) != -1)
		{
			for (int i = 0; i < charlen; i++)
			{
				appendStringInfoChar(&buf, *cp);
				remaining--;
				cp++;
			}
		}
		else
		{
			enlargeStringInfo(&buf, 2);
			pg_encoding_set_invalid(encoding, buf.data + buf.len);
			buf.len += 2;
			buf.data[buf.len] = '\0';
			remaining--;
			cp++;
		}
	}

	appendStringInfoChar(&buf, '"');
	return buf.data;
}

static FILE* CopyDataDestCallback_file;

static
void CopyDataDestCallback(void *data, int len)
{
	fwrite(data, len, 1, CopyDataDestCallback_file);
	fputs("\n",CopyDataDestCallback_file);
}


static void
SaveTables(QueryDesc* queryDesc)
{
	mode_t     oumask;
	FILE*      fCreatePers = NULL;
	FILE*      fCreateTemp = NULL;
	FILE*      fDataPers   = NULL;
	FILE*      fDataTemp   = NULL;
	ListCell*  lc;
	List*      tableOids = NULL;
	StringInfoData buf;

	/* Populate list of all temporary tables OIDs */
	if (dump_all_temp_tables) {
		ScanKeyData   key[1];
		Relation      pgclass;
		TableScanDesc scan;
		HeapTuple     tuple;
		
		ScanKeyInit(&key[0],
					Anum_pg_proc_pronamespace,
					BTEqualStrategyNumber, 
					F_OIDEQ,
					ObjectIdGetDatum(LookupCreationNamespace("pg_temp")));

		pgclass = table_open(RelationRelationId, AccessShareLock);
		scan = table_beginscan_catalog(pgclass, 1, key);

		while ((tuple = heap_getnext(scan, ForwardScanDirection)))
		{
			Form_pg_class classForm = (Form_pg_class) GETSTRUCT(tuple);
			if (classForm->relkind == RELKIND_RELATION && classForm->relpersistence == RELPERSISTENCE_TEMP)
				tableOids = list_append_unique_oid(tableOids, classForm->oid);
		}

		table_endscan(scan);
		table_close(pgclass, AccessShareLock);
	}

	/* Populate list of query's tables OIDs */
	foreach(lc, queryDesc->plannedstmt->relationOids) {
		tableOids = list_append_unique_oid(tableOids, lfirst_oid(lc));
	}
	
	oumask = umask((mode_t) ((~(S_IRUSR | S_IWUSR | S_IWUSR)) & (S_IRWXU | S_IRWXG | S_IRWXO)));
	
	if (dump_create || dump_indexes)
	{
		if (dump_persistent_tables)
			fCreatePers = CreateDumpFile("create_persistent.sql");
	
		if (dump_temporary_tables || dump_all_temp_tables)
			fCreateTemp = CreateDumpFile("create_temporary.sql");
	}

	if (dump_data)
	{
		if (dump_persistent_tables)
			fDataPers = CreateDumpFile("insert_persistent.sql");
		
		if (dump_temporary_tables || dump_all_temp_tables)
			fDataTemp = CreateDumpFile("insert_temporary.sql");

		if (data_format == DATA_FORMAT_COPY_FILE)
		{
			char* dirname = palloc(strlen(query_output_directory)*2+1);
			char* pos = dirname;
			for (char* c=query_output_directory; *c; c++)
			{
				if (*c == '\'')
					*(pos++) = '\\';
				*(pos++) = *c;
			}
			*(pos++) = '\0';

			if (fDataPers)
				fprintf(fDataPers, "\\set dir '%s'\n", dirname);

			if (fDataTemp)
				fprintf(fDataTemp, "\\set dir '%s'\n", dirname);

			pfree(dirname);
		}
	}

	umask(oumask);

	initStringInfo(&buf);

	foreach(lc, tableOids) {
		HeapTuple     classTuple;
		Form_pg_class classForm;
		Oid relid = ObjectIdGetDatum(lfirst_oid(lc));

		classTuple = SearchSysCache1(RELOID, relid);
		classForm = (Form_pg_class) GETSTRUCT(classTuple);

		if (classForm->relkind == RELKIND_RELATION &&
			((classForm->relpersistence == RELPERSISTENCE_PERMANENT && dump_persistent_tables) ||
			(classForm->relpersistence == RELPERSISTENCE_TEMP      && (dump_temporary_tables || dump_all_temp_tables)))
		){

			FieldInfo  *fields = (FieldInfo*)palloc(sizeof(FieldInfo) * classForm->relnatts);
			int         numFields = 0;
			FieldInfo  *field;
			AttrNumber  attno;
			char       *relname;

			relname = QuotedIdentifier(classForm->relname.data);

			for (attno = 1; attno <= classForm->relnatts; attno++)
			{
				bool isvarlena;
				Oid outfuncid;

				field = &fields[numFields];

				field->tuple = SearchSysCache2(ATTNUM,
										ObjectIdGetDatum(classForm->oid),
										Int16GetDatum(attno));

				field->form = (Form_pg_attribute) GETSTRUCT(field->tuple);

				if (field->form->attisdropped)
				{
					ReleaseSysCache(field->tuple);
					continue;
				}

				field->attnum = attno;
				field->first = (numFields==0);
				field->attname = QuotedIdentifier(field->form->attname.data);

				getTypeOutputInfo(field->form->atttypid, &outfuncid, &isvarlena);
				fmgr_info(outfuncid, &field->outfunc);

				numFields++;
			}

			if (dump_create)
			{
				/* Write table create statement */
				appendStringInfoString(&buf, "CREATE ");
				if (classForm->relpersistence == RELPERSISTENCE_TEMP)
					appendStringInfoString(&buf, "TEMPORARY ");
				appendStringInfoString(&buf, "TABLE ");
				appendStringInfoString(&buf, relname);
				appendStringInfoString(&buf, " (");
				for (field=&fields[0]; field < &fields[numFields]; field++)
				{
					if (!field->first)
						appendStringInfoString(&buf, ",");
					appendStringInfoString(&buf, "\n    ");
					appendStringInfoString(&buf, field->attname);
					appendStringInfoString(&buf, " ");
					appendStringInfoString(&buf, format_type_extended(field->form->atttypid, field->form->atttypmod, FORMAT_TYPE_TYPEMOD_GIVEN | ((field->form->atttypid >= FirstGenbkiObjectId) ? FORMAT_TYPE_FORCE_QUALIFY : 0)));
				}
				appendStringInfoString(&buf, "\n);\n\n");
			}

			/* Write indexes create statement */
			if (dump_indexes)
			{
				ListCell* lcIndex;
				Relation  rel = table_open(classForm->oid, NoLock);
				
				foreach(lcIndex, RelationGetIndexList(rel))
				{
					Oid indexOid = lfirst_oid(lcIndex);
					appendStringInfoString(&buf, text_to_cstring(DatumGetTextP(DirectFunctionCall1(pg_get_indexdef, indexOid))));
					appendStringInfoString(&buf, ";\n\n");
				}

				table_close(rel, NoLock);
			}

			if (classForm->relpersistence == RELPERSISTENCE_TEMP)
			{
				if (fCreateTemp)
					FlushStringInfoToFile(&buf, fCreateTemp);
			}
			else
			{
				if (fCreatePers)
					FlushStringInfoToFile(&buf, fCreatePers);
			}


			/* Write tables data */
			if (dump_data)
			{
				FILE* dest = (classForm->relpersistence == RELPERSISTENCE_TEMP) ? fDataTemp : fDataPers;
				if (dest)
				{
					if (data_format == DATA_FORMAT_INSERT)
					{
						Relation table;
						TableScanDesc scan;
						HeapTuple tuple;
						bool first;

						table = table_open(classForm->oid, AccessShareLock);
						scan = table_beginscan(table, GetActiveSnapshot(), 0, NULL);
						first =  true;

						while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
						{
							if(first)
							{
								first = false;

								appendStringInfoString(&buf, "INSERT INTO ");
								appendStringInfoString(&buf, relname);
								appendStringInfoString(&buf, " (");
				
								for (field=&fields[0]; field < &fields[numFields]; field++)
								{
									if(!field->first)
										appendStringInfoString(&buf, ", ");
									
									appendStringInfoString(&buf, field->attname);
								}
				
								appendStringInfoString(&buf, ") VALUES\n    (");
							}
							else
								appendStringInfoString(&buf, ",\n    (");

							for (field=&fields[0]; field < &fields[numFields]; field++)
							{
								const char *s;
								char *quoted;
								bool isnull;
								Datum datum;

								if (!field->first)
									appendStringInfoString(&buf, ",");

								datum = heap_getattr(tuple, field->attnum, table->rd_att, &isnull);

								if (isnull)
								{
									appendStringInfoString(&buf, "NULL");
									continue;
								}

								s = OutputFunctionCall(&field->outfunc, datum);

								switch (field->form->atttypid)
								{
									case INT2OID:
									case INT4OID:
									case INT8OID:
									case OIDOID:
									case FLOAT4OID:
									case FLOAT8OID:
									case NUMERICOID:
										{
											if (strspn(s, "0123456789 +-eE.") == strlen(s))
												appendStringInfoString(&buf, s);
											else
											{
												appendStringInfoString(&buf, "'");
												appendStringInfoString(&buf, s);
												appendStringInfoString(&buf, "'");
											}
										}
										break;
				
									case BITOID:
									case VARBITOID:
										appendStringInfoString(&buf, "B'");
										appendStringInfoString(&buf, s);
										appendStringInfoString(&buf, "'");
										break;
				
									case BOOLOID:
										if (strcmp(s, "t") == 0)
											appendStringInfoString(&buf, "true");
										else
											appendStringInfoString(&buf, "false");
										break;
				
									default:
										quoted = quote_literal_cstr(s);
										appendStringInfoString(&buf, quoted);
										pfree(quoted);
										break;
								}
							}
							appendStringInfoString(&buf, ")");
							FlushStringInfoToFile(&buf, dest);
						}

						table_endscan(scan);
						table_close(table, AccessShareLock);

						appendStringInfoString(&buf, ";\n\n");

						FlushStringInfoToFile(&buf, dest);
					}
					else if (data_format == DATA_FORMAT_COPY_FILE)
					{
						char*             filename;
						char*             basename;
						ParseState*       pstate;
						CopyStmt*         stmt;
						uint64            processed;
						
						basename = psprintf("table-%s.txt", relname);
						filename = psprintf("%s/%s", query_output_directory, basename);

						pstate = make_parsestate(NULL);
						pstate->p_sourcetext = "auto_dump internal copy";
						pstate->p_queryEnv   = create_queryEnv();

						stmt = makeNode(CopyStmt);
						stmt->type     = T_CopyStmt;
						stmt->relation = makeRangeVarFromNameList(stringToQualifiedNameList(relname, NULL));
						stmt->filename = filename;

						DoCopy(pstate, stmt, 0, strlen(pstate->p_sourcetext), &processed);

						fprintf(dest, "\\set command '\\\\copy %s from \\'' :dir '/%s\\''\n", relname, basename);
						fprintf(dest, ":command\n");

						pfree(basename);
						pfree(filename);
					}
					else if (data_format == DATA_FORMAT_COPY_STDIN)
					{
						ParseState* pstate;
						Relation rel;
						CopyToState cstate;

						pstate = make_parsestate(NULL);
						pstate->p_sourcetext = "auto_dump internal copy";
						pstate->p_queryEnv   = create_queryEnv();

						fprintf(dest, "COPY %s FROM stdin;\n", relname);

						rel = table_open(relid, NoLock);
						CopyDataDestCallback_file = dest;
						cstate = BeginCopyTo(pstate, rel, NULL, relid,
											NULL, false,
											CopyDataDestCallback, NULL, NIL);
						DoCopyTo(cstate);
						EndCopyTo(cstate);
						table_close(rel, NoLock);
						fprintf(dest, "\\.\n\n");
					}
				}
			}

			for (field=&fields[0]; field < &fields[numFields]; field++)
			{
				pfree(field->attname);
				ReleaseSysCache(field->tuple);
			}

			pfree(fields);
			pfree(relname);
		}

		ReleaseSysCache(classTuple);
	}

	pfree(buf.data);

	if (fCreatePers)
		fclose(fCreatePers);

	if (fCreateTemp)
		fclose(fCreateTemp);

	if (fDataPers)
		fclose(fDataPers);

	if (fDataTemp)
		fclose(fDataTemp);
}


static void
SaveQuery( QueryDesc* queryDesc )
{
	FILE*        f;
	mode_t       oumask;
	char*        unescaped;
	char*        dst;
	char const*  src;

	oumask = umask((mode_t) ((~(S_IRUSR | S_IWUSR | S_IWUSR)) & (S_IRWXU | S_IRWXG | S_IRWXO)));
	f = fopen(psprintf("%s/query.sql", query_output_directory), "wb");
	umask(oumask);

	unescaped = palloc( strlen(queryDesc->sourceText) + 3 );
	dst       = unescaped;
	src       = queryDesc->sourceText;
	while(*src){
		if( src[0] == '\\' && src[1] =='\\' ){
			src++;
		}
		*(dst++) = *(src++);
	}
	*(dst++) = ';';
	*(dst++) = '\n';
	*(dst++) = 0;
	fputs(unescaped, f);
	pfree(unescaped);

	fclose(f);
}


static void
SaveReadme(void)
{
	FILE*      fReadme     = NULL;
	fReadme = CreateDumpFile("readme.txt");
	fputs(
		"Дамп снят с использованием расширения auto_dump.\n"
		"\n"
		"Дамп содержит (в зависимости от настроек):\n"
		"  query.sql - текст запроса, для которого делается дамп\n"
		"  create_persistent.sql - команды по созданию постоянных таблиц и (или) индексов\n"
		"  create_temporary.sql - команды по созданию временных таблиц и (или) индексов\n"
		"  insert_persistent.sql - команды по заполнению постоянных таблиц\n"
		"  insert_temporary.sql - команды по заполнению временных таблиц\n"
		"  plan_explain.txt - план запроса в формате вывода команды EXPLAIN\n"
		"  plan_analyze.txt - план запроса в формате вывода команды EXPLAIN ANALYZE\n"
		"  table-<имя_таблица>.txt - файлы с данными таблиц (для auto_dump.data_format=\"copy-file\")\n"
		"\n"
		"\n"
		"Примеры использования дампа:\n"
		"\n"
		"1) Использование при наличии постоянных таблиц в базе:\n"
		"psql\n"
		"\\i create_temporary.sql\n"
		"\\i insert_temporary.sql\n"
		"\\i query.sql\n"
		"\n"
		"2) Использование на чистой базе:\n"
		"# Выполнить один раз:\n"
		"psql\n"
		"\\i create_persistent.sql\n"
		"\\i insert_persistent.sql\n"
		"# Выполнить много раз:\n"
		"psql\n"
		"\\i create_temporary.sql\n"
		"\\i insert_temporary.sql\n"
		"\\i query.sql\n", fReadme);
	fclose(fReadme);
}


static void
Dump(QueryDesc* queryDesc, bool havePlan, bool haveAnalysis)
{
	if (!query_dumped)
	{
		if(!PrepareDump())
			return;

		query_dumped = true;

		if (dump_create || dump_data || dump_indexes)
			SaveTables(queryDesc);
		
		if (dump_query)
			SaveQuery(queryDesc);

		if (dump_readme)
			SaveReadme();
	}

	if (havePlan && query_dumped && dump_plan && !plan_dumped)
	{
		plan_dumped = true;
		SavePlan(queryDesc, "plan_explain", false);
	}

	if (haveAnalysis && query_dumped && dump_plan && !plan_analysis_dumped)
	{
		plan_analysis_dumped = true;
		SavePlan(queryDesc, "plan_analyze", true);
	}
}


static void
ExecutorStart_hook_auto_dump(QueryDesc *queryDesc, int eflags)
{

	if (nesting_level == 0 && MyProcNumber != INVALID_PROC_NUMBER && backend_dump_mode)
	{
		if (backend_dump_mode[MyProcNumber] == MODE_NEXT)
			backend_dump_mode[MyProcNumber] = MODE_CURRENT;
		else
			backend_dump_mode[MyProcNumber] = MODE_OFF;
	}

	if (dump_enable && nesting_level == 0)
	{
		query_dumped         = false;
		plan_dumped          = false;
		plan_analysis_dumped = false;
		
		if (dump_plan)
			queryDesc->instrument_options |= INSTRUMENT_ALL;
		
		if (dump_plan || dump_on_bad_plan)
			queryDesc->instrument_options |= INSTRUMENT_ROWS;

		if(queryDesc->operation == CMD_SELECT
			|| queryDesc->operation == CMD_DELETE
			|| queryDesc->operation == CMD_INSERT
			|| queryDesc->operation == CMD_UPDATE
		){
			if (*dump_on_query_string && IsQueryMatch(queryDesc->sourceText, dump_on_query_string)) {
				Dump( queryDesc, false, false );
			}
		}
	}

	if (prev_ExecutorStart)
		prev_ExecutorStart(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);
	
	if (dump_enable && nesting_level == 0 && dump_on_duration >= 0 && queryDesc->totaltime == NULL)
	{
		MemoryContext oldcxt;
		oldcxt = MemoryContextSwitchTo(queryDesc->estate->es_query_cxt);
		queryDesc->totaltime = InstrAlloc(1, INSTRUMENT_ALL, false);
		MemoryContextSwitchTo(oldcxt);
	}

	if (dump_enable && query_dumped)
		Dump(queryDesc, true, false);
}


static void
ExecutorRun_hook_auto_dump(QueryDesc *queryDesc, ScanDirection direction,
					uint64 count)
{
	nesting_level++;
	PG_TRY();
	{
		if (prev_ExecutorRun)
			prev_ExecutorRun(queryDesc, direction, count);
		else
			standard_ExecutorRun(queryDesc, direction, count);
	}
	PG_FINALLY();
	{
		nesting_level--;
	}
	PG_END_TRY();

	if(dump_enable && nesting_level==0 && ((dump_plan && query_dumped) || (dump_on_bad_plan && IsBadPlan(queryDesc->planstate))))
		Dump(queryDesc, true, true);
}

static void
ExecutorFinish_hook_auto_dump(QueryDesc *queryDesc)
{
	nesting_level++;
	PG_TRY();
	{
		if (prev_ExecutorFinish)
			prev_ExecutorFinish(queryDesc);
		else
			standard_ExecutorFinish(queryDesc);
	}
	PG_FINALLY();
	{
		nesting_level--;
	}
	PG_END_TRY();
}


static void
ExecutorEnd_hook_auto_dump(QueryDesc *queryDesc)
{
	if (nesting_level == 0 && IS_MODE_ACTIVE())
	{
		backend_dump_mode[MyProcNumber] = MODE_OFF;
		Dump(queryDesc, true, true);
	}

	if(dump_enable && nesting_level==0 && queryDesc->totaltime && dump_on_duration >= 0 && !query_dumped)
	{
		InstrEndLoop(queryDesc->totaltime);
		if (queryDesc->totaltime->total * 1000.0 >= dump_on_duration)
			Dump(queryDesc, true, true);
	}

	if (prev_ExecutorEnd)
		prev_ExecutorEnd(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);
}



static void
ProcessInterrupts_hook_auto_dump(void)
{
	bool want_cancel = false;
	bool want_signal = false;

	if (dump_enable && dump_on_cancel)
		want_cancel = true;

	if (IS_MODE_ACTIVE())
		want_signal = true;
	
	if (want_cancel || want_signal)
	{
		PG_TRY();
			if (likely(!prev_ProcessInterrupts))
				standard_ProcessInterrupts();
			else
				prev_ProcessInterrupts();
		PG_CATCH();
			InterruptHoldoffCount++;

			if (ActivePortal && ActivePortal->queryDesc)
				Dump(ActivePortal->queryDesc, true, true);

			if (want_signal)
				backend_dump_mode[MyProcNumber] = MODE_OFF;

			InterruptHoldoffCount--;
			PG_RE_THROW();
		PG_END_TRY();
	}
	else
	{
		if (likely(!prev_ProcessInterrupts))
			standard_ProcessInterrupts();
		else
			prev_ProcessInterrupts();
	}
}



static void
request_auto_dump(int pid, int mode) {
	int num_backends = pgstat_fetch_stat_numbackends();
	int curr_backend;
	LocalPgBackendStatus* status = NULL;

	if (!backend_dump_mode)
		elog(ERROR,"auto_dump is not loaded via 'shared_preload_libraries'. Requesting of dump for other backends is not available.");

	if (!has_privs_of_role(GetUserId(), ROLE_PG_WRITE_SERVER_FILES))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					errmsg("permission denied to dump request"),
					errdetail("Only roles with privileges of the \"%s\" role may request auto_dump.",
							"pg_write_server_files")));

	for (curr_backend = 1; curr_backend <= num_backends; curr_backend++)
	{
		status = pgstat_get_local_beentry_by_index(curr_backend);
		if (status->backendStatus.st_procpid == pid)
			break;
		else
			status = NULL;
	}

	if(!status)
		elog(ERROR,"No backend found for pid %d", pid);

	if (status->proc_number == INVALID_PROC_NUMBER || status->proc_number > (MaxBackends + NUM_AUXILIARY_PROCS))
		elog(ERROR,"Backend is valid target for dump");

	if ((mode == MODE_IMMEDIATE || mode == MODE_CURRENT) && status->backendStatus.st_state != STATE_RUNNING && status->backendStatus.st_state != STATE_FASTPATH)
		elog(ERROR,"Backend is not running a query");
	
	backend_dump_mode[status->proc_number] = mode;
	pg_memory_barrier();
	
	if (mode == MODE_IMMEDIATE)
		DirectFunctionCall1(pg_cancel_backend, Int32GetDatum(pid));
}


Datum
auto_dump_immediate(PG_FUNCTION_ARGS) {
	request_auto_dump(PG_GETARG_INT32(0), MODE_IMMEDIATE);
	PG_RETURN_VOID();
}


Datum
auto_dump_current(PG_FUNCTION_ARGS) {
	request_auto_dump(PG_GETARG_INT32(0), MODE_CURRENT);
	PG_RETURN_VOID();
}


Datum
auto_dump_next(PG_FUNCTION_ARGS) {
	request_auto_dump(PG_GETARG_INT32(0), MODE_NEXT);
	PG_RETURN_VOID();
}


static Size
auto_dump_shmem_size(void)
{
	return MaxBackends + NUM_AUXILIARY_PROCS;
}

static void
auto_dump_shmem_request(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();

	RequestAddinShmemSpace(auto_dump_shmem_size());
}

static void
auto_dump_shmem_startup(void)
{
	bool found;

	backend_dump_mode = (char volatile*)ShmemInitStruct("auto_dump", auto_dump_shmem_size(), &found);

	if (!found)
	{
		for (int c=0; c <= MaxBackends; c++)
			backend_dump_mode[c] = MODE_OFF;
	}

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();
}

static void
auto_dump_cancel_current_dump(int code, Datum arg)
{
	(void) code;
	(void) arg;

	if (MyProcNumber != INVALID_PROC_NUMBER && backend_dump_mode)
		backend_dump_mode[MyProcNumber] = MODE_OFF;
}


void
_PG_init(void)
{
	DefineCustomBoolVariable("auto_dump.enable",
	                         "Enable auto-dump.",
	                         NULL,
	                         &dump_enable,
	                         dump_enable,
	                         PGC_SUSET,
	                         0,
	                         NULL, NULL, NULL);

	DefineCustomStringVariable("auto_dump.output_directory",
	                           "Output directory for dumped tables",
	                           NULL,
	                           &output_directory,
	                           output_directory,
	                           PGC_SUSET,
	                           0,
	                           NULL, NULL, NULL);

	DefineCustomStringVariable("auto_dump.dump_on_query_string",
	                           "Activation phrase for start dump query tables (more 10 characters).",
	                           NULL,
	                           &dump_on_query_string,
	                           dump_on_query_string,
	                           PGC_SUSET,
	                           0,
	                           NULL, NULL, NULL);

	DefineCustomBoolVariable("auto_dump.dump_on_cancel",
	                         "Dump tables when query is cancelled.",
	                         NULL,
	                         &dump_on_cancel,
	                         dump_on_cancel,
	                         PGC_SUSET,
	                         0,
	                         NULL, NULL, NULL);

	DefineCustomBoolVariable("auto_dump.dump_on_bad_plan",
	                         "Dump tables when query plan is considered bad.",
	                          NULL,
	                         &dump_on_bad_plan,
	                         dump_on_bad_plan,
	                         PGC_SUSET,
	                         0,
	                         NULL, NULL, NULL);

	DefineCustomIntVariable("auto_dump.dump_on_duration",
	                         "Dump tables when query duration is more than specified time. 0 to dump all, -1 to to ignore duration",
	                          NULL,
	                         &dump_on_duration,
	                         dump_on_duration,
	                         -1,
	                         INT_MAX,
	                         PGC_SUSET,
	                         0,
	                         NULL, NULL, NULL);


	DefineCustomBoolVariable("auto_dump.dump_all_temp_tables",
	                         "Dump all temporary tables of backend.",
	                         NULL,
	                         &dump_all_temp_tables,
	                         dump_all_temp_tables,
	                         PGC_SUSET,
	                         0,
	                         NULL, NULL, NULL);

	DefineCustomBoolVariable("auto_dump.dump_persistent_tables",
	                         "Dump persistent tables.",
	                         NULL,
	                         &dump_persistent_tables,
	                         dump_persistent_tables,
	                         PGC_SUSET,
	                         0,
	                         NULL, NULL, NULL);

	DefineCustomBoolVariable("auto_dump.dump_temporary_tables",
	                         "Dump temporary tables.",
	                         NULL,
	                         &dump_temporary_tables,
	                         dump_temporary_tables,
	                         PGC_SUSET,
	                         0,
	                         NULL, NULL, NULL);

	DefineCustomBoolVariable("auto_dump.dump_data",
	                         "Dump tables data.",
	                         NULL,
	                         &dump_data,
	                         dump_data,
	                         PGC_SUSET,
	                         0,
	                         NULL, NULL, NULL);

	DefineCustomBoolVariable("auto_dump.dump_indexes",
	                         "Dump indexes for tables.",
	                         NULL,
	                         &dump_indexes,
	                         dump_indexes,
	                         PGC_SUSET,
	                         0,
	                         NULL, NULL, NULL);

	DefineCustomBoolVariable("auto_dump.dump_query",
	                         "Dump query itself.",
	                         NULL,
	                         &dump_query,
	                         dump_query,
	                         PGC_SUSET,
	                         0,
	                         NULL, NULL, NULL);

	DefineCustomBoolVariable("auto_dump.dump_create",
	                         "Dump creation of tables.",
	                         NULL,
	                         &dump_create,
	                         dump_create,
	                         PGC_SUSET,
	                         0,
	                         NULL, NULL, NULL);

	DefineCustomBoolVariable("auto_dump.dump_plan",
	                         "Dump execution plan of query.",
	                         NULL,
	                         &dump_plan,
	                         dump_plan,
	                         PGC_SUSET,
	                         0,
	                         NULL, NULL, NULL);

	DefineCustomIntVariable("auto_dump.bad_plan_percent_threshold",
	                        "Sets the percent difference between estimated and actual row count to trigger plan dump.",
	                        NULL,
	                        &dump_plan_percent_threshold,
	                        dump_plan_percent_threshold,
	                        0, INT_MAX,
	                        PGC_SUSET,
	                        0,
	                        NULL,
	                        NULL,
	                        NULL);

	DefineCustomIntVariable("auto_dump.bad_plan_count_threshold",
	                        "Sets the row count difference between estimated and actual row count to trigger plan dump.",
	                        NULL,
	                        &dump_plan_count_threshold,
	                        dump_plan_count_threshold,
	                        0, INT_MAX,
	                        PGC_SUSET,
	                        0,
	                        NULL,
	                        NULL,
	                        NULL);

	DefineCustomEnumVariable("auto_dump.data_format",
	                         "Format of saved data",
	                         NULL,
	                         &data_format,
	                         data_format,
	                         data_format_options,
	                         PGC_SUSET,
	                         0,
	                         NULL, NULL, NULL);

	MarkGUCPrefixReserved("auto_dump");

	prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = ExecutorStart_hook_auto_dump;

	prev_ExecutorRun = ExecutorRun_hook;
	ExecutorRun_hook = ExecutorRun_hook_auto_dump;

	prev_ExecutorFinish = ExecutorFinish_hook;
	ExecutorFinish_hook = ExecutorFinish_hook_auto_dump;

	prev_ExecutorEnd = ExecutorEnd_hook;
	ExecutorEnd_hook = ExecutorEnd_hook_auto_dump;

	prev_ProcessInterrupts = ProcessInterrupts_hook;
	ProcessInterrupts_hook = ProcessInterrupts_hook_auto_dump;

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook		= auto_dump_shmem_startup;

	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook		= auto_dump_shmem_request;

	on_proc_exit(auto_dump_cancel_current_dump, 0);
}
