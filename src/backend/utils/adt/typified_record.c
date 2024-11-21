
/*
 */

#include "postgres.h"

#include "catalog/pg_type.h"
#include "utils/fmgrprotos.h"
#include "funcapi.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#include "catalog/namespace.h"

Datum
typedrow_in(PG_FUNCTION_ARGS)
{
	char	   *string = PG_GETARG_CSTRING(0);
	char	   *row_ptr = NULL;
	Datum		row;
	TupleDesc tupdesc;
	HeapTuple	tuple;
	Form_pg_type typ;
	Oid typinput;
	int i = 0;
	char *typname_ptr;
	List *typlist = NIL;
	ListCell *lc;

	tuple = SearchSysCache1(TYPEOID, RECORDOID);

	typ = (Form_pg_type) GETSTRUCT(tuple);
	typinput = typ->typinput;

	ReleaseSysCache(tuple);

	row_ptr = string;
	Assert(row_ptr[0] == '(');
	while (row_ptr[i] != ')')
	{
		i++;

		typname_ptr = &row_ptr[i];
		for (; row_ptr[i] != ')' && row_ptr[i] != ','; i++);
		typlist = lappend(typlist, pnstrdup(typname_ptr, (int)(&row_ptr[i] - typname_ptr)));
	}

	tupdesc = CreateTemplateTupleDesc(list_length(typlist));
	row_ptr = &row_ptr[i] + 1;

	i=1;
	foreach (lc, typlist)
	{
		char *typname = (char *) lfirst(lc);
		Oid nspoid;
		Oid elmtype;

		nspoid = get_namespace_oid("pg_catalog", false);
		elmtype = GetSysCacheOid2(TYPENAMENSP, Anum_pg_type_oid,
										 PointerGetDatum(typname),
										 nspoid);
		TupleDescInitEntry(tupdesc, i, NULL, elmtype, -1, 0);
		i++;
	}

	tupdesc = BlessTupleDesc(tupdesc);
	row = OidInputFunctionCall(typinput, row_ptr, RECORDOID, tupdesc->tdtypmod);
	return row;
}

/*
 *		typedrow_out			- converts typedrow to "num"
 */
Datum
typedrow_out(PG_FUNCTION_ARGS)
{
	Datum		input = PG_GETARG_DATUM(0);
	HeapTupleHeader rec = PG_GETARG_HEAPTUPLEHEADER(0);
	char *result;
	HeapTuple		tuple;
	Form_pg_type	typ;
	Oid				typoutput;
	StringInfoData buf;

	Oid			tupType;
	int32		tupTypmod;
	TupleDesc	tupdesc;
	int			i;

	tuple = SearchSysCache1(TYPEOID, RECORDOID);

	typ = (Form_pg_type) GETSTRUCT(tuple);
	typoutput = typ->typoutput;
	ReleaseSysCache(tuple);

	initStringInfo(&buf);
	result = OidOutputFunctionCall(typoutput, input);

	/* Extract type info from the tuple itself */
	tupType = HeapTupleHeaderGetTypeId(rec);
	tupTypmod = HeapTupleHeaderGetTypMod(rec);
	tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);

	appendStringInfoChar(&buf, '(');
	for (i=0; i < tupdesc->natts; i++)
	{
		HeapTuple typeTuple;
		Form_pg_type typeForm;
		Oid typoid = tupdesc->attrs[i].atttypid;

		if (i > 0)
			appendStringInfoChar(&buf, ',');

		if (tupdesc->attrs[i].atttypid == UNKNOWNOID)
			typoid = TEXTOID;
		typeTuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typoid));
		if (!HeapTupleIsValid(typeTuple))
			elog(ERROR, "cache lookup failed for type %u", typoid);
		typeForm = (Form_pg_type) GETSTRUCT(typeTuple);
		appendStringInfo(&buf, "%s", typeForm->typname.data);
		ReleaseSysCache(typeTuple);
	}

	appendStringInfoChar(&buf, ')');
	ReleaseTupleDesc(tupdesc);

	appendStringInfo(&buf, "%s", result);
	PG_RETURN_CSTRING(buf.data);
}

/*
 *		numeric_send			- converts numeric to binary format
 */
Datum
typified_record_send(PG_FUNCTION_ARGS)
{
	PG_RETURN_NULL();
}


/*
 *		numeric_recv			- converts external binary format to numeric
 *
 * External format is a sequence of int16's:
 * ndigits, weight, sign, dscale, NumericDigits.
 */
Datum
typified_record_recv(PG_FUNCTION_ARGS)
{
	PG_RETURN_NULL();
}
