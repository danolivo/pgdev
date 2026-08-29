#include "postgres.h"

#include "catalog/pg_type.h"

#include "replication/logical.h"
#include "replication/origin.h"

#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/catcache.h"
#include "utils/timestamp.h"
#include "utils/cash.h"
#include "utils/pg_locale.h"
#include "utils/date.h"
#include "utils/datetime.h"

#include "mchar.h"

PG_MODULE_MAGIC;


static Oid MCHAROID = InvalidOid;
static Oid MVARCHAROID = InvalidOid;
static const char cQuoteChar = '\'';
static const char cContinueChar = '!';

extern PGDLLEXPORT void _PG_init(void);
extern PGDLLEXPORT void _PG_output_plugin_init(OutputPluginCallbacks* cb);

typedef struct
{
	MemoryContext context;
	int record_buf_size;
	//Заказанный размер записи
	int prepare_header_size;
	//Размер заголовка, который записывает OutputPluginPrepareWrite в ctx->out
	bool include_xids;
	//флаг Записывать идентификатор транзакции
	bool skip_change;
	//флаг пропустить все, ничего не выводить
	bool xact_wrote_changes;
	//Признак того, что старт транзакции уже выведен.
} DecodingData;

static void decode_startup(LogicalDecodingContext* ctx,
	OutputPluginOptions* opt, bool is_init);
static void decode_shutdown(LogicalDecodingContext* ctx
	);
static void decode_begin_txn(LogicalDecodingContext* ctx,
	ReorderBufferTXN* txn);
static void decode_commit_txn(LogicalDecodingContext* ctx,
	ReorderBufferTXN* txn, XLogRecPtr commit_lsn);
static void decode_change(LogicalDecodingContext* ctx,
	ReorderBufferTXN* txn, Relation rel, ReorderBufferChange* change);
static void decode_truncate(LogicalDecodingContext* ctx,
	ReorderBufferTXN* txn, int nrelations, Relation relations[],
	ReorderBufferChange* change);
static bool filter_by_origin(LogicalDecodingContext *ctx,
	RepOriginId origin_id);

#ifndef U8_TRUNCATE_IF_INCOMPLETE
// © 2016 and later: Unicode, Inc. and others.
// License & terms of use: http://www.unicode.org/copyright.html
/*
*******************************************************************************
*
*   Copyright (C) 1999-2015, International Business Machines
*   Corporation and others.  All Rights Reserved.
*
*******************************************************************************
*   file name:  utf8.h
*   encoding:   UTF-8
*   tab size:   8 (not used)
*   indentation:4
*
*   created on: 1999sep13
*   created by: Markus W. Scherer
*/
#define U8_LEAD4_T1_BITS \
"\x00\x00\x00\x00\x00\x00\x00\x00\x1E\x0F\x0F\x0F\x00\x00\x00\x00"
#define U8_IS_VALID_LEAD4_AND_T1(lead, t1) \
(U8_LEAD4_T1_BITS[(uint8_t)(t1)>>4]&(1<<((lead)&7)))
#define U8_LEAD3_T1_BITS \
"\x20\x30\x30\x30\x30\x30\x30\x30\x30\x30\x30\x30\x30\x10\x30\x30"
#define U8_IS_VALID_LEAD3_AND_T1(lead, t1) \
(U8_LEAD3_T1_BITS[(lead)&0xf]&(1<<((uint8_t)(t1)>>5)))
#define U8_TRUNCATE_IF_INCOMPLETE(s, start, length) { \
	if((length)>(start)) { \
		uint8_t __b1=s[(length)-1]; \
		if(U8_IS_SINGLE(__b1)) { \
			/* common ASCII character */ \
		} else if(U8_IS_LEAD(__b1)) { \
			--(length); \
		} else if(U8_IS_TRAIL(__b1) && ((length)-2)>=(start)) { \
			uint8_t __b2=s[(length)-2]; \
			if(0xe0<=__b2 && __b2<=0xf4) { \
				if(__b2<0xf0 ? U8_IS_VALID_LEAD3_AND_T1(__b2, __b1) : \
						U8_IS_VALID_LEAD4_AND_T1(__b2, __b1)) { \
					(length)-=2; \
				} \
			} else if(U8_IS_TRAIL(__b2) && ((length)-3)>=(start)) { \
				uint8_t __b3=s[(length)-3]; \
				if(0xf0<=__b3 && __b3<=0xf4 && \
					U8_IS_VALID_LEAD4_AND_T1(__b3, __b2)) { \
					(length)-=3; \
				} \
			} \
		} \
	} \
}

#endif

void _PG_init(void)
{
}

void _PG_output_plugin_init(OutputPluginCallbacks* cb)
{
	AssertVariableIsOfType(&_PG_output_plugin_init, LogicalOutputPluginInit);

	cb->startup_cb = decode_startup;
	cb->begin_cb = decode_begin_txn;
	cb->change_cb = decode_change;
	cb->truncate_cb = decode_truncate;
	cb->commit_cb = decode_commit_txn;
	cb->shutdown_cb = decode_shutdown;
	cb->filter_by_origin_cb = filter_by_origin;
}

static bool tryExtractBoolOption(DefElem* elem, const char* name, bool* dest)
{
	if (strcmp(elem->defname, name) == 0)
	{
		if (elem->arg != NULL)
		{
			if (!parse_bool(strVal(elem->arg), dest))
				ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("could not parse value \"%s\" for parameter \"%s\"",
						strVal(elem->arg), elem->defname)));
		}
		return true;
	}
	else
		return false;
}

static bool tryExtractIntOption(DefElem* elem, const char* name, int32* dest)
{
	if (strcmp(elem->defname, name) == 0)
	{
		if (elem->arg != NULL)
			*dest = pg_strtoint32(strVal(elem->arg));
		return true;
	}
	else
		return false;
}

static void readTypeOID(char* typeName, Oid* typeOid)
{
	if (*typeOid == InvalidOid)
	{
		CatCList* catlist = SearchSysCacheList(TYPENAMENSP,
			1, CStringGetDatum(typeName), 0, 0);
		if (catlist->n_members == 1)
			*typeOid = ((Form_pg_type)GETSTRUCT(
				&catlist->members[0]->tuple))->oid;
		ReleaseSysCacheList(catlist);

		if (*typeOid == InvalidOid)
			elog(WARNING, "OID of type %s not defined!", typeName);
	}
}

static void decode_startup(LogicalDecodingContext* ctx,
	OutputPluginOptions* opt, bool is_init)
{
	ListCell* option;
	DecodingData* data = palloc0(sizeof(DecodingData));

	data->include_xids = true;
	data->skip_change = false;
	data->record_buf_size = ALLOCSET_DEFAULT_MAXSIZE / 4;
	foreach(option, ctx->output_plugin_options)
	{
		DefElem* elem = lfirst(option);
		Assert(elem->arg == NULL || IsA(elem->arg, String));

		if (!tryExtractBoolOption(elem, "include-xids",
			&data->include_xids))
			if (!tryExtractBoolOption(elem, "skip-change",
				&data->skip_change))
				if (!tryExtractIntOption(elem, "slice_size",
					&data->record_buf_size))
				{
					ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("option \"%s\" = \"%s\" is unknown",
							elem->defname,
							elem->arg ? strVal(elem->arg) : "(null)")
						)
					);
				}
	}
	data->context = AllocSetContextCreate(ctx->context,
		"text conversion context",
		ALLOCSET_DEFAULT_SIZES);
	ctx->output_plugin_private = data;

	opt->output_type = OUTPUT_PLUGIN_TEXTUAL_OUTPUT;
	opt->receive_rewrites = false;
}

static void decode_shutdown(LogicalDecodingContext* ctx)
{
	DecodingData* data = ctx->output_plugin_private;

	MemoryContextDelete(data->context);
}

static bool filter_by_origin(LogicalDecodingContext *ctx,
	RepOriginId origin_id)
{
	DecodingData* data = ctx->output_plugin_private;
	return data && data->skip_change;
}

static void decode_begin_txn(LogicalDecodingContext* ctx,
	ReorderBufferTXN* txn)
{
	DecodingData* data = ctx->output_plugin_private;

	data->xact_wrote_changes = false;
}

static void decode_commit_txn(LogicalDecodingContext* ctx,
	ReorderBufferTXN* txn, XLogRecPtr commit_lsn)
{
	DecodingData* data = ctx->output_plugin_private;

	if (!data->xact_wrote_changes || data->skip_change)
		return;

	OutputPluginPrepareWrite(ctx, true);
	if (data->include_xids)
		appendStringInfo(ctx->out, "C %u", txn->xid);
	else
		appendStringInfo(ctx->out, "C");
	OutputPluginWrite(ctx, true);
}

static int record_buf_size(LogicalDecodingContext* ctx) {
	return ((DecodingData*)(ctx->output_plugin_private))->record_buf_size;
}

static void prepareFlushedCtx(LogicalDecodingContext* ctx)
{
	ctx->out->len = 0;
	OutputPluginPrepareWrite(ctx, true);
	((DecodingData*)(ctx->output_plugin_private)
	)->prepare_header_size = ctx->out->len;
}

static int checkFlushCtx(LogicalDecodingContext* ctx, int toWriteSize)
{	//возвращает максимальное число байт,
	//которое можно записать до превышения лимита длинны
	int overflowRemain = record_buf_size(ctx) - ctx->out->len - 1;
	if (overflowRemain <= toWriteSize)
	{
		appendStringInfoChar(ctx->out, cContinueChar);
		switch (((DecodingData*)(ctx->output_plugin_private)
				)->prepare_header_size)
		{
		case 0:
			break;
		case 1 + sizeof(int64) * 3:
			memset(&ctx->out->data[1], 0, sizeof(int64) * 2);
			break;
		default:
			ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("Unsupported ctx->prepare_write function!")));

		}
		OutputPluginWrite(ctx, false);
		prepareFlushedCtx(ctx);
		return record_buf_size(ctx) - ctx->out->len - 1;
	}
	else
		return overflowRemain;
}

static void printByts(LogicalDecodingContext* ctx, Datum val)
{
	const char n[] = { "0123456789abcdef" };
	const int cDig = 16;
	char* bytsData = VARDATA(val);
	int32 bytsLen = VARSIZE_ANY_EXHDR(val);
	int resultSize = 3 + bytsLen * 2 + 1; //остаток, который требуется записать
	int overflowRemain = checkFlushCtx(ctx, 3);
	if (resultSize > overflowRemain)
		enlargeStringInfo(ctx->out, overflowRemain);
	else
		enlargeStringInfo(ctx->out, resultSize);

	appendStringInfoString(ctx->out, "\'\\x");
	overflowRemain -= 3;
	resultSize -= 3;

	{
		int32 i;
		for (i = 0; i < bytsLen; ++i)
		{
			int x;
			if (overflowRemain < 2)
			{
				overflowRemain = checkFlushCtx(ctx, 2);
				if (resultSize > overflowRemain)
					enlargeStringInfo(ctx->out, overflowRemain);
				else
					enlargeStringInfo(ctx->out, resultSize);
			}
			x = bytsData[i] & 255;
			ctx->out->data[ctx->out->len] = n[x / cDig];
			ctx->out->data[ctx->out->len + 1] = n[x % cDig];
			ctx->out->len += 2;
			overflowRemain -= 2;
			resultSize -= 2;
		}
	}
	if (overflowRemain < 1)
		checkFlushCtx(ctx, 1);
	appendStringInfoChar(ctx->out, cQuoteChar);
}

static bool truncateIfIncmoplete(const int maxCharSize, const char* str, int* len)
{
	if (maxCharSize == 1)
		return true;
	else
	{
		int dbEnc = GetDatabaseEncoding();
		if (dbEnc == PG_UTF8)
		{
			U8_TRUNCATE_IF_INCOMPLETE(str, 0, *len);
			return (len > 0);
		}
		else
		{	//медленный экзотичный вариант
			int truncCount;
			for (truncCount = 1; truncCount < maxCharSize; ++truncCount)
			{
				int charLen;
				for (charLen = 1;
					charLen <= maxCharSize && *len >= charLen;
					++charLen)
					if (pg_verify_mbstr(dbEnc, &str[*len - charLen],
										charLen, true))
						return true;

				--(*len);
			}
		}
	}

	return false;
}

static void printCharVarchar(LogicalDecodingContext* ctx, Datum val)
{
	const int maxCharSize = pg_database_encoding_max_length();
	char* bytsData = VARDATA(val);
	int32 bytsLen = VARSIZE_ANY_EXHDR(val);
	int overflowRemain = checkFlushCtx(ctx, 1 + maxCharSize);

	appendStringInfoChar(ctx->out, cQuoteChar);
	--overflowRemain;
	{
		char* pBegin = bytsData;
		int L = 0;
		int i;
		for (i = 0; i < bytsLen; ++i)
		{
			bool overflow;
			++L;
			overflow = (L >= overflowRemain);
			if (bytsData[i] == cQuoteChar || overflow || i + 1 == bytsLen)
			{
				if (overflow &&
					!(bytsData[i] == cQuoteChar || i + 1 == bytsLen))
				{
					if (!truncateIfIncmoplete(maxCharSize, pBegin, &L))

						ereport(ERROR,
							(errcode(ERRCODE_INVALID_CHARACTER_VALUE_FOR_CAST),
								errmsg("invalid string value")
							)
						);
				}
				appendBinaryStringInfo(ctx->out, pBegin, L);
				pBegin += L;
				if (bytsData[i] == cQuoteChar)
				{
					overflowRemain = checkFlushCtx(ctx, maxCharSize+1);
					appendStringInfoChar(ctx->out, bytsData[i]);
					--overflowRemain;
				}
				else if (overflow) //гарантированный сброс буфера
					overflowRemain = checkFlushCtx(ctx, record_buf_size(ctx));
				else
					overflowRemain = checkFlushCtx(ctx, maxCharSize);
				L = 0;
			}

		}
	}
	if (overflowRemain < 1)
		checkFlushCtx(ctx, 1);
	appendStringInfoChar(ctx->out, cQuoteChar);
}

static int printM(const UChar* wordsData,
	int wordsLen, LogicalDecodingContext* ctx)
{
	const int maxCharSize = pg_database_encoding_max_length();
	const UChar cQuoteUChar = L'\'';
	int overflowRemain = checkFlushCtx(ctx, 1 + maxCharSize);

	appendStringInfoChar(ctx->out, cQuoteChar);
	--overflowRemain;
	{
		const UChar* pBegin = wordsData;
		int L = 0;
		int i;
		for (i = 0; i < wordsLen; ++i)
		{
			bool overflow;
			++L;
			overflow = (L*maxCharSize >= overflowRemain);
			if (wordsData[i] == cQuoteUChar ||
				overflow ||
				i + 1 == wordsLen)
			{
				if (overflow &&
					!(wordsData[i] == cQuoteUChar || i + 1 == wordsLen))
				{
					if (U16_IS_LEAD(wordsData[i]))
						--L;

					if (L == 0 || (i > 0 && U16_IS_LEAD(wordsData[i - 1])))
						ereport(ERROR,
							(errcode(ERRCODE_INVALID_CHARACTER_VALUE_FOR_CAST),
								errmsg("invalid utf16 string value")
							)
						);
				}
				enlargeStringInfo(ctx->out, L * maxCharSize);
				ctx->out->len += UChar2Char(pBegin, L,
					&ctx->out->data[ctx->out->len]);
				pBegin += L;

				if (wordsData[i] == cQuoteUChar)
				{
					overflowRemain = checkFlushCtx(ctx, maxCharSize+1);
					appendStringInfoChar(ctx->out, cQuoteChar);
					--overflowRemain;
				}
				else if (overflow)
					overflowRemain = checkFlushCtx(ctx, record_buf_size(ctx));
				else
					overflowRemain = checkFlushCtx(ctx, maxCharSize);
				L = 0;
			}
		}
	}
	return overflowRemain;
}

static void printMVarchar(LogicalDecodingContext* ctx, Datum val)
{
	const UChar* pBegin = (UChar*)(DatumGetPointer(val) + MVARCHARHDRSZ);
	if (printM(pBegin, UVARCHARLENGTH(val), ctx) < 1)
		checkFlushCtx(ctx, 1);
	appendStringInfoChar(ctx->out, cQuoteChar);
}

static void printMChar(LogicalDecodingContext* ctx, Datum val)
{
	const UChar* pBegin = (UChar*)(DatumGetPointer(val) + MCHARHDRSZ);
	int32  trailBlanksCount =
		DatumGetMChar(val)->typmod - u_countChar32(pBegin, UCHARLENGTH(val));
	int overflowRemain = printM(pBegin, UCHARLENGTH(val), ctx);
	while (trailBlanksCount > 0)
	{

		if (trailBlanksCount > overflowRemain)
		{
			appendStringInfoSpaces(ctx->out, overflowRemain);
			trailBlanksCount -= overflowRemain;
			overflowRemain = checkFlushCtx(ctx, 1);
		}
		else
		{
			appendStringInfoSpaces(ctx->out, trailBlanksCount);
			overflowRemain -= trailBlanksCount;
			trailBlanksCount = 0;
		}
	}
	if (overflowRemain < 1)
		checkFlushCtx(ctx, 1);
	appendStringInfoChar(ctx->out, cQuoteChar);
}

static void appendCtxString(LogicalDecodingContext* ctx, char* str)
{
	int l = strlen(str);
	checkFlushCtx(ctx, l);
	appendBinaryStringInfo(ctx->out, str, l);
}

static void printTimestamp(LogicalDecodingContext* ctx, Datum val)
{
	Timestamp ts = DatumGetTimestamp(val);
	if (!TIMESTAMP_NOT_FINITE(ts))
	{
		struct pg_tm tm;
		fsec_t		fsec;
		if (timestamp2tm(ts, NULL, &tm, &fsec, NULL, NULL) == 0)
		{   //отсутствие в параметрах указателя на tz
			//приводит к конвертации часов (ts with timezone)
			//например было 10:23:54.123+02 получим 08:23:54
			char* str;
			checkFlushCtx(ctx, 14);
			enlargeStringInfo(ctx->out, 14);
			str = ctx->out->data + ctx->out->len;
			ctx->out->len += 14;
			str = pg_ultostr_zeropad(str,
				(tm.tm_year > 0) ? tm.tm_year : -(tm.tm_year - 1), 4);
			str = pg_ultostr_zeropad(str, tm.tm_mon, 2);
			str = pg_ultostr_zeropad(str, tm.tm_mday, 2);
			str = pg_ultostr_zeropad(str, tm.tm_hour, 2);
			str = pg_ultostr_zeropad(str, tm.tm_min, 2);
			str = pg_ultostr_zeropad(str, abs(tm.tm_sec), 2);
			return;
		}
	}
	appendCtxString(ctx, "'invalid timestamp'");
}

static void printDate(LogicalDecodingContext* ctx, Datum val)
{
	DateADT d = DatumGetDateADT(val);
	if (!DATE_NOT_FINITE(d))
	{
		char* str;
		int year, mon, day;
		j2date(d + POSTGRES_EPOCH_JDATE, &year, &mon, &day);
		checkFlushCtx(ctx, 8);
		enlargeStringInfo(ctx->out, 8);
		str = ctx->out->data + ctx->out->len;
		ctx->out->len += 8;
		str = pg_ultostr_zeropad(str, (year > 0) ? year : -(year - 1), 4);
		str = pg_ultostr_zeropad(str, mon, 2);
		str = pg_ultostr_zeropad(str, day, 2);
		return;
	}
	appendCtxString(ctx, "'invalid date'");
}

static void printTime(LogicalDecodingContext* ctx, Datum val)
{
	TimeADT t = DatumGetTimeADT(val);
	char* str;
	struct pg_tm tm;
	fsec_t		fsec;
	time2tm(t, &tm, &fsec);
	checkFlushCtx(ctx, 14);
	enlargeStringInfo(ctx->out, 14);
	str = ctx->out->data + ctx->out->len;
	ctx->out->len += 14;
	str = pg_ultostr_zeropad(str, 0, 4);
	str = pg_ultostr_zeropad(str, 0, 2);
	str = pg_ultostr_zeropad(str, 0, 2);
	str = pg_ultostr_zeropad(str, tm.tm_hour, 2);
	str = pg_ultostr_zeropad(str, tm.tm_min, 2);
	str = pg_ultostr_zeropad(str, abs(tm.tm_sec), 2);
}

static void printMoney(LogicalDecodingContext* ctx, Datum val)
{
	Cash v = DatumGetCash(val);
	char buf[128];
	char* pBuf = &buf[127];
	bool minus = (v < 0);
	struct lconv *lconvert = PGLC_localeconv();
	int points = lconvert->frac_digits;

	if (points < 0 || points > 10)
		points = 2;

	buf[127] = 0;
	if (minus)
		v = -v;

	do {
		*(--pBuf) = ((uint64)v % 10) + '0';
		--points;

		if (points == 0)
			*(--pBuf) = '.';

		if (v)
			v = ((uint64)v) / 10;
	} while (v || points >= 0);
	if (minus)
		*(--pBuf) = '-';

	appendCtxString(ctx, pBuf);
}

static void printBool(LogicalDecodingContext* ctx, Datum val)
{
	appendCtxString(ctx, DatumGetBool(val) ? "true" : "false");
}

static void printDefault(LogicalDecodingContext* ctx,
							Datum val, Oid typid, Oid	typoutput)
{   // Вывод с помощью стандартной OUTPUT функции ..._out
	char* dataAsChar = OidOutputFunctionCall(typoutput, val);
	switch (typid)
	{
	case INT2OID:
	case INT4OID:
	case INT8OID:
	case OIDOID:
	case FLOAT4OID:
	case FLOAT8OID:
	case NUMERICOID:
		appendCtxString(ctx, dataAsChar);
		break;

	case BITOID:
	case VARBITOID:
		checkFlushCtx(ctx, (int)strlen(dataAsChar) + 3);
		appendStringInfo(ctx->out, "B'%s'", dataAsChar);
		break;

	default:
		{
			const int maxCharSize = pg_database_encoding_max_length();
			const char* pBegin;
			const char* pEnd = dataAsChar;
			int overflowRemain = checkFlushCtx(ctx, maxCharSize + 1);

			appendStringInfoChar(ctx->out, cQuoteChar);
			--overflowRemain;
			//в отличие от printCharVarchar,
			//здесь я не знаю длинну, но точно знаю, что на конце ноль
			for (pBegin = dataAsChar; *pBegin; pBegin = pEnd)
			{
				bool overflow;
				while (*pEnd &&
						*pEnd != cQuoteChar &&
						(int)(pEnd - pBegin) < overflowRemain)
					++pEnd;
				overflow = (int)(pEnd - pBegin) >= overflowRemain;
				if (pEnd != pBegin)
				{
					if (overflow && *pEnd && *pEnd != cQuoteChar)
					{
						int32 L = (int32)(pEnd - pBegin);
						if (!truncateIfIncmoplete(maxCharSize, pBegin, &L))
							ereport(ERROR,
							(errcode(ERRCODE_INVALID_CHARACTER_VALUE_FOR_CAST),
								errmsg("invalid string value")
								));
						pEnd = pBegin + L;
					}
					appendBinaryStringInfo(ctx->out,
						pBegin, (int)(pEnd - pBegin));
				}

				if (*pEnd == cQuoteChar)
				{
					overflowRemain = checkFlushCtx(ctx, maxCharSize + 2);
					appendStringInfoChar(ctx->out, *pEnd);
					appendStringInfoChar(ctx->out, *pEnd);
					++pEnd;
					overflowRemain -= 2;
				}
				else if (overflow)
					overflowRemain = checkFlushCtx(ctx, record_buf_size(ctx));
				else
					overflowRemain = checkFlushCtx(ctx, maxCharSize);
			}
			if (overflowRemain < 1)
				checkFlushCtx(ctx, 1);
			appendStringInfoChar(ctx->out, cQuoteChar);
		}
		break;
	}
	pfree(dataAsChar);
}

static void printTuple(LogicalDecodingContext* ctx,
	TupleDesc tupdesc, HeapTuple tuple,
	bool skip_nulls, char* tableName)
{

	if (tuple == NULL)
		appendCtxString(ctx, " (no-tuple-data)");
	else
	{
		int natt;
		for (natt = 0; natt < tupdesc->natts; natt++)
		{
			bool typisvarlena;
			Oid	typoutput;
			Form_pg_attribute attr = TupleDescAttr(tupdesc, natt);
			Oid typid = attr->atttypid;
			bool isnull;
			Datum origval;

			if (attr->attisdropped)
				continue;
			if (attr->attnum < 0) // Don't print system columns,
				continue;//oid will already have been printed if present.

			origval = heap_getattr(tuple, natt + 1, tupdesc, &isnull);
			if (isnull)
			{
				if (!skip_nulls)
					appendCtxString(ctx, " null");
				continue;
			}

			checkFlushCtx(ctx, 1);
			appendStringInfoChar(ctx->out, ' ');

			getTypeOutputInfo(typid, &typoutput, &typisvarlena);

			if (typisvarlena)
			{
				if (VARATT_IS_EXTERNAL_ONDISK(origval))
					appendCtxString(ctx, "unchanged-toast-datum");
				else
				{
					Datum val = PointerGetDatum(PG_DETOAST_DATUM(origval));

					if (typid == BPCHAROID ||
						typid == VARCHAROID ||
						typid == TEXTOID)
						printCharVarchar(ctx, val);
					else if (typid == BYTEAOID)
						printByts(ctx, val);
					else if (typid > FirstNormalObjectId)  {
						readTypeOID("mchar", &MCHAROID);
						readTypeOID("mvarchar", &MVARCHAROID);

						if (typid == MCHAROID)
							printMChar(ctx, val);
						else if (typid == MVARCHAROID)
							printMVarchar(ctx, val);
						else
							printDefault(ctx, val, typid, typoutput);
					} else
						printDefault(ctx, val, typid, typoutput);

					if (DatumGetPointer(val) != DatumGetPointer(origval))
						pfree(DatumGetPointer(val));
				}

			}
			else
			{
				switch (typid)
				{
				case MONEYOID:
					printMoney(ctx, origval);
					break;
				case TIMESTAMPOID:
				case TIMESTAMPTZOID:
					printTimestamp(ctx, origval);
					break;
				case DATEOID:
					printDate(ctx, origval);
					break;
				case TIMEOID:
					printTime(ctx, origval);
					break;
				case BOOLOID:
					printBool(ctx, origval);
					break;
				default:
					printDefault(ctx, origval, typid, typoutput);
					break;
				}
			}
		}
	}
}

static void printTransaction(DecodingData* data,
	LogicalDecodingContext* ctx, ReorderBufferTXN* txn)
{
	if (data->xact_wrote_changes)
		return;

	OutputPluginPrepareWrite(ctx, false);
	if (data->include_xids)
		appendStringInfo(ctx->out, "B %u", txn->xid);
	else
		appendStringInfoString(ctx->out, "B");
	OutputPluginWrite(ctx, false);
	data->xact_wrote_changes = true;
}

static void decode_change(LogicalDecodingContext* ctx,
	ReorderBufferTXN* txn, Relation relation, ReorderBufferChange* change)
{
	DecodingData* data = ctx->output_plugin_private;
	if (data->skip_change)
		return;
	{
		MemoryContext old = MemoryContextSwitchTo(data->context);
		TupleDesc tupdesc = RelationGetDescr(relation);
		char* tableName = RelationGetRelationName(relation);

		printTransaction(data, ctx, txn);
		prepareFlushedCtx(ctx);
		switch (change->action)
		{
		case REORDER_BUFFER_CHANGE_INSERT:
		{
			appendStringInfoString(ctx->out, "I ");
			appendStringInfoString(ctx->out, tableName);
			printTuple(ctx, tupdesc, change->data.tp.newtuple, false, tableName);
		}
		break;
		case REORDER_BUFFER_CHANGE_UPDATE:
		{
			appendStringInfoString(ctx->out, "U ");
			appendStringInfoString(ctx->out, tableName);
			printTuple(ctx, tupdesc, change->data.tp.newtuple, false, tableName);
		}
		break;
		case REORDER_BUFFER_CHANGE_DELETE:
		{
			appendStringInfoString(ctx->out, "D ");
			appendStringInfoString(ctx->out, tableName);
			printTuple(ctx, tupdesc, change->data.tp.oldtuple, true, tableName);
		}
		break;
		default:
			Assert(false);
		}
		MemoryContextSwitchTo(old);
	}
	OutputPluginWrite(ctx, true);
	MemoryContextReset(data->context);
}

static void decode_truncate(LogicalDecodingContext* ctx,
	ReorderBufferTXN* txn, int nrelations,
	Relation relations[], ReorderBufferChange* change)
{
	int i;
	DecodingData* data = ctx->output_plugin_private;
	if (data->skip_change)
		return;

	printTransaction(data, ctx, txn);
	{
		MemoryContext old = MemoryContextSwitchTo(data->context);

		OutputPluginPrepareWrite(ctx, true);

		appendStringInfoString(ctx->out, "T ");

		for (i = 0; i < nrelations; i++)
		{
			if (i > 0)
				appendStringInfoString(ctx->out, ", ");

			appendStringInfoString(ctx->out,
				RelationGetRelationName(relations[i]));
		}

		MemoryContextSwitchTo(old);
	}
	OutputPluginWrite(ctx, true);
	MemoryContextReset(data->context);
}
