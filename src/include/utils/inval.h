/*-------------------------------------------------------------------------
 *
 * inval.h
 *	  POSTGRES cache invalidation dispatcher definitions.
 *
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/inval.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef INVAL_H
#define INVAL_H

#include "access/htup.h"
#include "storage/relfilelocator.h"
#include "utils/relcache.h"

extern PGDLLIMPORT int debug_discard_caches;

typedef void (*SyscacheCallbackFunction) (Datum arg, int cacheid, uint32 hashvalue);
typedef void (*RelcacheCallbackFunction) (Datum arg, Oid relid);
typedef void (*RelSyncCallbackFunction) (Datum arg, Oid relid);


extern void AcceptInvalidationMessages(void);

extern void AtEOXact_Inval(bool isCommit);

extern void PreInplace_Inval(void);
extern void AtInplace_Inval(void);
extern void ForgetInplace_Inval(void);

extern void AtEOSubXact_Inval(bool isCommit);

extern void PostPrepare_Inval(void);

extern void CommandEndInvalidationMessages(void);

extern void CacheInvalidateHeapTuple(Relation relation,
									 HeapTuple tuple,
									 HeapTuple newtuple);
extern void CacheInvalidateHeapTupleInplace(Relation relation,
											HeapTuple key_equivalent_tuple);

extern void CacheInvalidateCatalog(Oid catalogId);

extern void CacheInvalidateRelcache(Relation relation);

extern void CacheInvalidateRelcacheAll(void);

extern void CacheInvalidateRelcacheByTuple(HeapTuple classTuple);

extern void CacheInvalidateRelcacheByRelid(Oid relid);

extern void CacheInvalidateRelSync(Oid relid);

extern void CacheInvalidateRelSyncAll(void);

extern void CacheInvalidateSmgr(RelFileLocatorBackend rlocator);

extern void CacheInvalidateRelmap(Oid databaseId);

extern void CacheRegisterSyscacheCallback(int cacheid,
										  SyscacheCallbackFunction func,
										  Datum arg);

extern void CacheRegisterRelcacheCallback(RelcacheCallbackFunction func,
										  Datum arg);

extern void CacheRegisterRelSyncCallback(RelSyncCallbackFunction func,
										 Datum arg);

extern void CallSyscacheCallbacks(int cacheid, uint32 hashvalue);

extern void CallRelSyncCallbacks(Oid relid);

extern void InvalidateSystemCaches(void);
extern void InvalidateSystemCachesExtended(bool debug_discard);

extern void LogLogicalInvalidations(void);

/*
 * Hints that operation being performed is related to temporary tables.
 */
extern char temp_table_scope;

#define TEMP_TABLE_SCOPE_NOTEMP 0
#define TEMP_TABLE_SCOPE_SHARED 1
#define TEMP_TABLE_SCOPE_LOCAL  2

/*
 * This is modified PG_TRY/PG_FINALLY/PG_END_TRY block that conditionally sets
 * and restores `temp_table_scope` on error. It's optimized to do not use
 * try/catch mechanism when `isTemp` is false. When entering scope by using
 * `BEGIN_TEMP_TABLE_SCOPE` the previous value of `temp_table_scope` is saved,
 * and new value is set according to `level`. On upon reaching
 * `END_TEMP_TABLE_SCOPE` or exception, the value `temp_table_scope` is
 * restored to saved value. Thus, nesting of scope is possible.
 * 
 * When level `level` is `TEMP_TABLE_SCOPE_LOCAL` (or `BEGIN_TEMP_TABLE_SCOPE_LOCAL`
 * used with non-zero argument), some of shared invalidation messages aren't sent
 * to other sessions. 
 * 
 * When level is `TEMP_TABLE_SCOPE_LOCAL` or `TEMP_TABLE_SCOPE_SHARED`
 * (or `BEGIN_TEMP_TABLE_SCOPE_*` used with non-zero argument) all created WAL
 * records won't issue fsync on commit.
 */
#define BEGIN_TEMP_TABLE_SCOPE(level) \
	do { \
		const char            _temp_scope_level = (level); \
		const bool            _temp_scope_do = (_temp_scope_level != temp_table_scope); \
		bool                  _temp_scope_throw = false; \
		char                  _temp_scope_save_state; \
		sigjmp_buf*           _temp_scope_save_exception_stack = PG_exception_stack; \
		ErrorContextCallback* _temp_scope_save_error_stack; \
		sigjmp_buf            _temp_scope_save_sigjmp_buf; \
		if (_temp_scope_do) \
		{ \
			_temp_scope_save_state = temp_table_scope; \
			_temp_scope_save_error_stack = error_context_stack; \
			if (sigsetjmp(_temp_scope_save_sigjmp_buf, 0) == 0) \
			{ \
				PG_exception_stack = &_temp_scope_save_sigjmp_buf; \
				temp_table_scope = level; \
			} \
			else \
				_temp_scope_throw = true; \
		} \
		if (!_temp_scope_throw) \
		{

#define BEGIN_TEMP_TABLE_SCOPE_LOCAL(isTemp)  BEGIN_TEMP_TABLE_SCOPE( (isTemp) ? TEMP_TABLE_SCOPE_LOCAL  : TEMP_TABLE_SCOPE_NOTEMP )
#define BEGIN_TEMP_TABLE_SCOPE_SHARED(isTemp) BEGIN_TEMP_TABLE_SCOPE( (isTemp) ? TEMP_TABLE_SCOPE_SHARED : TEMP_TABLE_SCOPE_NOTEMP )

#define END_TEMP_TABLE_SCOPE() \
		} \
		PG_exception_stack = _temp_scope_save_exception_stack; \
		if (_temp_scope_do) \
		{ \
			error_context_stack = _temp_scope_save_error_stack; \
			temp_table_scope = _temp_scope_save_state; \
			if (_temp_scope_throw) \
				PG_RE_THROW(); \
		} \
	} while (0)

#define IsTempTableScope()       (temp_table_scope != TEMP_TABLE_SCOPE_NOTEMP)
#define IsLocalTempTableScope()  (temp_table_scope == TEMP_TABLE_SCOPE_LOCAL)
#define IsSharedTempTableScope() (temp_table_scope == TEMP_TABLE_SCOPE_SHARED)

#endif							/* INVAL_H */
