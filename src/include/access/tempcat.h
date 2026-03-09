#ifndef TEMPCAT_H
#define TEMPCAT_H
#include "postgres.h"

#include "access/genam.h"
#include "access/relscan.h"
#include "access/skey.h"
#include "executor/tuptable.h"
#include "utils/rel.h"

extern bool enable_temp_memory_catalog;

typedef struct TempCatScanData TempCatScanData;

extern void      temp_catalog_init(void);
extern void      temp_catalog_insert(Relation relation, HeapTuple htup);
extern void      temp_catalog_delete(Relation relation, ItemPointer ptr);
extern void      temp_catalog_update(Relation relation, ItemPointer ptr, HeapTuple htup);
extern void      temp_catalog_update_inplace(Relation relation, HeapTuple htup);
extern TempCatScanData* temp_catalog_beginscan(Relation rel, int nkeys, ScanKey key);
extern void      temp_catalog_endscan(TempCatScanData* scan);
extern HeapTuple temp_catalog_getnext(TempCatScanData* scan, BufferHeapTupleTableSlot* bslot);
extern bool      temp_catalog_is_fetched(TempCatScanData* scan);

extern ItemPointerData temp_catalog_tupmap_assign  (ItemPointer ptr, void* data);
extern bool            temp_catalog_tupmap_unassign(ItemPointer ptr, void* data);
extern void*           temp_catalog_tupmap_get     (ItemPointer ptr);
#endif