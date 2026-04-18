#include "postgres.h"

#include "access/skey.h"
#include "access/table.h"
#include "access/tempcat.h"
#include "access/xact.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "common/hashfn.h"
#include "lib/rbtree.h"
#include "nodes/execnodes.h"
#include "pgstat.h"
#include "utils/catcache.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/typcache.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

typedef struct TupleItem{
	dlist_node node;
	dlist_node stageCreatedNode;
	dlist_node stageDeletedNode;
	HeapTuple  tuple;
	Datum     *values;
	bool      *nulls;
} TupleItem;


typedef struct TreeItem{
	RBTNode    node;
	dlist_head tuples; /* TuplePointerItem::node */
}TreeItem;

typedef struct TuplePointerItem{
	dlist_node node;
	dlist_node deletionNode;
	TupleItem* tupleItem;
	TreeItem*  owner;
}TuplePointerItem;

typedef struct RelationItem{
	dlist_node node;
	Oid        relid;
	dlist_head indexes; /* IndexItem::node */
	dlist_head indexesScheduledForDeletion; /* IndexItem::node */
	dlist_head allTuples; /* TupleItem::node */
	dlist_head unstagedCreatedTuples; /* TupleItem::stageCreatedNode */
	dlist_head unstagedDeletedTuples; /* TupleItem::stageDeletedNode */
	dlist_head stagedCreatedTuples; /* TupleItem::stageCreatedNode */
	dlist_head stagedDeletedTuples; /* TupleItem::stageDeletedNode */
} RelationItem;

typedef struct IndexItem{
	dlist_node     node;
	RelationItem*  rel;
	int            nkeys;
	AttrNumber     attrNumbers[INDEX_MAX_KEYS];
	FmgrInfo       keyCmpFuncs[INDEX_MAX_KEYS];
	Oid            keyCollations[INDEX_MAX_KEYS];
	RBTree*        tree; /* TreeItem::node */
	dlist_head     scans; /* TempCatScanData::node */
	dlist_head     tuplePointersScheduledForDeletion; /* TuplePointerItem::deletionNode */
} IndexItem;

struct TempCatScanData{
	dlist_node     node;
	ScanKey        key;
	int            nkeys;
	bool           started;
	bool           finished;
	RelationItem*  rel;
	IndexItem*     index;
	RBTreeIterator treeIter;
	dlist_iter     listIter;
	RBTNode*       endNode;
	bool           continuous;
	uint64_t       count;
};


static dlist_head temp_rels; /* RelationItem::node */
static bool initialized = false;
bool enable_temp_memory_catalog = false;


static void
rbt_destroy(RBTree* tree)
{
	for(;;){
		RBTNode* node = rbt_leftmost(tree);
		if(node)
		{
			TreeItem* item = (TreeItem*)node;

			while(!dlist_is_empty(&item->tuples))
			{
				dlist_node* keyNode = dlist_pop_head_node(&item->tuples);
				TuplePointerItem* keyItem = dlist_container(TuplePointerItem,node,keyNode);

				if(!dlist_node_is_detached(&keyItem->deletionNode))
					dlist_delete(&keyItem->deletionNode);

				pfree(keyItem);
			}

			rbt_delete(tree, node);
			continue;
		}

		pfree(tree);
		return;
	}
}


static void
LocalInvalidateCatCache(int cacheId, uint32 hashValue, Oid dbId, void *context)
{
	SysCacheInvalidate(cacheId, hashValue);
	CallSyscacheCallbacks(cacheId, hashValue);
}

static void
LocalInvalidateCatCacheTupleNow(Oid relid, HeapTuple tuple, HeapTuple newTuple)
{
	GetTopTransactionId();
	GetCurrentCommandId(true);
	InvalidateCatalogSnapshot();

	if (!RelationInvalidatesSnapshotsOnly(relid)){
		PrepareToInvalidateCacheTuple(relid, tuple, newTuple, LocalInvalidateCatCache, NULL);
	}
}


static void
LocalInvalidateCatCacheTuple(Relation rel, HeapTuple tuple, HeapTuple newTuple)
{
	GetTopTransactionId();
	GetCurrentCommandId(true);
	CacheInvalidateHeapTuple(rel, tuple, newTuple);
	AcceptInvalidationMessages();
}


static TreeItem*
find_index_tree_item(IndexItem* indexItem, TupleItem* tupleItem, int cmp)
{

	TuplePointerItem      keyToSearch;
	TreeItem     nodeToSearch;

	keyToSearch.tupleItem = tupleItem;

	dlist_init(&nodeToSearch.tuples);
	dlist_push_tail(&nodeToSearch.tuples, &keyToSearch.node);

	if(cmp < 0)
		return (TreeItem*)rbt_find_less(indexItem->tree, (RBTNode*)&nodeToSearch, false);
	else if(cmp > 0)
		return (TreeItem*)rbt_find_great(indexItem->tree, (RBTNode*)&nodeToSearch, false);
	else
		return (TreeItem*)rbt_find(indexItem->tree, (RBTNode*)&nodeToSearch);
}




static void
delete_pending_key_items(IndexItem* idxItem)
{
	while (!dlist_is_empty(&idxItem->tuplePointersScheduledForDeletion))
	{
		dlist_node* node = dlist_pop_head_node(&idxItem->tuplePointersScheduledForDeletion);
		TuplePointerItem*  tuplePointerItem = (TuplePointerItem*) dlist_container(TuplePointerItem, deletionNode, node);
		TreeItem* owner = tuplePointerItem->owner;
		dlist_delete(&tuplePointerItem->node);
		pfree(tuplePointerItem);

		if (dlist_is_empty(&owner->tuples))
			rbt_delete(idxItem->tree, &owner->node);
	}
}


static void
cleanup( RelationItem* relEntry )
{
	dlist_iter         indexIter;
	dlist_mutable_iter miter;

	dlist_foreach(indexIter, &relEntry->indexes)
	{
		IndexItem* index = (IndexItem*) dlist_container(IndexItem, node, indexIter.cur);
		if (!dlist_is_empty(&index->scans))
			continue;

		delete_pending_key_items(index);
	}

	dlist_foreach_modify(miter, &relEntry->indexesScheduledForDeletion)
	{
		IndexItem* index = (IndexItem*) dlist_container(IndexItem, node, miter.cur);
		
		/* Don't delete (yet) indexes that are currently used for scans*/
		if (!dlist_is_empty(&index->scans))
			continue;

		rbt_destroy(index->tree);
		dlist_delete(&index->node);
		pfree(index);
	}
}


static void
insert_tuple_entry_for_index(IndexItem* idxItem, TupleItem* tupItem)
{
	TuplePointerItem* tuplePointer;
	TreeItem tempTreeItem;
	bool isNew;

	tuplePointer = palloc_object(TuplePointerItem);
	dlist_node_init(&tuplePointer->deletionNode);
	tuplePointer->tupleItem = tupItem;
	tuplePointer->owner = NULL;

	dlist_init(&tempTreeItem.tuples);
	dlist_push_tail(&tempTreeItem.tuples, &tuplePointer->node);
	rbt_insert(idxItem->tree, &tempTreeItem.node, &isNew);

	// if (isNew)
	// {
	// 	dlist_init(&tuplePointer->owner->tuples);
	// 	dlist_push_tail(&tuplePointer->owner->tuples, &tuplePointer->node);
	// }
}


static TupleItem*
create_tuple_item(HeapTuple htup, TupleDesc tupdesc, ItemPointer ptr)
{
	int attributeIndex;
	TupleItem* entry = palloc_object(TupleItem);
	entry->values = palloc_array(Datum,tupdesc->natts);
	entry->nulls  = palloc_array(bool, tupdesc->natts);
	entry->tuple  = heap_copytuple(htup);
	dlist_node_init(&entry->stageCreatedNode);
	dlist_node_init(&entry->stageDeletedNode);

	for(attributeIndex=0; attributeIndex < tupdesc->natts ;attributeIndex++)
		entry->values[attributeIndex] = heap_getattr(entry->tuple, attributeIndex+1, tupdesc, &entry->nulls[attributeIndex]);

	return entry;
}


static void
add_tuple_entry(RelationItem* relItem, TupleItem* tupItem, bool addToUnstaged)
{
	dlist_iter iter;

	dlist_foreach(iter, &relItem->indexes)
	{
		IndexItem* idxItem = dlist_container(IndexItem, node, iter.cur);
		insert_tuple_entry_for_index(idxItem, tupItem);
	}

	dlist_push_head(&relItem->allTuples, &tupItem->node);

	if (addToUnstaged)
		dlist_push_tail(&relItem->unstagedCreatedTuples, &tupItem->stageCreatedNode);

	tupItem->tuple->t_self = temp_catalog_tupmap_assign(NULL, tupItem);
}


static void
remove_tuple_entry(RelationItem* relItem, TupleItem* tupItem, bool staging)
{
	dlist_iter indexIter;

	dlist_foreach(indexIter, &relItem->indexes)
	{
		dlist_iter tupIter;
		IndexItem* idxItem = (IndexItem*) dlist_container(IndexItem, node, indexIter.cur);
		TreeItem*  node = find_index_tree_item(idxItem, tupItem, 0);
		if (node)
		{
			dlist_foreach(tupIter, &node->tuples)
			{
				TuplePointerItem* key = (TuplePointerItem*) dlist_container(TuplePointerItem, node, tupIter.cur);
				if (key->tupleItem == tupItem)
				{
					if (dlist_node_is_detached(&key->deletionNode))
						dlist_push_tail(&idxItem->tuplePointersScheduledForDeletion, &key->deletionNode);
					break;
				}
			}
		}

		if (!staging)
			delete_pending_key_items(idxItem);
	}

	dlist_delete(&tupItem->node);
	dlist_node_init(&tupItem->node);

	if (staging)
		dlist_push_tail(&relItem->unstagedDeletedTuples, &tupItem->stageDeletedNode);

	temp_catalog_tupmap_unassign(&tupItem->tuple->t_self, tupItem);
}



static RBTNode*
rbt_alloc(void *arg)
{
	return &(palloc_object(TreeItem)->node);
}


static void
rbt_free(RBTNode *x, void *arg)
{
	pfree(x);
}


static int
rbt_compare(TreeItem* a, TreeItem* b, IndexItem* index)
{
	TuplePointerItem* aKey = dlist_head_element(TuplePointerItem,node,&a->tuples);
	TuplePointerItem* bKey = dlist_head_element(TuplePointerItem,node,&b->tuples);
	
	for (int keyIndex=0; keyIndex < index->nkeys; keyIndex++)
	{
		int attributeIndex = index->attrNumbers[keyIndex] - 1;
		int cmp;

		cmp = DatumGetInt32(DirectFunctionCall2Coll(index->keyCmpFuncs[keyIndex].fn_addr, index->keyCollations[keyIndex], aKey->tupleItem->values[attributeIndex], bKey->tupleItem->values[attributeIndex]));
		if (cmp)
			return cmp;
	}
	return 0;
}

static void
rbt_combine(TreeItem* existing, TreeItem* newdata, IndexItem* index)
{
	while (!dlist_is_empty(&newdata->tuples))
	{
		dlist_node*        newTuplePointerNode = dlist_pop_head_node(&newdata->tuples);
		TuplePointerItem*  newTuplePointer     = dlist_container(TuplePointerItem, node, newTuplePointerNode);
		dlist_push_tail(&existing->tuples, &newTuplePointer->node);
		newTuplePointer->owner = existing;
	}
}


static void
rbt_fix(RBTNode *x, void *arg)
{
	dlist_iter tupIter;
	TreeItem* item = (TreeItem*)x;
	dlist_node* head = &item->tuples.head;

	/* Fix old head element address. */
	if (head->next == head->prev && head->next == head->next->next)
	{
		head->next = head;
		head->prev = head;
	}
	else
	{
		head->next->prev = head;
		head->prev->next = head;
	}
	
	dlist_foreach(tupIter, &item->tuples)
	{
		TuplePointerItem* key = (TuplePointerItem*) dlist_container(TuplePointerItem, node, tupIter.cur);
		key->owner = item;
	}
}


static bool
compare_keyItem_with_scanKey(IndexItem* indexItem, TuplePointerItem* keyItem, ScanKey keys, int nkeys)
{
	for (int keyIndex=0; keyIndex < nkeys; keyIndex++)
	{
		int attributeIndex = indexItem->attrNumbers[keyIndex] - 1;
		int cmp;
		
		cmp = DatumGetInt32(DirectFunctionCall2Coll(indexItem->keyCmpFuncs[keyIndex].fn_addr, indexItem->keyCollations[keyIndex], keyItem->tupleItem->values[attributeIndex], keys[keyIndex].sk_argument));
		
		switch(keys[keyIndex].sk_strategy){
		case BTLessStrategyNumber:
			if (cmp >= 0)
				return false;
			break;
		case BTLessEqualStrategyNumber:
			if (cmp > 0)
				return false;
			break;
		case BTEqualStrategyNumber:
			if (cmp != 0)
				return false;
			break;
		case BTGreaterEqualStrategyNumber:
			if (cmp < 0)
				return false;
			break;
		case BTGreaterStrategyNumber:
			if (cmp <= 0)
				return false;
			break;

		default:
				return false;
		}
	}

	return true;
}


static IndexItem*
get_index_entry(RelationItem* relEntry, Relation relation, AttrNumber* attrNumbers, int numKeys)
{
	IndexItem*         indexEntry = NULL;
	int                keyIndex;
	dlist_iter         iter;
	dlist_mutable_iter indexIter;

	dlist_foreach_modify(indexIter, &relEntry->indexes)
	{
		IndexItem* index = (IndexItem*) dlist_container(IndexItem, node, indexIter.cur);
		if (index->nkeys >= numKeys && memcmp(index->attrNumbers, attrNumbers, sizeof(AttrNumber) * numKeys)==0)
			return index;

		if (index->nkeys < numKeys && memcmp(index->attrNumbers, attrNumbers, sizeof(AttrNumber) * index->nkeys)==0)
		{
			dlist_delete(&index->node);
			dlist_push_tail(&relEntry->indexesScheduledForDeletion, &index->node);
		}
	}

	indexEntry = palloc_object(IndexItem);
	indexEntry->rel           = relEntry;
	indexEntry->nkeys         = numKeys;
	indexEntry->tree          = rbt_create( sizeof(TreeItem), (rbt_comparator)rbt_compare, (rbt_combiner)rbt_combine, rbt_alloc, rbt_free, rbt_fix, indexEntry);
	dlist_init(&indexEntry->scans);
	dlist_init(&indexEntry->tuplePointersScheduledForDeletion);

	for(keyIndex=0; keyIndex < numKeys; keyIndex++ )
	{
		TypeCacheEntry* typeEntry;
		FormData_pg_attribute* attribute = TupleDescAttr(relation->rd_att, attrNumbers[keyIndex]-1);

		typeEntry = lookup_type_cache(attribute->atttypid, TYPECACHE_CMP_PROC_FINFO);
		Assert(OidIsValid(typeEntry->cmp_proc_finfo.fn_oid));

		indexEntry->keyCmpFuncs[keyIndex]   = typeEntry->cmp_proc_finfo;
		indexEntry->keyCollations[keyIndex] = attribute->attcollation;
		indexEntry->attrNumbers[keyIndex]   = attribute->attnum;
	}

	dlist_foreach(iter, &relEntry->allTuples)
	{
		TupleItem* tupleEntry = dlist_container(TupleItem, node, iter.cur);
		insert_tuple_entry_for_index(indexEntry, tupleEntry);
	}

	dlist_push_tail(&relEntry->indexes, &indexEntry->node);
	
	return indexEntry;
}


static RelationItem*
find_relation_entry(Relation rel)
{
	dlist_iter iter;
	dlist_foreach(iter, &temp_rels)
	{
		RelationItem* item = dlist_container(RelationItem, node, iter.cur);
		if (item->relid == rel->rd_rel->oid)
			return item;
	}
	return NULL;
}


static RelationItem*
get_relation_entry(Relation relation)
{
	RelationItem* relEntry = find_relation_entry(relation);
	if (!relEntry)
	{
		relEntry = palloc_object(RelationItem);
		relEntry->relid = relation->rd_rel->oid;
		dlist_init(&relEntry->indexes);
		dlist_init(&relEntry->allTuples);
		dlist_init(&relEntry->indexesScheduledForDeletion);
		dlist_init(&relEntry->unstagedCreatedTuples);
		dlist_init(&relEntry->unstagedDeletedTuples);
		dlist_init(&relEntry->stagedCreatedTuples);
		dlist_init(&relEntry->stagedDeletedTuples);
		dlist_push_tail(&temp_rels, &relEntry->node);
	}

	return relEntry;
}


void
temp_catalog_insert(Relation relation, HeapTuple htup)
{
	RelationItem* relEntry = NULL;
	MemoryContext oldctx;
	TupleItem* item;

	oldctx = MemoryContextSwitchTo(TopMemoryContext);

	relEntry = get_relation_entry(relation);

	item = create_tuple_item(htup, relation->rd_att, NULL);
	add_tuple_entry(relEntry, item, true);

	LocalInvalidateCatCacheTuple(relation, item->tuple, NULL);
	htup->t_self = item->tuple->t_self;

	MemoryContextSwitchTo(oldctx);
}


void
temp_catalog_delete(Relation relation, ItemPointer ptr)
{
	RelationItem* relEntry;
	TupleItem* tupleEntry;

	relEntry = find_relation_entry(relation);
	if (!relEntry)
		return;

	tupleEntry = temp_catalog_tupmap_get(ptr);
	if (!tupleEntry)
		return;

	remove_tuple_entry(relEntry, tupleEntry, true);

	LocalInvalidateCatCacheTuple(relation, tupleEntry->tuple, NULL);

	pgstat_count_heap_delete(relation);

	cleanup(relEntry);
}


void
temp_catalog_update(Relation relation, ItemPointer ptr, HeapTuple htup)
{
	RelationItem* relEntry = NULL;
	TupleItem* oldTupleEntry;
	TupleItem* newTupleEntry;
	MemoryContext oldctx;

	relEntry = find_relation_entry(relation);
	if (!relEntry)
		return;

	oldctx = MemoryContextSwitchTo(TopMemoryContext);

	oldTupleEntry = (TupleItem*)temp_catalog_tupmap_get(ptr);

	remove_tuple_entry(relEntry, oldTupleEntry, true);

	newTupleEntry = create_tuple_item(htup, relation->rd_att, ptr);
	add_tuple_entry(relEntry, newTupleEntry, true);

	LocalInvalidateCatCacheTuple(relation, oldTupleEntry->tuple, newTupleEntry->tuple);

	cleanup(relEntry);

	MemoryContextSwitchTo(oldctx);
}


void
temp_catalog_update_inplace(Relation relation, HeapTuple htup)
{
	temp_catalog_update(relation, &htup->t_self, htup);
}


struct TempCatScanData*
temp_catalog_beginscan(Relation relation, int nkeys, ScanKey key)
{
	IndexItem*    indexEntry = NULL;
	RelationItem* relEntry = NULL;
	AttrNumber    attrNumbers[INDEX_MAX_KEYS];
	MemoryContext oldctx;
	int           strategy;
	TempCatScanData* scan;
	int           walkDir;
	TreeItem*     lastItem;
	TreeItem*     endItem;
	bool          continuous;
	static bool   nested = false;

	if (nested)
		return NULL;
	
	nested = true;

	relEntry = find_relation_entry(relation);
	if (!relEntry)
	{
		nested = false;
		return NULL;
	}

	for (int c=0; c < nkeys; c++)
		attrNumbers[c] = key[c].sk_attno;

	oldctx = MemoryContextSwitchTo(TopMemoryContext);

	indexEntry = get_index_entry(relEntry, relation, attrNumbers, nkeys);

	scan = palloc_object(TempCatScanData);
	scan->rel          = relEntry;
	scan->index        = indexEntry;
	scan->key          = key;
	scan->nkeys        = nkeys;
	scan->started      = false;
	scan->finished     = false;
	scan->listIter.cur = NULL;
	scan->listIter.end = NULL;
	scan->count = 0;

	if (nkeys)
	{
		strategy = key[nkeys-1].sk_strategy;
		for (int c=0; c < nkeys-1; c++)
		{
			if (key[c].sk_strategy != BTEqualStrategyNumber)
			{
				strategy = InvalidStrategy;
				break;
			}
		}
	}
	else
		strategy = InvalidStrategy;

	if (strategy != BTEqualStrategyNumber &&
	    strategy != BTGreaterStrategyNumber &&
	    strategy != BTGreaterEqualStrategyNumber &&
	    strategy != BTLessStrategyNumber &&
	    strategy != BTLessEqualStrategyNumber)
	{
		walkDir    = LeftRightWalk;
		lastItem   = NULL;
		endItem    = NULL;
		continuous = false;
	}
	else
	{
		TupleItem     tempTuple;
		Datum*        attrValues;
		int           maxAtt=0;

		for (int c=0; c < nkeys; c++)
			maxAtt = Max(maxAtt,attrNumbers[c]);

		attrValues = palloc_array(Datum,maxAtt);
		for (int c=0; c < nkeys; c++)
			attrValues[attrNumbers[c]-1] = key[c].sk_argument;

		tempTuple.values = attrValues;

		continuous = true;

		if (strategy == BTEqualStrategyNumber)
		{
			walkDir  = LeftRightWalk;
			lastItem = find_index_tree_item(indexEntry, &tempTuple, -1);
			endItem  = find_index_tree_item(indexEntry, &tempTuple, +1);
		}
		else if (strategy == BTGreaterStrategyNumber || strategy == BTGreaterEqualStrategyNumber)
		{
			walkDir  = LeftRightWalk;
			lastItem = find_index_tree_item(indexEntry, &tempTuple, -1);
			endItem  = NULL;
		}
		else
		{
			Assert(strategy == BTLessStrategyNumber ||
				   strategy == BTLessEqualStrategyNumber);
			walkDir  = RightLeftWalk;
			lastItem = find_index_tree_item(indexEntry, &tempTuple, +1);
			endItem  = NULL;
		}

		pfree(attrValues);
	}

	rbt_begin_iterate(indexEntry->tree, walkDir, &scan->treeIter);
	scan->treeIter.last_visited = &lastItem->node;
	scan->endNode               = &endItem->node;
	scan->continuous            = continuous;

	dlist_push_tail(&indexEntry->scans, &scan->node);

	MemoryContextSwitchTo(oldctx);

	nested = false;

	return scan;
}


void
temp_catalog_endscan(TempCatScanData* scan)
{
	if (!scan)
		return;
	
	dlist_delete(&scan->node);
	cleanup(scan->rel);
	pfree(scan);
}


HeapTuple
temp_catalog_getnext(TempCatScanData* scan, BufferHeapTupleTableSlot* bslot)
{
	if (!scan || scan->finished)
		return NULL;

	for(;;){
		TuplePointerItem* key;

		while (scan->listIter.cur == scan->listIter.end)
		{
			TreeItem* nextNode = (TreeItem*)rbt_iterate(&scan->treeIter);
			if (!nextNode)
			{
				scan->finished = true;
				return NULL;
			}

			scan->listIter.end = &nextNode->tuples.head;
			scan->listIter.cur = scan->listIter.end->next ? scan->listIter.end->next : scan->listIter.end;
		}

		key = dlist_container(TuplePointerItem,node,scan->listIter.cur);

		scan->listIter.cur = scan->listIter.cur->next;

		scan->count++;

		if (!dlist_node_is_detached(&key->deletionNode))
			continue;
	
		if (!compare_keyItem_with_scanKey(scan->index, key, scan->key, scan->nkeys))
		{
			if (scan->continuous)
			{
				scan->finished = true;
				return NULL;
			}

			continue;
		}

		scan->started = true;
		bslot->base.tuple = key->tupleItem->tuple;
		return key->tupleItem->tuple;
	}
}


bool
temp_catalog_is_fetched(TempCatScanData* scan)
{
	return scan && scan->started && !scan->finished;
}


static void
dlist_move(dlist_head *dst, dlist_head *src)
{
	if (dst->head.next == NULL)	/* convert NULL header to circular */
		dlist_init(dst);

	if (!dlist_is_empty(src))
	{
		dlist_node* head = dlist_head_node(src);
		dlist_node* tail = dlist_tail_node(src);
		tail->next = &dst->head;
		head->prev = dst->head.prev;
		dst->head.prev->next = head;
		dst->head.prev = tail;
		dlist_init(src);
	}
}


static void
free_tuple(TupleItem* item)
{
	pfree(item->values);
	pfree(item->nulls);
	heap_freetuple(item->tuple);
	pfree(item);
}


static void
free_deleted_tuples(dlist_head* list)
{	
	while (!dlist_is_empty(list))
	{
		dlist_node* node = dlist_pop_head_node(list);
		TupleItem* tupleItem = dlist_container(TupleItem, stageDeletedNode, node);
		free_tuple(tupleItem);
	}
	
	dlist_init(list);
}


static void
revert_created_tuples(RelationItem* relEntry, dlist_head* list)
{
	while (!dlist_is_empty(list))
	{
		dlist_node* node = dlist_pop_head_node(list);
		TupleItem* tupleItem = dlist_container(TupleItem, stageCreatedNode, node);

		if (dlist_node_is_detached(&tupleItem->stageDeletedNode)){
			remove_tuple_entry(relEntry, tupleItem, false);
		}else{
			dlist_delete(&tupleItem->stageDeletedNode);
		}

		LocalInvalidateCatCacheTupleNow(relEntry->relid, tupleItem->tuple, NULL);
		
		free_tuple(tupleItem);
	}
}


static void
revert_deleted_tuples(RelationItem* relEntry, dlist_head* list)
{
	while (!dlist_is_empty(list))
	{
		dlist_node* node = dlist_pop_head_node(list);
		TupleItem* tupleItem = dlist_container(TupleItem, stageDeletedNode, node);

		dlist_node_init(&tupleItem->stageDeletedNode);

		add_tuple_entry(relEntry, tupleItem, false);

		LocalInvalidateCatCacheTupleNow(relEntry->relid, tupleItem->tuple, NULL);
	}
}


static void
detach_created_tuples(dlist_head* list)
{
	while (!dlist_is_empty(list))
	{
		dlist_node* node = dlist_pop_head_node(list);
		dlist_node_init(node);
	}
}


static void
temp_cat_xact_cb(XactEvent event, void *arg)
{
	dlist_iter iter;
	if (event == XACT_EVENT_PRE_COMMIT || event == XACT_EVENT_PARALLEL_PRE_COMMIT)
	{
		dlist_foreach(iter, &temp_rels)
		{
			RelationItem* item = dlist_container(RelationItem, node, iter.cur);
			detach_created_tuples(&item->unstagedCreatedTuples);
			detach_created_tuples(&item->stagedCreatedTuples);
			free_deleted_tuples(&item->unstagedDeletedTuples);
			free_deleted_tuples(&item->stagedDeletedTuples);
			cleanup(item);
		}
	}
	else if (event == XACT_EVENT_PRE_ABORT || event == XACT_EVENT_PARALLEL_PRE_ABORT)
	{
		dlist_foreach(iter, &temp_rels)
		{
			RelationItem* item = dlist_container(RelationItem, node, iter.cur);
			MemoryContext oldctx = MemoryContextSwitchTo(TopMemoryContext);
			revert_created_tuples(item, &item->unstagedCreatedTuples);
			revert_created_tuples(item, &item->stagedCreatedTuples);
			revert_deleted_tuples(item, &item->unstagedDeletedTuples);
			revert_deleted_tuples(item, &item->stagedDeletedTuples);
			cleanup(item);
			MemoryContextSwitchTo(oldctx);
		}
	}
}


static void
temp_cat_subxact_cb(SubXactEvent event, SubTransactionId mySubid,
				   SubTransactionId parentSubid, void *arg)
{
	dlist_iter iter;
	if (event == SUBXACT_EVENT_COMMIT_SUB )
	{
		dlist_foreach(iter, &temp_rels)
		{
			RelationItem* item = dlist_container(RelationItem, node, iter.cur);
			dlist_move(&item->stagedCreatedTuples, &item->unstagedCreatedTuples);
			dlist_move(&item->stagedDeletedTuples, &item->unstagedDeletedTuples);
		}
	}
	else if (event == SUBXACT_EVENT_PRE_ABORT_SUB)
	{
		dlist_foreach(iter, &temp_rels)
		{
			RelationItem* item = dlist_container(RelationItem, node, iter.cur);
			MemoryContext oldctx = MemoryContextSwitchTo(TopMemoryContext);
			revert_created_tuples(item, &item->unstagedCreatedTuples);
			revert_deleted_tuples(item, &item->unstagedDeletedTuples);
			cleanup(item);
			MemoryContextSwitchTo(oldctx);
		}
	}
}


void
temp_catalog_init(void)
{
	if (!initialized)
	{
		dlist_init(&temp_rels);
		RegisterSubXactCallback(temp_cat_subxact_cb, NULL);
		RegisterXactCallback(temp_cat_xact_cb, NULL);
		initialized = true;
	}
}
