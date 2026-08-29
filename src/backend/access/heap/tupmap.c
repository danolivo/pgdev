#include "postgres.h"
#include "storage/itemptr.h"
#include "lib/rbtree.h"
#include "access/tempcat.h"

typedef struct MapItem{
	RBTNode         node;
	ItemPointerData pointer;
	void*           data;
}MapItem;

static RBTree*  tree;
static uint64_t counter;
static bool     overwrite = false;

#define COUNTER_MAX ( ((((uint64_t)1)<<16)-1) * (((uint64_t)1)<<32)  )

static int64_t
ItemPointerToInt(ItemPointer ptr)
{
	return (((int64_t)(ptr->ip_posid-1)) << 32) | (ptr->ip_blkid.bi_hi<<16) | ptr->ip_blkid.bi_lo;
}

static ItemPointerData
IntToItemPointer(int64_t i)
{
	ItemPointerData ret;
	ret.ip_posid = (i >> 32) + 1;
	ret.ip_blkid.bi_hi = i >>16;
	ret.ip_blkid.bi_lo = i;
	return ret;
}

static int
tupmap_rbt_compare(const RBTNode *a, const RBTNode *b, void *arg)
{
	MapItem* aItem = (MapItem*)a;
	MapItem* bItem = (MapItem*)b;

	return ItemPointerToInt(&aItem->pointer) - ItemPointerToInt(&bItem->pointer);
}

static void
tupmap_rbt_combine(RBTNode *existing, const RBTNode *newdata, void *arg)
{
	if (overwrite)
		((MapItem*)existing)->data =  ((MapItem*)newdata)->data;
}

static RBTNode*
tupmap_rbt_alloc(void *arg)
{
	return (RBTNode*)palloc_object(MapItem);
}

static void
tupmap_rbt_free(RBTNode *x, void *arg)
{
	pfree(x);
}


ItemPointerData
temp_catalog_tupmap_assign(ItemPointer ptr, void* data)
{
	if (!tree)
		tree = rbt_create( sizeof(MapItem), tupmap_rbt_compare, tupmap_rbt_combine, tupmap_rbt_alloc, tupmap_rbt_free, NULL, NULL);

	for(;;){
		bool     isNew;
		MapItem  newItem;
		MapItem* node;
		newItem.data = data;

		if (ptr){
			newItem.pointer = *ptr;
			overwrite = true;
		}else{
			if (unlikely(!counter))
				counter = 1;
			newItem.pointer = IntToItemPointer(counter);
			counter++;
			if (unlikely(counter >= COUNTER_MAX))
				counter = 0;

			overwrite = false;
		}

		node = (MapItem*)rbt_insert(tree, (RBTNode*)&newItem, &isNew);
		if(!isNew && !overwrite){
			continue;
		}

		return node->pointer;
	}
}


bool
temp_catalog_tupmap_unassign(ItemPointer ptr, void* data)
{
	MapItem searchItem;
	MapItem* item;

	if (!tree)
		return false;

	searchItem.pointer = *ptr;
	item = (MapItem*)rbt_find(tree, (RBTNode*)&searchItem);
	if (!item)
		return false;

	if (item->data != data)
		return false;

	rbt_delete(tree, (RBTNode*)item);
	return true;
}


void*
temp_catalog_tupmap_get(ItemPointer ptr)
{
	MapItem searchItem;
	MapItem* item;

	if (!tree)
		return false;

	searchItem.pointer = *ptr;
	item = (MapItem*)rbt_find(tree, (RBTNode*)&searchItem);
	if (!item)
		return NULL;

	return item->data;
}