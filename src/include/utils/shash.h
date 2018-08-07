#ifndef SHASH_H_
#define SHASH_H_

/* hash_search operations */
typedef enum
{
	SHASH_FIND,
	SHASH_ENTER,
	SHASH_REMOVE
} SHASHACTION;

/* States of a hash table element */
typedef enum
{
	SHASH_NUSED = 0,
	SHASH_USED,
	SHASH_REMOVED
} HESTATE;

typedef uint64 (*SHashValueFunc) (void *key, uint64 size, uint64 base);
typedef bool (*CompareFunc) (void* bucket1, void* bucket2);

typedef struct SHTABCTL
{
	uint64			ElementSize;
	uint64			KeySize;
	uint64			ElementsMaxNum;
	float			FillFactor;
	SHashValueFunc	HashFunc;
	CompareFunc		CompFunc;
} SHTABCTL;

typedef struct SHTAB
{
	SHTABCTL		Header;
	char			*Elements;
	uint64			nElements;
	HESTATE			*state;		/* State of an element of hash table*/
	uint64			SeqScanCurElem;
	uint64			HTableSize;
} SHTAB;

typedef struct SHTAB*	PSHTAB;

extern PSHTAB SHASH_Create(SHTABCTL shctl);
extern void SHASH_Clean(PSHTAB shtab);
extern void SHASH_Destroy(PSHTAB shtab);
extern uint64 SHASH_Entries(PSHTAB shtab);
extern void SHASH_SeqReset(PSHTAB shtab);
extern void* SHASH_SeqNext(PSHTAB shtab);
extern void* SHASH_Search(PSHTAB shtab, void *keyPtr, SHASHACTION action, bool *foundPtr);
extern uint64 DefaultHashValueFunc(void *key, uint64 size, uint64 base);

#endif /* SHASH_H_ */
