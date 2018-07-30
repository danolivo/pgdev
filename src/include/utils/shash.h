/*
 * shash.h
 *
 *  Created on: 27 июл. 2018 г.
 *      Author: andrey
 */

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

//typedef	unsigned long int uint64;
//typedef	unsigned char uint8;
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

extern SHTAB* SHASH_Create(SHTABCTL shctl);
extern void SHASH_Clean(SHTAB* shtab);
extern void SHASH_Destroy(SHTAB* shtab);
extern uint64 SHASH_Entries(SHTAB* shtab);
extern void SHASH_SeqReset(SHTAB* shtab);
extern void* SHASH_SeqNext(SHTAB* shtab);
extern void* SHASH_Search(SHTAB* shtab, void *keyPtr, SHASHACTION action, bool *foundPtr);
extern uint64 DefaultHashValueFunc(void *key, uint64 size, uint64 base);

#endif /* SHASH_H_ */
