/*-------------------------------------------------------------------------
 *
 * win32ntdll.c
 *	  Dynamically loaded Windows NT functions.
 *
 * Portions Copyright (c) 2021-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/port/win32ntdll.c
 *
 *-------------------------------------------------------------------------
 */

#include "c.h"

#include "port/win32ntdll.h"

RtlGetLastNtStatus_t pg_RtlGetLastNtStatus;
RtlNtStatusToDosError_t pg_RtlNtStatusToDosError;

#if WINVER >= _WIN32_WINNT_WIN8
NtFlushBuffersFileEx_t pg_NtFlushBuffersFileEx;
#endif

typedef struct NtDllRoutine
{
	const char *name;
	pg_funcptr_t *address;
} NtDllRoutine;

static const NtDllRoutine routines[] = {
	{"RtlGetLastNtStatus", (pg_funcptr_t *) &pg_RtlGetLastNtStatus},
	{"RtlNtStatusToDosError", (pg_funcptr_t *) &pg_RtlNtStatusToDosError},
#if WINVER >= _WIN32_WINNT_WIN8
	{"NtFlushBuffersFileEx", (pg_funcptr_t *) &pg_NtFlushBuffersFileEx}
#endif
};

static bool initialized;

int
initialize_ntdll(void)
{
	HMODULE		module;

	if (initialized)
		return 0;

	if (!(module = LoadLibraryEx("ntdll.dll", NULL, 0)))
	{
		_dosmaperr(GetLastError());
		return -1;
	}

	for (int i = 0; i < lengthof(routines); ++i)
	{
		pg_funcptr_t address;

		address = (pg_funcptr_t) GetProcAddress(module, routines[i].name);
		if (!address)
		{
			_dosmaperr(GetLastError());
			FreeLibrary(module);

			return -1;
		}

		*(pg_funcptr_t *) routines[i].address = address;
	}

	initialized = true;

	return 0;
}
