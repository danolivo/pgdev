#include "postgres.h"

#include "autoconfig.h"
#include "utils/palloc.h"
#include "utils/guc.h"

#include "common/fe_memutils.h"
#if defined(_WIN32)
#include <Windows.h>
#elif defined(__unix__) || defined(__unix) || defined(unix) || (defined(__APPLE__) && defined(__MACH__))
#include <unistd.h>
#include <sys/types.h>
#include <sys/param.h>
#if defined(BSD)
#include <sys/sysctl.h>
#endif
#endif


static size_t
GetSystemMemorySize(void)
{
#if defined(__CYGWIN__) || defined(__CYGWIN32__)
	MEMORYSTATUS status;
	status.dwLength = sizeof(status);
	GlobalMemoryStatus( &status );
	return (size_t)status.dwTotalPhys;

#elif defined(_WIN32) || defined(_WIN64)
	MEMORYSTATUSEX status;
	status.dwLength = sizeof(status);
	GlobalMemoryStatusEx( &status );
	return (size_t)status.ullTotalPhys;

#elif defined(CTL_HW) && defined(HW_MEMSIZE)
	int mib[2] = {CTL_HW, HW_MEMSIZE};
	int64_t size = 0;
	size_t len = sizeof(size);
	if (sysctl(mib, 2, &size, &len, NULL, 0) == 0)
		return (size_t)size;

#elif defined(CTL_HW) && defined(HW_PHYSMEM64)
	int mib[2] = {CTL_HW, HW_PHYSMEM64};
	int64_t size = 0;
	size_t len = sizeof(size);
	if (sysctl(mib, 2, &size, &len, NULL, 0) == 0)
		return (size_t)size;

#elif defined(_SC_AIX_REALMEM)
	return (size_t)sysconf(_SC_AIX_REALMEM) * (size_t)1024;

#elif defined(_SC_PHYS_PAGES) && defined(_SC_PAGESIZE)
	return (size_t)sysconf(_SC_PHYS_PAGES) * (size_t)sysconf(_SC_PAGESIZE);

#elif defined(_SC_PHYS_PAGES) && defined(_SC_PAGE_SIZE)
	return (size_t)sysconf(_SC_PHYS_PAGES) * (size_t)sysconf(_SC_PAGE_SIZE);

#elif defined(CTL_HW) && defined(HW_REALMEM)
	int mib[2] = {CTL_HW, HW_REALMEM};

	unsigned int size = 0;
	size_t len = sizeof(size);
	if (sysctl(mib, 2, &size, &len, NULL, 0) == 0)
		return (size_t)size;

#elif defined(CTL_HW) && defined(HW_PYSMEM)
	int mib[2] = {CTL_HW, HW_PYSMEM};
	unsigned int size = 0;
	size_t len = sizeof(size);
	if (sysctl(mib, 2, &size, &len, NULL, 0) == 0)
		return (size_t)size;
#endif

	return 0;
}


static int
GetCoreCount(void)
{
#if defined(_WIN32) || defined(_WIN64)
	SYSTEM_INFO si;
	GetSystemInfo(&si);
	return (int)si.dwNumberOfProcessors;
#elif defined(_SC_NPROCESSORS_ONLN)
	long nprocs = sysconf(_SC_NPROCESSORS_ONLN);
	if (nprocs != -1)
		return (int)nprocs;
#elif defined(CTL_HW) && defined(HW_NCPU)
	int mib[2] = {CTL_HW, HW_NCPU};
	int count = 0;
	size_t len = sizeof(size);
	if (sysctl(mib, 2, &size, &len, NULL, 0) == 0 && count > 0)
		return count;
#endif
	return 1;
}


static size_t
memory_round(size_t value, size_t minimum)
{
	size_t msb = 1;

	value = value * 9/8;
	while (true)
	{
		size_t msb_next = msb << 1;
		if (msb_next > value)
			break;
		msb = msb_next;
	}
	
	value &= (msb | (msb>>1));

	return (value > minimum) ? value : minimum;
}


static char*
memory_pretty_value(size_t value)
{
	char *units[] = { "B", "kB", "MB", "GB", "TB", "PB", "EB", NULL};
	char **unit = units;

	while (value > 1024 && unit[1])
	{
		value = (value + 512) >> 10;
		unit++;
	}

	return psprintf("%ld%s", value, *unit);
}

char**
initdb_autoconfig_update(char** lines)
{
	char** pos;
	size_t ram = GetSystemMemorySize();
	int cpu = GetCoreCount();
	int p_max_connections = 50 * cpu;
	int p_autovacuum_max_workers=Max(cpu*12/5, 4);
	size_t p_effective_cache_size = memory_round(ram*3/4,1*BLCKSZ);
	size_t p_shared_buffers = memory_round(ram/4, 16*BLCKSZ);
	size_t p_work_mem = memory_round(ram*2/p_max_connections, 64*BLCKSZ);
	size_t p_temp_buffers = memory_round(ram/8192, 100*BLCKSZ);
	size_t p_maintenance_work_mem = memory_round(ram/32, 1<<30);
	bool highload = (ram > ((int64_t)126)<<30); /* When things get serious */
	char** newlines;
	char** spos = lines;
	size_t nlines;

	nlines=0;
	while(lines[nlines])
		nlines++;

	newlines = palloc_array(char*, nlines + 128);
	pos = newlines;

	while(*spos)
	{
		if (strcmp(*spos, "# CUSTOMIZED OPTIONS\n")==0)
		{
			/*
			* Based on recommendations from:
			* https://its.1c.ru/db/metod8dev/content/5825/hdoc
			* https://its.1c.ru/db/metod8dev/content/5866/hdoc
			*/
			*(pos++) = psprintf("# Auto-generated configuration for 1C:Enterprise.\n");
			*(pos++) = psprintf("# Parameters are tuned for %s RAM and %d CPU cores.\n", memory_pretty_value(ram), cpu);
			*(pos++) = psprintf("# Values can be used as a good starting point for server configuration.\n");
			*(pos++) = psprintf("#------------------------------------------------------------------------------\n");
			*(pos++) = psprintf("\n");
			*(pos++) = psprintf("autovacuum                      = on\n");
			*(pos++) = psprintf("autovacuum_max_workers          = %d\n", p_autovacuum_max_workers);
			*(pos++) = psprintf("autovacuum_worker_slots         = %d\n", p_autovacuum_max_workers);
			*(pos++) = psprintf("autovacuum_naptime              = 20s\n");
			*(pos++) = psprintf("autovacuum_vacuum_cost_delay    = 10ms\n");
			*(pos++) = psprintf("autovacuum_vacuum_cost_limit    = -1\n");
			*(pos++) = psprintf("autovacuum_vacuum_scale_factor  = 0.01\n");
			*(pos++) = psprintf("bgwriter_delay                  = 20ms\n");
			*(pos++) = psprintf("bgwriter_lru_maxpages           = 500\n");
			*(pos++) = psprintf("bgwriter_lru_multiplier         = 4.0\n");
			*(pos++) = psprintf("checkpoint_completion_target    = 0.9\n");
			*(pos++) = psprintf("checkpoint_timeout              = 10min\n");
			*(pos++) = psprintf("commit_delay                    = 1000\n");
			*(pos++) = psprintf("commit_siblings                 = 5\n");
			*(pos++) = psprintf("effective_cache_size            = %s\n", memory_pretty_value(p_effective_cache_size));
			*(pos++) = psprintf("effective_io_concurrency        = 2\n");
			*(pos++) = psprintf("enable_temp_memory_catalog      = off # enable to store temporary tables metadata in RAM\n");
			*(pos++) = psprintf("enable_temp_rd_buffers          = off # enable to store small temporary tables data in RAM\n");
			*(pos++) = psprintf("escape_string_warning           = off\n");
			*(pos++) = psprintf("from_collapse_limit             = 20\n");
			*(pos++) = psprintf("fsync                           = on\n");
			*(pos++) = psprintf("geqo                            = on\n");
			*(pos++) = psprintf("geqo_threshold                  = 12\n");
			*(pos++) = psprintf("jit                             = off\n");
			*(pos++) = psprintf("join_collapse_limit             = 20\n");
			*(pos++) = psprintf("maintenance_work_mem            = %s\n", memory_pretty_value(p_maintenance_work_mem));
			*(pos++) = psprintf("max_connections                 = %d\n", p_max_connections);
			*(pos++) = psprintf("max_files_per_process           = 8000\n");
			*(pos++) = psprintf("max_locks_per_transaction       = 2000\n");
			*(pos++) = psprintf("max_logical_replication_workers = 0\n");
			*(pos++) = psprintf("max_parallel_workers            = 1\n");
			*(pos++) = psprintf("max_parallel_workers_per_gather = 0\n");
			*(pos++) = psprintf("max_prepared_transactions       = 0\n");
			*(pos++) = psprintf("max_wal_senders                 = 0\n");
			*(pos++) = psprintf("max_wal_size                    = %s\n", highload?"10GB":"2GB");
			*(pos++) = psprintf("max_worker_processes            = 2\n");
			*(pos++) = psprintf("min_wal_size                    = 1GB\n");
			*(pos++) = psprintf("online_analyze.enable           = off\n");
			*(pos++) = psprintf("online_analyze.local_tracking   = on\n");
			*(pos++) = psprintf("online_analyze.min_interval     = 10000\n");
			*(pos++) = psprintf("online_analyze.scale_factor     = 0.1\n");
			*(pos++) = psprintf("online_analyze.table_type       = 'temporary'\n");
			*(pos++) = psprintf("online_analyze.threshold        = 50\n");
			*(pos++) = psprintf("online_analyze.verbose          = off\n");
			*(pos++) = psprintf("plantuner.fix_empty_table       = on\n");
			*(pos++) = psprintf("random_page_cost                = 1.1\n");
			*(pos++) = psprintf("row_security                    = off\n");
			*(pos++) = psprintf("shared_buffers                  = %s\n", memory_pretty_value(p_shared_buffers));
			*(pos++) = psprintf("shared_preload_libraries        = 'mchar, fasttrun, fulleq'\n");
			*(pos++) = psprintf("ssl                             = off\n");
			*(pos++) = psprintf("standard_conforming_strings     = off\n");
			*(pos++) = psprintf("synchronous_commit              = off\n");
			*(pos++) = psprintf("temp_buffers                    = %s\n", memory_pretty_value(p_temp_buffers));
			*(pos++) = psprintf("track_activity_query_size       = 128\n");
			*(pos++) = psprintf("wal_level                       = minimal\n");
			*(pos++) = psprintf("work_mem                        = %s\n", memory_pretty_value(p_work_mem));
			*(pos++) = psprintf("# pg_stat_temp                    = '' <- it's recommended to place on a separate disk\n");
			*(pos++) = psprintf("# temp_tablespaces                = '' <- it's recommended to place on a separate disk\n");
			*(pos++) = psprintf("\n");
			*(pos++) = psprintf("#------------------------------------------------------------------------------\n");
		}

		*(pos++) = *(spos++);
	}

	*(pos++) = NULL;

	pfree(lines);
	return newlines;
}
