/*
 * libunwind_config.h
 *
 * Concrete substitute for heaptrack's CMake-generated
 * src/util/libunwind_config.h.  These flags are consulted ONLY by
 * trace_libunwind.cpp.  The default engine build (see ../Makefile) uses the
 * unwind-tables backend (trace_unwind_tables.cpp), so the values below are
 * inert.  If you build with TRACE=libunwind, regenerate this header from a
 * CMake configure of your libunwind (CMake's configure_file fills it in), or
 * set each flag to match the libunwind you link against.
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 */
#ifndef LIBUNWIND_CONFIG_H
#define LIBUNWIND_CONFIG_H

#define LIBUNWIND_HAS_UNW_BACKTRACE 0
#define LIBUNWIND_HAS_UNW_BACKTRACE_SKIP 0
#define LIBUNWIND_HAS_UNW_GETCONTEXT 1
#define LIBUNWIND_HAS_UNW_INIT_LOCAL 1
#define LIBUNWIND_HAS_UNW_SET_CACHE_SIZE 0
#define LIBUNWIND_HAS_UNW_CACHE_PER_THREAD 0

#endif /* LIBUNWIND_CONFIG_H */
