/*
 * config.h
 *
 * Concrete substitute for heaptrack's CMake-generated src/util/config.h, so
 * the heaptrack recording engine can be compiled into the PostgreSQL backend
 * without running heaptrack's CMake.  Values taken from heaptrack 1.6.80.
 * Regenerate (or re-copy from a CMake build) if you update the engine.
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 */
#ifndef HEAPTRACK_CONFIG_H
#define HEAPTRACK_CONFIG_H

#define HEAPTRACK_VERSION_STRING "1.6.80"
#define HEAPTRACK_VERSION_MAJOR 1
#define HEAPTRACK_VERSION_MINOR 6
#define HEAPTRACK_VERSION_PATCH 80
#define HEAPTRACK_VERSION ((HEAPTRACK_VERSION_MAJOR << 16) | (HEAPTRACK_VERSION_MINOR << 8) | (HEAPTRACK_VERSION_PATCH))

#define HEAPTRACK_FILE_FORMAT_VERSION 3

#define HEAPTRACK_DEBUG_BUILD 0

/* cfree()/valloc() were removed from glibc 2.26+. */
#define HAVE_CFREE 0
#define HAVE_VALLOC 0

#endif /* HEAPTRACK_CONFIG_H */
