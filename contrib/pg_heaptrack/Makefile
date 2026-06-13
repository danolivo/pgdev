# contrib/pg_heaptrack/Makefile

MODULE_big = pg_heaptrack
OBJS = \
	$(WIN32RES) \
	pg_heaptrack.o
PGFILEDESC = "pg_heaptrack - profile memory context chunk allocations with heaptrack"

EXTENSION = pg_heaptrack
DATA = pg_heaptrack--1.0.sql

# The recording engine is compiled into the backend (USE_HEAPTRACK build); this
# module only calls into it, so it links nothing extra.  It does need heaptrack's
# libheaptrack.h for the engine control-API prototypes -- point HEAPTRACK_SRC at
# the heaptrack clone's src/ (e.g. make HEAPTRACK_SRC=$HOME/pg/contrib/heaptrack/src).
HEAPTRACK_SRC ?= ../heaptrack/src
PG_CPPFLAGS = -I$(HEAPTRACK_SRC)/track -Wno-error=strict-prototypes
PG_CFLAGS = -Wno-error=strict-prototypes

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_heaptrack
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
