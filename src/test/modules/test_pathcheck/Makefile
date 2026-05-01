# src/test/modules/test_pathcheck/Makefile

PGFILEDESC = "test_pathcheck - test code for the single-pathlist invariant tracker"

MODULE_big = test_pathcheck
OBJS = \
	$(WIN32RES) \
	test_pathcheck.o

EXTENSION = test_pathcheck
DATA = test_pathcheck--1.0.sql

TAP_TESTS = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_pathcheck
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
