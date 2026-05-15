# src/test/modules/test_plan_cache/Makefile

MODULE_big = test_plan_cache
OBJS = \
	$(WIN32RES) \
	test_plan_cache.o
PGFILEDESC = "test_plan_cache - cursor_options vs plan_cache_mode precedence"

EXTENSION = test_plan_cache
DATA = test_plan_cache--1.0.sql

REGRESS = test_plan_cache

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_plan_cache
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
