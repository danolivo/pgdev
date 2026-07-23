# contrib/shared_numeric_agg/Makefile

MODULE_big = shared_numeric_agg
OBJS = \
	$(WIN32RES) \
	shared_numeric_agg.o

EXTENSION = shared_numeric_agg
DATA = shared_numeric_agg--1.0.sql
PGFILEDESC = "shared_numeric_agg - flat in-place numeric aggregates"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/shared_numeric_agg
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
