# contrib/pg_ext_planner/Makefile

MODULE_big = pg_ext_planner
OBJS = pg_ext_planner.o

REGRESS = memoize_subplan

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_ext_planner
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
