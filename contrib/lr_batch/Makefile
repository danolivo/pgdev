# contrib/lr_batch/Makefile

MODULE_big = lr_batch
OBJS = \
	$(WIN32RES) \
	lr_batch.o
PGFILEDESC = "lr_batch - batch INSERTs in the logical replication apply worker"

TAP_TESTS = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/lr_batch
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
