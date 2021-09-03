# contrib/pg_extension/Makefile

MODULE_big = pg_extension
OBJS = \
	$(WIN32RES) \
	pg_extension.o
PGFILEDESC = "pg_extension - template for future extensions"

EXTENSION = pg_extension

ifdef USE_PGXS
PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_extension
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
