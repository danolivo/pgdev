MODULES = relcleaner

EXTENSION = relcleaner
PGFILEDESC = "relcleaner - background worker for clean relation after deletion"

ifdef USE_PGXS
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/backend/access/bgheap
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
OBJS = relcleaner.o
include $(top_srcdir)/src/backend/common.mk
endif
