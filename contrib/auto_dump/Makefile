MODULES = auto_dump

EXTENSION = auto_dump
PGFILEDESC = "auto_dump"
DATA = auto_dump--1.0.sql auto_dump--1.0--1.1.sql

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/auto_dump
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
