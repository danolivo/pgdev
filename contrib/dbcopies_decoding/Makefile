MODULE_big = dbcopies_decoding
OBJS=dbcopies_decoding.o mchar_recode.o
REGRESS = simple

REGRESS_OPTS = --temp-config $(top_srcdir)/contrib/dbcopies_decoding/logical.conf

# Disabled because these tests require "wal_level=logical", which
# typical installcheck users do not have (e.g. buildfarm clients).
NO_INSTALLCHECK = 1

PG_CPPFLAGS += -I/usr/local/include -I$(top_srcdir)/contrib/mchar

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS = $(shell pg_config --pgxs)
include $(PGXS)
else
subdir = contrib/dbcopies_decoding
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

ifeq ($(PORTNAME),win32)
ICUNAME=icuin
else
ICUNAME=icui18n
endif

SHLIB_LINK += -L/usr/local/lib -licuuc -l$(ICUNAME) -Wl,-rpath,'$$ORIGIN'

installcheck-force:
	$(pg_regress_installcheck) $(REGRESS)

mchar_recode.c: $(top_srcdir)/contrib/mchar/mchar_recode.c
	cp -f $(top_srcdir)/contrib/mchar/mchar_recode.c ./
