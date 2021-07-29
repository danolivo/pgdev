# src/test/modules/csnsnapshot/Makefile

REGRESS_OPTS = --temp-config=$(top_srcdir)/src/test/modules/csnsnapshot/csn_snapshot.conf
NO_INSTALLCHECK = 1

TAP_TESTS = 1
EXTRA_INSTALL=contrib/postgres_fdw

# Don't support full consistency of distributed commit in READ COMMITTED
# transactions.
#PROVE_TESTS =	t/001_base.pl \
#				t/002_standby.pl \
#				t/003_time_skew.pl
PROVE_TESTS =					t/005_basic_visibility.pl

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/csnsnapshot
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
