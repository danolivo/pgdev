# src/test/modules/test_int128/Makefile

PGFILEDESC = "test_int128 - test 128-bit integer arithmetic"

PROGRAM = test_int128
OBJS = $(WIN32RES) test_int128.o

# The overflow-checked primitives in int128.h have three distinct bodies; see
# the comment at the top of test_int128.c.  Build one binary per configuration
# so that "make check" exercises all of them rather than only the default.
VARIANTS = test_int128_native$(X) test_int128_nobuiltin$(X)
VARIANT_OBJS = test_int128_native.o test_int128_nobuiltin.o

PG_CPPFLAGS = -I$(libpq_srcdir)
PG_LIBS_INTERNAL += $(libpq_pgport)

NO_INSTALL = 1
TAP_TESTS = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_int128
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

all: $(VARIANTS)

test_int128_native.o: test_int128.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -DUSE_NATIVE_INT128=1 -c -o $@ $<

test_int128_nobuiltin.o: test_int128.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -DUSE_NATIVE_INT128=1 \
		-DTEST_INT128_NO_BUILTIN_OVERFLOW -c -o $@ $<

test_int128_native$(X): test_int128_native.o $(WIN32RES)
	$(CC) $(CFLAGS) $^ $(PG_LIBS_INTERNAL) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@

test_int128_nobuiltin$(X): test_int128_nobuiltin.o $(WIN32RES)
	$(CC) $(CFLAGS) $^ $(PG_LIBS_INTERNAL) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@

clean: clean-variants
clean-variants:
	rm -f $(VARIANTS) $(VARIANT_OBJS)
