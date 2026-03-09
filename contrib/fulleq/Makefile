MODULE_big = fulleq
OBJS = fulleq.o	
DOCS = README.fulleq
REGRESS = fulleq
DATA_built = fulleq--2.0.sql fulleq--unpackaged--2.0.sql
EXTENSION=fulleq

ARGTYPE = bool bytea char name int8 int2 int4 text \
	oid xid cid oidvector float4 float8 macaddr \
	inet cidr varchar date time timestamp timestamptz \
	interval timetz

ifdef USE_PGXS
PGXS := $(shell pg_config --pgxs)
include $(PGXS)
else
subdir = contrib/fulleq
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

fulleq--2.0.sql: fulleq--2.0.sql.in
	$(top_srcdir)/$(subdir)/generate.sh packaged "$<" > "$@"

fulleq--unpackaged--2.0.sql: fulleq--unpackaged--2.0.sql.in
	$(top_srcdir)/$(subdir)/generate.sh unpackaged "$<" > "$@"

