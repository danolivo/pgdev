/* contrib/shared_numeric_agg/shared_numeric_agg--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION shared_numeric_agg" to load this file. \quit

CREATE FUNCTION numeric_flat_sum_trans(bytea, numeric)
RETURNS bytea
AS 'MODULE_PATHNAME'
LANGUAGE C PARALLEL SAFE;

CREATE FUNCTION numeric_flat_sum_combine(bytea, bytea)
RETURNS bytea
AS 'MODULE_PATHNAME'
LANGUAGE C PARALLEL SAFE;

CREATE FUNCTION numeric_flat_sum_final(bytea)
RETURNS numeric
AS 'MODULE_PATHNAME'
LANGUAGE C PARALLEL SAFE;

CREATE FUNCTION numeric_flat_avg_final(bytea)
RETURNS numeric
AS 'MODULE_PATHNAME'
LANGUAGE C PARALLEL SAFE;

-- Named sum/avg so that placing the extension schema ahead of pg_catalog
-- in search_path shadows the stock aggregates without query changes:
--     CREATE EXTENSION shared_numeric_agg SCHEMA shared_agg;
--     SET search_path = shared_agg, "$user", public, pg_catalog;

CREATE AGGREGATE sum(numeric) (
    SFUNC       = numeric_flat_sum_trans,
    STYPE       = bytea,
    COMBINEFUNC = numeric_flat_sum_combine,
    FINALFUNC   = numeric_flat_sum_final,
    PARALLEL    = SAFE
);

CREATE AGGREGATE avg(numeric) (
    SFUNC       = numeric_flat_sum_trans,
    STYPE       = bytea,
    COMBINEFUNC = numeric_flat_sum_combine,
    FINALFUNC   = numeric_flat_avg_final,
    PARALLEL    = SAFE
);

-- Plan-time substitution: the support function below is attached to the
-- stock aggregates and rewrites sum(numeric)/avg(numeric) into the flat
-- variants when enable_parallel_hash_agg is on, parallelism is possible,
-- and the argument's typmod bounds all values to the flat lane window.
-- With it in place, neither queries nor search_path need changing.

CREATE FUNCTION shared_numeric_agg_support(internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

ALTER FUNCTION pg_catalog.sum(numeric) SUPPORT shared_numeric_agg_support;
ALTER FUNCTION pg_catalog.avg(numeric) SUPPORT shared_numeric_agg_support;

-- NOTE: the ALTERs record a dependency of the stock aggregates on the
-- support function, so DROP EXTENSION is refused until the support links
-- are detached (there is no ALTER ... SUPPORT NONE yet):
--   UPDATE pg_proc SET prosupport = 0
--    WHERE oid IN ('pg_catalog.sum(numeric)'::regprocedure,
--                  'pg_catalog.avg(numeric)'::regprocedure);
--   DELETE FROM pg_depend
--    WHERE refobjid = 'shared_numeric_agg_support(internal)'::regprocedure
--      AND classid = 'pg_proc'::regclass;
