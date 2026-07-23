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
