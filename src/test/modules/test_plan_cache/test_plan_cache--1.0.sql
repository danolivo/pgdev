/* src/test/modules/test_plan_cache/test_plan_cache--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_plan_cache" to load this file. \quit

CREATE FUNCTION test_plan_cache_is_generic(query text,
                                           request_generic bool,
                                           request_custom bool,
                                           param int)
RETURNS bool
STRICT
AS 'MODULE_PATHNAME' LANGUAGE C;
