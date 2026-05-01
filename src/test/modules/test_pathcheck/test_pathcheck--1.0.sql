/* src/test/modules/test_pathcheck/test_pathcheck--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_pathcheck" to load this file. \quit

CREATE FUNCTION test_pathcheck_enable() RETURNS void
AS 'MODULE_PATHNAME', 'test_pathcheck_enable'
LANGUAGE C VOLATILE;

CREATE FUNCTION test_pathcheck_disable() RETURNS void
AS 'MODULE_PATHNAME', 'test_pathcheck_disable'
LANGUAGE C VOLATILE;
