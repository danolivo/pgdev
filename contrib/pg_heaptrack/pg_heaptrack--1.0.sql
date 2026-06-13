/* contrib/pg_heaptrack/pg_heaptrack--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_heaptrack" to load this file. \quit

-- Start (or resume) chunk-level heaptrack recording in this backend.
CREATE FUNCTION pg_heaptrack_start()
RETURNS void
AS 'MODULE_PATHNAME', 'pg_heaptrack_start'
LANGUAGE C;

-- Pause recording in this backend; the data file is flushed at backend exit.
CREATE FUNCTION pg_heaptrack_stop()
RETURNS void
AS 'MODULE_PATHNAME', 'pg_heaptrack_stop'
LANGUAGE C;

-- Report whether heaptrack recording is active in this backend.
CREATE FUNCTION pg_heaptrack_is_active()
RETURNS boolean
AS 'MODULE_PATHNAME', 'pg_heaptrack_is_active'
LANGUAGE C;

-- These touch only backend-local state, so do not hand them to non-superusers.
REVOKE ALL ON FUNCTION pg_heaptrack_start() FROM PUBLIC;
REVOKE ALL ON FUNCTION pg_heaptrack_stop() FROM PUBLIC;
REVOKE ALL ON FUNCTION pg_heaptrack_is_active() FROM PUBLIC;
