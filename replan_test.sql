SET query_inadequate_execution_time = 800;

EXPLAIN ANALYZE
SELECT oid,relname, pg_sleep(1) FROM pg_class WHERE relname='pg_class';

SELECT oid,relname, pg_sleep(1) FROM pg_class WHERE relname='pg_class';
