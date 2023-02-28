
SET query_inadequate_execution_time = 0;
DROP TABLE IF EXISTS a;
CREATE TABLE a AS SELECT -x AS x, repeat('abc', 100) AS y
  FROM generate_series (1,1e6) AS x;
ANALYZE a;

DELETE FROM a;
INSERT INTO a (x,y) SELECT x, repeat('abc', 100) AS y FROM generate_series (1,1e6) AS x;
EXPLAIN (ANALYZE, VERBOSE)
SELECT count(*) FROM
  (SELECT x FROM a WHERE x > 0) As q;
SET query_inadequate_execution_time = 3;

SELECT count(*) FROM
  (SELECT x FROM a WHERE x > 0) As q;

SET query_inadequate_execution_time = 0;
EXPLAIN (ANALYZE, VERBOSE)
SELECT count(*) FROM
  (SELECT x FROM a WHERE x > 0) As q;


ANALYZE a;
EXPLAIN (ANALYZE, VERBOSE)
SELECT count(*) FROM
  (SELECT x FROM a WHERE x > 0) As q;


/*
SELECT oid,relname, pg_sleep(1) FROM pg_class WHERE relname='pg_class';

EXPLAIN VERBOSE
SELECT oid,relname, pg_sleep(1) FROM pg_class WHERE relname='pg_class';
*/
