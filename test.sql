SET enable_seqscan = 'off';
EXPLAIN (COSTS OFF)
SELECT oid,relname FROM pg_class
WHERE
  relname = 'pg_am' OR
  oid = 13779 OR
  relname = 'pg_extension'
;

SELECT oid,relname FROM pg_class
WHERE
  relname = 'pg_am' OR
  oid = 13779 OR
  relname = 'pg_extension' OR
  oid < 2
;
/*
SET enable_seqscan = 'off';
EXPLAIN (COSTS OFF)
SELECT oid,relname FROM pg_class
WHERE
  relname IN ('pg_am', 'pg_extension') OR
  oid = 13779
;
*/