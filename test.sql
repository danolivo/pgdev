DROP TABLE IF EXISTS in_tbl,out_tbl CASCADE;

\set num 3e8

CREATE TABLE in_tbl AS (
	SELECT x,
		(random()* :num) AS y
	FROM generate_series(1, :num / 100) AS x
);

CREATE TABLE out_tbl AS (
	SELECT x,
		(random()* :num) AS y
	FROM generate_series(1, :num) AS x
);
ANALYZE;
SELECT oid,relpages,reltuples FROM pg_class WHERE relname='out_tbl';
SELECT oid,relpages,reltuples FROM pg_class WHERE relname='in_tbl';

SELECT pg_prewarm('in_tbl');
SELECT pg_prewarm('out_tbl');
