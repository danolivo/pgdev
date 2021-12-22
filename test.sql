DROP TABLE IF EXISTS in_tbl,out_tbl CASCADE;

\set num 1e6

CREATE TABLE in_tbl AS (
	SELECT x,
		(random()* :num) AS y
	FROM generate_series(1, :num / 100) AS x
);

CREATE TABLE out_tbl AS (
	SELECT x,
		(random()* :num) AS y
	FROM generate_series(1, :num)
);