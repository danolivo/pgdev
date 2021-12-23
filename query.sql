-- Prewarm buffers
EXPLAIN (ANALYZE, BUFFERS ON) SELECT * FROM in_tbl;
EXPLAIN (ANALYZE, BUFFERS ON) SELECT * FROM in_tbl;
EXPLAIN (ANALYZE, BUFFERS ON) SELECT * FROM out_tbl;
EXPLAIN (ANALYZE, BUFFERS ON) SELECT * FROM out_tbl;

-- Number of working processes = nworkers+1
SET max_parallel_workers_per_gather = 0;
EXPLAIN (ANALYZE, BUFFERS ON)
SELECT avg(in_tbl.x) FROM in_tbl JOIN out_tbl USING (y);
EXPLAIN (ANALYZE, BUFFERS ON)
SELECT avg(in_tbl.x) FROM in_tbl JOIN out_tbl USING (y);
EXPLAIN (ANALYZE, BUFFERS ON)
SELECT avg(in_tbl.x) FROM in_tbl JOIN out_tbl USING (y);

SET max_parallel_workers_per_gather = 1;
EXPLAIN (ANALYZE, BUFFERS ON)
SELECT avg(in_tbl.x) FROM in_tbl JOIN out_tbl USING (y);
EXPLAIN (ANALYZE, BUFFERS ON)
SELECT avg(in_tbl.x) FROM in_tbl JOIN out_tbl USING (y);
EXPLAIN (ANALYZE, BUFFERS ON)
SELECT avg(in_tbl.x) FROM in_tbl JOIN out_tbl USING (y);

SET max_parallel_workers_per_gather = 2;
EXPLAIN (ANALYZE, BUFFERS ON)
SELECT avg(in_tbl.x) FROM in_tbl JOIN out_tbl USING (y);
EXPLAIN (ANALYZE, BUFFERS ON)
SELECT avg(in_tbl.x) FROM in_tbl JOIN out_tbl USING (y);
EXPLAIN (ANALYZE, BUFFERS ON)
SELECT avg(in_tbl.x) FROM in_tbl JOIN out_tbl USING (y);

SET max_parallel_workers_per_gather = 3;
EXPLAIN (ANALYZE, BUFFERS ON)
SELECT avg(in_tbl.x) FROM in_tbl JOIN out_tbl USING (y);
EXPLAIN (ANALYZE, BUFFERS ON)
SELECT avg(in_tbl.x) FROM in_tbl JOIN out_tbl USING (y);
EXPLAIN (ANALYZE, BUFFERS ON)
SELECT avg(in_tbl.x) FROM in_tbl JOIN out_tbl USING (y);

SET max_parallel_workers_per_gather = 4;
EXPLAIN (ANALYZE, BUFFERS ON)
SELECT avg(in_tbl.x) FROM in_tbl JOIN out_tbl USING (y);
EXPLAIN (ANALYZE, BUFFERS ON)
SELECT avg(in_tbl.x) FROM in_tbl JOIN out_tbl USING (y);
EXPLAIN (ANALYZE, BUFFERS ON)
SELECT avg(in_tbl.x) FROM in_tbl JOIN out_tbl USING (y);

SET max_parallel_workers_per_gather = 7;
EXPLAIN (ANALYZE, BUFFERS ON)
SELECT avg(in_tbl.x) FROM in_tbl JOIN out_tbl USING (y);
EXPLAIN (ANALYZE, BUFFERS ON)
SELECT avg(in_tbl.x) FROM in_tbl JOIN out_tbl USING (y);
EXPLAIN (ANALYZE, BUFFERS ON)
SELECT avg(in_tbl.x) FROM in_tbl JOIN out_tbl USING (y);

SET max_parallel_workers_per_gather = 32;
EXPLAIN (ANALYZE, BUFFERS ON)
SELECT avg(in_tbl.x) FROM in_tbl JOIN out_tbl USING (y);
EXPLAIN (ANALYZE, BUFFERS ON)
SELECT avg(in_tbl.x) FROM in_tbl JOIN out_tbl USING (y);
EXPLAIN (ANALYZE, BUFFERS ON)
SELECT avg(in_tbl.x) FROM in_tbl JOIN out_tbl USING (y);

