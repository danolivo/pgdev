PAR = ("SET parallel_setup_cost=0; SET parallel_tuple_cost=0.005; "
       "SET min_parallel_table_scan_size=0; SET max_parallel_workers_per_gather=%d;")
SER = "SET max_parallel_workers_per_gather=0;"
S7  = "sum(c1),sum(c2),sum(c3),sum(c4),sum(c5),sum(c6),sum(c7)"
V7  = "sum(v_narrow),sum(v2),sum(v3),sum(v4),sum(v5),sum(v6),sum(v7)"
C20 = ",".join("c%d" % i for i in range(1, 21))

# (id, label, target commit, setup, sql)
QUERIES = [
 # ---- I: sum/avg transition state  (targets 9035685) -----------------
 ("I1",  "sum(numeric), 1 group -- BEST", "int128 sum", SER,
   "SELECT sum(v_narrow) FROM agg;"),
 ("I2",  "avg(numeric), 1 group", "int128 sum", SER,
   "SELECT avg(v_narrow) FROM agg;"),
 ("I3",  "7 x sum(numeric), 1 group", "int128 sum", SER,
   "SELECT %s FROM agg;" % V7),
 ("I4",  "sum, 1k groups, int8 key", "int128 sum", SER,
   "SELECT count(*) FROM (SELECT g_1k_int, sum(v_narrow) FROM agg GROUP BY g_1k_int) s;"),
 ("I5",  "sum, 1k groups, numeric key", "int128 sum", SER,
   "SELECT count(*) FROM (SELECT g_1k, sum(v_narrow) FROM agg GROUP BY g_1k) s;"),
 ("I6",  "sum, 1k groups, 80/20 skew", "int128 sum", SER,
   "SELECT count(*) FROM (SELECT g_skew, sum(v_narrow) FROM agg GROUP BY g_skew) s;"),
 ("I7",  "sum, 592k groups", "int128 sum", SER,
   "SELECT count(*) FROM (SELECT g_592k, sum(v_narrow) FROM agg GROUP BY g_592k) s;"),
 ("I8",  "sum, 5M groups (1 row each)", "int128 sum", SER,
   "SELECT count(*) FROM (SELECT g_uniq, sum(v_narrow) FROM agg GROUP BY g_uniq) s;"),
 ("I9",  "sum(25-digit) -- WORST: no int64 lane, never overflows", "int128 sum", SER,
   "SELECT sum(v_slow) FROM wide;"),
 ("I10", "sum(45-digit) -- rejected by width cap, promotes at row 1", "int128 sum", SER,
   "SELECT sum(v_huge) FROM wide;"),
 ("I11", "sum(numeric(32,2)) -- promotes mid-scan on overflow", "int128 sum", SER,
   "SELECT sum(v_ovf) FROM wide;"),
 ("I12", "sum, alternating display scale", "int128 sum", SER,
   "SELECT sum(v_mixscale) FROM agg;"),
 ("I13", "stddev_pop -- CONTROL, fast path excluded", "int128 sum", SER,
   "SELECT stddev_pop(v_narrow) FROM agg;"),
 ("I14", "moving sum, 100-row frame (inverse transition)", "int128 sum", SER,
   "SELECT count(*) FROM (SELECT sum(v_narrow) OVER (ORDER BY i ROWS BETWEEN 100 PRECEDING AND CURRENT ROW) "
   "FROM agg WHERE i <= 1000000) s;"),
 ("I15", "7 x sum, 592k groups, serial", "int128 sum", SER,
   "SELECT count(*) FROM (SELECT g, %s FROM par GROUP BY g) s;" % S7),

 # ---- II: parallel serialize / deserialize / combine  (targets 693023f)
 ("II1", "PARALLEL 7 x sum, 592k groups, 2 workers -- BEST", "serialization", PAR % 2,
   "SELECT count(*) FROM (SELECT g, %s FROM par GROUP BY g) s;" % S7),
 ("II2", "PARALLEL 7 x sum, 592k groups, 3 workers", "serialization", PAR % 3,
   "SELECT count(*) FROM (SELECT g, %s FROM par GROUP BY g) s;" % S7),
 ("II3", "PARALLEL 1 x sum, 592k groups, 2 workers", "serialization", PAR % 2,
   "SELECT count(*) FROM (SELECT g, sum(c1) FROM par GROUP BY g) s;"),
 ("II4", "PARALLEL sum(30-digit), 592k groups -- no int64 lane", "serialization", PAR % 2,
   "SELECT count(*) FROM (SELECT g, sum(c_ovf) FROM par GROUP BY g) s;"),
 ("II4b", "PARALLEL sum(45-digit), 592k groups -- all states promoted", "serialization", PAR % 2,
   "SELECT count(*) FROM (SELECT g, sum(c_huge) FROM par GROUP BY g) s;"),
 ("II5", "PARALLEL sum, 1k groups -- serialization negligible", "serialization", PAR % 2,
   "SELECT count(*) FROM (SELECT g_1k, sum(v_narrow) FROM agg GROUP BY g_1k) s;"),
 ("II6", "PARALLEL 7 x sum, 592k groups, 1 worker", "serialization", PAR % 1,
   "SELECT count(*) FROM (SELECT g, %s FROM par GROUP BY g) s;" % S7),
 ("II7", "PARALLEL sum, 1 group -- no per-group serialization", "serialization", PAR % 2,
   "SELECT sum(v_narrow) FROM agg;"),

 # ---- III: hash & comparison  (targets c3b903a) ----------------------
 ("III1", "GROUP BY numeric, 1 group", "hash/compare", SER,
   "SELECT count(*) FROM (SELECT g_one FROM agg GROUP BY g_one) s;"),
 ("III2", "GROUP BY numeric, 1k groups", "hash/compare", SER,
   "SELECT count(*) FROM (SELECT g_1k FROM agg GROUP BY g_1k) s;"),
 ("III3", "GROUP BY numeric, 1k groups, 80/20 skew", "hash/compare", SER,
   "SELECT count(*) FROM (SELECT g_skew FROM agg GROUP BY g_skew) s;"),
 ("III4", "GROUP BY numeric, 5M groups", "hash/compare", SER,
   "SELECT count(*) FROM (SELECT g_uniq FROM agg GROUP BY g_uniq) s;"),
 ("III5", "GROUP BY 20 numeric columns", "hash/compare", SER,
   "SELECT count(*) FROM (SELECT %s FROM bench20 GROUP BY %s) s;" % (C20, C20)),
 ("III6", "hash join on numeric, 5M x 1k", "hash/compare", SER,
   "SELECT count(*) FROM agg a JOIN dim1k d ON a.g_1k = d.g_1k;"),
 ("III7", "GROUP BY 200-digit numeric -- palloc fallback", "hash/compare", SER,
   "SELECT count(*) FROM (SELECT g_200 FROM longkey GROUP BY g_200) s;"),
 ("III8", "GROUP BY toasted numeric", "hash/compare", SER,
   "SELECT count(*) FROM (SELECT g_big FROM toastkey GROUP BY g_big) s;"),
 ("III9", "GROUP BY int8 -- CONTROL", "hash/compare", SER,
   "SELECT count(*) FROM (SELECT g_1k_int FROM agg GROUP BY g_1k_int) s;"),
]
