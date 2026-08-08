SER = "SET jit=off; SET max_parallel_workers_per_gather=0;"
SRT = "SET jit=off; SET max_parallel_workers_per_gather=0; SET enable_hashagg=off;"
PAR = ("SET jit=off; SET parallel_setup_cost=0; SET parallel_tuple_cost=0.005; "
       "SET min_parallel_table_scan_size=0; SET max_parallel_workers_per_gather=%d;")
S7  = "sum(v1),sum(v2),sum(v3),sum(v4),sum(v5),sum(v6),sum(v7)"
P7  = "sum(c1),sum(c2),sum(c3),sum(c4),sum(c5),sum(c6),sum(c7)"
C20 = ",".join("c%d" % i for i in range(1, 21))

# (id, label, commit-under-test, class, setup, sql)
QUERIES = [
 # ---------- A: hashing and comparison (c3b903a) ----------
 ("A1","GROUP BY numeric, 1 group","hash","typical",SER,
  "SELECT count(*) FROM (SELECT g_one FROM agg GROUP BY g_one) s;"),
 ("A2","GROUP BY numeric, 1k groups","hash","typical",SER,
  "SELECT count(*) FROM (SELECT g_1k FROM agg GROUP BY g_1k) s;"),
 ("A3","GROUP BY numeric, 1k groups, 80/20 skew","hash","typical",SER,
  "SELECT count(*) FROM (SELECT g_skew FROM agg GROUP BY g_skew) s;"),
 ("A4","GROUP BY numeric, 5M groups (1 row each)","hash","worst",SER,
  "SELECT count(*) FROM (SELECT g_uniq FROM agg GROUP BY g_uniq) s;"),
 ("A5","GROUP BY 20 numeric columns, 1k groups","hash","best",SER,
  "SELECT count(*) FROM (SELECT %s FROM bench20 GROUP BY %s) s;" % (C20, C20)),
 ("A6","hash join on numeric, 5M x 1k","hash","typical",SER,
  "SELECT count(*) FROM agg a JOIN dim1k d ON a.g_1k = d.g_1k;"),
 ("A7","GROUP BY 200-digit numeric (palloc fallback)","hash","worst",SER,
  "SELECT count(*) FROM (SELECT g_200 FROM longkey GROUP BY g_200) s;"),
 ("A8","GROUP BY toasted numeric","hash","worst",SER,
  "SELECT count(*) FROM (SELECT g_big FROM toastkey GROUP BY g_big) s;"),
 ("A9","GROUP BY mixed display scale (memcmp misses)","hash","worst",SER,
  "SELECT count(*) FROM (SELECT g FROM t_gmix GROUP BY g) s;"),
 ("A10","ORDER BY numeric, 5M rows","hash","neutral",SER,
  "SELECT g_uniq FROM agg ORDER BY g_uniq OFFSET 4999999;"),

 # ---------- B: int128 fast sum (9035685) ----------
 ("B1","sum(numeric(10,2)), 1 group","sum","best",SER,"SELECT sum(v) FROM t_nar;"),
 ("B2","avg(numeric(10,2)), 1 group","sum","best",SER,"SELECT avg(v) FROM t_nar;"),
 ("B3","7 x sum, 1 group","sum","best",SER,"SELECT %s FROM t_nar7;" % S7),
 ("B4","sum, 1k groups","sum","typical",SER,
  "SELECT count(*) FROM (SELECT g1k, sum(v) FROM t_g GROUP BY g1k) s;"),
 ("B5","sum, 1k groups, 80/20 skew","sum","typical",SER,
  "SELECT count(*) FROM (SELECT g_skew, sum(v_narrow) FROM agg GROUP BY g_skew) s;"),
 ("B6","sum, 592k groups","sum","typical",SER,
  "SELECT count(*) FROM (SELECT g592k, sum(v) FROM t_g GROUP BY g592k) s;"),
 ("B7","sum, 5M groups (1 row each)","sum","worst",SER,
  "SELECT count(*) FROM (SELECT guniq, sum(v) FROM t_g GROUP BY guniq) s;"),
 ("B8","sum(25-digit): no int64 lane, never promotes","sum","worst",SER,
  "SELECT sum(v) FROM t_slow;"),
 ("B9","sum(numeric(32,2)): promotes mid-scan","sum","worst",SER,
  "SELECT sum(v) FROM t_ovf;"),
 ("B10","sum(45-digit): promotes at row 1","sum","corner",SER,
  "SELECT sum(v) FROM t_huge;"),
 ("B11","sum, alternating display scale","sum","corner",SER,"SELECT sum(v) FROM t_mix;"),
 ("B12","sum with 30% NULLs","sum","corner",SER,"SELECT sum(v) FROM t_null;"),
 ("B13","sum with NaN/+-Inf mixed in","sum","corner",SER,"SELECT sum(v) FROM t_spec;"),
 ("B14","moving sum, 100-row frame (inverse transition)","sum","corner",SER,
  "SELECT count(*) FROM (SELECT sum(v) OVER (ROWS BETWEEN 100 PRECEDING AND CURRENT ROW) FROM t_nar) s;"),
 ("B16","sum(16-digit): widest value still in the int64 lane","sum","boundary",SER,
  "SELECT sum(v) FROM t_d4;"),
 ("B17","sum(17-digit): first value past the gate -- old code's slow lane","sum","boundary",SER,
  "SELECT sum(v) FROM t_d5;"),
 ("B15","sum over sorted GroupAggregate (no hashing)","sum","isolation",SRT,
  "SELECT count(*) FROM (SELECT g1k, sum(v) FROM t_g GROUP BY g1k) s;"),

 # ---------- C: parallel serialize / combine (693023f) ----------
 ("C1","PARALLEL 7 x sum, 592k groups, 1 worker","ser","scaling",PAR % 1,
  "SELECT count(*) FROM (SELECT g, %s FROM par GROUP BY g) s;" % P7),
 ("C2","PARALLEL 7 x sum, 592k groups, 2 workers","ser","best",PAR % 2,
  "SELECT count(*) FROM (SELECT g, %s FROM par GROUP BY g) s;" % P7),
 ("C3","PARALLEL 7 x sum, 592k groups, 3 workers","ser","best",PAR % 3,
  "SELECT count(*) FROM (SELECT g, %s FROM par GROUP BY g) s;" % P7),
 ("C4","PARALLEL 1 x sum, 592k groups, 2 workers","ser","typical",PAR % 2,
  "SELECT count(*) FROM (SELECT g, sum(c1) FROM par GROUP BY g) s;"),
 ("C5","PARALLEL sum(45-digit), 592k groups (all promoted)","ser","worst",PAR % 2,
  "SELECT count(*) FROM (SELECT g, sum(c_huge) FROM par GROUP BY g) s;"),
 ("C6","PARALLEL sum, 1k groups (serialization negligible)","ser","worst",PAR % 2,
  "SELECT count(*) FROM (SELECT g_1k, sum(v_narrow) FROM agg GROUP BY g_1k) s;"),
 ("C7","PARALLEL sum, 1 group (no per-group serialization)","ser","worst",PAR % 2,
  "SELECT sum(v) FROM t_nar;"),

 # ---------- X: controls ----------
 ("X1","count(*) -- CONTROL / normaliser","none","control",SER,
  "SELECT count(*) FROM t_nar;"),
 ("X2","stddev_pop -- CONTROL, fast path excluded","none","control",SER,
  "SELECT stddev_pop(v) FROM t_nar;"),
 ("X3","GROUP BY int8 -- CONTROL, no numeric hashing","none","control",SER,
  "SELECT count(*) FROM (SELECT gint, count(*) FROM t_g GROUP BY gint) s;"),
 ("X4","sum(int8) -- CONTROL, no numeric aggregate","none","control",SER,
  "SELECT sum(gint) FROM t_g;"),
]
