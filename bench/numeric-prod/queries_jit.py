# jit=on at default costs, as a production server runs it.  jit_above_cost is
# exceeded by all of these, and jit_inline_above_cost lets LLVM inline strict
# operator functions from the installed bitcode -- including numeric_eq --
# which is the thing this experiment is meant to detect.
J = "SET jit=on; SET jit_above_cost=0; SET jit_inline_above_cost=0; SET max_parallel_workers_per_gather=0;"
JP = ("SET jit=on; SET jit_above_cost=0; SET jit_inline_above_cost=0; "
      "SET parallel_setup_cost=0; SET parallel_tuple_cost=0.005; "
      "SET min_parallel_table_scan_size=0; SET max_parallel_workers_per_gather=2;")
C20=",".join("c%d"%i for i in range(1,21))
QUERIES=[
 ("J1","JIT GROUP BY numeric, 1k groups","hash","typical",J,
  "SELECT count(*) FROM (SELECT g_1k FROM agg GROUP BY g_1k) s;"),
 ("J2","JIT GROUP BY numeric, 1k groups, 80/20","hash","typical",J,
  "SELECT count(*) FROM (SELECT g_skew FROM agg GROUP BY g_skew) s;"),
 ("J3","JIT GROUP BY 20 numeric columns","hash","best",J,
  "SELECT count(*) FROM (SELECT %s FROM bench20 GROUP BY %s) s;"%(C20,C20)),
 ("J4","JIT hash join on numeric","hash","typical",J,
  "SELECT count(*) FROM agg a JOIN dim1k d ON a.g_1k = d.g_1k;"),
 ("J5","JIT sum(numeric(10,2)), 1 group","sum","best",J,"SELECT sum(v) FROM t_nar;"),
 ("J6","JIT sum(25-digit): old code's slow lane","sum","worst",J,"SELECT sum(v) FROM t_slow;"),
 ("J7","JIT sum(17-digit): first past the gate","sum","boundary",J,"SELECT sum(v) FROM t_d5;"),
 ("J8","JIT PARALLEL 7 x sum, 592k groups","ser","best",JP,
  "SELECT count(*) FROM (SELECT g, sum(c1),sum(c2),sum(c3),sum(c4),sum(c5),sum(c6),sum(c7) FROM par GROUP BY g) s;"),
 ("J9","JIT count(*) -- CONTROL / normaliser","none","control",J,"SELECT count(*) FROM t_nar;"),
 ("J10","JIT GROUP BY int8 -- CONTROL","none","control",J,
  "SELECT count(*) FROM (SELECT gint, count(*) FROM t_g GROUP BY gint) s;"),
]
