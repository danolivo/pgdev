SER = "SET jit=off; SET max_parallel_workers_per_gather=0;"
Q1  = ("sum(v), sum(v*v2), sum(v*v2*(1-v3/1000000)), "
       "sum(v*v2*(1-v3/1000000)*(1+v3/2000000)), count(*)")
QUERIES = [
 ("U1","sum(numeric): 1 column ref (transition palloc)","agg","best",SER,
  "SELECT sum(v) FROM t_k;"),
 ("U2","avg(numeric): 1 column ref","agg","best",SER,
  "SELECT avg(v) FROM t_k;"),
 ("U3","sum(v*v2): 2 refs + one multiply","arith","best",SER,
  "SELECT sum(v*v2) FROM t_k;"),
 ("U4","sum(v*v2+v3): 3 refs + multiply + add","arith","best",SER,
  "SELECT sum(v*v2+v3) FROM t_k;"),
 ("U5","TPC-H Q1 shape: 5 aggregates, ~9 refs/row","arith","best",SER,
  "SELECT %s FROM t_k;" % Q1),
 ("U6","projection only, no aggregate: count over v+v2","arith","typical",SER,
  "SELECT count(*) FROM (SELECT v+v2 AS x FROM t_k) s;"),
 ("U7","sum(v) GROUP BY numeric, 1k groups","agg","typical",SER,
  "SELECT count(*) FROM (SELECT g_1k, sum(v_narrow) FROM agg GROUP BY g_1k) s;"),
 ("U8","moving sum, 100-row frame (inverse transition)","agg","corner",SER,
  "SELECT count(*) FROM (SELECT sum(v) OVER (ROWS BETWEEN 100 PRECEDING AND CURRENT ROW) FROM t_k) s;"),
 ("U9","sum(25-digit): exceeds the stack buffer, falls back","agg","worst",SER,
  "SELECT sum(v) FROM t_slow;"),
 ("U10","count(*) -- CONTROL / normaliser","none","control",SER,
  "SELECT count(*) FROM t_k;"),
 ("U11","sum(int8) -- CONTROL, no numeric at all","none","control",SER,
  "SELECT sum(k1k) FROM t_k;"),
]
