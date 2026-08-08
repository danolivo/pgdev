SER = "SET jit=off; SET max_parallel_workers_per_gather=0;"
S3  = "sum(v),sum(v2),sum(v3)"
# int8 grouping keys throughout, so the per-group cost of the numeric
# accumulator is measured with no numeric hashing in the picture.
QUERIES = [
 ("E1","sum, 1 group (ungrouped) -- per-row check","sum","perrow",SER,
  "SELECT sum(v) FROM t_k;"),
 ("E2","sum, 1k groups, int8 key","sum","typical",SER,
  "SELECT count(*) FROM (SELECT k1k, sum(v) FROM t_k GROUP BY k1k) s;"),
 ("E3","sum, 100k groups, int8 key","sum","typical",SER,
  "SELECT count(*) FROM (SELECT k100k, sum(v) FROM t_k GROUP BY k100k) s;"),
 ("E4","sum, 592k groups, int8 key","sum","typical",SER,
  "SELECT count(*) FROM (SELECT k592k, sum(v) FROM t_k GROUP BY k592k) s;"),
 ("E5","3 x sum, 592k groups -- 3 states per group","sum","best",SER,
  "SELECT count(*) FROM (SELECT k592k, %s FROM t_k GROUP BY k592k) s;" % S3),
 ("E6","stddev_pop, 592k groups -- sumX and sumX2","sum","best",SER,
  "SELECT count(*) FROM (SELECT k592k, stddev_pop(v) FROM t_k GROUP BY k592k) s;"),
 ("E7","sum, 5M groups (1 row each)","sum","worst",SER,
  "SELECT count(*) FROM (SELECT kuniq, sum(v) FROM t_k GROUP BY kuniq) s;"),
 ("E8","sum(25-digit): exceeds inline capacity, still pallocs","sum","worst",SER,
  "SELECT sum(v) FROM t_slow;"),
 ("E9","count(*) -- CONTROL / normaliser","none","control",SER,
  "SELECT count(*) FROM t_k;"),
 ("E10","sum(int8) -- CONTROL, no numeric aggregate","none","control",SER,
  "SELECT sum(k1k) FROM t_k;"),
]
