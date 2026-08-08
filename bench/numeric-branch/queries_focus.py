SER = "SET max_parallel_workers_per_gather=0;"
PAR = ("SET parallel_setup_cost=0; SET parallel_tuple_cost=0.005; "
       "SET min_parallel_table_scan_size=0; SET max_parallel_workers_per_gather=%d;")
S7 = "sum(v1),sum(v2),sum(v3),sum(v4),sum(v5),sum(v6),sum(v7)"

QUERIES = [
 # narrow single-column tables: the transition function dominates
 ("F1",  "sum(numeric(10,2)), 1 group -- int64 lane, BEST", SER, "SELECT sum(v) FROM t_nar;"),
 ("F2",  "avg(numeric(10,2)), 1 group", SER, "SELECT avg(v) FROM t_nar;"),
 ("F3",  "7 x sum, 1 group", SER, "SELECT %s FROM t_nar7;" % S7),
 ("F4",  "sum, 1k groups, int8 key (no numeric hashing)", SER,
   "SELECT count(*) FROM (SELECT gint, sum(v) FROM t_g GROUP BY gint) s;"),
 ("F5",  "sum, 592k groups, numeric key", SER,
   "SELECT count(*) FROM (SELECT g592k, sum(v) FROM t_g GROUP BY g592k) s;"),
 ("F6",  "sum, 5M groups (1 row each)", SER,
   "SELECT count(*) FROM (SELECT guniq, sum(v) FROM t_g GROUP BY guniq) s;"),
 ("F7",  "sum(25-digit) -- WORST: no int64 lane, never promotes", SER, "SELECT sum(v) FROM t_slow;"),
 ("F8",  "sum(30-digit) -- promotes mid-scan at 1.7M rows", SER, "SELECT sum(v) FROM t_ovf;"),
 ("F9",  "sum(45-digit) -- promotes at row 1", SER, "SELECT sum(v) FROM t_huge;"),
 ("F10", "sum, alternating display scale", SER, "SELECT sum(v) FROM t_mix;"),
 ("F11", "stddev_pop -- CONTROL, fast path excluded", SER, "SELECT stddev_pop(v) FROM t_nar;"),
 ("F12", "count(*) -- CONTROL, no numeric work at all", SER, "SELECT count(*) FROM t_nar;"),
 ("F13", "moving sum, 100-row frame (inverse transition)", SER,
   "SELECT count(*) FROM (SELECT sum(v) OVER (ROWS BETWEEN 100 PRECEDING AND CURRENT ROW) FROM t_nar) s;"),
 # parallel serialize / combine, on the narrow 7-aggregate table
 ("F14", "PARALLEL 7 x sum, 592k groups, 2 workers", PAR % 2,
   "SELECT count(*) FROM (SELECT g592k, %s FROM t_g g JOIN t_nar7 n ON true WHERE false GROUP BY g592k) s;"
   % S7),
 ("F15", "PARALLEL 7 x sum, 592k groups, 2 workers", PAR % 2,
   "SELECT count(*) FROM (SELECT g, sum(c1),sum(c2),sum(c3),sum(c4),sum(c5),sum(c6),sum(c7) "
   "FROM par GROUP BY g) s;"),
 ("F16", "PARALLEL 7 x sum, 592k groups, 3 workers", PAR % 3,
   "SELECT count(*) FROM (SELECT g, sum(c1),sum(c2),sum(c3),sum(c4),sum(c5),sum(c6),sum(c7) "
   "FROM par GROUP BY g) s;"),
 ("F17", "PARALLEL sum(45-digit), 592k groups -- promoted states", PAR % 2,
   "SELECT count(*) FROM (SELECT g, sum(c_huge) FROM par GROUP BY g) s;"),
 # positive control: a case already known to move, to prove the rig sees effects
 ("F18", "GROUP BY numeric, 1k groups -- positive control", SER,
   "SELECT count(*) FROM (SELECT g1k FROM t_g GROUP BY g1k) s;"),
]
QUERIES = [q for q in QUERIES if q[0] != "F14"]
