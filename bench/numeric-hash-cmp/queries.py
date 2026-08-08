COLS20 = ",".join("c%d" % i for i in range(1, 21))

QUERIES = [
 # --- pure grouping (isolates hash_numeric + numeric_eq) ---
 ("A1 group 1 group (all rows -> 1)", "", "SELECT count(*) FROM (SELECT g_one FROM bench GROUP BY g_one) s;"),
 ("A2 group 1k groups",               "", "SELECT count(*) FROM (SELECT g_1k FROM bench GROUP BY g_1k) s;"),
 ("A3 group 1k groups, 80/20 skew",   "", "SELECT count(*) FROM (SELECT g_skew FROM bench GROUP BY g_skew) s;"),
 ("A4 group 100k groups",             "", "SELECT count(*) FROM (SELECT g_100k FROM bench GROUP BY g_100k) s;"),
 ("A5 group 5M groups (all unique)",  "", "SELECT count(*) FROM (SELECT g_uniq FROM bench GROUP BY g_uniq) s;"),
 # --- same, with a real numeric aggregate on top ---
 ("B1 sum(v), 1 group",               "", "SELECT count(*) FROM (SELECT g_one, sum(v) FROM bench GROUP BY g_one) s;"),
 ("B2 sum(v), 1k groups",             "", "SELECT count(*) FROM (SELECT g_1k, sum(v) FROM bench GROUP BY g_1k) s;"),
 ("B3 sum(v), 1k groups, 80/20 skew", "", "SELECT count(*) FROM (SELECT g_skew, sum(v) FROM bench GROUP BY g_skew) s;"),
 ("B4 sum(v), 5M groups (unique)",    "", "SELECT count(*) FROM (SELECT g_uniq, sum(v) FROM bench GROUP BY g_uniq) s;"),
 # --- key shapes ---
 ("C1 group by 20 numeric cols, 1k groups (1M rows)", "",
    "SELECT count(*) FROM (SELECT %s FROM bench20 GROUP BY %s) s;" % (COLS20, COLS20)),
 ("C2 group, mixed display scale (memcmp always misses)", "",
    "SELECT count(*) FROM (SELECT g_mix FROM bench_mix GROUP BY g_mix) s;"),
 ("C3 group by 60-digit numeric (fits local buf)", "",
    "SELECT count(*) FROM (SELECT g_60 FROM bench_long GROUP BY g_60) s;"),
 ("C4 group by 200-digit numeric (falls back to palloc)", "",
    "SELECT count(*) FROM (SELECT g_200 FROM bench_long GROUP BY g_200) s;"),
 ("C5 group by toasted numeric (compressed/external)", "",
    "SELECT count(*) FROM (SELECT g_big FROM bench_toast GROUP BY g_big) s;"),
 ("C6 control: group by int8, 1k groups", "",
    "SELECT count(*) FROM (SELECT g_1k_int FROM bench GROUP BY g_1k_int) s;"),
 # --- other consumers of the same support functions ---
 ("D1 hash join on numeric (5M x 1k)", "",
    "SELECT count(*) FROM bench b JOIN dim1k d ON b.g_1k = d.g_1k;"),
 ("D2 SELECT DISTINCT, 5M unique", "",
    "SELECT count(*) FROM (SELECT DISTINCT g_uniq FROM bench) s;"),
 ("D3 sort+GroupAggregate, 1k groups", "SET enable_hashagg = off;",
    "SELECT count(*) FROM (SELECT g_1k, sum(v) FROM bench GROUP BY g_1k) s;"),
 ("D4 ORDER BY numeric, 5M rows", "",
    "SELECT g_uniq FROM bench ORDER BY g_uniq OFFSET 4999999;"),
 ("D5 seqscan filter =  (numeric_eq, ~all false)", "",
    "SELECT count(*) FROM bench WHERE g_uniq = 12345.67;"),
 ("D6 seqscan filter >  (numeric_gt, all rows)", "",
    "SELECT count(*) FROM bench WHERE g_uniq > 0;"),
 ("D7 hash_numeric() over 5M rows", "",
    "SELECT sum(hash_numeric(g_uniq)::bigint) FROM bench;"),
]
