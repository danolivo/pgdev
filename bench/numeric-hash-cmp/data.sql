\set N 5000000

DROP TABLE IF EXISTS bench;
CREATE TABLE bench AS
SELECT i,
       1.00::numeric(15,2)                                        AS g_one,
       ((i::numeric)/100)::numeric(15,2)                          AS g_uniq,
       (((i % 100000)::numeric)/100)::numeric(15,2)               AS g_100k,
       (10000 + ((i % 1000)::numeric)/100)::numeric(15,2)         AS g_1k,
       -- 80/20: 200 of the 1000 groups hold 80% of the rows
       (CASE WHEN i % 5 <> 0
             THEN (10000 + (((i / 5) % 200)::numeric)/100)
             ELSE (10000 + ((200 + ((i / 5) % 800))::numeric)/100)
        END)::numeric(15,2)                                       AS g_skew,
       ((i % 997)::numeric + 0.25)::numeric(15,2)                 AS v,
       (i % 1000)::int8                                           AS g_1k_int
FROM generate_series(1, :N) i;

-- grouping key where numerically-equal values differ bytewise: same length,
-- different display scale, so the memcmp fast path runs and always misses
DROP TABLE IF EXISTS bench_mix;
CREATE TABLE bench_mix AS
SELECT i,
       (CASE WHEN i % 7 < 4
             THEN (((i % 1000)::numeric)/10)::numeric(20,1)::numeric
             ELSE (((i % 1000)::numeric)/10)::numeric(20,2)::numeric
        END)                                                      AS g_mix,
       ((i % 997)::numeric + 0.25)::numeric(15,2)                  AS v
FROM generate_series(1, :N) i;

-- long numerics: 60-digit (fits the local buffer), 200-digit (falls back)
DROP TABLE IF EXISTS bench_long;
CREATE TABLE bench_long AS
SELECT i,
       (repeat('7', 56) || lpad((i % 1000)::text, 4, '0'))::numeric   AS g_60,
       (repeat('7', 196) || lpad((i % 1000)::text, 4, '0'))::numeric  AS g_200,
       ((i % 997)::numeric + 0.25)::numeric(15,2)                     AS v
FROM generate_series(1, :N) i;

-- numerics big enough to be toasted/compressed (VARATT_IS_COMPRESSED/EXTERNAL)
DROP TABLE IF EXISTS bench_toast;
CREATE TABLE bench_toast AS
SELECT i,
       (repeat('7', 7996) || lpad((i % 1000)::text, 4, '0'))::numeric AS g_big
FROM generate_series(1, 200000) i;

-- 20 numeric columns, 1M rows, 1000 groups (the case quoted in the commit message)
DROP TABLE IF EXISTS bench20;
CREATE TABLE bench20 AS
SELECT i,
  (10000 + ((i%1000)::numeric)/100)::numeric(15,2) AS c1,
  (20000 + ((i%1000)::numeric)/100)::numeric(15,2) AS c2,
  (30000 + ((i%1000)::numeric)/100)::numeric(15,2) AS c3,
  (40000 + ((i%1000)::numeric)/100)::numeric(15,2) AS c4,
  (50000 + ((i%1000)::numeric)/100)::numeric(15,2) AS c5,
  (60000 + ((i%1000)::numeric)/100)::numeric(15,2) AS c6,
  (70000 + ((i%1000)::numeric)/100)::numeric(15,2) AS c7,
  (80000 + ((i%1000)::numeric)/100)::numeric(15,2) AS c8,
  (90000 + ((i%1000)::numeric)/100)::numeric(15,2) AS c9,
  (11000 + ((i%1000)::numeric)/100)::numeric(15,2) AS c10,
  (12000 + ((i%1000)::numeric)/100)::numeric(15,2) AS c11,
  (13000 + ((i%1000)::numeric)/100)::numeric(15,2) AS c12,
  (14000 + ((i%1000)::numeric)/100)::numeric(15,2) AS c13,
  (15000 + ((i%1000)::numeric)/100)::numeric(15,2) AS c14,
  (16000 + ((i%1000)::numeric)/100)::numeric(15,2) AS c15,
  (17000 + ((i%1000)::numeric)/100)::numeric(15,2) AS c16,
  (18000 + ((i%1000)::numeric)/100)::numeric(15,2) AS c17,
  (19000 + ((i%1000)::numeric)/100)::numeric(15,2) AS c18,
  (21000 + ((i%1000)::numeric)/100)::numeric(15,2) AS c19,
  (22000 + ((i%1000)::numeric)/100)::numeric(15,2) AS c20
FROM generate_series(1, 1000000) i;

DROP TABLE IF EXISTS dim1k;
CREATE TABLE dim1k AS SELECT DISTINCT g_1k FROM bench;

VACUUM ANALYZE;
CHECKPOINT;
