\set N 5000000
\set NPAR 3000000

-- =====================================================================
-- agg: the workhorse.  Grouping keys of every cardinality, plus value
-- columns chosen against the fast path's documented eligibility rules.
-- =====================================================================
DROP TABLE IF EXISTS agg;
CREATE TABLE agg AS
SELECT i,
  1.00::numeric(15,2)                                          AS g_one,
  (10000 + ((i % 1000)::numeric)/100)::numeric(15,2)           AS g_1k,
  -- 80/20: 200 of the 1000 groups hold 80% of the rows
  (CASE WHEN i % 5 <> 0
        THEN (10000 + (((i / 5) % 200)::numeric)/100)
        ELSE (10000 + ((200 + ((i / 5) % 800))::numeric)/100)
   END)::numeric(15,2)                                         AS g_skew,
  (((i % 592000)::numeric)/100)::numeric(15,2)                 AS g_592k,
  ((i::numeric)/100)::numeric(15,2)                            AS g_uniq,
  (i % 1000)::int8                                             AS g_1k_int,
  -- int64-lane eligible: <= 4 stored digits, coefficient fits int64
  (((i % 99999999)::numeric)/100)::numeric(10,2)               AS v_narrow,
  (((i * 7 % 99999999)::numeric)/100)::numeric(10,2)           AS v2,
  (((i * 11 % 99999999)::numeric)/100)::numeric(10,2)          AS v3,
  (((i * 13 % 99999999)::numeric)/100)::numeric(10,2)          AS v4,
  (((i * 17 % 99999999)::numeric)/100)::numeric(10,2)          AS v5,
  (((i * 19 % 99999999)::numeric)/100)::numeric(10,2)          AS v6,
  (((i * 23 % 99999999)::numeric)/100)::numeric(10,2)          AS v7,
  -- alternating display scale: forces a scale-up conversion on every
  -- other row once the accumulator has widened to dscale 6
  (CASE WHEN i % 2 = 0
        THEN (((i % 99999999)::numeric)/100)::numeric(16,2)::numeric
        ELSE (((i % 99999999)::numeric)/1000000)::numeric(16,6)::numeric
   END)                                                        AS v_mixscale
FROM generate_series(1, :N) i;

-- =====================================================================
-- wide: values that miss the int64 lane.  This is where a fast path that
-- is not profitable would show up as a regression.
-- =====================================================================
DROP TABLE IF EXISTS wide;
CREATE TABLE wide AS
SELECT i,
  1.00::numeric(15,2)                                          AS g_one,
  (10000 + ((i % 1000)::numeric)/100)::numeric(15,2)           AS g_1k,
  -- 25 digits -> 7 stored digits: passes the width cap, skips the int64
  -- lane, takes a checked 128-bit op per digit, and NEVER overflows
  -- (5e6 * 1e25 = 5e31, int128 holds ~1.7e38).  The per-row worst case.
  (repeat('9', 20) || lpad((i % 100000)::text, 5, '0'))::numeric   AS v_slow,
  -- 45 digits -> rejected by the cheap width cap on the first row, so the
  -- state promotes immediately and every later row is pure branch cost
  (repeat('9', 40) || lpad((i % 100000)::text, 5, '0'))::numeric   AS v_huge,
  -- numeric(32,2): the commit's own overflow example.  One group's sum
  -- crosses int128 after ~1.7M rows, forcing a mid-scan promotion.
  ((repeat('9', 28) || lpad((i % 100)::text, 2, '0'))::numeric
     + 0.25)::numeric(32,2)                                    AS v_ovf
FROM generate_series(1, :N) i;

-- =====================================================================
-- par: 3M rows / 592k groups / 7 numeric aggregates -- the exact shape
-- quoted in the serialization commit's message.
-- =====================================================================
DROP TABLE IF EXISTS par;
CREATE TABLE par AS
SELECT i,
  (((i % 592000)::numeric)/100)::numeric(15,2)                 AS g,
  (((i % 99999999)::numeric)/100)::numeric(10,2)               AS c1,
  (((i * 7 % 99999999)::numeric)/100)::numeric(10,2)           AS c2,
  (((i * 11 % 99999999)::numeric)/100)::numeric(10,2)          AS c3,
  (((i * 13 % 99999999)::numeric)/100)::numeric(10,2)          AS c4,
  (((i * 17 % 99999999)::numeric)/100)::numeric(10,2)          AS c5,
  (((i * 19 % 99999999)::numeric)/100)::numeric(10,2)          AS c6,
  (((i * 23 % 99999999)::numeric)/100)::numeric(10,2)          AS c7,
  -- wide enough that each group's partial sum promotes: exercises the
  -- serialized digit-array format rather than the fast one
  ((repeat('9', 28) || lpad((i % 100)::text, 2, '0'))::numeric
     + 0.25)::numeric(32,2)                                    AS c_ovf,
  -- 45 digits: rejected by the width cap, so every partial state promotes on
  -- its first row and must be serialized in the historical digit-array form
  (repeat('9', 40) || lpad((i % 100000)::text, 5, '0'))::numeric AS c_huge
FROM generate_series(1, :NPAR) i;

-- =====================================================================
-- key-shape tables for the hash/compare commit
-- =====================================================================
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

DROP TABLE IF EXISTS longkey;
CREATE TABLE longkey AS
SELECT i,
  (repeat('7', 196) || lpad((i % 1000)::text, 4, '0'))::numeric AS g_200,
  (((i % 99999999)::numeric)/100)::numeric(10,2)                AS v
FROM generate_series(1, 5000000) i;

DROP TABLE IF EXISTS toastkey;
CREATE TABLE toastkey AS
SELECT i,
  (repeat('7', 7996) || lpad((i % 1000)::text, 4, '0'))::numeric AS g_big
FROM generate_series(1, 200000) i;

DROP TABLE IF EXISTS dim1k;
CREATE TABLE dim1k AS SELECT DISTINCT g_1k FROM agg;

VACUUM ANALYZE;
CHECKPOINT;
