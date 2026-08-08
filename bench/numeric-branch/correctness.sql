\pset pager off
-- =====================================================================
-- Aggregate-result equivalence corpus.  Everything is rendered ::text so
-- a difference in display scale counts as a difference, which is the
-- property the fast-sum commit claims to preserve exactly.
-- =====================================================================
DROP TABLE IF EXISTS nasty;
CREATE TABLE nasty (id serial, grp int, v numeric);

INSERT INTO nasty (grp, v)
-- scales 0..8 across several magnitudes, both signs
SELECT k % 17, (CASE WHEN k % 3 = 0 THEN -1 ELSE 1 END) *
       ((k::numeric * 1234567) / (10::numeric ^ (k % 9)))::numeric(30, 8)
FROM generate_series(1, 4000) k;

INSERT INTO nasty (grp, v)
-- values straddling the int64 lane boundary (4 vs 5 stored digits)
SELECT 100 + k % 7, (repeat('9', 12 + k % 12) || '.' || repeat('3', k % 5))::numeric
FROM generate_series(1, 600) k;

INSERT INTO nasty (grp, v)
-- values that pass the width cap but skip the int64 lane
SELECT 200 + k % 5, (repeat('8', 20 + k % 10))::numeric FROM generate_series(1, 400) k;

INSERT INTO nasty (grp, v)
-- values rejected by the width cap outright
SELECT 300 + k % 3, (repeat('7', 41 + k % 20))::numeric FROM generate_series(1, 200) k;

INSERT INTO nasty (grp, v) VALUES
-- exact int128 boundaries: the second row of each pair must overflow the
-- accumulator and force an exact promotion
 (400,  170141183460469231731687303715884105727),
 (400,  1),
 (401, -170141183460469231731687303715884105728),
 (401, -1),
 (402,  85070591730234615865843651857942052864),
 (402,  85070591730234615865843651857942052864),
 (403,  99999999999999999999999999999999999999.9),
 (403,  0.1),
-- zeros of assorted scales, and scale widening after the fact
 (404, 0), (404, 0.0), (404, 0.00000000), (404, -0),
 (405, 1), (405, 1.000000000000), (405, 0.000000000001),
-- specials
 (500, 'NaN'), (500, 1), (500, 2),
 (501, 'Infinity'), (501, 1),
 (502, '-Infinity'), (502, 1),
 (503, 'Infinity'), (503, '-Infinity'), (503, 1),
 (504, 'NaN'), (504, 'Infinity'),
-- nulls and an all-null group
 (505, NULL), (505, 5), (506, NULL), (506, NULL);

ANALYZE nasty;

-- ---------------------------------------------------------------------
SET max_parallel_workers_per_gather = 0;

SELECT 'sum grouped'      AS what, md5(string_agg(x, ',' ORDER BY g)) AS digest
  FROM (SELECT grp g, coalesce(sum(v)::text, 'NULL') x FROM nasty GROUP BY grp) t
UNION ALL
SELECT 'avg grouped',     md5(string_agg(x, ',' ORDER BY g))
  FROM (SELECT grp g, coalesce(avg(v)::text, 'NULL') x FROM nasty GROUP BY grp) t
UNION ALL
SELECT 'sum+avg total',   md5(coalesce(sum(v)::text,'N') || '|' || coalesce(avg(v)::text,'N')) FROM nasty
UNION ALL
SELECT 'variance family', md5(string_agg(x, ',' ORDER BY g))
  FROM (SELECT grp g, coalesce(var_pop(v)::text,'N') || '/' || coalesce(stddev_samp(v)::text,'N') ||
               '/' || coalesce(var_samp(v)::text,'N') x FROM nasty GROUP BY grp) t
UNION ALL
-- moving aggregate: exercises the inverse transition (retreat-and-rescan)
SELECT 'moving sum',      md5(string_agg(x, ',' ORDER BY id))
  FROM (SELECT id, coalesce(sum(v) OVER (PARTITION BY grp ORDER BY id
                            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)::text,'N') x FROM nasty) t
UNION ALL
SELECT 'moving avg wide', md5(string_agg(x, ',' ORDER BY id))
  FROM (SELECT id, coalesce(avg(v) OVER (PARTITION BY grp ORDER BY id
                            ROWS BETWEEN 50 PRECEDING AND 10 FOLLOWING)::text,'N') x FROM nasty) t
UNION ALL
-- the benchmark data itself, grouped every way the benchmark groups it
SELECT 'agg sums',        md5(string_agg(x, ',' ORDER BY g))
  FROM (SELECT g_1k g, sum(v_narrow)::text || '|' || avg(v_narrow)::text || '|' ||
               sum(v_mixscale)::text || '|' || avg(v_mixscale)::text x
        FROM agg GROUP BY g_1k) t
UNION ALL
SELECT 'agg total',       md5(sum(v_narrow)::text || '|' || avg(v_narrow)::text || '|' ||
                             sum(v_mixscale)::text || '|' || avg(v_mixscale)::text) FROM agg
UNION ALL
SELECT 'wide sums',       md5(sum(v_slow)::text || '|' || avg(v_slow)::text || '|' ||
                             sum(v_huge)::text || '|' || sum(v_ovf)::text || '|' || avg(v_ovf)::text) FROM wide
UNION ALL
SELECT 'wide grouped',    md5(string_agg(x, ',' ORDER BY g))
  FROM (SELECT g_1k g, sum(v_slow)::text || '|' || sum(v_ovf)::text x FROM wide GROUP BY g_1k) t;

-- ---------------------------------------------------------------------
-- Same aggregates under a parallel plan: this is what actually crosses
-- numeric_avg_serialize / deserialize / combine.
RESET max_parallel_workers_per_gather;
SET max_parallel_workers_per_gather = 3;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0.005;
SET min_parallel_table_scan_size = 0;

SELECT 'PAR sum grouped'  AS what, md5(string_agg(x, ',' ORDER BY g)) AS digest
  FROM (SELECT grp g, coalesce(sum(v)::text, 'NULL') x FROM nasty GROUP BY grp) t
UNION ALL
SELECT 'PAR avg grouped', md5(string_agg(x, ',' ORDER BY g))
  FROM (SELECT grp g, coalesce(avg(v)::text, 'NULL') x FROM nasty GROUP BY grp) t
UNION ALL
SELECT 'PAR agg sums',    md5(string_agg(x, ',' ORDER BY g))
  FROM (SELECT g, sum(c1)::text || '|' || sum(c2)::text || '|' || sum(c3)::text || '|' ||
               sum(c4)::text || '|' || sum(c5)::text || '|' || sum(c6)::text || '|' || sum(c7)::text ||
               '|' || sum(c_ovf)::text || '|' || avg(c_ovf)::text || '|' || sum(c_huge)::text x
        FROM par GROUP BY g) t
UNION ALL
SELECT 'PAR wide',        md5(string_agg(x, ',' ORDER BY g))
  FROM (SELECT g_1k g, sum(v_slow)::text || '|' || sum(v_ovf)::text || '|' || avg(v_slow)::text x
        FROM wide GROUP BY g_1k) t;
