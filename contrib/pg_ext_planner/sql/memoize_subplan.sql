--
-- Many examples here are also an evidence of the core pull-up technique limits
-- That means some tests will change along further core improvements.
--
-- NOTE: Each explained test case should be followed by the executed one to
-- check that the memoization machinery really works. Don't forget we need some
-- produced tuples as an evidence!
--

LOAD 'pg_ext_planner';

-- Don't trust default settings, remember installcheck
SET pg_ext_planner.memoize_subplan = 'on';

-- Put back any costing issues inventing memoization tests
SET pg_ext_planner.force_memoize_subplan = 'on';

-- Set random_page_cost to its default value since it can be different in a real cluster installation
SET random_page_cost = 4.0;

-- Set DateStyle for explain output
SET DateStyle = 'ISO, YMD';

CREATE TABLE sub_tbl (x bigint);
INSERT INTO sub_tbl (x)
  SELECT value FROM generate_series(1,100) AS value;

CREATE TABLE upper_tbl (x bigint, y bigint, z numeric);
INSERT INTO upper_tbl (x,y,z)
  SELECT value % 5, value % 3, value FROM generate_series(1,10) AS value;
VACUUM ANALYZE sub_tbl, upper_tbl;

-- The core memoize switch off must disable our feature too.
SET enable_memoize = 'off';
EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(*) FROM upper_tbl u
WHERE u.y < (SELECT avg(s.x) FROM sub_tbl s WHERE s.x=u.x);
-- Set enable_memoize into 'on' value - in case the instance settings are different.
SET enable_memoize = 'on';

CREATE TABLE int_tbl(id int, val int);
INSERT INTO int_tbl
SELECT v.id
     , g.val
  FROM (VALUES(0, 10), (1, 41), (2, 11), (3, 71), (4, 121)) v(id, num)
  JOIN generate_series(v.num, v.num + 20) AS g(val)
    ON (1 = 1);

CREATE INDEX int_tbl_id_ix ON int_tbl(id);

VACUUM (ANALYZE) int_tbl;

CREATE TABLE dt_tbl(id int, dt date);
INSERT INTO dt_tbl
SELECT v.id
     , g.ts::date AS dt
  FROM (VALUES (0, make_date(2016, 1, 14), 50),
               (1, make_date(2017, 2, 19), 60),
               (2, make_date(2018, 3, 10), 20),
               (3, make_date(2019, 2, 28), 40),
               (4, make_date(2020, 12, 28), 35),
               (5, make_date(2021, 11, 22), 70),
               (6, make_date(2022, 1, 20), 34),
               (7, make_date(2023, 5, 1), 38),
               (8, make_date(2024, 4, 1), 110),
               (9, make_date(2013, 4, 1), 60)
       ) v(id, dt, delta)
  JOIN generate_series((v.dt - delta)::timestamp, v.dt::timestamp, interval '1 day') AS g(ts)
    ON (1 = 1);

CREATE INDEX dt_tbl_id_ix ON dt_tbl(id);

VACUUM (ANALYZE) dt_tbl;

CREATE TABLE main_tbl(id bigint, x bigint, y bigint, hundred int, thousand int, v int, dt date);

INSERT INTO main_tbl
SELECT g.val
     , g.val % 9 AS x
     , g.val % 5 AS y
     , g.val % 100 + 1 AS hundred
     , g.val % 1000 + 1 AS thousand
     , CASE WHEN (g.val % 5) % 2 != 0 THEN i.sm ELSE i.sm - 10 END AS v
     , v.dt - CASE WHEN (g.val % 9) % 2 = 0 THEN 0 ELSE 3 END AS ts
  FROM generate_series(1, 3000) AS g(val)
  JOIN (SELECT i.id, SUM(i.val) AS sm FROM int_tbl i GROUP BY i.id) i
    ON i.id = g.val % 5
  JOIN (VALUES (0, make_date(2016, 1, 14)),
               (1, make_date(2017, 2, 19)),
               (2, make_date(2018, 3, 10)),
               (3, make_date(2019, 2, 28)),
               (4, make_date(2020, 12, 28)),
               (5, make_date(2021, 11, 22)),
               (6, make_date(2022, 1, 20)),
               (7, make_date(2023, 5, 1)),
               (8, make_date(2024, 4, 1)),
               (9, make_date(2013, 4, 1))
       ) v(id, dt)
    ON g.val % 9 = v.id;

CREATE INDEX mt_id_ix ON main_tbl(id);

CREATE INDEX mt_hundred_ix ON main_tbl(hundred);

CREATE INDEX mt_thousand_ix ON main_tbl(thousand);

CREATE INDEX mt_dt_ix ON main_tbl(dt);

VACUUM (ANALYZE) main_tbl;

-- Simple BitmapHeapPath with Subplan
EXPLAIN (VERBOSE, COSTS OFF)
SELECT m.id, m.x, m.y, m.hundred, m.thousand, m.v
  FROM main_tbl m
 WHERE m.thousand BETWEEN 175 AND 177
   AND m.dt = (SELECT MAX(d.dt) FROM dt_tbl d WHERE d.id = m.x);

SELECT m.id, m.x, m.y, m.hundred, m.thousand, m.v
  FROM main_tbl m
 WHERE m.thousand BETWEEN 175 AND 177
   AND m.dt = (SELECT MAX(d.dt) FROM dt_tbl d WHERE d.id = m.x);

-- Bitmap with Subplan in its target list
EXPLAIN (VERBOSE, COSTS OFF)
SELECT (SELECT sum(s.x) FROM sub_tbl s WHERE s.x=m.x) AS res
  FROM main_tbl m
 WHERE m.thousand BETWEEN 175 AND 177;

SELECT (SELECT sum(s.x) FROM sub_tbl s WHERE s.x=m.x) AS res
  FROM main_tbl m
 WHERE m.thousand BETWEEN 175 AND 177;

-- BitmapAnd and Subplan
EXPLAIN (VERBOSE, COSTS OFF)
SELECT m.id, m.x, m.y, m.hundred, m.thousand, m.v
  FROM main_tbl m
 WHERE m.hundred IN (1, 2)
   AND m.v = (SELECT SUM(i.val) FROM int_tbl i WHERE i.id = m.y)
   AND m.dt BETWEEN '2021-11-19'::date AND '2021-11-20'::date;

SELECT m.id, m.x, m.y, m.hundred, m.thousand, m.v
  FROM main_tbl m
 WHERE m.hundred IN (1, 2)
   AND m.v = (SELECT SUM(i.val) FROM int_tbl i WHERE i.id = m.y)
   AND m.dt BETWEEN '2021-11-19'::date AND '2021-11-20'::date;

-- BitmapOr and Subplan
EXPLAIN (VERBOSE, COSTS OFF)
SELECT m.id, m.x, m.y, m.hundred, m.thousand, m.v
  FROM main_tbl m
 WHERE (m.hundred < 2 OR m.thousand < 3)
   AND m.v = (SELECT SUM(i.val) FROM int_tbl i WHERE i.id = m.y);

SELECT m.id, m.x, m.y, m.hundred, m.thousand, m.v
  FROM main_tbl m
 WHERE (m.hundred < 2 OR m.thousand < 3)
   AND m.v = (SELECT SUM(i.val) FROM int_tbl i WHERE i.id = m.y);

-- BitmapOr where each clause has its own subplan
EXPLAIN (VERBOSE, COSTS OFF)
SELECT m.*
  FROM main_tbl m
 WHERE (m.hundred = 35 AND m.v = (SELECT SUM(i.val) FROM int_tbl i WHERE i.id = m.y)) OR
       (m.thousand = 175 AND m.dt = (SELECT MAX(d.dt) FROM dt_tbl d WHERE d.id = m.x));

SELECT m.*
  FROM main_tbl m
 WHERE (m.hundred = 35 AND m.v = (SELECT SUM(i.val) FROM int_tbl i WHERE i.id = m.y)) OR
       (m.thousand = 175 AND m.dt = (SELECT MAX(d.dt) FROM dt_tbl d WHERE d.id = m.x));

-- BitmapAnd where each clause has its own subplan
EXPLAIN (VERBOSE, COSTS OFF)
SELECT m.thousand
  FROM main_tbl m
 WHERE (m.hundred BETWEEN 35 AND 55 AND m.v = (SELECT SUM(i.val) FROM int_tbl i WHERE i.id = m.y)) AND
       (m.thousand BETWEEN 150 AND 170 AND m.dt = (SELECT MAX(d.dt) FROM dt_tbl d WHERE d.id = m.x));

SELECT m.thousand
  FROM main_tbl m
 WHERE (m.hundred BETWEEN 35 AND 55 AND m.v = (SELECT SUM(i.val) FROM int_tbl i WHERE i.id = m.y)) AND
       (m.thousand BETWEEN 150 AND 170 AND m.dt = (SELECT MAX(d.dt) FROM dt_tbl d WHERE d.id = m.x));

-- BitmapAnd and BitmapOr under it
EXPLAIN (VERBOSE, COSTS OFF)
SELECT m.*
  FROM main_tbl m
 WHERE m.hundred BETWEEN 42 AND 60
   AND ((m.thousand BETWEEN 42 AND 46 AND m.dt = (SELECT MAX(d.dt) FROM dt_tbl d WHERE d.id = m.x)) OR
        (m.thousand BETWEEN 59 AND 62 AND m.v = (SELECT SUM(i.val) FROM int_tbl i WHERE i.id = m.y)));

SELECT m.*
  FROM main_tbl m
 WHERE m.hundred BETWEEN 42 AND 60
   AND ((m.thousand BETWEEN 42 AND 46 AND m.dt = (SELECT MAX(d.dt) FROM dt_tbl d WHERE d.id = m.x)) OR
        (m.thousand BETWEEN 59 AND 62 AND m.v = (SELECT SUM(i.val) FROM int_tbl i WHERE i.id = m.y)));

-- BitmapOr + BitmapAnd + BitmapOr
EXPLAIN (VERBOSE, COSTS OFF)
SELECT m.thousand
  FROM main_tbl m
 WHERE m.hundred = 42
   AND (m.thousand = 42 AND m.v = (SELECT SUM(i.val) FROM int_tbl i WHERE i.id = m.y) OR
        m.thousand = 842 AND m.dt = (SELECT MAX(d.dt) FROM dt_tbl d WHERE d.id = m.x)
        ) OR
        thousand = 10 AND m.v = (SELECT SUM(i.val) FROM int_tbl i WHERE i.id = m.y);

SELECT m.thousand
  FROM main_tbl m
 WHERE m.hundred = 42
   AND (m.thousand = 42 AND m.v = (SELECT SUM(i.val) FROM int_tbl i WHERE i.id = m.y) OR
        m.thousand = 842 AND m.dt = (SELECT MAX(d.dt) FROM dt_tbl d WHERE d.id = m.x)
        ) OR
        thousand = 10 AND m.v = (SELECT SUM(i.val) FROM int_tbl i WHERE i.id = m.y);

EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(*) FROM upper_tbl u
WHERE u.y < (SELECT avg(s.x) FROM sub_tbl s WHERE s.x=u.x);
SELECT count(*) FROM upper_tbl u
WHERE u.y < (SELECT avg(s.x) FROM sub_tbl s WHERE s.x=u.x);

-- Check ANY SubPlan inside a HAVING qual
EXPLAIN (COSTS OFF)
SELECT val.name FROM (VALUES ('pg_index', 1), ('pg_fake', 1), ('pg_class', 1)) AS val(name, num)
GROUP BY val.name
HAVING val.name IN (
  SELECT relname FROM pg_class s WHERE val.name LIKE '%_%');

SELECT val.name FROM (VALUES ('pg_index', 1), ('pg_fake', 1), ('pg_class', 1)) AS val(name, num)
GROUP BY val.name
HAVING val.name IN (
  SELECT relname FROM pg_class s WHERE val.name LIKE '%_%');

-- Create partial index with single condition
CREATE INDEX idx1 ON upper_tbl (z) WHERE (z <= 1);

EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(*) FROM upper_tbl u
WHERE u.y < (SELECT avg(s.x) FROM sub_tbl s WHERE s.x=u.x) AND u.z=1;

SELECT count(*) FROM upper_tbl u
WHERE u.y < (SELECT avg(s.x) FROM sub_tbl s WHERE s.x=u.x) AND u.z=1;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(*) FROM upper_tbl u
WHERE u.y < (SELECT avg(s.x) FROM sub_tbl s WHERE s.x=u.x) AND u.z<=1;

SELECT count(*) FROM upper_tbl u
WHERE u.y < (SELECT avg(s.x) FROM sub_tbl s WHERE s.x=u.x) AND u.z<=1;

DROP INDEX idx1;

-- Create partial index with multiple conditions
CREATE INDEX idx1 ON upper_tbl (z) WHERE (y>=3 and z>=11);

INSERT INTO upper_tbl (x,y,z) VALUES (5,3,11);
INSERT INTO upper_tbl (x,y,z) VALUES (5,3,12);
INSERT INTO upper_tbl (x,y,z) VALUES (5,3,11);

ANALYZE upper_tbl;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(*) FROM upper_tbl u
WHERE u.y > (SELECT avg(s.x) FROM sub_tbl s WHERE s.x=u.x) AND u.y=3 and u.z=11;

SELECT count(*) FROM upper_tbl u
WHERE u.y > (SELECT avg(s.x) FROM sub_tbl s WHERE s.x=u.x) AND u.y=3 and u.z=11;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(*) FROM upper_tbl u
WHERE u.y > (SELECT avg(s.x) FROM sub_tbl s WHERE s.x=u.x) AND u.y=3 and u.z>=11;

SELECT count(*) FROM upper_tbl u
WHERE u.y > (SELECT avg(s.x) FROM sub_tbl s WHERE s.x=u.x) AND u.y=3 and u.z>=11;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(*) FROM upper_tbl u
WHERE u.y > (SELECT avg(s.x) FROM sub_tbl s WHERE s.x=u.x) AND u.y=3 and u.z>12;

SELECT count(*) FROM upper_tbl u
WHERE u.y > (SELECT avg(s.x) FROM sub_tbl s WHERE s.x=u.x) AND u.y=3 and u.z>12;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(*) FROM upper_tbl u
WHERE u.y > (SELECT avg(s.x) FROM sub_tbl s WHERE s.x=u.x) AND u.y>=3 and u.z=11;

SELECT count(*) FROM upper_tbl u
WHERE u.y > (SELECT avg(s.x) FROM sub_tbl s WHERE s.x=u.x) AND u.y>=3 and u.z=11;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(*) FROM upper_tbl u join sub_tbl on sub_tbl.x = u.y
WHERE u.y > (SELECT avg(s.x) FROM sub_tbl s WHERE s.x=u.x) AND u.y>=3 and u.z=11;

DROP INDEX idx1;

-- Index Only Scan by idx2
CREATE INDEX idx1 ON upper_tbl (x);
CREATE INDEX idx2 ON upper_tbl (y);
CREATE INDEX idx3 ON upper_tbl (z);
set enable_seqscan = off;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t.y, (SELECT sum(t2.y) AS res FROM upper_tbl t2 WHERE t2.x = t.y) FROM upper_tbl t;

SELECT t.y, (SELECT sum(t2.y) AS res FROM upper_tbl t2 WHERE t2.x = t.y) FROM upper_tbl t;

reset enable_seqscan;

-- Memoize with Index Scan by idx3
EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(*) FROM upper_tbl u
WHERE u.y < (SELECT avg(s.x) FROM sub_tbl s WHERE s.x=u.x) AND u.z=1;

SELECT count(*) FROM upper_tbl u
WHERE u.y < (SELECT avg(s.x) FROM sub_tbl s WHERE s.x=u.x) AND u.z=1;

explain (costs off)
select * from
  upper_tbl t1 left join
  (select x as q1, 42 as q2 from upper_tbl t2) ss
  on t1.y = ss.q1
where
  1 = (select 1 from upper_tbl t3 where ss.q2 is not null limit 1)
order by 1,2;

select * from
  upper_tbl t1 left join
  (select x as q1, 42 as q2 from upper_tbl t2) ss
  on t1.y = ss.q1
where
  1 = (select 1 from upper_tbl t3 where ss.q2 is not null limit 1)
order by 1,2;

DROP INDEX idx1;
DROP INDEX idx2;
DROP INDEX idx3;

DELETE FROM upper_tbl WHERE x >= 5;

-- exercise rescan code path via a repeatedly-evaluated subquery
explain (costs off)
SELECT
    ARRAY(SELECT f.i FROM (
        (SELECT d + g.i FROM generate_series(4, 30, 3) d ORDER BY 1)
        UNION ALL
        (SELECT d + g.i FROM generate_series(0, 30, 5) d ORDER BY 1)
    ) f(i)
    ORDER BY f.i LIMIT 10)
FROM generate_series(1, 3) g(i);

-- Do not memoize a subplan containing grouping sets
explain (costs off)
  select v.c, (select count(*) from upper_tbl group by () having v.c)
    from (values (false),(true)) v(c) order by v.c;

-- use limit to force Subplan usage
explain (costs off)
select * from upper_tbl o where exists
  (select 1 from upper_tbl i where i.x=o.x limit 0);

-- No memoize since we change upper_tbl data
CREATE VIEW rw_view1 AS
  SELECT * FROM upper_tbl b
  WHERE EXISTS(SELECT 1 FROM upper_tbl r WHERE r.x = b.x)
  WITH CHECK OPTION;

EXPLAIN (costs off) INSERT INTO rw_view1 VALUES (5, null, null);
EXPLAIN (costs off) UPDATE rw_view1 SET x = x + 5;

DROP view rw_view1;

-- Index Scan with memoize
CREATE INDEX idx1 ON upper_tbl (z);

EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(*) FROM upper_tbl u
WHERE u.y < (SELECT avg(s.x) FROM sub_tbl s WHERE s.x=u.x) AND u.z=1;

-- Bitmap Heap Scan with memoize
SET enable_indexscan = 'off';

EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(*) FROM upper_tbl u
WHERE u.y < (SELECT avg(s.x) FROM sub_tbl s WHERE s.x=u.x) AND u.z=1;

RESET enable_indexscan;

DROP INDEX idx1;

--
-- Multiple subplans in the expression
--

EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(*) FROM upper_tbl u
WHERE
  u.y < (SELECT avg(s.x) FROM sub_tbl s WHERE s.x=u.x) AND
  (u.y + 1) < (SELECT avg(s.x) FROM sub_tbl s WHERE s.x=u.x);
SELECT count(*) FROM upper_tbl u
WHERE
  u.y < (SELECT avg(s.x) FROM sub_tbl s WHERE s.x=u.x) AND
  (u.y + 1) < (SELECT avg(s.x) FROM sub_tbl s WHERE s.x=u.x);

EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(*) FROM upper_tbl u
WHERE u.y < (SELECT avg(s.x) FROM sub_tbl s WHERE s.x=u.x) OR (
  (u.y + 1) < (SELECT avg(s.x) FROM sub_tbl s WHERE s.x=u.x) AND
  (u.y - 1) < (SELECT avg(s.x) FROM sub_tbl s WHERE s.x=u.x)
);
SELECT count(*) FROM upper_tbl u
WHERE u.y < (SELECT avg(s.x) FROM sub_tbl s WHERE s.x=u.x) OR (
  (u.y + 1) < (SELECT avg(s.x) FROM sub_tbl s WHERE s.x=u.x) AND
  (u.y - 1) < (SELECT avg(s.x) FROM sub_tbl s WHERE s.x=u.x)
);
-- Just a bit more complicated: with mixed vars and exprs
EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(*) FROM upper_tbl u
WHERE u.y < (SELECT avg(s.x) FROM sub_tbl s WHERE s.x=u.x) OR (
  (u.x + 1) < (SELECT avg(s.x) FROM sub_tbl s WHERE s.x=u.y) AND
  (u.y - 1) < (SELECT avg(s.x) FROM sub_tbl s WHERE s.x=u.x/2)
);
SELECT count(*) FROM upper_tbl u
WHERE u.y < (SELECT avg(s.x) FROM sub_tbl s WHERE s.x=u.x) OR (
  (u.x + 1) < (SELECT avg(s.x) FROM sub_tbl s WHERE s.x=u.y) AND
  (u.y - 1) < (SELECT avg(s.x) FROM sub_tbl s WHERE s.x=u.x/2)
);

-- TODO: Memoize multi-level references
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM upper_tbl u
WHERE u.y < (SELECT avg(s.x) FROM sub_tbl s
  WHERE s.x < (SELECT avg(u2.x) FROM upper_tbl u2
    WHERE u.x <> s.x OR u2.x < u.y));
 -- Check result's consistency among executions with enabled and disabled feature
SET pg_ext_planner.memoize_subplan = 'off';
SELECT * FROM upper_tbl u
WHERE u.y < (SELECT avg(s.x) FROM sub_tbl s
  WHERE s.x < (SELECT avg(u2.x) FROM upper_tbl u2
    WHERE u.x <> s.x OR u2.x < u.y));
SET pg_ext_planner.memoize_subplan = 'on';
SELECT * FROM upper_tbl u
WHERE u.y < (SELECT avg(s.x) FROM sub_tbl s
  WHERE s.x < (SELECT avg(u2.x) FROM upper_tbl u2
    WHERE u.x <> s.x OR u2.x < u.y));

-- Don't memoize subplan which refers an upper-level parent except the immediate
-- one. XXX: may we redesign that part?
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM upper_tbl u
WHERE
  u.y < (
    SELECT avg(s.x) FROM sub_tbl s
    WHERE s.x < (
      SELECT avg(u2.x) FROM upper_tbl u2
      WHERE u.x <> s.x OR u2.x < u.y)) OR
        u.y < (SELECT avg(s.x) FROM sub_tbl s WHERE s.x < u.z);
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM upper_tbl u
WHERE
  u.y < (SELECT avg(s.x) FROM sub_tbl s WHERE s.x < u.z) OR
  u.y < (SELECT avg(s.x) FROM sub_tbl s
  WHERE s.x < (SELECT avg(u2.x) FROM upper_tbl u2
    WHERE u.x <> s.x OR u2.x < u.y));

-- Subplan inside a target list
EXPLAIN (VERBOSE, COSTS OFF)
SELECT (SELECT avg(s.x) FROM sub_tbl s WHERE s.x=u.x) FROM upper_tbl u;

-- Subplan in a JOIN clause
SET enable_material = 'off';
EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(*) FROM upper_tbl u1, upper_tbl u2
WHERE u1.y < (SELECT avg(s.x) FROM sub_tbl s WHERE s.x=u2.x) AND
  u1.y < (SELECT avg(s.x) FROM sub_tbl s WHERE s.x=u1.x);

SET enable_nestloop = 'off';
EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(*) FROM upper_tbl u1, upper_tbl u2
WHERE
  u1.y < (SELECT avg(s.x) FROM sub_tbl s WHERE s.x=u2.x) AND -- Join filter
  u1.y < (SELECT avg(s.x) FROM sub_tbl s WHERE s.x=u1.x) AND -- Scan filter
  u1.y = (SELECT avg(s.x) FROM sub_tbl s WHERE s.x=u2.x); -- Join clause

SET enable_hashjoin = 'off';
EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(*) FROM upper_tbl u1, upper_tbl u2
WHERE
  u1.y < (SELECT avg(s.x) FROM sub_tbl s WHERE s.x=u2.x) AND -- Join filter
  u1.y < (SELECT avg(s.x) FROM sub_tbl s WHERE s.x=u1.x) AND -- Scan filter
  u1.y = (SELECT avg(s.x) FROM sub_tbl s WHERE s.x=u2.x); -- Join clause

--
-- Check cost model
--

SET pg_ext_planner.force_memoize_subplan = 'off';
EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(*) FROM upper_tbl u
WHERE u.y = (SELECT avg(s.x) FROM sub_tbl s WHERE s.x=u.x);
RESET enable_indexscan;

RESET enable_material;
RESET enable_nestloop;
RESET enable_hashjoin;
RESET pg_ext_planner.memoize_subplan;
RESET pg_ext_planner.force_memoize_subplan;
RESET random_page_cost;
RESET DateStyle;

DROP TABLE sub_tbl, upper_tbl, int_tbl, dt_tbl, main_tbl CASCADE;