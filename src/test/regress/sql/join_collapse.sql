--
-- Test join_collapse_limit_scale: internally-generated joins from sublink
-- pullup should get extra headroom when the scale factor exceeds 1.0.
--
CREATE TABLE jcls_t (id int PRIMARY KEY);
CREATE TABLE jcls_big (id int);
CREATE TABLE jcls_small (id int PRIMARY KEY);

INSERT INTO jcls_t SELECT g FROM generate_series(1, 1000) g;
INSERT INTO jcls_big SELECT g FROM generate_series(1, 1000) g;
INSERT INTO jcls_small VALUES (1);

ANALYZE jcls_t;
ANALYZE jcls_big;
ANALYZE jcls_small;

SET join_collapse_limit = 2;
SET from_collapse_limit = 2;

-- scale=1.0: feature disabled, second sublink can't be merged into the
-- same join problem.  Join order is constrained by the tree structure:
-- (jcls_t X jcls_big) X jcls_small.
SET join_collapse_limit_scale = 1.0;
EXPLAIN (COSTS OFF)
SELECT * FROM jcls_t
WHERE jcls_t.id IN (SELECT jcls_big.id FROM jcls_big)
  AND jcls_t.id IN (SELECT jcls_small.id FROM jcls_small);

-- scale=1.5: feature enabled, all three relations are at the same level.
-- The planner can reorder freely and should pick jcls_t X jcls_small first
-- (1 row) rather than jcls_t X jcls_big (1000 rows).
SET join_collapse_limit_scale = 1.5;
EXPLAIN (COSTS OFF)
SELECT * FROM jcls_t
WHERE jcls_t.id IN (SELECT jcls_big.id FROM jcls_big)
  AND jcls_t.id IN (SELECT jcls_small.id FROM jcls_small);

-- Here we add RHS of JOIN_SEMI as a last element that is out of standard
-- joinlist size, but scale factor allows to take it into account (new feature).
SET join_collapse_limit_scale = 1.0;
EXPLAIN (COSTS OFF)
SELECT * FROM jcls_t t, jcls_big b
WHERE t.id = b.id AND
  t.id IN (SELECT jcls_small.id FROM jcls_small);
SET join_collapse_limit_scale = 1.5;
EXPLAIN (COSTS OFF)
SELECT * FROM jcls_t t, jcls_big b
WHERE t.id = b.id AND
  t.id IN (SELECT jcls_small.id FROM jcls_small);

RESET join_collapse_limit;
RESET join_collapse_limit_scale;

DROP TABLE jcls_t, jcls_big, jcls_small;
