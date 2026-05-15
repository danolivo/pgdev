--
-- test_plan_cache: cursor_options vs plan_cache_mode precedence
--
-- Verifies that an SPI caller's explicit CURSOR_OPT_GENERIC_PLAN or
-- CURSOR_OPT_CUSTOM_PLAN takes precedence over the plan_cache_mode GUC,
-- and that the GUC remains authoritative when no caller flag is set.
--

CREATE EXTENSION test_plan_cache;

-- A small but real table with statistics, so the planner has something
-- concrete to plan against in both generic and custom shapes.
CREATE TABLE tpc(id int PRIMARY KEY, val text);
INSERT INTO tpc SELECT g, 'row-' || g FROM generate_series(1, 1000) g;
ANALYZE tpc;

-- Caller asks for a generic plan; result must be generic regardless of
-- plan_cache_mode.
SET plan_cache_mode = force_custom_plan;
SELECT test_plan_cache_is_generic(
    'SELECT * FROM tpc WHERE id = $1', true, false, 42)
    AS request_generic_under_force_custom;

SET plan_cache_mode = 'auto';
SELECT test_plan_cache_is_generic(
    'SELECT * FROM tpc WHERE id = $1', true, false, 42)
    AS request_generic_under_auto;

SET plan_cache_mode = force_generic_plan;
SELECT test_plan_cache_is_generic(
    'SELECT * FROM tpc WHERE id = $1', true, false, 42)
    AS request_generic_under_force_generic;

-- Caller asks for a custom plan; result must be custom regardless of
-- plan_cache_mode.
SET plan_cache_mode = force_generic_plan;
SELECT test_plan_cache_is_generic(
    'SELECT * FROM tpc WHERE id = $1', false, true, 42)
    AS request_custom_under_force_generic;

SET plan_cache_mode = 'auto';
SELECT test_plan_cache_is_generic(
    'SELECT * FROM tpc WHERE id = $1', false, true, 42)
    AS request_custom_under_auto;

SET plan_cache_mode = force_custom_plan;
SELECT test_plan_cache_is_generic(
    'SELECT * FROM tpc WHERE id = $1', false, true, 42)
    AS request_custom_under_force_custom;

-- With no caller hint, plan_cache_mode stays authoritative.
SET plan_cache_mode = force_generic_plan;
SELECT test_plan_cache_is_generic(
    'SELECT * FROM tpc WHERE id = $1', false, false, 42)
    AS no_hint_under_force_generic;

SET plan_cache_mode = force_custom_plan;
SELECT test_plan_cache_is_generic(
    'SELECT * FROM tpc WHERE id = $1', false, false, 42)
    AS no_hint_under_force_custom;

DROP TABLE tpc;
DROP EXTENSION test_plan_cache;
