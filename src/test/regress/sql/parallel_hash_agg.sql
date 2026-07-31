--
-- PARALLEL HASH AGGREGATE
--
-- Exercise parallel shared hash aggregation: all participating workers
-- cooperatively build one shared hash table (instead of each building a
-- private partial table that a serial Finalize step later combines), and
-- each participant then emits a disjoint share of the finished groups.
--

create schema parallel_hash_agg;
set search_path to parallel_hash_agg;

-- encourage parallel plans regardless of the machine's cost thresholds
set parallel_setup_cost = 0;
set parallel_tuple_cost = 0;
set min_parallel_table_scan_size = 0;
set max_parallel_workers_per_gather = 2;

-- Note the group count: the planner refuses the shared strategy below a
-- floor (SHARED_AGG_MIN_GROUPS), because with few groups the Finalize stage
-- it eliminates is cheap and the contention it adds is not.
create table pha_src (g int4, x int4, y int8, b bool);
insert into pha_src
  select i % 2000,
         (i % 7)::int4,
         (i % 13)::int8,
         (i % 2 = 0)
  from generate_series(1, 20000) i;
analyze pha_src;

--
-- 1. With the feature enabled, a query whose aggregates are all eligible
-- (fixed-length pass-by-value transition types: sum(int4), count(*),
-- min/max(int4), bool_and/bool_or) should produce a plain "Gather" over a
-- "Parallel HashAggregate" -- no "Partial"/"Finalize" split.
--
set enable_parallel_hash_agg = on;

explain (costs off)
  select g, sum(x), count(*), min(x), max(x), bool_and(b), bool_or(b)
  from pha_src group by g;

--
-- 2. Correctness: results must match what the classic Partial+Finalize
-- path produces.  Compute a reference with the feature disabled, then
-- compare against the feature-enabled results.
--
set enable_parallel_hash_agg = off;
create table pha_ref as
  select g, sum(x) as sx, count(*) as cnt, min(x) as mnx, max(x) as mxx,
         bool_and(b) as ba, bool_or(b) as bo
  from pha_src group by g;

set enable_parallel_hash_agg = on;
create table pha_shared as
  select g, sum(x) as sx, count(*) as cnt, min(x) as mnx, max(x) as mxx,
         bool_and(b) as ba, bool_or(b) as bo
  from pha_src group by g;

-- expect 0 rows: the two result sets must be identical
select count(*) as mismatched_rows from (
    select * from pha_ref except select * from pha_shared
    union all
    select * from pha_shared except select * from pha_ref
) diff;

--
-- 3. Eligibility of by-reference transition states, which are stored as
-- DSA blobs: min/max(numeric) qualify (their transition type is numeric
-- itself).  Use a table with many small groups, where the shared strategy
-- is the planner's own choice; compare against a copy that forbids
-- parallelism via its reloption.
--
create table pha_num (g int4, y int8, n numeric(12,3));
insert into pha_num
  select i % 5000, (i % 211)::int8, (i % 173) + (i % 97) * 0.001
  from generate_series(1, 20000) i;
analyze pha_num;

create table pha_num_ref as select * from pha_num;
alter table pha_num_ref set (parallel_workers = 0);
analyze pha_num_ref;

explain (costs off)
  select g, min(n), max(n), count(*)
  from pha_num group by g;

select count(*) as mismatched_rows from (
    (select g, min(n) mn, max(n) mx, count(*) c from pha_num group by g
     except
     select g, min(n), max(n), count(*) from pha_num_ref group by g)
    union all
    (select g, min(n), max(n), count(*) from pha_num_ref group by g
     except
     select g, min(n), max(n), count(*) from pha_num group by g)
) diff;

-- ... while the 'internal' pseudo-type must still fall back: the stock
-- sum(numeric) keeps its transition state in an opaque process-local
-- struct that no byte-copy can make shareable.  (avg(int4), by contrast,
-- uses a plain int8[] state and therefore qualifies.)
--
explain (costs off)
  select pha_src.g, sum(n), avg(x)
  from pha_src, pha_num where pha_num.g = pha_src.g group by pha_src.g;

--
-- 4. Rescan.  A Gather on the inner side of a nested loop is re-executed
-- once per outer row; each re-execution tears the shared table down and
-- builds a new one, so check that every pass produces the same answer.
--
set enable_material = off;

explain (costs off)
  select count(*) from
    (select g, sum(x) as sx from pha_src group by g) ss
    right join (values (1), (2), (3)) v(k) on true;

-- three identical passes over the same groups
select count(*) as nrows, sum(sx) as checksum from
  (select g, sum(x) as sx from pha_src group by g) ss
  right join (values (1), (2), (3)) v(k) on true;

-- ... which must be exactly three times one pass
select count(*) * 3 as nrows, sum(sx) * 3 as checksum from
  (select g, sum(x) as sx from pha_src group by g) ss;

reset enable_material;

--
-- 5. Force a spill: shrink the per-participant memory budget well below
-- what's needed to hold every group, using many more distinct groups and a
-- tiny work_mem/hash_mem_multiplier.  Confirm results are still correct and
-- that EXPLAIN ANALYZE reports the spill counters added for shared hash
-- aggregation.
--
create table pha_spill_src (g int4, x int4);
insert into pha_spill_src
  select i % 20000, (i % 11)::int4
  from generate_series(1, 60000) i;
analyze pha_spill_src;

set work_mem = '64kB';
set hash_mem_multiplier = 1.0;
set enable_sort = off;

set enable_parallel_hash_agg = off;
create table pha_spill_ref as
  select g, sum(x) as sx, count(*) as cnt
  from pha_spill_src group by g;

set enable_parallel_hash_agg = on;
create table pha_spill_shared as
  select g, sum(x) as sx, count(*) as cnt
  from pha_spill_src group by g;

-- expect 0 rows: correctness must hold even when the shared table spilled
select count(*) as mismatched_rows from (
    select * from pha_spill_ref except select * from pha_spill_shared
    union all
    select * from pha_spill_shared except select * from pha_spill_ref
) diff;

-- mask the actual counters (which legitimately vary with the number of
-- workers the system happens to launch) so the test only asserts that the
-- spill-statistics lines are present, not their exact values
create function explain_pha_spill(query text) returns setof text
language plpgsql as
$$
declare
    ln text;
begin
    for ln in
        execute 'explain (analyze, costs off, timing off, summary off, buffers off) ' || query
    loop
        -- mask counters that legitimately vary with how many workers the
        -- system happens to launch, keeping only structural plan shape and
        -- the presence of the spill-statistics fields under test
        ln := regexp_replace(ln, 'Shared Buckets: [0-9]+', 'Shared Buckets: N', 'g');
        ln := regexp_replace(ln, 'Shared Peak Memory Usage: [0-9]+kB', 'Shared Peak Memory Usage: NkB', 'g');
        ln := regexp_replace(ln, 'Spilled Tuples: [0-9]+', 'Spilled Tuples: N', 'g');
        ln := regexp_replace(ln, 'Spill Batches: [0-9]+', 'Spill Batches: N', 'g');
        ln := regexp_replace(ln, 'Workers Launched: [0-9]+', 'Workers Launched: N', 'g');
        ln := regexp_replace(ln, 'Rows Removed by Filter: [0-9]+', 'Rows Removed by Filter: N', 'g');
        ln := regexp_replace(ln, 'rows=[0-9]+(\.[0-9]+)?', 'rows=N', 'g');
        ln := regexp_replace(ln, 'loops=[0-9]+', 'loops=N', 'g');
        return next ln;
    end loop;
end;
$$;

select explain_pha_spill($$
  select g, sum(x), count(*) from pha_spill_src group by g
$$);

--
-- 6. Stopping early over a spilled table.  A participant that stops before
-- the batch cycle is over must leave the scan barrier on its way out;
-- otherwise the participants still cycling wait for an arrival that will
-- never come, and the query hangs rather than failing.
--
select count(*) from
  (select g, sum(x) from pha_spill_src group by g limit 5) ss;

--
-- 7. Rescan after a spill.  The spill tuplestores are single-pass, so each
-- re-execution needs fresh ones; this used to be refused outright at run
-- time, which made a legitimately planned query fail as a function of how
-- much data it happened to meet.
--
set enable_material = off;

select count(*) as nrows, sum(sx) as checksum from
  (select g, sum(x) as sx from pha_spill_src group by g) ss
  right join (values (1), (2)) v(k) on true;

select count(*) * 2 as nrows, sum(sx) * 2 as checksum from
  (select g, sum(x) as sx from pha_spill_src group by g) ss;

reset enable_material;
reset work_mem;
reset hash_mem_multiplier;

--
-- 8. By-reference states whose size changes from row to row, which is what
-- exercises both branches of the blob update: overwrite in place when the
-- new state is the same size, allocate and free when it is not.
--
create table pha_text (g int4, t text);
insert into pha_text
  select i % 3000, repeat('abc', 1 + (i % 17))
  from generate_series(1, 30000) i;
analyze pha_text;

create table pha_text_ref as select * from pha_text;
alter table pha_text_ref set (parallel_workers = 0);
analyze pha_text_ref;

explain (costs off)
  select g, min(t), max(t) from pha_text group by g;

select count(*) as mismatched_rows from (
    (select g, min(t) mn, max(t) mx from pha_text group by g
     except
     select g, min(t), max(t) from pha_text_ref group by g)
    union all
    (select g, min(t), max(t) from pha_text_ref group by g
     except
     select g, min(t), max(t) from pha_text group by g)
) diff;

--
-- 9. A parallel-aware Agg has to be able to run with no parallel context at
-- all.  ExecutePlan() does not enter parallel mode when it has been given a
-- tuple count, which is the case for SPI with a row limit -- how PL/pgSQL
-- runs SELECT ... INTO -- and for cursor FETCH.  There is then no shared
-- table to build, and the node must fall back to a private one instead of
-- complaining that it was never initialized.
--
do $$
declare
    nrows   int8;
    total   int8;
begin
    select count(*), sum(sx) into nrows, total
      from (select g, sum(x) as sx from pha_src group by g) ss;
    raise notice 'serial fallback: nrows=%, total=%', nrows, total;
end
$$;

begin;
declare pha_cur cursor for
  select g, sum(x) as sx from pha_src group by g order by g;
fetch 3 from pha_cur;
commit;

reset enable_sort;
reset enable_parallel_hash_agg;
reset parallel_setup_cost;
reset parallel_tuple_cost;
reset min_parallel_table_scan_size;
reset max_parallel_workers_per_gather;

drop schema parallel_hash_agg cascade;
reset search_path;
