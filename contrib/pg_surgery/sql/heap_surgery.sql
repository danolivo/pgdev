create extension pg_surgery;

-- create a normal heap table and insert some rows.
-- use a temp table so that vacuum behavior doesn't depend on global xmin
create temp table htab (a int);
insert into htab values (100), (200), (300), (400), (500);

-- test empty TID array
select heap_force_freeze('htab'::regclass, ARRAY[]::tid[]);

-- nothing should be frozen yet
select * from htab where xmin = 2;

-- freeze forcibly
select heap_force_freeze('htab'::regclass, ARRAY['(0, 4)']::tid[]);

-- now we should have one frozen tuple
select ctid, xmax from htab where xmin = 2;

-- kill forcibly
select heap_force_kill('htab'::regclass, ARRAY['(0, 4)']::tid[]);

-- should be gone now
select * from htab where ctid = '(0, 4)';

-- should now be skipped because it's already dead
select heap_force_kill('htab'::regclass, ARRAY['(0, 4)']::tid[]);
select heap_force_freeze('htab'::regclass, ARRAY['(0, 4)']::tid[]);

-- freeze two TIDs at once while skipping an out-of-range block number
select heap_force_freeze('htab'::regclass,
						 ARRAY['(0, 1)', '(0, 3)', '(1, 1)']::tid[]);

-- we should now have two frozen tuples
select ctid, xmax from htab where xmin = 2;

-- out-of-range TIDs should be skipped
select heap_force_freeze('htab'::regclass, ARRAY['(0, 0)', '(0, 6)']::tid[]);

-- set up a new table with a redirected line pointer
-- use a temp table so that vacuum behavior doesn't depend on global xmin
create temp table htab2(a int);
insert into htab2 values (100);
update htab2 set a = 200;
vacuum htab2;

-- redirected TIDs should be skipped
select heap_force_kill('htab2'::regclass, ARRAY['(0, 1)']::tid[]);

-- now create an unused line pointer
select ctid from htab2;
update htab2 set a = 300;
select ctid from htab2;
vacuum freeze htab2;

-- unused TIDs should be skipped
select heap_force_kill('htab2'::regclass, ARRAY['(0, 2)']::tid[]);

-- multidimensional TID array should be rejected
select heap_force_kill('htab2'::regclass, ARRAY[['(0, 2)']]::tid[]);

-- TID array with nulls should be rejected
select heap_force_kill('htab2'::regclass, ARRAY[NULL]::tid[]);

-- but we should be able to kill the one tuple we have
select heap_force_kill('htab2'::regclass, ARRAY['(0, 3)']::tid[]);

-- materialized view.
-- note that we don't commit the transaction, so autovacuum can't interfere.
begin;
create materialized view mvw as select a from generate_series(1, 3) a;

select * from mvw where xmin = 2;
select heap_force_freeze('mvw'::regclass, ARRAY['(0, 3)']::tid[]);
select * from mvw where xmin = 2;

select heap_force_kill('mvw'::regclass, ARRAY['(0, 3)']::tid[]);
select * from mvw where ctid = '(0, 3)';
rollback;

-- check that it fails on an unsupported relkind
create view vw as select 1;
select heap_force_kill('vw'::regclass, ARRAY['(0, 1)']::tid[]);
select heap_force_freeze('vw'::regclass, ARRAY['(0, 1)']::tid[]);

-- test heap_force_unfreeze: clear frozen hint bits but leave VM intact
create extension pageinspect;
create extension pg_visibility;

create table htab3 (a numeric);
insert into htab3 values (1), (2), (3);
-- we need more than 32 blocks when VACUUM start skipping all-frozen ones.
insert into htab3 (a) select random() + 4 from generate_series(1,1E4);
vacuum freeze htab3;

-- check visibility map flags: all_visible and all_frozen should be true
select * from pg_visibility_map('htab3') where blkno = 0;

select
  h.ctid, age(h.xmin) > age(relfrozenxid) as xmin_older_frozenXid, h.xmax, h.a
from htab3 h join pg_class on true where oid = 'htab3'::regclass and h.a <= 3;

select ctid as target_ctid from htab3 order by ctid limit 1 \gset

-- unfreeze tuple 1 and 3: clears hint bits, leaves VM ALL_FROZEN intact
select heap_force_unfreeze('htab3'::regclass, ARRAY['(0, 1)']::tid[]);
select heap_force_unfreeze('htab3'::regclass, ARRAY['(0, 3)']::tid[]);

select * from pg_visibility_map('htab3') where blkno = 0;
select
  h.ctid, age(h.xmin) > age(relfrozenxid) as xmin_older_frozenXid, h.xmax, h.a
from htab3 h join pg_class on true where oid = 'htab3'::regclass and h.a <= 3;

-- only FULL vacuum and vacuum with disabled page skipping detects our corruption.
vacuum (DISABLE_PAGE_SKIPPING) htab3;
vacuum htab3;
vacuum freeze htab3;
vacuum full htab3;

-- check that VACUUM introduced nothing to our corrupted tuples
select * from pg_visibility_map('htab3') where blkno = 0;
select t_ctid, raw_flags, combined_flags
from heap_page_items(get_raw_page('htab3', 0)),
  lateral heap_tuple_infomask_flags(t_infomask, t_infomask2)
  where t_infomask is not null or t_infomask2 is not null
order by t_ctid limit 4;

set vacuum_freeze_soft_check = 'on';

-- Now, we should warn on each problematic tuple
vacuum (DISABLE_PAGE_SKIPPING) htab3;
vacuum htab3;
vacuum freeze htab3;

select * from pg_visibility_map('htab3') where blkno = 0;
select t_ctid, raw_flags, combined_flags
from heap_page_items(get_raw_page('htab3', 0)),
  lateral heap_tuple_infomask_flags(t_infomask, t_infomask2)
  where t_infomask is not null or t_infomask2 is not null
order by t_ctid limit 4;

set vacuum_freeze_soft_check = 'off';

drop extension pg_visibility;
drop extension pageinspect;

-- cleanup.
drop view vw;
drop extension pg_surgery;
