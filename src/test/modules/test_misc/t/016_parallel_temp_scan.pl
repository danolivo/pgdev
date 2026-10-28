
# Copyright (c) 2026, PostgreSQL Global Development Group

# Parallel workers may read the leader's temporary tables only after the
# leader has flushed its dirty local buffers.  These checks need a worker to
# actually run the query, which the core regression tests cannot guarantee
# under load; a dedicated instance can.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('node');
$node->init;
$node->append_conf(
	'postgresql.conf', qq{
max_parallel_workers_per_gather = 2
parallel_setup_cost = 0
parallel_tuple_cost = 0
min_parallel_table_scan_size = 0
min_parallel_index_scan_size = 0
write_page_cost = 0.0001
parallel_leader_participation = off
});
$node->start;

$node->safe_psql(
	'postgres', q{
	CREATE TABLE perm AS SELECT g FROM generate_series(1, 50000) g;
	ANALYZE perm;
	-- the temporary table only exists in the sessions below
	SET check_function_bodies = off;
	CREATE FUNCTION tmp_count() RETURNS bigint LANGUAGE sql PARALLEL SAFE
		AS 'SELECT count(*) FROM tmp';
});

# Each psql call below is a separate session; temporary tables and their
# local buffers are per-backend, so every session builds its own.
my $setup = q{
	CREATE TEMP TABLE tmp AS SELECT x FROM generate_series(1, 1000) x;
	-- these rows stay dirty in the leader's local buffers
	INSERT INTO tmp SELECT x FROM generate_series(1001, 2000) x;
};

# 1. The planner cannot see the temporary table behind the function, so no
#    flush is requested; the worker must refuse rather than return the 1000
#    rows that happen to be on disk.
my ($ret, $stdout, $stderr) = $node->psql('postgres',
	$setup . 'SELECT max(tmp_count()) FROM perm;');
isnt($ret, 0, 'temp access in a worker without a flush fails');
like(
	$stderr,
	qr/cannot access temporary tables during a parallel operation/,
	'... with the expected error');

# 2. The same function is fine once the query itself scans the temporary
#    table: the planner requests the flush and the workers see all rows.
($ret, $stdout, $stderr) = $node->psql('postgres',
	$setup
	  . 'SELECT max(tmp_count()) FROM perm JOIN tmp ON perm.g = tmp.x;');
is($ret, 0, 'temp access in a worker after a flush succeeds');
is($stdout, '2000', '... and sees the rows that were only in local buffers');

# 3. Parallel scans of the temporary table itself, through Gather and
#    Gather Merge, see the unflushed rows.
($ret, $stdout, $stderr) = $node->psql('postgres',
	$setup . q{
	SET enable_hashagg = off;
	SELECT count(*), sum(x) FROM tmp;
	SELECT x FROM tmp GROUP BY x ORDER BY x DESC LIMIT 1;
	});
is($ret, 0, 'parallel scans of a temporary table succeed');
is($stdout, "2000|2001000\n2000", '... and see every row');

$node->stop;
done_testing();
