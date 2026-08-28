# Copyright (c) 2026, PostgreSQL Global Development Group

# Injection-point tests for the parallel Repartition node.
#
# These cover the two failure modes that cannot be provoked by ordinary SQL:
# a participant that reaches the exchange arbitrarily late, and a participant
# that dies inside the sink phase.  Both are barrier-lifetime questions, and
# both are the kind of bug that otherwise shows up years later on one
# buildfarm animal.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Time::HiRes qw(usleep);
use Test::More;

if ($ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

my $node = PostgreSQL::Test::Cluster->new('repartition_inj');
$node->init;
$node->append_conf(
	'postgresql.conf', qq{
max_worker_processes = 16
max_parallel_workers = 8
max_parallel_workers_per_gather = 4
parallel_setup_cost = 0
min_parallel_table_scan_size = 0
});
$node->start;

if (!$node->check_extension('injection_points'))
{
	plan skip_all => 'Extension injection_points not installed';
}
$node->safe_psql('postgres', 'CREATE EXTENSION injection_points;');

$node->safe_psql('postgres', q{
	CREATE TABLE rp AS
	  SELECT (i % 20000) AS k, (i % 7) AS v FROM generate_series(1, 200000) i;
	ANALYZE rp;
});

my $force = 'SET parallel_repartition_partitions = 4;'
  . ' SET debug_parallel_repartition = on;';
my $agg = 'SELECT k, count(*) c FROM rp GROUP BY k';
my $qry = "SELECT count(*), sum(c) FROM ($agg) s;";

my $want = $node->safe_psql('postgres',
	"SET max_parallel_workers_per_gather = 0; $qry");
is($want, '20000|200000', 'baseline');

#
# 1. Every worker is held at the start of its sink phase while the leader runs
#    ahead.  The leader must block at the barrier rather than draining the
#    exchange on its own; when the workers are released their tuples must
#    still be counted.
#
$node->safe_psql('postgres',
	"SELECT injection_points_attach('repartition-worker-sink-start', 'wait');");

my $bg = $node->background_psql('postgres', on_error_stop => 0);
$bg->query_until(qr/starting/,
	"\\echo starting\n$force $qry\n");

# Wait until at least one worker is parked in the injection point.  If the
# leader could pass the barrier alone it would already have finished by now.
$node->poll_query_until('postgres', q{
	SELECT count(*) > 0 FROM pg_stat_activity
	 WHERE wait_event = 'repartition-worker-sink-start'
}) or die 'timed out waiting for a worker to reach the injection point';

my $still_running = $node->safe_psql('postgres', q{
	SELECT count(*) > 0 FROM pg_stat_activity
	 WHERE backend_type = 'client backend'
	   AND query LIKE '%GROUP BY k%'
	   AND state = 'active'
});
is($still_running, 't', 'leader has not finished while workers are held');

# Detach first so that a worker which has not reached the point yet does not
# park after we start waking people up; a wakeup only releases the processes
# already waiting, and four workers do not arrive at the same instant.
$node->safe_psql('postgres',
	"SELECT injection_points_detach('repartition-worker-sink-start');");
for (my $i = 0; $i < 100; $i++)
{
	my $waiting = $node->safe_psql('postgres', q{
		SELECT count(*) FROM pg_stat_activity
		 WHERE wait_event = 'repartition-worker-sink-start'
	});
	last if $waiting eq '0';
	$node->psql('postgres',
		"SELECT injection_points_wakeup('repartition-worker-sink-start');");
	usleep(50_000);
}

my $got = $bg->query_until(qr/\d\|\d/, "");
like($got, qr/20000\|200000/, 'no tuples lost when workers arrive late');
$bg->quit;

#
# 2. A participant raises an error in the middle of the sink phase.  Nobody
#    may be left waiting on the barrier, and the cluster must stay usable.
#
$node->safe_psql('postgres',
	"SELECT injection_points_attach('repartition-sink-done', 'error');");

my ($ret, $stdout, $stderr) = $node->psql('postgres', "$force $qry");
isnt($ret, 0, 'error in the sink phase fails the query');
like($stderr, qr/error triggered for injection point repartition-sink-done/,
	'and it is our error');

$node->safe_psql('postgres',
	"SELECT injection_points_detach('repartition-sink-done');");

my $after = $node->safe_psql('postgres', "$force $qry");
is($after, '20000|200000', 'cluster is fine afterwards');

$node->stop;
done_testing();
