# Copyright (c) 2026, PostgreSQL Global Development Group

# Stress the barrier lifecycle of the parallel Repartition node.
#
# The interesting failure modes are not wrong answers but hangs: a participant
# waiting at the sink barrier for somebody who will never arrive.  Each test
# below is wrapped in a statement_timeout so that a hang fails the test instead
# of stalling the run.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('repartition');
$node->init;
$node->append_conf(
	'postgresql.conf', qq{
max_worker_processes = 16
max_parallel_workers = 8
max_parallel_workers_per_gather = 4
parallel_setup_cost = 0
min_parallel_table_scan_size = 0
statement_timeout = '60s'
});
$node->start;

$node->safe_psql('postgres', q{
	CREATE TABLE rp AS
	  SELECT (i % 20000) AS k, (i % 7) AS v FROM generate_series(1, 200000) i;
	ANALYZE rp;
});

# The cost model will not pick this shape on a table small enough for a test,
# and no data shape reliably makes it.  debug_parallel_repartition penalises
# every competing path, which is the only deterministic way to exercise the
# node.
my $force = 'SET parallel_repartition_partitions = 4;'
  . ' SET debug_parallel_repartition = on;';
my $agg = 'SELECT k, count(*) c, avg(v::numeric) a FROM rp GROUP BY k';

# The plan we mean to exercise is actually chosen.
my $plan = $node->safe_psql('postgres',
	"$force EXPLAIN (COSTS OFF) $agg;");
like($plan, qr/Parallel Repartition/, 'repartition plan is chosen');

# Baseline answer, computed without the feature.
my $want = $node->safe_psql('postgres',
	q{SET enable_parallel_repartition = off;
	  SET max_parallel_workers_per_gather = 0;
	  SELECT count(*), sum(c), round(sum(a), 4) FROM (}
	  . $agg . q{) s;});

# Same answer under a range of participant configurations.  Each of these
# reaches the barrier with a different number of real participants.
my @cases = (
	[ 'default',            '' ],
	[ 'leader off',         'SET parallel_leader_participation = off;' ],
	[ 'no workers',         'SET max_parallel_workers = 0;' ],
	[ 'one worker',         'SET max_parallel_workers_per_gather = 1;' ],
	[ 'eight workers',      'SET max_parallel_workers_per_gather = 8;' ],
	[ 'one partition',      'SET parallel_repartition_partitions = 1;' ],
	[ 'many partitions',    'SET parallel_repartition_partitions = 64;' ],
	[ 'fewer partitions than participants',
	  'SET parallel_repartition_partitions = 2; SET max_parallel_workers_per_gather = 8;' ],
);

foreach my $case (@cases)
{
	my ($name, $setup) = @$case;
	my $got = $node->safe_psql('postgres',
		"$force $setup SELECT count(*), sum(c), round(sum(a), 4) FROM ($agg) s;");
	is($got, $want, "same answer: $name");
}

# Early shutdown.  The leader stops reading long before the exchange is
# drained, so every participant must be able to leave the barrier and the
# shared file set unilaterally.  Repeated, because this is a race.
for my $i (1 .. 20)
{
	my $got = $node->safe_psql('postgres',
		"$force SELECT count(*) FROM (SELECT k FROM ($agg) s LIMIT 1) x;");
	is($got, '1', "early shutdown does not hang, iteration $i");
}

# Rescan: a correlated subquery re-executes the Gather, which drives
# ExecParallelReinitialize and hence our ReInitializeDSM.
$node->safe_psql('postgres',
	'CREATE TABLE rp_outer AS SELECT generate_series(1, 3) AS g;');
my $rescan = $node->safe_psql('postgres', qq{
	$force
	SELECT count(*) FROM rp_outer o
	 WHERE EXISTS (SELECT 1 FROM ($agg) s WHERE s.k = o.g);
});
is($rescan, '3', 'rescan of a plan containing Repartition');

# The feature must be switchable off without changing answers.
my $off = $node->safe_psql('postgres',
	"SET enable_parallel_repartition = off;
	 SELECT count(*), sum(c), round(sum(a), 4) FROM ($agg) s;");
is($off, $want, 'same answer with the feature disabled');

$node->stop;
done_testing();
