use strict;
use warnings;

use TestLib;
use Test::More tests => 3;
use PostgresNode;

my $node1 = PostgresNode->new('csn1');
$node1->init;
$node1->append_conf('postgresql.conf', qq{
					enable_csn_snapshot = on
					csn_snapshot_defer_time = 10
					max_prepared_transactions = 30
					csn_time_shift = 0
					shared_preload_libraries = 'postgres_fdw'
					postgres_fdw.use_csn_snapshots = true
					log_statement = none
					default_transaction_isolation = 'REPEATABLE READ'
					log_min_messages = LOG
					});

my $node2 = PostgresNode->new('csn2');
$node2->init;
$node2->append_conf('postgresql.conf', qq{
					enable_csn_snapshot = on
					csn_snapshot_defer_time = 10
					max_prepared_transactions = 30
					csn_time_shift = 0
					shared_preload_libraries = 'postgres_fdw'
					postgres_fdw.use_csn_snapshots = true
					log_statement = none
					default_transaction_isolation = 'REPEATABLE READ'
					log_min_messages = LOG
					});
$node1->start;
$node2->start;

$node1->safe_psql('postgres', "
	CREATE EXTENSION postgres_fdw;
	CREATE SERVER remote FOREIGN DATA WRAPPER postgres_fdw	OPTIONS (port '".$node2->port."');
	CREATE USER MAPPING FOR PUBLIC SERVER remote;
	CREATE TABLE test(key int, x bigint) PARTITION BY LIST (key);
	CREATE TABLE t1 PARTITION OF test FOR VALUES IN (1);
	CREATE FOREIGN TABLE t2 PARTITION OF test FOR VALUES IN (2) SERVER remote;
");
$node2->safe_psql('postgres', "
	CREATE EXTENSION postgres_fdw;
	CREATE SERVER remote FOREIGN DATA WRAPPER postgres_fdw	OPTIONS (port '".$node1->port."');
	CREATE USER MAPPING FOR PUBLIC SERVER remote;
	CREATE TABLE test(key int, x bigint) PARTITION BY LIST (key);
	CREATE FOREIGN TABLE t1 PARTITION OF test FOR VALUES IN (1) SERVER remote;
	CREATE TABLE t2 PARTITION OF test FOR VALUES IN (2);
");
$node1->safe_psql('postgres', "
	INSERT INTO test (key, x) VALUES (1, -1);
	INSERT INTO test (key, x) VALUES (2, 1);
");

$node1->safe_psql('postgres', "VACUUM FULL");
$node2->safe_psql('postgres', "VACUUM FULL");

# ##############################################################################
#
# Tests
#
# ##############################################################################

my $updates = File::Temp->new();
append_to_file($updates, q{
	BEGIN;
		UPDATE test SET x = x + 1;
		UPDATE test SET x = x - 1;
	END;
});

my $local_update1 = File::Temp->new();
append_to_file($local_update1, q{
	BEGIN;
		UPDATE t1 SET x = x + 1;
		UPDATE t1 SET x = x - 1;
	END;
});
my $local_update2 = File::Temp->new();
append_to_file($local_update2, q{
	BEGIN;
		UPDATE t2 SET x = x + 1;
		UPDATE t2 SET x = x - 1;
	END;
});

my ($pgb_handle1, $pgb_handle2, $pgb_handle3, $sum1, $sum2, $errors, $selects, $started);
my $test_time = 30;
my $result;

# ##############################################################################
#
# Concurrent local UPDATE and global SELECT
#
# ##############################################################################
$errors = 0;
$selects = 0;
$started = time();
$pgb_handle1 = $node1->pgbench_async(-n, -c => 5, -T => $test_time, -f => $local_update1, 'postgres' );
while (time() - $started < $test_time)
{
	$result = $node2->safe_psql('postgres', "
		SELECT 'sum=' || sum(x), (SELECT x FROM t1), (SELECT x FROM t2)
		FROM test;");

	if ( index($result, "sum=0") < 0 )
	{
		diag("[$selects] Isolation error. result = [ $result ]");
		$errors++;
		$node1->stop();
		$node2->stop();
		exit(1);
	}
	$selects++;
}
$node1->pgbench_await($pgb_handle1);
note("TOTAL: selects = $selects, errors = $errors");
is($errors == 0, 1, 'Local updates');
#exit(1);

# ##############################################################################
#
# Global UPDATE and global SELECT
#
# ##############################################################################
$errors = 0;
$selects = 0;
$started = time();
$pgb_handle1 = $node1->pgbench_async(-n, -c => 5, -T => $test_time, -f => $updates, 'postgres' );
while (time() - $started < $test_time)
{
	$result = $node2->safe_psql('postgres', "
		SELECT 'sum=' || sum(x), (SELECT x FROM t1), (SELECT x FROM t2)
		FROM test;");

	if ( index($result, "sum=0") < 0 )
	{
		diag("[$selects] Isolation error. result = [ $result ]");
		$errors++;
	}
	$selects++;
}
$node1->pgbench_await($pgb_handle1);
note("TOTAL: selects = $selects, errors = $errors");
is($errors == 0, 1, 'Distributed updates');

# ##############################################################################
#
# Local UPDATEs, global UPDATE and global SELECT
#
# ##############################################################################
$errors = 0;
$selects = 0;
$started = time();
$pgb_handle1 = $node1->pgbench_async(-n, -c => 2, -T => $test_time, -f => $updates, 'postgres' );
$pgb_handle2 = $node2->pgbench_async(-n, -c => 2, -T => $test_time, -f => $local_update2, 'postgres' );
$pgb_handle3 = $node1->pgbench_async(-n, -c => 2, -T => $test_time, -f => $local_update1, 'postgres' );
while (time() - $started < $test_time)
{
	$sum1 = $node1->safe_psql('postgres', "SELECT sum(x) FROM test;");
	$sum2 = $node2->safe_psql('postgres', "SELECT sum(x) FROM test;");

	if ( ($sum1 ne 0) or ($sum2 ne 0) )
	{
		diag("[$selects] Isolation error. Sums = [ $sum1, $sum2 ]");
		$errors++;
	}
	$selects++;
}
$node1->pgbench_await($pgb_handle1);
$node1->pgbench_await($pgb_handle3);
$node2->pgbench_await($pgb_handle2);
note("TOTAL: selects = $selects, errors = $errors");
is($errors == 0, 1, 'Mix of local and distributed updates');

$node1->stop();
$node2->stop();
