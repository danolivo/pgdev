use strict;
use warnings;

use TestLib;
use Test::More tests => 13;
use PostgresNode;

my $node1 = PostgresNode->new('csn1');
$node1->init;
$node1->append_conf('postgresql.conf', qq{
					enable_csn_snapshot = on
					csn_snapshot_defer_time = 10
					max_prepared_transactions = 10
					csn_time_shift = 0
					shared_preload_libraries = 'postgres_fdw'
					postgres_fdw.use_csn_snapshots = true
					});
$node1->start;
my $node2 = PostgresNode->new('csn2');
$node2->init;
$node2->append_conf('postgresql.conf', qq{
					enable_csn_snapshot = on
					csn_snapshot_defer_time = 10
					max_prepared_transactions = 10
					csn_time_shift = 0
					shared_preload_libraries = 'postgres_fdw'
					postgres_fdw.use_csn_snapshots = true
					});
$node2->start;

$node1->safe_psql('postgres', "
	CREATE EXTENSION postgres_fdw;
	CREATE SERVER remote FOREIGN DATA WRAPPER postgres_fdw	OPTIONS (port '".$node2->port."');
	CREATE USER MAPPING FOR PUBLIC SERVER remote;
	CREATE TABLE summary(value int, ntrans int);
	INSERT INTO summary (value, ntrans) VALUES (0, 0);
");
$node2->safe_psql('postgres', "
	CREATE EXTENSION postgres_fdw;
	CREATE SERVER remote FOREIGN DATA WRAPPER postgres_fdw	OPTIONS (port '".$node1->port."');
	CREATE USER MAPPING FOR PUBLIC SERVER remote;
	CREATE FOREIGN TABLE summary(value int, ntrans int) SERVER remote;
");

$node1->safe_psql('postgres', "
	CREATE TABLE t (id int, payload int) PARTITION BY HASH(id);
	CREATE TABLE t_1 PARTITION OF t FOR VALUES WITH (modulus 2, remainder 0);
	CREATE FOREIGN TABLE t_2 PARTITION OF t FOR VALUES WITH (modulus 2, remainder 1) SERVER remote;
");
$node2->safe_psql('postgres', "
	CREATE TABLE t (id serial, payload int) PARTITION BY HASH(id);
	CREATE FOREIGN TABLE t_1 PARTITION OF t FOR VALUES WITH (modulus 2, remainder 0) SERVER remote;
	CREATE TABLE t_2 PARTITION OF t FOR VALUES WITH (modulus 2, remainder 1);
");

$node1->safe_psql('postgres', "INSERT INTO t(id, payload) (SELECT gs.*, 1 FROM generate_series(1,100) AS gs)");
$node2->safe_psql('postgres', "INSERT INTO t(id, payload) (SELECT gs.*, 2 FROM generate_series(101,200) AS gs)");
my $count1 = $node1->safe_psql('postgres', "SELECT SUM(payload) FROM t");
my $count2 = $node2->safe_psql('postgres', "SELECT SUM(payload) FROM t");
is( (($count1 == 300) and ($count1 == $count2)), 1, 'Correct insert');

# ##############################################################################
#
# Basic test. Check REPEATABLE READ anomaly.
# ntrans is needed to control that some transactions were committed.
#
# ##############################################################################

my $q1 = File::Temp->new();
append_to_file($q1, q{
	START TRANSACTION ISOLATION LEVEL REPEATABLE READ;
	UPDATE summary SET value = value + (SELECT SUM(payload) FROM t);
	UPDATE summary SET value = value - (SELECT SUM(payload) FROM t);
	UPDATE summary SET ntrans = ntrans + 1;
	COMMIT;
});
my $q2 = File::Temp->new();
append_to_file($q2, q{
	BEGIN;
	\set pl random(-100, 100)
	\set id random(1, 200)
	UPDATE t SET payload = :pl WHERE id = :id;
	COMMIT;
});

my $seconds = 5;
my $pgb_handle1;
my $pgb_handle2;

$pgb_handle1 = $node1->pgbench_async(-n, -c => 5, -T => $seconds, -f => $q1, 'postgres' );
$pgb_handle2 = $node2->pgbench_async(-n, -c => 1, -T => $seconds, -f => $q2, 'postgres' );
$node1->pgbench_await($pgb_handle1);
$node2->pgbench_await($pgb_handle2);

$count1 = $node1->safe_psql('postgres', "SELECT SUM(value) FROM summary");
$count2 = $node2->safe_psql('postgres', "SELECT SUM(value) FROM summary");
my $ntrans = $node2->safe_psql('postgres', "SELECT SUM(ntrans) FROM summary");
note("$count1, $count2, $ntrans");
is( ( ($ntrans > 0) and ($count1 == 0) and ($count1 == $count2)), 1, 'Correct update');

# ##############################################################################
#
# Test on 'snapshot too old'
#
# ##############################################################################
$node1->safe_psql('postgres', "UPDATE summary SET ntrans = 0;");
$node2->safe_psql('postgres', "ALTER SYSTEM SET csn_time_shift = -100");
$node2->restart();

# READ COMMITTED transactions ignores the time skew.
$node2->psql('postgres', "UPDATE summary SET ntrans = 1");
$ntrans = $node1->safe_psql('postgres', "SELECT ntrans FROM summary");
note("$ntrans");
is( $ntrans, 1, 'Read committed behavior if snapshot turn sour');

# But REPEATABLE READ transactions isn't
$node1->safe_psql('postgres', "ALTER SYSTEM SET csn_time_shift = +100");
$node1->restart();
my $err = '';
$node2->psql('postgres', "START TRANSACTION ISOLATION LEVEL REPEATABLE READ; UPDATE summary SET ntrans = 2; COMMIT;", stderr => \$err);
$ntrans = $node1->safe_psql('postgres', "SELECT ntrans FROM summary");
note("$ntrans");
is( (($ntrans == 1) and (index($err, 'csn snapshot too old') != -1)), 1, 'Read committed can\'t update if snapshot turn sour');

# ##############################################################################
#
# Test on issue #1:
# 'xact confirmed as committed, so any following xact must see its effects'.
#
# ##############################################################################
$node1->safe_psql('postgres', "delete from t");
$node1->safe_psql('postgres', "ALTER SYSTEM SET csn_time_shift = 5");
$node2->safe_psql('postgres', "ALTER SYSTEM SET csn_time_shift = 0");
$node1->restart();
$node2->restart();

my $st_sec; my $end_sec;
my $time_diff;

$node2->safe_psql('postgres', "START TRANSACTION ISOLATION LEVEL REPEATABLE READ; INSERT INTO t VALUES(1,1), (3,1); COMMIT;");
$ntrans = $node2->safe_psql('postgres', "START TRANSACTION ISOLATION LEVEL REPEATABLE READ; SELECT count(*) FROM t; COMMIT;");
is( $ntrans, 2, 'Slow node can see mix node data change');
$ntrans = $node1->safe_psql('postgres', "START TRANSACTION ISOLATION LEVEL REPEATABLE READ; SELECT count(*) FROM t; COMMIT;");
is( $ntrans, 2, 'Fast node can see mix node data change');

$node2->safe_psql('postgres', "START TRANSACTION ISOLATION LEVEL REPEATABLE READ; INSERT INTO t VALUES(1,1); COMMIT;");
$ntrans = $node2->safe_psql('postgres', "START TRANSACTION ISOLATION LEVEL REPEATABLE READ; SELECT count(*) FROM t; COMMIT;");
is( $ntrans, 3, 'CURRENTLY FAILED:Data change to fast node on slow node, and slow node can see data change');

# READ COMMITED mode ignores the time skew.
$node1->safe_psql('postgres', "UPDATE summary SET ntrans = 1");
$ntrans = $node2->safe_psql('postgres', "SELECT ntrans FROM summary");
note("ntrans: $ntrans\n");
is( $ntrans, 1, 'See committed values in the READ COMMITTED mode');

# Access from the future
$node1->safe_psql('postgres', "START TRANSACTION ISOLATION LEVEL REPEATABLE READ; UPDATE summary SET ntrans = ntrans + 1; COMMIT;");
$ntrans = $node2->safe_psql('postgres', "START TRANSACTION ISOLATION LEVEL REPEATABLE READ; SELECT ntrans FROM summary; COMMIT;");
note("ntrans: $ntrans\n");
is( $ntrans, 1, 'Do not see values, committed in the future at the REPEATABLE READ mode');

# But...
$node1->safe_psql('postgres', "ALTER SYSTEM SET csn_time_shift = 0");
$node2->safe_psql('postgres', "ALTER SYSTEM SET csn_time_shift = 5");
$node1->restart();
$node2->restart();

# Check READ COMMITED mode
$node2->safe_psql('postgres', "UPDATE summary SET ntrans = 2");
$ntrans = $node1->safe_psql('postgres', "SELECT ntrans FROM summary");
note("ntrans: $ntrans\n");
is( $ntrans, 2, 'See committed values in the READ COMMITTED mode, step 2');

# Node from the future will wait for a time before UPDATE table.
($st_sec) = localtime();
$node2->safe_psql('postgres', "START TRANSACTION ISOLATION LEVEL REPEATABLE READ; UPDATE summary SET ntrans = 3; COMMIT;");
($end_sec) = localtime(); $time_diff = $end_sec - $st_sec;
$ntrans = $node1->safe_psql('postgres', "START TRANSACTION ISOLATION LEVEL REPEATABLE READ; SELECT ntrans FROM summary; COMMIT;");
note("ntrans: $ntrans, Test time: $time_diff seconds");
is( ($ntrans == 3), 1, 'The test execution time correlates with the time offset.');

# Node from the future will wait for a time before SELECT from a table.
$node1->safe_psql('postgres', "START TRANSACTION ISOLATION LEVEL REPEATABLE READ; UPDATE summary SET ntrans = 4; COMMIT;");
($st_sec) = localtime();
$ntrans = $node2->safe_psql('postgres', "START TRANSACTION ISOLATION LEVEL REPEATABLE READ; SELECT ntrans FROM summary; COMMIT;");
($end_sec) = localtime(); $time_diff = $end_sec - $st_sec;
note("ntrans: $ntrans, Test time: $time_diff seconds ($end_sec, $st_sec)");
is( ($ntrans == 4), 1, 'See values, committed in the past. The test execution time correlates with the time offset.');

$node1->safe_psql('postgres', "UPDATE summary SET ntrans = 0, value = 0");
$q1 = File::Temp->new();
append_to_file($q1, q{
	UPDATE summary SET value = value + 1, ntrans = ntrans + 1;
});
$q2 = File::Temp->new();
append_to_file($q2, q{
	START TRANSACTION ISOLATION LEVEL REPEATABLE READ;
	UPDATE summary SET value = value + (SELECT SUM(ntrans) FROM summary);
	UPDATE summary SET value = value - (SELECT SUM(ntrans) FROM summary);
	COMMIT;
});
$seconds = 3;
$pgb_handle1 = $node1->pgbench_async(-n, -c => 1, -T => $seconds, -f => $q1, 'postgres' );
$pgb_handle2 = $node2->pgbench_async(-n, -c => 1, -T => $seconds, -f => $q2, 'postgres' );
$node1->pgbench_await($pgb_handle1);
$node2->pgbench_await($pgb_handle2);

$count1 = $node1->safe_psql('postgres', "SELECT SUM(value) FROM summary");
$count2 = $node1->safe_psql('postgres', "SELECT SUM(ntrans) FROM summary");
note("$count1, $count2");
is( ( ($count1 > 0) and ($count1 == $count2)), 1, 'Skew test');

$node1->stop();
$node2->stop();

