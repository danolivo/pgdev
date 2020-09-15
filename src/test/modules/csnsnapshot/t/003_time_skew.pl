use strict;
use warnings;

use TestLib;
use Test::More tests => 5;
use PostgresNode;

my $node1 = get_new_node('csn1');
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
my $node2 = get_new_node('csn2');
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
	CREATE TABLE t_0 PARTITION OF t FOR VALUES WITH (modulus 2, remainder 0);
	CREATE FOREIGN TABLE t_1 PARTITION OF t FOR VALUES WITH (modulus 2, remainder 1) SERVER remote;
");
$node2->safe_psql('postgres', "
	CREATE TABLE t (id serial, payload int) PARTITION BY HASH(id);
	CREATE TABLE t_1 PARTITION OF t FOR VALUES WITH (modulus 2, remainder 1);
	CREATE FOREIGN TABLE t_0 PARTITION OF t FOR VALUES WITH (modulus 2, remainder 0) SERVER remote;
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
my $pgb_handle1 = $node1->pgbench_async(-n, -c => 5, -T => $seconds, -f => $q1, 'postgres' );
my $pgb_handle2 = $node2->pgbench_async(-n, -c => 5, -T => $seconds, -f => $q2, 'postgres' );
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

$node2->psql('postgres', "UPDATE summary SET ntrans = 1");
$ntrans = $node1->safe_psql('postgres', "SELECT ntrans FROM summary");
note("$ntrans");
is( $ntrans, 0, 'Do not update if snapshot turn sour');

# ##############################################################################
#
# Test on issue #1:
# 'xact confirmed as committed, so any following xact must see its effects'.
#
# ##############################################################################
$node1->safe_psql('postgres', "ALTER SYSTEM SET csn_time_shift = 5");
$node2->safe_psql('postgres', "ALTER SYSTEM SET csn_time_shift = 0");
$node1->restart();
$node2->restart();

$node1->safe_psql('postgres', "UPDATE summary SET ntrans = 1");
$ntrans = $node2->safe_psql('postgres', "SELECT ntrans FROM summary");
is( $ntrans, 0, 'Do not see values, committed in the future, step 1');

# But...
$node1->safe_psql('postgres', "ALTER SYSTEM SET csn_time_shift = 0");
$node2->safe_psql('postgres', "ALTER SYSTEM SET csn_time_shift = 5");
$node1->restart();
$node2->restart();

$node2->safe_psql('postgres', "UPDATE summary SET ntrans = 1");
$ntrans = $node1->safe_psql('postgres', "SELECT ntrans FROM summary");
note("$ntrans");
is( $ntrans, 0, 'Do not see values, committed in the future, step 2');

$node1->stop();
$node2->stop();

