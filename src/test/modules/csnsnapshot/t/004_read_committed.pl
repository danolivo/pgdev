use strict;
use warnings;

use TestLib;
use Test::More tests => 2;
use PostgresNode;

my $node1 = PostgresNode->new('csn1');
$node1->init;
$node1->append_conf('postgresql.conf', qq{
					max_prepared_transactions = 20
					shared_preload_libraries = 'postgres_fdw'
					enable_csn_snapshot = on
					csn_snapshot_defer_time = 10
					postgres_fdw.use_csn_snapshots = true
					csn_time_shift = 0
					});
$node1->start;
my $node2 = PostgresNode->new('csn2');
$node2->init;
$node2->append_conf('postgresql.conf', qq{
					max_prepared_transactions = 20
					shared_preload_libraries = 'postgres_fdw'
					enable_csn_snapshot = on
					csn_snapshot_defer_time = 10
					postgres_fdw.use_csn_snapshots = true
					csn_time_shift = 0
					});
$node2->start;

# Create foreign servers
$node1->safe_psql('postgres', "
	CREATE EXTENSION postgres_fdw;
	CREATE SERVER remote FOREIGN DATA WRAPPER postgres_fdw	OPTIONS (port '".$node2->port."');
	CREATE USER MAPPING FOR PUBLIC SERVER remote;
");
$node2->safe_psql('postgres', "
	CREATE EXTENSION postgres_fdw;
	CREATE SERVER remote FOREIGN DATA WRAPPER postgres_fdw	OPTIONS (port '".$node1->port."');
	CREATE USER MAPPING FOR PUBLIC SERVER remote;
");

# Create sharded table
$node1->safe_psql('postgres', "
	CREATE TABLE dept1(name TEXT);
	CREATE FOREIGN TABLE dept2 (name TEXT) SERVER remote;
");
$node2->safe_psql('postgres', "
	CREATE TABLE dept2(name TEXT);
	CREATE FOREIGN TABLE dept1 (name TEXT) SERVER remote;
	CREATE TABLE results(success_tx int);
	INSERT INTO results (success_tx) VALUES (0);
");

# Fill the table
$node1->safe_psql('postgres', "INSERT INTO dept1 (name) VALUES ('Jonathan')");
$node1->safe_psql('postgres', "INSERT INTO dept2 (name) VALUES ('Hoshi')");
$node2->safe_psql('postgres', "INSERT INTO dept1 (name) VALUES ('Leonard')");
my $count1 = $node1->safe_psql('postgres', "SELECT count(*) FROM ((SELECT * FROM dept1) UNION (SELECT * FROM dept2)) AS a");
my $count2 = $node2->safe_psql('postgres', "SELECT count(*) FROM ((SELECT * FROM dept1) UNION (SELECT * FROM dept2)) AS a");
note("$count1, $count2");
is( (($count1 == 3) and ($count1 == $count2)), 1, 'Correct insert');

# Queries
my $q1 = File::Temp->new();
append_to_file($q1, q{
	BEGIN;
	SELECT count(*) AS cnt FROM dept1; \gset
	\if :cnt > 0
		INSERT INTO dept2 (SELECT * FROM dept1);
		DELETE FROM dept1;
	\else
		INSERT INTO dept1 (SELECT * FROM dept2);
		DELETE FROM dept2;
	\endif

	COMMIT;
});
my $q2 = File::Temp->new();
append_to_file($q2, q{
	SELECT count(*) AS cnt FROM ((SELECT * FROM dept1) UNION (SELECT * FROM dept2)) AS a; \gset
	\if :cnt = 3
		UPDATE results SET success_tx = success_tx + 1;
	\endif
});
my $transactions = 1000;
my $pgb_handle1 = $node1->pgbench_async(-n, -c => 1, -t => $transactions, -f => $q1, 'postgres' );
my $pgb_handle2 = $node2->pgbench_async(-n, -c => 20, -t => $transactions, -f => $q2, 'postgres' );
$node1->pgbench_await($pgb_handle1);
$node2->pgbench_await($pgb_handle2);

$count2 = $node2->safe_psql('postgres', "SELECT success_tx FROM results");
note("$count2");
is( $count2, 20*$transactions, 'Correct READ COMMITTED updates');

$node1->stop();
$node2->stop();

