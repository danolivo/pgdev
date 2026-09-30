
# Copyright (c) 2026, PostgreSQL Global Development Group

# Tests for lr_batch: batched application of remote INSERTs.
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node_publisher = PostgreSQL::Test::Cluster->new('publisher');
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->start;

my $node_subscriber = PostgreSQL::Test::Cluster->new('subscriber');
$node_subscriber->init;
$node_subscriber->append_conf(
	'postgresql.conf', qq(
shared_preload_libraries = 'lr_batch'
lr_batch.subscriptions = 'sub'
lr_batch.max_tuples = 100
log_min_messages = debug1
wal_retrieve_retry_interval = 100ms
));
$node_subscriber->start;

# Tables on the publisher
$node_publisher->safe_psql(
	'postgres', qq(
CREATE TABLE t (id int PRIMARY KEY, v text);
CREATE TABLE t2 (id int PRIMARY KEY);
CREATE TABLE t_idx (a int);
CREATE TABLE t_nn (id int PRIMARY KEY);
CREATE TABLE t_part (id int);
CREATE PUBLICATION pub FOR TABLE t, t2, t_idx, t_nn, t_part;
));

# Tables on the subscriber.  t_idx is owned by an unprivileged role and has
# an expression index whose function logs the user it runs as.  t_nn has a
# NOT NULL column that the publisher does not have.  t_part is a partition
# with a bound the publisher knows nothing about.
$node_subscriber->safe_psql(
	'postgres', qq(
CREATE ROLE regress_tabowner;
CREATE TABLE t (id int PRIMARY KEY, v text);
CREATE TABLE t2 (id int PRIMARY KEY);
CREATE FUNCTION lr_batch_whoami(int) RETURNS int
  LANGUAGE plpgsql IMMUTABLE AS
  \$\$ BEGIN RAISE LOG 'lr_batch_whoami: %', current_user; RETURN \$1; END \$\$;
CREATE TABLE t_idx (a int);
CREATE INDEX t_idx_expr ON t_idx (lr_batch_whoami(a));
ALTER TABLE t_idx OWNER TO regress_tabowner;
CREATE TABLE t_nn (id int PRIMARY KEY, extra int NOT NULL);
CREATE TABLE t_parent (id int) PARTITION BY RANGE (id);
CREATE TABLE t_part PARTITION OF t_parent FOR VALUES FROM (0) TO (100);
));

my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';
$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION sub CONNECTION '$publisher_connstr' PUBLICATION pub WITH (streaming = off)"
);
$node_subscriber->wait_for_subscription_sync($node_publisher, 'sub');

my $result;
my $log_offset;

# Bulk load: rows arrive in batches of lr_batch.max_tuples.
$log_offset = -s $node_subscriber->logfile;
$node_publisher->safe_psql('postgres',
	"INSERT INTO t SELECT g, 'x' FROM generate_series(1, 1000) g");
$node_publisher->wait_for_catchup('sub');
$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*), count(DISTINCT id) FROM t");
is($result, '1000|1000', 'bulk insert is replicated');
ok( $node_subscriber->log_contains(
		qr/lr_batch: inserted 100 tuples into relation "t"/, $log_offset),
	'bulk insert went through the batched path');

# An UPDATE or DELETE of a row inserted earlier in the same transaction
# must see it.
$node_publisher->safe_psql(
	'postgres', qq(
BEGIN;
INSERT INTO t VALUES (5000, 'a');
UPDATE t SET v = 'b' WHERE id = 5000;
INSERT INTO t VALUES (5001, 'a');
DELETE FROM t WHERE id = 5001;
INSERT INTO t VALUES (5002, 'a');
COMMIT;
));
$node_publisher->wait_for_catchup('sub');
$result = $node_subscriber->safe_psql('postgres',
	"SELECT id, v FROM t WHERE id >= 5000 ORDER BY id");
is($result, "5000|b\n5002|a", 'INSERT followed by UPDATE/DELETE in one transaction');

# Interleaved relations: every switch flushes, order is preserved.
$node_publisher->safe_psql(
	'postgres', qq(
BEGIN;
INSERT INTO t VALUES (6000, 'i');
INSERT INTO t2 VALUES (1);
INSERT INTO t VALUES (6001, 'i');
INSERT INTO t2 VALUES (2);
COMMIT;
));
$node_publisher->wait_for_catchup('sub');
$result = $node_subscriber->safe_psql('postgres',
	"SELECT (SELECT count(*) FROM t WHERE id >= 6000), (SELECT count(*) FROM t2)");
is($result, '2|2', 'interleaved INSERTs into two relations');

# Index expressions run as the table owner, even when the flush is
# triggered by COMMIT.
$log_offset = -s $node_subscriber->logfile;
$node_publisher->safe_psql('postgres',
	"INSERT INTO t_idx SELECT generate_series(1, 10)");
$node_publisher->wait_for_catchup('sub');
$result =
  $node_subscriber->safe_psql('postgres', "SELECT count(*) FROM t_idx");
is($result, '10', 'rows with an expression index are replicated');
ok( $node_subscriber->log_contains(
		qr/lr_batch: inserted 10 tuples into relation "t_idx"/, $log_offset),
	'expression-index table went through the batched path');
{
	my $log = slurp_file($node_subscriber->logfile, $log_offset);
	my @users = ($log =~ /lr_batch_whoami: (\S+)/g);
	my @wrong = grep { $_ ne 'regress_tabowner' } @users;
	ok(@users > 0 && @wrong == 0,
		'index expressions are evaluated as the table owner');
}

# A NOT NULL constraint the publisher does not have is enforced.
$log_offset = -s $node_subscriber->logfile;
$node_publisher->safe_psql('postgres', "INSERT INTO t_nn VALUES (1)");
$node_subscriber->wait_for_log(
	qr/null value in column "extra" of relation "t_nn" violates not-null constraint/,
	$log_offset);
$result =
  $node_subscriber->safe_psql('postgres', "SELECT count(*) FROM t_nn");
is($result, '0', 'row violating NOT NULL is not inserted');
$node_subscriber->safe_psql('postgres',
	"ALTER TABLE t_nn ALTER COLUMN extra DROP NOT NULL");
$node_publisher->wait_for_catchup('sub');
$result =
  $node_subscriber->safe_psql('postgres', "SELECT count(*) FROM t_nn");
is($result, '1', 'apply resumes once the constraint is dropped');

# The partition bound of a partition used as a target is enforced.
$log_offset = -s $node_subscriber->logfile;
$node_publisher->safe_psql('postgres', "INSERT INTO t_part VALUES (150)");
$node_subscriber->wait_for_log(
	qr/new row for relation "t_part" violates partition constraint/,
	$log_offset);
$result =
  $node_subscriber->safe_psql('postgres', "SELECT count(*) FROM t_parent");
is($result, '0', 'row outside the partition bound is not inserted');
$node_subscriber->safe_psql('postgres',
	"ALTER TABLE t_parent DETACH PARTITION t_part");
$node_publisher->wait_for_catchup('sub');
$result =
  $node_subscriber->safe_psql('postgres', "SELECT count(*) FROM t_part");
is($result, '1', 'apply resumes once the partition is detached');

# A pre-existing subscriber row is reported as an insert_exists conflict
# and counted in the statistics, as in the per-row path.
$node_subscriber->safe_psql('postgres', "INSERT INTO t VALUES (9000, 'local')");
$log_offset = -s $node_subscriber->logfile;
$node_publisher->safe_psql('postgres',
	"INSERT INTO t SELECT g, 'remote' FROM generate_series(8990, 9010) g");
$node_subscriber->wait_for_log(
	qr/conflict detected on relation "public.t": conflict=insert_exists/,
	$log_offset);
$node_subscriber->poll_query_until('postgres',
	"SELECT confl_insert_exists > 0 FROM pg_stat_subscription_stats WHERE subname = 'sub'"
) or die "timed out waiting for the conflict to be counted";
pass('insert_exists conflict is reported and counted');
$node_subscriber->safe_psql('postgres', "DELETE FROM t WHERE id = 9000");
$node_publisher->wait_for_catchup('sub');
$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*), min(v), max(v) FROM t WHERE id BETWEEN 8990 AND 9010");
is($result, '21|remote|remote', 'apply resumes once the local row is removed');

# With streaming enabled, batching turns itself off.
$log_offset = -s $node_subscriber->logfile;
$node_subscriber->safe_psql('postgres',
	"ALTER SUBSCRIPTION sub SET (streaming = on)");
# Changing streaming restarts the worker; wait for the new one.
$node_publisher->poll_query_until('postgres',
	"SELECT count(*) = 1 FROM pg_stat_replication WHERE application_name = 'sub' AND state = 'streaming'"
) or die "timed out waiting for the apply worker to reconnect";
$node_publisher->safe_psql('postgres',
	"INSERT INTO t SELECT g, 'y' FROM generate_series(7000, 7199) g");
$node_publisher->wait_for_catchup('sub');
ok( $node_subscriber->log_contains(
		qr/lr_batch: batching is disabled for subscription "sub" because streaming is enabled/,
		$log_offset),
	'batching reports that it is disabled with streaming enabled');
$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*) FROM t WHERE id BETWEEN 7000 AND 7199");
is($result, '200', 'per-row path is used with streaming enabled');
ok( !$node_subscriber->log_contains(
		qr/lr_batch: inserted \d+ tuples into relation "t"/, $log_offset),
	'no batched flush with streaming enabled');

$node_subscriber->stop('fast');
$node_publisher->stop('fast');

done_testing();
