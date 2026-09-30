
# Copyright (c) 2026, PostgreSQL Global Development Group

# Tests for lr_batch: batched application of remote INSERTs.
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node_publisher = PostgreSQL::Test::Cluster->new('publisher');
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->append_conf('postgresql.conf',
	'logical_decoding_work_mem = 64kB');
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

# Interleaved relations: each relation has its own buffer, flushed at commit.
$log_offset = -s $node_subscriber->logfile;
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
ok( $node_subscriber->log_contains(
		qr/lr_batch: inserted 2 tuples into relation "t2"/, $log_offset),
	'interleaved INSERTs are buffered per relation');

# More relations in one transaction than there are buffers.
my $mtables = join(', ', map { "m$_" } 1 .. 40);
$node_publisher->safe_psql('postgres',
	join('', map { "CREATE TABLE m$_ (id int PRIMARY KEY);" } 1 .. 40)
	  . "ALTER PUBLICATION pub ADD TABLE $mtables;");
$node_subscriber->safe_psql('postgres',
	join('', map { "CREATE TABLE m$_ (id int PRIMARY KEY);" } 1 .. 40)
	  . "ALTER SUBSCRIPTION sub REFRESH PUBLICATION;");
$node_subscriber->wait_for_subscription_sync($node_publisher, 'sub');
$node_publisher->safe_psql('postgres',
	"BEGIN;"
	  . join('', map { my $r = $_; map { "INSERT INTO m$_ VALUES ($r);" } 1 .. 40 } 1 .. 3)
	  . "COMMIT;");
$node_publisher->wait_for_catchup('sub');
$result = $node_subscriber->safe_psql('postgres',
	join(' UNION ALL ', map { "SELECT count(*) FROM m$_" } 1 .. 40));
is($result, join("\n", ('3') x 40), 'one transaction into 40 relations');

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
ok( $node_subscriber->log_contains(
		qr/CONTEXT:  applying \d+ batched INSERTs to relation "public.t"/,
		$log_offset),
	'the error context names the relation of the batch');
$node_subscriber->safe_psql('postgres', "DELETE FROM t WHERE id = 9000");
$node_publisher->wait_for_catchup('sub');
$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*), min(v), max(v) FROM t WHERE id BETWEEN 8990 AND 9010");
is($result, '21|remote|remote', 'apply resumes once the local row is removed');

# Streamed transactions (streaming = on): the leader spools them and replays
# them at STREAM COMMIT, then commits without a COMMIT message; the buffer
# must be flushed at pre-commit.  Subtransaction rollbacks are cut out of the
# spool file before replay.
$node_subscriber->safe_psql('postgres',
	"ALTER SUBSCRIPTION sub SET (streaming = on)");
# Changing streaming restarts the worker; wait for the new one.
$node_publisher->poll_query_until('postgres',
	"SELECT count(*) = 1 FROM pg_stat_replication WHERE application_name = 'sub' AND state = 'streaming'"
) or die "timed out waiting for the apply worker to reconnect";
$log_offset = -s $node_subscriber->logfile;
$node_publisher->safe_psql(
	'postgres', qq(
BEGIN;
INSERT INTO t SELECT g, 's' FROM generate_series(10000, 17999) g;
SAVEPOINT s1;
INSERT INTO t SELECT g, 'rolled back' FROM generate_series(20000, 20999) g;
ROLLBACK TO SAVEPOINT s1;
INSERT INTO t SELECT g, 's' FROM generate_series(30000, 30456) g;
COMMIT;
));
$node_publisher->wait_for_catchup('sub');
$result = $node_publisher->safe_psql('postgres',
	"SELECT stream_txns > 0 FROM pg_stat_replication_slots WHERE slot_name = 'sub'");
is($result, 't', 'the transaction was streamed');
$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*), count(*) FILTER (WHERE id BETWEEN 20000 AND 20999) FROM t WHERE id >= 10000 AND id < 40000");
is($result, '8457|0', 'streamed transaction with a rolled-back subtransaction');
ok( $node_subscriber->log_contains(
		qr/lr_batch: inserted 100 tuples into relation "t"/, $log_offset),
	'the spooled transaction was applied through the batched path');

# streaming = parallel: the parallel apply worker batches too.  It opens a
# savepoint for every subtransaction between two changes; rows buffered
# before that point must land in the parent.  The row counts are chosen so
# that each savepoint finds a partial batch in the buffer.
$node_subscriber->safe_psql('postgres',
	"ALTER SUBSCRIPTION sub SET (streaming = parallel)");
$node_publisher->poll_query_until('postgres',
	"SELECT count(*) = 1 FROM pg_stat_replication WHERE application_name = 'sub' AND state = 'streaming'"
) or die "timed out waiting for the apply worker to reconnect";
$log_offset = -s $node_subscriber->logfile;
$node_publisher->safe_psql(
	'postgres', qq(
BEGIN;
INSERT INTO t SELECT g, 'p' FROM generate_series(40000, 47949) g;
SAVEPOINT s1;
INSERT INTO t SELECT g, 'rolled back' FROM generate_series(50000, 50999) g;
ROLLBACK TO SAVEPOINT s1;
INSERT INTO t SELECT g, 'p' FROM generate_series(52000, 52049) g;
SAVEPOINT s2;
INSERT INTO t SELECT g, 'p' FROM generate_series(53000, 53149) g;
RELEASE SAVEPOINT s2;
INSERT INTO t SELECT g, 'p' FROM generate_series(54000, 54029) g;
COMMIT;
));
$node_publisher->wait_for_catchup('sub');
ok( $node_subscriber->log_contains(
		qr/defining savepoint \S+ in logical replication parallel apply worker/,
		$log_offset),
	'the transaction was applied by a parallel apply worker, with savepoints');
$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*) FILTER (WHERE id BETWEEN 40000 AND 47999), count(*) FILTER (WHERE id BETWEEN 50000 AND 50999), count(*) FILTER (WHERE id BETWEEN 52000 AND 54999) FROM t");
is($result, '7950|0|230', 'parallel apply with savepoints applies correctly');
ok( $node_subscriber->log_contains(
		qr/lr_batch: inserted 100 tuples into relation "t"/, $log_offset),
	'the parallel apply worker batches');
ok( !$node_subscriber->log_contains(
		qr/lr_batch: subtransaction started with/, $log_offset),
	'no subtransaction was started with buffered tuples');

$log_offset = -s $node_subscriber->logfile;
$node_publisher->safe_psql('postgres',
	"INSERT INTO t SELECT g, 'small' FROM generate_series(60000, 60036) g");
$node_publisher->wait_for_catchup('sub');
$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*) FROM t WHERE id >= 60000");
is($result, '37', 'non-streamed transaction under streaming = parallel');
ok( $node_subscriber->log_contains(
		qr/lr_batch: inserted 37 tuples into relation "t"/, $log_offset),
	'the leader batches a non-streamed transaction');

$node_subscriber->stop('fast');
$node_publisher->stop('fast');

done_testing();
