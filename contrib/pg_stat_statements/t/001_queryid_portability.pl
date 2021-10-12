use strict;
use warnings;
use PostgresNode;
use TestLib;

use Test::More tests => 3;

my ($node1, $node2, $result1, $result2);

$node1 = PostgresNode->new('node1');
$node1->init;
$node1->append_conf('postgresql.conf', qq{
	shared_preload_libraries = 'pg_stat_statements'
	pg_stat_statements.track = 'all'
});
$node1->start;

$node2 = PostgresNode->new('node2');
$node2->init;
$node2->append_conf('postgresql.conf', qq{
	shared_preload_libraries = 'pg_stat_statements'
	pg_stat_statements.track = 'all'
});
$node2->start;
$node2->safe_psql('postgres', qq{CREATE TABLE a(); DROP TABLE a;});

$node1->safe_psql('postgres', q(CREATE EXTENSION pg_stat_statements));
$node2->safe_psql('postgres', q(CREATE EXTENSION pg_stat_statements));

$node1->safe_psql('postgres', "
	SELECT pg_stat_statements_reset();
	CREATE TABLE a (x int, y varchar);
	CREATE TABLE b (x int);
	SELECT * FROM a;"
);
$node2->safe_psql('postgres', "
	SELECT pg_stat_statements_reset();
	CREATE TABLE a (y varchar, x int);
	CREATE TABLE b (x int);
	SELECT * FROM a;
");

$result1 = $node1->safe_psql('postgres', "SELECT queryid FROM pg_stat_statements WHERE query LIKE 'SELECT * FROM a';");
$result2 = $node2->safe_psql('postgres', "SELECT queryid FROM pg_stat_statements WHERE query LIKE 'SELECT * FROM a';");
is($result1, $result2);

$node1->safe_psql('postgres', "SELECT x FROM a");
$node2->safe_psql('postgres', "SELECT x FROM a");
$result1 = $node1->safe_psql('postgres', "SELECT queryid FROM pg_stat_statements WHERE query LIKE 'SELECT x FROM a';");
$result2 = $node2->safe_psql('postgres', "SELECT queryid FROM pg_stat_statements WHERE query LIKE 'SELECT x FROM a';");
is(($result1 != $result2), 1); # TODO

$node1->safe_psql('postgres', "SELECT * FROM a,b WHERE a.x = b.x;");
$node2->safe_psql('postgres', "SELECT * FROM b,a WHERE a.x = b.x;");
$result1 = $node1->safe_psql('postgres', "SELECT queryid FROM pg_stat_statements WHERE query LIKE 'SELECT * FROM a,b WHERE a.x = b.x;'");
$result2 = $node2->safe_psql('postgres', "SELECT queryid FROM pg_stat_statements WHERE query LIKE 'SELECT * FROM b,a WHERE a.x = b.x;'");
diag("$result1, \n $result2");
is($result1, $result2);

$node1->stop();
$node2->stop();
