# Copyright (c) 2026, PostgreSQL Global Development Group
#
# 001_double_add: deliberately re-present an existing Path to add_path()
# from a set_rel_pathlist hook.  Under USE_ASSERT_CHECKING the membership
# tracker in pathcheck.c sees the duplicate insert and raises an elog
# ERROR with text "already present in a pathlist".  We verify psql sees
# that error.  The session continues; the backend is not killed.
#
# Skipped on non-assert builds because the assertion only fires there.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('pathcheck');
$node->init();
$node->start();

my $assert = $node->safe_psql('postgres', 'SHOW debug_assertions');

if ($assert ne 'on')
{
	plan skip_all =>
	  'pathcheck assertion only fires on USE_ASSERT_CHECKING builds';
}

$node->safe_psql('postgres', 'CREATE EXTENSION test_pathcheck');

# Enable the hook, then plan a SELECT.  The hook re-presents the cheapest
# existing path on the base rel back to add_path(); the membership tracker
# sees the duplicate insert and raises ERROR.
my ($rc, $stdout, $stderr) = $node->psql(
	'postgres',
	q{SELECT test_pathcheck_enable();
	  SELECT count(*) FROM pg_class;
	  SELECT test_pathcheck_disable();});

isnt($rc, 0,
	'planning a query with the double-add hook installed raises an error');

like(
	$stderr,
	qr/already present in a pathlist/,
	'pathcheck tracker fires "already present" diagnostic');

$node->stop;

done_testing();
