# Copyright (c) 2026, PostgreSQL Global Development Group
#
# 002_multi_statement: pin the lifetime contract going forward.  Under
# USE_ASSERT_CHECKING, running multiple statements in a single transaction
# should not produce any spurious assertion failure or WARNING from the
# pathcheck tracker.  The hash is allocated lazily in the planner's
# working memory context, and a MemoryContextCallback tears it down when
# that context resets, so entries cannot leak across statements.
#
# Note: queries below intentionally use LIMIT so that final_rel paths are
# fresh wrapper Paths (LimitPath) rather than aliases of current_rel
# paths.  See the v2 hand-off note for the latent
# add_path(final_rel, current_rel-path) pattern in
# planner.c:apply_scanjoin_target_to_paths' caller chain that a bare
# "SELECT ... FROM pg_class" would otherwise exercise.
#
# Skipped on non-assert builds because the assertion is compiled out.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('multi');
$node->init();
$node->start();

my $assert = $node->safe_psql('postgres', 'SHOW debug_assertions');

if ($assert ne 'on')
{
	plan skip_all =>
	  'pathcheck tracker only active on USE_ASSERT_CHECKING builds';
}

my $log_offset = -s $node->logfile;

my ($rc, $stdout, $stderr) = $node->psql(
	'postgres',
	q{BEGIN;
	  SELECT count(*) FROM pg_class LIMIT 1;
	  SELECT count(*) FROM pg_class LIMIT 1;
	  SELECT count(*) FROM pg_class LIMIT 1;
	  COMMIT;});

is($rc, 0, 'multi-statement transaction completes cleanly');

unlike(
	$stderr,
	qr/already present in a pathlist|removed from pathlist but never recorded/,
	'no spurious pathcheck diagnostics across statements');

# Also confirm nothing landed in the server log.
ok( !$node->log_contains(
		qr/already present in a pathlist|removed from pathlist but never recorded/,
		$log_offset),
	'no pathcheck diagnostics in server log');

$node->stop;

done_testing();
