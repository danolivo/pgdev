# Copyright (c) 2025-2026, PostgreSQL Global Development Group

# Test 128-bit integer arithmetic code in int128.h

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Utils;
use Test::More;

# One executable per implementation of the overflow-checked primitives: the
# two-limb fallback, native int128 with the overflow builtins, and native
# int128 with the manual sign rule.  See test_int128.c.
my @exes = ('test_int128', 'test_int128_native', 'test_int128_nobuiltin');

# Run each test program with 1M iterations
my $size = 1_000_000;

foreach my $exe (@exes)
{
	note "testing executable $exe";

	my ($stdout, $stderr) = run_command([ $exe, $size ]);

	SKIP:
	{
		skip "no native int128 type", 2 if $stdout =~ /skipping tests/;

		is($stdout, "", "$exe: no stdout");
		is($stderr, "", "$exe: no stderr");
	}
}

done_testing();
