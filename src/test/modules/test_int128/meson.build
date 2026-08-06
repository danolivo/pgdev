# Copyright (c) 2025-2026, PostgreSQL Global Development Group

test_int128_sources = files(
  'test_int128.c',
)

if host_system == 'windows'
  test_int128_sources += rc_bin_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'test_int128',
    '--FILEDESC', 'test int128 program',])
endif

test_int128 = executable('test_int128',
  test_int128_sources,
  dependencies: [frontend_code, libpq],
  kwargs: default_bin_args + {
    'install': false,
  },
)
testprep_targets += test_int128

# The overflow-checked primitives in int128.h have three distinct bodies; see
# the comment at the top of test_int128.c.  Build one binary per configuration
# so the test exercises all of them rather than only the default.
test_int128_native = executable('test_int128_native',
  test_int128_sources,
  dependencies: [frontend_code, libpq],
  c_args: ['-DUSE_NATIVE_INT128=1'],
  kwargs: default_bin_args + {
    'install': false,
  },
)
testprep_targets += test_int128_native

test_int128_nobuiltin = executable('test_int128_nobuiltin',
  test_int128_sources,
  dependencies: [frontend_code, libpq],
  c_args: ['-DUSE_NATIVE_INT128=1', '-DTEST_INT128_NO_BUILTIN_OVERFLOW'],
  kwargs: default_bin_args + {
    'install': false,
  },
)
testprep_targets += test_int128_nobuiltin


tests += {
  'name': 'test_int128',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'tap': {
    'tests': [
      't/001_test_int128.pl',
    ],
    'deps': [test_int128, test_int128_native, test_int128_nobuiltin],
  },
}
