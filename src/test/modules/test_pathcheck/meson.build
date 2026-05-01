# Copyright (c) 2026, PostgreSQL Global Development Group

test_pathcheck_sources = files(
  'test_pathcheck.c',
)

if host_system == 'windows'
  test_pathcheck_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'test_pathcheck',
    '--FILEDESC', 'test_pathcheck - test code for the single-pathlist invariant tracker',])
endif

test_pathcheck = shared_module('test_pathcheck',
  test_pathcheck_sources,
  kwargs: pg_test_mod_args,
)
test_install_libs += test_pathcheck

test_install_data += files(
  'test_pathcheck.control',
  'test_pathcheck--1.0.sql',
)

tests += {
  'name': 'test_pathcheck',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'tap': {
    'tests': [
      't/001_double_add.pl',
      't/002_multi_statement.pl',
    ],
  },
}
