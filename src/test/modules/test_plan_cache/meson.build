# Copyright (c) 2026, PostgreSQL Global Development Group

test_plan_cache_sources = files(
  'test_plan_cache.c',
)

if host_system == 'windows'
  test_plan_cache_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'test_plan_cache',
    '--FILEDESC', 'test_plan_cache - cursor_options vs plan_cache_mode precedence',])
endif

test_plan_cache = shared_module('test_plan_cache',
  test_plan_cache_sources,
  kwargs: pg_test_mod_args,
)
test_install_libs += test_plan_cache

test_install_data += files(
  'test_plan_cache.control',
  'test_plan_cache--1.0.sql',
)

tests += {
  'name': 'test_plan_cache',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'test_plan_cache',
    ],
  },
}
