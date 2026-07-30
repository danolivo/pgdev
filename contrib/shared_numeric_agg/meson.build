# Copyright (c) 2026, PostgreSQL Global Development Group

shared_numeric_agg_sources = files(
  'shared_numeric_agg.c',
)

if host_system == 'windows'
  shared_numeric_agg_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'shared_numeric_agg',
    '--FILEDESC', 'shared_numeric_agg - flat-state numeric aggregates for parallel shared hash aggregation',])
endif

shared_numeric_agg = shared_module('shared_numeric_agg',
  shared_numeric_agg_sources,
  kwargs: contrib_mod_args,
)
contrib_targets += shared_numeric_agg

install_data(
  'shared_numeric_agg--1.0.sql',
  'shared_numeric_agg.control',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'shared_numeric_agg',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'shared_numeric_agg',
    ],
  },
}
