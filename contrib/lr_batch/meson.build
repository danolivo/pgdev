# Copyright (c) 2026, PostgreSQL Global Development Group

lr_batch_sources = files(
  'lr_batch.c',
)

if host_system == 'windows'
  lr_batch_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'lr_batch',
    '--FILEDESC', 'lr_batch - batch INSERTs in the logical replication apply worker',])
endif

lr_batch = shared_module('lr_batch',
  lr_batch_sources,
  kwargs: contrib_mod_args,
)
contrib_targets += lr_batch

tests += {
  'name': 'lr_batch',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'tap': {
    'tests': [
      't/001_batch.pl',
    ],
  },
}
