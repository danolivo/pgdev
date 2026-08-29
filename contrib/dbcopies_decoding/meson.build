dbcopies_decoding_sources = files(
  'dbcopies_decoding.c',
  '../mchar/mchar_recode.c'
)

if host_system == 'windows'
  dbcopies_decoding_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'dbcopies_decoding',
    '--FILEDESC', 'dbcopies_decoding',])
endif

dbcopies_decoding = shared_module('dbcopies_decoding',
  dbcopies_decoding_sources,
   include_directories: '../mchar',
  kwargs: contrib_mod_args + {
    'dependencies': [icu_i18n, contrib_mod_args['dependencies']],
  },
)
contrib_targets += dbcopies_decoding

install_data(
  kwargs: contrib_data_args,
)

tests += {
  'name': 'dbcopies_decoding',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'simple'
    ],
    'regress_args': ['--temp-config', files('logical.conf')],
    # Disabled because these tests require "wal_level=logical", which
    # typical installcheck users do not have (e.g. buildfarm clients).
    'runningcheck': false,
  },
}
