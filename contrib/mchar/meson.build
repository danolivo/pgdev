mchar_sources = files(
  'mchar_io.c',
  'mchar_proc.c',
  'mchar_op.c',
  'mchar_recode.c',
  'mchar_like.c'
)

if host_system == 'windows'
  mchar_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'mchar',
    '--FILEDESC', 'mchar',])
endif

mchar = shared_module('mchar',
  mchar_sources,
  kwargs: contrib_mod_args + {
    'dependencies': [icu_i18n, contrib_mod_args['dependencies']],
  },
)
contrib_targets += mchar

install_data(
  'mchar.control',
  'mchar--2.2.1.sql',
  'mchar--2.0.1--2.1.sql',
  'mchar--2.0--2.1.sql',
  'mchar--2.1.1--2.2.sql',
  'mchar--2.1--2.2.sql',
  'mchar--2.2--2.2.1.sql',
  'mchar--unpackaged--2.0.sql',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'mchar',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'init',
      'mchar',
      'mvarchar',
      'mm',
      'like',
      'compat',
    ]
  },
}

# TODO: DOCS = README.mchar