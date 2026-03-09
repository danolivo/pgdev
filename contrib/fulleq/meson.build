fulleq_sources = files(
  'fulleq.c'
)

if host_system == 'windows'
  fulleq_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'fulleq',
    '--FILEDESC', 'fulleq',])
endif

contrib_targets += custom_target('fulleq--2.0.sql',
  input: 'fulleq--2.0.sql.in',
  output: 'fulleq--2.0.sql',
  command: [meson.current_source_dir() / 'generate.sh', 'packaged', '@INPUT@'],
  capture: true,
  install: true,
  install_dir: contrib_data_args['install_dir'],
)

contrib_targets += custom_target('fulleq--unpackaged--2.0.sql',
  input: 'fulleq--unpackaged--2.0.sql.in',
  output: 'fulleq--unpackaged--2.0.sql',
  command: [meson.current_source_dir() / 'generate.sh', 'unpackaged', '@INPUT@'],
  capture: true,
  install: true,
  install_dir: contrib_data_args['install_dir'],
)

fulleq = shared_module('fulleq',
  fulleq_sources,
  kwargs: contrib_mod_args + {
    'dependencies': contrib_mod_args['dependencies'],
  },
)
contrib_targets += fulleq

install_data(
  'fulleq.control',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'fulleq',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'fulleq'
    ]
  },
}

# TODO: DOCS = README.fulleq