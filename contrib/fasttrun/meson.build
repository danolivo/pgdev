fasttrun_sources = files(
  'fasttrun.c'
)

if host_system == 'windows'
  fasttrun_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'fasttrun',
    '--FILEDESC', 'fasttrun',])
endif

fasttrun = shared_module('fasttrun',
  fasttrun_sources,
  kwargs: contrib_mod_args + {
    'dependencies': contrib_mod_args['dependencies'],
  },
)
contrib_targets += fasttrun

install_data(
  'fasttrun.control',
  'fasttrun--2.0.sql',
  'fasttrun--unpackaged--2.0.sql',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'fasttrun',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'fasttrun'
    ]
  },
}

# TODO: DOCS = README.fasttrun