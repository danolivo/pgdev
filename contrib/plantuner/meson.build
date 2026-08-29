plantuner_sources = files(
  'plantuner.c'
)

if host_system == 'windows'
  plantuner_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'plantuner',
    '--FILEDESC', 'plantuner',])
endif

plantuner = shared_module('plantuner',
  plantuner_sources,
  kwargs: contrib_mod_args + {
    'dependencies': contrib_mod_args['dependencies'],
  },
)
contrib_targets += plantuner

install_data(
  kwargs: contrib_data_args,
)

tests += {
  'name': 'plantuner',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'plantuner',
    ]
  },
}

# TODO: DOCS = README.plantuner