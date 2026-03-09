online_analyze_sources = files(
  'online_analyze.c'
)

if host_system == 'windows'
  online_analyze_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'online_analyze',
    '--FILEDESC', 'online_analyze',])
endif

online_analyze = shared_module('online_analyze',
  online_analyze_sources,
  kwargs: contrib_mod_args + {
    'dependencies': contrib_mod_args['dependencies'],
  },
)
contrib_targets += online_analyze

install_data(
  kwargs: contrib_data_args,
)


# TODO: DOCS = README.online_analyze