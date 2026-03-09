auto_dump_sources = files(
  'auto_dump.c',
)

auto_dump = shared_module('auto_dump',
  auto_dump_sources,
  kwargs: contrib_mod_args,
)
contrib_targets += auto_dump

install_data(
  'auto_dump.control',
  'auto_dump--1.0.sql',
  'auto_dump--1.0--1.1.sql',
  kwargs: contrib_data_args,
)

# tests += {
#   'name': 'auto_dump',
#   'sd': meson.current_source_dir(),
#   'bd': meson.current_build_dir(),
#   'regress': {
#     'sql': [
#       'dump',
#       # 'bad_plan',
#     ],
#   },
# }
