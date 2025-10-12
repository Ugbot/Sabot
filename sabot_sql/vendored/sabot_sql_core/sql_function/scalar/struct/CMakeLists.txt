add_library(
  sabot_sql_func_struct_main
  STATIC
  remap_struct.cpp
  struct_extract.cpp
  struct_pack.cpp
  struct_concat.cpp
  struct_contains.cpp)
set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_func_struct_main>
    PARENT_SCOPE)
