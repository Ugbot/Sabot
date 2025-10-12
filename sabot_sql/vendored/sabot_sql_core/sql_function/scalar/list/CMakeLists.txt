add_library(
  sabot_sql_func_list_nested
  STATIC
  contains_or_position.cpp
  list_extract.cpp
  list_resize.cpp
  list_zip.cpp
  list_select.cpp)
set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_func_list_nested>
    PARENT_SCOPE)
