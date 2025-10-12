add_library(
  sabot_sql_common_multi_file
  STATIC
  base_file_reader.cpp
  multi_file_function.cpp
  multi_file_list.cpp
  multi_file_reader.cpp
  multi_file_column_mapper.cpp
  union_by_name.cpp)
set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_common_multi_file>
    PARENT_SCOPE)
