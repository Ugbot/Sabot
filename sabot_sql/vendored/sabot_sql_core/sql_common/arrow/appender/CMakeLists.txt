add_library(
  sabot_sql_common_arrow_appender
  STATIC
  bool_data.cpp
  fixed_size_list_data.cpp
  null_data.cpp
  struct_data.cpp
  union_data.cpp
  append_data.cpp)
set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_common_arrow_appender>
    PARENT_SCOPE)
