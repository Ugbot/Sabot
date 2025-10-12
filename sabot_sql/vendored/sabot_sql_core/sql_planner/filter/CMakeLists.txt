add_library(
  sabot_sql_planner_filter
  STATIC
  conjunction_filter.cpp
  constant_filter.cpp
  dynamic_filter.cpp
  expression_filter.cpp
  in_filter.cpp
  null_filter.cpp
  struct_filter.cpp
  optional_filter.cpp)
set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_planner_filter>
    PARENT_SCOPE)
