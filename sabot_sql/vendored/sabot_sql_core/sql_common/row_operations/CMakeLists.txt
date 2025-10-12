add_library(
  sabot_sql_row_operations
  STATIC
  row_aggregate.cpp
  row_scatter.cpp
  row_gather.cpp
  row_matcher.cpp
  row_external.cpp
  row_radix_scatter.cpp
  row_heap_scatter.cpp
  row_heap_gather.cpp)
set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_row_operations>
    PARENT_SCOPE)
