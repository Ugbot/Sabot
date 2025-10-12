add_subdirectory(cast)

add_library(
  sabot_sql_main_capi
  STATIC
  aggregate_function-c.cpp
  appender-c.cpp
  arrow-c.cpp
  cast_function-c.cpp
  config-c.cpp
  data_chunk-c.cpp
  datetime-c.cpp
  sabot_sql-c.cpp
  sabot_sql_value-c.cpp
  error_data-c.cpp
  expression-c.cpp
  helper-c.cpp
  hugeint-c.cpp
  logical_types-c.cpp
  pending-c.cpp
  prepared-c.cpp
  profiling_info-c.cpp
  replacement_scan-c.cpp
  result-c.cpp
  scalar_function-c.cpp
  stream-c.cpp
  table_description-c.cpp
  table_function-c.cpp
  threading-c.cpp
  value-c.cpp
  file_system-c.cpp)

set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_main_capi>
    PARENT_SCOPE)
