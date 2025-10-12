add_library(
  sabot_sql_common_arrow
  STATIC
  arrow_appender.cpp
  arrow_converter.cpp
  arrow_type_extension.cpp
  arrow_merge_event.cpp
  arrow_query_result.cpp
  arrow_util.cpp
  arrow_wrapper.cpp
  physical_arrow_collector.cpp
  physical_arrow_batch_collector.cpp
  schema_metadata.cpp)
add_subdirectory(appender)
set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_common_arrow>
    PARENT_SCOPE)
