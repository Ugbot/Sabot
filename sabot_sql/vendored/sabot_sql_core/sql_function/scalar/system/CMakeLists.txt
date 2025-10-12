add_library(
  sabot_sql_func_system
  STATIC
  aggregate_export.cpp
  write_log.cpp
  current_transaction_id.cpp
  current_connection_id.cpp
  current_query_id.cpp
  parse_log_message.cpp)
set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_func_system>
    PARENT_SCOPE)
