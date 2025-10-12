add_library(
  sabot_sql_main
  STATIC
  client_context.cpp
  connection.cpp
  database.cpp
  db_instance.cpp
  query_profiler.cpp
  query_result.cpp
  secret_manager.cpp
  secret_storage.cpp
  storage_manager.cpp
)

set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_main>
    PARENT_SCOPE)