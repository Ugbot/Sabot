add_subdirectory(dependency)

add_library(
  sabot_sql_catalog_entries
  STATIC
  copy_function_catalog_entry.cpp
  duck_index_entry.cpp
  duck_schema_entry.cpp
  duck_table_entry.cpp
  type_catalog_entry.cpp
  index_catalog_entry.cpp
  macro_catalog_entry.cpp
  pragma_function_catalog_entry.cpp
  schema_catalog_entry.cpp
  sequence_catalog_entry.cpp
  table_catalog_entry.cpp
  column_dependency_manager.cpp
  scalar_function_catalog_entry.cpp
  table_function_catalog_entry.cpp
  view_catalog_entry.cpp)
set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_catalog_entries>
    PARENT_SCOPE)
