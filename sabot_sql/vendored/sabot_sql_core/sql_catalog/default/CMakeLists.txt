add_library(
  sabot_sql_catalog_default_entries
  STATIC
  default_functions.cpp
  default_generator.cpp
  default_schemas.cpp
  default_table_functions.cpp
  default_types.cpp
  default_views.cpp)
set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_catalog_default_entries>
    PARENT_SCOPE)
