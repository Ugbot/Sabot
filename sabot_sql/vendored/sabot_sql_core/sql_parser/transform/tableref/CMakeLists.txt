add_library(
  sabot_sql_transformer_tableref
  STATIC
  transform_base_tableref.cpp
  transform_join.cpp
  transform_from.cpp
  transform_pivot.cpp
  transform_subquery.cpp
  transform_table_function.cpp
  transform_tableref.cpp)
set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_transformer_tableref>
    PARENT_SCOPE)
