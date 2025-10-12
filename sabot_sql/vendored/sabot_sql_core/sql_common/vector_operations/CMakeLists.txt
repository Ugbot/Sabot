add_library(
  sabot_sql_vector_operations STATIC
  boolean_operators.cpp
  vector_cast.cpp
  comparison_operators.cpp
  vector_copy.cpp
  generators.cpp
  vector_hash.cpp
  vector_storage.cpp
  null_operations.cpp
  numeric_inplace_operators.cpp
  is_distinct_from.cpp)
set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_vector_operations>
    PARENT_SCOPE)
