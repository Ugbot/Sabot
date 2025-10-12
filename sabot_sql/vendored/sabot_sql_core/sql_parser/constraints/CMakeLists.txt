add_library(
  sabot_sql_constraints STATIC check_constraint.cpp not_null_constraint.cpp
  unique_constraint.cpp foreign_key_constraint.cpp)
set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_constraints>
    PARENT_SCOPE)
