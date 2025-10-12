add_library(
  sabot_sql_transformer_helpers
  STATIC
  transform_alias.cpp
  transform_cte.cpp
  transform_groupby.cpp
  transform_orderby.cpp
  transform_sample.cpp
  transform_typename.cpp
  nodetype_to_string.cpp)
set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_transformer_helpers>
    PARENT_SCOPE)
