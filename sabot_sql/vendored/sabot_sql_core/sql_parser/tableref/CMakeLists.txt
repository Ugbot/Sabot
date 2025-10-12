add_library(
  sabot_sql_parser_tableref
  STATIC
  at_clause.cpp
  basetableref.cpp
  bound_ref_wrapper.cpp
  delimgetref.cpp
  emptytableref.cpp
  expressionlistref.cpp
  column_data_ref.cpp
  joinref.cpp
  pivotref.cpp
  showref.cpp
  subqueryref.cpp
  table_function.cpp)
set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_parser_tableref>
    PARENT_SCOPE)
