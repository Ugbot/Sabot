add_subdirectory(expression)
add_subdirectory(parsed_data)
add_subdirectory(query_node)
add_subdirectory(statement)
add_subdirectory(tableref)
add_subdirectory(transform)
add_subdirectory(constraints)

add_library(
  sabot_sql_parser
  STATIC
  base_expression.cpp
  column_definition.cpp
  column_list.cpp
  constraint.cpp
  expression_util.cpp
  keyword_helper.cpp
  parsed_expression_iterator.cpp
  parsed_expression.cpp
  parser.cpp
  qualified_name.cpp
  query_error_context.cpp
  query_node.cpp
  result_modifier.cpp
  tableref.cpp
  transformer.cpp
)

set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_parser>
    PARENT_SCOPE)