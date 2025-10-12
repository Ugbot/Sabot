add_subdirectory(binder)
add_subdirectory(expression)
add_subdirectory(expression_binder)
add_subdirectory(filter)
add_subdirectory(operator)
add_subdirectory(subquery)

add_library(
  sabot_sql_planner
  STATIC
  bind_context.cpp
  binder.cpp
  binding_alias.cpp
  bound_parameter_map.cpp
  bound_result_modifier.cpp
  collation_binding.cpp
  expression.cpp
  expression_binder.cpp
  expression_iterator.cpp
  joinside.cpp
  logical_operator_deep_copy.cpp
  logical_operator_visitor.cpp
  logical_operator.cpp
  planner.cpp
  pragma_handler.cpp
  table_binding.cpp
  table_filter_state.cpp
  table_filter.cpp
)

set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_planner>
    PARENT_SCOPE)