add_library(
  sabot_sql_bind_query_node
  STATIC
  bind_select_node.cpp
  bind_setop_node.cpp
  bind_recursive_cte_node.cpp
  bind_cte_node.cpp
  bind_table_macro_node.cpp
  plan_query_node.cpp
  plan_recursive_cte_node.cpp
  plan_cte_node.cpp
  plan_select_node.cpp
  plan_setop.cpp
  plan_subquery.cpp)
set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_bind_query_node>
    PARENT_SCOPE)
