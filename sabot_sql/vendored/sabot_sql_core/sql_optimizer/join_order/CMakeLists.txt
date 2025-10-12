add_library(
  sabot_sql_optimizer_join_order
  STATIC
  query_graph.cpp
  join_relation_set.cpp
  join_node.cpp
  join_order_optimizer.cpp
  cardinality_estimator.cpp
  cost_model.cpp
  plan_enumerator.cpp
  relation_manager.cpp
  query_graph_manager.cpp
  relation_statistics_helper.cpp)
set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_optimizer_join_order>
    PARENT_SCOPE)
