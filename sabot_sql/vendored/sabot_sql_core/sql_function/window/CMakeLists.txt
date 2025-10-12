add_library(
  sabot_sql_func_window
  STATIC
  window_aggregate_function.cpp
  window_aggregate_states.cpp
  window_aggregator.cpp
  window_boundaries_state.cpp
  window_collection.cpp
  window_constant_aggregator.cpp
  window_custom_aggregator.cpp
  window_distinct_aggregator.cpp
  window_executor.cpp
  window_index_tree.cpp
  window_merge_sort_tree.cpp
  window_naive_aggregator.cpp
  window_rank_function.cpp
  window_rownumber_function.cpp
  window_segment_tree.cpp
  window_shared_expressions.cpp
  window_token_tree.cpp
  window_value_function.cpp)

set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_func_window>
    PARENT_SCOPE)
