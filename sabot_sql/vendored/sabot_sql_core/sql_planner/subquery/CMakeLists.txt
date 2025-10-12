add_library(
  sabot_sql_planner_subquery STATIC flatten_dependent_join.cpp
  has_correlated_expressions.cpp rewrite_correlated_expressions.cpp
  rewrite_cte_scan.cpp)
set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_planner_subquery>
    PARENT_SCOPE)
