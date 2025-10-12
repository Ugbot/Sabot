add_library(
  sabot_sql_optimizer_compressed_materialization STATIC compress_aggregate.cpp
  compress_comparison_join.cpp compress_distinct.cpp compress_order.cpp)
set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES}
    $<TARGET_STATICS:sabot_sql_optimizer_compressed_materialization>
    PARENT_SCOPE)
