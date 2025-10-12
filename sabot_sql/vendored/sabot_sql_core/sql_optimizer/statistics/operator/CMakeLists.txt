add_library(
  sabot_sql_optimizer_statistics_op
  STATIC
  propagate_aggregate.cpp
  propagate_cross_product.cpp
  propagate_filter.cpp
  propagate_get.cpp
  propagate_limit.cpp
  propagate_join.cpp
  propagate_order.cpp
  propagate_projection.cpp
  propagate_set_operation.cpp
  propagate_window.cpp)
set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_optimizer_statistics_op>
    PARENT_SCOPE)
