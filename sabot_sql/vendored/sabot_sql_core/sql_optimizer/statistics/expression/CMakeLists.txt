add_library(
  sabot_sql_optimizer_statistics_expr
  STATIC
  propagate_aggregate.cpp
  propagate_between.cpp
  propagate_case.cpp
  propagate_cast.cpp
  propagate_comparison.cpp
  propagate_conjunction.cpp
  propagate_constant.cpp
  propagate_columnref.cpp
  propagate_function.cpp
  propagate_operator.cpp)
set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_optimizer_statistics_expr>
    PARENT_SCOPE)
