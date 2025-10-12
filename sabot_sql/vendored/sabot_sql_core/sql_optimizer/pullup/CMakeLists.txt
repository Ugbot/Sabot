add_library(
  sabot_sql_optimizer_pullup
  STATIC
  pullup_filter.cpp
  pullup_projection.cpp
  pullup_from_left.cpp
  pullup_both_side.cpp
  pullup_set_operation.cpp)
set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_optimizer_pullup>
    PARENT_SCOPE)
