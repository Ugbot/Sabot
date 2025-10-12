add_library(sabot_sql_value_operations STATIC comparison_operations.cpp)
set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_value_operations>
    PARENT_SCOPE)
