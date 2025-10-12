add_subdirectory(distributive)

add_library(sabot_sql_func_aggr STATIC sorted_aggregate_function.cpp)
set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_func_aggr>
    PARENT_SCOPE)
