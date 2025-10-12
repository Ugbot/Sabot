add_library(sabot_sql_function_map STATIC map_contains.cpp)

set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_function_map>
    PARENT_SCOPE)
