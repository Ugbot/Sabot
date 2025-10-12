add_library(sabot_sql_func_pragma STATIC pragma_functions.cpp
                  pragma_queries.cpp)

set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_func_pragma>
    PARENT_SCOPE)
