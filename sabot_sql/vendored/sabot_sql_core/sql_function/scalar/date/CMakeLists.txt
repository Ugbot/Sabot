add_library(sabot_sql_func_date STATIC strftime.cpp)

set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_func_date>
    PARENT_SCOPE)
