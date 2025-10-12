add_library(sabot_sql_func_seq STATIC nextval.cpp)
set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_func_seq>
    PARENT_SCOPE)
