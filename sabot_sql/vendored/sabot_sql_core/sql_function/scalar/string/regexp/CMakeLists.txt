add_library(sabot_sql_func_string_regexp STATIC regexp_util.cpp
                  regexp_extract_all.cpp)
set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_func_string_regexp>
    PARENT_SCOPE)
