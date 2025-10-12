add_library(sabot_sql_func_generic_main STATIC constant_or_null.cpp
                  error.cpp getvariable.cpp)

set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_func_generic_main>
    PARENT_SCOPE)
