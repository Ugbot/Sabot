add_library(sabot_sql_func_variant_main STATIC variant_utils.cpp
                  variant_extract.cpp variant_typeof.cpp)
set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_func_variant_main>
    PARENT_SCOPE)
