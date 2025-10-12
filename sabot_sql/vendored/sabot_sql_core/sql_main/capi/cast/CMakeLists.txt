add_library(sabot_sql_main_capi_cast STATIC utils-c.cpp from_decimal-c.cpp)

set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_main_capi_cast>
    PARENT_SCOPE)
