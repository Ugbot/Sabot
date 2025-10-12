add_library(sabot_sql_arrow_conversion STATIC arrow_duck_schema.cpp
                  arrow_array_scan_state.cpp arrow_type_info.cpp)
set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_arrow_conversion>
    PARENT_SCOPE)
