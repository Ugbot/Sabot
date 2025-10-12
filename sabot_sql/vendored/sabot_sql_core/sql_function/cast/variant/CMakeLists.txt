add_library(sabot_sql_variant_cast STATIC from_variant.cpp to_variant.cpp
                  to_json.cpp)

set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_variant_cast>
    PARENT_SCOPE)
