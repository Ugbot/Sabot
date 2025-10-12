add_library(sabot_sql_union_cast STATIC from_struct.cpp)

set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_union_cast>
    PARENT_SCOPE)
