add_library(sabot_sql_common_operators STATIC cast_operators.cpp
                  convert_to_string.cpp string_cast.cpp)
set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_common_operators>
    PARENT_SCOPE)
