add_library(sabot_sql_common_crypto STATIC md5.cpp)
set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_common_crypto>
    PARENT_SCOPE)
