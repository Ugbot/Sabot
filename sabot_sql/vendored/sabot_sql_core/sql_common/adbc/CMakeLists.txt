add_library(sabot_sql_adbc STATIC adbc.cpp driver_manager.cpp)
add_subdirectory(nanoarrow)
set(ALL_STATIC_FILES
    ${ALL_STATIC_FILES} $<TARGET_STATICS:sabot_sql_adbc>
    PARENT_SCOPE)
