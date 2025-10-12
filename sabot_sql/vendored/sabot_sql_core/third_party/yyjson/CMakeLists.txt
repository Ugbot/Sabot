if(POLICY CMP0063)
    cmake_policy(SET CMP0063 NEW)
endif()

add_library(sabot_sql_yyjson STATIC yyjson.cpp)

target_include_directories(
        sabot_sql_yyjson
        PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)
set_target_properties(sabot_sql_yyjson PROPERTIES EXPORT_NAME sabot_sql_sabot_sql_yyjson)

install(TARGETS sabot_sql_yyjson
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

disable_target_warnings(sabot_sql_yyjson)
