if(POLICY CMP0063)
    cmake_policy(SET CMP0063 NEW)
endif()

add_library(sabot_sql_miniz STATIC miniz.cpp)

target_include_directories(
  sabot_sql_miniz
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>)
set_target_properties(sabot_sql_miniz PROPERTIES EXPORT_NAME sabot_sql_sabot_sql_miniz)

install(TARGETS sabot_sql_miniz
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

disable_target_warnings(sabot_sql_miniz)
