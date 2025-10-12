if(POLICY CMP0063)
    cmake_policy(SET CMP0063 NEW)
endif()

add_library(sabot_sql_skiplistlib STATIC SkipList.cpp)

target_include_directories(
  sabot_sql_skiplistlib
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>)
set_target_properties(sabot_sql_skiplistlib PROPERTIES EXPORT_NAME sabot_sql_skiplistlib)

install(TARGETS sabot_sql_skiplistlib
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

disable_target_warnings(sabot_sql_skiplistlib)
