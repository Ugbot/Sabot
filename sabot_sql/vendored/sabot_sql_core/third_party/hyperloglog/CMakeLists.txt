if(POLICY CMP0063)
    cmake_policy(SET CMP0063 NEW)
endif()

add_library(sabot_sql_hyperloglog STATIC hyperloglog.cpp sds.cpp)

target_include_directories(
  sabot_sql_hyperloglog
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>)
set_target_properties(sabot_sql_hyperloglog PROPERTIES EXPORT_NAME sabot_sql_hyperloglog)

install(TARGETS sabot_sql_hyperloglog
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

disable_target_warnings(sabot_sql_hyperloglog)