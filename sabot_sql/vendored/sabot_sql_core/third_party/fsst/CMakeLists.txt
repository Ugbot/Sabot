if(POLICY CMP0063)
    cmake_policy(SET CMP0063 NEW)
endif()

find_package(Threads REQUIRED)

set(CMAKE_CXX_VISIBILITY_PRESET hidden)

add_library(sabot_sql_fsst STATIC libfsst.cpp)

target_include_directories(sabot_sql_fsst PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>)
set_target_properties(sabot_sql_fsst PROPERTIES EXPORT_NAME sabot_sql_fsst)

install(TARGETS sabot_sql_fsst
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

disable_target_warnings(sabot_sql_fsst)