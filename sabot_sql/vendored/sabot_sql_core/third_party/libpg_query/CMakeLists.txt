cmake_minimum_required(VERSION 3.5...3.29)

project(sabot_sql_pg_query CXX C)
if(POLICY CMP0063)
    cmake_policy(SET CMP0063 NEW)
endif()
set(CMAKE_CXX_VISIBILITY_PRESET hidden)

add_library(sabot_sql_pg_query STATIC
            postgres_parser.cpp
            pg_functions.cpp
            src_backend_parser_parser.cpp
            src_backend_parser_gram.cpp
            src_backend_nodes_list.cpp
            src_backend_parser_scansup.cpp
            src_backend_nodes_makefuncs.cpp
            src_backend_nodes_value.cpp
            src_backend_parser_scan.cpp
            src_common_keywords.cpp)

target_include_directories(
  sabot_sql_pg_query
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)
set_target_properties(sabot_sql_pg_query PROPERTIES EXPORT_NAME sabot_sql_sabot_sql_pg_query)

install(TARGETS sabot_sql_pg_query
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

disable_target_warnings(sabot_sql_pg_query)

set_target_properties(sabot_sql_pg_query PROPERTIES POSITION_INDEPENDENT_CODE ON)
