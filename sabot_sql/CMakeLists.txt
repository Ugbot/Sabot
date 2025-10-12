cmake_minimum_required(VERSION 3.20)
project(SabotSQL VERSION 0.1.0 LANGUAGES CXX)

# C++20 required for modern features
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Find dependencies
find_package(Arrow REQUIRED PATHS ${CMAKE_SOURCE_DIR}/../vendor/arrow/cpp/build/install NO_DEFAULT_PATH)
find_package(ArrowFlight REQUIRED PATHS ${CMAKE_SOURCE_DIR}/../vendor/arrow/cpp/build/install NO_DEFAULT_PATH)

# DuckDB is in ../vendor/duckdb
set(DUCKDB_INCLUDE_DIR ${CMAKE_SOURCE_DIR}/../vendor/duckdb/src/include)
set(DUCKDB_BUILD_DIR ${CMAKE_SOURCE_DIR}/../vendor/duckdb/build/release)
# Try to find DuckDB library
find_library(DUCKDB_LIB 
    NAMES duckdb libduckdb
    PATHS ${DUCKDB_BUILD_DIR}/src
    NO_DEFAULT_PATH
)

# Include directories
include_directories(
    ${CMAKE_SOURCE_DIR}/include
    ${ARROW_INCLUDE_DIR}
    ${DUCKDB_INCLUDE_DIR}
)

# Library sources
set(SABOT_SQL_SOURCES
    # SQL engine
    src/sql/duckdb_bridge.cpp
    src/sql/sql_operator_translator.cpp
    src/sql/query_engine.cpp
    src/sql/enhanced_parser.cpp
    src/sql/streaming_operators.cpp
    
    # SQL-specific operators
    src/operators/table_scan.cpp
    src/operators/cte.cpp
    src/operators/subquery.cpp
)

# Main library
add_library(sabot_sql SHARED ${SABOT_SQL_SOURCES})

target_link_libraries(sabot_sql
    Arrow::arrow_shared
    ArrowFlight::arrow_flight_shared
)

# Link DuckDB if available
if(DUCKDB_LIB)
    target_link_libraries(sabot_sql ${DUCKDB_LIB})
    message(STATUS "DuckDB library found: ${DUCKDB_LIB}")
else()
    message(WARNING "DuckDB library not found - SQL features will not be available. Build DuckDB with: cd vendor/duckdb && make")
endif()

target_include_directories(sabot_sql PUBLIC
    ${CMAKE_SOURCE_DIR}/include
    ${ARROW_INCLUDE_DIR}
    ${DUCKDB_INCLUDE_DIR}
)

# Installation
install(TARGETS sabot_sql
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    RUNTIME DESTINATION bin
)

install(DIRECTORY include/sabot_sql
    DESTINATION include
    FILES_MATCHING PATTERN "*.h"
)

message(STATUS "SabotSQL configuration:")
message(STATUS "  Arrow include: ${ARROW_INCLUDE_DIR}")
message(STATUS "  DuckDB include: ${DUCKDB_INCLUDE_DIR}")
message(STATUS "  DuckDB library: ${DUCKDB_LIB}")
message(STATUS "  Build type: ${CMAKE_BUILD_TYPE}")

# Test executable
add_executable(sabot_sql_test
    examples/test_enhanced_parser.cpp
)

target_link_libraries(sabot_sql_test
    sabot_sql
    Arrow::arrow_shared
)

if(DUCKDB_LIB)
    target_link_libraries(sabot_sql_test ${DUCKDB_LIB})
endif()

# Enable testing
enable_testing()
add_test(NAME sabot_sql_test COMMAND sabot_sql_test)