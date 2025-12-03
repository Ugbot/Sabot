cmake_minimum_required(VERSION 3.20)
project(SabotSQL VERSION 0.1.0 LANGUAGES CXX C)

# C++17 required
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Build type
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

# Compiler flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG")
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0")

# ============================================================================
# Find DuckDB (vendored as git submodule)
# ============================================================================

set(DUCKDB_ROOT ${CMAKE_SOURCE_DIR}/../vendor/duckdb)
set(DUCKDB_INCLUDE_DIR ${DUCKDB_ROOT}/src/include)
set(DUCKDB_LIBRARY ${DUCKDB_ROOT}/build/release/src/libduckdb.dylib)

if(NOT EXISTS ${DUCKDB_LIBRARY})
    message(FATAL_ERROR "DuckDB library not found at ${DUCKDB_LIBRARY}. Please build DuckDB first:\n  cd ${DUCKDB_ROOT} && make")
endif()

message(STATUS "DuckDB library: ${DUCKDB_LIBRARY}")
message(STATUS "DuckDB include: ${DUCKDB_INCLUDE_DIR}")

# ============================================================================
# Find Arrow (vendored)
# ============================================================================

set(ARROW_ROOT ${CMAKE_SOURCE_DIR}/../vendor/arrow/cpp/build/install)
set(ARROW_INCLUDE_DIR ${ARROW_ROOT}/include)
set(ARROW_LIB_DIR ${ARROW_ROOT}/lib)

find_library(ARROW_LIB arrow PATHS ${ARROW_LIB_DIR} NO_DEFAULT_PATH REQUIRED)
find_library(ARROW_FLIGHT_LIB arrow_flight PATHS ${ARROW_LIB_DIR} NO_DEFAULT_PATH REQUIRED)

message(STATUS "Arrow library: ${ARROW_LIB}")
message(STATUS "Arrow include: ${ARROW_INCLUDE_DIR}")

# ============================================================================
# Find other vendored dependencies
# ============================================================================

# librdkafka (built in sabot_sql/)
find_library(RDKAFKA_LIB rdkafka++
    PATHS ${CMAKE_SOURCE_DIR}/librdkafka/src-cpp
    NO_DEFAULT_PATH REQUIRED)

# simdjson (built in sabot_sql/)
find_library(SIMDJSON_LIB simdjson
    PATHS ${CMAKE_SOURCE_DIR}/simdjson
    NO_DEFAULT_PATH REQUIRED)

# Avro (built in sabot_sql/)
find_library(AVRO_LIB avrocpp_s
    PATHS ${CMAKE_SOURCE_DIR}/avro-cpp
    NO_DEFAULT_PATH REQUIRED)

# Protobuf (built in sabot_sql/)
find_library(PROTOBUF_LIB protobuf
    PATHS ${CMAKE_SOURCE_DIR}/protobuf
    NO_DEFAULT_PATH REQUIRED)

# CURL (system)
find_package(CURL REQUIRED)

# ============================================================================
# Include directories
# ============================================================================

include_directories(
    ${CMAKE_SOURCE_DIR}/include
    ${DUCKDB_INCLUDE_DIR}
    ${ARROW_INCLUDE_DIR}
    ${CMAKE_SOURCE_DIR}/librdkafka/src-cpp
    ${CMAKE_SOURCE_DIR}/simdjson/singleheader
    ${CMAKE_SOURCE_DIR}/avro-cpp/api
    ${CMAKE_SOURCE_DIR}/protobuf/src
)

# ============================================================================
# Collect source files
# ============================================================================

# Sabot SQL sources - Only SQL core files for now (streaming excluded due to missing deps)
file(GLOB SABOT_SQL_SOURCES
    "${CMAKE_SOURCE_DIR}/src/sql/*.cpp"
)

# Exclude SQL files with compilation issues (API mismatches with current DuckDB/Arrow)
list(FILTER SABOT_SQL_SOURCES EXCLUDE REGEX "enhanced_parser\\.cpp$")
list(FILTER SABOT_SQL_SOURCES EXCLUDE REGEX "streaming_operators\\.cpp$")
list(FILTER SABOT_SQL_SOURCES EXCLUDE REGEX "extended_parser\\.cpp$")
list(FILTER SABOT_SQL_SOURCES EXCLUDE REGEX "sabot_sql_bridge\\.cpp$")
list(FILTER SABOT_SQL_SOURCES EXCLUDE REGEX "query_engine\\.cpp$")
list(FILTER SABOT_SQL_SOURCES EXCLUDE REGEX "sql_operator_translator\\.cpp$")
list(FILTER SABOT_SQL_SOURCES EXCLUDE REGEX "sabot_operator_translator\\.cpp$")
list(FILTER SABOT_SQL_SOURCES EXCLUDE REGEX "c_api\\.cpp$")
list(FILTER SABOT_SQL_SOURCES EXCLUDE REGEX "simple_executor\\.cpp$")
list(FILTER SABOT_SQL_SOURCES EXCLUDE REGEX "flink_sql_extension\\.cpp$")

# Count sources for logging
list(LENGTH SABOT_SQL_SOURCES SABOT_SQL_COUNT)

# ============================================================================
# Create SHARED library
# ============================================================================

add_library(sabot_sql SHARED
    ${SABOT_SQL_SOURCES}
)

# Compiler definitions
target_compile_definitions(sabot_sql PUBLIC
    SABOT_SQL_BUILD_LIBRARY
)

# Include directories
target_include_directories(sabot_sql PUBLIC
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${DUCKDB_INCLUDE_DIR}>
    $<BUILD_INTERFACE:${ARROW_INCLUDE_DIR}>
    $<INSTALL_INTERFACE:include>
)

# Link libraries
target_link_libraries(sabot_sql
    ${ARROW_LIB}
    ${ARROW_FLIGHT_LIB}
    ${DUCKDB_LIBRARY}
    ${RDKAFKA_LIB}
    ${SIMDJSON_LIB}
    ${AVRO_LIB}
    ${PROTOBUF_LIB}
    CURL::libcurl
)

# Install
install(TARGETS sabot_sql
    LIBRARY DESTINATION lib
)

install(DIRECTORY include/
    DESTINATION include
    FILES_MATCHING PATTERN "*.hpp" PATTERN "*.h"
)

# Summary
message(STATUS "============================================")
message(STATUS "SabotSQL Shared Library")
message(STATUS "============================================")
message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")
message(STATUS "C++ standard: ${CMAKE_CXX_STANDARD}")
message(STATUS "Sabot SQL sources: ${SABOT_SQL_COUNT}")
message(STATUS "Output: libsabot_sql.dylib")
message(STATUS "Uses: DuckDB library (${DUCKDB_LIBRARY})")
message(STATUS "============================================")
