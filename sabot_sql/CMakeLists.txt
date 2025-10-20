cmake_minimum_required(VERSION 3.20)
project(SabotSQL VERSION 0.1.0 LANGUAGES CXX)

# C++20 required for modern features
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Find dependencies
find_package(Arrow REQUIRED PATHS ${CMAKE_SOURCE_DIR}/../vendor/arrow/cpp/build/install NO_DEFAULT_PATH)
find_package(ArrowFlight REQUIRED PATHS ${CMAKE_SOURCE_DIR}/../vendor/arrow/cpp/build/install NO_DEFAULT_PATH)

# Add librdkafka (vendored source)
set(RDKAFKA_BUILD_STATIC ON CACHE BOOL "Build static library")
set(RDKAFKA_BUILD_EXAMPLES OFF CACHE BOOL "Build examples")
set(RDKAFKA_BUILD_TESTS OFF CACHE BOOL "Build tests")
add_subdirectory(${CMAKE_SOURCE_DIR}/../vendor/librdkafka ${CMAKE_BINARY_DIR}/librdkafka)

# Include directories
include_directories(
    ${CMAKE_SOURCE_DIR}/include
    ${ARROW_INCLUDE_DIR}
    ${CMAKE_SOURCE_DIR}/../vendor/librdkafka/src-cpp
    ${CMAKE_SOURCE_DIR}/../vendor/librdkafka/src
    ${CMAKE_SOURCE_DIR}/../vendor
)

# SabotSQL library sources
set(SABOT_SQL_SOURCES
    # SQL engine
    src/sql/simple_sabot_sql_bridge.cpp
    src/sql/sabot_operator_translator.cpp
    src/sql/binder_rewrites.cpp
    # Execution
    src/execution/morsel_executor.cpp
    # Streaming connectors
    src/streaming/source_connector.cpp
    src/streaming/kafka_connector.cpp
)

# Main library
add_library(sabot_sql SHARED ${SABOT_SQL_SOURCES})

target_link_libraries(sabot_sql
    Arrow::arrow_shared
    ArrowFlight::arrow_flight_shared
    rdkafka++
)

target_include_directories(sabot_sql PUBLIC
    ${CMAKE_SOURCE_DIR}/include
    ${ARROW_INCLUDE_DIR}
)

# Enforce Sabot-only execution: define a macro for conditional compilation if needed
target_compile_definitions(sabot_sql PUBLIC SABOT_SQL_EXECUTION_SABOT_ONLY)

# Installation
install(TARGETS sabot_sql
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
)

install(DIRECTORY include/ DESTINATION include)

# Examples/Tests
add_executable(test_kafka_connector examples/test_kafka_connector.cpp)
target_link_libraries(test_kafka_connector sabot_sql)

message(STATUS "SabotSQL minimal configuration:")
message(STATUS "  Arrow include: ${ARROW_INCLUDE_DIR}")
message(STATUS "  Build type: ${CMAKE_BUILD_TYPE}")
message(STATUS "  Note: Minimal build for testing renamed architecture")
