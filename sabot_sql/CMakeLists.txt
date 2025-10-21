cmake_minimum_required(VERSION 3.20)
project(SabotSQL VERSION 0.1.0 LANGUAGES CXX)

# C++20 required for modern features
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Find dependencies
find_package(Arrow REQUIRED PATHS ${CMAKE_SOURCE_DIR}/../vendor/arrow/cpp/build/install NO_DEFAULT_PATH)
find_package(ArrowFlight REQUIRED PATHS ${CMAKE_SOURCE_DIR}/../vendor/arrow/cpp/build/install NO_DEFAULT_PATH)
find_package(CURL REQUIRED)

# Add librdkafka (vendored source)
set(RDKAFKA_BUILD_STATIC ON CACHE BOOL "Build static library")
set(RDKAFKA_BUILD_EXAMPLES OFF CACHE BOOL "Build examples")
set(RDKAFKA_BUILD_TESTS OFF CACHE BOOL "Build tests")
add_subdirectory(${CMAKE_SOURCE_DIR}/../vendor/librdkafka ${CMAKE_BINARY_DIR}/librdkafka)

# Add simdjson (vendored source - SIMD-accelerated JSON parsing)
set(SIMDJSON_JUST_LIBRARY ON CACHE BOOL "Build library only")
set(SIMDJSON_BUILD_STATIC ON CACHE BOOL "Build static library")
add_subdirectory(${CMAKE_SOURCE_DIR}/../vendor/simdjson ${CMAKE_BINARY_DIR}/simdjson)

# Add Avro C++ (vendored source - for Avro deserialization)
set(BUILD_SHARED_LIBS OFF CACHE BOOL "Build static library")
add_subdirectory(${CMAKE_SOURCE_DIR}/../vendor/avro/lang/c++ ${CMAKE_BINARY_DIR}/avro-cpp EXCLUDE_FROM_ALL)

# Add Protobuf (vendored source - for Protobuf deserialization)
set(protobuf_BUILD_TESTS OFF CACHE BOOL "Build tests")
set(protobuf_BUILD_EXAMPLES OFF CACHE BOOL "Build examples")
set(protobuf_MSVC_STATIC_RUNTIME OFF CACHE BOOL "MSVC static runtime")
add_subdirectory(${CMAKE_SOURCE_DIR}/../vendor/protobuf ${CMAKE_BINARY_DIR}/protobuf EXCLUDE_FROM_ALL)

# Include directories
include_directories(
    ${CMAKE_SOURCE_DIR}/include
    ${ARROW_INCLUDE_DIR}
    ${CMAKE_SOURCE_DIR}/../vendor/librdkafka/src-cpp
    ${CMAKE_SOURCE_DIR}/../vendor/librdkafka/src
    ${CMAKE_SOURCE_DIR}/../vendor/simdjson/include
    ${CMAKE_SOURCE_DIR}/../vendor/avro/lang/c++/include
    ${CMAKE_SOURCE_DIR}/../vendor/protobuf/src
    ${CMAKE_SOURCE_DIR}/../vendor
)

# SabotSQL library sources
set(SABOT_SQL_SOURCES
    # SQL engine
    src/sql/simple_sabot_sql_bridge.cpp
    src/sql/simple_executor.cpp
    src/sql/c_api.cpp
    src/sql/sabot_operator_translator.cpp
    src/sql/binder_rewrites.cpp
    src/sql/string_operations.cpp
    # Execution
    src/execution/morsel_executor.cpp
    # Operators
    # src/operators/table_scan.cpp  # TODO: Fix API issues
    src/operators/filter.cpp
    src/operators/projection.cpp
    src/operators/aggregate.cpp
    src/operators/join.cpp
    src/operators/sort.cpp
    # src/operators/cte.cpp  # Existing - has issues
    # src/operators/subquery.cpp  # Existing - has issues

    # Streaming connectors
    src/streaming/source_connector.cpp
    src/streaming/kafka_connector.cpp
    src/streaming/schema_registry_client.cpp
    # src/streaming/avro_decoder.cpp  # TODO: Fix avro codec_traits issue
    # src/streaming/protobuf_decoder.cpp  # TODO: Fix string concatenation issue
    src/streaming/watermark_tracker.cpp
    src/streaming/window_operator.cpp
    src/streaming/checkpoint_coordinator.cpp
    src/streaming/dimension_broadcast.cpp
    src/streaming/marbledb_integration.cpp
    src/streaming/morsel_plan_extensions.cpp
    # Phase 3: Sabot integration
    src/streaming/sabot_execution_coordinator.cpp
    src/streaming/checkpoint_barrier_injector.cpp
    src/streaming/streaming_agent_distributor.cpp
    src/streaming/sabot_orchestrator_client.cpp
    src/streaming/error_handler.cpp
    src/streaming/monitoring.cpp
)

# Main library
add_library(sabot_sql SHARED ${SABOT_SQL_SOURCES})

target_link_libraries(sabot_sql
    Arrow::arrow_shared
    ArrowFlight::arrow_flight_shared
    rdkafka++
    simdjson
    avrocpp_s
    libprotobuf
    CURL::libcurl
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

add_executable(test_streaming_operators examples/test_streaming_operators.cpp)
target_link_libraries(test_streaming_operators sabot_sql)

add_executable(test_checkpoint_and_dimension examples/test_checkpoint_and_dimension.cpp)
target_link_libraries(test_checkpoint_and_dimension sabot_sql)

add_executable(test_checkpoint_simple examples/test_checkpoint_simple.cpp)
target_link_libraries(test_checkpoint_simple sabot_sql)

add_executable(test_marbledb_and_plan examples/test_marbledb_and_plan.cpp)
target_link_libraries(test_marbledb_and_plan sabot_sql)

# Phase 3: Sabot integration tests
add_executable(test_sabot_execution_coordinator examples/test_sabot_execution_coordinator.cpp)
target_link_libraries(test_sabot_execution_coordinator sabot_sql)

add_executable(test_checkpoint_barrier_injector examples/test_checkpoint_barrier_injector.cpp)
target_link_libraries(test_checkpoint_barrier_injector sabot_sql)

add_executable(test_streaming_agent_distributor examples/test_streaming_agent_distributor.cpp)
target_link_libraries(test_streaming_agent_distributor sabot_sql)

# Schema Registry integration test
add_executable(test_schema_registry_integration examples/test_schema_registry_integration.cpp)
target_link_libraries(test_schema_registry_integration sabot_sql)

message(STATUS "SabotSQL minimal configuration:")
message(STATUS "  Arrow include: ${ARROW_INCLUDE_DIR}")
message(STATUS "  Build type: ${CMAKE_BUILD_TYPE}")
message(STATUS "  Note: Minimal build for testing renamed architecture")
