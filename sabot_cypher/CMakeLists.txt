cmake_minimum_required(VERSION 3.20)
project(SabotCypher VERSION 0.1.0 LANGUAGES CXX)

# C++20 required
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Find Arrow
find_package(Arrow REQUIRED PATHS ${CMAKE_SOURCE_DIR}/../vendor/arrow/cpp/build/install NO_DEFAULT_PATH)
find_package(ArrowAcero QUIET PATHS ${CMAKE_SOURCE_DIR}/../vendor/arrow/cpp/build/install NO_DEFAULT_PATH)

# Build Kuzu frontend first (parser, binder, planner, optimizer)
# Note: Using the original CMakeLists to avoid breaking dependencies
# We'll link against the built Kuzu library
set(BUILD_KUZU_FRONTEND OFF CACHE BOOL "Build Kuzu frontend library")

if(BUILD_KUZU_FRONTEND)
    message(STATUS "Building Kuzu frontend from vendored source...")
    # This would add the Kuzu build
    # For now, we'll proceed without it and use forward declarations
    message(STATUS "Kuzu frontend build deferred - using forward declarations for now")
endif()

# Include directories
include_directories(
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/vendored/sabot_cypher_core/src/include
    ${CMAKE_SOURCE_DIR}/vendored/sabot_cypher_core/third_party
    ${ARROW_INCLUDE_DIR}
    ${CMAKE_SOURCE_DIR}/../sabot/_cython/graph/query
    ${CMAKE_SOURCE_DIR}/../vendor/arrow/cpp/src
)

# Sabot translator sources
set(SABOT_CYPHER_SOURCES
    src/cypher/sabot_cypher_bridge.cpp
    src/cypher/logical_plan_translator.cpp
    src/cypher/expression_evaluator.cpp
    src/execution/arrow_executor.cpp
    src/streaming/temporal_graph_store.cpp
    src/streaming/streaming_processor.cpp
)

# Main library
add_library(sabot_cypher SHARED ${SABOT_CYPHER_SOURCES})

target_link_libraries(sabot_cypher
    Arrow::arrow_shared
    ${CMAKE_SOURCE_DIR}/../vendor/arrow/cpp/build/install/lib/libarrow_compute.dylib
    # Note: kuzu_frontend will be added here once built
)

target_include_directories(sabot_cypher PUBLIC
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/vendored/sabot_cypher_core/src/include
    ${ARROW_INCLUDE_DIR}
)

# Note: For now, we're building WITHOUT the Kuzu frontend
# This is a skeleton to verify structure compiles
# Next step: integrate Kuzu parser/binder/optimizer as static lib

# Python extension module
find_package(Python3 COMPONENTS Interpreter Development)
if(Python3_FOUND)
    message(STATUS "Python3 found: ${Python3_VERSION}")
    message(STATUS "Python3 include: ${Python3_INCLUDE_DIRS}")
    
    # Build pybind11 module
    add_library(sabot_cypher_native MODULE bindings/python/pybind_module.cpp)
    
    target_link_libraries(sabot_cypher_native PRIVATE
        sabot_cypher
        Python3::Python
        Arrow::arrow_shared
        ${CMAKE_SOURCE_DIR}/../vendor/arrow/cpp/build/install/lib/libarrow_compute.dylib
    )
    
    target_include_directories(sabot_cypher_native PRIVATE
        ${CMAKE_SOURCE_DIR}/vendored/sabot_cypher_core/third_party/pybind11/include
        ${Python3_INCLUDE_DIRS}
        ${CMAKE_SOURCE_DIR}/../vendor/arrow/python/pyarrow/src
        ${ARROW_INCLUDE_DIR}
    )
    
    # Set output name and location
    set_target_properties(sabot_cypher_native PROPERTIES
        PREFIX ""
        OUTPUT_NAME "sabot_cypher_native"
        SUFFIX ".so"
    )
    
    # Copy to sabot_cypher directory for import
    add_custom_command(TARGET sabot_cypher_native POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy
        $<TARGET_FILE:sabot_cypher_native>
        ${CMAKE_SOURCE_DIR}/sabot_cypher_native.so
    )
    
    message(STATUS "Python module sabot_cypher_native will be built")
else()
    message(STATUS "Python3 not found - skipping Python module build")
endif()

# Test executables
add_executable(test_api test_api.cpp)
target_link_libraries(test_api sabot_cypher)

add_executable(test_filter test_filter.cpp)
target_link_libraries(test_filter sabot_cypher)

add_executable(test_operators test_operators.cpp)
target_link_libraries(test_operators sabot_cypher)

add_executable(test_streaming test_streaming.cpp)
target_link_libraries(test_streaming sabot_cypher)

# Installation
install(TARGETS sabot_cypher
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
)

install(DIRECTORY include/ DESTINATION include)

message(STATUS "SabotCypher configuration:")
message(STATUS "  Arrow include: ${ARROW_INCLUDE_DIR}")
message(STATUS "  Build type: ${CMAKE_BUILD_TYPE}")
message(STATUS "  Note: Skeleton build - Kuzu frontend integration pending")

