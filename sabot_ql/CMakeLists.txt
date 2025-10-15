cmake_minimum_required(VERSION 3.20)
project(SabotQL VERSION 0.1.0 LANGUAGES CXX)

# C++20 required for modern features
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Find dependencies
find_package(Arrow REQUIRED PATHS ${CMAKE_SOURCE_DIR}/../vendor/arrow/cpp/build/install NO_DEFAULT_PATH)
find_package(ArrowFlight REQUIRED PATHS ${CMAKE_SOURCE_DIR}/../vendor/arrow/cpp/build/install NO_DEFAULT_PATH)

# MarbleDB is in ../MarbleDB
set(MARBLE_INCLUDE_DIR ${CMAKE_SOURCE_DIR}/../MarbleDB/include)
set(MARBLE_LIB_DIR ${CMAKE_SOURCE_DIR}/../MarbleDB/build)

# QLever is in ../vendor/qlever
set(QLEVER_INCLUDE_DIR ${CMAKE_SOURCE_DIR}/../vendor/qlever/src)

# Include directories
include_directories(
    ${CMAKE_SOURCE_DIR}/include
    ${MARBLE_INCLUDE_DIR}
    ${QLEVER_INCLUDE_DIR}
    ${ARROW_INCLUDE_DIR}
)

# Library sources
set(SABOT_QL_SOURCES
    # Storage layer
    src/storage/triple_store_impl.cpp
    src/storage/vocabulary_impl.cpp

    # Operators
    src/operators/operator.cpp
    src/operators/join.cpp
    src/operators/aggregate.cpp
    src/operators/sort.cpp
    src/operators/union.cpp

    # Execution engine
    src/execution/executor.cpp

    # SPARQL engine
    src/sparql/ast.cpp
    src/sparql/parser.cpp
    src/sparql/planner.cpp
    src/sparql/expression_evaluator.cpp
    src/sparql/query_engine.cpp
)

# Main library
add_library(sabot_ql SHARED ${SABOT_QL_SOURCES})

target_link_libraries(sabot_ql
    Arrow::arrow_shared
    ArrowFlight::arrow_flight_shared
    ${MARBLE_LIB_DIR}/libmarble.a
)

target_include_directories(sabot_ql PUBLIC
    ${CMAKE_SOURCE_DIR}/include
    ${MARBLE_INCLUDE_DIR}
    ${QLEVER_INCLUDE_DIR}
    ${ARROW_INCLUDE_DIR}
)

# Installation
install(TARGETS sabot_ql
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    RUNTIME DESTINATION bin
)

install(DIRECTORY include/sabot_ql
    DESTINATION include
    FILES_MATCHING PATTERN "*.h"
)

# Tests (to be added)
# enable_testing()
# add_subdirectory(tests)

# Python bindings (optional - requires pybind11)
option(BUILD_PYTHON_BINDINGS "Build Python bindings" OFF)
if(BUILD_PYTHON_BINDINGS)
    find_package(pybind11 QUIET)
    if(pybind11_FOUND)
        pybind11_add_module(sabot_ql_native bindings/python/pybind_module.cpp)
        target_link_libraries(sabot_ql_native PRIVATE sabot_ql)
        message(STATUS "Building Python bindings: YES")
    else()
        message(STATUS "pybind11 not found - Python bindings disabled")
    endif()
else()
    message(STATUS "Building Python bindings: NO (use -DBUILD_PYTHON_BINDINGS=ON)")
endif()

message(STATUS "SabotQL configuration:")
message(STATUS "  Arrow include: ${ARROW_INCLUDE_DIR}")
message(STATUS "  MarbleDB include: ${MARBLE_INCLUDE_DIR}")
message(STATUS "  QLever include: ${QLEVER_INCLUDE_DIR}")
message(STATUS "  Build type: ${CMAKE_BUILD_TYPE}")
