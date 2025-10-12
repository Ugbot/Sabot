cmake_minimum_required(VERSION 3.20)
project(SabotQL VERSION 0.1.0 LANGUAGES CXX)

# C++20 required for modern features
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Find dependencies
find_package(Arrow REQUIRED PATHS ${CMAKE_SOURCE_DIR}/../vendor/arrow/cpp/build/install NO_DEFAULT_PATH)
find_package(ArrowFlight REQUIRED PATHS ${CMAKE_SOURCE_DIR}/../vendor/arrow/cpp/build/install NO_DEFAULT_PATH)

# Find abseil (used by QLever for high-performance hash maps)
find_package(absl REQUIRED PATHS ${CMAKE_SOURCE_DIR}/../vendor/qlever/build/_deps/abseil-cpp-build NO_DEFAULT_PATH)

# MarbleDB is in ../MarbleDB
set(MARBLE_INCLUDE_DIR ${CMAKE_SOURCE_DIR}/../MarbleDB/include)

# QLever utilities (we use their hash map and LRU cache)
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

    # Parser
    src/parser/ntriples_parser.cpp

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
    src/sparql/planner.cpp
    src/sparql/expression_evaluator.cpp
    src/sparql/query_engine.cpp
)

# Main library (when we have sources)
# add_library(sabot_ql SHARED ${SABOT_QL_SOURCES})
# target_link_libraries(sabot_ql
#     Arrow::arrow_shared
#     marble_db
# )

# Installation
# install(TARGETS sabot_ql
#     LIBRARY DESTINATION lib
#     ARCHIVE DESTINATION lib
#     RUNTIME DESTINATION bin
# )

# install(DIRECTORY include/sabot_ql
#     DESTINATION include
#     FILES_MATCHING PATTERN "*.h"
# )

# Tests (to be added)
# enable_testing()
# add_subdirectory(tests)

message(STATUS "SabotQL configuration:")
message(STATUS "  Arrow include: ${ARROW_INCLUDE_DIR}")
message(STATUS "  MarbleDB include: ${MARBLE_INCLUDE_DIR}")
message(STATUS "  Build type: ${CMAKE_BUILD_TYPE}")
