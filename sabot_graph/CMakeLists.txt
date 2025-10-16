cmake_minimum_required(VERSION 3.20)
project(SabotGraph VERSION 0.1.0 LANGUAGES CXX)

# C++20 required (same as sabot_sql)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Find Arrow
find_package(Arrow REQUIRED PATHS ${CMAKE_SOURCE_DIR}/../vendor/arrow/cpp/build/install NO_DEFAULT_PATH)

# MarbleDB paths
set(MARBLE_INCLUDE_DIR ${CMAKE_SOURCE_DIR}/../MarbleDB/include)
set(MARBLE_LIB_DIR ${CMAKE_SOURCE_DIR}/../MarbleDB/build)

# SabotCypher paths
set(SABOT_CYPHER_INCLUDE_DIR ${CMAKE_SOURCE_DIR}/../sabot_cypher/include)
set(SABOT_CYPHER_LIB_DIR ${CMAKE_SOURCE_DIR}/../sabot_cypher/build)

# SabotQL paths
set(SABOT_QL_INCLUDE_DIR ${CMAKE_SOURCE_DIR}/../sabot_ql/include)
set(SABOT_QL_LIB_DIR ${CMAKE_SOURCE_DIR}/../sabot_ql/build)

# Include directories
include_directories(
    ${CMAKE_SOURCE_DIR}/include
    ${MARBLE_INCLUDE_DIR}
    ${SABOT_CYPHER_INCLUDE_DIR}
    ${SABOT_QL_INCLUDE_DIR}
    ${ARROW_INCLUDE_DIR}
)

# SabotGraph sources
set(SABOT_GRAPH_SOURCES
    src/graph/sabot_graph_bridge.cpp
    src/graph/graph_plan_translator.cpp
    src/state/marble_graph_backend.cpp
)

# Main library
add_library(sabot_graph SHARED ${SABOT_GRAPH_SOURCES})

target_link_libraries(sabot_graph
    Arrow::arrow_shared
    ${CMAKE_SOURCE_DIR}/../vendor/arrow/cpp/build/install/lib/libarrow_compute.dylib
    # MarbleDB will be linked when available
    # ${MARBLE_LIB_DIR}/libmarble.a
    # SabotCypher will be linked when available
    # ${SABOT_CYPHER_LIB_DIR}/libsabot_cypher.dylib
    # SabotQL will be linked when available
    # ${SABOT_QL_LIB_DIR}/libsabot_ql.dylib
)

target_include_directories(sabot_graph PUBLIC
    ${CMAKE_SOURCE_DIR}/include
    ${MARBLE_INCLUDE_DIR}
    ${SABOT_CYPHER_INCLUDE_DIR}
    ${SABOT_QL_INCLUDE_DIR}
    ${ARROW_INCLUDE_DIR}
)

# Test executables
add_executable(test_sabot_graph_bridge test_sabot_graph_bridge.cpp)
target_link_libraries(test_sabot_graph_bridge sabot_graph)

# Installation
install(TARGETS sabot_graph
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
)

install(DIRECTORY include/ DESTINATION include)

message(STATUS "SabotGraph configuration:")
message(STATUS "  Arrow include: ${ARROW_INCLUDE_DIR}")
message(STATUS "  MarbleDB include: ${MARBLE_INCLUDE_DIR}")
message(STATUS "  SabotCypher include: ${SABOT_CYPHER_INCLUDE_DIR}")
message(STATUS "  SabotQL include: ${SABOT_QL_INCLUDE_DIR}")
message(STATUS "  Build type: ${CMAKE_BUILD_TYPE}")

