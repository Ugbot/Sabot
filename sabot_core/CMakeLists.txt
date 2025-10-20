cmake_minimum_required(VERSION 3.16)
project(sabot_core VERSION 0.1.0 LANGUAGES CXX)

# C++17 standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find Arrow
find_package(Arrow QUIET)
if(NOT Arrow_FOUND)
    # Try to use vendored Arrow
    set(ARROW_INCLUDE_DIR "${CMAKE_SOURCE_DIR}/../vendor/arrow/cpp/src")
    message(STATUS "Using vendored Arrow at ${ARROW_INCLUDE_DIR}")
endif()

# Include directories
include_directories(
    ${CMAKE_SOURCE_DIR}/include
    ${ARROW_INCLUDE_DIR}
)

# Build query optimizer library
add_subdirectory(src/query)

# Build operator registry
add_subdirectory(src/operators)

# Build shuffle coordinator
add_subdirectory(src/shuffle)

# Installation
install(DIRECTORY include/sabot DESTINATION include)
