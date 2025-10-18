cmake_minimum_required(VERSION 3.20)
project(sabot_core VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Options
option(SABOT_CORE_BUILD_TESTS "Build sabot_core tests" ON)
option(SABOT_CORE_BUILD_PYTHON "Build Python bindings" ON)

# Find Arrow
find_package(Arrow REQUIRED)

# Include directories
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)

# Source files
set(SABOT_CORE_SOURCES
    src/query/optimizer.cpp
    src/query/logical_plan.cpp
    src/query/rules.cpp
)

# Create static library
add_library(sabot_core STATIC ${SABOT_CORE_SOURCES})

target_link_libraries(sabot_core
    PUBLIC
        Arrow::arrow_static
)

target_include_directories(sabot_core
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

# Compiler options for performance
if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(sabot_core PRIVATE
        -O3
        -march=native
        -ffast-math
        -Wall
        -Wextra
    )
endif()

# Python bindings (Cython)
if(SABOT_CORE_BUILD_PYTHON)
    # Python bindings will be built via setup.py
    # Just ensure library is available
endif()

# Tests
if(SABOT_CORE_BUILD_TESTS)
    enable_testing()
    # Add tests here
endif()

# Installation
install(TARGETS sabot_core
    EXPORT sabot_core_targets
    ARCHIVE DESTINATION lib
    LIBRARY DESTINATION lib
    INCLUDES DESTINATION include
)

install(DIRECTORY include/sabot
    DESTINATION include
    FILES_MATCHING PATTERN "*.h"
)

