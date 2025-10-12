cmake_minimum_required(VERSION 3.20)
project(SabotSQLCore VERSION 0.1.0 LANGUAGES CXX)

# C++20 required for modern features
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Option to ensure the vendored core does NOT compile/link any physical operators
option(SABOT_SQL_DISABLE_VENDORED_PHYSICAL "Disable vendored physical execution operators (planning only)" ON)

# Include directories
include_directories(
    ${CMAKE_SOURCE_DIR}/include
)

# For now, create a minimal interface library
# We'll build the core components later
add_library(sabot_sql_core INTERFACE)

# Compiler definitions
target_compile_definitions(sabot_sql_core INTERFACE
    SABOT_SQL_MAIN_LIBRARY
    SABOT_SQL_BUILD_LIBRARY
)

# Include directories for the interface
target_include_directories(sabot_sql_core INTERFACE
    ${CMAKE_SOURCE_DIR}/include
)

message(STATUS "SabotSQL Core configuration:")
message(STATUS "  Include directory: ${CMAKE_SOURCE_DIR}/include")
message(STATUS "  Build type: ${CMAKE_BUILD_TYPE}")
message(STATUS "  C++ standard: ${CMAKE_CXX_STANDARD}")
message(STATUS "  Note: Core components will be built incrementally")
message(STATUS "  Vendored physical disabled: ${SABOT_SQL_DISABLE_VENDORED_PHYSICAL}")