cmake_minimum_required(VERSION 3.20)
project(MarbleDB VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Options
option(MARBLE_BUILD_TESTS "Build MarbleDB tests" ON)
option(MARBLE_BUILD_EXAMPLES "Build MarbleDB examples" ON)
option(MARBLE_BUILD_BENCHMARKS "Build MarbleDB benchmarks" OFF)
option(MARBLE_USE_ASAN "Enable AddressSanitizer" OFF)
option(MARBLE_USE_TSAN "Enable ThreadSanitizer" OFF)
option(MARBLE_USE_UBSAN "Enable UndefinedBehaviorSanitizer" OFF)

# Dependencies
find_package(Boost REQUIRED COMPONENTS filesystem system)
find_package(Threads REQUIRED)

# Try to find system-installed Arrow first
find_package(Arrow QUIET)
find_package(Parquet QUIET)

if(NOT Arrow_FOUND OR NOT Parquet_FOUND)
    message(WARNING "Arrow/Parquet not found system-wide. "
                   "Please build Arrow from ../vendor/arrow/cpp first:\n"
                   "  cd ../vendor/arrow/cpp\n"
                   "  mkdir build && cd build\n"
                   "  cmake .. -DARROW_BUILD_STATIC=ON -DARROW_BUILD_SHARED=OFF \\\n"
                   "           -DARROW_PARQUET=ON -DARROW_CPP=ON \\\n"
                   "           -DARROW_BUILD_TESTS=OFF -DARROW_BUILD_EXAMPLES=OFF\n"
                   "  make -j$(nproc)\n"
                   "  sudo make install\n"
                   "Then re-run CMake for MarbleDB.")

    # Create dummy targets to allow CMake to continue
    add_library(Arrow::arrow_static INTERFACE IMPORTED)
    add_library(Parquet::parquet_static INTERFACE IMPORTED)
else()
    message(STATUS "Found system-installed Arrow ${Arrow_VERSION} and Parquet")
endif()

# Sanitizers
if(MARBLE_USE_ASAN)
    add_compile_options(-fsanitize=address)
    add_link_options(-fsanitize=address)
endif()

if(MARBLE_USE_TSAN)
    add_compile_options(-fsanitize=thread)
    add_link_options(-fsanitize=thread)
endif()

if(MARBLE_USE_UBSAN)
    add_compile_options(-fsanitize=undefined)
    add_link_options(-fsanitize=undefined)
endif()

# Compiler warnings
if(MSVC)
    add_compile_options(/W4)
else()
    add_compile_options(-Wall -Wextra -Wpedantic)
endif()

# Include directories
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/include)

# Source files
file(GLOB_RECURSE MARBLE_SOURCES
    "src/**/*.cpp"
    "src/*.cpp"
)

# Explicitly list source files to ensure they're included
set(MARBLE_SOURCES
    ${MARBLE_SOURCES}
    src/util/status.cpp
    src/core/schema.cpp
    src/core/task_scheduler.cpp
    src/core/file_system.cpp
    src/core/stream.cpp
    src/core/memtable.cpp
    src/core/disruptor.cpp
    src/core/compaction.cpp
    src/core/wal.cpp
    src/core/version.cpp
        src/core/transaction.cpp
        src/core/query.cpp
)

# Create library
add_library(marble STATIC ${MARBLE_SOURCES})
target_link_libraries(marble
    PUBLIC
        Arrow::arrow_static
        Parquet::parquet_static
        Boost::filesystem
        Boost::system
        Threads::Threads
)

target_include_directories(marble
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

# Tests
if(MARBLE_BUILD_TESTS)
    enable_testing()

    # Try to find GTest, but don't fail if not found
    find_package(GTest QUIET)
    if(GTest_FOUND)
        file(GLOB_RECURSE TEST_SOURCES
            "tests/**/*.cpp"
            "tests/*.cpp"
        )

        add_executable(marble_tests ${TEST_SOURCES})
        target_link_libraries(marble_tests
            PRIVATE
                marble
                GTest::gtest_main
        )

        include(GoogleTest)
        gtest_discover_tests(marble_tests)
    else()
        message(WARNING "GTest not found - tests will not be built")
    endif()
endif()

# Examples
if(MARBLE_BUILD_EXAMPLES)
    # Handle single-file examples in examples/ directory
    file(GLOB EXAMPLE_SOURCES "examples/*.cpp")
    foreach(EXAMPLE_SRC ${EXAMPLE_SOURCES})
        get_filename_component(EXAMPLE_NAME ${EXAMPLE_SRC} NAME_WE)
        add_executable(${EXAMPLE_NAME} ${EXAMPLE_SRC})
        target_link_libraries(${EXAMPLE_NAME} PRIVATE marble)
    endforeach()

    # Handle examples in subdirectories
    file(GLOB EXAMPLE_DIRS "examples/*")
    foreach(EXAMPLE_DIR ${EXAMPLE_DIRS})
        if(IS_DIRECTORY ${EXAMPLE_DIR})
            get_filename_component(EXAMPLE_NAME ${EXAMPLE_DIR} NAME)
            file(GLOB EXAMPLE_DIR_SOURCES "${EXAMPLE_DIR}/*.cpp")

            if(EXAMPLE_DIR_SOURCES)
                add_executable(${EXAMPLE_NAME} ${EXAMPLE_DIR_SOURCES})
                target_link_libraries(${EXAMPLE_NAME} PRIVATE marble)
            endif()
        endif()
    endforeach()
endif()

# Benchmarks
if(MARBLE_BUILD_BENCHMARKS)
    # Benchmarks are under development
    message(STATUS "Benchmarks are currently under development - skipping")
    # add_subdirectory(benchmarks)
endif()

# Installation
install(TARGETS marble
    EXPORT MarbleDBTargets
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    RUNTIME DESTINATION bin
    INCLUDES DESTINATION include
)

install(DIRECTORY include/marble
    DESTINATION include
    FILES_MATCHING PATTERN "*.h"
)

install(EXPORT MarbleDBTargets
    FILE MarbleDBTargets.cmake
    NAMESPACE MarbleDB::
    DESTINATION lib/cmake/MarbleDB
)

# Generate and install config files
include(CMakePackageConfigHelpers)
write_basic_package_version_file(
    MarbleDBConfigVersion.cmake
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion
)

configure_package_config_file(
    cmake/MarbleDBConfig.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/MarbleDBConfig.cmake
    INSTALL_DESTINATION lib/cmake/MarbleDB
)

install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/MarbleDBConfig.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/MarbleDBConfigVersion.cmake
    DESTINATION lib/cmake/MarbleDB
)
