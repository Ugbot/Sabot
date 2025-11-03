cmake_minimum_required(VERSION 3.20)
project(MarbleDB VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Options
option(MARBLE_BUILD_TESTS "Build MarbleDB tests" ON)
option(MARBLE_BUILD_EXAMPLES "Build MarbleDB examples" ON)
option(MARBLE_BUILD_BENCHMARKS "Build MarbleDB benchmarks" OFF)
option(MARBLE_BUILD_SERVER "Build MarbleDB server binary" ON)
option(MARBLE_BUILD_SHARED "Build shared library" OFF)
option(MARBLE_BUILD_STATIC "Build static library" ON)
option(MARBLE_ENABLE_RAFT "Enable Raft consensus support" OFF)  # Disabled - not needed for Arrow Batch API
option(MARBLE_USE_ASAN "Enable AddressSanitizer" OFF)
option(MARBLE_USE_TSAN "Enable ThreadSanitizer" OFF)
option(MARBLE_USE_UBSAN "Enable UndefinedBehaviorSanitizer" OFF)

# Dependencies
find_package(Boost REQUIRED COMPONENTS filesystem system coroutine)
find_package(Threads REQUIRED)

# Use ONLY vendored Arrow (never system Arrow per project requirements)
set(ARROW_VENDORED_PATH ${CMAKE_CURRENT_SOURCE_DIR}/../vendor/arrow/cpp/build/install)
find_package(Arrow REQUIRED PATHS ${ARROW_VENDORED_PATH}/lib/cmake/Arrow NO_DEFAULT_PATH)
find_package(Parquet REQUIRED PATHS ${ARROW_VENDORED_PATH}/lib/cmake/Parquet NO_DEFAULT_PATH)

# Find Arrow Flight
find_package(ArrowFlight QUIET)
if(NOT ArrowFlight_FOUND)
    # Try pkg-config for Arrow Flight
    find_package(PkgConfig QUIET)
    if(PKG_CONFIG_FOUND)
        pkg_check_modules(ARROW_FLIGHT arrow-flight)
        if(ARROW_FLIGHT_FOUND)
            set(ArrowFlight_FOUND TRUE)
        endif()
    endif()

    # Try to find Arrow Flight as part of Arrow
    if(NOT ArrowFlight_FOUND AND Arrow_FOUND)
        get_target_property(ARROW_FLIGHT_LIB ArrowFlight::arrow_flight_shared LOCATION)
        if(ARROW_FLIGHT_LIB)
            set(ArrowFlight_FOUND TRUE)
        endif()
    endif()
endif()

# Find nlohmann/json for JSON parsing
find_package(nlohmann_json QUIET)
if(NOT nlohmann_json_FOUND)
    # Try pkg-config as fallback
    find_package(PkgConfig)
    if(PKG_CONFIG_FOUND)
        pkg_check_modules(JSON nlohmann_json)
        if(JSON_FOUND)
            set(nlohmann_json_FOUND TRUE)
        endif()
    endif()
endif()

message(STATUS "Using vendored Arrow ${Arrow_VERSION} from ${ARROW_VENDORED_PATH}")

# NuRaft for Raft consensus
if(MARBLE_ENABLE_RAFT)
    set(NURAFT_DIR ${CMAKE_CURRENT_SOURCE_DIR}/vendor/nuraft)
    if(EXISTS ${NURAFT_DIR})
        message(STATUS "Found NuRaft at ${NURAFT_DIR}")

        # Build NuRaft
        set(NURAFT_BUILD_DIR ${CMAKE_CURRENT_BINARY_DIR}/nuraft-build)
        file(MAKE_DIRECTORY ${NURAFT_BUILD_DIR})

        # Configure NuRaft
        execute_process(
            COMMAND ${CMAKE_COMMAND}
                -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
                -DCMAKE_INSTALL_PREFIX=${CMAKE_CURRENT_BINARY_DIR}/nuraft-install
                -DBUILD_TESTING=OFF
                -DCMAKE_PREFIX_PATH=${NURAFT_DIR}
                -S ${NURAFT_DIR}
                -B ${NURAFT_BUILD_DIR}
            RESULT_VARIABLE NURAFT_CONFIG_RESULT
        )

        if(NURAFT_CONFIG_RESULT EQUAL 0)
            # Build NuRaft
            execute_process(
                COMMAND ${CMAKE_COMMAND} --build ${NURAFT_BUILD_DIR} --parallel
                RESULT_VARIABLE NURAFT_BUILD_RESULT
            )

            if(NURAFT_BUILD_RESULT EQUAL 0)
                # Install NuRaft
                execute_process(
                    COMMAND ${CMAKE_COMMAND} --install ${NURAFT_BUILD_DIR}
                    RESULT_VARIABLE NURAFT_INSTALL_RESULT
                )

                if(NURAFT_INSTALL_RESULT EQUAL 0)
                    # Find the built NuRaft
                    find_library(NURAFT_LIBRARY
                        NAMES nuraft
                        PATHS ${CMAKE_CURRENT_BINARY_DIR}/nuraft-install/lib
                        NO_DEFAULT_PATH
                    )

                    if(NURAFT_LIBRARY)
                        message(STATUS "Successfully built and found NuRaft: ${NURAFT_LIBRARY}")
                        set(NURAFT_FOUND TRUE)
                        set(NURAFT_INCLUDE_DIR ${CMAKE_CURRENT_BINARY_DIR}/nuraft-install/include)
                    else()
                        message(WARNING "NuRaft library not found after build")
                    endif()
                else()
                    message(WARNING "Failed to install NuRaft")
                endif()
            else()
                message(WARNING "Failed to build NuRaft")
            endif()
        else()
            message(WARNING "Failed to configure NuRaft")
        endif()
    else()
        message(WARNING "NuRaft not found at ${NURAFT_DIR}. Please ensure the submodule is initialized.")
    endif()
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

# Add NuRaft includes if enabled
if(MARBLE_ENABLE_RAFT AND NURAFT_FOUND)
    include_directories(${NURAFT_INCLUDE_DIR})
endif()

# Source files
file(GLOB_RECURSE MARBLE_SOURCES
    "src/**/*.cpp"
    "src/*.cpp"
)

# Remove files that should not be included
list(REMOVE_ITEM MARBLE_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/src/core/flight_service.cpp")

# Remove Raft sources if not enabled
if(NOT MARBLE_ENABLE_RAFT)
    file(GLOB_RECURSE RAFT_SOURCES "src/raft/*.cpp")
    foreach(raft_file ${RAFT_SOURCES})
        list(REMOVE_ITEM MARBLE_SOURCES ${raft_file})
    endforeach()
endif()

# Remove arrow_flight_transport.cpp (requires Arrow Flight module not available)
list(REMOVE_ITEM MARBLE_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/src/raft/arrow_flight_transport.cpp")

# Remove async.cpp (has compilation issues and is not needed for basic functionality)
list(REMOVE_ITEM MARBLE_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/src/core/async.cpp")

# Remove query.cpp files (incomplete implementations with compilation errors)
list(REMOVE_ITEM MARBLE_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/src/core/query.cpp")
list(REMOVE_ITEM MARBLE_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/src/query/query.cpp")

# Re-enabled mvcc_transaction.cpp - contains CreateMVCCTransaction needed for benchmarks
# list(REMOVE_ITEM MARBLE_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/src/core/mvcc_transaction.cpp")

# Re-enabled record_ref.cpp - fixed SimpleRecordRef error (now uses ArrowRecordRef)
# list(REMOVE_ITEM MARBLE_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/src/core/record_ref.cpp")

# Remove schema.cpp (has conflicting SimpleRecord definition - demo/test file)
list(REMOVE_ITEM MARBLE_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/src/core/schema.cpp")

# Remove transaction.cpp (has redefinition and abstract class issues)
list(REMOVE_ITEM MARBLE_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/src/core/transaction.cpp")

# Explicitly list source files to ensure they're included
set(MARBLE_SOURCES
    ${MARBLE_SOURCES}
    src/util/status.cpp
    # src/core/schema.cpp  # Excluded - has conflicting SimpleRecord definition
    src/core/metrics.cpp
    src/core/advanced_features.cpp
    src/core/task_scheduler.cpp
    src/core/file_system.cpp
    src/core/memtable.cpp
    src/core/memtable_new.cpp
    src/core/sstable.cpp
    src/core/lsm_storage.cpp
    src/core/disruptor.cpp
    src/core/compaction.cpp
    src/core/wal.cpp
    src/core/version.cpp
    # src/core/transaction.cpp  # Excluded - redefinition and abstract class issues
    src/core/table.cpp
    src/core/analytics.cpp
    src/core/skipping_index.cpp
    src/core/typed_record.cpp
    src/core/sstable_arrow.cpp
    src/core/api.cpp
    src/core/arrow_serialization.cpp
    src/core/c_api.cpp
    src/core/temporal.cpp
    src/core/temporal_reconstruction.cpp
    src/core/advanced_query.cpp
    src/core/execution_engine.cpp
    src/core/block_optimizations.cpp
    src/core/hot_key_cache.cpp
)

# Add Raft sources if enabled
if(MARBLE_ENABLE_RAFT)
    list(APPEND MARBLE_SOURCES
        src/raft/raft_server.cpp
        src/raft/raft_wrappers.cpp
        # src/raft/arrow_flight_transport.cpp  # Disabled: requires Arrow Flight module
        src/raft/marble_wal_state_machine.cpp
        src/raft/marble_db_state_machine.cpp
        src/raft/marble_log_store.cpp
        src/raft/raft_config_manager.cpp
    )
endif()

# Create libraries
if(MARBLE_BUILD_STATIC)
    add_library(marble_static STATIC ${MARBLE_SOURCES})
    set_target_properties(marble_static PROPERTIES
        OUTPUT_NAME marble
        POSITION_INDEPENDENT_CODE ON
    )
    target_link_libraries(marble_static
        PUBLIC
            Arrow::arrow_shared
            Parquet::parquet_shared
            Boost::filesystem
            Boost::system
            Threads::Threads
    )

    # Link NuRaft if enabled
    if(MARBLE_ENABLE_RAFT AND NURAFT_FOUND)
        target_link_libraries(marble_static
            PUBLIC
                ${NURAFT_LIBRARY}
        )
    endif()

    # Link Arrow Flight if available (only when RAFT is enabled)
    if(MARBLE_ENABLE_RAFT)
        if(ArrowFlight_FOUND)
            target_link_libraries(marble_static PUBLIC ArrowFlight::arrow_flight_shared)
            target_compile_definitions(marble_static PUBLIC MARBLE_ENABLE_ARROW_FLIGHT)
        elseif(ARROW_FLIGHT_FOUND)
            target_link_libraries(marble_static PUBLIC ${ARROW_FLIGHT_LIBRARIES})
            target_include_directories(marble_static PUBLIC ${ARROW_FLIGHT_INCLUDE_DIRS})
            target_compile_definitions(marble_static PUBLIC MARBLE_ENABLE_ARROW_FLIGHT)
        elseif(TARGET ArrowFlight::arrow_flight_shared)
            target_link_libraries(marble_static PUBLIC ArrowFlight::arrow_flight_shared)
            target_compile_definitions(marble_static PUBLIC MARBLE_ENABLE_ARROW_FLIGHT)
        endif()
    endif()

    target_include_directories(marble_static
        PUBLIC
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
            $<INSTALL_INTERFACE:include>
    )

    # Alias for backward compatibility
    add_library(marble ALIAS marble_static)
endif()

if(MARBLE_BUILD_SHARED)
    add_library(marble_shared SHARED ${MARBLE_SOURCES})
    set_target_properties(marble_shared PROPERTIES
        OUTPUT_NAME marble
        VERSION ${PROJECT_VERSION}
        SOVERSION ${PROJECT_VERSION_MAJOR}
    )
    target_link_libraries(marble_shared
        PUBLIC
            Arrow::arrow_shared
            Parquet::parquet_shared
            Boost::filesystem
            Boost::system
            Threads::Threads
    )

    # Link NuRaft if enabled
    if(MARBLE_ENABLE_RAFT AND NURAFT_FOUND)
        target_link_libraries(marble_shared
            PUBLIC
                ${NURAFT_LIBRARY}
        )
    endif()

    # Link Arrow Flight if available (only when RAFT is enabled)
    if(MARBLE_ENABLE_RAFT)
        if(ArrowFlight_FOUND)
            target_link_libraries(marble_shared PUBLIC ArrowFlight::arrow_flight_shared)
            target_compile_definitions(marble_shared PUBLIC MARBLE_ENABLE_ARROW_FLIGHT)
        elseif(ARROW_FLIGHT_FOUND)
            target_link_libraries(marble_shared PUBLIC ${ARROW_FLIGHT_LIBRARIES})
            target_include_directories(marble_shared PUBLIC ${ARROW_FLIGHT_INCLUDE_DIRS})
            target_compile_definitions(marble_shared PUBLIC MARBLE_ENABLE_ARROW_FLIGHT)
        elseif(TARGET ArrowFlight::arrow_flight_shared)
            target_link_libraries(marble_shared PUBLIC ArrowFlight::arrow_flight_shared)
            target_compile_definitions(marble_shared PUBLIC MARBLE_ENABLE_ARROW_FLIGHT)
        endif()
    endif()

    target_include_directories(marble_shared
        PUBLIC
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
            $<INSTALL_INTERFACE:include>
    )

    # Alias for convenience
    add_library(marble::shared ALIAS marble_shared)
endif()

# Default library target (for backward compatibility)
if(MARBLE_BUILD_STATIC)
    # Create main marble alias pointing to static library
    if(NOT TARGET marble)
        add_library(marble ALIAS marble_static)
    endif()
elseif(MARBLE_BUILD_SHARED)
    # Create main marble alias pointing to shared library
    if(NOT TARGET marble)
        add_library(marble ALIAS marble_shared)
    endif()
else()
    message(FATAL_ERROR "At least one of MARBLE_BUILD_STATIC or MARBLE_BUILD_SHARED must be ON")
endif()

# Server binary
if(MARBLE_BUILD_SERVER)
    add_executable(marbledb_server src/main.cpp)

    # Link against the library
    if(MARBLE_BUILD_SHARED)
        target_link_libraries(marbledb_server PRIVATE marble_shared)
    else()
        target_link_libraries(marbledb_server PRIVATE marble_static)
    endif()

    # Set output name
    set_target_properties(marbledb_server PROPERTIES
        OUTPUT_NAME marbledb
    )

    # Install server binary
    install(TARGETS marbledb_server
        RUNTIME DESTINATION bin
    )
endif()

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

        # Link against the appropriate library
        if(MARBLE_BUILD_SHARED)
            target_link_libraries(marble_tests PRIVATE marble_shared GTest::gtest_main)
        else()
            target_link_libraries(marble_tests PRIVATE marble_static GTest::gtest_main)
        endif()

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
        # Skip raft_example.cpp as it's not compatible yet
        # Include marble_raft_cluster_example only if Raft is enabled
        if(NOT EXAMPLE_NAME STREQUAL "raft_example")
            if(EXAMPLE_NAME STREQUAL "marble_raft_cluster_example" OR
                EXAMPLE_NAME STREQUAL "marble_raft_full_example" OR
                EXAMPLE_NAME STREQUAL "raft_demo" OR
                EXAMPLE_NAME STREQUAL "arrow_flight_raft_demo")
                if(MARBLE_ENABLE_RAFT)
                    add_executable(${EXAMPLE_NAME} ${EXAMPLE_SRC})

                    # Link against the appropriate library
                    if(MARBLE_BUILD_SHARED)
                        target_link_libraries(${EXAMPLE_NAME} PRIVATE marble_shared)
                    else()
                        target_link_libraries(${EXAMPLE_NAME} PRIVATE marble_static)
                    endif()
                endif()
            else()
                add_executable(${EXAMPLE_NAME} ${EXAMPLE_SRC})

                # Link against the appropriate library
                if(MARBLE_BUILD_SHARED)
                    target_link_libraries(${EXAMPLE_NAME} PRIVATE marble_shared)
                else()
                    target_link_libraries(${EXAMPLE_NAME} PRIVATE marble_static)
                endif()
            endif()
        endif()
    endforeach()

    # Handle examples in subdirectories
    file(GLOB EXAMPLE_DIRS "examples/*")
    foreach(EXAMPLE_DIR ${EXAMPLE_DIRS})
        if(IS_DIRECTORY ${EXAMPLE_DIR})
            get_filename_component(EXAMPLE_NAME ${EXAMPLE_DIR} NAME)
            file(GLOB EXAMPLE_DIR_SOURCES "${EXAMPLE_DIR}/*.cpp")

            if(EXAMPLE_DIR_SOURCES)
                add_executable(${EXAMPLE_NAME} ${EXAMPLE_DIR_SOURCES})

                # Link against the appropriate library
                if(MARBLE_BUILD_SHARED)
                    target_link_libraries(${EXAMPLE_NAME} PRIVATE marble_shared)
                else()
                    target_link_libraries(${EXAMPLE_NAME} PRIVATE marble_static)
                endif()
            endif()
        endif()
    endforeach()
endif()

# Benchmarks
if(MARBLE_BUILD_BENCHMARKS)
    add_subdirectory(benchmarks)
endif()

# Installation
if(MARBLE_BUILD_STATIC)
    install(TARGETS marble_static
        EXPORT MarbleDBTargets
        ARCHIVE DESTINATION lib
        INCLUDES DESTINATION include
    )
endif()

if(MARBLE_BUILD_SHARED)
    install(TARGETS marble_shared
        EXPORT MarbleDBTargets
        LIBRARY DESTINATION lib
        INCLUDES DESTINATION include
    )
endif()

# Install server binary if built
if(MARBLE_BUILD_SERVER)
    install(TARGETS marbledb_server
        RUNTIME DESTINATION bin
    )
endif()

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

add_executable(pushdown_demo examples/demos/pushdown_demo.cpp)
target_link_libraries(pushdown_demo marble_static)

add_executable(c_api_example examples/language_bindings/c_api_example.c)
target_link_libraries(c_api_example marble_c_api)

#==============================================================================
# C API Library
#==============================================================================

# Create shared library for C API
add_library(marble_c_api SHARED
    src/core/c_api.cpp
    src/core/api.cpp
)

target_link_libraries(marble_c_api
    marble_static
    ${CMAKE_THREAD_LIBS_INIT}
)

set_target_properties(marble_c_api PROPERTIES
    VERSION 0.1.0
    SOVERSION 0
    OUTPUT_NAME "marble"
)

# Install C API library and headers
install(TARGETS marble_c_api
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
)

install(FILES include/marble/c_api.h
    DESTINATION include/marble
)

# Add tests subdirectory
add_subdirectory(tests)

install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/MarbleDBConfig.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/MarbleDBConfigVersion.cmake
    DESTINATION lib/cmake/MarbleDB
)
