#==============================================================================
# MarbleDB Test Suite CMake Configuration
#==============================================================================

# Find required packages
find_package(GTest REQUIRED)
find_package(Arrow REQUIRED)
find_package(Threads REQUIRED)

# Include directories
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../vendor/arrow/cpp/src)
include_directories(${ARROW_INCLUDE_DIR})

# Test options (similar to RocksDB)
option(WITH_ALL_TESTS "Build all tests, rather than a small subset" ON)
option(WITH_TEST_UTILS "Build test utilities" ON)

# Test harness library (similar to RocksDB's testharness)
add_library(testharness
    test_util/testharness.cpp
)
target_link_libraries(testharness
    GTest::GTest
)

# Database test utilities library
add_library(db_test_util
    test_util/db_test_util.cpp
)
target_link_libraries(db_test_util
    testharness
    marble_static
    GTest::GTest
    Arrow::arrow_shared
    Threads::Threads
)

# Legacy test utilities (for backward compatibility)
add_library(test_utils
    test_utils.cpp
)
target_link_libraries(test_utils
    marble_static
    GTest::GTest
    GTest::Main
    Arrow::arrow_shared
    Threads::Threads
)

#==============================================================================
# Unit Tests (now built automatically below)
#==============================================================================

#==============================================================================
# Test Lists (similar to RocksDB)
#==============================================================================

# Tests that actually test REAL MarbleDB (not mocks)
set(BASIC_TESTS
    unit/test_status.cpp
    unit/test_record_system.cpp
    unit/test_record_operations.cpp
    unit/test_lsm_storage.cpp
    unit/test_pushdown.cpp
    integration/test_mvcc_comprehensive.cpp
    integration/db_test.cc
    integration/db_compaction_test.cc
    integration/crash_recovery_test.cc
    stress/db_stress_test.cc
    stress/memory_stress_test.cc
    stress/io_stress_test.cc
    fuzz/db_fuzz_test.cc
    performance/microbench_test.cc
)

# All tests (when WITH_ALL_TESTS is enabled)
if(WITH_ALL_TESTS)
    list(APPEND BASIC_TESTS
        integration/test_query_execution.cpp
        integration/test_marble_core.cpp
        integration/test_arctic_bitemporal.cpp
        # Add more tests here as they are created
    )
endif()

#==============================================================================
# Build Tests
#==============================================================================

# Build all tests automatically
foreach(sourcefile ${BASIC_TESTS})
    get_filename_component(exename ${sourcefile} NAME_WE)
    add_executable(${exename} ${sourcefile})

    # Link appropriate libraries based on test type
    if(${sourcefile} MATCHES "integration/")
        target_link_libraries(${exename} db_test_util marble_static
                            GTest::GTest GTest::Main Arrow::arrow_shared Threads::Threads)
    else()
        target_link_libraries(${exename} marble_static GTest::GTest GTest::Main Arrow::arrow_shared)
    endif()

    # Set test properties
    set_target_properties(${exename}
        PROPERTIES EXCLUDE_FROM_DEFAULT_BUILD_RELEASE 1
        EXCLUDE_FROM_DEFAULT_BUILD_MINRELEASE 1
        EXCLUDE_FROM_DEFAULT_BUILD_RELWITHDEBINFO 1
    )
endforeach(sourcefile ${BASIC_TESTS})

#==============================================================================
# Performance Tests & Benchmarks
#==============================================================================

# Find benchmark library (optional)
find_package(benchmark QUIET)

add_executable(test_pushdown_performance performance/test_pushdown_performance.cpp)
target_link_libraries(test_pushdown_performance marble_static GTest::GTest GTest::Main Arrow::arrow_shared test_utils)

if(benchmark_FOUND)
    add_executable(db_performance_benchmark ../benchmarks/db_performance.cpp)
    target_link_libraries(db_performance_benchmark marble_static benchmark::benchmark Arrow::arrow_shared)
    target_include_directories(db_performance_benchmark PRIVATE ../include)

    # Add benchmark to performance tests
    add_custom_target(run_performance_tests
        COMMAND test_pushdown_performance
        COMMAND db_performance_benchmark
        DEPENDS test_pushdown_performance db_performance_benchmark
        COMMENT "Running performance tests and benchmarks"
    )

    # Register benchmark
    add_test(NAME DBPerformanceBenchmark COMMAND db_performance_benchmark)
    set_property(TEST DBPerformanceBenchmark PROPERTY LABELS "performance")
else()
    message(STATUS "Google Benchmark library not found - performance benchmarks disabled")
    add_custom_target(run_performance_tests
        COMMAND test_pushdown_performance
        DEPENDS test_pushdown_performance
        COMMENT "Running performance tests only"
    )
endif()

#==============================================================================
# Test Suite Organization
#==============================================================================

# Create a custom target to run all tests
set(ALL_TEST_TARGETS)
foreach(sourcefile ${BASIC_TESTS})
    get_filename_component(exename ${sourcefile} NAME_WE)
    list(APPEND ALL_TEST_TARGETS ${exename})
endforeach()

add_custom_target(run_all_tests
    COMMAND ${ALL_TEST_TARGETS}
    DEPENDS ${ALL_TEST_TARGETS}
    COMMENT "Running all MarbleDB tests"
)

# Create a custom target to run only unit tests
set(UNIT_TEST_TARGETS)
foreach(sourcefile ${BASIC_TESTS})
    if(${sourcefile} MATCHES "unit/")
        get_filename_component(exename ${sourcefile} NAME_WE)
        list(APPEND UNIT_TEST_TARGETS ${exename})
    endif()
endforeach()

add_custom_target(run_unit_tests
    COMMAND ${UNIT_TEST_TARGETS}
    DEPENDS ${UNIT_TEST_TARGETS}
    COMMENT "Running unit tests only"
)

# Create a custom target to run only integration tests
set(INTEGRATION_TEST_TARGETS)
foreach(sourcefile ${BASIC_TESTS})
    if(${sourcefile} MATCHES "integration/")
        get_filename_component(exename ${sourcefile} NAME_WE)
        list(APPEND INTEGRATION_TEST_TARGETS ${exename})
    endif()
endforeach()

add_custom_target(run_integration_tests
    COMMAND ${INTEGRATION_TEST_TARGETS}
    DEPENDS ${INTEGRATION_TEST_TARGETS}
    COMMENT "Running integration tests only"
)

#==============================================================================
# Test Configuration
#==============================================================================

# Enable testing
enable_testing()

# Register tests with CTest
add_test(NAME StatusTest COMMAND test_status)
add_test(NAME RecordSystemTest COMMAND test_record_system)
add_test(NAME RecordOperationsTest COMMAND test_record_operations)
add_test(NAME LSMStorageTest COMMAND test_lsm_storage)
add_test(NAME PushdownTest COMMAND test_pushdown)
add_test(NAME QueryExecutionTest COMMAND test_query_execution)
add_test(NAME MarbleCoreTest COMMAND test_marble_core)
add_test(NAME ArcticBitemporalTest COMMAND test_arctic_bitemporal)
add_test(NAME PushdownPerformanceTest COMMAND test_pushdown_performance)

# Set test properties
set_tests_properties(
    StatusTest
    RecordSystemTest
    RecordOperationsTest
    LSMStorageTest
    PushdownTest
    QueryExecutionTest
    MarbleCoreTest
    ArcticBitemporalTest
    PushdownPerformanceTest
    PROPERTIES
    ENVIRONMENT "ARROW_DEFAULT_MEMORY_POOL=system"
)

# Add test labels for filtering
set_property(TEST StatusTest PROPERTY LABELS "unit")
set_property(TEST RecordSystemTest PROPERTY LABELS "unit")
set_property(TEST RecordOperationsTest PROPERTY LABELS "unit")
set_property(TEST LSMStorageTest PROPERTY LABELS "unit")
set_property(TEST PushdownTest PROPERTY LABELS "unit")
set_property(TEST QueryExecutionTest PROPERTY LABELS "integration")
set_property(TEST MarbleCoreTest PROPERTY LABELS "integration")
set_property(TEST ArcticBitemporalTest PROPERTY LABELS "integration")
set_property(TEST PushdownPerformanceTest PROPERTY LABELS "performance")

#==============================================================================
# Test Coverage and Reporting
#==============================================================================

# Option to enable code coverage
option(ENABLE_COVERAGE "Enable code coverage reporting" OFF)

if(ENABLE_COVERAGE)
    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O0 --coverage")
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} --coverage")

        # Create coverage target
        add_custom_target(coverage
            COMMAND lcov --capture --directory . --output-file coverage.info
            COMMAND lcov --remove coverage.info '/usr/*' '*/vendor/*' '*/test*' --output-file coverage.info
            COMMAND genhtml coverage.info --output-directory coverage_report
            WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
            COMMENT "Generating code coverage report"
        )
    endif()
endif()

#==============================================================================
# Test Documentation
#==============================================================================

# Generate test documentation
find_package(Doxygen)

if(DOXYGEN_FOUND)
    set(DOXYGEN_INPUT_DIR ${CMAKE_CURRENT_SOURCE_DIR})
    set(DOXYGEN_OUTPUT_DIR ${CMAKE_CURRENT_BINARY_DIR}/docs)
    set(DOXYGEN_PROJECT_NAME "MarbleDB Test Suite")

    configure_file(${CMAKE_CURRENT_SOURCE_DIR}/Doxyfile.in
                   ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile @ONLY)

    add_custom_target(test_docs
        COMMAND ${DOXYGEN_EXECUTABLE} ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        COMMENT "Generating test documentation"
    )
endif()

# Print test configuration summary
message(STATUS "MarbleDB Test Configuration:")
message(STATUS "  Unit tests: ${CMAKE_CURRENT_SOURCE_DIR}/unit/")
message(STATUS "  Integration tests: ${CMAKE_CURRENT_SOURCE_DIR}/integration/")
message(STATUS "  Performance tests: ${CMAKE_CURRENT_SOURCE_DIR}/performance/")
message(STATUS "  Code coverage: ${ENABLE_COVERAGE}")
if(DOXYGEN_FOUND)
    message(STATUS "  Test documentation: Enabled")
else()
    message(STATUS "  Test documentation: Disabled (Doxygen not found)")
endif()
