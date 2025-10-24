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

# Test utilities library
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
# Unit Tests
#==============================================================================

add_executable(test_status unit/test_status.cpp)
target_link_libraries(test_status marble_static GTest::GTest GTest::Main)

add_executable(test_record_system unit/test_record_system.cpp)
target_link_libraries(test_record_system marble_static GTest::GTest GTest::Main Arrow::arrow_shared)

add_executable(test_pushdown unit/test_pushdown.cpp)
target_link_libraries(test_pushdown marble_static GTest::GTest GTest::Main Arrow::arrow_shared)

#==============================================================================
# Integration Tests
#==============================================================================

add_executable(test_query_execution integration/test_query_execution.cpp)
target_link_libraries(test_query_execution marble_static GTest::GTest GTest::Main Arrow::arrow_shared test_utils)

add_executable(test_marble_core integration/test_marble_core.cpp)
target_link_libraries(test_marble_core marble_static GTest::GTest GTest::Main Arrow::arrow_shared)

add_executable(test_arctic_bitemporal integration/test_arctic_bitemporal.cpp)
target_link_libraries(test_arctic_bitemporal marble_static GTest::GTest GTest::Main Arrow::arrow_shared)

#==============================================================================
# Performance Tests
#==============================================================================

add_executable(test_pushdown_performance performance/test_pushdown_performance.cpp)
target_link_libraries(test_pushdown_performance marble_static GTest::GTest GTest::Main Arrow::arrow_shared test_utils)

#==============================================================================
# Test Suite Organization
#==============================================================================

# Create a custom target to run all tests
add_custom_target(run_all_tests
    COMMAND test_status
    COMMAND test_record_system
    COMMAND test_pushdown
    COMMAND test_query_execution
    COMMAND test_marble_core
    COMMAND test_arctic_bitemporal
    COMMAND test_pushdown_performance
    DEPENDS
        test_status
        test_record_system
        test_pushdown
        test_query_execution
        test_marble_core
        test_arctic_bitemporal
        test_pushdown_performance
    COMMENT "Running all MarbleDB tests"
)

# Create a custom target to run only unit tests
add_custom_target(run_unit_tests
    COMMAND test_status
    COMMAND test_record_system
    COMMAND test_pushdown
    DEPENDS
        test_status
        test_record_system
        test_pushdown
    COMMENT "Running unit tests only"
)

# Create a custom target to run only integration tests
add_custom_target(run_integration_tests
    COMMAND test_query_execution
    DEPENDS test_query_execution
    COMMENT "Running integration tests only"
)

# Create a custom target to run only performance tests
add_custom_target(run_performance_tests
    COMMAND test_pushdown_performance
    DEPENDS test_pushdown_performance
    COMMENT "Running performance tests only"
)

#==============================================================================
# Test Configuration
#==============================================================================

# Enable testing
enable_testing()

# Register tests with CTest
add_test(NAME StatusTest COMMAND test_status)
add_test(NAME RecordSystemTest COMMAND test_record_system)
add_test(NAME PushdownTest COMMAND test_pushdown)
add_test(NAME QueryExecutionTest COMMAND test_query_execution)
add_test(NAME MarbleCoreTest COMMAND test_marble_core)
add_test(NAME ArcticBitemporalTest COMMAND test_arctic_bitemporal)
add_test(NAME PushdownPerformanceTest COMMAND test_pushdown_performance)

# Set test properties
set_tests_properties(
    StatusTest
    RecordSystemTest
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
