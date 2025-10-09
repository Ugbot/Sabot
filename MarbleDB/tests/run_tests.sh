#!/bin/bash

# MarbleDB Test Runner Script
# Provides convenient commands for running different test categories

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
BUILD_DIR="build"
TEST_TIMEOUT=300  # 5 minutes timeout for individual tests

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_header() {
    echo -e "${BLUE}================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}================================${NC}"
}

# Check if we're in the right directory
check_directory() {
    if [[ ! -f "CMakeLists.txt" ]]; then
        print_error "Please run this script from the MarbleDB root directory"
        exit 1
    fi
}

# Check if build directory exists
check_build() {
    if [[ ! -d "$BUILD_DIR" ]]; then
        print_error "Build directory '$BUILD_DIR' not found. Please run 'cmake -B $BUILD_DIR' first"
        exit 1
    fi
}

# Run a test with timeout
run_test_with_timeout() {
    local test_name="$1"
    local test_cmd="$2"

    print_status "Running $test_name..."

    # Run with timeout
    if timeout $TEST_TIMEOUT $test_cmd 2>/dev/null; then
        print_status "$test_name passed"
        return 0
    else
        local exit_code=$?
        if [[ $exit_code -eq 124 ]]; then
            print_error "$test_name timed out after ${TEST_TIMEOUT}s"
        else
            print_error "$test_name failed (exit code: $exit_code)"
        fi
        return 1
    fi
}

# Build tests
build_tests() {
    print_header "Building Tests"

    if [[ ! -d "$BUILD_DIR" ]]; then
        print_status "Creating build directory..."
        mkdir -p "$BUILD_DIR"
    fi

    cd "$BUILD_DIR"

    print_status "Running CMake..."
    cmake .. -DCMAKE_BUILD_TYPE=Release

    print_status "Building tests..."
    make -j$(nproc) run_all_tests

    cd ..
    print_status "Tests built successfully"
}

# Run unit tests
run_unit_tests() {
    print_header "Running Unit Tests"

    cd "$BUILD_DIR"

    local failed_tests=0

    # Run each unit test
    run_test_with_timeout "Record System Tests" "./test_record_system" || ((failed_tests++))
    run_test_with_timeout "Pushdown Tests" "./test_pushdown" || ((failed_tests++))

    cd ..

    if [[ $failed_tests -eq 0 ]]; then
        print_status "All unit tests passed!"
    else
        print_error "$failed_tests unit test(s) failed"
        return 1
    fi
}

# Run integration tests
run_integration_tests() {
    print_header "Running Integration Tests"

    cd "$BUILD_DIR"

    local failed_tests=0

    # Run each integration test
    run_test_with_timeout "Query Execution Tests" "./test_query_execution" || ((failed_tests++))

    cd ..

    if [[ $failed_tests -eq 0 ]]; then
        print_status "All integration tests passed!"
    else
        print_error "$failed_tests integration test(s) failed"
        return 1
    fi
}

# Run performance tests
run_performance_tests() {
    print_header "Running Performance Tests"

    cd "$BUILD_DIR"

    local failed_tests=0

    # Run each performance test
    run_test_with_timeout "Pushdown Performance Tests" "./test_pushdown_performance" || ((failed_tests++))

    cd ..

    if [[ $failed_tests -eq 0 ]]; then
        print_status "All performance tests passed!"
    else
        print_error "$failed_tests performance test(s) failed"
        return 1
    fi
}

# Run legacy tests
run_legacy_tests() {
    print_header "Running Legacy Tests"

    cd "$BUILD_DIR"

    local failed_tests=0

    # Run legacy tests
    run_test_with_timeout "Status Tests" "./test_status" || ((failed_tests++))
    run_test_with_timeout "Marble Core Tests" "./test_marble_core" || ((failed_tests++))
    run_test_with_timeout "Arctic Bitemporal Tests" "./test_arctic_bitemporal" || ((failed_tests++))

    cd ..

    if [[ $failed_tests -eq 0 ]]; then
        print_status "All legacy tests passed!"
    else
        print_error "$failed_tests legacy test(s) failed"
        return 1
    fi
}

# Run all tests
run_all_tests() {
    print_header "Running All Tests"

    local total_failed=0

    run_unit_tests || ((total_failed++))
    echo
    run_integration_tests || ((total_failed++))
    echo
    run_performance_tests || ((total_failed++))
    echo
    run_legacy_tests || ((total_failed++))

    echo
    if [[ $total_failed -eq 0 ]]; then
        print_header "🎉 ALL TESTS PASSED!"
        print_status "MarbleDB test suite completed successfully"
    else
        print_header "❌ SOME TESTS FAILED"
        print_error "$total_failed test suite(s) had failures"
        return 1
    fi
}

# Run tests with CTest
run_ctest() {
    print_header "Running Tests with CTest"

    cd "$BUILD_DIR"

    print_status "Running CTest..."
    if ctest --output-on-failure --timeout $TEST_TIMEOUT; then
        print_status "CTest completed successfully"
    else
        print_error "CTest failed"
        cd ..
        return 1
    fi

    cd ..
}

# Generate coverage report
generate_coverage() {
    print_header "Generating Code Coverage Report"

    cd "$BUILD_DIR"

    if [[ ! -f "CMakeCache.txt" ]]; then
        print_error "Build directory not configured. Run 'cmake -DENABLE_COVERAGE=ON ..' first"
        cd ..
        return 1
    fi

    print_status "Building with coverage..."
    make -j$(nproc) coverage

    if [[ -d "coverage_report" ]]; then
        print_status "Coverage report generated: $BUILD_DIR/coverage_report/index.html"
        print_status "Open in browser: open $BUILD_DIR/coverage_report/index.html"
    else
        print_error "Coverage report generation failed"
        cd ..
        return 1
    fi

    cd ..
}

# Clean build directory
clean_build() {
    print_header "Cleaning Build Directory"

    if [[ -d "$BUILD_DIR" ]]; then
        print_status "Removing $BUILD_DIR..."
        rm -rf "$BUILD_DIR"
        print_status "Build directory cleaned"
    else
        print_warning "Build directory '$BUILD_DIR' does not exist"
    fi
}

# Show usage
show_usage() {
    echo "MarbleDB Test Runner"
    echo
    echo "Usage: $0 [COMMAND]"
    echo
    echo "Commands:"
    echo "  build         Build all tests"
    echo "  unit          Run unit tests only"
    echo "  integration   Run integration tests only"
    echo "  performance   Run performance tests only"
    echo "  legacy        Run legacy tests only"
    echo "  all           Run all tests (default)"
    echo "  ctest         Run tests using CTest"
    echo "  coverage      Generate code coverage report"
    echo "  clean         Clean build directory"
    echo "  help          Show this help message"
    echo
    echo "Examples:"
    echo "  $0 build          # Build tests"
    echo "  $0 unit           # Run unit tests"
    echo "  $0 all            # Run all tests"
    echo "  $0 ctest          # Run with CTest"
    echo "  $0 coverage       # Generate coverage report"
    echo
    echo "Note: Make sure to run 'cmake -B build' first for coverage builds"
}

# Main script logic
main() {
    local command="${1:-all}"

    check_directory

    case "$command" in
        "build")
            build_tests
            ;;
        "unit")
            check_build
            run_unit_tests
            ;;
        "integration")
            check_build
            run_integration_tests
            ;;
        "performance")
            check_build
            run_performance_tests
            ;;
        "legacy")
            check_build
            run_legacy_tests
            ;;
        "all")
            check_build
            run_all_tests
            ;;
        "ctest")
            check_build
            run_ctest
            ;;
        "coverage")
            generate_coverage
            ;;
        "clean")
            clean_build
            ;;
        "help"|"-h"|"--help")
            show_usage
            ;;
        *)
            print_error "Unknown command: $command"
            echo
            show_usage
            exit 1
            ;;
    esac
}

# Run main function
main "$@"
