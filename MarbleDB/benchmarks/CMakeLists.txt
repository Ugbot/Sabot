# MarbleDB Benchmarks

cmake_minimum_required(VERSION 3.15)

# Simple comprehensive benchmark
add_executable(marble_bench simple_bench.cpp)
target_link_libraries(marble_bench PRIVATE marble)

# P1 optimization benchmark
add_executable(p1_optimization_bench p1_optimization_bench.cpp)
target_link_libraries(p1_optimization_bench PRIVATE marble)

# Simple P1 functionality test
add_executable(simple_p1_test simple_p1_test.cpp)
target_link_libraries(simple_p1_test PRIVATE marble)

# Performance optimizations benchmark (sorted blocks, block bloom filters, negative cache)
add_executable(performance_optimizations_bench performance_optimizations_bench.cpp)
target_link_libraries(performance_optimizations_bench PRIVATE marble)

# Install benchmarks
install(TARGETS
    marble_bench
    p1_optimization_bench
    simple_p1_test
    performance_optimizations_bench
    RUNTIME DESTINATION bin/benchmarks
)
