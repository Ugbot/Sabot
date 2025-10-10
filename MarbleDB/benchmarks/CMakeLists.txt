# MarbleDB Benchmarks

cmake_minimum_required(VERSION 3.15)

# Simple comprehensive benchmark
add_executable(marble_bench simple_bench.cpp)
target_link_libraries(marble_bench PRIVATE marble)

# Install benchmarks
install(TARGETS 
    marble_bench
    RUNTIME DESTINATION bin/benchmarks
)
