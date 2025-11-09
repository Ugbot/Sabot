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

# Comprehensive test harness for benchmarking
add_executable(marble_bench_harness marble_bench_harness.cpp)
target_link_libraries(marble_bench_harness PRIVATE marble)

# Simple working benchmark (avoids Arrow batch API issues)
add_executable(marble_bench_simple marble_bench_simple.cpp)
target_link_libraries(marble_bench_simple PRIVATE marble)

# New vendor-pattern benchmarks (RocksDB/Tonbo style)
# Common infrastructure library
add_library(marble_bench_common STATIC common.cpp)
target_link_libraries(marble_bench_common PUBLIC marble)
target_include_directories(marble_bench_common PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

# Batch write benchmark
add_executable(batch_write_bench batch_write_bench.cpp)
target_link_libraries(batch_write_bench PRIVATE marble_bench_common marble)

# RocksDB baseline benchmark (simpler, working version)
add_executable(rocksdb_baseline rocksdb_baseline.cpp)
target_include_directories(rocksdb_baseline PRIVATE
    ${CMAKE_SOURCE_DIR}/../vendor/rocksdb/include
)
target_link_libraries(rocksdb_baseline PRIVATE
    ${CMAKE_SOURCE_DIR}/../vendor/rocksdb/build/librocksdb.a
    z  # zlib (required by RocksDB)
    bz2  # bzip2 (required by RocksDB)
)

# MarbleDB baseline benchmark (comparable to RocksDB)
add_executable(marbledb_baseline marbledb_baseline.cpp)
target_link_libraries(marbledb_baseline PRIVATE marble)

# RocksDB API optimizations benchmark
add_executable(rocksdb_api_optimizations_bench rocksdb_api_optimizations_bench.cpp)
target_include_directories(rocksdb_api_optimizations_bench PRIVATE
    ${PROJECT_SOURCE_DIR}/include
    ${ROCKSDB_INCLUDE_DIR}
)
target_link_libraries(rocksdb_api_optimizations_bench PRIVATE
    marble
    ${ROCKSDB_LIBRARIES}
)

# Install benchmarks
install(TARGETS
    marble_bench
    p1_optimization_bench
    simple_p1_test
    performance_optimizations_bench
    marble_bench_harness
    marble_bench_simple
    batch_write_bench
    rocksdb_baseline
    marbledb_baseline
    RUNTIME DESTINATION bin/benchmarks
)

# Install config example
install(FILES bench_config_example.json
    DESTINATION share/marbledb/benchmarks
)
