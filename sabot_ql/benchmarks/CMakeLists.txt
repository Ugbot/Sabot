# SabotQL Benchmarks

# HashJoin performance benchmark
add_executable(benchmark_hashjoin
    benchmark_hashjoin.cpp
)

target_link_libraries(benchmark_hashjoin
    sabot_ql
    Arrow::arrow_shared
    ${MARBLE_LIB_DIR}/libmarble.a
)

target_include_directories(benchmark_hashjoin PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${MARBLE_INCLUDE_DIR}
    ${ARROW_INCLUDE_DIR}
)

# Set output directory for benchmarks
set_target_properties(benchmark_hashjoin
    PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/benchmarks
)

# Property Path performance benchmark
add_executable(bench_property_paths
    bench_property_paths.cpp
)

target_link_libraries(bench_property_paths
    sabot_ql
    Arrow::arrow_shared
    ${MARBLE_LIB_DIR}/libmarble.a
)

target_include_directories(bench_property_paths PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${MARBLE_INCLUDE_DIR}
    ${ARROW_INCLUDE_DIR}
)

set_target_properties(bench_property_paths
    PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/benchmarks
)

# Olympic Dataset Property Path benchmark (Multi-Agent)
add_executable(bench_olympics_property_paths
    bench_olympics_property_paths.cpp
)

target_link_libraries(bench_olympics_property_paths
    sabot_ql
    Arrow::arrow_shared
    ${MARBLE_LIB_DIR}/libmarble.a
    pthread  # For multi-threading
)

target_include_directories(bench_olympics_property_paths PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${MARBLE_INCLUDE_DIR}
    ${ARROW_INCLUDE_DIR}
)

set_target_properties(bench_olympics_property_paths
    PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/benchmarks
)

# QLever Olympics benchmark (real queries from QLever docs)
add_executable(bench_qlever_olympics
    bench_qlever_olympics.cpp
)

target_link_libraries(bench_qlever_olympics
    sabot_ql
    Arrow::arrow_shared
    ${MARBLE_LIB_DIR}/libmarble.a
)

target_include_directories(bench_qlever_olympics PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${MARBLE_INCLUDE_DIR}
    ${ARROW_INCLUDE_DIR}
)

set_target_properties(bench_qlever_olympics
    PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/benchmarks
)
