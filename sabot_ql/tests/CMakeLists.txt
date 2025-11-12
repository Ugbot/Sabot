# Test executable for MarbleDB storage
add_executable(test_marbledb_storage test_marbledb_storage.cpp)

target_link_libraries(test_marbledb_storage
    sabot_ql
    Arrow::arrow_shared
    ${MARBLE_LIB_DIR}/libmarble.a
)

target_include_directories(test_marbledb_storage PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${MARBLE_INCLUDE_DIR}
    ${ARROW_INCLUDE_DIR}
)

# ================================================================
# Phase 1: MarbleDB Core Unit Tests
# ================================================================

# Test 1.1: Database lifecycle
add_executable(test_lifecycle unit/marbledb/test_lifecycle.cpp)
target_link_libraries(test_lifecycle
    sabot_ql
    Arrow::arrow_shared
    ${MARBLE_LIB_DIR}/libmarble.a
)
target_include_directories(test_lifecycle PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${MARBLE_INCLUDE_DIR}
    ${ARROW_INCLUDE_DIR}
)

# Test 1.2: Put/Get operations
add_executable(test_put_get unit/marbledb/test_put_get.cpp)
target_link_libraries(test_put_get
    sabot_ql
    Arrow::arrow_shared
    ${MARBLE_LIB_DIR}/libmarble.a
)
target_include_directories(test_put_get PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${MARBLE_INCLUDE_DIR}
    ${ARROW_INCLUDE_DIR}
)

# Test 1.3: Delete operations
add_executable(test_delete unit/marbledb/test_delete.cpp)
target_link_libraries(test_delete
    sabot_ql
    Arrow::arrow_shared
    ${MARBLE_LIB_DIR}/libmarble.a
)
target_include_directories(test_delete PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${MARBLE_INCLUDE_DIR}
    ${ARROW_INCLUDE_DIR}
)

# Test 1.4: Batch operations
add_executable(test_batch unit/marbledb/test_batch.cpp)
target_link_libraries(test_batch
    sabot_ql
    Arrow::arrow_shared
    ${MARBLE_LIB_DIR}/libmarble.a
)
target_include_directories(test_batch PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${MARBLE_INCLUDE_DIR}
    ${ARROW_INCLUDE_DIR}
)

# Test 1.5: Persistence and recovery
add_executable(test_persistence unit/marbledb/test_persistence.cpp)
target_link_libraries(test_persistence
    sabot_ql
    Arrow::arrow_shared
    ${MARBLE_LIB_DIR}/libmarble.a
)
target_include_directories(test_persistence PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${MARBLE_INCLUDE_DIR}
    ${ARROW_INCLUDE_DIR}
)

# ================================================================
# Phase 2: Iterator & Scanning Tests
# ================================================================

# Test 2.1: Basic iterator scanning
add_executable(test_basic_scan unit/iterator/test_basic_scan.cpp)
target_link_libraries(test_basic_scan
    sabot_ql
    Arrow::arrow_shared
    ${MARBLE_LIB_DIR}/libmarble.a
)
target_include_directories(test_basic_scan PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${MARBLE_INCLUDE_DIR}
    ${ARROW_INCLUDE_DIR}
)

# Test 2.2: Range scanning
add_executable(test_range_scan unit/iterator/test_range_scan.cpp)
target_link_libraries(test_range_scan
    sabot_ql
    Arrow::arrow_shared
    ${MARBLE_LIB_DIR}/libmarble.a
)
target_include_directories(test_range_scan PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${MARBLE_INCLUDE_DIR}
    ${ARROW_INCLUDE_DIR}
)

# ================================================================
# Phase 3: RDF Triple Store Unit Tests
# ================================================================

# Test 3.1: Column family setup for indexes
add_executable(test_triple_indexes unit/rdf/test_triple_indexes.cpp)
target_link_libraries(test_triple_indexes
    sabot_ql
    Arrow::arrow_shared
    ${MARBLE_LIB_DIR}/libmarble.a
)
target_include_directories(test_triple_indexes PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${MARBLE_INCLUDE_DIR}
    ${ARROW_INCLUDE_DIR}
)

# Test 3.2: Single triple insert & retrieval
add_executable(test_single_triple unit/rdf/test_single_triple.cpp)
target_link_libraries(test_single_triple
    sabot_ql
    Arrow::arrow_shared
    ${MARBLE_LIB_DIR}/libmarble.a
)
target_include_directories(test_single_triple PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${MARBLE_INCLUDE_DIR}
    ${ARROW_INCLUDE_DIR}
)

# Test 3.7: Iterator-based triple retrieval (DEBUG)
add_executable(test_triple_iterator unit/rdf/test_triple_iterator.cpp)
target_link_libraries(test_triple_iterator
    sabot_ql
    Arrow::arrow_shared
    ${MARBLE_LIB_DIR}/libmarble.a
)
target_include_directories(test_triple_iterator PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${MARBLE_INCLUDE_DIR}
    ${ARROW_INCLUDE_DIR}
)

# ================================================================
# Phase 4: Advanced Features Tests
# ================================================================

# Test 4.1: Sparse index and bloom filter
add_executable(test_sparse_index unit/advanced/test_sparse_index.cpp)
target_link_libraries(test_sparse_index
    sabot_ql
    Arrow::arrow_shared
    ${MARBLE_LIB_DIR}/libmarble.a
)
target_include_directories(test_sparse_index PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${MARBLE_INCLUDE_DIR}
    ${ARROW_INCLUDE_DIR}
)

# Test 4.2: Merge operations and compaction
add_executable(test_merge_compaction unit/advanced/test_merge_compaction.cpp)
target_link_libraries(test_merge_compaction
    sabot_ql
    Arrow::arrow_shared
    ${MARBLE_LIB_DIR}/libmarble.a
)
target_include_directories(test_merge_compaction PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${MARBLE_INCLUDE_DIR}
    ${ARROW_INCLUDE_DIR}
)

# Test 4.3: Cache and memory management
add_executable(test_cache_memory unit/advanced/test_cache_memory.cpp)
target_link_libraries(test_cache_memory
    sabot_ql
    Arrow::arrow_shared
    ${MARBLE_LIB_DIR}/libmarble.a
)
target_include_directories(test_cache_memory PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${MARBLE_INCLUDE_DIR}
    ${ARROW_INCLUDE_DIR}
)

# ================================================================
# Phase 7: Performance & Stress Tests
# ================================================================

# Test 7.1: Throughput and latency benchmarks
add_executable(test_throughput_latency unit/performance/test_throughput_latency.cpp)
target_link_libraries(test_throughput_latency
    sabot_ql
    Arrow::arrow_shared
    ${MARBLE_LIB_DIR}/libmarble.a
)
target_include_directories(test_throughput_latency PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${MARBLE_INCLUDE_DIR}
    ${ARROW_INCLUDE_DIR}
)

# Test 7.2: Stress testing
add_executable(test_stress unit/performance/test_stress.cpp)
target_link_libraries(test_stress
    sabot_ql
    Arrow::arrow_shared
    ${MARBLE_LIB_DIR}/libmarble.a
)
target_include_directories(test_stress PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${MARBLE_INCLUDE_DIR}
    ${ARROW_INCLUDE_DIR}
)

# ================================================================

# End-to-end integration test for SPARQL engine
add_executable(test_sparql_e2e test_sparql_e2e.cpp)

target_link_libraries(test_sparql_e2e
    sabot_ql
    Arrow::arrow_shared
    ${CMAKE_SOURCE_DIR}/../vendor/arrow/cpp/build/install/lib/libarrow_compute.dylib
    ${MARBLE_LIB_DIR}/libmarble.a
)

target_include_directories(test_sparql_e2e PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${MARBLE_INCLUDE_DIR}
    ${ARROW_INCLUDE_DIR}
)

# End-to-end integration test for SPARQL 1.1 property paths
add_executable(test_property_paths_e2e test_property_paths_e2e.cpp)

target_link_libraries(test_property_paths_e2e
    sabot_ql
    Arrow::arrow_shared
    ${CMAKE_SOURCE_DIR}/../vendor/arrow/cpp/build/install/lib/libarrow_compute.dylib
    ${MARBLE_LIB_DIR}/libmarble.a
)

target_include_directories(test_property_paths_e2e PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${MARBLE_INCLUDE_DIR}
    ${ARROW_INCLUDE_DIR}
)
