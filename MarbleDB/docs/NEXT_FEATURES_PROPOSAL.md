# MarbleDB Next Features: Join, OLTP, and OLAP Improvements

**Date:** October 18, 2025
**Status:** Approved Proposal
**Goal:** Make MarbleDB better for joins (leveraging Arrow native), faster for OLTP, and faster for OLAP

---

## Executive Summary

MarbleDB has strong fundamentals (LSM-tree, Arrow native, zone maps, bloom filters, Raft consensus) but lacks **join implementations** and **SIMD-vectorized operators**. The header files define interfaces for joins, aggregations, and window functions in `advanced_query.h` and `execution_engine.h`, but **none are implemented**.

This proposal focuses on 3 areas to dramatically improve performance:

1. **Arrow-Native Joins** - Leverage zero-copy format for fast joins
2. **OLTP Optimizations** - Make point lookups and writes faster
3. **OLAP Optimizations** - SIMD aggregations, parallel execution, materialized views

---

## Current State Analysis

### ✅ What MarbleDB HAS

**Storage & Indexing:**
- Arrow IPC columnar format with zero-copy operations
- LSM-tree storage with leveled compaction
- Sparse indexes (index every 8,192 rows)
- Zone maps (min/max/null counts per page)
- Block-level bloom filters (1% false positive rate)
- Hot key cache (LRU, 64 MB default)

**OLTP Features:**
- Zero-copy RecordRef (10-100× less memory)
- Merge operators (counters, append, set union, min/max, JSON merge)
- Column families (multi-tenant, type-safe isolation)
- Multi-get batching (10-50× faster than individual lookups)
- Delete range (1000× faster bulk deletion)
- Transactions (optimistic MVCC)

**Performance (Validated in benchmarks):**
- Write throughput: 4-7M rows/sec
- Analytical scans: 20-50M rows/sec (5-20× faster than traditional LSM)
- Point lookups: 5-10 μs (hot), 20-50 μs (cold)
- Missing key lookups: 2-5 μs (bloom filter optimization)

**Distributed Systems:**
- NuRaft consensus integration
- Arrow Flight WAL streaming
- Raft-replicated manifests
- Dynamic cluster membership

### ❌ What MarbleDB LACKS

**Join Operations:**
- Interface defined in `include/marble/advanced_query.h` (`ExecuteJoin()`) but **NOT IMPLEMENTED**
- No hash join implementation
- No merge join for sorted data
- No broadcast join for distributed queries
- `grep "hash_join|merge_join"` returns **0 results** ❌

**SIMD Vectorization:**
- Mentioned in `TECHNICAL_PLAN.md` and `README.md` but **NO ACTUAL CODE**
- `grep "SIMD|__m256|__m512|_mm_"` returns **only documentation** ❌
- Arrow Compute has SIMD internally, but MarbleDB doesn't use it

**Parallel Execution:**
- No parallel hash aggregation (single-threaded GROUP BY)
- No multi-threaded scans
- Morsel-driven parallelism framework exists in `execution_engine.h` but incomplete

**Advanced Query Features:**
- Window functions interface defined (`WindowFunction`, `WindowSpec`) but **NOT IMPLEMENTED**
- Time series analytics interface defined (`TimeSeriesAnalytics`) but **NOT IMPLEMENTED**
- Query optimization is basic (filter pushdown only, no join reordering)

---

## Proposal: 3 High-Impact Feature Categories

### Category 1: Arrow-Native Join Implementations ⭐⭐⭐

**Problem:** MarbleDB cannot execute JOIN queries. `ExecuteJoin()` is a pure virtual function with no concrete implementation.

**Why Arrow Native Format Helps Joins:**

1. **Zero-Copy Data Transfer:** Join operators pass RecordBatch pointers instead of copying data
2. **Columnar Layout:** SIMD-parallel key comparisons on join columns
3. **Arrow Compute Library:** Pre-built hash tables and join primitives
4. **Batched Processing:** Process 2048-8192 rows at a time (cache-friendly)

---

#### 1.1 Hash Join (Arrow Native)

**Algorithm:** Build hash table from smaller table, probe with larger table.

**Implementation Outline:**

```cpp
// include/marble/join/hash_join.h
#pragma once
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <marble/status.h>

namespace marble {

class ArrowHashJoin {
public:
    ArrowHashJoin(const std::string& left_key,
                  const std::string& right_key,
                  JoinType join_type = JoinType::kInner);

    /**
     * @brief Build phase: Create hash table from build side (smaller table)
     */
    Status BuildPhase(const arrow::Table& build_table);

    /**
     * @brief Probe phase: Probe hash table with probe side (larger table)
     */
    Status ProbePhase(const arrow::Table& probe_table,
                     std::shared_ptr<arrow::Table>* result);

    /**
     * @brief Execute complete join operation
     */
    Status Execute(const arrow::Table& left_table,
                  const arrow::Table& right_table,
                  std::shared_ptr<arrow::Table>* result);

private:
    std::string left_key_;
    std::string right_key_;
    JoinType join_type_;

    // Hash table: key → row indices
    std::unordered_map<int64_t, std::vector<int64_t>> hash_table_;

    // Build table reference (for fetching matched rows)
    std::shared_ptr<arrow::Table> build_table_;

    Status BuildHashTableInt64(const arrow::Int64Array& key_array);
    Status ProbeHashTableInt64(const arrow::Int64Array& probe_keys,
                              std::vector<std::pair<int64_t, int64_t>>& matches);
};

} // namespace marble
```

**Arrow-Specific Optimizations:**

1. **Use Arrow Compute's HashTable:**
   ```cpp
   #include <arrow/compute/kernels/hash_aggregate.h>

   // Arrow Compute already has optimized hash tables
   arrow::compute::Grouper grouper;
   grouper.Consume(key_arrays);
   ```

2. **Zero-Copy Result Construction:**
   ```cpp
   // Instead of copying data, slice arrays and create new RecordBatch
   std::vector<std::shared_ptr<arrow::Array>> result_arrays;
   for (auto [left_idx, right_idx] : matches) {
       result_arrays.push_back(left_table->column(i)->Slice(left_idx, 1));
       result_arrays.push_back(right_table->column(j)->Slice(right_idx, 1));
   }
   auto result_batch = arrow::RecordBatch::Make(schema, matches.size(), result_arrays);
   ```

3. **SIMD Hash Computation:**
   Arrow's hash functions use SIMD internally (XXHash with AVX2/NEON).

**Performance Target:**
- 1M × 1M join: **2-5 seconds** (vs 10-30 seconds naive nested loop)
- **5-10× faster** than row-based joins
- **Zero-copy overhead** (only index manipulation)

**Effort Estimate:** 3-5 implementation steps
- Step 1: Implement hash table building
- Step 2: Implement probe phase
- Step 3: Handle different join types (inner, left, right, full)
- Step 4: Add tests
- Step 5: Integrate with execution engine

**Files to Create:**
- `include/marble/join/hash_join.h`
- `src/join/hash_join.cpp`
- `tests/unit/test_hash_join.cpp`
- `benchmarks/join_benchmark.cpp`

---

#### 1.2 Merge Join (Sorted Data)

**When to Use:** Both sides already sorted on join key (common for time-series data).

**Algorithm:** Linear scan through both sorted tables, matching on equality.

**Implementation Outline:**

```cpp
// include/marble/join/merge_join.h
#pragma once
#include <arrow/api.h>
#include <marble/status.h>

namespace marble {

class ArrowMergeJoin {
public:
    ArrowMergeJoin(const std::string& left_key,
                   const std::string& right_key,
                   JoinType join_type = JoinType::kInner);

    /**
     * @brief Execute merge join (assumes both tables sorted on join key)
     */
    Status Execute(const arrow::Table& left_sorted,
                  const arrow::Table& right_sorted,
                  std::shared_ptr<arrow::Table>* result);

private:
    std::string left_key_;
    std::string right_key_;
    JoinType join_type_;

    Status MergePhase(const arrow::Array& left_keys,
                     const arrow::Array& right_keys,
                     std::vector<std::pair<int64_t, int64_t>>& matches);
};

} // namespace marble
```

**Arrow-Specific Optimizations:**

1. **Leverage Sorted Block Property:**
   MarbleDB already sorts blocks by key during compaction. Merge join can exploit this.

2. **SIMD Comparisons:**
   ```cpp
   // Compare 8 keys at once with AVX-512
   __m512i left_vec = _mm512_loadu_si512(&left_keys[left_idx]);
   __m512i right_vec = _mm512_set1_epi64(right_keys[right_idx]);
   __mmask8 eq_mask = _mm512_cmpeq_epi64_mask(left_vec, right_vec);
   ```

3. **Batch Processing:**
   Process 2048 rows per DataChunk, minimizing iterator overhead.

**Performance Target:**
- 1M × 1M sorted join: **0.5-2 seconds**
- **10-20× faster** than hash join when data is already sorted
- **O(N + M) complexity** vs O(N log M) for hash join

**Use Case:**
```sql
-- Time-series join on timestamp (both tables sorted)
SELECT * FROM trades t1
JOIN quotes t2 ON t1.timestamp = t2.timestamp
WHERE t1.timestamp BETWEEN '2025-01-01' AND '2025-01-31';
```

**Effort Estimate:** 2-4 steps
- Step 1: Implement merge phase (linear scan)
- Step 2: Handle join types
- Step 3: Add SIMD optimizations (optional)
- Step 4: Tests and benchmarks

**Files to Create:**
- `include/marble/join/merge_join.h`
- `src/join/merge_join.cpp`
- `tests/unit/test_merge_join.cpp`

---

#### 1.3 Broadcast Join (Distributed)

**When to Use:** Small table (< 100 MB) joining with large table (> 1 GB) across multiple nodes.

**Algorithm:** Broadcast small table to all nodes, perform local hash join on each node.

**Implementation Outline:**

```cpp
// include/marble/join/broadcast_join.h
#pragma once
#include <arrow/api.h>
#include <arrow/flight/api.h>
#include <marble/status.h>
#include <marble/raft/cluster_manager.h>

namespace marble {

class BroadcastJoin {
public:
    BroadcastJoin(RaftClusterManager* cluster_manager);

    /**
     * @brief Broadcast small table to all nodes via Arrow Flight
     */
    Status BroadcastSmallTable(const arrow::Table& small_table,
                              const std::string& join_key);

    /**
     * @brief Execute local hash join on current node
     */
    Status LocalHashJoin(const arrow::RecordBatch& local_batch,
                        std::shared_ptr<arrow::RecordBatch>* result);

    /**
     * @brief Execute distributed broadcast join
     */
    Status Execute(const arrow::Table& small_table,
                  const std::string& large_table_name,
                  const std::string& join_key,
                  std::shared_ptr<arrow::Table>* result);

private:
    RaftClusterManager* cluster_manager_;

    // Cached broadcast table
    std::shared_ptr<arrow::Table> broadcast_table_;
    std::unique_ptr<ArrowHashJoin> local_join_;
};

} // namespace marble
```

**Arrow Flight Advantages:**

1. **Zero-Copy Network Transfer:**
   ```cpp
   // Send table via Arrow Flight (no serialization overhead)
   arrow::flight::FlightClient client;
   arrow::flight::FlightDescriptor descriptor;
   auto writer = client.DoPut(descriptor);
   writer->WriteRecordBatch(*small_table_batch);
   ```

2. **Efficient Serialization:**
   Arrow IPC format is already network-ready (no conversion needed).

3. **Streaming Large Results:**
   ```cpp
   // Each node streams local join results back
   auto reader = client.DoGet(ticket);
   while (true) {
       arrow::RecordBatch batch;
       if (!reader->ReadNext(&batch)) break;
       result_batches.push_back(batch);
   }
   ```

**Performance Target:**
- Broadcast 100 MB table to 10 nodes: **200-500 ms**
- Join 100M rows (distributed): **5-10 seconds**
- **Near-linear scaling** with number of nodes

**Effort Estimate:** 4-6 steps
- Step 1: Implement Arrow Flight broadcast
- Step 2: Cache broadcast table at each node
- Step 3: Local hash join on each node
- Step 4: Collect and merge results
- Step 5: Handle failures and retries
- Step 6: Tests and distributed benchmarks

**Files to Create:**
- `include/marble/join/broadcast_join.h`
- `src/join/broadcast_join.cpp`
- `tests/integration/test_distributed_join.cpp`

---

#### 1.4 Join Operator Integration

**Goal:** Integrate join implementations with existing DuckDB-style execution engine.

**Add to `execution_engine.h`:**

```cpp
/**
 * @brief Join Operator - Executes joins between two tables
 */
class JoinOperator : public PhysicalOperator {
public:
    enum class JoinAlgorithm {
        kHash,       // Hash join (general purpose)
        kMerge,      // Merge join (sorted data)
        kNestedLoop, // Nested loop (fallback for small tables)
        kBroadcast   // Broadcast join (distributed)
    };

    JoinOperator(std::unique_ptr<PhysicalOperator> left_child,
                std::unique_ptr<PhysicalOperator> right_child,
                const JoinCondition& condition,
                JoinType join_type = JoinType::kInner,
                JoinAlgorithm algorithm = JoinAlgorithm::kHash);

    Status GetChunk(std::unique_ptr<DataChunk>* chunk) override;
    Status Initialize() override;
    std::vector<PhysicalOperator*> GetChildren() const override;

private:
    std::unique_ptr<PhysicalOperator> left_child_;
    std::unique_ptr<PhysicalOperator> right_child_;
    JoinCondition condition_;
    JoinType join_type_;
    JoinAlgorithm algorithm_;

    // Join implementation (polymorphic)
    std::unique_ptr<ArrowHashJoin> hash_join_;
    std::unique_ptr<ArrowMergeJoin> merge_join_;

    // Buffered join results
    std::vector<std::unique_ptr<DataChunk>> result_chunks_;
    size_t current_chunk_index_ = 0;

    Status ExecuteHashJoin();
    Status ExecuteMergeJoin();
    Status ChooseJoinAlgorithm();  // Auto-select based on data properties
};
```

**Join Algorithm Selection:**

```cpp
Status JoinOperator::ChooseJoinAlgorithm() {
    // 1. Check if both sides sorted → Merge Join
    if (left_child_->IsSorted(condition_.left_column) &&
        right_child_->IsSorted(condition_.right_column)) {
        algorithm_ = JoinAlgorithm::kMerge;
        return Status::OK();
    }

    // 2. Check table sizes → Broadcast join if small table
    int64_t left_size = EstimateSize(left_child_);
    int64_t right_size = EstimateSize(right_child_);
    if (left_size < 100 * 1024 * 1024 || right_size < 100 * 1024 * 1024) {
        algorithm_ = JoinAlgorithm::kBroadcast;
        return Status::OK();
    }

    // 3. Default to hash join
    algorithm_ = JoinAlgorithm::kHash;
    return Status::OK();
}
```

**Effort Estimate:** 2-3 steps
- Step 1: Implement JoinOperator class
- Step 2: Add join algorithm selection logic
- Step 3: Integrate with QueryExecutor's CreatePipeline

**Files to Modify:**
- `include/marble/execution_engine.h` (add JoinOperator class)
- `src/core/execution_engine.cpp` (implement JoinOperator)
- `tests/unit/test_join_operator.cpp`

---

### Category 2: OLTP Performance Improvements ⭐⭐

**Current State:**
- Hot key lookups: 5-10 μs (competitive with Tonbo)
- Cold key lookups: 20-50 μs (2× slower than Tonbo)
- Write throughput: 4-7M rows/sec

**Goal:** Improve cold lookups to 10-15 μs, improve write throughput to 10-20M rows/sec.

---

#### 2.1 Sorted Blocks (Quick Win)

**Problem:** Currently blocks store keys in insertion order, requiring linear scan (O(8192) comparisons).

**Solution:** Sort keys within each block during write, enabling binary search (O(13) comparisons).

**Already Documented in:** `docs/POINT_LOOKUP_OPTIMIZATIONS.md` (lines 319-330)

**Implementation:**

```cpp
// In SSTable::Create() or Block::Write()
void Block::FinalizeBlock() {
    // Sort records by key before writing to disk
    std::sort(records_.begin(), records_.end(),
             [](const Record& a, const Record& b) {
                 return a.key->Compare(*b.key) < 0;
             });

    // Write sorted block
    WriteToStorage();
}

// In Block::Get() - use binary search instead of linear scan
Status Block::Get(const std::shared_ptr<Key>& key, std::shared_ptr<Record>* record) {
    // Binary search in sorted block: O(log 8192) = 13 comparisons
    auto it = std::lower_bound(records_.begin(), records_.end(), key,
                              [](const Record& rec, const std::shared_ptr<Key>& k) {
                                  return rec.key->Compare(*k) < 0;
                              });

    if (it != records_.end() && it->key->Compare(*key) == 0) {
        *record = std::make_shared<Record>(*it);
        return Status::OK();
    }
    return Status::NotFound("Key not found");
}
```

**Performance Impact:**
- Cold key lookups: 20-50 μs → **10-15 μs** (2-5× faster)
- **Zero memory overhead**
- **Zero runtime overhead** (sort during write, which is rare)

**Effort Estimate:** 1-2 steps
- Step 1: Add sorting to block finalization
- Step 2: Replace linear scan with binary search

**Files to Modify:**
- `src/storage/block.cpp` (add sorting)
- `src/storage/sstable.cpp` (use binary search in Get)

---

#### 2.2 Negative Cache

**Problem:** Repeated lookups for non-existent keys still scan entire block (20-50 μs).

**Solution:** Cache keys that definitely don't exist using a bloom filter.

**Already Documented in:** `docs/POINT_LOOKUP_OPTIMIZATIONS.md` (lines 53-77)

**Implementation:**

```cpp
// include/marble/storage/negative_cache.h
#pragma once
#include <marble/storage/bloom_filter.h>
#include <marble/key.h>

namespace marble {

class NegativeCache {
public:
    NegativeCache(size_t capacity = 10000, double fpr = 0.01);

    /**
     * @brief Record a key that was not found
     */
    void RecordMiss(const std::shared_ptr<Key>& key);

    /**
     * @brief Check if key definitely doesn't exist
     */
    bool DefinitelyNotExists(const std::shared_ptr<Key>& key) const;

    /**
     * @brief Clear negative cache (after writes)
     */
    void Clear();

private:
    BloomFilter negative_bloom_;  // Reuse existing bloom filter code
    size_t capacity_;
};

} // namespace marble
```

**Integration with DB::Get():**

```cpp
Status MarbleDB::Get(const ReadOptions& options,
                    const std::shared_ptr<Key>& key,
                    std::shared_ptr<Record>* record) {
    // 1. Check negative cache first (1-2 μs)
    if (negative_cache_->DefinitelyNotExists(key)) {
        return Status::NotFound("Key in negative cache");
    }

    // 2. Check hot key cache
    if (hot_key_cache_->Get(key, record)) {
        return Status::OK();
    }

    // 3. Search in storage
    Status status = SearchInStorage(key, record);

    // 4. If not found, add to negative cache
    if (status.IsNotFound()) {
        negative_cache_->RecordMiss(key);
    }

    return status;
}
```

**Performance Impact:**
- Missing key lookups: 20-50 μs → **2-5 μs** (10-25× faster)
- Memory overhead: ~200 KB for 10K recent misses
- Especially useful for JOIN operations (probing for non-existent keys)

**Effort Estimate:** 1-2 steps
- Step 1: Implement NegativeCache class
- Step 2: Integrate with DB::Get()

**Files to Create:**
- `include/marble/storage/negative_cache.h`
- `src/storage/negative_cache.cpp`
- `tests/unit/test_negative_cache.cpp`

---

#### 2.3 WAL Batching for Write Throughput

**Problem:** Each Put() operation writes to WAL individually, causing excessive I/O.

**Solution:** Batch multiple writes together, flush every 10ms or 1000 operations.

**Implementation:**

```cpp
// include/marble/storage/batched_wal.h
#pragma once
#include <marble/wal.h>
#include <vector>
#include <mutex>
#include <condition_variable>
#include <thread>

namespace marble {

class BatchedWAL {
public:
    BatchedWAL(const std::string& wal_path,
              size_t batch_size = 1000,
              uint64_t flush_interval_ms = 10);
    ~BatchedWAL();

    /**
     * @brief Add write operation to batch
     */
    Status Put(const std::shared_ptr<Key>& key,
              const std::shared_ptr<Record>& record);

    /**
     * @brief Force flush current batch
     */
    Status Flush();

    /**
     * @brief Start background flush thread
     */
    void StartBackgroundFlusher();

    /**
     * @brief Stop background flush thread
     */
    void StopBackgroundFlusher();

private:
    struct WriteOperation {
        enum Type { PUT, DELETE, MERGE };
        Type type;
        std::shared_ptr<Key> key;
        std::shared_ptr<Record> record;
    };

    std::vector<WriteOperation> batch_buffer_;
    std::mutex batch_mutex_;
    std::condition_variable batch_cv_;

    size_t batch_size_;
    uint64_t flush_interval_ms_;

    std::unique_ptr<WAL> underlying_wal_;
    std::thread background_flusher_;
    std::atomic<bool> stop_flusher_{false};

    Status FlushBatch();
    void BackgroundFlusherThread();
};

} // namespace marble
```

**Background Flusher Logic:**

```cpp
void BatchedWAL::BackgroundFlusherThread() {
    while (!stop_flusher_.load()) {
        std::unique_lock<std::mutex> lock(batch_mutex_);

        // Wait for batch to fill or timeout
        batch_cv_.wait_for(lock, std::chrono::milliseconds(flush_interval_ms_),
                          [this] { return batch_buffer_.size() >= batch_size_ || stop_flusher_; });

        if (!batch_buffer_.empty()) {
            FlushBatch();
        }
    }
}
```

**Performance Impact:**
- Write throughput: 4-7M rows/sec → **10-20M rows/sec** (2.5-5× faster)
- Write latency: +10ms average (batching delay)
- Trade-off: Slight latency increase for massive throughput gain

**Effort Estimate:** 2-3 steps
- Step 1: Implement BatchedWAL class
- Step 2: Add background flusher thread
- Step 3: Integrate with DB write path

**Files to Create:**
- `include/marble/storage/batched_wal.h`
- `src/storage/batched_wal.cpp`
- `tests/unit/test_batched_wal.cpp`

---

### Category 3: OLAP Performance Improvements ⭐⭐⭐

**Current State:**
- Analytical scans: 20-50M rows/sec
- Aggregations: Scalar loops (no SIMD)
- GROUP BY: Single-threaded

**Goal:** 5-8× faster aggregations with SIMD, 3-7× faster GROUP BY with parallelism.

---

#### 3.1 SIMD-Vectorized Aggregations

**Problem:** Current aggregations use scalar loops.

**Solution:** Use SIMD intrinsics to process 8 values at once (AVX-512) or 2 values (NEON).

**Implementation (AVX-512 for x86):**

```cpp
// include/marble/compute/simd_aggregate.h
#pragma once
#include <arrow/api.h>
#include <marble/status.h>

#ifdef __AVX512F__
#include <immintrin.h>
#endif

#ifdef __ARM_NEON
#include <arm_neon.h>
#endif

namespace marble {

class SIMDAggregator {
public:
    /**
     * @brief SIMD-accelerated SUM for int64 column
     */
    static arrow::Result<int64_t> SumInt64(const arrow::Int64Array& array);

    /**
     * @brief SIMD-accelerated COUNT for any column
     */
    static arrow::Result<int64_t> Count(const arrow::Array& array);

    /**
     * @brief SIMD-accelerated MIN for int64 column
     */
    static arrow::Result<int64_t> MinInt64(const arrow::Int64Array& array);

    /**
     * @brief SIMD-accelerated MAX for int64 column
     */
    static arrow::Result<int64_t> MaxInt64(const arrow::Int64Array& array);

private:
#ifdef __AVX512F__
    static int64_t SumInt64_AVX512(const int64_t* values, size_t count);
    static int64_t MinInt64_AVX512(const int64_t* values, size_t count);
    static int64_t MaxInt64_AVX512(const int64_t* values, size_t count);
#endif

#ifdef __ARM_NEON
    static int64_t SumInt64_NEON(const int64_t* values, size_t count);
    static int64_t MinInt64_NEON(const int64_t* values, size_t count);
    static int64_t MaxInt64_NEON(const int64_t* values, size_t count);
#endif
};

} // namespace marble
```

**AVX-512 SUM Implementation:**

```cpp
#ifdef __AVX512F__
int64_t SIMDAggregator::SumInt64_AVX512(const int64_t* values, size_t count) {
    __m512i sum_vec = _mm512_setzero_si512();

    // Process 8 int64 values at a time
    size_t i = 0;
    for (; i + 8 <= count; i += 8) {
        __m512i vals = _mm512_loadu_si512(&values[i]);
        sum_vec = _mm512_add_epi64(sum_vec, vals);
    }

    // Horizontal reduction: sum all 8 lanes
    int64_t result = _mm512_reduce_add_epi64(sum_vec);

    // Handle remaining elements (< 8)
    for (; i < count; ++i) {
        result += values[i];
    }

    return result;
}
#endif
```

**Apple Silicon NEON Implementation:**

```cpp
#ifdef __ARM_NEON
int64_t SIMDAggregator::SumInt64_NEON(const int64_t* values, size_t count) {
    int64x2_t sum_vec = vdupq_n_s64(0);

    // Process 2 int64 values at a time
    size_t i = 0;
    for (; i + 2 <= count; i += 2) {
        int64x2_t vals = vld1q_s64(&values[i]);
        sum_vec = vaddq_s64(sum_vec, vals);
    }

    // Extract lanes and sum
    int64_t result = vgetq_lane_s64(sum_vec, 0) + vgetq_lane_s64(sum_vec, 1);

    // Handle remaining element
    if (i < count) {
        result += values[i];
    }

    return result;
}
#endif
```

**Integration with AggregateOperator:**

```cpp
Status AggregateOperator::ComputeAggregates() {
    for (const auto& agg_func : aggregates_) {
        if (agg_func.function_name == "SUM") {
            // Use SIMD-accelerated SUM
            auto array = chunk->GetColumn(agg_func.column_name);
            auto int64_array = std::static_pointer_cast<arrow::Int64Array>(array);
            auto result = SIMDAggregator::SumInt64(*int64_array);
            // ... store result
        } else if (agg_func.function_name == "COUNT") {
            // Use SIMD-accelerated COUNT
            // ...
        }
    }
}
```

**Performance Impact:**
- Aggregations with AVX-512: **5-8× faster**
- Aggregations with NEON: **2-3× faster**
- `SELECT SUM(amount) FROM transactions`: 100ms → **12-20ms**

**Effort Estimate:** 3-4 steps
- Step 1: Implement SUM with SIMD (AVX-512 + NEON)
- Step 2: Implement COUNT, MIN, MAX with SIMD
- Step 3: Integrate with AggregateOperator
- Step 4: Benchmarks and tests

**Files to Create:**
- `include/marble/compute/simd_aggregate.h`
- `src/compute/simd_aggregate.cpp`
- `tests/unit/test_simd_aggregate.cpp`
- `benchmarks/simd_aggregate_bench.cpp`

---

#### 3.2 Parallel Hash Aggregation

**Problem:** GROUP BY operations use single thread, wasting CPU cores.

**Solution:** Partition data by hash(group_key) across threads, aggregate in parallel, merge results.

**Implementation:**

```cpp
// include/marble/compute/parallel_aggregator.h
#pragma once
#include <arrow/api.h>
#include <marble/status.h>
#include <marble/execution_engine.h>
#include <thread>
#include <vector>

namespace marble {

class ParallelHashAggregator {
public:
    ParallelHashAggregator(size_t num_threads = 0);  // 0 = hardware concurrency

    /**
     * @brief Execute parallel hash aggregation
     */
    Status Execute(const arrow::Table& input,
                  const std::vector<std::string>& group_by_columns,
                  const std::vector<AggregateOperator::AggregateFunction>& aggregates,
                  std::shared_ptr<arrow::Table>* result);

private:
    size_t num_threads_;

    /**
     * @brief Phase 1: Partition data by hash(group_key) % num_threads
     */
    Status PartitionPhase(const arrow::Table& input,
                         const std::vector<std::string>& group_by_columns,
                         std::vector<std::shared_ptr<arrow::Table>>& partitions);

    /**
     * @brief Phase 2: Each thread aggregates its partition independently
     */
    Status AggregatePhase(const arrow::Table& partition,
                         const std::vector<std::string>& group_by_columns,
                         const std::vector<AggregateOperator::AggregateFunction>& aggregates,
                         std::unordered_map<std::string, std::vector<int64_t>>& local_state);

    /**
     * @brief Phase 3: Merge thread-local aggregation results
     */
    Status MergePhase(const std::vector<std::unordered_map<std::string, std::vector<int64_t>>>& states,
                     const std::vector<std::string>& group_by_columns,
                     const std::vector<AggregateOperator::AggregateFunction>& aggregates,
                     std::shared_ptr<arrow::Table>* result);
};

} // namespace marble
```

**Partition Phase (Hash-based Data Distribution):**

```cpp
Status ParallelHashAggregator::PartitionPhase(
    const arrow::Table& input,
    const std::vector<std::string>& group_by_columns,
    std::vector<std::shared_ptr<arrow::Table>>& partitions) {

    partitions.resize(num_threads_);

    // Get group-by column
    auto group_col = input.GetColumnByName(group_by_columns[0]);
    auto int64_array = std::static_pointer_cast<arrow::Int64Array>(group_col->chunk(0));

    // Hash each row and assign to partition
    std::vector<std::vector<int64_t>> partition_indices(num_threads_);
    for (int64_t i = 0; i < int64_array->length(); ++i) {
        int64_t value = int64_array->Value(i);
        size_t partition_id = std::hash<int64_t>{}(value) % num_threads_;
        partition_indices[partition_id].push_back(i);
    }

    // Create partitioned tables
    for (size_t p = 0; p < num_threads_; ++p) {
        auto indices_array = arrow::Int64Array::Make(partition_indices[p]);
        ARROW_ASSIGN_OR_RAISE(partitions[p], arrow::compute::Take(input, indices_array));
    }

    return Status::OK();
}
```

**Aggregate Phase (Parallel Execution):**

```cpp
Status ParallelHashAggregator::Execute(
    const arrow::Table& input,
    const std::vector<std::string>& group_by_columns,
    const std::vector<AggregateOperator::AggregateFunction>& aggregates,
    std::shared_ptr<arrow::Table>* result) {

    // Phase 1: Partition
    std::vector<std::shared_ptr<arrow::Table>> partitions;
    MARBLE_RETURN_IF_ERROR(PartitionPhase(input, group_by_columns, partitions));

    // Phase 2: Parallel aggregation
    std::vector<std::unordered_map<std::string, std::vector<int64_t>>> thread_states(num_threads_);
    std::vector<std::thread> threads;

    for (size_t t = 0; t < num_threads_; ++t) {
        threads.emplace_back([&, t]() {
            AggregatePhase(*partitions[t], group_by_columns, aggregates, thread_states[t]);
        });
    }

    // Wait for all threads
    for (auto& thread : threads) {
        thread.join();
    }

    // Phase 3: Merge results
    MARBLE_RETURN_IF_ERROR(MergePhase(thread_states, group_by_columns, aggregates, result));

    return Status::OK();
}
```

**Performance Impact:**
- GROUP BY with 4 threads: **3-4× faster**
- GROUP BY with 8 threads: **5-7× faster**
- `SELECT category, SUM(amount) FROM transactions GROUP BY category`: 500ms → **70-150ms**

**Effort Estimate:** 4-5 steps
- Step 1: Implement partition phase (hash-based)
- Step 2: Implement parallel aggregate phase
- Step 3: Implement merge phase
- Step 4: Integrate with AggregateOperator
- Step 5: Tests and benchmarks

**Files to Create:**
- `include/marble/compute/parallel_aggregator.h`
- `src/compute/parallel_aggregator.cpp`
- `tests/unit/test_parallel_aggregator.cpp`
- `benchmarks/parallel_aggregate_bench.cpp`

---

#### 3.3 Materialized Views with Automatic Refresh

**Use Case:** Pre-compute expensive aggregations, refresh incrementally.

**Implementation:**

```cpp
// include/marble/query/materialized_view.h
#pragma once
#include <arrow/api.h>
#include <marble/status.h>
#include <marble/db.h>

namespace marble {

class MaterializedView {
public:
    MaterializedView(const std::string& view_name,
                    const std::string& sql_query,
                    Database* db);

    /**
     * @brief Create materialized view
     */
    Status Create();

    /**
     * @brief Refresh materialized view (full recomputation)
     */
    Status FullRefresh();

    /**
     * @brief Incremental refresh (apply deltas since last refresh)
     */
    Status IncrementalRefresh(const arrow::RecordBatch& new_data);

    /**
     * @brief Query materialized view
     */
    Status Query(const ScanSpec& spec,
                std::shared_ptr<arrow::Table>* result);

    /**
     * @brief Get last refresh timestamp
     */
    uint64_t GetLastRefreshTime() const { return last_refresh_time_; }

private:
    std::string view_name_;
    std::string sql_query_;
    Database* db_;

    std::shared_ptr<arrow::Table> cached_result_;
    uint64_t last_refresh_time_ = 0;

    Status ExecuteQuery(std::shared_ptr<arrow::Table>* result);
};

/**
 * @brief Materialized View Manager
 */
class MaterializedViewManager {
public:
    explicit MaterializedViewManager(Database* db);

    /**
     * @brief Create new materialized view
     */
    Status CreateView(const std::string& view_name,
                     const std::string& sql_query);

    /**
     * @brief Drop materialized view
     */
    Status DropView(const std::string& view_name);

    /**
     * @brief Get materialized view
     */
    Status GetView(const std::string& view_name,
                  std::shared_ptr<MaterializedView>* view);

    /**
     * @brief Refresh all views
     */
    Status RefreshAll();

    /**
     * @brief Start background refresh thread
     */
    void StartBackgroundRefresh(uint64_t interval_seconds);

    /**
     * @brief Stop background refresh thread
     */
    void StopBackgroundRefresh();

private:
    Database* db_;
    std::unordered_map<std::string, std::shared_ptr<MaterializedView>> views_;

    std::thread background_refresher_;
    std::atomic<bool> stop_refresher_{false};
    uint64_t refresh_interval_seconds_ = 300;  // 5 minutes default

    void BackgroundRefreshThread();
};

} // namespace marble
```

**Example Usage:**

```cpp
// Create materialized view
auto mv_manager = db->GetMaterializedViewManager();
mv_manager->CreateView("daily_sales",
    "SELECT date, SUM(amount) as total_sales "
    "FROM transactions "
    "GROUP BY date");

// Query materialized view (instant, no aggregation)
std::shared_ptr<MaterializedView> view;
mv_manager->GetView("daily_sales", &view);

ScanSpec spec;
spec.filter = "date >= '2025-01-01'";
std::shared_ptr<arrow::Table> result;
view->Query(spec, &result);  // <1ms (metadata lookup instead of aggregation)

// Automatic refresh every 5 minutes
mv_manager->StartBackgroundRefresh(300);
```

**Performance Impact:**
- Pre-aggregated queries: **100-1000× faster** (metadata lookup vs full scan + aggregate)
- `SELECT date, SUM(amount) FROM transactions GROUP BY date`: 10 seconds → **10 milliseconds**

**Effort Estimate:** 5-6 steps
- Step 1: Implement MaterializedView class
- Step 2: Implement full refresh
- Step 3: Implement incremental refresh
- Step 4: Implement MaterializedViewManager
- Step 5: Add background refresh thread
- Step 6: Tests and benchmarks

**Files to Create:**
- `include/marble/query/materialized_view.h`
- `src/query/materialized_view.cpp`
- `tests/unit/test_materialized_view.cpp`

---

#### 3.4 Dictionary Encoding for Low-Cardinality Columns

**Problem:** String columns with repeated values (e.g., "USD", "EUR", "GBP") waste storage and are slow to process.

**Solution:** Encode strings as integers referencing a dictionary.

**Arrow Native Support:** Arrow has `arrow::DictionaryType` built-in.

**Implementation:**

```cpp
// include/marble/storage/dictionary_encoder.h
#pragma once
#include <arrow/api.h>
#include <marble/status.h>

namespace marble {

class DictionaryEncoder {
public:
    /**
     * @brief Encode string column to dictionary-encoded column
     */
    static arrow::Result<std::shared_ptr<arrow::Array>>
    Encode(const arrow::StringArray& input);

    /**
     * @brief Decode dictionary-encoded column back to strings
     */
    static arrow::Result<std::shared_ptr<arrow::Array>>
    Decode(const arrow::DictionaryArray& input);

    /**
     * @brief Check if column is worth dictionary encoding
     */
    static bool ShouldEncode(const arrow::StringArray& input,
                           double cardinality_threshold = 0.1);

private:
    static arrow::Result<std::shared_ptr<arrow::DictionaryArray>>
    BuildDictionary(const arrow::StringArray& input);
};

} // namespace marble
```

**Example:**

```cpp
// Before: String array
// ["USD", "EUR", "USD", "GBP", "USD", "EUR"]
// Storage: 6 strings × 3 bytes = 18 bytes

// After: Dictionary encoding
// Dictionary: ["USD", "EUR", "GBP"]  (3 strings)
// Indices: [0, 1, 0, 2, 0, 1]  (6 integers)
// Storage: 3 strings + 6 ints = 9 + 24 = 33 bytes
// But with compression: ~10 bytes (integers compress very well)

auto string_array = ...; // Input string array
auto dict_result = DictionaryEncoder::Encode(*string_array);
auto dict_array = dict_result.ValueOrDie();

// Dictionary encoding is transparent to Arrow Compute
auto sum_result = arrow::compute::Sum(dict_array); // Works on integers!
```

**Performance Impact:**
- Storage: **3-10× less** for low-cardinality columns (< 10% unique values)
- Aggregations: **2-3× faster** (integer operations vs string comparisons)
- Joins: **3-5× faster** (hash integers instead of strings)

**Effort Estimate:** 2-3 steps
- Step 1: Implement DictionaryEncoder class
- Step 2: Integrate with table write path
- Step 3: Tests and benchmarks

**Files to Create:**
- `include/marble/storage/dictionary_encoder.h`
- `src/storage/dictionary_encoder.cpp`
- `tests/unit/test_dictionary_encoder.cpp`

---

#### 3.5 Arrow Compute Integration

**Problem:** MarbleDB implements operators manually instead of using Arrow Compute's optimized functions.

**Solution:** Use Arrow Compute for filters, aggregations, and other operations (already SIMD-optimized).

**Arrow Compute Functions:**
- `arrow::compute::Filter` - SIMD-vectorized filtering
- `arrow::compute::Sum`, `Mean`, `Min`, `Max` - SIMD aggregations
- `arrow::compute::GroupBy` - Hash-based GROUP BY
- `arrow::compute::Sort` - Parallel sorting
- `arrow::compute::HashJoin` - Built-in hash join (might exist!)

**Implementation:**

```cpp
// include/marble/compute/arrow_compute_wrapper.h
#pragma once
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <marble/status.h>

namespace marble {

class ArrowComputeWrapper {
public:
    /**
     * @brief Filter table using Arrow Compute
     */
    static arrow::Result<std::shared_ptr<arrow::Table>>
    Filter(const arrow::Table& input,
          const std::string& filter_expr);

    /**
     * @brief Aggregate using Arrow Compute
     */
    static arrow::Result<arrow::Datum>
    Aggregate(const arrow::Table& input,
             const std::string& column,
             const std::string& function);  // "sum", "mean", "min", "max"

    /**
     * @brief Sort using Arrow Compute
     */
    static arrow::Result<std::shared_ptr<arrow::Table>>
    Sort(const arrow::Table& input,
        const std::vector<std::string>& sort_columns,
        const std::vector<bool>& ascending);

    /**
     * @brief GROUP BY using Arrow Compute
     */
    static arrow::Result<std::shared_ptr<arrow::Table>>
    GroupBy(const arrow::Table& input,
           const std::vector<std::string>& group_by_columns,
           const std::vector<std::pair<std::string, std::string>>& aggregates);
};

} // namespace marble
```

**Example (Filter with Arrow Compute):**

```cpp
#include <arrow/compute/api.h>

arrow::Result<std::shared_ptr<arrow::Table>>
ArrowComputeWrapper::Filter(const arrow::Table& input,
                           const std::string& filter_expr) {
    // Parse filter expression to Arrow Compute expression
    // E.g., "amount > 1000" → arrow::compute::Expression

    arrow::compute::ExecContext ctx;
    auto filter_result = arrow::compute::CallFunction("filter", {input.ToRecordBatch(), filter_expr}, &ctx);

    if (!filter_result.ok()) {
        return arrow::Status::ExecutionError("Filter failed");
    }

    return arrow::Table::FromRecordBatches(filter_result.ValueOrDie().record_batch());
}
```

**Performance Impact:**
- Filters: **2-3× faster** (Arrow Compute uses SIMD)
- Aggregations: **3-5× faster** (pre-optimized kernels)
- Sorting: **2-4× faster** (parallel sorting)

**Effort Estimate:** 2-3 steps
- Step 1: Wrap Arrow Compute functions
- Step 2: Integrate with FilterOperator, AggregateOperator
- Step 3: Benchmarks comparing manual vs Arrow Compute

**Files to Create:**
- `include/marble/compute/arrow_compute_wrapper.h`
- `src/compute/arrow_compute_wrapper.cpp`
- `tests/unit/test_arrow_compute_wrapper.cpp`

---

## Recommended Implementation Order

### Phase 1: Quick Wins (Weeks 1-2)

**Priority:** High-impact, low-effort improvements

1. **Sorted blocks for point lookups** (OLTP, 1-2 steps)
   - Modify `src/storage/block.cpp` to sort records during write
   - Replace linear scan with binary search in `Block::Get()`
   - **Impact:** Cold lookups 20-50 μs → 10-15 μs (2-5× faster)

2. **Negative cache** (OLTP, 1-2 steps)
   - Create `NegativeCache` class using existing bloom filter
   - Integrate with `DB::Get()` to check before storage lookup
   - **Impact:** Missing key lookups 20-50 μs → 2-5 μs (10-25× faster)

3. **Arrow Compute integration** (OLAP, 2-3 steps)
   - Wrap `arrow::compute::Filter`, `Sum`, `Mean` functions
   - Integrate with `FilterOperator` and `AggregateOperator`
   - **Impact:** Filters and aggregations 2-3× faster

**Expected Results After Phase 1:**
- Point lookups competitive with Tonbo
- Aggregations 2-3× faster
- Foundation for advanced features

---

### Phase 2: Join Implementations (Weeks 3-5)

**Priority:** Critical missing feature

4. **Hash join** (3-5 steps)
   - Implement `ArrowHashJoin` class (build + probe phases)
   - Support inner, left, right, full outer joins
   - Integrate with execution engine
   - **Impact:** Enable JOIN queries (currently not possible)

5. **Merge join** (2-4 steps)
   - Implement `ArrowMergeJoin` for sorted data
   - Optimize for time-series joins
   - **Impact:** 10-20× faster joins for sorted data

6. **Join operator integration** (2-3 steps)
   - Add `JoinOperator` to execution engine
   - Implement join algorithm selection
   - Connect to query planner
   - **Impact:** Full JOIN support in SQL queries

**Expected Results After Phase 2:**
- MarbleDB can execute JOIN queries
- 1M × 1M join in 2-5 seconds
- Competitive with analytical databases

---

### Phase 3: Parallel Execution (Weeks 6-8)

**Priority:** Dramatic OLAP performance improvement

7. **SIMD aggregations** (3-4 steps)
   - Implement SUM, COUNT, MIN, MAX with AVX-512/NEON
   - Integrate with `AggregateOperator`
   - **Impact:** Aggregations 5-8× faster

8. **Parallel hash aggregation** (4-5 steps)
   - Implement partition → aggregate → merge pipeline
   - Multi-threaded GROUP BY execution
   - **Impact:** GROUP BY 3-7× faster with multiple cores

9. **Dictionary encoding** (2-3 steps)
   - Implement `DictionaryEncoder` using Arrow's built-in support
   - Auto-detect low-cardinality columns
   - **Impact:** 3-10× storage savings, 2-3× faster aggregations

**Expected Results After Phase 3:**
- Analytical queries 5-10× faster
- Multi-core CPU utilization
- Storage efficiency improved

---

### Phase 4: Advanced Features (Weeks 9-12)

**Priority:** Production-ready features

10. **Materialized views** (5-6 steps)
    - Implement `MaterializedView` class
    - Full and incremental refresh
    - Background auto-refresh
    - **Impact:** Pre-aggregated queries 100-1000× faster

11. **Broadcast join** (4-6 steps)
    - Implement distributed broadcast via Arrow Flight
    - Local hash join on each node
    - Result merging
    - **Impact:** Distributed joins 10× faster for small tables

12. **WAL batching** (2-3 steps)
    - Implement `BatchedWAL` class
    - Background flusher thread
    - **Impact:** Write throughput 2.5-5× faster

**Expected Results After Phase 4:**
- Production-ready analytical database
- Distributed query support
- High write throughput

---

## Expected Performance Improvements

### OLTP (Point Lookups, Writes)

| Metric | Before | After Phase 1 | After Phase 4 | Total Improvement |
|--------|--------|--------------|--------------|-------------------|
| **Hot lookups** | 5-10 μs | 5-10 μs | 5-10 μs | **Same** |
| **Cold lookups** | 20-50 μs | **10-15 μs** | **10-15 μs** | **2-5× faster** |
| **Missing keys** | 20-50 μs | **2-5 μs** | **2-5 μs** | **10-25× faster** |
| **Write throughput** | 4M rows/sec | 4M rows/sec | **10-20M rows/sec** | **2.5-5× faster** |

---

### OLAP (Analytical Queries)

| Query Type | Before | After Phase 1 | After Phase 3 | Total Improvement |
|------------|--------|--------------|--------------|-------------------|
| **Full scan** | 20-50M rows/s | 20-50M rows/s | 20-50M rows/s | **Same** |
| **Filtered scan** | 20-50M rows/s | **50-80M rows/s** | **100M rows/s** | **2-5× faster** |
| **SUM/COUNT** | 100ms | **30-50ms** | **12-20ms** | **5-8× faster** |
| **GROUP BY** | 500ms | 500ms | **70-150ms** | **3-7× faster** |
| **JOIN (1M × 1M)** | **N/A** | **N/A** | **2-5 seconds** | **NEW** |
| **Materialized views** | **N/A** | **N/A** | **<10ms** | **100-1000× faster** |

---

### Arrow-Specific Advantages

**Zero-Copy Operations:**
- Joins pass RecordBatch pointers instead of copying data
- Filters produce sliced arrays (no memory allocation)
- Projections create views into existing arrays

**SIMD Everywhere:**
- Arrow Compute uses AVX2/AVX-512/NEON internally
- Hash functions, comparisons, aggregations all vectorized
- 5-8× speedup for numeric operations

**Columnar Efficiency:**
- Process one column at a time (cache-friendly)
- Skip columns not needed (projection pushdown)
- Dictionary encoding for strings (integers are faster)

**Interoperability:**
- Direct integration with PyArrow, Pandas, DuckDB
- Arrow Flight for zero-copy network transfer
- Arrow IPC for zero-copy disk I/O

---

## Why This Matters

### Current MarbleDB (Before Proposal)

✅ **Strengths:**
- Excellent storage layer (Arrow, LSM, zone maps, bloom filters)
- Good OLTP features (merge operators, column families, zero-copy RecordRef)
- Fast analytical scans (20-50M rows/sec)

❌ **Weaknesses:**
- **No joins** - Cannot run most analytical queries
- **No SIMD** - Not leveraging Arrow's strengths
- **Single-threaded aggregations** - Wasting CPU cores
- Point lookups 2× slower than Tonbo

---

### MarbleDB After This Proposal

✅ **New Capabilities:**
- **Full JOIN support** (hash, merge, broadcast) - NEW FEATURE
- **SIMD-accelerated aggregations** - 5-8× faster
- **Parallel GROUP BY** - 3-7× faster
- **Competitive OLTP** - 10 μs cold lookups (was 20-50 μs)
- **True Arrow-native** - Leveraging Arrow Compute library
- **Materialized views** - 100-1000× faster pre-aggregated queries

**Result:** MarbleDB becomes a **production-ready analytical database** that's fast for both OLTP and OLAP, with best-in-class join performance thanks to Arrow's zero-copy format.

---

## Architecture Impact

### Before: Storage-Focused Database

```
┌────────────────────────────────────────┐
│         MarbleDB (Before)               │
├────────────────────────────────────────┤
│                                         │
│  ✅ Arrow Storage                       │
│  ✅ LSM-tree with zone maps            │
│  ✅ Bloom filters                       │
│  ✅ Raft consensus                      │
│  ✅ Column families                     │
│                                         │
│  ❌ No joins                            │
│  ❌ No SIMD                             │
│  ❌ Single-threaded GROUP BY            │
│                                         │
└────────────────────────────────────────┘
```

---

### After: Full Analytical Database

```
┌────────────────────────────────────────────────────────┐
│              MarbleDB (After Proposal)                  │
├────────────────────────────────────────────────────────┤
│                                                         │
│  🚀 JOIN LAYER (NEW)                                    │
│     ├─ Hash Join (Arrow native, zero-copy)             │
│     ├─ Merge Join (sorted data, O(N+M))                │
│     └─ Broadcast Join (distributed, Flight)            │
│                                                         │
│  🚀 SIMD EXECUTION (NEW)                                │
│     ├─ SIMD aggregations (AVX-512/NEON)                │
│     ├─ Parallel GROUP BY (multi-threaded)              │
│     └─ Arrow Compute integration                       │
│                                                         │
│  🚀 ADVANCED FEATURES (NEW)                             │
│     ├─ Materialized views (auto-refresh)               │
│     ├─ Dictionary encoding (low-cardinality)           │
│     └─ WAL batching (high write throughput)            │
│                                                         │
│  ✅ STORAGE (Enhanced)                                  │
│     ├─ Arrow Storage                                   │
│     ├─ Sorted blocks (binary search)                   │
│     ├─ Negative cache (bloom filter)                   │
│     ├─ Zone maps & bloom filters                       │
│     └─ Hot key cache                                   │
│                                                         │
│  ✅ DISTRIBUTED (Existing)                              │
│     ├─ Raft consensus                                  │
│     ├─ Arrow Flight WAL                                │
│     └─ Column families                                 │
│                                                         │
└────────────────────────────────────────────────────────┘
```

---

## Success Metrics

### After Phase 1 (Quick Wins)
- [ ] Cold point lookups: < 15 μs (currently 20-50 μs)
- [ ] Missing key lookups: < 5 μs (currently 20-50 μs)
- [ ] Aggregations: 2-3× faster with Arrow Compute

### After Phase 2 (Joins)
- [ ] JOIN queries functional (currently N/A)
- [ ] 1M × 1M hash join: < 5 seconds
- [ ] 1M × 1M merge join (sorted): < 2 seconds

### After Phase 3 (Parallel Execution)
- [ ] SIMD aggregations: 5-8× faster
- [ ] GROUP BY with 4 threads: 3-4× faster
- [ ] Dictionary encoding: 3-10× storage savings

### After Phase 4 (Production Ready)
- [ ] Materialized views: < 10ms pre-aggregated queries
- [ ] Distributed broadcast join: < 10 seconds (100M rows)
- [ ] Write throughput: 10-20M rows/sec

---

## Risks and Mitigation

### Risk 1: Complexity of Join Implementations

**Risk:** Joins are complex, especially with multiple join types (inner, left, right, full).

**Mitigation:**
- Start with inner join only (simplest)
- Use Arrow Compute's `HashJoin` function if available
- Extensive unit tests with known correct outputs
- Reference implementation from DuckDB

### Risk 2: SIMD Portability

**Risk:** AVX-512 not available on all systems, NEON only on ARM.

**Mitigation:**
- Provide fallback scalar implementations
- Runtime CPU feature detection
- Use Arrow Compute as fallback (already has SIMD)

### Risk 3: Parallel Execution Overhead

**Risk:** Thread spawning overhead may negate benefits for small datasets.

**Mitigation:**
- Only use parallelism for large datasets (> 100K rows)
- Measure overhead and adjust thresholds
- Use thread pool (reuse threads)

### Risk 4: Arrow Compute API Stability

**Risk:** Arrow Compute API may change between versions.

**Mitigation:**
- Pin to specific Arrow version (15.0+)
- Wrap Arrow Compute calls in abstraction layer
- Document Arrow version requirements

---

## Next Steps

1. **Review and approve this proposal**
2. **Create GitHub issues** for each implementation step
3. **Start Phase 1 (Quick Wins)** - 1-2 weeks
4. **Validate performance improvements** with benchmarks
5. **Proceed to Phase 2 (Joins)** - 3-5 weeks

---

**Document Status:** ✅ Approved Proposal
**Next Review:** After Phase 1 completion
**Target Completion:** 12 weeks (3 months)
