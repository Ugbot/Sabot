# Storage Techniques - Quick Reference

**For detailed information, see:** `advanced_storage_techniques.md`

---

## Top 5 Techniques (Immediate Impact)

### 1. Searchable MemTable ⭐⭐⭐ USER SUGGESTION
**What:** In-memory indexes (hash + B-tree) on MemTable
**Benefit:** 10-100x faster queries on recent data
**Cost:** 50% memory overhead, +10-20μs write latency
**Implementation:** `memtable.h`, add `InMemoryIndex` class

```cpp
// Query MemTable directly
memtable->SearchByIndex("user_id_idx", eq_query, &results);  // O(1) hash
memtable->SearchByIndex("timestamp_idx", range_query, &results);  // O(log n)
```

### 2. Lazy Index Building ⭐⭐⭐ USER SUGGESTION
**What:** Build indexes on-demand or in background
**Benefit:** Non-blocking index creation
**Cost:** Initial queries slower, hybrid query execution
**Implementation:** `lazy_index_builder.h`

```cpp
// Index builds automatically on first query
auto status = lazy_index_builder_->BuildOnDemand(index_name);
// Query uses partial index + scan fallback
```

### 3. Large Block Writes ⭐⭐⭐
**What:** 8MiB write blocks, 128KB flush size
**Benefit:** 2-3x write throughput
**Cost:** None (pure win)
**Implementation:** `lsm_storage.h`, change config

```cpp
config.write_block_size_mb = 8;
config.flush_size_kb = 128;  // Optimal for NVMe
```

### 4. Background Defragmentation ⭐⭐⭐
**What:** Continuous space reclamation
**Benefit:** 30-50% storage savings
**Cost:** 1-3% CPU overhead
**Implementation:** `defragmentation.cpp`, background thread

```cpp
config.defrag_threshold = 0.40;  // Rewrite if <40% live data
```

### 5. Write Buffer Back-Pressure ⭐⭐⭐
**What:** Memory limits with back-pressure
**Benefit:** No OOM crashes, predictable memory
**Cost:** Write latency under pressure
**Implementation:** `write_buffer_manager.cpp`

```cpp
config.max_write_buffer_mb = 256;
config.strategy = WriteBufferConfig::kSlowWrites;
```

---

## Configuration Cheat Sheet

### Real-Time Analytics
```cpp
LSMTreeConfig config;
// Searchable MemTable with eager indexes
config.memtable_config.indexes = {
    {"user_id_idx", {"user_id"}, IndexConfig::kEager},
    {"timestamp_idx", {"timestamp"}, IndexConfig::kEager}
};
config.memtable_config.enable_hash_index = true;
config.memtable_config.enable_btree_index = true;

// Large writes
config.write_block_size_mb = 8;
config.flush_size_kb = 128;

// Aggressive defrag
config.defrag_config.defrag_threshold = 0.50;
config.defrag_config.defrag_interval_sec = 60;

// Write buffer
config.write_buffer_config.max_write_buffer_mb = 512;
config.write_buffer_config.strategy = WriteBufferConfig::kSlowWrites;
```

### Time-Series Storage
```cpp
LSMTreeConfig config;
// Lazy indexes (queried by time range)
config.memtable_config.indexes = {
    {"timestamp_idx", {"timestamp"}, IndexConfig::kEager},
    {"device_id_idx", {"device_id"}, IndexConfig::kBackground}
};

// Large writes for streaming
config.write_block_size_mb = 16;
config.flush_size_kb = 256;

// Aggressive defrag (old data deleted)
config.defrag_config.defrag_threshold = 0.40;
```

### OLTP (Transactional)
```cpp
LSMTreeConfig config;
// Searchable MemTable with hot keys
config.memtable_config.indexes = {
    {"primary_key_idx", {"id"}, IndexConfig::kEager},
    {"user_id_idx", {"user_id"}, IndexConfig::kEager}
};
config.memtable_config.enable_hash_index = true;  // Fast equality

// Moderate writes
config.write_block_size_mb = 4;
config.flush_size_kb = 64;

// Conservative defrag
config.defrag_config.defrag_threshold = 0.30;
config.defrag_config.defrag_interval_sec = 300;
```

---

## Index Strategy Decision Tree

```
Is data queried within seconds of write?
├─ YES → Use Searchable MemTable (in-memory indexes)
│         + Immediate query availability
│         + 10-100x faster than scan
│         - 50% memory overhead
│
└─ NO → Use Lazy SSTable Indexes
          ├─ Known query patterns?
          │  ├─ YES → Eager build (kEager)
          │  └─ NO → Lazy build (kLazy)
          │
          └─ Large dataset?
             ├─ YES → Background build (kBackground)
             └─ NO → Eager build (kEager)
```

---

## Performance Targets

| Technique | Baseline | Target | File to Modify |
|-----------|----------|--------|----------------|
| Indexed Query (MemTable) | 50K/sec | 1M/sec | `memtable.cpp` |
| SSTable Write | 300K/sec | 900K/sec | `sstable.cpp` |
| Storage Efficiency | 40% live | 70% live | `defragmentation.cpp` |
| Write Buffer OOM | Frequent | Never | `write_buffer_manager.cpp` |

---

## Implementation Order (Phase 1)

### Week 1: Day 1-2
1. **Searchable MemTable** - Highest impact
   - Add `InMemoryIndex` struct to `memtable.h`
   - Implement hash + B-tree in-memory
   - Add `SearchByIndex()` method
   - Update `Put()` to maintain indexes

### Week 1: Day 3-4
2. **Large Block Writes** - Easy win
   - Add config parameters to `LSMTreeConfig`
   - Implement `BufferedSSTableWriter`
   - Test with 128KB flush size

### Week 1: Day 5
3. **Write Buffer Manager** - Stability
   - Create `write_buffer_manager.h/cpp`
   - Implement back-pressure strategies
   - Integrate with LSM Tree `Put()`

### Week 2: Day 1-3
4. **Lazy Index Builder** - Non-blocking
   - Create `lazy_index_builder.h/cpp`
   - Implement priority queue
   - Hybrid query execution

### Week 2: Day 4-5
5. **Background Defragmentation** - Storage savings
   - Create `defragmentation.h/cpp`
   - Implement selection algorithm
   - Background rewrite loop

---

## Monitoring Checklist

### Metrics to Track

**MemTable Indexes:**
```cpp
marble.memtable.index_queries          // Queries using indexes
marble.memtable.index_hits             // Hash/B-tree hits
marble.memtable.index_memory_bytes     // Memory used by indexes
```

**Write Performance:**
```cpp
marble.lsm.write_throughput_mb_sec     // MB/s write rate
marble.lsm.write_block_size_bytes      // Actual block sizes
marble.lsm.write_buffer_backpressure   // Back-pressure events
```

**Defragmentation:**
```cpp
marble.defrag.sstables_processed       // SSTables defragmented
marble.defrag.bytes_reclaimed          // Space reclaimed
marble.defrag.live_data_ratio          // Average live ratio
```

**Lazy Indexes:**
```cpp
marble.index.build_queue_depth         // Pending builds
marble.index.partial_query_fallbacks   // Queries using fallback
marble.index.background_builds         // Active builds
```

---

## Common Pitfalls & Solutions

### Pitfall 1: Index Memory Explosion
**Problem:** MemTable indexes use too much memory
**Solution:** Set `max_index_memory_mb` limit, use lazy indexes for rarely-queried columns

### Pitfall 2: Defrag Thrashing
**Problem:** Defrag runs too aggressively, impacts writes
**Solution:** Increase `defrag_threshold` (e.g., 0.3 → 0.5), reduce `defrag_threads`

### Pitfall 3: Write Buffer OOM
**Problem:** Back-pressure not aggressive enough
**Solution:** Lower `max_write_buffer_mb`, use `kFailWrites` strategy for safety

### Pitfall 4: Lazy Index Never Completes
**Problem:** Background builder starved by high write load
**Solution:** Increase `build_priority`, add more `build_threads`

---

## Testing Checklist

### Unit Tests
- [ ] `SearchableMemTable_HashIndex_EqualityQuery`
- [ ] `SearchableMemTable_BTreeIndex_RangeQuery`
- [ ] `LazyIndexBuilder_BuildOnDemand`
- [ ] `LazyIndexBuilder_PartialIndexQuery`
- [ ] `BackgroundDefragmenter_SelectCandidates`
- [ ] `BackgroundDefragmenter_RewriteSSTable`
- [ ] `WriteBufferManager_BackPressure`
- [ ] `BufferedSSTableWriter_LargeBlocks`

### Integration Tests
- [ ] End-to-end query with MemTable indexes
- [ ] Lazy index build during active writes
- [ ] Defragmentation with concurrent writes
- [ ] Write buffer back-pressure under load

### Benchmark Tests
- [ ] MemTable indexed query throughput
- [ ] SSTable write throughput (large blocks)
- [ ] Storage efficiency after defrag
- [ ] Write buffer stability under stress

---

## Quick Start

### 1. Enable Searchable MemTable
```cpp
#include "marble/lsm_storage.h"

LSMTreeConfig config;
config.memtable_config.indexes = {
    {"user_id_idx", {"user_id"}, IndexConfig::kEager}
};
config.memtable_config.enable_hash_index = true;

auto lsm = CreateLSMTree();
lsm->Init(config);

// Query
IndexQuery query;
query.type = IndexQuery::kEquality;
query.index_name = "user_id_idx";
query.value = "user_12345";

std::vector<Record> results;
lsm->GetActiveMemTable()->SearchByIndex("user_id_idx", query, &results);
```

### 2. Enable Large Block Writes
```cpp
LSMTreeConfig config;
config.write_block_size_mb = 8;      // 8MiB blocks
config.flush_size_kb = 128;          // 128KB flushes (NVMe)

auto lsm = CreateLSMTree();
lsm->Init(config);
```

### 3. Enable Defragmentation
```cpp
LSMTreeConfig config;
config.defrag_config.enable_defrag = true;
config.defrag_config.defrag_threshold = 0.40;
config.defrag_config.defrag_threads = 1;

auto lsm = CreateLSMTree();
lsm->Init(config);
// Defrag runs automatically in background
```

### 4. Enable Write Buffer Limits
```cpp
LSMTreeConfig config;
config.write_buffer_config.max_write_buffer_mb = 256;
config.write_buffer_config.strategy = WriteBufferConfig::kSlowWrites;

auto lsm = CreateLSMTree();
lsm->Init(config);

// Writes automatically back-pressured when buffer full
```

---

## File Structure Summary

```
MarbleDB/
├── include/marble/
│   ├── lsm_storage.h                  # ADD: Large block write config
│   ├── memtable.h                     # ADD: Searchable MemTable with indexes
│   ├── lazy_index_builder.h          # NEW: Lazy index building
│   ├── defragmentation.h              # NEW: Background defragmentation
│   └── write_buffer_manager.h         # NEW: Write buffer back-pressure
│
├── src/core/
│   ├── lsm_tree.cpp                   # MODIFY: Integrate all techniques
│   ├── memtable.cpp                   # MODIFY: Add index maintenance
│   ├── sstable.cpp                    # MODIFY: Buffered writes
│   ├── lazy_index_builder.cpp         # NEW: Lazy build implementation
│   ├── defragmentation.cpp            # NEW: Defrag loop
│   └── write_buffer_manager.cpp       # NEW: Back-pressure logic
│
└── docs/
    ├── advanced_storage_techniques.md # DONE: Full documentation
    └── storage_techniques_quick_reference.md  # DONE: This file
```

---

## Next Steps

1. ✅ Documentation complete
2. ⏳ Implement Searchable MemTable (highest priority)
3. ⏳ Add config parameters for large block writes
4. ⏳ Implement Write Buffer Manager
5. ⏳ Create benchmark suite

**Estimated time to complete Phase 1:** 2 weeks
**Expected performance improvement:** 2-5x across all operations
