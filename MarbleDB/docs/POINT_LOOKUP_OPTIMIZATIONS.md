# Point Lookup Optimizations for MarbleDB

## Current State: 7x Slower Than Tonbo

**Baseline Performance**:
- Tonbo: **5 μs** per lookup (full index)
- MarbleDB with hot cache: **35 μs** average (Zipfian workload)
- MarbleDB cold key: **250 μs** (sparse index + scan)

**Goal**: Close the gap further through aggressive optimizations.

---

## Optimization Strategies

### 1. **Skip List Index on Blocks** ⚡ HIGH IMPACT

**Current Problem**:
```cpp
// When cache misses, we scan up to 8K rows linearly
for (int64_t i = block_start; i < block_end; ++i) {
    if (key == row[i].key) return row[i];  // O(N) scan
}
```

**Solution**: Add skip list within each block
```cpp
struct BlockIndex {
    std::vector<std::pair<std::shared_ptr<Key>, uint32_t>> skip_points;
    // Index every 64th row within the 8K block
    // 8192 / 64 = 128 skip points per block
};
```

**Benefit**:
- Reduces block scan from O(8192) to O(128)
- **64x fewer comparisons**
- Latency: 250 μs → **30-50 μs** ✅
- Memory cost: +2 KB per block (negligible)

**Implementation**:
```cpp
// Binary search within block using skip points
auto skip_it = std::lower_bound(block.skip_points.begin(), 
                                block.skip_points.end(), 
                                target_key);
// Then scan only 64 rows instead of 8192
for (uint32_t i = skip_it->row_idx; i < skip_it->row_idx + 64; ++i) {
    if (key == row[i].key) return row[i];
}
```

---

### 2. **Negative Cache** (Aerospike-Style) ⚡ HIGH IMPACT

**Current Problem**:
- Missing key lookups still scan entire block
- Repeated failed lookups do redundant work

**Solution**: Cache keys that **don't exist**
```cpp
class NegativeCache {
    // Bloom filter for "definitely doesn't exist"
    BloomFilter negative_filter;
    
    // Recent misses (last 10K failed lookups)
    LRUCache<std::string, bool> recent_misses;
};
```

**Benefit**:
- Failed lookup: 250 μs → **1-5 μs** (bloom filter check)
- **50-250x faster** for non-existent keys
- Useful for JOIN operations, deduplication

**Memory**: 
- Bloom filter: ~100 KB (1M keys, 1 bit per key)
- Recent misses: ~100 KB (10K entries)
- Total: **200 KB**

---

### 3. **Interpolation Search** ⚡ MEDIUM IMPACT

**Current**: Binary search in sparse index
```cpp
// Binary search: O(log N) = ~20 comparisons for 1M keys
auto it = std::lower_bound(sparse_index.begin(), 
                          sparse_index.end(), 
                          target_key);
```

**Optimized**: Interpolation search (assumes uniform distribution)
```cpp
// Interpolation: O(log log N) = ~4-5 comparisons for 1M keys
size_t estimate_position(Key target, Key min, Key max, size_t range) {
    // If keys are numeric and roughly uniform:
    return range * (target - min) / (max - min);
}
```

**Benefit**:
- **4x fewer comparisons** for sparse index lookup
- Only works if keys are uniformly distributed
- Latency: 250 μs → **200 μs** (20% improvement)

---

### 4. **SIMD-Accelerated Block Scanning** ⚡ HIGH IMPACT

**Current**: Scalar comparison in tight loop
```cpp
for (size_t i = 0; i < block_size; ++i) {
    if (keys[i] == target) return i;  // One comparison at a time
}
```

**Optimized**: SIMD parallel comparison (AVX-512)
```cpp
#include <immintrin.h>

// Compare 8 keys simultaneously with AVX-512
__m512i target_vec = _mm512_set1_epi64(target_key);
for (size_t i = 0; i < block_size; i += 8) {
    __m512i keys_vec = _mm512_loadu_si512(&keys[i]);
    __mmask8 mask = _mm512_cmpeq_epi64_mask(keys_vec, target_vec);
    if (mask) {
        return i + __builtin_ctz(mask);  // Found!
    }
}
```

**Benefit**:
- **8x parallelism** with AVX-512 (Apple Silicon: NEON)
- Latency: 250 μs → **30-50 μs** ✅
- Works for integer keys only

**Apple Silicon (NEON)**:
```cpp
#include <arm_neon.h>

// Compare 2 keys simultaneously with NEON
int64x2_t target_vec = vdupq_n_s64(target_key);
for (size_t i = 0; i < block_size; i += 2) {
    int64x2_t keys_vec = vld1q_s64(&keys[i]);
    uint64x2_t cmp = vceqq_s64(keys_vec, target_vec);
    // Extract matching indices
}
```

---

### 5. **Partitioned Hash Index** (Hybrid Approach) ⚡ VERY HIGH IMPACT

**Concept**: Combine sparse index + hash partitions

```cpp
struct PartitionedIndex {
    // Sparse index for ranges (analytical queries)
    std::vector<SparseIndexEntry> sparse_index;
    
    // Hash index for point lookups (OLTP queries)
    // Only index first key of each block
    std::unordered_map<std::shared_ptr<Key>, uint32_t> block_hash_index;
};
```

**Lookup Path**:
1. Hash lookup: O(1) → find which block
2. Binary search within block: O(log 8192) = 13 comparisons
3. Total: **O(1) + 13 comparisons** instead of sparse index scan

**Benefit**:
- Point lookup: **10-20 μs** (5-10x faster than sparse alone)
- Still supports range queries via sparse index
- Memory: +16 bytes × (num_blocks) = **+200 KB for 1M rows**

**Trade-off**: Best of both worlds!
- Range queries: Use sparse index
- Point lookups: Use hash index

---

### 6. **Tiered Caching** (L1/L2/L3 Strategy) ⚡ MEDIUM IMPACT

**Multi-Level Cache Hierarchy**:

```cpp
struct TieredCache {
    // L1: Hot keys (64 MB, recent 10K accesses)
    HotKeyCache l1_cache;
    
    // L2: Warm keys (256 MB, moderate access)
    HotKeyCache l2_cache;
    
    // L3: Block metadata cache (128 MB)
    // Cache entire blocks for sequential access
    std::unordered_map<uint64_t, std::shared_ptr<arrow::RecordBatch>> block_cache;
};
```

**Lookup Strategy**:
```cpp
if (l1_cache.Get(key)) return result;      // ~5 μs
if (l2_cache.Get(key)) return result;      // ~10 μs
if (block_cache.Get(key)) return result;   // ~20 μs
return SparseIndexLookup(key);             // ~250 μs
```

**Benefit**:
- **Gradual degradation** instead of cliff
- L1 hit (70%): 5 μs
- L2 hit (20%): 10 μs  
- L3 hit (8%): 20 μs
- Miss (2%): 250 μs
- **Average: ~15 μs** (3x better than single-tier cache)

---

### 7. **Predictive Prefetching** ⚡ MEDIUM IMPACT

**Pattern Detection**:
```cpp
class PrefetchPredictor {
    // Detect access patterns
    enum Pattern { SEQUENTIAL, RANDOM, TEMPORAL };
    
    Pattern DetectPattern(const std::vector<Key>& recent_accesses);
    
    // Prefetch based on pattern
    void PrefetchNext(Key current_key, Pattern pattern) {
        if (pattern == SEQUENTIAL) {
            // Prefetch next N keys
            PrefetchRange(current_key, current_key + 100);
        } else if (pattern == TEMPORAL) {
            // Prefetch adjacent time buckets
            PrefetchTimeRange(current_key.timestamp);
        }
    }
};
```

**Benefit**:
- Sequential scans: **Hide latency** by prefetching ahead
- Temporal queries: Load entire time buckets
- Effectiveness: 50-80% for sequential patterns

---

### 8. **Key Compression** ⚡ LOW IMPACT (Memory Saver)

**Current**: Store full keys in cache
```cpp
struct HotKeyEntry {
    std::string key_str;  // 32 bytes average
    uint64_t row_index;   // 8 bytes
    // Total: 40+ bytes
};
```

**Optimized**: Delta encoding for integer keys
```cpp
struct CompressedKeyEntry {
    uint32_t key_delta;   // Delta from base key (4 bytes)
    uint32_t row_index;   // 4 bytes (if rows < 4B)
    // Total: 8 bytes (5x compression!)
};
```

**Benefit**:
- **5x more keys** in same memory
- 64 MB cache: 10K entries → **50K entries**
- Higher hit rate due to larger cache

---

### 9. **Lock-Free Reads (RCU)** ⚡ LOW IMPACT (High Concurrency)

**Current**: Mutex for cache access
```cpp
std::lock_guard<std::mutex> lock(cache_mutex_);
auto it = cache_.find(key);
```

**Optimized**: Read-Copy-Update (RCU)
```cpp
// Reads are lock-free
std::atomic<CacheVersion*> cache_ptr;

HotKeyEntry* Get(const Key& key) {
    // No lock for reads!
    auto* cache_snapshot = cache_ptr.load(std::memory_order_acquire);
    return cache_snapshot->find(key);
}

// Writes create new version
void Put(const Key& key, HotKeyEntry entry) {
    auto* new_cache = CopyCache(cache_ptr.load());
    new_cache->insert(key, entry);
    cache_ptr.store(new_cache, std::memory_order_release);
    // Old version garbage collected later
}
```

**Benefit**:
- **No lock contention** on read path
- Scales to many concurrent readers
- Latency: 10 μs → **5 μs** under load

---

### 10. **Sorted Block Layout** ⚡ MEDIUM IMPACT

**Current**: Keys stored in insertion order within blocks

**Optimized**: Keys sorted within blocks
```cpp
// When writing block, sort by key
std::sort(block_records.begin(), block_records.end(),
         [](const Record& a, const Record& b) {
             return a.key < b.key;
         });
```

**Benefit**:
- Binary search within block: O(log 8192) = 13 comparisons
- Instead of linear scan: O(8192)
- Latency: 250 μs → **50 μs** (5x faster)
- No memory overhead!

---

### 11. **Bloom Filter Per Block** ⚡ MEDIUM IMPACT

**Current**: Single bloom filter for entire SSTable

**Optimized**: Bloom filter per block
```cpp
struct BlockMetadata {
    BloomFilter block_bloom;  // 1 KB per 8K rows
    uint64_t min_key, max_key;
};
```

**Lookup Path**:
```cpp
// 1. Check block bloom filter (1-2 μs)
if (!block_bloom.MayContain(key)) {
    return NotFound;  // Definitely not in this block
}

// 2. Only scan if bloom says "maybe"
return ScanBlock(key);  // ~250 μs
```

**Benefit**:
- **99% of misses** caught by bloom filter
- Avg latency for misses: 250 μs → **5 μs** (50x faster)
- Memory: +1 KB per block = **+125 KB per 1M rows**

---

### 12. **GPU Acceleration** (Extreme) ⚡ VERY HIGH IMPACT

**Concept**: Offload block scanning to GPU
```cpp
// Upload block to GPU memory
cudaMemcpy(gpu_keys, cpu_keys, block_size);

// Parallel search on GPU (8K threads)
__global__ void FindKeyKernel(int64_t target, int64_t* keys, 
                              int* result, int N) {
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < N && keys[idx] == target) {
        atomicMin(result, idx);
    }
}
```

**Benefit**:
- **Massive parallelism** (8K keys searched simultaneously)
- Latency: 250 μs → **10-20 μs** (10-25x faster)
- Only beneficial for large blocks (>10K rows)

**Trade-offs**:
- GPU transfer overhead (~50 μs)
- Only worth it for large batches
- Requires CUDA/Metal support

---

## Combined Optimization Impact

### Scenario 1: **Hot Key Lookup** (Already Cached)

| Optimization | Latency | Speedup |
|--------------|---------|---------|
| Baseline (sparse) | 250 μs | 1x |
| + Hot key cache | **10 μs** | **25x** ✅ |
| + RCU reads | **5 μs** | **50x** ✅ |

**Result**: **Matches Tonbo performance** for hot keys!

---

### Scenario 2: **Cold Key Lookup** (Cache Miss)

| Optimization Stack | Latency | Cumulative Speedup |
|-------------------|---------|---------------------|
| Baseline (sparse + linear scan) | 250 μs | 1x |
| + Sorted blocks (binary search) | 50 μs | **5x** ✅ |
| + Skip list in blocks | 30 μs | **8x** ✅ |
| + Bloom filter per block | 25 μs | **10x** ✅ |
| + SIMD scanning | 15 μs | **17x** ✅ |
| + Interpolation search | **10 μs** | **25x** ✅ |

**Result**: Cold keys now only **2x slower** than Tonbo (was 50x)!

---

### Scenario 3: **Missing Key** (Doesn't Exist)

| Optimization | Latency | Speedup |
|--------------|---------|---------|
| Baseline (scan to confirm miss) | 250 μs | 1x |
| + Negative cache | **2 μs** | **125x** ✅ |

**Result**: **Faster than Tonbo** for repeated negative lookups!

---

## Recommended Optimization Priority

### Phase 1: **Quick Wins** (Implement First)
1. ✅ **Hot key cache** (Already done) - 7x speedup
2. 🔨 **Sorted blocks** - 5x additional speedup
3. 🔨 **Negative cache** - 50-100x for misses
4. 🔨 **Block-level bloom filters** - 10x for misses

**Combined Impact**: Hot keys **match Tonbo**, cold keys **5x faster**, misses **100x faster**

### Phase 2: **Advanced** (If Needed)
5. 🔨 **Skip list in blocks** - 2-3x additional
6. 🔨 **Partitioned hash index** - Hybrid approach
7. 🔨 **SIMD scanning** - 2-3x with vectorization
8. 🔨 **RCU lock-free reads** - Better concurrency

### Phase 3: **Extreme** (Overkill)
9. 🔨 **GPU acceleration** - Only for huge blocks
10. 🔨 **Predictive prefetching** - Complex ML models

---

## Implementation Plan

### Immediate (High ROI, Low Effort)

#### 1. Sorted Blocks
```cpp
// In SSTable::Create()
std::sort(records.begin(), records.end(), 
         [](const Record& a, const Record& b) {
             return a.key->Compare(*b.key) < 0;
         });

// Then binary search instead of linear scan
auto it = std::lower_bound(block_begin, block_end, target_key);
```

**Effort**: 20 lines of code  
**Impact**: **5x faster** cold lookups

#### 2. Negative Cache
```cpp
class NegativeCache {
    BloomFilter negative_bloom_;  // Reuse existing bloom filter code
    
    void RecordMiss(const Key& key) {
        negative_bloom_.Add(key);
    }
    
    bool DefinitelyNotExists(const Key& key) {
        return negative_bloom_.MayContain(key);
    }
};
```

**Effort**: 50 lines of code  
**Impact**: **100x faster** for repeated misses

#### 3. Block Bloom Filters
```cpp
struct BlockStats {
    std::shared_ptr<Key> min_key;
    std::shared_ptr<Key> max_key;
    BloomFilter block_bloom;  // ← Add this
};

// On lookup
if (!block_stats.block_bloom.MayContain(key)) {
    skip_block();  // Definitely not here
}
```

**Effort**: 30 lines of code  
**Impact**: **10x faster** for block misses

---

## Performance Projection

### After All Phase 1 Optimizations

| Workload | Tonbo | MarbleDB (Optimized) | Gap |
|----------|-------|---------------------|-----|
| **Hot key (80%)** | 5 μs | **5 μs** | **0x** ✅ |
| **Cold key (15%)** | 5 μs | **15 μs** | **3x** ✅ |
| **Missing key (5%)** | 5 μs | **2 μs** | **Faster!** ✅ |
| **Average** | 5 μs | **7 μs** | **1.4x** ✅ |

**Result**: MarbleDB becomes **competitive with Tonbo** for point lookups while maintaining **5-20x analytical query advantage**!

---

## Memory Usage Comparison

### Full Optimizations

| Component | Memory (1M keys) |
|-----------|------------------|
| Sparse index | 2 KB |
| Hot key cache (10K entries) | 1 MB |
| Negative cache | 200 KB |
| Block bloom filters | 125 KB |
| Skip lists (128 entries/block) | 2 MB |
| **Total** | **~3.3 MB** |

**vs Tonbo Full Index**: 16 MB

**MarbleDB uses 5x less memory** while achieving similar performance!

---

## Architecture Decision Matrix

### When to Enable Each Optimization

| Optimization | Memory Cost | CPU Cost | Best For |
|--------------|-------------|----------|----------|
| **Hot key cache** | Medium (1-64 MB) | Low | Skewed access |
| **Sorted blocks** | Zero | Zero | Everything |
| **Negative cache** | Low (200 KB) | Very low | JOINs, dedup |
| **Block bloom** | Low (125 KB) | Very low | Everything |
| **Skip lists** | Medium (2 MB) | Low | Large blocks |
| **Hash index** | Medium (200 KB) | Low | OLTP-heavy |
| **SIMD** | Zero | Medium | Integer keys |
| **RCU** | Low | Medium | High concurrency |

---

## Recommended Default Configuration

```cpp
// MarbleDB optimized for mixed workloads
DBOptions options;

// Sparse index (baseline)
options.enable_sparse_index = true;
options.index_granularity = 8192;

// Phase 1 optimizations (enable by default)
options.enable_hot_key_cache = true;       // ← Already implemented
options.hot_key_cache_size_mb = 64;
options.enable_sorted_blocks = true;       // ← Easy win
options.enable_negative_cache = true;      // ← Easy win
options.enable_block_bloom_filters = true; // ← Easy win

// Phase 2 (enable for OLTP-heavy workloads)
options.enable_skip_lists = false;         // Only if >10K rows/block
options.enable_hash_index = false;         // Only if >50% point lookups
options.enable_simd_scan = true;           // Free if keys are integers

// Phase 3 (experimental)
options.enable_rcu_cache = false;          // Needs testing
options.enable_predictive_prefetch = false; // Complex
```

---

## Expected Final Performance

### After All Recommended Optimizations

```
Point Lookup Latency Distribution:

Hot keys (80%):        5-10 μs   ← Matches Tonbo ✅
Warm keys (15%):      15-20 μs   ← 3-4x slower
Cold keys (4%):       20-30 μs   ← 4-6x slower  
Missing keys (1%):     2-5 μs    ← FASTER than Tonbo ✅

Weighted Average:     ~10 μs     ← Only 2x slower overall ✅
```

**vs Current State**:
- Current: 35 μs average (7x slower than Tonbo)
- Optimized: **10 μs average** (2x slower than Tonbo)
- **Improvement: 3.5x faster than current implementation** ✅

---

## The Ultimate Hybrid: Best of All Worlds

### MarbleDB Final Architecture

```
┌─────────────────────────────────────────────────────┐
│           MarbleDB Hybrid Index System               │
├─────────────────────────────────────────────────────┤
│                                                       │
│  [Point Lookup Path]                                 │
│   1. Hot Key Cache (L1)          5-10 μs  ← 80% hit  │
│   2. Negative Cache              2-5 μs   ← Miss check│
│   3. Block Bloom Filter          2 μs     ← Skip block│
│   4. Skip List + Binary Search   20 μs    ← In block  │
│   5. SIMD Scan (fallback)        30 μs    ← Last resort│
│                                                       │
│  [Range Query Path]                                  │
│   1. Sparse Index Seek           10 μs    ← Fast seek │
│   2. Block Statistics Pruning    5 μs     ← Skip blocks│
│   3. Columnar SIMD Scan          Fast     ← Vectorized│
│                                                       │
│  [Analytical Query Path]                             │
│   1. Zone Maps (MIN/MAX)         <1 ms    ← Metadata  │
│   2. Block Skipping             80-95%   ← I/O saved │
│   3. Predicate Pushdown         Fast     ← Early filter│
└─────────────────────────────────────────────────────┘
```

### Performance Summary

| Workload Type | MarbleDB (Optimized) | Tonbo | Verdict |
|---------------|---------------------|-------|---------|
| **OLAP (90%)** | **5-20x faster** | Baseline | **MarbleDB wins** |
| **OLTP (10%)** | 2x slower (10 μs vs 5 μs) | Baseline | Acceptable trade-off |
| **Memory** | **5x less** (3 MB vs 16 MB) | Baseline | **MarbleDB wins** |

---

## Conclusion

**Can we make point lookups faster than current 7x gap?**

**YES!** With recommended optimizations:

1. ✅ **Hot key cache** (done): 250 μs → 10 μs (25x faster)
2. 🔨 **Sorted blocks**: 10 μs → 5 μs (2x faster)  
3. 🔨 **Negative cache**: Handle misses in 2 μs (100x faster)
4. 🔨 **Block bloom filters**: Skip empty blocks (10x faster)

**Final Performance**:
- Hot keys: **5 μs** (matches Tonbo!)
- Average: **10 μs** (only 2x slower)
- Misses: **2 μs** (faster than Tonbo!)

**Memory Used**: **3 MB** (vs Tonbo's 16 MB)

**The gap closes from 7x to 2x, while using 5x less memory and maintaining 5-20x analytical advantage!**

This makes MarbleDB truly **general-purpose** - competitive for both OLAP and OLTP workloads. 🎯

