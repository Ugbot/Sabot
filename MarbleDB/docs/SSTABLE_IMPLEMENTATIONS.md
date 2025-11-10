# MarbleDB SSTable Implementations

## Overview

MarbleDB has **two distinct SSTable writer implementations** serving different use cases and performance profiles. Understanding the differences is critical for optimization work.

## 1. SSTableWriterImpl (Standard Writer)

**File**: `src/core/sstable.cpp`
**Format**: Custom binary format
**Use case**: Traditional key-value workloads, RDF triple stores
**Status**: ✅ Production-ready with bloom filter optimization

### Architecture

```
SSTableWriterImpl
├── Add(key, value)              # Collect hash for bloom filter
├── Finish()                     # Build bloom filter, write file
│   ├── Sort entries by key
│   ├── BuildBloomFilterFromHashes()  # RocksDB-style deferred construction
│   ├── WriteHeader()
│   ├── WriteData()
│   ├── WriteIndex()
│   ├── WriteBloomFilter()
│   └── WriteMetadata()
└── Helper methods
    ├── ChooseNumProbes()        # Adaptive hash count (0.693 * bits_per_key)
    ├── SetBit() / CheckBit()    # Byte-aligned bit operations
    └── Hash deduplication       # Skip duplicate keys
```

### File Format

```
[MAGIC(8)] [VERSION(4)] [INDEX_OFFSET(8)] [BLOOM_OFFSET(8)] [METADATA_OFFSET(8)]
[KEY(8)] [VALUE_SIZE(4)] [VALUE(N)] ...   # Data section
[KEY(8)] [OFFSET(8)] ...                  # Sparse index
[NUM_BITS(8)] [NUM_PROBES(4)] [BYTES]     # Bloom filter
[ENTRY_COUNT] [MIN_KEY] [MAX_KEY] ...     # Metadata
```

### Bloom Filter Implementation

**Recent Optimization** (commit 064281f8):
- **Before**: Built twice (Add() + Finish()) = 2x overhead
- **After**: Built once in Finish() using RocksDB technique
- **Performance**: ~50% faster flush, eliminates duplicate work

**Key characteristics**:
- Deferred construction: collect hashes in `Add()`, build in `Finish()`
- Adaptive num_probes: `0.693 * bits_per_key` (optimal for 1% FPR)
- Hash deduplication: skip duplicate adjacent keys
- Byte-aligned bit array: `vector<uint8_t>` for SIMD-friendly ops
- Lock-free: no mutexes/atomics (single-threaded writer)

**Verification**:
```
25,784 keys → 32,230 bytes bloom filter
6 hash probes, <1ms construction time
```

### Performance Profile

| Operation | Throughput | Notes |
|-----------|-----------|-------|
| Sequential writes | 180-240 K/sec | Hash collection overhead |
| Point lookups | Testing | Depends on bloom filter hit rate |
| Range scans | Testing | Sparse index-based |

### When to Use

- ✅ RDF triple stores (3 × int64 keys)
- ✅ Traditional key-value workloads
- ✅ When bloom filters are critical
- ✅ Point lookup-heavy workloads
- ❌ High-throughput streaming writes (use mmap writer)

---

## 2. MmapSSTableWriter (High-Performance Writer)

**File**: `src/core/mmap_sstable_writer.cpp`
**Format**: Arrow IPC (Interprocess Communication)
**Use case**: High-throughput streaming, petabyte-scale workloads
**Status**: ⚠️ Production-ready, **missing bloom filters**

### Architecture

```
MmapSSTableWriter
├── Add(key, value)              # Buffer in Arrow RecordBatch
│   ├── batch_keys_.push_back(key)
│   ├── batch_values_.push_back(value)
│   └── FlushBatchBuffer() if full
├── FlushBatchBuffer()           # Write 4096-row batches
│   ├── CreateRecordBatchFromBuffer()
│   └── Write to memory-mapped region
└── Finish()
    ├── Flush remaining batch
    ├── WriteIndex()             # Sparse index
    ├── WriteMetadata()
    └── msync(MS_ASYNC)          # Async flush to disk
```

### File Format

```
Arrow IPC Schema
├── Schema: [key: uint64, value: binary]
├── RecordBatch 0 (4096 rows)
├── RecordBatch 1 (4096 rows)
├── ...
└── Footer with metadata
```

### Memory-Mapped I/O

**Key innovation**: Zero-syscall writes during data path

```c++
// Open file
fd = open(filepath, O_RDWR | O_CREAT | O_TRUNC, 0644);

// Pre-allocate zone (8MB default)
ftruncate(fd, zone_size);

// Memory map
mapped_region = mmap(NULL, zone_size, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);

// Write directly to memory - NO SYSCALLS!
memcpy(mapped_region + offset, data, size);

// Async flush
msync(mapped_region, size, MS_ASYNC);
```

### Performance Profile

| Operation | Throughput | Notes |
|-----------|-----------|-------|
| Sequential writes | 240-260 K/sec | Zero-copy, no syscalls |
| Batch writes | 500-1000 MB/sec | Direct memory writes |
| Point lookups | **N/A** | No bloom filters yet |
| Range scans | **Testing** | Arrow IPC format |

### Current Limitations

⚠️ **Missing bloom filters**:
- No `ContainsKey()` optimization
- Point lookups must scan all RecordBatches
- **Performance impact**: O(n) lookups vs O(1) with bloom filters

⚠️ **Format mismatch**:
- Arrow IPC format (columnar)
- Standard SSTable format (row-oriented)
- Cannot share bloom filter code directly

### When to Use

- ✅ High-throughput streaming writes
- ✅ Batch-oriented workloads
- ✅ Petabyte-scale data
- ✅ NVMe optimized workloads
- ❌ Point lookup-heavy (until bloom filters added)
- ❌ RDF triple stores (no bloom filters)

---

## Comparison Matrix

| Feature | SSTableWriterImpl | MmapSSTableWriter |
|---------|------------------|-------------------|
| **Format** | Custom binary | Arrow IPC |
| **Write path** | Buffered I/O | Memory-mapped |
| **Syscalls** | ~100 per flush | ~1 per flush |
| **Bloom filters** | ✅ Optimized | ❌ Missing |
| **Point lookups** | Fast (bloom filter) | Slow (full scan) |
| **Range scans** | Sparse index | RecordBatch iteration |
| **Batch writes** | Medium | Very fast |
| **Memory usage** | Low | Medium (8MB zones) |
| **Production ready** | ✅ Yes | ⚠️ Limited use cases |

---

## Configuration

### Enabling/Disabling Mmap Writer

```cpp
LSMTreeConfig config;
config.enable_mmap_flush = true;   // Use MmapSSTableWriter (default)
config.enable_mmap_flush = false;  // Use SSTableWriterImpl
```

### Mmap Writer Settings

```cpp
config.flush_zone_size_mb = 8;     // Initial zone size (8MB default)
config.use_async_msync = true;     // MS_ASYNC for non-blocking flush
```

### Bloom Filter Settings (SSTableWriterImpl only)

```cpp
config.enable_bloom_filters = true;
config.sstable_bloom_filter_fp_rate = 0.01;  // 1% false positive rate
```

---

## Recent Optimizations

### Commit 064281f8: RocksDB-Style Bloom Filters

**Problem**: Duplicate bloom filter construction
- Built in `Add()`: O(k) per key (k = num_probes)
- Rebuilt in `Finish()`: O(n*k) total
- Result: 2x construction overhead

**Solution**: Deferred construction
- Collect hashes in `Add()`: O(1) per key
- Build once in `Finish()`: O(n*k) total
- Result: ~50% faster flush

**Techniques borrowed from RocksDB**:
1. Adaptive num_probes: `0.693 * bits_per_key`
2. Hash deduplication
3. Byte-aligned bit arrays
4. Lock-free implementation

**Verification**:
```cpp
[DEBUG] BuildBloomFilterFromHashes: num_keys=25784
[DEBUG] BuildBloomFilterFromHashes: num_bits=257840
[DEBUG] BuildBloomFilterFromHashes: num_bytes=32230 (~31KB)
[DEBUG] BuildBloomFilterFromHashes: num_probes=6
[DEBUG] BuildBloomFilterFromHashes: Filter built ✅
```

---

## Future Work

### High Priority

1. **Add bloom filters to MmapSSTableWriter**
   - Challenge: Arrow IPC format (columnar) vs bloom filter (row-oriented)
   - Solution: Store bloom filter in Arrow metadata or separate file
   - Expected impact: 100-1000x faster point lookups

2. **Implement SSTable reading for Arrow format**
   - Currently only tested with legacy format
   - Need Arrow IPC reader with bloom filter support

### Medium Priority

3. **SIMD bloom filter queries** (RocksDB AVX2 technique)
   - 8-way parallel bit checks
   - Expected: 2-4x faster lookups

4. **Cache-local bloom filters** (RocksDB 64-byte cache lines)
   - Better CPU cache utilization
   - Expected: 10-20% faster lookups

5. **Compaction support for Arrow format**
   - Merge RecordBatches from multiple SSTables
   - Rebuild bloom filters during compaction

---

## Choosing the Right Writer

### Use SSTableWriterImpl when:
- Point lookups are common (>10% of operations)
- RDF triple store workload
- Bloom filter hit rate is important
- Traditional key-value workload

### Use MmapSSTableWriter when:
- Streaming write-heavy workload (>90% writes)
- Batch-oriented operations
- Petabyte-scale data
- NVMe SSDs (benefit from large zones)
- Range scans dominate over point lookups

### Not sure?
- Start with `enable_mmap_flush = false` (SSTableWriterImpl)
- Profile your workload
- Switch to mmap if write throughput is bottleneck
- Keep SSTableWriterImpl if point lookup latency matters

---

## Architecture Decision Records

### Why Two Implementations?

**Historical context**:
1. SSTableWriterImpl: Original implementation, RocksDB-inspired
2. MmapSSTableWriter: Added for high-throughput workloads

**Why not merge?**:
- Different formats (binary vs Arrow IPC)
- Different performance profiles
- Arrow IPC benefits from columnar layout
- Binary format benefits from bloom filters

**Long-term plan**:
- Add bloom filters to MmapSSTableWriter
- Deprecate SSTableWriterImpl for most use cases
- Keep SSTableWriterImpl for specialized workloads

---

## Debugging

### Checking Which Writer is Used

```cpp
// In FlushMemTable()
if (config_.enable_mmap_flush) {
    writer = CreateMmapSSTableWriter(...);  // MmapSSTableWriter
} else {
    writer = sstable_manager_->CreateWriter(...);  // SSTableWriterImpl
}
```

### Enabling Debug Output

```cpp
// In src/core/sstable.cpp (SSTableWriterImpl)
std::cerr << "[DEBUG] BuildBloomFilterFromHashes: num_keys=" << num_keys << std::endl;

// In src/core/mmap_sstable_writer.cpp (MmapSSTableWriter)
std::cerr << "[DEBUG] FlushBatchBuffer: " << batch_keys_.size() << " keys" << std::endl;
```

### Common Issues

**Issue**: Benchmark hangs during flush
**Cause**: MmapSSTableWriter Finish() waiting on file I/O
**Solution**: Use `config.use_async_msync = true` for non-blocking flush

**Issue**: Point lookups fail after flush
**Cause**: MmapSSTableWriter has no bloom filters
**Solution**: Use `config.enable_mmap_flush = false` until bloom filters added

**Issue**: Bloom filter not working
**Cause**: Check if `enable_bloom_filters = true` in config
**Solution**: Enable bloom filters and rebuild

---

## References

- RocksDB Bloom Filter Implementation: `vendor/rocksdb/table/block_based/filter_policy.cc`
- Arrow IPC Format: `vendor/arrow/cpp/src/arrow/ipc/`
- MarbleDB SSTable Code: `src/core/sstable.cpp`, `src/core/mmap_sstable_writer.cpp`
