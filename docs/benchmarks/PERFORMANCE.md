# Sabot Performance

**Last Updated:** November 26, 2025
**Status:** Benchmark results - treat with appropriate skepticism

## Overview

This document contains benchmark results for Sabot components. Performance claims should be independently verified before relying on them.

## SQL Performance (DuckDB-based)

Sabot's SQL execution uses DuckDB under the hood, so performance is essentially DuckDB performance.

**What this means:**
- Query execution speed = DuckDB speed
- We add some overhead for our wrapper layer
- DuckDB is already highly optimized

**Honest Assessment:**
- SQL queries are fast because DuckDB is fast
- We don't add significant value over using DuckDB directly
- The integration layer adds minor overhead

## MarbleDB Storage Engine

MarbleDB is an Arrow-native LSM storage engine. Below are benchmark results from actual runs.

### Benchmark: Arrow-Native vs Legacy Path

**Test Configuration:**
- 100 batches x 1000 rows = 100,000 total rows
- 5 iterations averaged
- Apple Silicon (M-series)

**Results:**

| Path | Write Time | Read Time | Total |
|------|-----------|-----------|-------|
| Legacy (with serialization) | 1.87 ms | 2.58 ms | 4.45 ms |
| Arrow-Native (zero-copy) | 3.27 ms | 0.07 ms | 3.34 ms |

**Analysis:**
- Arrow-native reads are ~39x faster due to zero-copy
- Arrow-native writes are slower (~0.57x) due to Arrow overhead
- Overall ~1.3x faster total throughput

### Benchmark: RocksDB Baseline

**Test Configuration:**
- 100,000 keys
- 512 byte values
- Bloom filters enabled

**Results:**

| Operation | Throughput | Latency |
|-----------|------------|---------|
| Sequential Writes | 178 K/sec | 5.6 μs/op |
| Point Lookups | 357 K/sec | 2.8 μs/op |
| Range Scan | 4.4 M/sec | 0.23 μs/op |

**Note:** This is RocksDB performance on the same hardware for comparison.

### Test Suite Status

MarbleDB has 19 unit tests passing:
- HashBloomFilterTest: 6/6 passing
- BloomFilterStrategyTest: 4/4 passing
- CacheStrategyTest: 4/4 passing
- OptimizationPipelineTest: 5/5 passing

## Stream Processing

**Honest Assessment:**
- Basic stream operators work (filter, map, window)
- Kafka integration is basic
- No production benchmarks available
- Performance claims from earlier docs are unverified

## Graph/Cypher

**Status:** Not functional - no benchmarks available

## RDF/SPARQL

**Status:** Basic queries work but not benchmarked for performance

**Honest Assessment:**
- Simple queries execute
- Complex queries may be slow or fail
- No optimization work done
- Performance likely poor on large datasets

## What We Don't Know

These performance aspects have not been measured:
- Memory usage under load
- Performance at scale (>1M rows)
- Concurrent access patterns
- Production workload behavior
- Long-running stability

## How to Run Benchmarks

### MarbleDB Benchmarks

```bash
cd MarbleDB/build/benchmarks

# RocksDB baseline
env DYLD_LIBRARY_PATH=/path/to/arrow/lib ./rocksdb_baseline

# Arrow-native comparison
env DYLD_LIBRARY_PATH=/path/to/arrow/lib ./bench_arrow_native_vs_legacy
```

### Running Tests

```bash
cd MarbleDB/build/tests
env DYLD_LIBRARY_PATH=/path/to/arrow/lib ./test_optimization_strategies
```

## Caveats

1. **Hardware varies** - Results on your machine will differ
2. **Microbenchmarks** - Don't represent real workloads
3. **Not production tested** - No long-running stability data
4. **Single-threaded** - Concurrent performance unknown
5. **Small datasets** - Large scale behavior untested

## Recommendations

- Run your own benchmarks on your hardware
- Test with your actual workload patterns
- Don't rely on these numbers for production decisions
- Consider using DuckDB directly for SQL workloads
