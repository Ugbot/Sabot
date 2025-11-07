# MarbleDB Pluggable Optimization Architecture - Implementation Roadmap

**Project:** Pluggable Optimization System
**Author:** Claude (with Ben Gamble)
**Date:** 2025-11-04
**Estimated Duration:** 14 days
**Status:** Phase 0 (Planning)

---

## Quick Reference

| Phase | Duration | Status | Key Deliverables |
|-------|----------|--------|------------------|
| 0. Planning & Documentation | 1 day | ✅ COMPLETE | Design docs, roadmap |
| 1. Core Infrastructure | 2 days | ⏳ NEXT | Base interfaces, pipeline |
| 2. Strategy Implementations | 3 days | 📋 PLANNED | 4 strategies implemented |
| 3. Auto-Configuration | 2 days | 📋 PLANNED | Factory + schema detection |
| 4. Integration & Migration | 3 days | 📋 PLANNED | Hook integration, dual paths |
| 5. Comprehensive Validation | 2 days | 📋 PLANNED | Tests + benchmarks |
| 6. Finalization | 1 day | 📋 PLANNED | Docs + migration guide |

**Total Estimated Effort:** 14 days

---

## Phase 0: Planning & Documentation ✅ COMPLETE

### Goals
- Create comprehensive architecture design
- Document decision rationale
- Get team alignment
- Establish success criteria

### Deliverables
- ✅ `docs/planning/PLUGGABLE_OPTIMIZATIONS_DESIGN.md` (55KB)
- ✅ `docs/planning/OPTIMIZATION_REFACTOR_ROADMAP.md` (THIS FILE)
- ⏳ `PROJECT_MAP.md` updates

### Tasks Completed
- [x] Research existing schema types (RDF, key-value, time-series, etc.)
- [x] Document current optimization pain points
- [x] Design OptimizationStrategy interface
- [x] Design factory pattern for auto-configuration
- [x] Document API design
- [x] Create file structure plan
- [x] Define migration strategy
- [x] Document expected performance improvements

### Success Criteria
- ✅ Comprehensive design doc exists
- ✅ All stakeholders understand architecture
- ✅ Clear implementation path defined

---

## Phase 1: Core Infrastructure (Days 2-3)

### Goals
- Build OptimizationStrategy base framework
- Create OptimizationPipeline for composing strategies
- Integrate with ColumnFamilyOptions (non-invasive)
- Ensure no disruption to existing code

### Estimated Duration: 2 days

### Deliverables

#### Day 2: Base Interfaces
**Files to Create:**
- `include/marble/optimization_strategy.h` (~400 lines)
  - `OptimizationStrategy` base class
  - `ReadContext` and `WriteContext` structs
  - `CompactionContext` and `FlushContext` structs
  - `OptimizationPipeline` class

- `src/core/optimization_strategy.cpp` (~300 lines)
  - `OptimizationPipeline::AddStrategy()`
  - `OptimizationPipeline::OnRead()` - calls all strategies
  - `OptimizationPipeline::OnWrite()` - calls all strategies
  - `OptimizationPipeline::Serialize()` / `Deserialize()`

**Files to Modify:**
- `include/marble/column_family.h` (~50 lines added)
  - Add `OptimizationConfig` struct to `ColumnFamilyOptions`
  - Add `optimizations_` member to `ColumnFamilyHandle`

- `CMakeLists.txt` (~10 lines)
  - Add new source files to `marble_static` target

#### Day 3: Factory Skeleton + Unit Tests
**Files to Create:**
- `include/marble/optimization_factory.h` (~200 lines)
  - `OptimizationFactory` class
  - `WorkloadHints` struct
  - `SchemaType` enum
  - Factory method signatures

- `src/core/optimization_factory.cpp` (~200 lines)
  - `DetectSchemaType()` implementation
  - Factory skeletons (actual strategies in Phase 2)
  - Auto-configuration logic framework

- `tests/unit/test_optimization_base.cpp` (~300 lines)
  - Test `OptimizationPipeline::AddStrategy()`
  - Test `OnRead()` / `OnWrite()` hook ordering
  - Test serialization roundtrip
  - Test schema type detection

### Tasks Breakdown

**Day 2 Morning (4h): Base Interface**
- [ ] Create `OptimizationStrategy` abstract base class
  - [ ] Define virtual methods (OnRead, OnWrite, OnCompaction, OnFlush)
  - [ ] Define context structs (ReadContext, WriteContext, etc.)
  - [ ] Add serialization interface

- [ ] Create `OptimizationPipeline` class
  - [ ] Implement `AddStrategy()` / `RemoveStrategy()`
  - [ ] Implement `OnRead()` to call all strategies in order
  - [ ] Implement short-circuit logic (NotFound → stop pipeline)

**Day 2 Afternoon (4h): Integration Points**
- [ ] Modify `ColumnFamilyOptions`
  - [ ] Add `OptimizationConfig` struct
  - [ ] Add workload hints
  - [ ] Add enabled_strategies list

- [ ] Modify `ColumnFamilyHandle`
  - [ ] Add `optimizations_` unique_ptr member
  - [ ] Add getter methods

- [ ] Update `CMakeLists.txt`
  - [ ] Add new source files
  - [ ] Verify build passes

**Day 3 Morning (4h): Factory Framework**
- [ ] Create `OptimizationFactory` class
  - [ ] Implement `DetectSchemaType()` with heuristics
  - [ ] Add factory method skeletons (defer actual strategy creation to Phase 2)
  - [ ] Add auto-configuration framework

**Day 3 Afternoon (4h): Unit Tests**
- [ ] Write tests for `OptimizationPipeline`
  - [ ] Test adding/removing strategies
  - [ ] Test hook execution order
  - [ ] Test short-circuit behavior

- [ ] Write tests for schema detection
  - [ ] Test RDF triple detection
  - [ ] Test key-value detection
  - [ ] Test time-series detection

- [ ] Verify all existing MarbleDB tests still pass

### Dependencies
- None (foundational work)

### Success Criteria
- ✅ All new files compile without errors
- ✅ Unit tests pass (100% coverage for base classes)
- ✅ All existing MarbleDB tests still pass (no regression)
- ✅ Code review approved
- ✅ Documentation comments complete

### Risks & Mitigation
| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Breaking existing code | Low | High | Thorough testing, non-invasive changes |
| Interface design issues | Medium | Medium | Design review before implementation |
| Build system issues | Low | Low | Test build frequently |

---

## Phase 2: Strategy Implementations (Days 4-6)

### Goals
- Implement all 4 core optimization strategies
- Achieve feature parity with existing optimizations
- Comprehensive unit testing for each strategy

### Estimated Duration: 3 days

### Deliverables

#### Day 4: BloomFilterStrategy
**Files to Create:**
- `include/marble/optimizations/bloom_filter_strategy.h` (~200 lines)
  - `BloomFilterStrategy` class
  - Config struct
  - Statistics tracking

- `src/core/optimizations/bloom_filter_strategy.cpp` (~400 lines)
  - `OnRead()` - check bloom, return NotFound if absent
  - `OnWrite()` - add key to bloom
  - `OnFlush()` - serialize block blooms
  - `OnCompaction()` - merge bloom filters

- `tests/unit/test_bloom_filter_strategy.cpp` (~300 lines)
  - Test false positive rate
  - Test serialization
  - Test compaction merge

#### Day 5: CacheStrategy + SkippingIndexStrategy
**Files to Create:**
- `include/marble/optimizations/cache_strategy.h` (~250 lines)
  - `CacheStrategy` class
  - Hot cache + negative cache integration
  - Access tracking

- `src/core/optimizations/cache_strategy.cpp` (~500 lines)
  - `OnRead()` - check hot cache, check negative cache
  - `OnReadComplete()` - update caches, adaptive promotion
  - `OnWrite()` - invalidate caches
  - Leverage existing `HotKeyCache` and `NegativeCache` classes

- `include/marble/optimizations/skipping_index_strategy.h` (~200 lines)
  - `SkippingIndexStrategy` class
  - Config for generic vs time-series

- `src/core/optimizations/skipping_index_strategy.cpp` (~400 lines)
  - `OnRead()` - check if query can skip blocks
  - `OnFlush()` - build block statistics
  - `OnCompaction()` - merge statistics
  - Leverage existing `SkippingIndex` class

- `tests/unit/test_cache_strategy.cpp` (~300 lines)
- `tests/unit/test_skipping_index_strategy.cpp` (~300 lines)

#### Day 6: TripleStoreStrategy + Integration
**Files to Create:**
- `include/marble/optimizations/triple_store_strategy.h` (~250 lines)
  - `TripleStoreStrategy` class
  - Predicate-aware bloom filters
  - Join statistics

- `src/core/optimizations/triple_store_strategy.cpp` (~500 lines)
  - `OnRead()` - check predicate bloom, check predicate cache
  - `OnWrite()` - update predicate blooms, update join stats
  - `OnFlush()` - serialize per-predicate metadata
  - RDF-specific optimizations

- `tests/unit/test_triple_store_strategy.cpp` (~400 lines)
  - Test predicate-aware blooms
  - Test join statistics
  - Test with actual RDF workload

**Directory Structure:**
```
src/core/optimizations/
├── bloom_filter_strategy.cpp
├── cache_strategy.cpp
├── skipping_index_strategy.cpp
└── triple_store_strategy.cpp

include/marble/optimizations/
├── bloom_filter_strategy.h
├── cache_strategy.h
├── skipping_index_strategy.h
└── triple_store_strategy.h

tests/unit/
├── test_bloom_filter_strategy.cpp
├── test_cache_strategy.cpp
├── test_skipping_index_strategy.cpp
└── test_triple_store_strategy.cpp
```

### Tasks Breakdown

**Day 4: BloomFilterStrategy**
- [ ] Implement `BloomFilterStrategy` class
  - [ ] OnRead() - check block bloom, return NotFound if key absent
  - [ ] OnWrite() - add key to current block bloom
  - [ ] OnFlush() - serialize bloom filters with SSTable
  - [ ] OnCompaction() - merge bloom filters from input SSTables

- [ ] Leverage existing `BloomFilter` class from `include/marble/bloom_filter.h`
  - [ ] Reuse Add(), MayContain() methods
  - [ ] Match existing false positive rate

- [ ] Write comprehensive unit tests
  - [ ] Test basic add/check operations
  - [ ] Verify false positive rate ≤ configured value
  - [ ] Test serialization roundtrip
  - [ ] Test bloom filter merging during compaction

**Day 5 Morning: CacheStrategy**
- [ ] Implement `CacheStrategy` class
  - [ ] OnRead() - check hot key cache, check negative cache
  - [ ] OnReadComplete() - update access tracker, adaptive promotion
  - [ ] OnWrite() - invalidate both caches for updated key

- [ ] Integrate existing cache classes
  - [ ] Use `HotKeyCache` from `include/marble/hot_key_cache.h`
  - [ ] Use `NegativeCache` from same file
  - [ ] Use `AccessTracker` for adaptive promotion

- [ ] Unit tests
  - [ ] Test hot cache hit/miss
  - [ ] Test negative cache hit/miss
  - [ ] Test adaptive promotion (key promoted after N accesses)
  - [ ] Test LRU eviction

**Day 5 Afternoon: SkippingIndexStrategy**
- [ ] Implement `SkippingIndexStrategy` class
  - [ ] OnRead() - check if query predicate matches block statistics
  - [ ] OnFlush() - compute min/max/count per block
  - [ ] OnCompaction() - merge statistics from input blocks

- [ ] Integrate existing `SkippingIndex` from `include/marble/skipping_index.h`
  - [ ] Use generic skipping index for most schemas
  - [ ] Use `TimeSeriesSkippingIndex` for time-series data

- [ ] Unit tests
  - [ ] Test block skipping for range queries
  - [ ] Test time-series bucket optimization
  - [ ] Test statistics accuracy
  - [ ] Test compaction merge

**Day 6: TripleStoreStrategy**
- [ ] Implement `TripleStoreStrategy` class
  - [ ] OnRead() - check per-predicate bloom, check predicate cache
  - [ ] OnWrite() - update predicate bloom, track join selectivity
  - [ ] OnFlush() - serialize per-predicate metadata
  - [ ] RDF-specific query optimizations

- [ ] Per-predicate bloom filters
  - [ ] Create bloom filter for each unique predicate
  - [ ] Size blooms based on predicate frequency

- [ ] Predicate caching
  - [ ] Cache frequent predicate results
  - [ ] LRU eviction when cache full

- [ ] Join statistics
  - [ ] Track predicate selectivity (% of triples returned)
  - [ ] Use for query planning hints

- [ ] Unit tests
  - [ ] Test with SPO, POS, OSP schemas
  - [ ] Test predicate-aware bloom filters
  - [ ] Test predicate caching
  - [ ] Test join statistics tracking

### Dependencies
- Phase 1 complete (base interfaces exist)
- Existing classes: `BloomFilter`, `HotKeyCache`, `NegativeCache`, `SkippingIndex`

### Success Criteria
- ✅ All 4 strategies compile without errors
- ✅ All unit tests pass (95%+ code coverage)
- ✅ Memory usage within expected bounds
- ✅ Serialization works correctly (can persist + restore state)
- ✅ Performance matches or exceeds existing implementations

### Risks & Mitigation
| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Strategy performance worse than existing | Medium | High | Benchmark continuously, optimize hot paths |
| Memory leaks in caching strategies | Low | High | Valgrind testing, careful RAII usage |
| Bloom filter merge logic incorrect | Low | Medium | Unit tests with known data, verify FP rate |
| Skipping index statistics inaccurate | Low | Medium | Validate with real queries, compare results |

---

## Phase 3: Auto-Configuration (Days 7-8)

### Goals
- Implement schema type detection heuristics
- Build auto-configuration logic in factory
- Enable smart defaults for common workloads

### Estimated Duration: 2 days

### Deliverables

#### Day 7: Schema Detection + Factory Logic
**Files to Modify:**
- `src/core/optimization_factory.cpp` (~500 lines added)
  - Implement `DetectSchemaType()` with heuristics
  - Implement `CreateFromCapabilities()` with auto-config logic
  - Add factory methods for each strategy type

**New Logic:**
```cpp
// RDF Triple detection
if (3 int64 columns && names match (S,P,O) pattern)
    → SchemaType::kRDFTriple

// Key-Value detection
if (2 columns)
    → SchemaType::kKeyValue

// Time-Series detection
if (has timestamp column)
    → SchemaType::kTimeSeries

// Based on detected type + hints, select strategies:
RDF Triple + skewed → BloomFilter + TripleStore + Cache
Key-Value + skewed → Cache + BloomFilter
Time-Series → SkippingIndex (no bloom, no cache)
```

#### Day 8: WorkloadHints Integration + Testing
**Files to Create:**
- `tests/unit/test_optimization_factory.cpp` (~500 lines)
  - Test schema type detection for each type
  - Test auto-configuration for RDF triples
  - Test auto-configuration for key-value
  - Test auto-configuration for time-series
  - Test workload hints affect configuration
  - Test manual override works

**Files to Modify:**
- `src/core/optimization_factory.cpp`
  - Refine auto-config based on test results
  - Add tuning parameters based on workload hints
  - Handle edge cases (unknown schema type, conflicting hints)

### Tasks Breakdown

**Day 7 Morning: Schema Detection**
- [ ] Implement `DetectSchemaType()`
  - [ ] Heuristic for RDF triple (3 int64 columns, specific names)
  - [ ] Heuristic for key-value (2 columns)
  - [ ] Heuristic for time-series (timestamp column present)
  - [ ] Heuristic for property graph (complex nested schema)
  - [ ] Handle unknown schemas gracefully

**Day 7 Afternoon: Auto-Config Logic**
- [ ] Implement `CreateFromCapabilities()`
  - [ ] Call `DetectSchemaType()`
  - [ ] Based on schema type, select strategies
  - [ ] Tune strategy configs based on `WorkloadHints`
  - [ ] Return configured `OptimizationPipeline`

- [ ] Implement strategy-specific configs
  - [ ] `ConfigureForRDFTriple()` - bloom + triple store + cache
  - [ ] `ConfigureForKeyValue()` - cache + bloom (if skewed)
  - [ ] `ConfigureForTimeSeries()` - skipping index only

**Day 8 Morning: WorkloadHints Tuning**
- [ ] Integrate `WorkloadHints` into auto-config
  - [ ] High skewness → enable cache, increase cache size
  - [ ] Point lookups → enable bloom filters
  - [ ] Range scans → enable skipping index
  - [ ] Full scans → disable all optimizations

- [ ] Fine-tune strategy parameters
  - [ ] Bloom filter bits_per_key based on selectivity
  - [ ] Cache size based on skewness
  - [ ] Skipping index granularity based on query patterns

**Day 8 Afternoon: Comprehensive Testing**
- [ ] Write tests for schema detection
  - [ ] Test each schema type detected correctly
  - [ ] Test edge cases (ambiguous schemas)

- [ ] Write tests for auto-configuration
  - [ ] Test each schema type gets correct strategies
  - [ ] Test workload hints affect configuration
  - [ ] Test manual override works

- [ ] Integration tests
  - [ ] Create CF with auto-config enabled
  - [ ] Verify correct strategies instantiated
  - [ ] Verify strategies have correct configs

### Dependencies
- Phase 2 complete (all strategies implemented)
- Schema examples for each type (RDF, KV, time-series)

### Success Criteria
- ✅ Schema type detection 100% accurate on known types
- ✅ Auto-configured strategies match manual config for common cases
- ✅ WorkloadHints demonstrably affect strategy selection/tuning
- ✅ Unknown schemas handled gracefully (fall back to generic config)
- ✅ All unit tests pass

### Risks & Mitigation
| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Schema detection false positives | Medium | Low | Conservative heuristics, allow manual override |
| Auto-config choices suboptimal | Medium | Medium | Extensive testing, tuning benchmarks |
| WorkloadHints ignored or misused | Low | Low | Clear documentation, validation tests |

---

## Phase 4: Integration & Migration (Days 9-11)

### Goals
- Integrate OptimizationPipeline into MarbleDB read/write paths
- Run both old and new systems in parallel
- Validate correctness and performance
- Prepare for gradual migration

### Estimated Duration: 3 days

### Deliverables

#### Day 9: Read Path Integration
**Files to Modify:**
- `src/core/api.cpp` (~100 lines modified)
  - Modify `Get()` to call `pipeline->OnRead()`
  - Pass `ReadContext` with query info
  - Handle short-circuit (NotFound from bloom filter)
  - Maintain old code path in parallel (dual execution)

- `src/core/column_family.cpp` (~50 lines)
  - Initialize `optimizations_` in `CreateColumnFamily()`
  - Call `OptimizationFactory::CreateFromCapabilities()`
  - Store pipeline in `ColumnFamilyHandle`

**Integration Points:**
```cpp
// In Get() method
if (cf_info->optimizations_) {
    ReadContext ctx{key, options};
    auto status = cf_info->optimizations_->OnRead(&ctx);
    if (status.IsNotFound()) {
        // Short-circuit - bloom filter says key doesn't exist
        return Status::NotFound();
    }
}

// OLD CODE PATH (runs in parallel for validation)
if (cf_info->bloom_filter) {
    // ... existing bloom check
}
```

#### Day 10: Write Path + Compaction Integration
**Files to Modify:**
- `src/core/api.cpp` (~100 lines modified)
  - Modify `Put()` to call `pipeline->OnWrite()`
  - Modify `InsertBatch()` to call `OnWrite()` for each row
  - Modify `Delete()` to call `OnWrite()` (invalidate caches)

- `src/core/sstable.cpp` (~100 lines)
  - Call `pipeline->OnFlush()` during memtable flush
  - Serialize optimization metadata with SSTable footer
  - Deserialize metadata when opening SSTable

- `src/core/lsm_storage.cpp` (~100 lines)
  - Call `pipeline->OnCompaction()` during compaction
  - Merge optimization metadata from input SSTables
  - Propagate metadata to output SSTables

#### Day 11: Dual Code Path Validation + Metrics
**Files to Create:**
- `src/core/optimization_metrics.cpp` (~200 lines)
  - Track optimization hit rates, latencies
  - Compare old vs new system performance
  - Log differences for analysis

**Files to Modify:**
- `src/core/api.cpp`
  - Add performance logging
  - Compare old vs new results (should match!)
  - Warn if results differ

**Testing:**
- Run all existing MarbleDB tests with dual paths enabled
- Verify old and new systems produce identical results
- Measure performance difference (should be < 5%)

### Tasks Breakdown

**Day 9: Read Path Integration**
- [ ] Initialize optimization pipeline on CF creation
  - [ ] In `CreateColumnFamily()`, call factory
  - [ ] Store pipeline in `ColumnFamilyHandle`

- [ ] Integrate into `Get()` method
  - [ ] Create `ReadContext` from `Key` + `ReadOptions`
  - [ ] Call `pipeline->OnRead(&ctx)` before secondary index lookup
  - [ ] Handle `NotFound` short-circuit
  - [ ] Call `pipeline->OnReadComplete()` after successful read

- [ ] Run dual code paths
  - [ ] Keep existing optimization code running
  - [ ] Compare results (should match)

- [ ] Test with existing test suite
  - [ ] All tests should still pass
  - [ ] No performance regression

**Day 10: Write Path + Compaction**
- [ ] Integrate into `Put()` method
  - [ ] Create `WriteContext` from key + record
  - [ ] Call `pipeline->OnWrite(&ctx)` before LSM write

- [ ] Integrate into `InsertBatch()` method
  - [ ] Call `OnWrite()` for each row in batch
  - [ ] Or add `OnWriteBatch()` method for efficiency

- [ ] Integrate into `Flush()` operation
  - [ ] Create `FlushContext` with memtable metadata
  - [ ] Call `pipeline->OnFlush(&ctx)` before flush
  - [ ] Serialize pipeline state to SSTable footer

- [ ] Integrate into `Compact()` operation
  - [ ] Create `CompactionContext` with input/output SSTables
  - [ ] Call `pipeline->OnCompaction(&ctx)`
  - [ ] Merge optimization metadata

**Day 11: Validation + Metrics**
- [ ] Implement performance metrics
  - [ ] Track old system latencies
  - [ ] Track new system latencies
  - [ ] Track hit rates (bloom, cache, etc.)

- [ ] Add result validation
  - [ ] Verify old and new produce same results
  - [ ] Log warnings if differ

- [ ] Run comprehensive test suite
  - [ ] All unit tests with dual paths
  - [ ] All integration tests
  - [ ] Verify correctness

- [ ] Performance profiling
  - [ ] Measure overhead of new system
  - [ ] Identify bottlenecks
  - [ ] Optimize hot paths

### Dependencies
- Phase 2 complete (strategies implemented)
- Phase 3 complete (factory works)

### Success Criteria
- ✅ All existing tests pass with new system enabled
- ✅ Old and new systems produce identical results (100% match)
- ✅ Performance within 5% of old system
- ✅ No memory leaks (Valgrind clean)
- ✅ No crashes under load testing

### Risks & Mitigation
| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Results differ between old/new systems | Medium | High | Extensive validation, compare on every operation |
| Performance regression | Medium | High | Continuous benchmarking, optimize hot paths |
| Integration bugs (crashes, hangs) | Medium | High | Thorough testing, fuzzing, stress testing |
| Metadata serialization issues | Low | Medium | Test roundtrip, verify SSTable compatibility |

---

## Phase 5: Comprehensive Validation (Days 12-13)

### Goals
- Validate correctness across all workloads
- Prove performance improvements
- Ensure no regressions
- Build confidence for production deployment

### Estimated Duration: 2 days

### Deliverables

#### Day 12: Test Suite Execution
**Unit Tests:**
- Run all existing MarbleDB unit tests (~200 tests)
- Run all new optimization unit tests (~40 tests)
- Verify 100% pass rate

**Integration Tests:**
**Files to Create:**
- `tests/integration/test_cf_optimizations.cpp` (~600 lines)
  - Test RDF triple workload (SPARQL queries)
  - Test OLTP workload (skewed key-value)
  - Test time-series workload (range queries)
  - Verify performance improvements vs old system

#### Day 13: Performance Benchmarks
**Files to Create:**
- `benchmarks/optimization_strategy_bench.cpp` (~800 lines)
  - Benchmark each strategy individually
  - Benchmark auto-configuration overhead
  - Compare old vs new system performance
  - Measure memory usage

**Workload Benchmarks:**
1. **RDF Triple Benchmark**
   - Load 1M triples (SPO index)
   - Run 1000 SPARQL queries (point + join)
   - Measure: throughput, latency, memory

2. **OLTP Benchmark**
   - Load 10M key-value pairs
   - Run YCSB workload (Zipfian distribution)
   - Measure: throughput, latency, cache hit rate

3. **Time-Series Benchmark**
   - Load 100M time-series records
   - Run range queries (various time windows)
   - Measure: scan throughput, blocks skipped

### Tasks Breakdown

**Day 12 Morning: Unit Test Validation**
- [ ] Run all existing MarbleDB tests
  - [ ] With new system disabled (baseline)
  - [ ] With new system enabled (validation)
  - [ ] Verify 100% pass rate for both

- [ ] Run new optimization unit tests
  - [ ] Test each strategy individually
  - [ ] Test factory auto-configuration
  - [ ] Test pipeline composition

- [ ] Fix any failing tests
  - [ ] Debug failures
  - [ ] Fix implementation bugs
  - [ ] Re-run until 100% pass

**Day 12 Afternoon: Integration Tests**
- [ ] RDF triple workload test
  - [ ] Create SPO, POS, OSP indexes
  - [ ] Load 100K triples
  - [ ] Run SPARQL queries (point, range, join)
  - [ ] Verify correct results
  - [ ] Measure performance improvement

- [ ] OLTP workload test
  - [ ] Create key-value table
  - [ ] Load 1M records
  - [ ] Run Zipfian access pattern (skewed)
  - [ ] Verify cache hit rate > 80%
  - [ ] Measure performance improvement

- [ ] Time-series workload test
  - [ ] Create time-series table
  - [ ] Load 10M records (1 month of data)
  - [ ] Run range queries (hour, day, week)
  - [ ] Verify blocks skipped > 90%
  - [ ] Measure scan throughput

**Day 13 Morning: Performance Benchmarks**
- [ ] Benchmark RDF triple workload
  - [ ] Old system baseline
  - [ ] New system (auto-configured)
  - [ ] Compare: throughput, latency, memory
  - [ ] Expected: 2-5x faster queries

- [ ] Benchmark OLTP workload
  - [ ] Old system baseline
  - [ ] New system (auto-configured)
  - [ ] Compare: throughput, latency, cache effectiveness
  - [ ] Expected: 10-50x faster hot key access

- [ ] Benchmark time-series workload
  - [ ] Old system baseline (no skipping index)
  - [ ] New system (SkippingIndexStrategy)
  - [ ] Compare: scan throughput, CPU usage
  - [ ] Expected: 100-1000x faster range scans

**Day 13 Afternoon: Memory & Stress Testing**
- [ ] Memory profiling
  - [ ] Measure optimization overhead
  - [ ] Verify no memory leaks (Valgrind)
  - [ ] Measure peak memory usage

- [ ] Stress testing
  - [ ] Run high-throughput workload (1M ops/sec)
  - [ ] Run for extended duration (1 hour)
  - [ ] Verify stability (no crashes, no hangs)

- [ ] Document results
  - [ ] Create performance report
  - [ ] Document known limitations
  - [ ] Create tuning recommendations

### Dependencies
- Phase 4 complete (integration working)
- Benchmark datasets prepared (RDF, OLTP, time-series)

### Success Criteria
- ✅ All tests pass (100% pass rate)
- ✅ Performance ≥ old system on all workloads
- ✅ Target improvements achieved:
  - RDF: 2-5x faster
  - OLTP: 10-50x faster for hot keys
  - Time-series: 100-1000x faster range scans
- ✅ Memory overhead < 20%
- ✅ No crashes or hangs under stress

### Risks & Mitigation
| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Performance targets not met | Medium | High | Profile, optimize, adjust targets if needed |
| Memory usage too high | Low | Medium | Tune cache sizes, reduce metadata |
| Stress test failures | Low | High | Fix bugs, improve error handling |
| Unknown edge cases | Medium | Medium | Extensive fuzzing, property-based testing |

---

## Phase 6: Finalization & Documentation (Day 14)

### Goals
- Create user-facing documentation
- Document migration path
- Create tuning guide
- Prepare for production deployment

### Estimated Duration: 1 day

### Deliverables

**Files to Create:**
- `docs/PLUGGABLE_OPTIMIZATIONS.md` (~100 lines)
  - User guide for pluggable optimizations
  - How to enable/configure
  - Examples for common use cases

- `docs/OPTIMIZATION_TUNING_GUIDE.md` (~150 lines)
  - When to use which strategy
  - How to tune parameters
  - Performance troubleshooting

- `docs/OPTIMIZATION_MIGRATION_GUIDE.md` (~80 lines)
  - How to migrate from old global settings
  - Per-table migration checklist
  - Rollback instructions

**Files to Update:**
- `PROJECT_MAP.md`
  - Add pluggable optimization architecture section
  - Document current status (production ready)
  - Link to design docs

- `README.md` (if exists at MarbleDB level)
  - Mention pluggable optimizations
  - Link to docs

### Tasks Breakdown

**Morning (4h): User Documentation**
- [ ] Create `PLUGGABLE_OPTIMIZATIONS.md`
  - [ ] Overview of architecture
  - [ ] Quick start guide
  - [ ] Examples:
    - [ ] RDF triple store configuration
    - [ ] OLTP key-value configuration
    - [ ] Time-series configuration
  - [ ] API reference

- [ ] Create `OPTIMIZATION_TUNING_GUIDE.md`
  - [ ] When to use BloomFilterStrategy
  - [ ] When to use CacheStrategy
  - [ ] When to use SkippingIndexStrategy
  - [ ] When to use TripleStoreStrategy
  - [ ] Parameter tuning guide:
    - [ ] bloom_filter_bits_per_key
    - [ ] hot_cache_size_mb
    - [ ] skipping_index_granularity
  - [ ] Performance troubleshooting

**Afternoon (4h): Migration Guide + Final Cleanup**
- [ ] Create `OPTIMIZATION_MIGRATION_GUIDE.md`
  - [ ] Step-by-step migration from old global settings
  - [ ] Feature flag approach (gradual rollout)
  - [ ] Rollback procedure if issues arise
  - [ ] Monitoring recommendations

- [ ] Update `PROJECT_MAP.md`
  - [ ] Add "Pluggable Optimization Architecture" section
  - [ ] Link to design docs
  - [ ] Document status: Production Ready

- [ ] Code cleanup
  - [ ] Remove debug logging
  - [ ] Add final code comments
  - [ ] Format code (clang-format)

- [ ] Final testing
  - [ ] Run full test suite one last time
  - [ ] Verify documentation examples work
  - [ ] Check for TODOs in code

### Dependencies
- Phase 5 complete (all testing done)
- Performance benchmarks complete

### Success Criteria
- ✅ User documentation clear and complete
- ✅ Migration guide provides step-by-step instructions
- ✅ Tuning guide covers all common scenarios
- ✅ PROJECT_MAP.md updated
- ✅ Code clean and well-commented

### Risks & Mitigation
| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Documentation incomplete | Low | Low | Review with stakeholders |
| Examples don't work | Low | Medium | Test all examples in docs |

---

## Summary Timeline

```
Week 1:
Mon (Day 1):  Phase 0 - Planning & Documentation ✅
Tue (Day 2):  Phase 1 - Base interfaces
Wed (Day 3):  Phase 1 - Factory skeleton + tests
Thu (Day 4):  Phase 2 - BloomFilterStrategy
Fri (Day 5):  Phase 2 - CacheStrategy + SkippingIndexStrategy

Week 2:
Mon (Day 6):  Phase 2 - TripleStoreStrategy
Tue (Day 7):  Phase 3 - Schema detection + auto-config
Wed (Day 8):  Phase 3 - WorkloadHints + testing
Thu (Day 9):  Phase 4 - Read path integration
Fri (Day 10): Phase 4 - Write path + compaction

Week 3:
Mon (Day 11): Phase 4 - Dual path validation
Tue (Day 12): Phase 5 - Test suite execution
Wed (Day 13): Phase 5 - Performance benchmarks
Thu (Day 14): Phase 6 - Documentation + finalization
```

---

## Risk Register

### High Priority Risks

| Risk | Phase | Mitigation | Owner |
|------|-------|------------|-------|
| Performance regression | 4-5 | Continuous benchmarking, optimize hot paths | Dev |
| Incorrect results from new system | 4 | Dual code path validation, extensive testing | Dev |
| Memory leaks in caching | 2-5 | Valgrind testing, RAII patterns | Dev |
| Integration breaks existing code | 4 | Non-invasive changes, thorough testing | Dev |

### Medium Priority Risks

| Risk | Phase | Mitigation | Owner |
|------|-------|------------|-------|
| Auto-config suboptimal choices | 3 | Extensive testing, allow manual override | Dev |
| Serialization bugs | 2, 4 | Test roundtrip, SSTable compatibility | Dev |
| Build system issues | 1 | Test build frequently, simple CMake changes | Dev |

### Low Priority Risks

| Risk | Phase | Mitigation | Owner |
|------|-------|------------|-------|
| Documentation incomplete | 6 | Review with stakeholders | Dev |
| Unknown edge cases | 5 | Fuzzing, property-based testing | Dev |

---

## Success Metrics

### Functional Metrics
- ✅ All existing tests pass (100%)
- ✅ All new tests pass (100%)
- ✅ Code coverage ≥ 90% for new code
- ✅ No memory leaks (Valgrind clean)
- ✅ No crashes in stress testing (1h @ 1M ops/sec)

### Performance Metrics
- ✅ RDF triple queries: 2-5x faster
- ✅ OLTP hot key access: 10-50x faster
- ✅ Time-series range scans: 100-1000x faster
- ✅ Memory overhead: < 20%
- ✅ No regression on existing workloads (within 5%)

### Documentation Metrics
- ✅ Design doc complete and reviewed
- ✅ User guide published
- ✅ Tuning guide published
- ✅ Migration guide published
- ✅ API documentation complete

---

## Rollout Plan (Post-Phase 6)

### Stage 1: Alpha Testing (Week 4)
- Deploy to test environment
- Enable for non-critical tables
- Monitor performance and errors
- Gather feedback

### Stage 2: Beta Testing (Week 5-6)
- Deploy to staging environment
- Enable for critical tables (with monitoring)
- Run production-like workloads
- Validate performance improvements

### Stage 3: Production Rollout (Week 7+)
- Gradual rollout per-table
- Start with tables that benefit most (time-series, RDF)
- Monitor metrics closely
- Rollback plan ready

### Stage 4: Old Code Deprecation (Week 12+)
- Mark old optimization code as deprecated
- Migrate all tables to new system
- Remove old code in future release

---

**END OF ROADMAP**

This roadmap provides a detailed, step-by-step plan for implementing the pluggable optimization architecture. Each phase has clear deliverables, success criteria, and risk mitigation strategies.
