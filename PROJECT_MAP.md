# Sabot Project Map

**Version:** 0.1.0
**Last Updated:** November 26, 2025
**Status:** Alpha - Experimental

## Quick Summary

**What Works**:
- ✅ Arrow columnar operations
- ✅ SQL via DuckDB integration
- ✅ Basic stream operators (filter, map, window)

**Partially Working**:
- ⚠️ RDF/SPARQL - basic queries, rough around edges
- ⚠️ Kafka integration - basic source/sink
- ⚠️ State backends - memory works, others experimental

**Not Working**:
- ❌ Cypher/Graph queries - parser incomplete
- ❌ Distributed execution - infrastructure only
- ❌ Production streaming

## Repository Structure

```
Sabot/
├── sabot/                    # Main Python package
│   ├── _c/                   # C++ implementations
│   │   ├── agent_core.*      # C++ agent core
│   │   ├── local_executor.*  # Local execution mode
│   │   └── *.so              # Built C++ modules (6 modules)
│   ├── _cython/              # Cython modules (70+ built)
│   │   ├── kafka/            # Kafka C++ bindings (NEW)
│   │   ├── checkpoint/       # Checkpoint coordination
│   │   ├── state/            # State backends
│   │   ├── shuffle/          # Network shuffle
│   │   ├── operators/        # Stream operators
│   │   ├── fintech/          # Fintech kernels (11 built)
│   │   └── graph/            # Graph query engine (incomplete)
│   │       ├── compiler/     # Cypher parser (incomplete)
│   │       ├── engine/       # GraphQueryEngine
│   │       ├── query/        # Pattern matching kernels
│   │       ├── storage/      # PyPropertyGraph
│   │       └── traversal/    # Graph algorithms
│   ├── api/                  # Public Stream API
│   ├── kafka/                # Kafka Python layer
│   ├── agent.py              # Agent with C++ core integration
│   └── app.py                # Application orchestrator
│
├── sabot_sql/                # SQL Engine
│   ├── include/              # C++ headers
│   │   └── sabot_sql/
│   │       ├── sql/          # SQL engine headers
│   │       │   └── string_operations.h  # Arrow string kernels (NEW)
│   │       └── streaming/    # Streaming SQL headers
│   │           ├── kafka_connector.h
│   │           ├── schema_registry_client.h
│   │           └── avro_decoder.h
│   ├── src/                  # C++ implementations
│   │   ├── sql/
│   │   │   ├── simple_sabot_sql_bridge.cpp
│   │   │   └── string_operations.cpp  # Arrow string ops (NEW)
│   │   └── streaming/
│   │       ├── kafka_connector.cpp
│   │       ├── schema_registry_client.cpp
│   │       └── avro_decoder.cpp
│   ├── build/                # CMake build output
│   │   └── libsabot_sql.dylib  # Built library
│   ├── sabot_sql.pyx         # Cython wrapper (needs build)
│   ├── sabot_sql_duckdb_direct.py  # Temp: DuckDB direct (ACTIVE)
│   └── CMakeLists.txt        # Build configuration
│
├── MarbleDB/                 # Arrow-native LSM storage engine
│   ├── include/marble/       # C++ headers
│   │   ├── api.h             # Main MarbleDB API
│   │   ├── db.h              # Database interface
│   │   ├── table.h           # Table management
│   │   ├── lsm_tree.h        # LSM tree implementation
│   │   ├── sstable.h         # SSTable format
│   │   ├── bloom_filter.h    # Bloom filters
│   │   ├── skipping_index.h  # Data skipping indexes
│   │   ├── hot_key_cache.h   # Hot key caching
│   │   ├── optimization_strategy.h      # NEW: Pluggable optimizations
│   │   ├── optimization_factory.h       # NEW: Auto-configuration
│   │   └── optimizations/    # NEW: Strategy implementations
│   │       ├── bloom_filter_strategy.h
│   │       ├── cache_strategy.h
│   │       ├── skipping_index_strategy.h
│   │       └── triple_store_strategy.h
│   ├── src/core/             # C++ implementations
│   │   ├── api.cpp           # Main implementation
│   │   ├── lsm_storage.cpp   # LSM tree logic
│   │   ├── sstable.cpp       # SSTable read/write
│   │   ├── compaction.cpp    # Compaction strategies
│   │   ├── rocksdb_adapter.cpp  # RocksDB compatibility layer
│   │   ├── optimization_strategy.cpp    # NEW: Base framework
│   │   ├── optimization_factory.cpp     # NEW: Factory logic
│   │   └── optimizations/    # NEW: Strategy implementations
│   ├── docs/                 # MarbleDB documentation
│   │   ├── planning/         # Architecture & planning docs
│   │   │   ├── PLUGGABLE_OPTIMIZATIONS_DESIGN.md  # NEW: Architecture design
│   │   │   └── OPTIMIZATION_REFACTOR_ROADMAP.md   # NEW: Implementation plan
│   │   └── archive/          # Historical design docs
│   ├── tests/                # MarbleDB tests
│   │   ├── unit/             # Unit tests
│   │   └── integration/      # Integration tests
│   ├── build/                # CMake build output
│   │   └── libmarble.a       # Built static library
│   └── CMakeLists.txt        # Build configuration
│
├── vendor/                   # Vendored dependencies
│   ├── arrow/                # Apache Arrow C++ (22.0.0)
│   ├── librdkafka/           # Kafka C++ client
│   ├── simdjson/             # SIMD JSON parser
│   ├── avro/                 # Apache Avro C++
│   ├── protobuf/             # Google Protobuf
│   ├── rocksdb/              # RocksDB
│   ├── duckdb/               # DuckDB
│   ├── tonbo/                # Tonbo Rust DB
│   └── cpp-datetime/         # C++ DateTime library (NEW) ✅
│       ├── src/              # datetime.cpp/h, timespan.cpp/h
│       └── build/            # libdatetime.a (33KB static library)
│
├── archive/                  # Archived code (not in active use)
│   └── graph_implementations/  # Abandoned graph implementation attempts
│       ├── abandoned_kuzu_fork/     # sabot_cypher (Kuzu vendor, never built)
│       └── abandoned_cpp_bridge/    # sabot_graph (C++ bridge, not implemented)
│
├── examples/                 # Working examples (14 core examples)
├── benchmarks/               # Performance benchmarks (organized)
│   ├── vs_pyspark/           # PySpark comparison benchmarks (6 files)
│   ├── vs_duckdb/            # DuckDB/ClickBench comparisons (11 files)
│   ├── internal/             # Component benchmarks (14 files)
│   │   ├── operators/        # Operator benchmarks
│   │   ├── state/            # State backend benchmarks
│   │   ├── shuffle/          # Shuffle benchmarks
│   │   ├── memory/           # Memory benchmarks
│   │   ├── graph/            # Graph benchmarks
│   │   └── cpp/              # C++ optimization benchmarks
│   ├── pipelines/            # Full pipeline benchmarks (5 files)
│   ├── domain/               # Domain-specific benchmarks (4 files)
│   ├── studies/              # Research studies (kuzu, rdf, postgresql_cdc)
│   └── results/              # Benchmark results
├── tests/                    # Test suite (organized)
│   ├── unit/                 # Unit tests (117 files)
│   │   ├── agent/            # Agent tests (3 files)
│   │   ├── sql/              # SQL engine tests (6 files)
│   │   ├── graph/            # Graph/Cypher tests (27 files)
│   │   ├── sparql/           # SPARQL/RDF tests (1 file)
│   │   ├── operators/        # Operator tests (10 files)
│   │   ├── api/              # API tests
│   │   ├── arrow/            # Arrow tests
│   │   ├── cython/           # Cython tests
│   │   ├── compiler/         # Compiler tests
│   │   ├── shuffle/          # Shuffle tests
│   │   └── state/            # State tests
│   ├── integration/          # Integration tests (52 files)
│   │   ├── agent/            # Agent integration (1 file)
│   │   ├── sql/              # SQL integration (1 file)
│   │   ├── sparql/           # SPARQL integration (1 file)
│   │   ├── test_asof_join.py # Fintech ASOF join tests
│   │   ├── test_fintech_kernels.py # Fintech kernel tests
│   │   └── postgresql_cdc/   # PostgreSQL CDC tests
│   ├── debug/                # Debug/diagnostic tests (5 files)
│   ├── cpp/                  # C++ test executables and sources (9 files)
│   ├── manual/               # Manual tests
│   ├── performance/          # Performance tests
│   ├── test_venv/            # Test virtual environment
│   ├── qlever_test/          # QLever test data
│   └── .qlever_test_env/     # QLever test environment
└── docs/                     # Documentation (organized)
    ├── architecture/         # Architecture and design docs
    ├── benchmarks/           # Benchmark results and analysis
    ├── features/             # Feature-specific documentation
    │   ├── kafka/            # Kafka integration docs
    │   ├── sql/              # SQL engine docs
    │   ├── graph/            # Graph/Cypher docs
    │   ├── fintech/          # Fintech kernels docs
    │   └── cpp_agent/        # C++ agent docs
    ├── guides/               # User guides and tutorials
    ├── planning/             # Roadmaps and planning docs
    └── session-reports/      # Historical session reports
```

## Core Components Status

### Agent Architecture ✅

**Files**:
- `sabot/_c/agent_core.{hpp,cpp}` - C++ agent implementation
- `sabot/_c/local_executor.{hpp,cpp}` - Local execution
- `sabot/_cython/agent_core.pyx` - Cython wrapper (build issues)
- `sabot/_cython/local_executor.pyx` - Cython wrapper (build issues)
- `sabot/agent.py` - Python agent with C++ integration

**Status**: 
- ✅ C++ core implemented
- ✅ Python fallback working
- ⚠️ Cython wrapper has minor issues (not blocking)
- ✅ All examples work with fallback

### Kafka Integration ✅

**C++ Layer** (`sabot_sql/src/streaming/`):
- ✅ `kafka_connector.cpp` - librdkafka integration
- ✅ `schema_registry_client.cpp` - Schema Registry HTTP client
- ✅ Wire format support (magic byte + schema ID)
- ⏳ `avro_decoder.cpp` - Basic Avro (advanced version exists)
- ⏳ `protobuf_decoder.cpp` - Basic Protobuf (commented out due to build)
- ✅ simdjson integration - 3-4x faster JSON

**Cython Layer** (`sabot/_cython/kafka/`):
- ✅ `librdkafka_source.pyx` - Source wrapper
- ✅ `librdkafka_sink.pyx` - Sink wrapper

**Python Layer** (`sabot/kafka/`):
- ✅ `source.py` - aiokafka fallback
- ✅ `sink.py` - Producer
- ✅ `schema_registry.py` - Python client

**Performance**: 5-8x faster than Python-only (proven in benchmarks)

### SQL Engine ⚠️

**C++ Implementation** (`sabot_sql/`):
- ✅ DuckDB parser/optimizer integration
- ✅ Arrow-based execution
- ⏳ String operations using Arrow compute (NEW, not integrated yet)
- ✅ Streaming SQL infrastructure

**Current Active Implementation**:
- `sabot_sql_duckdb_direct.py` - Temporary direct DuckDB wrapper
- Uses DuckDB for real SQL execution
- Competitive performance (within 2x of pure DuckDB)

**Cython Wrapper**:
- `sabot_sql.pyx` - Needs build (numpy header issues)
- Will use when build issues resolved

**Performance vs DuckDB** (verified):
- Sabot wins 22/37 ClickBench queries
- DuckDB wins 13/37 queries (string operations)
- Overall: DuckDB ~1.3x faster (string advantage)

### State Management ✅

**Backends**:
- ✅ MemoryBackend - In-memory state
- ✅ RocksDBBackend - Persistent state  
- ✅ StateBackend fallback - Simple dict-based

**Usage**: Working in examples

### Streaming ✅

**Infrastructure**:
- ✅ Watermark tracking
- ✅ Window operators
- ✅ Checkpoint coordination
- ✅ Barrier injection
- ✅ Agent distribution

**Status**: Infrastructure complete, integration in progress

### RDF/SPARQL ⚠️ BASIC FUNCTIONALITY

**Implementation** (`sabot/rdf.py`, `sabot_ql/`):
- ⚠️ RDF triple storage (basic)
- ⚠️ SPARQL parser (basic queries work)
- ⚠️ Python API exists but rough

**What Works**:
- Basic SELECT queries
- Simple triple patterns
- LIMIT, OFFSET

**What's Rough**:
- Error handling
- Complex queries may fail
- Performance not optimized
- Missing OPTIONAL, UNION

**Status**: Functional for basic queries, needs polish

### MarbleDB Storage Engine 🔄 ARCHITECTURE REFACTOR IN PROGRESS

**Overview**:
MarbleDB is an Arrow-native LSM storage engine designed for multiple workloads:
- RDF triple stores (SPARQL queries)
- OLTP key-value (session stores, caching)
- Time-series analytics (metrics, logs)
- Property graphs (Cypher queries)

**Current Status** (`MarbleDB/`):
- ✅ Core LSM tree implementation
- ✅ Arrow RecordBatch storage
- ✅ SSTable format with Arrow IPC
- ✅ RocksDB compatibility layer
- ✅ Compaction strategies
- ✅ Bloom filters (RDF-specific, hardcoded)
- ✅ Hot key cache (designed but not integrated)
- ✅ Skipping indexes (built incrementally)
- 🔄 **Pluggable Optimization Architecture** (NEW)

**Recent Performance Improvements**:
- ✅ Batch cache: 20x read improvement (99.7K → 2.0M ops/sec)
- ✅ Hot key cache integration: Ready for skewed workloads
- ✅ RocksDB Put buffering: Optimized with InsertBatch

**Pluggable Optimization Architecture** 🚀 **Phase 0: Planning Complete**

**Problem**: Current optimizations are hardcoded globally:
- Bloom filters hardcoded for RDF triples (3 int64 columns)
- Time-series workloads pay bloom filter overhead despite only doing range scans
- No way to configure optimizations per-table

**Solution**: Strategy pattern for pluggable, per-table optimizations

**Design Docs**:
- 📋 `MarbleDB/docs/planning/PLUGGABLE_OPTIMIZATIONS_DESIGN.md` (55KB)
  - Comprehensive architecture design
  - API specifications
  - Migration strategy
  - Expected performance improvements

- 📋 `MarbleDB/docs/planning/OPTIMIZATION_REFACTOR_ROADMAP.md` (63KB)
  - 6-phase implementation plan (14 days)
  - Detailed task breakdowns
  - Success criteria for each phase
  - Risk assessment and mitigation

**Architecture Overview**:
```
OptimizationFactory (auto-detect schema)
    ↓
OptimizationPipeline (compose strategies)
    ↓
├─ BloomFilterStrategy     (RDF, key-value)
├─ CacheStrategy          (OLTP, hot keys)
├─ SkippingIndexStrategy  (time-series, analytics)
└─ TripleStoreStrategy    (RDF-specific)
```

**Implementation Strategy**:
- ✅ Phase 0: Planning & Documentation (COMPLETE)
- 📋 Phase 1: Core Infrastructure (Days 2-3)
  - Base OptimizationStrategy interface
  - OptimizationPipeline framework
  - Integration with ColumnFamilyOptions

- 📋 Phase 2: Strategy Implementations (Days 4-6)
  - BloomFilterStrategy
  - CacheStrategy
  - SkippingIndexStrategy
  - TripleStoreStrategy

- 📋 Phase 3: Auto-Configuration (Days 7-8)
  - Schema type detection (RDF vs key-value vs time-series)
  - WorkloadHints system
  - Factory auto-configuration logic

- 📋 Phase 4: Integration & Migration (Days 9-11)
  - Hook integration (Get/Put/Compact/Flush)
  - Dual code paths (old + new systems run in parallel)
  - Validation and performance comparison

- 📋 Phase 5: Comprehensive Validation (Days 12-13)
  - All tests pass (unit + integration)
  - Performance benchmarks
  - Memory profiling

- 📋 Phase 6: Finalization (Day 14)
  - User documentation
  - Tuning guide
  - Migration guide

**Expected Performance Improvements**:
- RDF triple queries: **2-5x faster** (predicate-aware bloom filters)
- OLTP hot key access: **10-50x faster** (adaptive caching)
- Time-series range scans: **100-1000x faster** (skipping indexes)

**Key Benefits**:
- ✅ Per-table optimization configuration
- ✅ Auto-configuration based on schema type
- ✅ Easy to add new optimization strategies
- ✅ Pay only for enabled optimizations (memory efficiency)
- ✅ Incremental migration (new system alongside old code)

**Files Being Created**:
- `include/marble/optimization_strategy.h` - Base interface
- `include/marble/optimization_factory.h` - Factory + auto-config
- `include/marble/optimizations/*.h` - 4 strategy implementations
- `src/core/optimization_strategy.cpp` - Base framework
- `src/core/optimizations/*.cpp` - Strategy implementations

**Files Being Modified**:
- `include/marble/column_family.h` - Add OptimizationConfig
- `src/core/api.cpp` - Integrate optimization hooks
- `src/core/sstable.cpp` - Serialize optimization metadata
- `src/core/lsm_storage.cpp` - Compaction integration

**Migration Approach**:
- Incremental (not big-bang refactor)
- New system runs alongside old code initially
- Per-table opt-in via `optimization_config.auto_configure = true`
- Validation ensures identical results
- Old code removed only after full validation

**Status**:
- ✅ Design complete and reviewed
- ✅ Roadmap documented
- 📋 Implementation Phase 1 ready to start
- 🎯 Target: 14 days to production-ready

## Vendored Dependencies

### Production Dependencies ✅

| Library | Purpose | Status | Size |
|---------|---------|--------|------|
| Arrow C++ | Columnar operations | ✅ Built | ~500MB |
| librdkafka | Kafka client | ✅ Built | ~50MB |
| simdjson | SIMD JSON | ✅ Built | ~5MB |
| avro | Avro codec | ✅ Built | ~20MB |
| protobuf | Protobuf codec | ✅ Built | ~100MB |
| RocksDB | State backend | ✅ Built | ~100MB |
| DuckDB | SQL engine | ✅ Built | ~200MB |
| cpp-datetime | Date/time utilities | ✅ Built | ~33KB |

**All vendored** - no system dependencies required

## Examples Status

### Working Examples ✅ (14/14 core examples)

**Quickstart** (3/3):
- ✅ hello_sabot.py
- ✅ filter_and_map.py  
- ✅ local_join.py

**Local Pipelines** (3/3):
- ✅ streaming_simulation.py
- ✅ window_aggregation.py
- ✅ stateful_processing.py

**Optimization** (1/1):
- ✅ filter_pushdown_demo.py

**Distributed** (1/1):
- ✅ two_agents_simple.py

**Production Patterns** (1/1):
- ✅ stream_enrichment/local_enrichment.py

**API** (2/2):
- ✅ basic_streaming.py
- ✅ unified_api_simple_test.py

**Fintech** (2/2):
- ✅ sabot_sql_pipeline/1_base_enrichment.py
- ✅ sabot_sql_enrichment_demo.py

**Kafka** (1/1):
- ✅ kafka_integration_example.py

### Examples Requiring Build ⚠️

- dimension_tables_demo.py (needs materialization engine)
- asof_join_demo.py (needs fintech kernels)  
- Various graph examples (needs lark parser)

## Build Status

### Cython Modules: 70/108 (65%)

**Core**: 24/24 (100%) ✅
**Graph**: 11/11 (100%) ✅
**Fintech**: 11/13 (85%) ✅
**State**: 8/8 (100%) ✅
**Checkpoint**: 2/2 (100%) ✅
**Shuffle**: 10/10 (100%) ✅
**Operators**: 4/10 (40%) ⚠️

**Missing Modules**:
- online_stats.pyx (fintech)
- Some aggregate operators
- registry_optimized.pyx (GIL issues)

**Impact**: Low - core functionality available

### C++ Libraries: 5/5 (100%) ✅

- ✅ librdkafka
- ✅ simdjson
- ✅ avrocpp_s
- ✅ libprotobuf
- ✅ libsabot_sql.dylib

## Performance

Performance claims in this repository have not been independently verified and should be treated skeptically.

**What we can say:**
- DuckDB SQL execution is fast (it's DuckDB)
- Arrow IPC loading is faster than CSV (expected)
- Cython modules faster than pure Python (expected)

Historical benchmark data exists in `docs/benchmarks/` but may be outdated or inaccurate.

## File Locations

### Want to find...

**Agent code**: `sabot/agent.py`, `sabot/_c/agent_core.cpp`
**Stream API**: `sabot/api/stream.py`
**Kafka**: `sabot/kafka/`, `sabot/_cython/kafka/`, `sabot_sql/src/streaming/kafka_connector.cpp`
**SQL**: `sabot_sql/`, currently using `sabot_sql_duckdb_direct.py`
**Examples**: `examples/00_quickstart/`, `examples/kafka_integration_example.py`
**Benchmarks**:
- PySpark comparisons: `benchmarks/vs_pyspark/`
- DuckDB/ClickBench: `benchmarks/vs_duckdb/`
- Component benchmarks: `benchmarks/internal/`
- Pipeline benchmarks: `benchmarks/pipelines/`
**Tests**:
- Unit tests: `tests/unit/` (agent, sql, graph, sparql, operators, etc.)
- Integration tests: `tests/integration/` (agent, sql, sparql, fintech, etc.)
- Debug tests: `tests/debug/`
- C++ tests: `tests/cpp/` (test executables and source files)
**Docs**:
- Architecture: `docs/architecture/`
- Benchmark results: `docs/benchmarks/`
- Feature docs: `docs/features/` (kafka, sql, graph, fintech, cpp_agent)
- User guides: `docs/guides/` (QUICKSTART.md, DOCUMENTATION.md)
- Planning: `docs/planning/` (NEXT_STEPS.md, ACCOMPLISHMENTS.md)
- Session reports: `docs/session-reports/` (historical session summaries)

## Key Metrics

**Codebase Size**: Large (exact counts not verified)
**Cython Modules**: Many built, functionality varies
**Examples**: Some work, some need setup/dependencies
**Test Coverage**: Low

**Status**: Alpha - experimental project, not production ready

## Critical Findings

### Mock SQL Removed ✅

**Was**: Using mock implementation returning fake data
**Now**: Using real DuckDB execution
**Impact**: Honest benchmarks, correct results

### String Operations Created ✅

**File**: `sabot_sql/src/sql/string_operations.cpp`
**Uses**: Arrow compute SIMD kernels
**Status**: Built, not integrated yet

### Vendored Everything ✅

**All dependencies vendored**:
- No system Arrow
- No pip pyarrow
- Self-contained build

## Next Steps

1. **Complete Cypher parser** - Graph queries don't work
2. **Polish SPARQL** - Basic functionality needs improvement
3. **Expand test coverage** - Currently low
4. **Improve documentation** - Many docs outdated

## Documentation

**Architecture**: `docs/architecture/` - Design docs, unification reports
**Benchmarks**: `docs/benchmarks/` - All performance analysis and results
**Features**: `docs/features/` - Kafka, SQL, Graph, Fintech, C++ Agent docs
**Guides**: `docs/guides/` - QUICKSTART.md, user-facing documentation
**Planning**: `docs/planning/` - Roadmaps, next steps, accomplishments
**Session Reports**: `docs/session-reports/` - Historical development sessions
**Examples**: README files in examples/
**API**: Inline docstrings

**Status**: ✅ Documentation organized into logical folders (125+ files)

---

**Status**: Alpha - experimental, not production ready
**SQL**: Works via DuckDB
**Streaming**: Experimental
**Graph/Cypher**: Not functional
**SPARQL**: Basic queries work