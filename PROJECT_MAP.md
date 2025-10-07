# Sabot Project Map

**Version:** 0.1.0-alpha
**Last Updated:** October 7, 2025
**Total LOC:** ~60,000 lines
**Status:** Experimental / Alpha

**🎯 KEY CHANGE:** Sabot now uses **vendored Apache Arrow C++** - NO pip pyarrow dependency!

This document provides a comprehensive map of the Sabot codebase structure, explaining the purpose of each directory and key files.

---

## Repository Structure Overview

```
sabot/
├── sabot/                 # Main package (~60K LOC)
│   ├── _cython/           # Cython-accelerated modules
│   │   ├── checkpoint/    # Distributed snapshot coordination
│   │   ├── state/         # State backends (Memory, RocksDB)
│   │   ├── time/          # Watermark tracking and timers
│   │   ├── operators/     # Stream transformation operators
│   │   ├── arrow/         # Arrow columnar processing
│   │   └── shuffle/       # Network shuffle transport (NEW)
│   ├── agents/            # Agent runtime system
│   ├── api/               # High-level user-facing API
│   ├── checkpoint/        # Checkpoint coordination (Python wrappers)
│   ├── cluster/           # Distributed cluster coordination
│   ├── core/              # Core stream processing engine
│   ├── execution/         # Flink-style execution graph
│   ├── kafka/             # Kafka integration
│   ├── redis/             # Redis state backend
│   ├── state/             # State management (Python wrappers)
│   ├── time/              # Time and watermark management (Python wrappers)
│   ├── stores/            # State store implementations
│   ├── types/             # Type definitions
│   ├── utils/             # Utilities
│   ├── app.py             # Main App class (1,759 lines)
│   ├── cli.py             # Faust-style CLI (1,468 lines)
│   └── __init__.py        # Public API exports
│
├── examples/              # Example applications
├── tests/                 # Test suite (~21 test files)
├── benchmarks/            # Performance benchmarks
├── vendor/                # Vendored dependencies
│   ├── arrow/             # Apache Arrow C++
│   ├── cyredis/           # CyRedis (Redis client)
│   ├── duckdb/            # DuckDB
│   ├── rocksdb/           # RocksDB C++
│   └── tonbo/             # Tonbo (Rust embedded DB with FFI)
├── docs/                  # Documentation
│   ├── design/            # Design documents
│   │   └── UNIFIED_BATCH_ARCHITECTURE.md  # Core architecture spec
│   ├── implementation/    # Implementation plans
│   │   └── PHASE2_AUTO_NUMBA_COMPILATION.md  # Numba UDF compilation plan
│   └── ...                # Other documentation
├── docker-compose.yml     # Infrastructure (Kafka, Postgres, Redis)
├── setup.py               # Build configuration
└── pyproject.toml         # Project metadata
```

---

## Core Package: `sabot/`

### Entry Points

#### `__init__.py` (192 lines)
**Purpose:** Public API exports and unified namespace (`import sabot as sb`)

**Key Exports:**
- Core: `App`, `RAFTStream`, `create_app()`
- Checkpoint: `Barrier`, `BarrierTracker`, `Coordinator`
- State: `ValueState`, `MapState`, `ListState`, `MemoryBackend`, `RocksDBBackend`
- Time: `WatermarkTracker`, `Timers`, `EventTime`
- API: `Stream`, `OutputStream`, window functions
- Distributed: `ComposableLauncher`, `DistributedCoordinator`

**Status:** ✅ Well-defined public API

---

#### `app.py` (1,759 lines)
**Purpose:** Main application class - core orchestration layer

**Key Classes:**
- `App` - Main application class (lines 1-1400)
  - Agent registration via `@app.agent()` decorator
  - Topic/table management
  - Kafka consumer lifecycle
  - State backend initialization
  - Optional GPU (RAFT) integration
  - Optional durable execution (DBOS-style)

- `Table` - Embedded table abstraction (lines 1401-1529)
  - Key-value table backed by state stores
  - Synchronous/async access patterns

**Status:** ⚠️ Large God object, needs refactoring
**Issues:**
- Mixes too many responsibilities
- Optional dependency failures silently swallowed
- Nested Table class should be extracted

---

#### `cli.py` (1,468 lines)
**Purpose:** Faust-style command-line interface

**Commands:**
- `sabot -A myapp:app worker` - Start worker process
- `sabot agents` - List agents
- `sabot topics` - List topics
- `sabot tables` - List tables
- `sabot web` - Start web UI (stub)

**Status:** ⚠️ Partially implemented, contains mock implementations
**Issues:**
- Mock `create_app()` function (lines 45-61)
- Agent execution not fully wired up
- Needs refactoring into submodules

---

### Cython Acceleration: `sabot/_cython/`

Performance-critical modules implemented in Cython for 10-100x speedup over pure Python.

#### `_cython/checkpoint/` (Checkpoint Coordination)
**Purpose:** Distributed snapshot coordination using Chandy-Lamport algorithm

**Files:**
- `barrier.pyx` / `barrier.pxd` - Barrier message representation
- `barrier_tracker.pyx` / `barrier_tracker.pxd` - Track barriers across channels
- `coordinator.pyx` / `coordinator.pxd` - Checkpoint coordinator
  - Initiates distributed snapshots
  - Coordinates barrier alignment
  - **Performance:** <10μs barrier initiation (measured)
- `storage.pyx` / `storage.pxd` - Checkpoint persistence
- `recovery.pyx` / `recovery.pxd` - Recovery from checkpoints

**Submodule:**
- `dbos/` - DBOS-inspired durable execution integration
  - `durable_checkpoint_coordinator.pyx` - Postgres-backed checkpoints
  - `durable_state_store.pyx` - Durable state management

**Status:** ✅ Core implementation complete, well-designed
**Performance:** Measured <10μs barrier initiation on M1 Pro

---

#### `_cython/state/` (State Management)
**Purpose:** High-performance state backends for stateful stream processing

**Files:**
- `state_backend.pyx` - Abstract state backend interface
- `memory_backend.pyx` - In-memory state backend
  - **Performance:** 1M+ ops/sec (measured)
  - LRU eviction with configurable max_size
  - TTL support
- `rocksdb_state.pyx` - RocksDB-backed state (experimental)
  - Persistent state for large working sets
  - Falls back to SQLite if RocksDB unavailable
  - **Status:** 🚧 Partially implemented
- `primitives.pyx` - State primitives
  - ValueState, ListState, MapState
  - ReducingState, AggregatingState

**Status:** ✅ Memory backend working, RocksDB experimental
**Issues:**
- RocksDB backend has dual code paths (RocksDB/SQLite fallback)
- Complex types stored in-memory instead of persisted

---

#### `_cython/time/` (Time & Watermarks)
**Purpose:** Event-time processing with watermark tracking

**Files:**
- `watermark_tracker.pyx` / `watermark_tracker.pxd`
  - Tracks watermarks across partitions
  - Computes global watermark (min of all partitions)
  - **Performance:** <5μs tracking update (claimed)
- `timers.pyx` / `timers.pxd`
  - Timer service for delayed processing
  - Event-time and processing-time timers
- `event_time.pyx` - Event-time utilities

**Status:** ✅ Core primitives implemented
**Note:** Integration with agent runtime incomplete

---

#### `_cython/operators/` (Stream Operators)
**Purpose:** Cython-accelerated stream transformation operators

**Files:**
- `base_operator.pyx` / `base_operator.pxd` - **NEW**: Base class for all operators (Phase 3)
  - Centralized base class extracted from transform.pyx
  - Provides `process_batch()` and `process_morsel()` interface
  - Handles iteration (__iter__, __aiter__), shuffle interface, metadata
  - Default process_morsel() implementation for morsel-driven parallelism
  - **Status:** ✅ Complete and tested (183 lines)

- `morsel_operator.pyx` / `morsel_operator.pxd` - **NEW**: Morsel-driven parallel execution (Phase 3)
  - Wrapper for automatic morsel-driven parallel execution
  - Heuristics: Batches < 10K rows bypass morsel processing
  - Async parallel processing with configurable workers
  - Result reassembly and statistics tracking
  - **Status:** ✅ Complete and tested (229 lines)

- `filter.pyx` - Filter operator
- `map.pyx` - Map transformation
- `flatmap.pyx` - FlatMap operator
- `aggregation.pyx` - Aggregation operators (sum, count, avg, etc.)
- `join.pyx` - Stream join operators (hash join, nested loop)
- `window.pyx` - Window operators (tumbling, sliding, session)

- `numba_compiler.pyx` / `numba_compiler.pxd` - **NEW**: Auto-Numba JIT compilation
  - Automatic compilation of user-defined functions with Numba
  - AST analysis to detect patterns (loops, NumPy, Pandas, Arrow)
  - Strategy selection: NJIT (loops), VECTORIZE (NumPy), SKIP (Arrow/Pandas)
  - LRU cache (1000 entries) with MD5 source hash keys
  - Graceful fallback to Python if Numba unavailable or compilation fails
  - **Performance:** 10-50x speedup for loops, 50-100x for NumPy operations
  - **Integration:** Automatically called by CythonMapOperator in transform.pyx
  - **Status:** ✅ Complete and tested (Phase 2)

**Status:** ✅ Core operators (base, morsel) complete | 🚧 Specialized operators partially implemented
**Issues:**
- Several operators disabled due to compilation issues
- Not fully integrated with Stream API

---

#### `_cython/arrow/` (Arrow Integration)
**Purpose:** Zero-copy columnar processing with Apache Arrow

**Files:**
- `arrow_core_simple.pyx` - Basic Arrow operations
- `batch_processor.pyx` - Batch-level transformations

**Status:** 🚧 Experimental, integration incomplete
**Note:** Claims vendored Arrow but uses pyarrow from pip

---

#### `_cython/shuffle/` (Shuffle Transport)
**Purpose:** High-performance network shuffle for distributed stream processing

**Files:**
- `flight_transport_lockfree.pyx` / `flight_transport_lockfree.pxd` - Lock-free Arrow Flight transport
  - Zero-copy network data transfer using Arrow Flight RPC
  - Atomic connection pooling with lock-free data structures
  - Client/Server implementation for distributed shuffle
  - **Compiled size:** 138KB
  - **Status:** ✅ Compiled and tested

- `lock_free_queue.pyx` / `lock_free_queue.pxd` - Lock-free queue primitives
  - SPSC (Single Producer Single Consumer) ring buffers
  - MPSC (Multi Producer Single Consumer) ring buffers
  - Lock-free partition queues for concurrent access
  - **Compiled size:** 118KB
  - **Status:** ✅ Compiled and tested

- `atomic_partition_store.pyx` / `atomic_partition_store.pxd` - Atomic partition storage
  - LMAX Disruptor-style lock-free hash table
  - Atomic operations for partition data management
  - High-throughput concurrent partition access
  - **Compiled size:** 113KB
  - **Status:** ✅ Compiled and tested

- `shuffle_transport.pyx` / `shuffle_transport.pxd` - High-level shuffle API
  - `ShuffleServer` class for receiving shuffled data
  - `ShuffleClient` class for sending shuffled data
  - Integrates Flight transport, queues, and partition store
  - **Compiled size:** 146KB
  - **Status:** ✅ Compiled and tested

- `partitioner.pyx` - Partitioning strategies (hash, range, custom)
- `shuffle_buffer.pyx` - Shuffle buffering and batching
- `shuffle_manager.pyx` - Shuffle coordination and lifecycle
- `flight_transport.pyx` - Original Flight transport implementation

**Status:** ✅ Core lock-free transport infrastructure complete
**Performance:** Zero-copy network transfer, lock-free concurrent access
**Use Case:** Distributed joins, aggregations, and repartitioning operations

---

### Agent Runtime: `sabot/agents/`

**Purpose:** Actor-based agent execution system

**Files:**
- `runtime.py` (22,874 lines) - Agent runtime coordinator
  - Process spawning and lifecycle management
  - Supervision strategies (ONE_FOR_ONE, ONE_FOR_ALL, REST_FOR_ONE)
  - Resource monitoring
  - **Status:** ⚠️ Partially stubbed out

- `supervisor.py` (12,618 lines) - Supervisor implementations
  - Hierarchical supervision trees
  - Restart strategies
  - **Status:** 🚧 In progress

- `lifecycle.py` (18,274 lines) - Agent lifecycle management
  - Start, stop, restart hooks
  - Graceful shutdown

- `partition_manager.py` (18,842 lines) - Partition assignment
  - Kafka partition rebalancing
  - Work distribution

- `resources.py` (15,345 lines) - Resource monitoring
  - CPU, memory, I/O tracking
  - Backpressure signals

**Status:** ⚠️ Critical component with incomplete implementation
**Priority:** P0 - Required for production use

---

### High-Level API: `sabot/api/`

**Purpose:** User-facing Stream API (Pythonic, composable)

**Files:**
- `stream.py` (23,506 lines) - Main Stream API
  - `Stream.from_kafka()` - Kafka source
  - `.map()`, `.filter()`, `.flatmap()` - Transformations
  - `.window()` - Windowing operations
  - `.join()` - Stream joins
  - `.select()`, `.group_by()` - SQL-like operations

- `state.py` (11,614 lines) - User-facing state API
  - `ValueState`, `ListState` wrappers
  - Simplified state access patterns

- `window.py` (9,886 lines) - Window functions
  - `tumbling()`, `sliding()`, `session()` windows
  - Window triggers and eviction

**Status:** ✅ Well-designed API, partial implementation
**Design:** Inspired by Flink DataStream API + Arrow integration

---

### Kafka Integration: `sabot/kafka/`

**Purpose:** Apache Kafka source/sink connectors

**Files:**
- `source.py` (9,837 lines) - Kafka source
  - Consumer group management
  - Configurable codecs (JSON, Avro, Protobuf, Arrow, MessagePack)
  - Schema registry integration
  - **Status:** ✅ Basic implementation working

- `sink.py` (10,593 lines) - Kafka sink
  - Producer with buffering
  - Partitioning strategies
  - At-least-once delivery

- `schema_registry.py` (10,199 lines) - Confluent Schema Registry
  - Avro schema management
  - Schema evolution support

**Status:** ✅ Basic Kafka integration functional
**Issues:**
- No batch deserialization
- Limited error handling for Schema Registry failures

---

### Execution Layer: `sabot/execution/`

**Purpose:** Flink-style execution graph and job scheduling

**Files:**
- `job_graph.py` (13,874 lines) - Logical job graph
  - Operator DAG representation
  - Parallelism configuration

- `execution_graph.py` (15,005 lines) - Physical execution graph
  - Task scheduling
  - Slot allocation
  - **Status:** 🚧 Partially implemented

- `slot_pool.py` (15,318 lines) - Task slot management
  - Resource allocation for parallel tasks
  - Slot sharing

**Status:** 🚧 Experimental, not fully integrated

---

### Cluster Management: `sabot/cluster/`

**Purpose:** Distributed coordination for multi-node deployments

**Files:**
- `coordinator.py` (39,954 lines) - Cluster coordinator
  - Leader election
  - Work distribution
  - Health monitoring

- `discovery.py` (18,761 lines) - Service discovery
  - Node registration
  - Peer discovery

- `balancer.py` (14,358 lines) - Load balancing
  - Partition rebalancing
  - Work stealing

- `fault_tolerance.py` (18,590 lines) - Fault tolerance
  - Failure detection
  - Automatic recovery

- `health.py` (19,154 lines) - Health checks
  - Liveness and readiness probes

**Status:** 🚧 Designed but incomplete implementation

---

### Feature Engineering: `sabot/features/`

**Purpose:** High-performance feature engineering for streaming and batch ML pipelines

**Key Components:**

**Feature Extractors** (`extractors.pyx` / `extractors.pxd`)
- Cython-accelerated feature computation on Arrow batches
- `RollingMeanExtractor` - Rolling average over time windows
- `RollingStdExtractor` - Rolling standard deviation
- `PercentileExtractor` - Percentile computation (e.g., 95th percentile)
- `TimeBasedExtractor` - Time-based features (hour, day_of_week)
- Zero-copy Arrow operations for 10-100x speedup

**Feature Store** (`store.py`)
- CyRedis-backed async feature storage
- TTL-based expiration for streaming features
- Batch operations for high throughput
- Key format: `feature:{entity_id}:{feature_name}:{timestamp}`

**Feature Registry** (`registry.py`)
- Centralized feature metadata and validation
- Built-in crypto/fintech features:
  - `price_rolling_mean_5m` - 5-minute price moving average
  - `volume_std_1h` - 1-hour volume standard deviation
  - `spread_percentile_95` - 95th percentile bid-ask spread
- Factory pattern for extractor creation

**Feature Sink** (`sink.py`)
- Stream sink for writing features to CyRedis
- Async batch writes for performance
- Integration with Stream API via `.to_feature_store()`

**Stream API Integration:**
```python
# Apply features using standard operators
stream.with_features([
    'price_rolling_mean_5m',
    'volume_std_1h'
]).to_feature_store(
    feature_store=feature_store,
    entity_key_column='symbol',
    feature_columns=['price_rolling_mean_5m', 'volume_std_1h'],
    ttl=300
)
```

**Helper Functions:**
- `create_feature_map(extractors)` - Convert extractors to map function
- `to_feature_store()` - Async sink for feature storage

**Performance:**
- Throughput: 1-10M events/sec (depending on feature complexity)
- Latency: <10ms per batch
- CyRedis writes: Batched async (1000s of features/write)

**Example:** `examples/crypto_features_demo.py`
- Real-time Coinbase ticker → Feature extraction → CyRedis storage

**Status:** ✅ Core implementation complete, uses standard operators + CyRedis

---

### State Stores: `sabot/stores/`

**Purpose:** Concrete state store implementations

**Files:**
- Memory store (in-process, fast, volatile)
- RocksDB store (persistent, large state)
- Redis store (distributed, shared state)
- PostgreSQL store (durable, ACID)

**Status:** 🚧 Memory store working, others experimental

---

### Supporting Modules

#### `sabot/core/`
- `stream_engine.py` - Core stream processing engine
- `serializers.py` - Codec implementations (JSON, Avro, Arrow, etc.)
- `metrics.py` - Metrics collection
- `_ops.pyx` - Core operators (Cython)

**Status:** ✅ Core engine designed, partial implementation

---

#### `sabot/checkpoint/` (Python Wrappers)
Python wrappers around Cython checkpoint modules for easier access.

**Status:** ✅ Working wrappers

---

#### `sabot/state/` (Python Wrappers)
Python wrappers around Cython state backends.

**Status:** ✅ Working wrappers

---

#### `sabot/time/` (Python Wrappers)
Python wrappers around Cython time modules.

**Status:** ✅ Working wrappers

---

#### `sabot/redis/`
- `redis_client.pyx` - High-performance Redis client (Cython)
- Stream manager for Redis Streams

**Status:** 🚧 Experimental

---

#### `sabot/types/`
Type definitions and protocols for type safety.

**Status:** ✅ Type stubs defined

---

#### `sabot/utils/`
Utility functions and helpers.

---

#### `sabot/observability/`
Observability integrations (metrics, tracing, logging).

**Status:** 🚧 Basic metrics, tracing incomplete

---

#### `sabot/monitoring/`
Runtime monitoring and dashboards.

**Status:** 🚧 Stub implementation

---

## Key Standalone Files

### Root Directory

#### `setup.py` (19,013 lines)
**Purpose:** Build configuration for Cython extensions

**Highlights:**
- Cython module compilation
- Arrow integration (looks for vendored Arrow)
- Conditional compilation based on available dependencies

**Issues:**
- Many Cython modules disabled due to compilation issues:
  - `windows.pyx`, `joins.pyx`, `morsel_parallelism.pyx`, `arrow_core_simple.pyx`

---

#### `pyproject.toml` (215 lines)
**Purpose:** Project metadata and dependencies

**Dependencies:**
- Core: pyarrow, asyncio, structlog, typer, pydantic
- Kafka: aiokafka, confluent-kafka
- Serialization: fastavro, orjson, msgpack
- State: redis, rocksdb (optional)
- Durable execution: sqlalchemy, alembic
- Heavy ML deps: xgboost, lightgbm, prophet, scikit-learn
  - ⚠️ **Note:** Unclear why ML libraries are core dependencies

**Issues:**
- Dependency bloat (40+ core dependencies)
- Uses pip pyarrow instead of vendored version

---

#### `docker-compose.yml` (2,826 lines)
**Purpose:** Infrastructure stack for development/testing

**Services:**
- **Redpanda** (Kafka-compatible) - Port 19092
- **Redpanda Console** - Port 8080 (web UI)
- **PostgreSQL** - Port 5432 (durable execution)
- **Redis** - Port 6379 (distributed state)

**Status:** ✅ Working development environment

---

## Examples: `examples/`

**Purpose:** Example applications and demos (39 Python files)

**Key Examples:**

### 1. **Fraud Detection** (`fraud_app.py`)
- Multi-pattern fraud detection (velocity, amount anomaly, geo-impossible)
- Real-time fraud alerts
- **Measured:** 3K-6K txn/s on M1 Pro
- **Status:** ✅ Working demo

### 2. **Fintech Data Enrichment** (`fintech_enrichment_demo/`)
- Real-time market data processing pipeline
- Multi-stream enrichment (quotes, securities, trades)
- Stream joins and reference data lookups
- **Components:**
  - Data generators (1.2M quotes, 10M securities, 1M trades)
  - Kafka producers (16K rec/sec total throughput)
  - Demonstrates: joins, windowing, stateful enrichment
- **Status:** ✅ Data generators working, Sabot integration TODO

### 3. **Stream Analytics** (`streaming/`)
- `windowed_analytics.py` - Tumbling/sliding window operations
- `multi_agent_coordination.py` - Coordinated agent execution
- **Status:** ✅ Working examples

### 4. **Data Processing** (`data/`)
- `arrow_operations.py` - Columnar processing with Arrow
- Zero-copy operations
- **Status:** ✅ Working example

**Status:** ✅ Basic examples working, fintech demo data generators complete

---

## Tests: `tests/`

**Purpose:** Test suite (26 test files)

**Coverage:**
- Unit tests for core modules
- Integration tests (limited)
- Cython module tests
- **NEW**: Phase 2 tests - NumbaCompiler test suite
  - `tests/unit/test_numba_compilation.py` - NumbaCompiler test suite
  - Pattern detection tests (loops, NumPy, Pandas, Arrow)
  - Compilation strategy tests (NJIT, VECTORIZE, SKIP)
  - Performance benchmarks (5-50x speedup verification)
  - Cache tests (compilation overhead, cache hits)
  - MapOperator integration tests
  - Edge cases (lambdas, builtins, class methods)

- **NEW**: Phase 3 tests - Morsel-driven parallelism (✅ 49 tests passing)
  - `tests/unit/operators/test_base_operator.py` (315 lines, 26 tests)
    - BaseOperator interface, process_batch(), process_morsel()
    - Iteration interfaces (__iter__, __aiter__)
    - Shuffle interface, metadata methods
    - **Status:** ✅ 23 passed, 3 skipped
  - `tests/unit/operators/test_morsel_processing.py` (347 lines, 14 test classes)
    - Default process_morsel() implementation
    - Morsel metadata handling
    - Edge cases (None, empty, filtered)
    - **Status:** ✅ All passing
  - `tests/unit/operators/test_morsel_operator.py` (359 lines, 11 test classes)
    - MorselDrivenOperator heuristics
    - Small batch bypass, large batch processing
    - Statistics collection
    - **Status:** Created
  - `tests/integration/test_morsel_integration.py` (346 lines, 8 test classes)
    - End-to-end morsel integration
    - Different worker counts (2, 4, 8)
    - Large datasets (100K-1M rows)
    - **Status:** Created
  - `tests/integration/test_parallel_correctness.py` (417 lines, 8 test classes)
    - Parallel vs sequential correctness verification
    - Different batch sizes and data types
    - Random data testing
    - **Status:** Created

**Status:** ✅ Phase 3 core tests passing (49 tests) | ⚠️ Overall coverage still low (~10%)
**Priority:** P1 - Phase 3 complete, continue expanding coverage

---

## Benchmarks: `benchmarks/`

**Purpose:** Performance benchmarking scripts

**Key Benchmarks:**
- Fraud detection throughput
- State backend operations
- Checkpoint coordination latency
- **NEW**: `benchmarks/numba_compilation_bench.py` - Numba compilation benchmarks (Phase 2)
  - Simple loop benchmark (10-50x speedup target)
  - Complex math benchmark (multi-operation loops)
  - Batch processing with MapOperator
  - Compilation overhead measurement (<100ms target)
  - Cache hit performance (<1ms)

**Status:** ✅ Basic benchmarks available, Phase 2 benchmarks complete

---

## Vendored Libraries

All vendored dependencies are now in the `vendor/` directory for consistency and better organization.

### `vendor/arrow/` (Apache Arrow C++)
**Purpose:** Self-contained Arrow C++ library - zero pip dependencies

**Structure:**
- `cpp/` - Arrow C++ source code (upstream Apache Arrow)
- `cpp/build/install/` - Built Arrow libraries and headers (created by build.py)
- `python/pyarrow/` - Cython .pxd bindings for Arrow C++ API

**Status:** ✅ **ACTIVE - Primary Arrow implementation**
**Build:** Built via `python build.py` (Phase 2)
**Usage:** All Cython modules use `cimport pyarrow.lib as ca` → resolves to vendored bindings
**Why:** Full version control, optimized builds, no pip dependency conflicts

---

### `vendor/rocksdb/` (RocksDB C++)
**Purpose:** High-performance embedded key-value store for persistent state

**Structure:**
- Root: RocksDB C++ source code (upstream Facebook RocksDB)
- `build/install/` - Built RocksDB libraries and headers (created by build.py)

**Status:** ✅ **ACTIVE - Used by RocksDB-dependent modules**
**Build:** Built via `python build.py` (Phase 3, 10-30 minutes first build)
**Usage:**
- 6 RocksDB-only modules (checkpoint, state, time)
- 2 mixed RocksDB+Tonbo modules (coordinator, storage)
**Why:** Full version control, consistent builds, no Homebrew dependency

---

### `vendor/tonbo/` (Tonbo Rust Embedded DB)
**Purpose:** Rust-based LSM embedded database with FFI interface

**Structure:**
- Root: Tonbo Rust crate source code
- `tonbo-ffi/` - C FFI interface to Tonbo
- `tonbo-ffi/target/release/libtonbo_ffi.dylib` - Pre-built FFI library

**Status:** ✅ **ACTIVE - Used via FFI**
**Build:** Pre-built FFI library (18MB dylib)
**Usage:**
- 7 Tonbo-only modules (operators, shuffle, state)
- 2 mixed RocksDB+Tonbo modules (coordinator, storage)
- Cython modules use `cimport` with tonbo_ffi.pxd
**Why:** Rust performance, columnar storage with Parquet, LSM architecture

---

## Documentation: `docs/`

**Available Docs (20+ markdown files):**
- Architecture guides
- API references
- Implementation status reports
- Roadmaps (Flink parity)
- Getting started guides

**Key Design Documents:**
- `docs/design/UNIFIED_BATCH_ARCHITECTURE.md` - **NEW**: Complete unified streaming/batch architecture
  - Everything is batches (streaming = infinite batching)
  - Auto-Numba UDF compilation
  - Morsel-driven parallelism
  - DBOS control plane
  - Agents as worker nodes
  - Clean data/control plane separation

**Implementation Plans:**
- `docs/implementation/PHASE7_PLAN_OPTIMIZATION.md` - **NEW**: Query optimization implementation guide
  - Filter pushdown (2-5x speedup on filtered joins)
  - Projection pushdown (20-40% memory reduction)
  - Join reordering (10-30% speedup on multi-join queries)
  - Operator fusion (5-15% speedup on chained transforms)
  - Based on DuckDB optimizer architecture
  - 44 hours estimated effort (3 weeks)

**Status:** ✅ Extensive documentation
**New (Oct 2025):**
- ✅ Unified architecture design specification
- ✅ Phase 7 plan optimization implementation guide

---

## Build Artifacts

### `build/`
Cython-compiled C extensions and build intermediates.

### `sabot.egg-info/`
Python package metadata.

### `__pycache__/`, `*.pyc`
Python bytecode cache.

---

## Development Files

### `.venv/`
Virtual environment for development.

### `.git/`
Git repository metadata.

### `.gitignore`
Git ignore patterns.

---

## Summary Statistics

| Metric | Count |
|--------|-------|
| **Total Lines of Code** | ~60,000 |
| **Python/Cython Files** | 8,492 |
| **Test Files** | 21 |
| **Example Files** | 39 |
| **Documentation Files** | 20+ markdown files |
| **Cython Modules** | 30+ (some disabled) |

---

## Module Status Legend

| Symbol | Meaning |
|--------|---------|
| ✅ | Working / Complete |
| 🚧 | In Progress / Partial |
| ⚠️ | Issues / Needs Work |
| ❌ | Not Implemented |

---

## Navigation Tips

1. **Start with:** `sabot/__init__.py` to see public API
2. **Core logic:** `sabot/app.py` for application orchestration
3. **Performance:** `sabot/_cython/` for Cython-accelerated code
4. **User API:** `sabot/api/stream.py` for Stream operations
5. **Integration:** `sabot/kafka/` for Kafka connectors
6. **Network shuffle:** `sabot/_cython/shuffle/` for distributed data transfer
7. **Examples:** `examples/fraud_app.py` for working demo

---

## Key Architectural Layers

```
┌─────────────────────────────────────────────────┐
│  User Code (Applications)                       │
│  examples/fraud_app.py, user apps               │
└─────────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────────┐
│  Public API (sabot/__init__.py)                 │
│  import sabot as sb                             │
│  sb.App, sb.Stream, sb.agent()                  │
└─────────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────────┐
│  High-Level API (sabot/api/)                    │
│  Stream operations, window functions            │
└─────────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────────┐
│  Application Layer (sabot/app.py, cli.py)       │
│  Agent management, CLI, orchestration           │
└─────────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────────┐
│  Core Engine (sabot/core/)                      │
│  Stream engine, serializers, metrics            │
└─────────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────────┐
│  Cython Acceleration (sabot/_cython/)           │
│  Checkpoint, State, Time, Operators, Shuffle    │
│  **10-100x faster than pure Python**            │
└─────────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────────┐
│  Infrastructure (Kafka, Redis, RocksDB, etc.)   │
│  docker-compose.yml services                    │
└─────────────────────────────────────────────────┘
```

---

## Critical Path for Production Readiness

Based on code review, the following modules are blocking production use:

1. **P0 - Critical:**
   - `sabot/agents/runtime.py` - Complete agent execution
   - `tests/` - Expand test coverage from ~5% to 60%+
   - `sabot/cli.py` - Remove mock implementations

2. **P1 - High Priority:**
   - `sabot/_cython/state/rocksdb_state.pyx` - Complete RocksDB backend
   - `sabot/execution/` - Finish execution graph
   - Error handling and recovery throughout

3. **P2 - Medium Priority:**
   - Arrow integration (batch processing)
   - Distributed coordination completion
   - Performance optimization

---

## Questions About the Codebase?

- **"Where is checkpoint coordination?"** → `sabot/_cython/checkpoint/coordinator.pyx`
- **"How do I create a stream?"** → `sabot/api/stream.py` or `sabot/app.py` (via `@app.agent()`)
- **"Where are Kafka connectors?"** → `sabot/kafka/source.py` and `sink.py`
- **"Where is network shuffle?"** → `sabot/_cython/shuffle/` (lock-free Arrow Flight transport)
- **"How do I do feature engineering?"** → `sabot/features/` (extractors + CyRedis store, see `examples/crypto_features_demo.py`)
- **"Why is performance good?"** → Cython modules in `sabot/_cython/`
- **"What's not working?"** → See test coverage (~5%), many stubs in `agents/runtime.py`, `cli.py`
- **"Where are examples?"** → `examples/fraud_app.py` and other files in `examples/`

---

**Last Updated:** October 6, 2025
**Codebase Version:** v0.1.0-alpha
**For More Info:** See [README.md](README.md) or [ARCHITECTURE.md](docs/ARCHITECTURE.md)
