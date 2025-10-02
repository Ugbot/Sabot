# Sabot Project Map

**Version:** 0.1.0-alpha
**Last Updated:** October 2, 2025
**Total LOC:** ~60,000 lines
**Status:** Experimental / Alpha

This document provides a comprehensive map of the Sabot codebase structure, explaining the purpose of each directory and key files.

---

## Repository Structure Overview

```
sabot/
├── sabot/                 # Main package (~60K LOC)
│   ├── _cython/           # Cython-accelerated modules
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
├── rocksdb/               # Vendored RocksDB Python bindings
├── tonbo/                 # Vendored Tonbo (Rust embedded DB)
├── docs/                  # Documentation
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
- `filter.pyx` - Filter operator
- `map.pyx` - Map transformation
- `flatmap.pyx` - FlatMap operator
- `aggregation.pyx` - Aggregation operators (sum, count, avg, etc.)
- `join.pyx` - Stream join operators (hash join, nested loop)
- `window.pyx` - Window operators (tumbling, sliding, session)

**Status:** 🚧 Partially implemented, some disabled in setup.py
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

**Purpose:** Test suite (21 test files)

**Coverage:**
- Unit tests for core modules
- Integration tests (limited)
- Cython module tests

**Status:** ⚠️ Insufficient coverage (~5%)
**Priority:** P0 - Critical for production readiness

---

## Benchmarks: `benchmarks/`

**Purpose:** Performance benchmarking scripts

**Key Benchmarks:**
- Fraud detection throughput
- State backend operations
- Checkpoint coordination latency

**Status:** ✅ Basic benchmarks available

---

## Vendored Libraries

### `rocksdb/` (Vendored RocksDB Python Bindings)
**Purpose:** Embedded key-value store for persistent state

**Status:** 🚧 Present but integration incomplete
**Note:** RocksDB backend falls back to SQLite if unavailable

---

### `tonbo/` (Vendored Tonbo - Rust Embedded DB)
**Purpose:** Alternative embedded database (Rust-based)

**Status:** 🚧 Submodule present but not integrated
**Note:** Experimental, not used in current implementation

---

### `third-party/arrow/` (Vendored Apache Arrow)
**Purpose:** Columnar in-memory format and compute engine

**Status:** ⚠️ **Claimed but not actually used**
**Issue:** setup.py looks for vendored Arrow, but pyproject.toml uses `pyarrow>=10.0.0` from pip

---

## Documentation: `docs/`

**Available Docs (20+ markdown files):**
- Architecture guides
- API references
- Implementation status reports
- Roadmaps (Flink parity)
- Getting started guides

**Status:** ✅ Extensive documentation (sometimes aspirational)

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
6. **Examples:** `examples/fraud_app.py` for working demo

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
│  Checkpoint, State, Time, Operators             │
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
- **"Why is performance good?"** → Cython modules in `sabot/_cython/`
- **"What's not working?"** → See test coverage (~5%), many stubs in `agents/runtime.py`, `cli.py`
- **"Where are examples?"** → `examples/fraud_app.py` and other files in `examples/`

---

**Last Updated:** October 2, 2025
**Codebase Version:** v0.1.0-alpha
**For More Info:** See [README.md](README.md) or [ARCHITECTURE.md](docs/ARCHITECTURE.md)
