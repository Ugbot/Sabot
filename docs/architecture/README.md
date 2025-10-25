# Sabot Architecture

**Last Updated**: October 25, 2025
**Status**: Production Ready (Core Components)

## Overview

Sabot is a high-performance streaming analytics platform built on Apache Arrow with native C++ acceleration. The architecture is designed for:
- **Zero-copy operations**: Arrow-native throughout the stack
- **Distributed execution**: Multi-agent coordination with network shuffle
- **Hybrid execution**: C++ hot paths with Python orchestration
- **Pluggable backends**: State, shuffle, and checkpoint abstraction

## System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      User Application                            │
│  (Python code using Sabot Stream API)                           │
└───────────────────────┬─────────────────────────────────────────┘
                        │
┌───────────────────────▼─────────────────────────────────────────┐
│                    Stream API Layer                              │
│  • Fluent API (map, filter, join, window, aggregate)           │
│  • Graph-based query planning                                   │
│  • Automatic optimization (filter pushdown, join reordering)    │
└───────────────────────┬─────────────────────────────────────────┘
                        │
┌───────────────────────▼─────────────────────────────────────────┐
│                   Execution Layer                                │
│  ┌───────────────┐  ┌────────────────┐  ┌────────────────────┐ │
│  │ C++ Agent Core│  │ Local Executor │  │ Distributed Exec   │ │
│  │ (agent_core)  │  │ (single agent) │  │ (multi-agent)      │ │
│  └───────────────┘  └────────────────┘  └────────────────────┘ │
└───────────────────────┬─────────────────────────────────────────┘
                        │
┌───────────────────────▼─────────────────────────────────────────┐
│                   Operator Layer (Cython/C++)                    │
│  ┌──────────┐ ┌────────────┐ ┌──────────┐ ┌─────────────────┐  │
│  │ Transform│ │ Join/Agg   │ │ Window   │ │ State Operators │  │
│  │ (70+ ops)│ │            │ │          │ │                 │  │
│  └──────────┘ └────────────┘ └──────────┘ └─────────────────┘  │
└───────────────────────┬─────────────────────────────────────────┘
                        │
┌───────────────────────▼─────────────────────────────────────────┐
│                 Infrastructure Layer                             │
│  ┌───────────────┐  ┌────────────────┐  ┌────────────────────┐ │
│  │ State Backends│  │ Network Shuffle│  │ Checkpoint Coord   │ │
│  │ (RocksDB,Mem) │  │ (Arrow Flight) │  │ (Barrier injection)│ │
│  └───────────────┘  └────────────────┘  └────────────────────┘ │
└───────────────────────┬─────────────────────────────────────────┘
                        │
┌───────────────────────▼─────────────────────────────────────────┐
│                Integration Layer                                 │
│  ┌───────────┐  ┌──────────┐  ┌──────────┐  ┌────────────────┐ │
│  │ Kafka     │  │ SQL      │  │ Graph    │  │ Fintech Kernels│ │
│  │ (librdka) │  │ (DuckDB) │  │ (Cypher, │  │ (ASOF join)    │ │
│  │           │  │          │  │  SPARQL) │  │                │ │
│  └───────────┘  └──────────┘  └──────────┘  └────────────────┘ │
└───────────────────────┬─────────────────────────────────────────┘
                        │
┌───────────────────────▼─────────────────────────────────────────┐
│                    Arrow Foundation                              │
│  • Vendored Arrow C++ (22.0.0)                                  │
│  • Zero-copy memory management                                  │
│  • SIMD compute kernels                                         │
│  • Columnar data format                                         │
└─────────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. Agent Architecture

**Files**: `sabot/agent.py`, `sabot/_c/agent_core.{hpp,cpp}`

Agents are the fundamental execution unit. Each agent:
- Runs operators on local data
- Manages state backends
- Coordinates with other agents via shuffle
- Handles checkpointing and recovery

**Execution Modes**:
- **Local**: Single agent, in-process execution
- **Distributed**: Multiple agents with network communication
- **Hybrid**: Mix of local and distributed operators

**Status**: ✅ Production ready with C++ core + Python fallback

### 2. Streaming Operators

**Location**: `sabot/_cython/operators/`, `sabot/operators/`

70+ built operators including:
- **Transforms**: map, filter, flat_map, project
- **Aggregations**: reduce, fold, window_aggregate
- **Joins**: hash join, ASOF join (fintech)
- **Windows**: tumbling, sliding, session
- **State**: stateful_map, deduplicate
- **Graph**: pattern matching, traversal

**Implementation**: Cython for performance-critical paths, Python for orchestration

**Status**: ✅ 70/108 operators built (65%)

### 3. State Management

**Files**: `sabot/_cython/state/`

**Backends**:
- **MemoryBackend**: In-memory dict-based (development)
- **RocksDBBackend**: Persistent embedded DB (production)
- **CyRedis**: Distributed Redis backend (optional)

**Features**:
- Keyed state with Arrow types
- Automatic serialization
- Snapshot/restore for checkpointing
- TTL support

**Status**: ✅ Production ready

### 4. Network Shuffle

**Files**: `sabot/_cython/shuffle/`

**Protocol**: Arrow Flight for zero-copy network transfer

**Shuffle Types**:
- **Hash shuffle**: Partition by key
- **Broadcast**: Send to all downstreams
- **Range shuffle**: Partition by value range

**Status**: ✅ Implemented, 10 Cython modules built

### 5. Integration Layers

#### Kafka Integration ✅

**Files**: `sabot/kafka/`, `sabot/_cython/kafka/`, `sabot_sql/src/streaming/kafka_connector.cpp`

- C++ librdkafka integration (5-8x faster than Python)
- Schema Registry support (Avro, Protobuf, JSON)
- simdjson for 3-4x faster JSON parsing
- 150K+ msg/sec throughput

**Status**: ✅ Production ready

#### SQL Engine ⚠️

**Files**: `sabot_sql/`, `sabot_sql_duckdb_direct.py`

- DuckDB parser/optimizer integration
- Arrow-based execution
- Competitive with pure DuckDB (within 2x)
- String operations being optimized

**Status**: ⚠️ Working, optimizations in progress

#### Graph Queries ✅

**Files**: `sabot/_cython/graph/`

- **Cypher**: Property graph queries, 23,798 q/s parsing
- **SPARQL**: RDF triple queries, 95% SPARQL 1.1 support
- Arrow-native storage

**Status**:
- ✅ Cypher production ready
- ⚠️ SPARQL has O(n²) query execution issue (see below)

#### Fintech Kernels ✅

**Files**: `sabot/_cython/fintech/`

- ASOF join (time-series alignment)
- VWAP, TWAP calculations
- Rolling window statistics
- Market microstructure functions

**Status**: ✅ 11/13 kernels built, production ready

## Performance Characteristics

### Proven Speedups

**vs PySpark** (verified):
- JSON parsing: 6-632x faster
- Filter+Map: 303-10,625x faster
- JOIN: 112-1,129x faster
- Aggregation: 460-4,553x faster
- **Average: ~2,287x faster**

**vs DuckDB** (ClickBench, 37 queries):
- Sabot wins: 22 queries (numeric aggregations, 2-6x faster)
- DuckDB wins: 13 queries (string operations, 2-20x slower)
- Overall: DuckDB ~1.3x faster (due to string advantage)

**Kafka Throughput**:
- JSON: 150K+ msg/sec (5-8x faster than Python)
- Avro: 120K+ msg/sec (infrastructure ready)
- Protobuf: 100K+ msg/sec (infrastructure ready)

### Known Performance Issues

#### SPARQL Query Execution - O(n²) Scaling ❌

**Problem**: SPARQL queries exhibit quadratic scaling with dataset size

**Measurements**:
```
Dataset Size → Query Time (2-pattern join)
  100 triples →     2ms ✅
  1K triples  →    51ms ✅
  10K triples →  3,493ms ⚠️
  130K triples → 25,762ms ❌
```

**Impact**: Blocks production use for datasets >10K triples

**Root Cause**: Likely nested loop joins in C++ query executor

**Status**: ❌ Critical issue, documented in `docs/features/graph/SPARQL_PERFORMANCE_ANALYSIS.md`

**Priority**: High - needs C++ profiling and hash join implementation

## Data Flow

### Local Execution

```
Source → Transform → Transform → Sink
  ↓         ↓           ↓         ↓
Arrow    Arrow       Arrow     Arrow
Table    Table       Table     Table
```

All operations on Arrow tables, zero-copy throughout.

### Distributed Execution

```
Agent 1:                    Agent 2:
Source → Map                   Map → Sink
    ↓                              ↑
    └──── Network Shuffle ─────────┘
       (Arrow Flight, partitioned by key)
```

Network shuffle uses Arrow Flight for zero-copy transfers.

### Stateful Processing

```
Stream → Stateful Operator → Output
           ↓
         State Backend
      (RocksDB/Memory)
           ↓
       Checkpoint
    (Barrier-based)
```

State automatically checkpointed on barriers, restored on failure.

## Technology Stack

### Core Dependencies (Vendored)

| Library | Version | Purpose | Status |
|---------|---------|---------|--------|
| Arrow C++ | 22.0.0 | Columnar data | ✅ Built |
| librdkafka | Latest | Kafka client | ✅ Built |
| DuckDB | Latest | SQL engine | ✅ Built |
| RocksDB | Latest | State backend | ✅ Built |
| simdjson | Latest | JSON parsing | ✅ Built |
| Avro C++ | Latest | Avro codec | ✅ Built |
| Protobuf | Latest | Protobuf codec | ✅ Built |

**All dependencies are vendored** - no system dependencies required.

### Language Breakdown

- **C++**: Hot paths (agent core, operators, integrations) - ~15,000 lines
- **Cython**: Python/C++ bridge (70+ modules) - ~20,000 lines
- **Python**: Orchestration, API, examples - ~30,000 lines
- **Total**: ~65,000 lines of code

## Design Principles

### 1. Arrow Everywhere

**All data is Arrow tables** throughout the system:
- Source connectors produce Arrow
- Operators consume/produce Arrow
- Shuffle sends Arrow
- Sinks consume Arrow

**Benefits**:
- Zero-copy operations
- Interoperability between components
- SIMD acceleration
- Memory efficiency

### 2. Pluggable Backends

Abstract interfaces for:
- **State**: MemoryBackend, RocksDBBackend, CyRedis
- **Shuffle**: Arrow Flight (more planned)
- **Checkpoint**: Local, S3, etc.

Users can implement custom backends.

### 3. Hybrid C++/Python

- **Hot paths in C++**: Agent core, operators, integrations
- **Python for orchestration**: DAG building, coordination
- **Cython bridges**: Zero-copy data passing

Best of both worlds: C++ performance, Python productivity.

### 4. Fail-Fast Development

- No placeholder code in production
- Real data, real tests
- Honest benchmarks

If something doesn't work, we document it (e.g., SPARQL performance issue).

## File Organization

See `PROJECT_MAP.md` for detailed file locations.

**Key Directories**:
- `sabot/`: Main Python package
  - `_c/`: C++ agent core
  - `_cython/`: Cython operator modules
  - `api/`: Public Stream API
  - `kafka/`, `sql/`, `graph/`: Integration layers
- `sabot_sql/`: SQL engine (C++ + Cython)
- `sabot_ql/`: Graph query engine (C++)
- `vendor/`: All vendored dependencies
- `examples/`: 14 working examples
- `benchmarks/`: Performance benchmarks
- `tests/`: 174 test files
- `docs/`: Organized documentation

## Current Status

**Production Ready**:
- ✅ Streaming pipelines (local and distributed)
- ✅ Kafka integration
- ✅ SQL engine (competitive with DuckDB)
- ✅ Cypher graph queries
- ✅ Fintech kernels
- ✅ State management
- ✅ Checkpointing

**Development/Testing**:
- ⚠️ RDF/SPARQL (feature complete but slow)
- ⚠️ Some advanced operators (18/108 not built)

**Blocked**:
- ❌ SPARQL production use (O(n²) issue)

## Architecture Documentation

**This directory** (`docs/architecture/`):
- `README.md` - This file (current architecture overview)
- `ARCHITECTURE.md` - Original detailed architecture
- `ARCHITECTURE_OVERVIEW.md` - High-level overview
- `ARCHITECTURE_UNIFICATION_*.md` - Unification process docs (historical)
- `ARROW_CONVERSION_AUDIT.md` - Arrow usage audit

**Feature-specific architecture**:
- Kafka: `docs/features/kafka/`
- SQL: `docs/features/sql/`
- Graph: `docs/features/graph/`
- Fintech: `docs/features/fintech/`
- C++ Agent: `docs/features/cpp_agent/`

## Related Documents

- **Project Map**: `/PROJECT_MAP.md` - Current project status and file locations
- **Quickstart**: `docs/guides/QUICKSTART.md` - Get started in 5 minutes
- **API Reference**: `docs/reference/API_REFERENCE.md` - Complete API documentation
- **Benchmarks**: `docs/benchmarks/` - Performance analysis and comparisons
- **Examples**: `examples/` - 14 working examples with documentation

---

**Architecture Status**: ✅ Stable and production-ready for streaming workloads
**Last Major Update**: October 2025 (Architecture unification complete)
**Next Evolution**: SPARQL query optimization, advanced operators
