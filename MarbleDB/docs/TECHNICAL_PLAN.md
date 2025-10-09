# MarbleDB Technical Plan

## Overview
MarbleDB is a high-performance, distributed analytical database built on Apache Arrow, designed for time-series and real-time analytics workloads. It combines QuestDB-style ingestion performance with ClickHouse-class analytical capabilities and ArcticDB-style bitemporal versioning, all distributed via Raft consensus for strong consistency.

**Vision**: A unified analytical database that excels at both ingestion and complex queries, with time travel, real-time streaming, and distributed consistency.

## Core Architecture

### 1. Arrow-First Storage Hierarchy

MarbleDB implements a hierarchical storage format optimized for analytical workloads:

```
Segment (immutable, atomic commit unit)
├── Stripe (co-accessed columns for late materialization)
│   ├── Column Chunk (contiguous values)
│   │   └── Page (Arrow IPC buffers, compressed)
│   │       └── Footer (checksums, statistics, metadata)
└── Segment Footer (checksums, row counts, min/max timestamps, bloom filters)
```

**Key Design Decisions:**
- **Zero-copy access**: Arrow IPC enables direct memory mapping
- **Columnar stripes**: Groups frequently co-accessed columns
- **Page-level indexing**: Fine-grained pruning and access
- **Segment immutability**: Enables atomic commits and time travel

### 2. Time-Series Partitioning Strategy

**Primary Partitioning**: Time-based windows (1m/5m/1h/day configurable)
**Secondary Clustering**: Multi-column sort keys (e.g., `(symbol, tenant, shard)`)
**Hot Segments**: 32–256 MB in-memory segments, time or size-based sealing

### 3. Bitemporal Versioning

**System Time (MVCC)**: `AS OF <timestamp | snapshot_id>` for transaction isolation
**Valid Time**: Implicit `valid_from`, `valid_to` columns for temporal modeling
**Storage**: Delete vectors + overlay for efficient point-in-time reconstruction

### 4. Distributed Consensus Layer

**Raft Integration**: Strong consistency across cluster nodes
- **State Machines**: WAL replication + schema change coordination
- **Persistent Logs**: Crash-recoverable operation history
- **Dynamic Membership**: Add/remove nodes without downtime
- **Arrow Flight Transport**: High-performance inter-node communication

## Ingestion Pipeline

### 1. High-Throughput Append-Only Ingestion

**Flight DoPut Interface**: Streaming Arrow RecordBatch ingestion
- **Partitioning**: Automatic routing to time + cluster key partitions
- **Lock-free staging**: Multiple concurrent writers to hot segments
- **Backpressure**: Bounded queues with overload strategies

**Disruptor Stages**:
1. **Receive** → Validate → Schema evolve → Encode/compress
2. **Build indexes** → Seal → Fsync → Manifest commit

**Performance Targets**:
- Sustained ingest: > 10-50 MB/s/core
- 99p latency to visibility: < 1-2 seconds
- Hot segment sealing: < 100ms blocking

### 2. Schema Evolution & Backpressure

**Additive Schema Changes**: New columns with automatic null backfill
**Type Safety**: Compatible type widening (int32 → int64, etc.)
**Concurrent Writers**: Fair queuing with configurable backpressure policies

## Execution Engine & Query Processing

### 1. Vectorized Kernel Architecture

**SIMD-Optimized Operations**:
- Filter, project, hash aggregate, partial sort
- Late materialization: Filter keys first, fetch payloads second
- Rolling windows: SIMD-accelerated temporal aggregations

**Performance Goals**:
- 60-80% CPU time in vectorized kernels
- Cache-aligned batch processing (64k rows typical)
- NUMA-aware thread placement

### 2. Multi-Level Pruning Strategy

**Pruning Hierarchy** (evaluated in order):
1. **Manifest Pruning**: Partition/time window elimination
2. **Stripe Synopsis**: Composite mini-indexes for hot queries
3. **Page Zone Maps**: Min/max/null counts + quantiles
4. **Bloom Filters**: Membership testing (fixed-width + token/n-gram)
5. **Dictionary Checks**: Low-cardinality optimizations
6. **Adaptive Replanning**: Adjust strategy based on observed selectivity

**Index Formats**:
- **Zone Maps**: `min, max, nulls, quantiles[0.25,0.5,0.75]`
- **Bloom Filters**: Target FPR <2%, size optimized per cardinality
- **Interval Trees**: Valid-time range queries
- **Sparse Key Maps**: `(cluster_key) → (segment, page)` for point lookups

### 3. Query Compilation Pipeline

**Deterministic Planner**:
1. Normalize filters (CNF conversion, NOT propagation)
2. Partition pruning by manifest metadata
3. Zone map evaluation (min/max/null checks)
4. Bloom filter pruning for selective predicates
5. Adaptive replanning if selectivity deviates significantly

**Tunable Thresholds**:
- S1 (30%): Synopsis selectivity threshold
- S2 (10%): Zone map selectivity threshold
- S3 (3%): Bloom selectivity threshold
- δ (2×): Adaptive replanning deviation factor

## Storage & Compaction Architecture

### 1. Compaction Strategies

**Minor Compaction**: Merge small hot segments
**Major Compaction**: Recluster by (time, key), rebuild indexes
**TTL Management**: Policy-based retention with vacuum
**Raft Coordination**: Leadership-aware to prevent thrashing

### 2. Caching & I/O Optimization

**Unified Cache Hierarchy**:
- Page cache (decompressed Arrow arrays)
- Footer/synopsis metadata cache
- Bloom filter cache with admission control

**Prefetching**:
- Cooperative readahead based on pruning predictions
- NUMA-local cache placement
- Ghost entries for cache admission decisions

## Distributed Systems Integration

### 1. Raft Consensus Layer

**Completed Integration**:
- `MarbleWalStateMachine`: WAL replication across nodes
- `MarbleSchemaStateMachine`: DDL coordination
- `MarbleLogStore`: Persistent Raft logs with recovery
- `RaftClusterManager`: Dynamic configuration management
- `ArrowFlightTransport`: High-performance inter-node comms

### 2. Failure Recovery & Consistency

**WAL Replay**: Complete partial segments on recovery
**Raft Snapshots**: System-time versioning for time travel
**Exactly-Once Semantics**: Batch IDs prevent duplicates
**Linearizability**: Jepsen-style testing for distributed correctness

## API Design & Tooling

### 1. Embedded C++/Python APIs

**Core Interface**:
```cpp
// Database lifecycle
auto db = MarbleDB::Open("/path/to/db");
db->CreateTable(schema);
db->Append(record_batch);
auto snapshot = db->Commit();

// Query interface
ScanSpec spec;
spec.columns = {"timestamp", "value"};
spec.filter = "timestamp > '2024-01-01'";
spec.as_of = snapshot_id;
auto result = db->Scan(spec);
```

### 2. Flight & ADBC Integration

**Flight Service**:
```cpp
// Streaming ingestion
flight_client.DoPut(descriptor, record_batch);

// Query execution
auto result = flight_client.DoGet(scan_spec_descriptor);
```

**ADBC Driver**: SQL → ScanSpec translation for broad ecosystem compatibility.

### 3. Administrative Tools

**marblectl**: Inspect manifests, dump indexes, tail streams, diff snapshots
**Metrics**: Ingest rate, prune ratio, Bloom FPR, scan efficiency, cache hit rates
**Tracing**: Per-query spans for performance debugging

## 🏛️ MarbleDB Grand Vision & Technical Plan

MarbleDB aims to become the **analytical database that combines the best of QuestDB, ClickHouse, and ArcticDB** - delivering high-performance time-series ingestion, analytical query performance, and bitemporal time travel, all with distributed consistency.

This document outlines the **complete technical vision** and **realistic implementation strategy** to achieve this ambitious goal.

---

## 🎯 The Grand Vision: Analytical Database for the Modern Era

MarbleDB will be a **unified analytical database** that excels at:

1. **Time-Series Ingestion**: QuestDB-level performance for high-velocity data streams
2. **Analytical Queries**: ClickHouse-competitive performance on complex aggregations
3. **Bitemporal Time Travel**: ArcticDB-style versioning for audit and temporal queries
4. **Distributed Consistency**: Raft-based strong consistency without sacrificing performance
5. **Streaming Analytics**: Real-time processing with exactly-once semantics

**Unique Value Proposition**: The only analytical database that delivers all these capabilities without compromise.

---

## 📋 Implementation Strategy: Realistic Path to the Vision

### Phase 1: Foundation (3-4 months) - Establish Technical Feasibility

**Goal**: Working end-to-end system that proves the architectural direction

**Deliverables**:
- ✅ **Basic Arrow Storage**: File-based columnar storage using Feather format
- ✅ **Flight Ingestion**: Arrow RecordBatch append via DoPut
- ✅ **Time-Partitioned Queries**: Basic scanning with time-based filtering
- ✅ **Embedded API**: Simple C++/Python interfaces

**Success Metrics**:
- Store/query time-series data end-to-end
- 1-5 MB/s/core ingestion baseline
- Basic time-range queries working

### Phase 2: Analytical Performance (3-4 months) - Justify the Architecture

**Goal**: Deliver the analytical performance that differentiates MarbleDB

**Deliverables**:
- **Indexing Layer**: Zone maps, bloom filters, basic statistics
- **Vectorized Execution**: SIMD-accelerated query operators
- **Aggregation Engine**: COUNT, SUM, AVG, GROUP BY operations
- **Query Optimization**: Filter pushdown and basic planning

**Success Metrics**:
- 5-20× query performance improvement with indexing
- Handle 10-100GB datasets efficiently
- 50-80% query plans utilize indexes

### Phase 3: Distributed Reliability (4-6 months) - Production-Ready Foundation

**Goal**: Fault-tolerant, scalable distributed system

**Deliverables**:
- **Raft Consensus**: Multi-node clusters with strong consistency
- **WAL + Recovery**: Crash-safe operation with minimal data loss
- **Basic Compaction**: Size-based file merging and cleanup
- **Cluster Management**: Node addition/removal, basic monitoring
- **Streaming**: Real-time data tailing capabilities

**Success Metrics**:
- 3-node clusters survive single node failures
- <1 minute crash recovery
- Linear scaling with additional nodes

### Phase 4: Advanced Analytics (6-12 months) - Unique Capabilities

**Goal**: Features that make MarbleDB uniquely valuable

**Deliverables**:
- **Bitemporal Time Travel**: System + valid time versioning
- **Advanced Pruning**: Multi-level indexing (100-1000× performance gains)
- **Streaming Analytics**: Windowed operations, complex event processing
- **Cloud Integration**: S3/GCS support for elastic storage
- **Enterprise Security**: mTLS, row-level access control, audit logging

**Success Metrics**:
- Complex analytical queries with time travel
- 100-1000× selective query improvements
- Cloud-native deployment patterns

### Phase 5: Ecosystem & Scale (12+ months) - Market Leadership

**Goal**: Complete analytical database ecosystem

**Deliverables**:
- **GPU Acceleration**: CUDA/ROCm for heavy analytical workloads
- **Advanced Analytics**: Statistical functions, ML model integration
- **Deep Ecosystem**: ADBC drivers, BI tool connectors, SQL dialects
- **Global Scale**: Multi-region, cross-cloud, geo-distributed deployments
- **Advanced Features**: Vector search, inverted indexes, specialized analytics

**Success Metrics**:
- Competitive with ClickHouse on analytical benchmarks
- Market-leading time travel and bitemporal capabilities
- Enterprise-grade scalability and reliability

---

## 🌟 The Complete MarbleDB Grand Plan

This section outlines the **full technical scope** of what MarbleDB could become - the comprehensive vision that guides long-term development. This is **aspirational** and represents the complete feature set rather than immediate implementation priorities.

### 1. Ingestion: QuestDB-Class Performance

**Must Have**:
- ✅ **Flight DoPut Streaming**: Arrow RecordBatch ingestion with backpressure
- ✅ **Time-Based Partitioning**: Configurable windows (1m/5m/1h/day) with automatic routing
- ✅ **Multi-Column Clustering**: Sort keys like (symbol, tenant, shard) for query optimization
- ✅ **Concurrent Writers**: Lock-free staging with fair queuing
- ✅ **Schema Evolution**: Additive fields with automatic null backfill

**Performance Targets**:
- Sustained ingest: >10-50 MB/s per core
- 99p latency: <1-2 seconds to visibility
- Hot segment sealing: <100ms blocking

### 2. Storage: Arrow-First Architecture

**Hierarchical Design**:
```
Segment (immutable, atomic unit)
├── Stripe (co-accessed columns)
│   ├── Column Chunk (contiguous values)
│   │   └── Page (Arrow IPC buffers, compressed)
│   │       └── Footer (checksums, statistics, bloom filters)
└── Segment Footer (manifest, row counts, min/max timestamps)
```

**Key Innovations**:
- **Zero-copy access**: Direct mmap of Arrow IPC pages
- **Multi-level pruning**: Manifest → Zone Maps → Bloom → Dictionary checks
- **Raft-replicated manifests**: Consistent metadata across cluster

### 3. Bitemporal Time Travel: ArcticDB-Style

**Dual Time Dimensions**:
- **System Time**: MVCC snapshots with `AS OF <timestamp | snapshot_id>`
- **Valid Time**: `valid_from`, `valid_to` columns for temporal modeling
- **Delete Vectors**: Efficient overlay for point-in-time reconstruction
- **Interval Indexing**: Fast temporal range queries

**Advanced Features**:
- Diff queries between snapshots
- Temporal aggregations with time predicates
- Audit trails with full historical reconstruction

### 4. Query Engine: ClickHouse-Class Analytics

**Pruning Stack** (in evaluation order):
1. **Manifest pruning**: Partition/time window elimination
2. **Zone maps**: Min/max/nulls + quantiles per page
3. **Bloom filters**: Membership testing (fixed-width + token/n-gram)
4. **Dictionary checks**: Low-cardinality optimizations
5. **Adaptive replanning**: Adjust strategy based on selectivity

**Execution Engine**:
- **Vectorized kernels**: SIMD-optimized operators (filter, project, aggregate)
- **Late materialization**: Keys first, payloads second
- **60-80% CPU in kernels**: Target for analytical workloads

### 5. Distributed Layer: Raft + Arrow Flight

**Consensus**:
- **State Machines**: WAL replication + schema coordination + manifest commits
- **Persistent Logs**: Crash-recoverable operation history
- **Dynamic Membership**: Add/remove nodes without downtime
- **Split-brain prevention**: Jepsen-tested linearizability

**Communication**:
- **Arrow Flight transport**: High-performance inter-node data movement
- **Streaming replication**: Real-time data synchronization
- **Cross-region support**: WAN-optimized protocols

### 6. Advanced Features: Market Differentiation

**Streaming & CDC**:
- **Tailing cursors**: `DoGet(..., follow=true)` for real-time streams
- **Change data capture**: Key + operation type with snapshot IDs
- **Exactly-once semantics**: Cursor persistence and deduplication

**Enterprise Capabilities**:
- **Security**: mTLS, API tokens, row-level access control, audit logging
- **Multi-tenancy**: Resource isolation, usage metering, governance
- **Observability**: Metrics, tracing, performance monitoring

**Cloud Integration**:
- **Object storage**: S3/GCS for elastic capacity
- **Auto-scaling**: Compute and storage elasticity
- **Multi-region**: Geo-distributed deployments

### 7. Ecosystem: Broad Compatibility

**APIs & Protocols**:
- **Flight SQL**: SQL-over-Flight with ADBC drivers
- **Embedded**: C++/Python native APIs
- **REST/GraphQL**: HTTP interfaces for web applications
- **Streaming**: Kafka/S3 event source integration

**Tooling**:
- **marblectl**: Administrative command-line tool
- **BI Connectors**: Tableau, PowerBI, Superset integration
- **Language SDKs**: Go, Rust, Java bindings

### 8. Performance Vision

**Benchmark Targets**:
- **Ingestion**: 50+ MB/s/core sustained throughput
- **Queries**: 100-1000× data reduction via pruning
- **Time Travel**: Efficient bitemporal reconstruction
- **Streaming**: Sub-second end-to-end latency
- **Distributed**: Linear scaling across 100+ nodes

**Competitive Positioning**:
- **QuestDB ingestion** + **ClickHouse analytics** + **ArcticDB time travel** + **TiDB consistency**
- **Arrow-native**: Superior interoperability vs custom formats
- **Unified platform**: Single system for all analytical workloads

---

## 🎯 Strategic Direction

### Why This Grand Plan Matters

1. **Market Gap**: No analytical database combines ingestion + analytics + time travel + consistency
2. **Arrow Momentum**: Ecosystem standardization provides competitive advantage
3. **Cloud Native**: Elastic storage and compute from day one
4. **Streaming First**: Real-time capabilities differentiate from batch-oriented competitors

### Implementation Philosophy

1. **Start Simple**: MVP first, then layer sophistication
2. **Prove Concepts**: Each phase validates architectural decisions
3. **Ecosystem First**: Arrow/Raft integration provides foundation
4. **Performance Driven**: Measure everything, optimize relentlessly

### Risk Mitigation

1. **Technical Feasibility**: Arrow/Raft integration already proven
2. **Incremental Complexity**: Features added as value justifies complexity
3. **Market Validation**: Each phase provides user feedback
4. **Team Scaling**: Architecture supports growing engineering team

### Success Metrics

**Phase 1-2**: Working system with basic analytical capabilities
**Phase 3**: Production-ready distributed database
**Phase 4**: Feature-complete analytical platform
**Phase 5**: Market-leading analytical database

This grand plan represents MarbleDB's journey from **promising prototype** to **market-leading analytical database** - ambitious but achievable through disciplined, incremental development.

## Performance Benchmarks & Targets

### Micro-Benchmarks
- **Scan Throughput**: Memory bandwidth limited on cold data
- **Predicate Selectivity**: 1e-6 to 1.0 range coverage
- **Index Overhead**: <5% storage for zone maps + bloom filters

### Macro-Benchmarks
- **Ingest Rate**: 10-50 MB/s per core (configurable compression)
- **Query Latency**: Sub-second for analytical queries
- **Pruning Efficiency**: 10-100× bytes read reduction
- **Time Travel**: Efficient point-in-time reconstruction

### Distributed Benchmarks
- **Replication Latency**: Sub-100ms cross-node consistency
- **Failover Time**: Sub-second leader election
- **Cluster Scaling**: Linear throughput with node addition

## Team Requirements

**Phase 1-2**: 5-7 engineers
- 2x Storage/Execution Engine (SIMD, Arrow expertise)
- 2x Distributed Systems (Raft, consensus)
- 1x Time-Series/Analytics (query optimization)
- 1x DevOps/Release Engineering
- 1x Product/Testing

**Skills Required**:
- C++20, SIMD intrinsics, Arrow ecosystem
- Distributed systems, Raft consensus
- Performance engineering, benchmarking
- Time-series data models, analytical query patterns

## Risk Mitigation

### Technical Risks
- **Complexity**: Phased approach prevents feature creep
- **Performance**: Early benchmarking validates assumptions
- **Distributed Consistency**: Jepsen testing ensures correctness

### Execution Risks
- **Team Size**: Critical mass needed for complex system
- **Arrow Maturity**: Ecosystem stability for production use
- **Market Timing**: Competitive landscape evolution

## Competitive Advantages

**Unique Value Proposition**:
1. **Arrow-Native**: Superior interoperability vs custom formats
2. **Bitemporal + Time Travel**: Regulatory compliance features
3. **QuestDB Ingestion + ClickHouse Analytics**: Best of both worlds
4. **Raft Consistency**: Strong consistency in analytical space
5. **Time-Series Optimized**: Purpose-built for IoT/financial workloads

**Target Markets**:
- Financial services (auditing, time travel)
- IoT platforms (high-throughput ingestion)
- Real-time analytics systems
- Event streaming with analytical storage
