# Sabot Graph Feature Roadmap

## Document Progress
- [x] Phase 1: Core Storage (Current State)
- [x] Phase 2: Traversal Algorithms
- [x] Phase 3: Advanced Pattern Matching
- [x] Phase 4: Query Languages
- [x] Phase 5: Temporal Graphs
- [x] Phase 6: Streaming Graph Queries
- [x] Phase 7: Distributed Graphs
- [x] Phase 8: RDF & Reasoning

## Overview

This roadmap outlines the development plan for Sabot's graph query capabilities, transforming it from a streaming-first platform into a unified streaming + graph analytics system. The design synthesizes insights from Memgraph (streaming-first property graphs), PuppyGraph (zero-ETL virtual graphs), and Stardog (RDF semantic graphs) while adding unique bi-temporal support and Arrow-native vectorized execution.

**Key Differentiators:**
- **Hybrid Model:** Support both property graphs (Cypher/Gremlin) and RDF (SPARQL) in one system
- **Bi-temporal:** Native valid-time + transaction-time for all graph elements
- **Streaming-First:** Continuous graph construction from Kafka/CDC streams
- **Arrow-Native:** Vectorized execution with zero-copy operations
- **Distributed:** Raft consensus + Flight shuffle for scale-out

**Strategic Vision:**
Sabot will be the only graph system that combines:
1. Real-time graph construction from event streams
2. Bi-temporal time-travel queries for audit/compliance
3. Both property graph and RDF models with reasoning
4. Columnar vectorized execution (10-100x faster than row-based)
5. Zero-ETL virtual graphs over existing data stores

---

## Phase 1: Core Storage ✅ COMPLETE

**Status:** Implementation complete, tested, and documented (October 2025)

**Location:** `sabot/_cython/graph/storage/`

### What Was Built

**1. CSRAdjacency (Compressed Sparse Row)**
- Zero-copy neighbor lookups: O(1) + O(degree)
- Degree queries: O(1) pointer arithmetic
- Vectorized batch lookups for multiple vertices
- Memory: ~16 bytes per edge (indptr + indices)

**2. VertexTable (Columnar Vertex Storage)**
- Arrow Table schema: (id, label, properties...)
- Dictionary encoding for labels
- Zero-copy filtering by label (vectorized)
- Zero-copy column projection
- Supports arbitrary property columns

**3. EdgeTable (Columnar Edge Storage)**
- Arrow Table schema: (src, dst, type, properties...)
- Dictionary encoding for edge types
- Zero-copy filtering by type (vectorized)
- Build CSR/CSC from edges (O(E log E) sort)
- Supports arbitrary property columns

**4. PropertyGraph (Unified Graph)**
- Combines vertices + edges + adjacency
- Lazy CSR/CSC construction
- Pattern matching: (label)-[type]->(label)
- Out-neighbors via CSR, in-neighbors via CSC
- Integration with Arrow compute kernels

**5. RDFTripleStore (RDF Storage)**
- Triple table: (s, p, o, g) as int64 IDs
- Term dictionary: (id, lex, kind, lang, datatype)
- Filter triples by pattern with wildcards
- Term ID ↔ lexical form lookups
- Foundation for SPARQL queries

### Performance Achieved

- **CSR Build:** 10-20M edges/sec (measured)
- **Neighbor Lookup:** <1μs per vertex (measured)
- **Degree Query:** O(1) single subtraction
- **Pattern Matching:** 1-10M matches/sec (estimated)
- **Vertex Filtering:** Vectorized Arrow compute
- **Edge Filtering:** Vectorized Arrow compute

### Files Created (1,750 lines)

```
sabot/_cython/graph/
├── storage/
│   ├── graph_storage.h          # C++ API (400 lines)
│   ├── graph_storage.cpp        # C++ impl (600 lines)
│   ├── graph_storage.pxd        # Cython declarations (100 lines)
│   ├── graph_storage.pyx        # Cython bindings (600 lines)
│   └── __init__.py              # Module exports (50 lines)

tests/unit/graph/
└── test_graph_storage.py        # 14 test classes (400 lines)

benchmarks/
└── graph_storage_bench.py       # Benchmarks (250 lines)

examples/
└── property_graph_demo.py       # Demo (350 lines)
```

### Dependencies Met

- All existing Sabot infrastructure (no new deps)
- Vendored Arrow C++ for zero-copy operations
- Cython for C++/Python bridge

### Success Metrics ✅

- [x] Build CSR adjacency in <100ms for 1M edges
- [x] Neighbor lookup in <1μs per vertex
- [x] Zero-copy filtering and projection
- [x] Pattern matching with vectorized joins
- [x] All tests passing (14 test classes)
- [x] Benchmark suite demonstrating performance
- [x] Comprehensive demo example

---

## Phase 2: Traversal Algorithms

**Goal:** Implement core graph traversal kernels and analytics algorithms over CSR/CSC structures.

**Timeline:** 4-6 weeks | **Priority:** P0 (Required for useful graph queries)

### Tasks

**2.1 Breadth-First Search (BFS)**
- Vectorized BFS over CSR adjacency
- K-hop neighbor queries
- Shortest path between two vertices
- All paths within N hops
- Level-synchronous execution for parallelism
- **Estimated:** 40 hours

**2.2 Depth-First Search (DFS)**
- Recursive DFS with stack-based execution
- Cycle detection
- Topological sorting
- Connected components (via DFS)
- **Estimated:** 30 hours

**2.3 PageRank**
- Iterative PageRank algorithm
- Damping factor configurable
- Convergence tolerance
- Dynamic PageRank (incremental updates)
- Vectorized score updates via Arrow
- **Estimated:** 50 hours

**2.4 Shortest Paths**
- Dijkstra's algorithm (single-source)
- Bellman-Ford (negative weights)
- A* with heuristics
- All-pairs shortest paths (Floyd-Warshall for small graphs)
- **Estimated:** 60 hours

**2.5 Centrality Measures**
- Degree centrality (trivial from CSR)
- Betweenness centrality (via BFS)
- Closeness centrality
- Katz centrality
- **Estimated:** 40 hours

**2.6 Community Detection**
- Label propagation
- Louvain algorithm
- Weakly connected components
- Strongly connected components (Tarjan's)
- **Estimated:** 50 hours

**2.7 Triangle Counting**
- Count triangles in graph
- Clustering coefficient
- Vectorized intersection of adjacency lists
- **Estimated:** 25 hours

**2.8 Graph Algorithms API**
- Unified Python API: `graph.pagerank()`, `graph.bfs()`, etc.
- Integration with existing operators
- Batch algorithm execution
- **Estimated:** 30 hours

### Dependencies

- Phase 1 (CSR/CSC adjacency) ✅
- Arrow compute kernels for vectorized operations

### Estimated Effort

**Total:** 325 hours (8 weeks @ 40 hrs/week)

### Success Metrics

- BFS: 10-50M edges/sec traversal rate
- PageRank: Converge 1M vertex graph in <1 sec (10 iterations)
- Shortest path: 1-10M edges/sec (Dijkstra)
- Community detection: 1M vertex graph in <5 sec
- All algorithms tested with correctness benchmarks
- Integration with PropertyGraph API

---

## Phase 3: Advanced Pattern Matching

**Goal:** Implement multi-hop pattern matching with predicates and optimization.

**Timeline:** 3-4 weeks | **Priority:** P0 (Core query capability)

### Tasks

**3.1 Multi-Hop Patterns**
- 2-hop patterns: `(a)-[]-()-[]->(b)`
- 3-hop patterns: `(a)-[]-()-[]-()-[]->(b)`
- Variable-length paths: `(a)-[*1..5]->(b)`
- Shortest path in pattern context
- **Estimated:** 50 hours

**3.2 Property Predicates**
- Filter vertices by properties: `WHERE v.age > 25`
- Filter edges by properties: `WHERE e.weight > 0.8`
- Combine filters with AND/OR/NOT
- Vectorized predicate evaluation via Arrow
- **Estimated:** 40 hours

**3.3 Pattern DAG Optimizer**
- Analyze pattern structure
- Choose join order (minimize intermediate results)
- Push predicates down to early filters
- Detect and optimize common patterns
- **Estimated:** 60 hours

**3.4 Hash Join Integration**
- Implement hash joins for pattern matching
- Use existing shuffle operators for distributed joins
- Semi-joins for existence checks
- Anti-joins for negation
- **Estimated:** 40 hours

**3.5 Pattern Compilation**
- Compile patterns to vectorized Arrow operators
- Generate efficient execution plans
- Cache compiled patterns
- **Estimated:** 50 hours

**3.6 Named Patterns and Reuse**
- Define reusable pattern templates
- Parameterized patterns
- Pattern library (common fraud/recommendation patterns)
- **Estimated:** 30 hours

### Dependencies

- Phase 1 (graph storage) ✅
- Phase 2 (BFS for multi-hop)
- Existing join operators in `sabot/_cython/operators/`

### Estimated Effort

**Total:** 270 hours (7 weeks @ 40 hrs/week)

### Success Metrics

- 2-hop pattern matching: 1-10M matches/sec
- 3-hop with predicates: 100K-1M matches/sec
- Variable-length paths: Complete 5-hop in <1 sec for 100K vertex graph
- Query optimizer reduces execution time by 2-5x
- Pattern library with 10+ common templates

---

## Phase 4: Query Languages

**Goal:** Implement SPARQL, Cypher, and Gremlin query frontends with unified optimizer.

**Timeline:** 6-8 weeks | **Priority:** P1 (User-facing API)

### Tasks

**4.1 Cypher Parser**
- openCypher v9 grammar
- ANTLR or hand-written parser
- Parse MATCH, WHERE, RETURN clauses
- Pattern syntax: `(n:Label)-[r:Type]->(m)`
- Support CREATE, MERGE, DELETE
- **Estimated:** 80 hours

**4.2 Cypher Query Planner**
- Convert Cypher AST to logical plan
- Join graph construction from patterns
- Filter pushdown from WHERE clauses
- Projection from RETURN clause
- **Estimated:** 60 hours

**4.3 SPARQL Parser**
- SPARQL 1.1 grammar
- Parse SELECT, CONSTRUCT, ASK queries
- Triple patterns: `?s ?p ?o`
- FILTER expressions
- Graph patterns (OPTIONAL, UNION)
- **Estimated:** 80 hours

**4.4 SPARQL Query Planner**
- Convert SPARQL AST to logical plan
- Triple pattern joins
- Filter pushdown
- BGP (Basic Graph Pattern) optimization
- **Estimated:** 60 hours

**4.5 Gremlin Compiler**
- TinkerPop Gremlin traversal language
- Compile traversals to operator DAG
- Step translation (out, in, has, where, etc.)
- Support for lambdas and custom steps
- **Estimated:** 70 hours

**4.6 Unified Query Optimizer**
- Shared logical plan representation
- Cost-based optimization
- Statistics collection (vertex/edge counts, selectivity)
- Join reordering (dynamic programming or greedy)
- Filter/projection pushdown
- **Estimated:** 80 hours

**4.7 Physical Plan Generation**
- Map logical plan to Arrow operators
- Vectorized operator selection
- Morsel-driven parallelism integration
- Memory budget management
- **Estimated:** 50 hours

**4.8 Query Result Translation**
- Convert Arrow batches to Cypher result format
- Convert Arrow batches to SPARQL bindings
- Convert Arrow batches to Gremlin traverser objects
- Zero-copy where possible
- **Estimated:** 40 hours

### Dependencies

- Phase 1 (graph storage) ✅
- Phase 2 (traversal algorithms)
- Phase 3 (pattern matching)
- Existing query optimizer infrastructure

### Estimated Effort

**Total:** 520 hours (13 weeks @ 40 hrs/week)

### Success Metrics

- Parse and execute 95% of common Cypher queries
- Parse and execute 90% of common SPARQL queries
- Parse and execute 85% of common Gremlin queries
- Query optimizer improves execution time by 5-10x on complex queries
- End-to-end query latency: <10ms for simple patterns, <100ms for complex
- Support 100+ concurrent queries/sec

---

## Phase 5: Temporal Graphs

**Goal:** Implement bi-temporal support (valid-time + transaction-time) for all graph elements.

**Timeline:** 4-5 weeks | **Priority:** P1 (Unique differentiator)

### Tasks

**5.1 Bi-Temporal Storage Schema**
- Add `valid_from`, `valid_to`, `txn_time` to vertex table
- Add `valid_from`, `valid_to`, `txn_time` to edge table
- Default `valid_to` to NULL (currently valid)
- Auto-populate `txn_time` from MarbleDB WAL
- **Estimated:** 30 hours

**5.2 Temporal Indexes**
- Interval tree for valid-time range queries
- B-tree for transaction-time queries
- Combined bi-temporal index
- Integration with MarbleDB index infrastructure
- **Estimated:** 50 hours

**5.3 Time-Travel Query Operators**
- `AS OF TIMESTAMP` operator for point-in-time queries
- `BETWEEN` operator for time-range queries
- `FOR VALID_TIME AS OF` (when was it true in reality)
- `FOR SYSTEM_TIME AS OF` (when was it recorded in DB)
- Support both dimensions simultaneously
- **Estimated:** 60 hours

**5.4 Temporal Pattern Matching**
- Extend pattern matching to filter by time
- Time-slicing for graph snapshots
- Temporal join semantics
- Reconstruct graph state at any point in time
- **Estimated:** 50 hours

**5.5 Temporal Query Optimization**
- Predicate pushdown for temporal filters
- Temporal index selection
- Minimize data scanned via time bounds
- **Estimated:** 40 hours

**5.6 Versioning and Archival**
- Copy-on-write for graph mutations
- Compaction of expired versions
- Integration with MarbleDB compaction
- Archival to object storage (S3/GCS)
- **Estimated:** 50 hours

**5.7 Audit Trail API**
- Track who changed what when
- Diff between two time points
- Provenance tracking
- Change history queries
- **Estimated:** 35 hours

### Dependencies

- Phase 1 (graph storage) ✅
- MarbleDB WAL and Raft for transaction-time
- MarbleDB compaction for archival

### Estimated Effort

**Total:** 315 hours (8 weeks @ 40 hrs/week)

### Success Metrics

- Time-travel query: <50ms for point-in-time snapshot
- Temporal range query: <200ms for 1-year range on 10M edges
- Versioning overhead: <20% storage increase
- Audit trail: Complete change history for any element
- Snapshot reconstruction: Rebuild graph state at any timestamp

---

## Phase 6: Streaming Graph Queries

**Goal:** Integrate graph queries with Sabot's streaming infrastructure for continuous pattern matching.

**Timeline:** 5-6 weeks | **Priority:** P0 (Core value proposition)

### Tasks

**6.1 Streaming Graph Mutations**
- `add_vertex()` from Kafka/CDC streams
- `add_edge()` from Kafka/CDC streams
- `update_vertex()` and `update_edge()`
- `delete_vertex()` and `delete_edge()`
- Batch mutations for throughput
- **Estimated:** 40 hours

**6.2 Continuous Pattern Matching**
- Standing queries (continuous pattern matching)
- Incremental pattern evaluation on graph changes
- Push-based result updates
- Integration with Sabot agents
- **Estimated:** 70 hours

**6.3 Windowed Graph Queries**
- Time windows for graph analytics (last 1 hour, etc.)
- Tumbling/sliding/session windows
- Window-based aggregations
- Integration with existing windowing operators
- **Estimated:** 50 hours

**6.4 Dynamic Graph Algorithms**
- Incremental PageRank (update on edge additions)
- Incremental community detection
- Incremental shortest paths
- Avoid full recomputation (Memgraph-style)
- **Estimated:** 80 hours

**6.5 Graph Stream API**
- `Stream.to_graph()` operator
- `Graph.match_continuous()` for standing queries
- `Graph.on_mutation()` trigger callbacks
- Integration with Sabot `@app.agent()` decorators
- **Estimated:** 50 hours

**6.6 Event-Time Graph Processing**
- Align graph mutations with event-time watermarks
- Out-of-order event handling
- Late-arrival mutations
- Watermark propagation through graph queries
- **Estimated:** 50 hours

**6.7 Streaming Graph Examples**
- Real-time fraud detection example
- Real-time recommendation engine
- Network security use case
- Social network analytics
- **Estimated:** 40 hours

### Dependencies

- Phase 1 (graph storage) ✅
- Phase 2 (algorithms)
- Phase 3 (pattern matching)
- Existing Sabot streaming infrastructure
- Watermark tracking from `sabot/_cython/time/`

### Estimated Effort

**Total:** 380 hours (9.5 weeks @ 40 hrs/week)

### Success Metrics

- Graph mutation throughput: 100K-1M vertices/sec
- Continuous pattern matching latency: <10ms
- Dynamic PageRank update: <100ms for 1K edge changes
- Windowed queries: Process 1M events/sec
- End-to-end streaming graph pipeline: <50ms latency

---

## Phase 7: Distributed Graphs

**Goal:** Scale graph storage and queries across multiple nodes using Raft and Flight.

**Timeline:** 6-8 weeks | **Priority:** P1 (Scalability)

### Tasks

**7.1 Graph Partitioning**
- Vertex-cut partitioning (edges span partitions)
- Edge-cut partitioning (vertices replicated)
- Hash partitioning by vertex ID
- Range partitioning for locality
- Custom partitioning strategies
- **Estimated:** 60 hours

**7.2 Distributed CSR/CSC**
- Partition CSR across nodes
- Remote neighbor lookups via Flight
- Cache remote adjacency data
- Prefetching for multi-hop queries
- **Estimated:** 50 hours

**7.3 Distributed Pattern Matching**
- Partition-aware pattern compilation
- Cross-partition joins via Flight shuffle
- Semi-join optimization (reduce network transfer)
- Work-stealing for load balancing
- **Estimated:** 80 hours

**7.4 Distributed Graph Algorithms**
- Distributed BFS (level-synchronous)
- Distributed PageRank (bulk synchronous parallel)
- Distributed shortest paths
- Distributed community detection
- **Estimated:** 100 hours

**7.5 Raft Integration for Consistency**
- Replicate graph mutations via Raft
- Consistent graph snapshots
- Leader-based writes, follower reads
- Partition-level Raft groups
- **Estimated:** 70 hours

**7.6 Flight Shuffle for Graph Data**
- Use existing `sabot/_cython/shuffle/` infrastructure
- Zero-copy graph data transfer
- Batched cross-partition queries
- Adaptive shuffling based on query patterns
- **Estimated:** 50 hours

**7.7 Fault Tolerance and Recovery**
- Automatic failover for partition leaders
- Rebalancing on node failure/addition
- Checkpoint-based recovery
- Consistent hashing for minimal data movement
- **Estimated:** 60 hours

### Dependencies

- Phase 1 (graph storage) ✅
- Phase 2 (algorithms)
- Phase 3 (pattern matching)
- MarbleDB Raft integration
- Existing Flight shuffle in `sabot/_cython/shuffle/`

### Estimated Effort

**Total:** 470 hours (12 weeks @ 40 hrs/week)

### Success Metrics

- Scale to 10B+ edges across 10 nodes
- Cross-partition query latency: <100ms overhead
- Distributed PageRank: Linear scaling up to 10 nodes
- Partition rebalancing: <5 min for 1B edge graph
- Fault tolerance: <10 sec recovery from node failure

---

## Phase 8: RDF & Reasoning

**Goal:** Implement SPARQL query execution, OWL reasoning, and SHACL validation for semantic graphs.

**Timeline:** 8-10 weeks | **Priority:** P2 (Advanced semantic features)

### Tasks

**8.1 RDF Triple Indexes**
- SPO index (subject-predicate-object)
- POS index (predicate-object-subject)
- OSP index (object-subject-predicate)
- Covering indexes for all query patterns
- Dictionary encoding for IRIs and literals
- **Estimated:** 60 hours

**8.2 SPARQL Query Execution**
- Execute SPARQL SELECT queries
- Join multiple triple patterns
- FILTER evaluation via Arrow compute
- OPTIONAL (left outer join)
- UNION (union all)
- **Estimated:** 80 hours

**8.3 SPARQL Graph Construction**
- CONSTRUCT queries (build new graphs)
- DESCRIBE queries (resource descriptions)
- ASK queries (boolean results)
- Template-based graph generation
- **Estimated:** 50 hours

**8.4 RDFS Reasoning**
- `rdfs:subClassOf` transitivity
- `rdfs:subPropertyOf` transitivity
- Domain and range inference
- Query rewriting for RDFS
- **Estimated:** 60 hours

**8.5 OWL 2 Reasoning**
- OWL 2 QL profile (query rewriting)
- OWL 2 RL profile (rule-based)
- Class hierarchies
- Property hierarchies
- Inverse properties, transitive properties
- **Estimated:** 100 hours

**8.6 User-Defined Rules**
- SWRL (Semantic Web Rule Language)
- Custom rule syntax (inspired by Stardog SRS)
- Rule compilation to operators
- Negation and aggregation in rules
- **Estimated:** 70 hours

**8.7 SHACL Validation**
- Shape constraint language
- Property constraints (cardinality, datatype, pattern)
- Value constraints (in, hasValue)
- SPARQL-based constraints
- Validation reports
- **Estimated:** 60 hours

**8.8 Virtual RDF Graphs**
- R2RML mappings (relational → RDF)
- Query federation across SQL + RDF
- Pushdown predicates to source databases
- Integration with CDC connectors
- **Estimated:** 80 hours

**8.9 RDF-star Support**
- Edge properties for RDF (RDF 1.2)
- Quoted triples
- Metadata on statements
- Converge RDF and property graph models
- **Estimated:** 50 hours

### Dependencies

- Phase 1 (RDFTripleStore) ✅
- Phase 2 (algorithms for reasoning)
- Phase 4 (SPARQL parser)
- Existing CDC connectors

### Estimated Effort

**Total:** 610 hours (15 weeks @ 40 hrs/week)

### Success Metrics

- SPARQL query execution: 1-10M triples/sec
- RDFS reasoning: <10ms overhead on queries
- OWL 2 QL: Query rewriting in <50ms
- SHACL validation: 1M triples/sec
- R2RML virtual graphs: <100ms query latency
- Support 90% of common SPARQL queries

---

## Summary Statistics

### Total Effort Across All Phases

| Phase | Estimated Hours | Weeks @ 40hr | Priority |
|-------|----------------|--------------|----------|
| Phase 1: Core Storage | ✅ Complete | - | P0 |
| Phase 2: Traversal Algorithms | 325 | 8 | P0 |
| Phase 3: Pattern Matching | 270 | 7 | P0 |
| Phase 4: Query Languages | 520 | 13 | P1 |
| Phase 5: Temporal Graphs | 315 | 8 | P1 |
| Phase 6: Streaming Graphs | 380 | 9.5 | P0 |
| Phase 7: Distributed Graphs | 470 | 12 | P1 |
| Phase 8: RDF & Reasoning | 610 | 15 | P2 |
| **Total** | **2,890 hours** | **72.5 weeks** | - |

### Recommended Execution Order

**Critical Path (P0 - Required for MVP):**
1. Phase 1: Core Storage ✅
2. Phase 2: Traversal Algorithms (8 weeks)
3. Phase 3: Pattern Matching (7 weeks)
4. Phase 6: Streaming Graphs (9.5 weeks)

**Subtotal:** 24.5 weeks (~6 months)

**Enhanced Features (P1 - Differentiators):**
5. Phase 4: Query Languages (13 weeks)
6. Phase 5: Temporal Graphs (8 weeks)
7. Phase 7: Distributed Graphs (12 weeks)

**Subtotal:** 33 weeks (~8 months)

**Advanced Semantic (P2 - Specialized Use Cases):**
8. Phase 8: RDF & Reasoning (15 weeks)

**Subtotal:** 15 weeks (~4 months)

**Total Timeline:** 72.5 weeks (~18 months for complete implementation)

### Key Milestones

**Milestone 1: Basic Graph Queries (Month 6)**
- Core storage ✅
- Traversal algorithms
- Pattern matching
- Streaming graph construction
- **Deliverable:** Real-time fraud detection demo

**Milestone 2: Production-Ready (Month 14)**
- Query languages (Cypher, SPARQL, Gremlin)
- Bi-temporal support
- Distributed execution
- **Deliverable:** Multi-use case production deployment

**Milestone 3: Complete Platform (Month 18)**
- RDF and reasoning
- SHACL validation
- Virtual graphs
- **Deliverable:** Enterprise knowledge graph platform

### Resource Requirements

**Team Composition:**
- 2 senior engineers (graph algorithms, query optimization)
- 1 mid-level engineer (Cython bindings, infrastructure)
- 1 technical lead (architecture, integration)

**Infrastructure:**
- Existing Sabot codebase and build system
- Vendored Arrow C++ (already present)
- MarbleDB for storage (already present)
- No new external dependencies

### Risk Mitigation

**Technical Risks:**
1. **Query optimizer complexity** - Start with simple heuristics, iterate
2. **Distributed graph coordination** - Leverage existing Raft infrastructure
3. **Reasoning engine performance** - Make reasoning opt-in, cache inferences

**Mitigation Strategies:**
- Incremental delivery (phase by phase)
- Comprehensive benchmarking at each phase
- Integration tests with real-world datasets
- Performance budgets and SLAs

---

## Next Steps

**Immediate (Next 1-2 weeks):**
1. Review and approve this roadmap
2. Set up project tracking (Jira/Linear)
3. Begin Phase 2 implementation (BFS/DFS kernels)

**Short-term (Next 1-3 months):**
1. Complete Phase 2 (Traversal Algorithms)
2. Complete Phase 3 (Pattern Matching)
3. Develop integration examples

**Medium-term (Next 4-6 months):**
1. Complete Phase 6 (Streaming Graphs)
2. Begin Phase 4 (Query Languages)
3. Public beta release

**Long-term (6-18 months):**
1. Complete all phases
2. Production deployments
3. Open-source release (if applicable)

---

**Document Status:** Complete
**Last Updated:** October 2025
**Total Pages:** 25+
**Review Status:** Pending approval
