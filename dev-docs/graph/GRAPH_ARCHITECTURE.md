# Sabot Graph Architecture

## Document Progress
- [x] Overview & Philosophy
- [x] Storage Layer
- [x] Query Execution Engine
- [x] Temporal Data Model
- [x] Streaming Integration
- [x] Arrow Optimizations
- [x] Comparison with Other Systems

## 1. Overview & Philosophy

### 1.1 Design Principles

Sabot's graph architecture is built on five core principles that differentiate it from existing graph databases:

**1. Arrow-Native Everything**
- All graph data stored in Arrow columnar format
- Zero-copy operations throughout the execution pipeline
- Vectorized kernels for graph algorithms
- SIMD-friendly data layouts
- Interoperability with the entire Arrow ecosystem

**2. Streaming-First Graph Construction**
- Graphs built continuously from event streams (Kafka, CDC)
- No batch ETL required
- Real-time graph mutations
- Event-time semantics for graph operations
- Watermark propagation through graph queries

**3. Bi-Temporal by Default**
- Every vertex and edge has valid-time and transaction-time
- Time-travel queries to any point in history
- Audit trails built-in
- Regulatory compliance (finance, healthcare)
- No application-level time modeling required

**4. Hybrid Data Models**
- Property graphs (Cypher, Gremlin) for operational queries
- RDF graphs (SPARQL) for semantic reasoning
- Unified storage layer supporting both models
- Transparent conversion between models
- Query any data model via any query language

**5. Distributed by Design**
- Raft consensus for consistency
- Arrow Flight for zero-copy shuffle
- Partition-aware query compilation
- Linear scaling to 10B+ edges
- Fault tolerance with automatic recovery

### 1.2 Strategic Positioning

**vs. Memgraph:**
- Sabot adds bi-temporal support (Memgraph has TTL but no time-travel)
- Sabot adds RDF/SPARQL (Memgraph is property-graph only)
- Sabot uses Arrow columnar format (Memgraph is row-based)
- Shared: Streaming-first philosophy, dynamic algorithms

**vs. PuppyGraph:**
- Sabot adds physical graph storage (PuppyGraph is virtual-only)
- Sabot adds reasoning engine (PuppyGraph has none)
- Sabot adds streaming mutations (PuppyGraph is read-only)
- Shared: Zero-ETL virtual graphs, multi-language queries

**vs. Stardog:**
- Sabot adds streaming graph construction (Stardog is batch-oriented)
- Sabot adds bi-temporal support (Stardog has basic versioning)
- Sabot uses Arrow columnar format (Stardog is triple-store)
- Shared: SPARQL, reasoning, virtual graphs

**Unique Value:**
Sabot is the only system that combines:
- Streaming + graph in one platform
- Bi-temporal time-travel queries
- Property graphs + RDF + reasoning
- Columnar vectorized execution
- Distributed Arrow-native storage

### 1.3 Use Cases

**Financial Fraud Detection**
- Stream transactions from Kafka → Build transaction graph
- Continuous pattern matching for fraud rings
- Time-travel queries for forensic analysis
- PageRank for entity importance
- Bi-temporal audit trail for compliance

**Real-Time Knowledge Graphs**
- Ingest events from multiple sources (CDC, APIs, logs)
- Build unified knowledge graph with reasoning
- SPARQL queries for semantic search
- Virtual graphs over existing relational databases
- No ETL, always up-to-date

**Network Security**
- Stream network logs → Build connection graph
- Community detection for attack clusters
- Shortest path for lateral movement analysis
- Time-travel for incident reconstruction
- Continuous threat detection

**Recommendation Engines**
- Stream user interactions → Build social graph
- Collaborative filtering via graph traversals
- Real-time PageRank for trending content
- Windowed queries (last 1 hour interactions)
- Sub-10ms recommendation latency

**Supply Chain Optimization**
- Stream sensor data → Build dependency graph
- Shortest path for routing
- Community detection for risk clusters
- Time-travel for what-if analysis
- Continuous bottleneck detection

### 1.4 Architecture Layers

```
┌─────────────────────────────────────────────────────────┐
│  Application Layer                                      │
│  - Python API (sb.Graph, sb.RDFGraph)                   │
│  - Stream operators (Stream.to_graph(), match_pattern) │
│  - Agent integration (@app.agent)                       │
└─────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────┐
│  Query Language Layer                                   │
│  - Cypher Parser & Planner (openCypher v9)              │
│  - SPARQL Parser & Planner (SPARQL 1.1)                 │
│  - Gremlin Compiler (TinkerPop)                         │
│  - Unified Query Optimizer (cost-based)                 │
└─────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────┐
│  Reasoning Engine (Optional)                            │
│  - RDFS Reasoner (subclass, subproperty)                │
│  - OWL 2 QL/RL Reasoner (query rewriting)               │
│  - User-Defined Rules (SWRL)                            │
│  - Query Rewriting (inject inferences)                  │
└─────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────┐
│  Execution Engine                                       │
│  - Vectorized Operators (BARQ-style batching)           │
│  - Morsel-Driven Parallelism (DuckDB-style)             │
│  - Graph Algorithms (BFS, PageRank, community)          │
│  - Pattern Matcher (multi-hop, predicates)              │
│  - Temporal Operators (AS OF, BETWEEN)                  │
└─────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────┐
│  Storage Layer (Dual Model)                             │
│  ┌─────────────────────┐ ┌──────────────────────────┐  │
│  │ Property Graph      │ │ RDF Triple Store         │  │
│  │ - VertexTable       │ │ - Triple table (s,p,o,g) │  │
│  │ - EdgeTable         │ │ - Term dictionary        │  │
│  │ - CSR/CSC adjacency │ │ - SPO/POS/OSP indexes    │  │
│  │ - Bi-temporal cols  │ │ - Bi-temporal cols       │  │
│  └─────────────────────┘ └──────────────────────────┘  │
│                                                          │
│  Unified Representation: Arrow Columnar Format          │
└─────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────┐
│  MarbleDB (Persistent Storage)                          │
│  - WAL (transaction-time tracking)                      │
│  - Raft (distributed consensus)                         │
│  - Columnar files (Parquet-like)                        │
│  - Temporal indexes (interval trees)                    │
└─────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────┐
│  Distributed Layer                                      │
│  - Graph Partitioning (vertex-cut, edge-cut)            │
│  - Flight Shuffle (zero-copy cross-partition)           │
│  - Distributed Algorithms (BSP-style)                   │
│  - Fault Tolerance (Raft replication)                   │
└─────────────────────────────────────────────────────────┘
```

---

## 2. Storage Layer

### 2.1 Property Graph Storage

**Physical Schema:**

**Vertex Table:**
```
Schema:
  - id: int64 (dense, 0..N-1)
  - label: dictionary<string> (vertex type)
  - valid_from: timestamp (bi-temporal)
  - valid_to: timestamp (bi-temporal, NULL = current)
  - txn_time: timestamp (bi-temporal, auto from WAL)
  - <property_columns>: any Arrow type

Example:
┌─────┬────────┬──────────────┬────────────┬──────────────┬────────┬────────┐
│ id  │ label  │ valid_from   │ valid_to   │ txn_time     │ name   │ age    │
├─────┼────────┼──────────────┼────────────┼──────────────┼────────┼────────┤
│ 0   │ Person │ 2020-01-01   │ NULL       │ 2025-10-09   │ Alice  │ 30     │
│ 1   │ Person │ 2020-01-01   │ NULL       │ 2025-10-09   │ Bob    │ 35     │
│ 2   │ Company│ 2015-01-01   │ NULL       │ 2025-10-09   │ Acme   │ NULL   │
└─────┴────────┴──────────────┴────────────┴──────────────┴────────┴────────┘
```

**Edge Table:**
```
Schema:
  - src: int64 (source vertex ID)
  - dst: int64 (destination vertex ID)
  - type: dictionary<string> (edge type)
  - valid_from: timestamp (bi-temporal)
  - valid_to: timestamp (bi-temporal, NULL = current)
  - txn_time: timestamp (bi-temporal, auto from WAL)
  - <property_columns>: any Arrow type

Example:
┌─────┬─────┬──────────┬──────────────┬────────────┬──────────────┬────────┐
│ src │ dst │ type     │ valid_from   │ valid_to   │ txn_time     │ since  │
├─────┼─────┼──────────┼──────────────┼────────────┼──────────────┼────────┤
│ 0   │ 1   │ KNOWS    │ 2020-01-01   │ NULL       │ 2025-10-09   │ 2018   │
│ 0   │ 2   │ WORKS_AT │ 2020-01-01   │ 2024-12-31 │ 2025-10-09   │ 2019   │
│ 1   │ 2   │ WORKS_AT │ 2021-01-01   │ NULL       │ 2025-10-09   │ 2021   │
└─────┴─────┴──────────┴──────────────┴────────────┴──────────────┴────────┘
```

**CSR Adjacency (Out-Edges):**
```
indptr:  [0, 2, 3, 3]       # Offsets for each vertex
indices: [1, 2, 2]           # Neighbor IDs
edge_data: StructArray       # Optional edge properties

Memory: ~16 bytes per edge (int64 indptr + int64 indices)
Lookup: O(1) to find neighbors of vertex v
```

**CSC Adjacency (In-Edges):**
```
indptr:  [0, 1, 3, 3]       # Offsets for reverse direction
indices: [0, 0, 1]           # In-neighbor IDs

Used for: Reverse traversals, PageRank, in-degree
```

**Dictionary Encoding:**
- Labels and types stored as int32 dictionary indices
- Reduces memory by 4x (int32 vs string)
- Faster comparisons (integer equality)
- SIMD-friendly vectorized filters

### 2.2 RDF Triple Storage

**Physical Schema:**

**Triple Table (Quads):**
```
Schema:
  - s: int64 (subject term ID)
  - p: int64 (predicate term ID)
  - o: int64 (object term ID)
  - g: int64 (graph term ID, optional)
  - valid_from: timestamp (bi-temporal)
  - valid_to: timestamp (bi-temporal)
  - txn_time: timestamp (bi-temporal)

Example:
┌─────┬────┬────┬────┬──────────────┬────────────┬──────────────┐
│ s   │ p  │ o  │ g  │ valid_from   │ valid_to   │ txn_time     │
├─────┼────┼────┼────┼──────────────┼────────────┼──────────────┤
│ 100 │ 10 │ 200│ 1  │ 2020-01-01   │ NULL       │ 2025-10-09   │
│ 100 │ 11 │ 300│ 1  │ 2020-01-01   │ NULL       │ 2025-10-09   │
│ 200 │ 10 │ 400│ 1  │ 2021-01-01   │ NULL       │ 2025-10-09   │
└─────┴────┴────┴────┴──────────────┴────────────┴──────────────┘

Where:
  100 = <http://example.org/Alice>
  10  = <http://xmlns.com/foaf/0.1/knows>
  200 = <http://example.org/Bob>
  etc.
```

**Term Dictionary:**
```
Schema:
  - id: int64 (term ID)
  - lex: string (lexical form)
  - kind: uint8 (IRI=0, Literal=1, Blank=2)
  - lang: string (language tag, e.g., "en")
  - datatype: string (datatype IRI, e.g., xsd:integer)

Example:
┌─────┬───────────────────────────────┬──────┬──────┬────────────────┐
│ id  │ lex                           │ kind │ lang │ datatype       │
├─────┼───────────────────────────────┼──────┼──────┼────────────────┤
│ 100 │ http://example.org/Alice      │ 0    │ NULL │ NULL           │
│ 10  │ http://foaf/0.1/knows         │ 0    │ NULL │ NULL           │
│ 200 │ http://example.org/Bob        │ 0    │ NULL │ NULL           │
│ 300 │ "30"                          │ 1    │ NULL │ xsd:integer    │
│ 400 │ "Hello"                       │ 1    │ "en" │ xsd:string     │
└─────┴───────────────────────────────┴──────┴──────┴────────────────┘
```

**RDF Indexes (Covering Orders):**
- **SPO Index:** Sorted by (subject, predicate, object) - for `?p ?o` lookups given subject
- **POS Index:** Sorted by (predicate, object, subject) - for `?s` lookups given predicate+object
- **OSP Index:** Sorted by (object, subject, predicate) - for `?s ?p` lookups given object

Each index is a sorted Arrow table with zero-copy slicing.

### 2.3 Unified Arrow Representation

**Key Insight:** Both property graphs and RDF graphs are stored as Arrow columnar batches.

**Property Graph → Arrow:**
```
VertexTable = Arrow Table
EdgeTable = Arrow Table
CSR indptr = Int64Array
CSR indices = Int64Array
Edge properties = StructArray
```

**RDF Graph → Arrow:**
```
Triple table = Arrow Table
Term dictionary = Arrow Table
SPO index = Sorted Arrow Table
POS index = Sorted Arrow Table
OSP index = Sorted Arrow Table
```

**Benefits:**
1. **Zero-Copy Conversion:** Switch between models without copying data
2. **Vectorized Operations:** Use Arrow compute kernels for both models
3. **Interoperability:** Export to Parquet, Pandas, DuckDB, etc.
4. **SIMD Optimizations:** CPU vectorization for filters, joins, aggregations
5. **Memory Efficiency:** Columnar compression, dictionary encoding

### 2.4 Bi-Temporal Metadata

**Valid-Time:**
- `valid_from`: When the fact became true in the real world
- `valid_to`: When the fact stopped being true (NULL = still valid)
- User-specified or defaults to txn_time

**Transaction-Time:**
- `txn_time`: When the fact was recorded in the database
- Auto-populated from MarbleDB WAL timestamp
- Immutable once written

**Temporal Query Semantics:**
```python
# Point-in-time query (valid-time)
graph.match_pattern('Person', 'WORKS_AT', 'Company')
     .as_of_valid_time('2023-01-01')

# Point-in-time query (transaction-time)
graph.match_pattern('Person', 'WORKS_AT', 'Company')
     .as_of_system_time('2024-01-01')

# Bi-temporal query (both dimensions)
graph.match_pattern('Person', 'WORKS_AT', 'Company')
     .as_of_valid_time('2023-01-01')
     .as_of_system_time('2024-01-01')

# Time-range query
graph.match_pattern('Person', 'WORKS_AT', 'Company')
     .valid_time_between('2020-01-01', '2025-01-01')
```

---

## 3. Query Execution Engine

### 3.1 Query Compilation Pipeline

**Cypher Example:**
```cypher
MATCH (p:Person)-[:WORKS_AT]->(c:Company)
WHERE p.age > 25 AND c.city = 'San Francisco'
RETURN p.name, c.name
FOR VALID_TIME AS OF '2023-01-01'
```

**Compilation Steps:**

**1. Parse → AST:**
```
Match
  Pattern: (p:Person)-[:WORKS_AT]->(c:Company)
  Where: AND(GT(p.age, 25), EQ(c.city, "SF"))
  Return: [p.name, c.name]
  TemporalClause: ValidTime('2023-01-01')
```

**2. AST → Logical Plan:**
```
LogicalPlan:
  Project(p.name, c.name)
    Filter(p.age > 25 AND c.city = "SF")
      TemporalFilter(valid_time = '2023-01-01')
        Join(p.id = edge.src AND c.id = edge.dst)
          Scan(VertexTable, label = "Person")  -> p
          Scan(EdgeTable, type = "WORKS_AT")   -> edge
          Scan(VertexTable, label = "Company") -> c
```

**3. Logical Plan → Optimized Plan:**
```
OptimizedPlan:
  Project(p.name, c.name)
    Join(p.id = edge.src AND c.id = edge.dst)
      TemporalFilter(p.valid_from <= '2023-01-01' < p.valid_to)
        Filter(p.age > 25)
          Scan(VertexTable, label = "Person")  -> p
      Scan(EdgeTable, type = "WORKS_AT")       -> edge
      TemporalFilter(c.valid_from <= '2023-01-01' < c.valid_to)
        Filter(c.city = "SF")
          Scan(VertexTable, label = "Company") -> c
```

**Optimizations Applied:**
- Filter pushdown (age, city filters pushed before join)
- Temporal predicate pushdown (valid_time filters pushed to scans)
- Join reordering (smaller filtered table joined first)
- Dictionary-encoded equality (label/type filters use int32 comparison)

**4. Optimized Plan → Physical Plan:**
```
PhysicalPlan:
  VectorizedProject(p.name, c.name)
    HashJoin(p.id = edge.src, c.id = edge.dst, batch_size=10000)
      VectorizedFilter(p.age > 25)
        TemporalScan(VertexTable, label="Person", as_of='2023-01-01')
      VectorizedScan(EdgeTable, type="WORKS_AT")
      VectorizedFilter(c.city = "SF")
        TemporalScan(VertexTable, label="Company", as_of='2023-01-01')
```

**5. Physical Plan → Arrow Operators:**
- Each operator is a Cython class with `process_batch(RecordBatch) -> RecordBatch`
- Morsel-driven parallelism (10K row morsels)
- Zero-copy between operators

### 3.2 Vectorized Graph Operators

**Inspired by Stardog's BARQ engine:**

**Batch-Based Execution:**
- Operate on batches of 1K-100K rows instead of row-by-row
- SIMD operations via Arrow compute kernels
- Amortize function call overhead
- Better CPU cache utilization

**Key Operators:**

**VectorizedScan:**
```python
# Scan vertex table filtered by label
result = pc.filter(vertex_table, pc.equal(vertex_table['label'], 'Person'))
# Returns: Arrow Table with all Person vertices
```

**VectorizedFilter:**
```python
# Filter by property predicate
mask = pc.greater(vertex_table['age'], 25)
result = pc.filter(vertex_table, mask)
# Returns: Arrow Table with age > 25
```

**HashJoin (Vectorized):**
```python
# Hash join on vertex ID
left_table = persons  # (id, name, age)
right_table = edges   # (src, dst, type)
result = hash_join(left_table, right_table, left_key='id', right_key='src')
# Returns: Arrow Table with joined rows
```

**TemporalFilter:**
```python
# Bi-temporal filter
valid_mask = (pc.less_equal(table['valid_from'], as_of_time) &
              pc.greater(table['valid_to'], as_of_time))
result = pc.filter(table, valid_mask)
# Returns: Arrow Table with valid rows at as_of_time
```

**Performance:**
- 10-100x faster than row-by-row execution
- SIMD vectorization for filters, arithmetic
- Dictionary-encoded comparisons (int32 vs string)

### 3.3 Pattern Matching Engine

**Multi-Hop Pattern Example:**
```cypher
MATCH (a:Person)-[:KNOWS*2..4]->(b:Person)
WHERE a.name = 'Alice'
RETURN b.name
```

**Execution Strategy:**

**1. Filter Start Vertices:**
```python
start_vertices = filter_by_label('Person')
start_vertices = filter_by_property(start_vertices, 'name', 'Alice')
# Result: Single vertex with ID = 0 (Alice)
```

**2. Expand to Hop 1:**
```python
hop1_neighbors = csr.get_neighbors_batch(start_vertices)
# Result: All 1-hop neighbors of Alice
```

**3. Filter Hop 1 by Edge Type:**
```python
hop1_edges = filter_edges_by_type(hop1_neighbors, 'KNOWS')
# Result: Only KNOWS edges
```

**4. Expand to Hop 2:**
```python
hop2_neighbors = csr.get_neighbors_batch(hop1_edges.dst)
# Result: All 2-hop neighbors
```

**5. Repeat for Hops 3-4:**
```python
# Continue expanding until max hop count (4)
```

**6. Filter End Vertices:**
```python
end_vertices = filter_by_label(final_neighbors, 'Person')
# Result: Only Person vertices
```

**7. Project Result:**
```python
result = project_columns(end_vertices, ['name'])
# Result: Names of all 2-4 hop KNOWS neighbors
```

**Optimizations:**
- Prune paths that violate predicates early
- Use visited set to avoid cycles
- Batch neighbor lookups (vectorized)
- Cache intermediate results

### 3.4 Graph Algorithms

**PageRank (Iterative):**
```python
def pagerank(graph, damping=0.85, max_iter=20, tol=1e-6):
    N = graph.num_vertices()
    ranks = np.ones(N) / N  # Initial rank

    for iter in range(max_iter):
        new_ranks = np.zeros(N)

        # Vectorized rank propagation
        for v in range(N):
            neighbors = graph.get_neighbors(v)
            degree = len(neighbors)
            if degree > 0:
                contribution = ranks[v] / degree
                new_ranks[neighbors] += contribution

        # Apply damping
        new_ranks = (1 - damping) / N + damping * new_ranks

        # Check convergence
        if np.linalg.norm(new_ranks - ranks) < tol:
            break

        ranks = new_ranks

    return ranks
```

**BFS (Level-Synchronous):**
```python
def bfs(graph, start_vertex):
    visited = set([start_vertex])
    frontier = [start_vertex]
    distances = {start_vertex: 0}

    level = 0
    while frontier:
        next_frontier = []

        # Vectorized neighbor lookup
        neighbors_batch = graph.csr().get_neighbors_batch(pa.array(frontier))

        for row in neighbors_batch.to_pylist():
            neighbor = row['neighbor_id']
            if neighbor not in visited:
                visited.add(neighbor)
                next_frontier.append(neighbor)
                distances[neighbor] = level + 1

        frontier = next_frontier
        level += 1

    return distances
```

**Community Detection (Label Propagation):**
```python
def label_propagation(graph, max_iter=100):
    N = graph.num_vertices()
    labels = np.arange(N)  # Initial: each vertex = own community

    for iter in range(max_iter):
        changed = False

        for v in range(N):
            neighbors = graph.get_neighbors(v)
            if len(neighbors) == 0:
                continue

            # Find most frequent label among neighbors
            neighbor_labels = labels[neighbors.to_numpy()]
            counts = np.bincount(neighbor_labels)
            most_frequent = np.argmax(counts)

            if labels[v] != most_frequent:
                labels[v] = most_frequent
                changed = True

        if not changed:
            break

    return labels
```

---

## 4. Temporal Data Model

### 4.1 Bi-Temporal Semantics

**Valid-Time (Effective Time):**
- Represents when a fact is true in the real world
- User-specified or defaults to transaction-time
- Example: Alice worked at Acme from 2020 to 2024 (valid-time)

**Transaction-Time (System Time):**
- Represents when a fact was recorded in the database
- Auto-populated from MarbleDB WAL timestamp
- Immutable once written
- Example: The system recorded this fact on 2025-10-09 (txn-time)

**Bi-Temporal Table:**
```
┌──────┬─────────┬──────────────┬────────────┬──────────────┬────────┐
│ id   │ name    │ valid_from   │ valid_to   │ txn_time     │ salary │
├──────┼─────────┼──────────────┼────────────┼──────────────┼────────┤
│ 100  │ Alice   │ 2020-01-01   │ 2024-12-31 │ 2020-01-02   │ 80K    │ Row 1
│ 100  │ Alice   │ 2025-01-01   │ NULL       │ 2025-01-02   │ 100K   │ Row 2
└──────┴─────────┴──────────────┴────────────┴──────────────┴────────┘
```

**Interpretation:**
- Row 1: Alice earned $80K from 2020-2024 (valid-time), recorded on 2020-01-02 (txn-time)
- Row 2: Alice earns $100K from 2025-present (valid-time), recorded on 2025-01-02 (txn-time)

### 4.2 Temporal Query Examples

**Query 1: What was Alice's salary on 2023-06-01? (Valid-Time)**
```cypher
MATCH (p:Person {name: 'Alice'})
RETURN p.salary
FOR VALID_TIME AS OF '2023-06-01'

Result: 80K  (Row 1 is valid on 2023-06-01)
```

**Query 2: What did the database think Alice's salary was on 2024-01-01? (Transaction-Time)**
```cypher
MATCH (p:Person {name: 'Alice'})
RETURN p.salary
FOR SYSTEM_TIME AS OF '2024-01-01'

Result: 80K  (Row 1 was recorded before 2024-01-01, Row 2 not yet recorded)
```

**Query 3: What was the salary recorded in 2025 for the period 2023? (Bi-Temporal)**
```cypher
MATCH (p:Person {name: 'Alice'})
RETURN p.salary
FOR VALID_TIME AS OF '2023-06-01'
AND FOR SYSTEM_TIME AS OF '2025-02-01'

Result: 80K  (Row 1: valid on 2023-06-01, recorded before 2025-02-01)
```

**Query 4: Show salary history (all versions)**
```cypher
MATCH (p:Person {name: 'Alice'})
RETURN p.salary, p.valid_from, p.valid_to, p.txn_time
ORDER BY p.txn_time

Result:
  80K, 2020-01-01, 2024-12-31, 2020-01-02
  100K, 2025-01-01, NULL, 2025-01-02
```

### 4.3 Temporal Indexes

**Interval Tree (Valid-Time Range Queries):**
```
Query: Find all vertices valid between 2022-2024

Interval Tree:
  [2020-2024]: [Alice (Row 1), Bob, ...]
  [2025-NULL]: [Alice (Row 2), Charlie, ...]

Lookup: O(log N + K) where K = number of overlapping intervals
```

**B-Tree (Transaction-Time Point Queries):**
```
Query: Find all vertices recorded before 2024-01-01

B-Tree on txn_time:
  2020-01-02 -> [Alice (Row 1), ...]
  2021-03-15 -> [Bob, ...]
  2025-01-02 -> [Alice (Row 2), ...]

Lookup: O(log N)
```

**Combined Bi-Temporal Index:**
- 2D index on (valid_from, txn_time)
- Range queries on both dimensions
- Used for bi-temporal queries

### 4.4 Copy-on-Write Versioning

**Update Semantics:**
```python
# Initial insert
graph.add_vertex(
    id=100,
    label='Person',
    name='Alice',
    salary=80000,
    valid_from='2020-01-01'
)
# Creates Row 1 with txn_time=now()

# Update (copy-on-write)
graph.update_vertex(
    id=100,
    salary=100000,
    valid_from='2025-01-01'
)
# Creates Row 2 with txn_time=now()
# Sets Row 1.valid_to='2024-12-31'
```

**Storage:**
- Old versions retained for time-travel queries
- Compaction removes expired versions (configurable retention)
- Archival to object storage (S3/GCS) for long-term history

---

## 5. Streaming Integration

### 5.1 Stream → Graph Pipeline

**Architecture:**
```
Kafka/CDC Stream → Sabot Agent → Graph Mutations → MarbleDB
                                      ↓
                               Pattern Matching
                                      ↓
                               Alert Stream (Kafka)
```

**Example:**
```python
import sabot as sb

app = sb.App('fraud-detection')
graph = sb.Graph(name='transactions', model='property_graph')

@app.agent(app.topic('transactions'))
async def build_transaction_graph(stream):
    """Build graph from transaction stream."""
    async for batch in stream.batch():
        # Add vertices (accounts)
        for txn in batch:
            await graph.add_vertex(
                id=txn.account_id,
                label='Account',
                balance=txn.balance,
                risk_score=txn.risk_score,
                valid_from=txn.timestamp
            )

        # Add edges (transfers)
        for txn in batch:
            await graph.add_edge(
                src=txn.from_account,
                dst=txn.to_account,
                type='TRANSFER',
                amount=txn.amount,
                valid_from=txn.timestamp
            )

@app.agent(app.topic('pattern_queries'))
async def detect_fraud_patterns(stream):
    """Continuous pattern matching for fraud."""
    async for query in stream:
        # Pattern: Money laundering rings
        # (a:Account)-[:TRANSFER]->(b:Account)-[:TRANSFER]->(c:Account)
        # WHERE a == c  (circular transfer)

        result = graph.match_pattern(
            '(a:Account)-[:TRANSFER]->(b:Account)-[:TRANSFER]->(c:Account)',
            where='a.id == c.id AND amount > 10000'
        )

        async for match in result:
            # Alert on suspicious pattern
            await app.topic('fraud_alerts').send({
                'accounts': [match.a, match.b, match.c],
                'pattern': 'circular_transfer',
                'timestamp': sb.current_event_time()
            })
```

### 5.2 Continuous Pattern Matching

**Standing Queries:**
```python
# Register standing query (runs continuously)
fraud_query = graph.register_standing_query(
    pattern='(a:Account)-[:TRANSFER *2..4]->(b:Account)',
    where='a.risk_score > 0.8 AND sum(edge.amount) > 100000',
    on_match=lambda match: alert_fraud(match)
)

# Query updates incrementally as graph changes
# No full recalculation required
```

**Incremental Evaluation:**
- Graph mutations trigger pattern re-evaluation
- Only affected patterns recomputed
- Delta-based updates (add/remove matches)
- Sub-second latency for alerts

### 5.3 Event-Time Graph Processing

**Watermarks and Graph Mutations:**
```python
@app.agent(app.topic('events'))
async def time_aware_graph(stream):
    """Graph mutations aligned with event-time watermarks."""
    async for event in stream:
        # Add vertex with event-time as valid_from
        await graph.add_vertex(
            id=event.entity_id,
            label=event.entity_type,
            valid_from=event.event_time,  # Event-time, not wall-clock
            **event.properties
        )

        # Watermark indicates all events <= watermark have arrived
        watermark = stream.current_watermark()

        # Safe to compact graph (remove expired versions)
        if watermark:
            await graph.compact_before(watermark)
```

**Late Arrivals:**
- Late events (< watermark) still processed
- Graph mutations backfilled with correct valid_from
- Triggers re-evaluation of affected patterns
- Configurable allowed lateness

### 5.4 Windowed Graph Queries

**Time Windows:**
```python
# Tumbling window: Last 1 hour
windowed_graph = graph.window(
    size='1 hour',
    strategy='tumbling'
)

# Sliding window: Last 5 minutes, every 1 minute
windowed_graph = graph.window(
    size='5 minutes',
    slide='1 minute',
    strategy='sliding'
)

# Session window: Burst of activity with 10 min timeout
windowed_graph = graph.window(
    gap='10 minutes',
    strategy='session'
)

# Query windowed graph
result = windowed_graph.match_pattern(
    '(a:Account)-[:TRANSFER]->(b:Account)',
    where='amount > 10000'
)
# Returns only matches in current window
```

**Window Implementation:**
- Filter graph by valid_from in window range
- Use temporal indexes for efficient filtering
- Window boundaries aligned with watermarks
- Zero-copy slicing of Arrow tables

### 5.5 Dynamic Graph Algorithms

**Incremental PageRank (Memgraph-style):**
```python
# Initial PageRank computation
ranks = graph.pagerank()

# Graph mutation (add edge)
graph.add_edge(src=0, dst=1, type='FOLLOWS')

# Incremental update (10-100x faster than full recomputation)
new_ranks = graph.pagerank_update(
    added_edges=[(0, 1)],
    previous_ranks=ranks
)
```

**How It Works:**
1. Detect which vertices affected by edge addition/removal
2. Recompute only affected subgraph
3. Propagate changes via BFS from affected vertices
4. Converge faster (fewer iterations)

**Supported Dynamic Algorithms:**
- PageRank (incremental score updates)
- Community detection (label propagation)
- Shortest paths (A* with known bounds)
- Connected components (union-find)

---

## 6. Arrow Optimizations

### 6.1 Zero-Copy Data Flow

**End-to-End Arrow Pipeline:**
```
Kafka (bytes) → Arrow Deserializer → RecordBatch
                                        ↓
                                  Graph Mutations
                                        ↓
                                  VertexTable (Arrow)
                                  EdgeTable (Arrow)
                                        ↓
                                  Pattern Matching
                                        ↓
                                  Result Batch (Arrow)
                                        ↓
                                  Kafka Serializer → bytes
```

**Zero-Copy Guarantees:**
- Neighbor lookups return Arrow array slices (no copy)
- Filters create boolean masks (no copy of data)
- Projections create column views (no copy of columns)
- Joins use Arrow compute kernels (zero-copy where possible)

**Memory Savings:**
- 10GB graph → 1GB temporary memory (vs 10GB+ for copy-based)
- Batch processing amortizes overhead
- Dictionary encoding reduces string storage by 4x

### 6.2 Vectorized Graph Kernels

**SIMD Operations:**
```python
# Filter vertices by age > 25 (SIMD vectorized)
import pyarrow.compute as pc

mask = pc.greater(vertices['age'], 25)  # SIMD comparison
filtered = pc.filter(vertices, mask)     # SIMD gather

# 10-100x faster than Python loop
```

**Batch Neighbor Lookups:**
```python
# Lookup neighbors for 10K vertices (vectorized)
vertices = pa.array([0, 1, 2, ..., 9999])
neighbors = csr.get_neighbors_batch(vertices)

# Single batch operation vs 10K individual lookups
# Result: 100-1000x faster
```

**Dictionary-Encoded Filters:**
```python
# Filter by label (dictionary-encoded, int32 comparison)
label_dict = {'Person': 0, 'Company': 1}
mask = pc.equal(vertices['label'], 0)  # int32 comparison (SIMD)
# vs string comparison (slow, no SIMD)
```

### 6.3 Columnar Compression

**Dictionary Encoding:**
- Labels, types, IRIs → int32 indices
- 4x memory savings
- Faster comparisons
- Shared dictionaries across batches

**Run-Length Encoding:**
- Sorted data (e.g., temporal columns) → RLE
- 10-100x compression for timestamp sequences

**Null Bitmap:**
- NULL values → single bit per value
- 64x savings vs sentinel values

**Example:**
```
Original:
  labels: ['Person', 'Person', 'Company', 'Person']  (320 bytes)

Dictionary Encoded:
  dictionary: ['Person', 'Company']                   (80 bytes)
  indices: [0, 0, 1, 0]                               (16 bytes)
  Total: 96 bytes  → 3.3x compression
```

### 6.4 Integration with Arrow Ecosystem

**Parquet Export:**
```python
# Export graph to Parquet
vertices_table = graph.vertices().table()
pa.parquet.write_table(vertices_table, 'vertices.parquet')

# Later: Load and query with DuckDB
import duckdb
duckdb.query("SELECT * FROM 'vertices.parquet' WHERE age > 25")
```

**Pandas Interop:**
```python
# Convert to Pandas for analysis
df = graph.vertices().table().to_pandas()
df.groupby('label')['age'].mean()
```

**DuckDB Integration:**
```python
# Register graph as DuckDB table (zero-copy)
import duckdb
conn = duckdb.connect()
conn.register('vertices', graph.vertices().table())
conn.query("SELECT label, COUNT(*) FROM vertices GROUP BY label")
```

**Flight Export:**
```python
# Stream graph to remote system via Arrow Flight
flight_server = pa.flight.FlightServer()
flight_server.do_put(graph.vertices().table(), descriptor='vertices')
```

---

## 7. Comparison with Other Systems

### 7.1 Feature Matrix

| Feature | Sabot | Memgraph | PuppyGraph | Stardog |
|---------|-------|----------|------------|---------|
| **Data Models** | Property Graph + RDF | Property Graph | Property Graph | RDF |
| **Query Languages** | Cypher, SPARQL, Gremlin | Cypher | Cypher, Gremlin | SPARQL |
| **Streaming Ingestion** | ✅ Native (Kafka, CDC) | ✅ Native (Kafka) | ❌ Virtual only | ❌ Batch only |
| **Bi-Temporal** | ✅ Native (valid + txn time) | ⚠️ TTL only | ❌ None | ⚠️ Basic versioning |
| **Reasoning** | ✅ RDFS, OWL 2 QL/RL | ❌ None | ❌ None | ✅ RDFS, OWL 2 |
| **Storage Format** | Arrow Columnar | Row-based | Virtual (no storage) | Triple store |
| **Vectorized Execution** | ✅ BARQ-style batching | ❌ Row-by-row | ✅ Vectorized | ✅ BARQ engine |
| **Distributed** | ✅ Raft + Flight shuffle | ⚠️ HA only | ✅ Multi-source | ⚠️ Cluster mode |
| **Virtual Graphs** | ✅ R2RML + JSON schema | ❌ None | ✅ Zero-ETL core | ✅ R2RML |
| **ACID Transactions** | ✅ Raft consensus | ✅ ACID | ❌ Read-only | ✅ ACID |
| **Dynamic Algorithms** | ✅ Incremental | ✅ Incremental | ❌ None | ❌ None |
| **Graph Algorithms** | BFS, PageRank, etc. | MAGE library | Built-in | Limited |
| **Open Source** | ✅ Planned | ✅ Yes | ❌ Proprietary | ❌ Enterprise |

### 7.2 Performance Comparison

**Property Graph Queries:**

| Operation | Sabot (Target) | Memgraph | Neo4j | PuppyGraph |
|-----------|---------------|----------|-------|-----------|
| 1-hop traversal | <1μs | <1ms | 1-10ms | 1-10ms |
| 2-hop pattern | <10ms | 1-10ms | 10-100ms | 10-50ms |
| 5-hop pattern | <100ms | 10-100ms | timeout | <3s |
| PageRank (1M vertices) | <1s | <5s | minutes | minutes |
| Pattern match (1M edges) | 1-10M/sec | 1-5M/sec | 100K/sec | 1-10M/sec |

**RDF Queries:**

| Operation | Sabot (Target) | Stardog | GraphDB | Jena |
|-----------|---------------|---------|---------|------|
| Triple pattern (1M triples) | 1-10M/sec | 1-10M/sec (BARQ) | 100K-1M/sec | 10K-100K/sec |
| SPARQL join (3 patterns) | <10ms | 10-100ms | 100-500ms | seconds |
| Reasoning (RDFS) | <10ms overhead | <50ms overhead | 100ms overhead | seconds |
| Time-travel query | <50ms | N/A | N/A | N/A |

**Streaming Workloads:**

| Operation | Sabot (Target) | Memgraph | Kafka Streams | Flink |
|-----------|---------------|----------|---------------|-------|
| Graph mutations | 100K-1M/sec | 100K/sec | N/A | N/A |
| Continuous pattern matching | <10ms latency | 10-100ms | N/A | 100ms-1s |
| Dynamic PageRank update | <100ms | <500ms | N/A | N/A |
| End-to-end pipeline | <50ms | <200ms | <100ms | <200ms |

### 7.3 Unique Advantages

**Sabot's Differentiators:**

**1. Bi-Temporal Time-Travel**
- Query graph state at any point in valid-time or transaction-time
- Audit trails for compliance (finance, healthcare)
- Forensic analysis (what happened when)
- No competitor has native bi-temporal support

**2. Hybrid RDF + Property Graphs**
- Switch between data models without copying data
- Use Cypher for operational queries
- Use SPARQL for semantic queries
- Reasoning works across both models

**3. Arrow-Native Performance**
- Columnar storage → 10-100x faster than row-based
- Zero-copy operations throughout pipeline
- SIMD vectorization for filters, joins
- Interoperable with Pandas, DuckDB, Parquet

**4. Streaming-First Architecture**
- Built for continuous graph construction
- No batch ETL required
- Event-time semantics
- Sub-second latency for pattern matching

**5. Distributed Graph with Raft**
- Strong consistency via Raft consensus
- Automatic failover and recovery
- Zero-copy shuffle via Arrow Flight
- Linear scaling to 10B+ edges

### 7.4 Trade-offs

**Sabot's Limitations:**

**1. Memory Overhead (Bi-Temporal)**
- Storing multiple versions increases storage by 20-50%
- Trade-off: Audit trails and time-travel queries
- Mitigation: Configurable retention and compaction

**2. Complexity (Hybrid Model)**
- Supporting both property graphs and RDF adds complexity
- Trade-off: Flexibility for different use cases
- Mitigation: Users choose model per graph

**3. Learning Curve (Multiple Query Languages)**
- Cypher, SPARQL, Gremlin require different skills
- Trade-off: Use the right tool for the job
- Mitigation: Unified optimizer, consistent API

**4. Maturity (New System)**
- Sabot is newer than Neo4j, Stardog, etc.
- Trade-off: Modern architecture vs proven stability
- Mitigation: Extensive testing, gradual rollout

---

**Document Status:** Complete (Sections 1-7)
**Last Updated:** October 2025
**Total Lines:** 1,100+
**Next:** Mark progress checkboxes complete
