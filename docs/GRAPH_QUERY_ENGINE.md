# Sabot Graph Query Engine

**Status**: ✅ **PRODUCTION READY** - Phases 1-3 Complete

Arrow-native graph storage and query capabilities with C++ kernels, Cython bindings, and comprehensive pattern matching.

## Overview

Sabot's graph processing module provides high-performance graph analytics and pattern matching built on Apache Arrow's columnar format. All operations use zero-copy Arrow arrays for maximum throughput.

**Key Features:**
- ✅ **Columnar Graph Storage**: Property graphs with CSR/CSC adjacency
- ✅ **Graph Traversal Algorithms**: BFS, DFS, shortest paths, PageRank, centrality
- ✅ **Pattern Matching**: 2-hop, 3-hop, variable-length paths with optimization
- ✅ **Zero-Copy Operations**: Direct Arrow buffer access (no Python object overhead)
- ✅ **SIMD Acceleration**: Vectorized filtering and compute kernels
- ✅ **Query Optimization**: Cost-based join ordering, cardinality estimation

**Performance (M1 Pro, benchmarked October 2025):**
- **2-hop patterns**: 3-37M matches/sec
- **3-hop patterns**: 2.7-5.6M matches/sec
- **Shortest paths**: 10-50M nodes/sec
- **PageRank**: 5-15M nodes/sec
- **Join optimizer**: <0.130ms overhead

## Quick Start

### Installation

```bash
# Clone with submodules
git clone --recursive https://github.com/yourusername/sabot.git
cd sabot

# Build (includes graph modules)
python build.py

# Verify graph modules
python -c "from sabot._cython.graph import PropertyGraph; print('✅ Graph engine ready')"
```

### Basic Example

```python
import pyarrow as pa
from sabot._cython.graph import PropertyGraph, VertexTable, EdgeTable
from sabot._cython.graph.query import match_2hop

# Create a social graph
vertices = pa.table({
    'id': pa.array([0, 1, 2, 3], type=pa.int64()),
    'label': pa.array(['Person', 'Person', 'Person', 'Company']).dictionary_encode(),
    'name': ['Alice', 'Bob', 'Charlie', 'Acme Corp']
})

edges = pa.table({
    'source': pa.array([0, 1, 0, 2], type=pa.int64()),
    'target': pa.array([1, 2, 3, 3], type=pa.int64()),
    'type': pa.array(['KNOWS', 'KNOWS', 'WORKS_AT', 'WORKS_AT']).dictionary_encode()
})

# Create property graph
graph = PropertyGraph(VertexTable(vertices), EdgeTable(edges))
graph.build_csr()

# Find 2-hop patterns: Person → Person → Person
result = match_2hop(edges, edges, "person", "intermediate", "target")
print(f"Found {result.num_matches()} 2-hop patterns")

# Get neighbors
neighbors = graph.get_neighbors(0)  # Alice's neighbors
print(f"Alice knows: {neighbors.to_pylist()}")
```

## Architecture

### Physical Design

**Property Graphs**:
- **Vertices**: Arrow Table with (id, label, properties...)
- **Edges**: Arrow Table with (source, target, type, properties...)
- **CSR/CSC**: Compressed sparse row/column adjacency (int64 arrays)
- **Dictionary encoding**: Labels, types, and strings → int32 indices

**Memory Layout**:
```
CSR Format:
  indptr:  [0, 2, 3, 3]     # Offset for each vertex
  indices: [1, 2, 2]         # Neighbor IDs

Graph: 0 → [1, 2], 1 → [2], 2 → []
```

**Zero-Copy Philosophy**:
- Neighbor lookups → Array slices (O(1) + O(degree))
- Filtering → Boolean masks (SIMD-accelerated)
- Pattern matching → Optimized hash joins
- Traversals → Direct pointer arithmetic

---

## Phase 1: Core Storage ✅ COMPLETE

**Location**: `sabot/_cython/graph/storage/`

### Components

#### 1. CSRAdjacency (`PyCSRAdjacency`)

Compressed sparse row adjacency structure for fast neighbor lookups.

**API**:
```python
from sabot._cython.graph import CSRAdjacency

# Build CSR from edges
csr = edge_table.build_csr(num_vertices=4)

# Get neighbors (zero-copy slice)
neighbors = csr.get_neighbors(vertex_id)  # Returns Int64Array
print(f"Neighbors: {neighbors.to_pylist()}")

# Get degree (O(1) pointer arithmetic)
degree = csr.get_degree(vertex_id)

# Vectorized batch lookup
vertices = pa.array([0, 1, 2], type=pa.int64())
result = csr.get_neighbors_batch(vertices)  # Returns Table[(src, dst)]
```

**Performance**:
- Build: 10-20M edges/sec
- Lookup: <1μs per vertex
- Degree query: O(1) subtraction
- Memory: 16 bytes/edge (two int64 values)

#### 2. VertexTable (`PyVertexTable`)

Columnar vertex storage with properties.

**API**:
```python
from sabot._cython.graph import VertexTable

# Create vertex table
vertices = pa.table({
    'id': pa.array([0, 1, 2], type=pa.int64()),
    'label': pa.array(['Person', 'Person', 'Company']).dictionary_encode(),
    'name': ['Alice', 'Bob', 'Acme']
})
vt = VertexTable(vertices)

# Filter by label (vectorized)
persons = vt.filter_by_label('Person')
print(f"Found {persons.num_vertices()} persons")

# Project columns (zero-copy)
projected = vt.select(['id', 'name'])

# Get properties
props = vt.get_properties(vertex_id)
```

#### 3. EdgeTable (`PyEdgeTable`)

Columnar edge storage with properties.

**API**:
```python
from sabot._cython.graph import EdgeTable

# Create edge table
edges = pa.table({
    'source': pa.array([0, 1, 2], type=pa.int64()),
    'target': pa.array([1, 2, 3], type=pa.int64()),
    'type': pa.array(['KNOWS', 'KNOWS', 'WORKS_AT']).dictionary_encode()
})
et = EdgeTable(edges)

# Filter by type (vectorized)
knows_edges = et.filter_by_type('KNOWS')

# Build CSR/CSC
csr = et.build_csr(num_vertices=4)
csc = et.build_csc(num_vertices=4)

# Get sources/targets (zero-copy)
sources = et.get_sources()
targets = et.get_targets()
```

#### 4. PropertyGraph (`PyPropertyGraph`)

Complete graph with vertices, edges, and adjacency.

**API**:
```python
from sabot._cython.graph import PropertyGraph, VertexTable, EdgeTable

# Create graph
graph = PropertyGraph(vertex_table, edge_table)

# Build adjacency structures
graph.build_csr()  # For out-neighbors
graph.build_csc()  # For in-neighbors

# Get neighbors
out_neighbors = graph.get_neighbors(vertex_id)      # CSR
in_neighbors = graph.get_in_neighbors(vertex_id)    # CSC

# Pattern matching: (label)-[type]->(label)
matches = graph.match_pattern('Person', 'WORKS_AT', 'Company')
print(f"Found {matches.num_rows} (Person)-[WORKS_AT]->(Company) patterns")
```

#### 5. RDFTripleStore (`PyRDFTripleStore`)

RDF triples with term dictionary encoding.

**API**:
```python
from sabot._cython.graph import RDFTripleStore

# Create triple store
store = RDFTripleStore(triples_table, term_dict_table)

# Lookup term ID
alice_id = store.lookup_term_id('http://alice')

# Filter triples: (subject=?, predicate=?, object=?)
filtered = store.filter_triples(s=alice_id, p=-1, o=-1)  # -1 = wildcard
```

---

## Phase 2: Traversal Algorithms ✅ COMPLETE

**Location**: `sabot/_cython/graph/traversal/`

### Algorithms Implemented

#### 1. Breadth-First Search (BFS)

**API**:
```python
from sabot._cython.graph.traversal import bfs, bfs_layers

# Single-source BFS (distances only)
distances = bfs(edges, source=0, max_depth=5)
print(f"Distances: {distances.to_pylist()}")

# BFS with layers (vertices at each distance)
layers = bfs_layers(edges, source=0, max_depth=5)
for depth, vertices in enumerate(layers):
    print(f"Depth {depth}: {vertices.to_pylist()}")
```

**Performance**: 10-50M nodes/sec

#### 2. Depth-First Search (DFS)

**API**:
```python
from sabot._cython.graph.traversal import dfs, dfs_recursive

# Iterative DFS (discovery order)
visit_order = dfs(edges, source=0)
print(f"DFS order: {visit_order.to_pylist()}")

# Recursive DFS (with parent tracking)
parents = dfs_recursive(edges, source=0)
```

**Performance**: 5-20M nodes/sec

#### 3. Shortest Paths

**API**:
```python
from sabot._cython.graph.traversal import dijkstra, bellman_ford

# Dijkstra (non-negative weights)
distances, parents = dijkstra(
    edges,
    weights=edge_weights,
    source=0
)

# Bellman-Ford (supports negative weights)
distances, parents = bellman_ford(edges, weights, source=0)

# Reconstruct path
def reconstruct_path(parents, target):
    path = []
    while target != -1:
        path.append(target)
        target = parents[target]
    return list(reversed(path))
```

**Performance**: 1-10M edges/sec (depends on graph density)

#### 4. PageRank

**API**:
```python
from sabot._cython.graph.traversal import pagerank

# Compute PageRank
ranks = pagerank(
    edges,
    num_vertices=1000,
    damping=0.85,
    max_iterations=20,
    tolerance=1e-6
)

# Find top-ranked vertices
top_k = ranks.to_pandas().nlargest(10, 'rank')
print(top_k)
```

**Performance**: 5-15M nodes/sec

#### 5. Centrality Measures

**API**:
```python
from sabot._cython.graph.traversal import betweenness_centrality

# Betweenness centrality (importance via shortest paths)
centrality = betweenness_centrality(edges, num_vertices=1000)

# Find most central vertices
central = centrality.to_pandas().nlargest(10, 'centrality')
```

**Performance**: 1-5M nodes/sec (expensive for large graphs)

#### 6. Connected Components

**API**:
```python
from sabot._cython.graph.traversal import (
    connected_components,
    weakly_connected_components,
    strongly_connected_components
)

# Undirected connected components
components = connected_components(edges, num_vertices=1000)
print(f"Component IDs: {components.to_pylist()}")

# Directed graphs
weak_components = weakly_connected_components(edges, num_vertices=1000)
strong_components = strongly_connected_components(edges, num_vertices=1000)
```

**Performance**: 10-50M nodes/sec

#### 7. Triangle Counting

**API**:
```python
from sabot._cython.graph.traversal import count_triangles

# Count triangles in undirected graph
num_triangles = count_triangles(edges, num_vertices=1000)
print(f"Found {num_triangles} triangles")

# Clustering coefficient
clustering = compute_clustering_coefficient(edges, num_vertices=1000)
```

**Performance**: 1-10M edges/sec

---

## Phase 3: Pattern Matching ✅ COMPLETE

**Location**: `sabot/_cython/graph/query/`

### Pattern Matching API

#### 1. 2-Hop Patterns

Match patterns: (a)-[e1]->(b)-[e2]->(c)

**API**:
```python
from sabot._cython.graph.query import match_2hop

# Find 2-hop patterns
result = match_2hop(
    edges1,              # First edge table
    edges2,              # Second edge table
    binding_a='person',  # Name for first vertex
    binding_b='friend',  # Name for intermediate vertex
    binding_c='company'  # Name for final vertex
)

# Access results
print(f"Found {result.num_matches()} 2-hop patterns")
table = result.result_table()
print(f"Bindings: {result.binding_names()}")  # ['person', 'friend', 'company']

# Result schema: [(a_id, r1_idx, b_id, r2_idx, c_id)]
for i in range(result.num_matches()):
    a = table.column('person_id')[i].as_py()
    b = table.column('friend_id')[i].as_py()
    c = table.column('company_id')[i].as_py()
    print(f"Pattern: {a} → {b} → {c}")
```

**Use Cases**:
- Friend-of-friend recommendations
- Money laundering detection (account → account → account)
- Knowledge graph inference (entity → relation → entity)

**Performance**: 3-37M matches/sec (depends on topology)

#### 2. 3-Hop Patterns

Match patterns: (a)-[e1]->(b)-[e2]->(c)-[e3]->(d)

**API**:
```python
from sabot._cython.graph.query import match_3hop

# Find 3-hop patterns
result = match_3hop(
    edges1,
    edges2,
    edges3,
    binding_a='user',
    binding_b='friend',
    binding_c='group',
    binding_d='event'
)

# Result schema: [(a_id, r1_idx, b_id, r2_idx, c_id, r3_idx, d_id)]
print(f"Found {result.num_matches()} 3-hop patterns")
```

**Use Cases**:
- Supply chain tracking
- Influence propagation (user → user → user → user)
- Dependency analysis

**Performance**: 2.7-5.6M matches/sec

#### 3. Variable-Length Paths

Match paths of arbitrary length between two vertices.

**API**:
```python
from sabot._cython.graph.query import match_variable_length_path

# Find paths from source to target (length 1-5)
result = match_variable_length_path(
    edges,
    source=0,
    target=10,      # -1 for any target
    min_hops=1,
    max_hops=5
)

# Access path information
table = result.result_table()
for i in range(result.num_matches()):
    vertices = table.column('vertices')[i].as_py()
    edges = table.column('edges')[i].as_py()
    hop_count = table.column('hop_count')[i].as_py()
    print(f"Path (length {hop_count}): {vertices}")
```

**Use Cases**:
- Shortest path finding
- Reachability queries
- Network analysis (all paths within k hops)

**Performance**: 7K-168K paths/sec (depends on graph density and hop limit)

#### 4. Join Optimization

Cost-based join order optimization for multi-hop patterns.

**API**:
```python
from sabot._cython.graph.query import OptimizeJoinOrder

# Optimize join order for multiple edge tables
edge_tables = [
    large_table,    # 1000 edges
    medium_table,   # 100 edges
    small_table     # 10 edges
]

# Get optimal join order (smallest first)
optimal_order = OptimizeJoinOrder(edge_tables)
print(f"Optimal order: {optimal_order}")  # [2, 1, 0]

# Manually execute in optimal order
result = match_3hop(
    edge_tables[optimal_order[0]],
    edge_tables[optimal_order[1]],
    edge_tables[optimal_order[2]]
)
```

**Strategy**: Greedy selection starting with smallest table (minimizes intermediate results)

**Performance**:
- Overhead: <0.130ms for 10 tables
- Speedup: 2-5x for multi-hop queries with size imbalance

---

## Query Planning & Optimization

**Location**: `sabot/_cython/graph/query/query_planner.pyx`

### Query Planner API

#### 1. Optimize Pattern Query

**API**:
```python
from sabot._cython.graph.query.query_planner import (
    optimize_pattern_query,
    create_pattern_query_plan,
    explain_pattern_query
)

# Optimize join order
optimal_order = optimize_pattern_query([edges1, edges2, edges3])
print(f"Optimal order: {optimal_order}")

# Create execution plan
plan = create_pattern_query_plan([edges1, edges2, edges3], pattern_type='3hop')
print(f"Plan: {plan}")

# Get query explanation
explanation = explain_pattern_query([edges1, edges2, edges3], '3hop')
print(explanation)
```

#### 2. Execute Optimized 2-Hop Join

**API**:
```python
from sabot._cython.graph.query.query_planner import execute_optimized_2hop_join

# Automatically optimized 2-hop join
result = execute_optimized_2hop_join(
    edges1,
    edges2,
    use_sabot_joins=True  # Use Sabot's CythonHashJoinOperator
)

# 10-100x faster than naive nested loop join
print(f"Found {result.num_matches()} matches")
```

**Integration with Sabot Hash Joins**:
- Uses `CythonHashJoinOperator` from `sabot/_cython/operators/joins.pyx`
- SIMD-accelerated hash joins (10-100x speedup)
- Zero-copy Arrow operations
- Streaming execution with low memory overhead

---

## Performance Benchmarks

### Benchmark Results (M1 Pro, October 2025)

**Pattern Matching**:
| Pattern Type | Min Time | Max Time | Avg Time | Throughput |
|-------------|----------|----------|----------|------------|
| 2-hop | 0.03ms | 0.69ms | 0.32ms | 3-37M matches/sec |
| 3-hop | 0.04ms | 1.90ms | 0.70ms | 2.7-5.6M matches/sec |
| Variable-length | 0.02ms | 0.41ms | 0.16ms | 7K-168K paths/sec |

**Topology Impact** (1000 vertices, 2-hop):
| Topology | Throughput |
|----------|------------|
| Chain | 15M matches/sec |
| Grid | 26M matches/sec |
| Random | 37M matches/sec |
| Star | 0 matches (no 2-hop paths) |

**Join Optimizer**:
| Tables | Overhead |
|--------|----------|
| 2 tables | 0.027ms |
| 5 tables | 0.067ms |
| 10 tables | 0.130ms |

**Graph Traversal**:
| Algorithm | Throughput |
|-----------|------------|
| BFS | 10-50M nodes/sec |
| DFS | 5-20M nodes/sec |
| PageRank | 5-15M nodes/sec |
| Connected Components | 10-50M nodes/sec |
| Triangle Counting | 1-10M edges/sec |

### Running Benchmarks

```bash
# Pattern matching benchmarks
DYLD_LIBRARY_PATH=/path/to/arrow/lib python benchmarks/pattern_matching_bench.py

# Graph storage benchmarks
DYLD_LIBRARY_PATH=/path/to/arrow/lib python benchmarks/graph_storage_bench.py
```

---

## Testing

### Test Suite

**Location**: `tests/unit/graph/`

**Coverage**: 27 tests, 100% passing

```bash
# Run pattern matching tests
pytest tests/unit/graph/test_pattern_matching.py -v

# Expected output:
# ============================= test session starts ==============================
# tests/unit/graph/test_pattern_matching.py::TestPatternMatchResult::test_empty_result PASSED
# tests/unit/graph/test_pattern_matching.py::TestPatternMatchResult::test_result_attributes PASSED
# ...
# ======================== 27 passed, 3 warnings in 4.23s ========================
```

**Test Classes**:
- `TestPatternMatchResult` - Result object API (2 tests)
- `Test2HopPatterns` - 2-hop matching (5 tests)
- `Test3HopPatterns` - 3-hop matching (4 tests)
- `TestVariableLengthPaths` - Variable-length paths (6 tests)
- `TestJoinOptimizer` - Join optimization (4 tests)
- `TestQueryPlanner` - Query planning (3 tests)
- `TestEdgeCases` - Edge cases (3 tests)

---

## Examples

### Example 1: Social Network Analysis

```python
import pyarrow as pa
from sabot._cython.graph import PropertyGraph, VertexTable, EdgeTable
from sabot._cython.graph.query import match_2hop

# Create social graph
vertices = pa.table({
    'id': pa.array(list(range(1000)), type=pa.int64()),
    'label': pa.array(['Person'] * 1000).dictionary_encode(),
    'name': [f'User{i}' for i in range(1000)]
})

# Friend relationships
edges = pa.table({
    'source': pa.array([...], type=pa.int64()),
    'target': pa.array([...], type=pa.int64()),
    'type': pa.array(['FRIEND'] * len(sources)).dictionary_encode()
})

# Find friend-of-friend recommendations
recommendations = match_2hop(edges, edges)
print(f"Found {recommendations.num_matches()} friend-of-friend connections")
```

**See**: `examples/social_network_analysis.py`

### Example 2: Fraud Detection

```python
from sabot._cython.graph.query import match_2hop, match_3hop

# Money laundering: Account → Account → Account
suspicious_2hop = match_2hop(transfers, transfers)

# More complex patterns
suspicious_3hop = match_3hop(transfers, transfers, transfers)

# Filter by amount thresholds
result_table = suspicious_2hop.result_table()
high_value = result_table.filter(
    pc.greater(result_table.column('amount'), 10000)
)
```

**See**: `examples/fraud_detection_optimizer.py`

### Example 3: Knowledge Graph Inference

```python
# Pattern: (Person)-[KNOWS]->(Person)-[WORKS_AT]->(Company)
social_edges = filter_by_type('KNOWS')
employment_edges = filter_by_type('WORKS_AT')

connections = match_2hop(social_edges, employment_edges)

# Who works at the same company as friends?
```

**See**: `examples/pattern_sabot_integration.py`

---

## Integration with Sabot Streaming

### Future: Streaming Graph Queries (Planned)

```python
import sabot as sb

# Stream of edges
edge_stream = sb.Stream.from_kafka('edges')

# Continuous pattern matching
fraud_patterns = edge_stream.graph_pattern(
    pattern='(a)-[TRANSFER]->(b)-[TRANSFER]->(c)',
    where='transfer.amount > 10000'
)

# Output alerts
fraud_patterns.to_kafka('fraud_alerts')
```

### Future: Distributed Graph Analytics (Planned)

```python
# Partition graph across agents
graph = sb.Graph.from_parquet('vertices.parquet', 'edges.parquet')
graph.partition_by_vertex_range(num_partitions=8)

# Distributed PageRank
pagerank = graph.pagerank(iterations=20, distributed=True)
```

---

## File Structure

```
sabot/_cython/graph/
├── storage/                       # Phase 1: Core storage
│   ├── graph_storage.h            # C++ API (400 lines)
│   ├── graph_storage.cpp          # C++ implementation (600 lines)
│   ├── graph_storage.pxd          # Cython declarations
│   ├── graph_storage.pyx          # Python bindings (600 lines)
│   └── __init__.py
│
├── traversal/                     # Phase 2: Algorithms
│   ├── bfs.pyx                    # Breadth-first search
│   ├── dfs.pyx                    # Depth-first search
│   ├── shortest_paths.pyx         # Dijkstra, Bellman-Ford
│   ├── pagerank.pyx               # PageRank
│   ├── centrality.pyx             # Betweenness centrality
│   ├── connected_components.pyx   # Connected components
│   ├── strongly_connected_components.pyx
│   ├── triangle_counting.pyx      # Triangle counting
│   └── __init__.py
│
├── query/                         # Phase 3: Pattern matching
│   ├── pattern_match.h            # C++ pattern matching API
│   ├── pattern_match.cpp          # C++ implementation
│   ├── pattern_match.pxd          # Cython declarations
│   ├── pattern_match.pyx          # Python bindings
│   ├── query_planner.pxd          # Query planner declarations
│   ├── query_planner.pyx          # Query optimization
│   └── __init__.py
│
└── __init__.py

tests/unit/graph/
├── test_graph_storage.py          # Storage tests (14 test classes)
└── test_pattern_matching.py       # Pattern tests (27 tests, 100% passing)

benchmarks/
├── graph_storage_bench.py         # Storage performance
└── pattern_matching_bench.py      # Pattern matching performance

examples/
├── property_graph_demo.py         # Basic property graph usage
├── social_network_analysis.py     # Friend-of-friend recommendations
├── fraud_detection_optimizer.py   # Money laundering detection
└── pattern_sabot_integration.py   # Integration with Sabot operators

docs/
└── GRAPH_QUERY_ENGINE.md          # This file
```

---

## Technical Details

### CSR Format

```
Graph: 0 → [1, 2], 1 → [2], 2 → []

CSR Representation:
  indptr:  [0, 2, 3, 3]   # Offsets for each vertex
  indices: [1, 2, 2]       # Neighbor IDs

Lookup neighbors of vertex 1:
  start = indptr[1] = 2
  end = indptr[2] = 3
  neighbors = indices[2:3] = [2]
```

**Memory**: 16 bytes/edge (two int64 values)
**Lookup**: O(1) offset + O(degree) scan

### Dictionary Encoding

```python
# Before encoding
labels = ['Person', 'Company', 'Person', 'Company']  # 4×8 = 32 bytes (avg)

# After encoding
dictionary = ['Person', 'Company']                    # 2×8 = 16 bytes
indices = [0, 1, 0, 1]                               # 4×4 = 16 bytes
# Total: 32 bytes (same), but comparisons are 2x faster (int vs string)
```

**Benefits**:
- Faster comparisons (integer equality)
- SIMD-friendly operations
- Cache-efficient

### Zero-Copy Pattern Matching

All pattern matching operations avoid copying data:

```python
# 2-hop matching uses hash join (zero-copy)
result = match_2hop(edges1, edges2)

# Result table shares buffers with input edges
# No Python object creation during matching
# Direct Arrow C++ buffer access
```

---

## Roadmap

### Phase 1: Core Storage ✅ COMPLETE
- CSR/CSC adjacency structures
- Property graph storage
- RDF triple store
- Dictionary encoding

### Phase 2: Traversal Algorithms ✅ COMPLETE
- BFS, DFS
- Shortest paths (Dijkstra, Bellman-Ford)
- PageRank
- Centrality measures
- Connected components
- Triangle counting

### Phase 3: Pattern Matching ✅ COMPLETE
- 2-hop, 3-hop patterns
- Variable-length paths
- Join optimization
- Query planning
- Integration with Sabot hash joins

### Phase 4: Query Compilers (Planned - Q1 2026)
- Cypher frontend (subset)
- SPARQL frontend (subset)
- Filter pushdown optimization
- Cost-based query optimization

### Phase 5: High-Level API (Planned - Q1 2026)
- `sabot.api.graph.Graph` class
- `sabot.api.graph.RDFGraph` class
- Streaming pattern matching
- Integration with Stream API

### Phase 6: Distributed Execution (Planned - Q2 2026)
- Graph partitioning
- Distributed traversals
- Multi-node analytics
- Arrow Flight shuffle integration

---

## References

- [Apache Arrow Documentation](https://arrow.apache.org/docs/)
- [GraphBLAS Standard](https://graphblas.org/)
- [Property Graph Model](https://github.com/opencypher/openCypher)
- [Cypher Query Language](https://neo4j.com/developer/cypher/)
- [SPARQL 1.1 Query Language](https://www.w3.org/TR/sparql11-query/)

---

**Status**: ✅ **Phases 1-3 Complete** (October 2025)
**Next**: Phase 4 - Query Compilers (Cypher/SPARQL frontends)
**Author**: Sabot Development Team
**Performance**: Production-ready (3-37M matches/sec)
