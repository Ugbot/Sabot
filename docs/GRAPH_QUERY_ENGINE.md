# Sabot Graph Query Engine

Arrow-native graph storage and query capabilities with C++ kernels and Cython bindings.

## Overview

Sabot now includes a high-performance graph processing module that combines:
- **Property graphs** (vertices/edges with properties)
- **RDF/SPARQL** (triples/quads with term dictionaries)
- **Streaming graph queries** (continuous pattern matching)
- **Batch graph queries** (traditional graph analytics)

All built on Arrow's columnar format for zero-copy operations and vectorized processing.

## Architecture

### Physical Design

**Property Graphs**:
- Vertices: Arrow Table with (id, label, properties...)
- Edges: Arrow Table with (src, dst, type, properties...)
- CSR/CSC: Arrow Int64Arrays (indptr, indices, edge_data)
- Dictionary encoding for labels, types, and strings

**RDF Triples**:
- Triples: Arrow Table with (s, p, o) as int64
- Term Dictionary: Arrow Table with (id, lex, kind, lang, datatype)
- Covering orders: (SPO, POS, OSP) sorted projections

### Zero-Copy Philosophy

All operations work directly on Arrow arrays:
- Neighbor lookups → array slices (no copying)
- Filtering → boolean masks (vectorized)
- Joins → hash joins via Arrow compute
- CSR traversals → pointer arithmetic

## Features Implemented

### Step 1: Core Storage ✅ COMPLETE

**Location**: `sabot/_cython/graph/storage/`

**Components**:

1. **CSRAdjacency** (`PyCSRAdjacency`)
   - Compressed sparse row adjacency structure
   - Zero-copy neighbor lookups: O(1) + O(degree)
   - Degree queries: O(1) pointer arithmetic
   - Vectorized batch lookups for multiple vertices

2. **VertexTable** (`PyVertexTable`)
   - Columnar vertex storage with properties
   - Filter by label (dictionary-encoded, vectorized)
   - Column projection (zero-copy select)
   - Get properties (zero-copy column access)

3. **EdgeTable** (`PyEdgeTable`)
   - Columnar edge storage with properties
   - Filter by type (dictionary-encoded, vectorized)
   - Build CSR/CSC from edges (sorted + offsets)
   - Get sources/destinations (zero-copy)

4. **PropertyGraph** (`PyPropertyGraph`)
   - Combined graph (vertices + edges + adjacency)
   - Pattern matching: (label)-[type]->(label)
   - Out-neighbors via CSR, in-neighbors via CSC
   - Lazy CSR/CSC construction

5. **RDFTripleStore** (`PyRDFTripleStore`)
   - RDF triples with term dictionary encoding
   - Filter triples by pattern (s, p, o with wildcards)
   - Term ID ↔ lexical form lookups

**Files Created**:
```
sabot/_cython/graph/
├── storage/
│   ├── graph_storage.h          # C++ API (400 lines)
│   ├── graph_storage.cpp        # C++ implementation (600 lines)
│   ├── graph_storage.pxd        # Cython declarations (100 lines)
│   ├── graph_storage.pyx        # Cython bindings (600 lines)
│   └── __init__.py
├── traversal/__init__.py
├── operators/__init__.py
├── query/__init__.py
├── __init__.py
└── README.md

tests/unit/graph/
├── test_graph_storage.py        # 14 test classes (400 lines)
└── __init__.py

benchmarks/
└── graph_storage_bench.py       # Performance benchmarks (250 lines)

examples/
└── property_graph_demo.py       # Comprehensive demo (350 lines)

docs/
└── GRAPH_QUERY_ENGINE.md        # This file
```

**Build Integration**:
- Updated `build.py` to compile C++ + Cython together
- Automatic discovery via existing module scanning
- Links against vendored Arrow C++ libraries

## Performance Targets

Based on Arrow-native graph engines:

| Operation | Target | Notes |
|-----------|--------|-------|
| CSR Build | 10-20 M edges/sec | Sorting + offset array |
| Neighbor Lookup | <1μs | O(1) pointer arithmetic |
| Degree Query | O(1) | Single subtraction |
| Pattern Matching | 1-10 M matches/sec | Vectorized filters + joins |
| Vertex Filtering | 10-100 M/sec | Arrow compute kernels |
| Edge Filtering | 10-100 M/sec | Arrow compute kernels |

## Usage Examples

### Basic Property Graph

```python
import pyarrow as pa
from sabot._cython.graph import VertexTable, EdgeTable, PropertyGraph

# Create vertex table
vertices = pa.table({
    'id': pa.array([0, 1, 2, 3], type=pa.int64()),
    'label': pa.array(['Person', 'Person', 'Company', 'Company']).dictionary_encode(),
    'name': ['Alice', 'Bob', 'Acme', 'Globex']
})

# Create edge table
edges = pa.table({
    'src': pa.array([0, 1, 0], type=pa.int64()),
    'dst': pa.array([2, 2, 3], type=pa.int64()),
    'type': pa.array(['WORKS_AT', 'WORKS_AT', 'OWNS']).dictionary_encode(),
    'since': [2018, 2019, 2020]
})

# Create property graph
vt = VertexTable(vertices)
et = EdgeTable(edges)
graph = PropertyGraph(vt, et)

# Build CSR for fast traversals
graph.build_csr()

# Get neighbors
neighbors = graph.get_neighbors(0)  # Alice's neighbors
print(f"Alice's neighbors: {neighbors.to_pylist()}")

# Pattern matching: (Person)-[WORKS_AT]->(Company)
result = graph.match_pattern('Person', 'WORKS_AT', 'Company')
print(f"Matches: {result.num_rows}")
```

### Filtering and Projection

```python
# Filter vertices by label
persons = vt.filter_by_label('Person')
print(f"Found {persons.num_vertices()} persons")

# Filter edges by type
works_at = et.filter_by_type('WORKS_AT')
print(f"Found {works_at.num_edges()} WORKS_AT edges")

# Project columns (zero-copy)
projected = vt.select(['id', 'name'])
print(f"Schema: {projected.schema()}")
```

### CSR Adjacency

```python
# Build CSR from edge table
csr = et.build_csr(num_vertices=4)

# Get neighbors for vertex 0
neighbors = csr.get_neighbors(0)
print(f"Neighbors: {neighbors.to_pylist()}")

# Get degree
degree = csr.get_degree(0)
print(f"Degree: {degree}")

# Vectorized batch lookup
vertices = pa.array([0, 1, 2], type=pa.int64())
batch = csr.get_neighbors_batch(vertices)
print(f"Found {batch.num_rows} neighbor pairs")
```

### RDF Triple Store

```python
from sabot._cython.graph import RDFTripleStore

# Create triples table
triples = pa.table({
    's': pa.array([1, 1, 2], type=pa.int64()),
    'p': pa.array([10, 11, 10], type=pa.int64()),
    'o': pa.array([20, 30, 40], type=pa.int64()),
})

# Create term dictionary
term_dict = pa.table({
    'id': pa.array([1, 2, 10, 11, 20, 30, 40], type=pa.int64()),
    'lex': ['http://alice', 'http://bob', 'http://knows',
            'http://age', 'http://charlie', '25', '30'],
    'kind': pa.array([0, 0, 0, 0, 0, 1, 1], type=pa.uint8()),
})

# Create triple store
store = RDFTripleStore(triples, term_dict)

# Lookup term ID
alice_id = store.lookup_term_id('http://alice')
print(f"Alice's ID: {alice_id}")

# Filter triples: all with subject alice_id
filtered = store.filter_triples(s=alice_id, p=-1, o=-1)
print(f"Alice's triples: {filtered.num_rows}")
```

## Building

Build the graph storage module:

```bash
# Build everything
python build.py

# The graph storage module will be automatically discovered
# and compiled as a 'core' module (Arrow-dependent)
```

## Testing

Run tests:

```bash
# Run graph storage tests
pytest tests/unit/graph/test_graph_storage.py -v

# Run all tests
pytest tests/ -v
```

## Benchmarking

Run performance benchmarks:

```bash
# Run graph storage benchmarks
python benchmarks/graph_storage_bench.py

# Expected output:
# - CSR Build: 10-20 M edges/sec
# - Neighbor Lookup: <1μs per vertex
# - Pattern Matching: 1-10 M matches/sec
```

## Demo

Run the comprehensive demo:

```bash
python examples/property_graph_demo.py

# Demonstrates:
# - Graph creation
# - CSR/CSC construction
# - Neighbor traversals
# - Vertex/edge filtering
# - Pattern matching
# - Performance measurements
```

## Roadmap

### Step 2: Traversal Kernels (Week 2)
- BFS/DFS over CSR buffers
- K-hop neighbors
- Shortest paths
- PageRank, triangle counting
- Connected components

### Step 3: Pattern Matching (Week 2)
- Multi-hop patterns (2-hop, 3-hop, variable-length)
- Property predicates in patterns
- Pattern DAG optimization
- Integration with existing join operators

### Step 4: Query Compilers (Week 3)
- Cypher frontend (subset)
- SPARQL frontend (subset)
- Query optimization (filter pushdown, join reordering)
- Cost-based optimization with statistics

### Step 5: High-Level API (Week 3)
- `sabot.api.graph.Graph` class
- `sabot.api.graph.RDFGraph` class
- Extend `sabot.api.stream.Stream` with graph operators
- Streaming pattern matching

### Step 6: Distributed Execution (Week 4)
- Partition graphs by vertex ID range
- Use existing Flight shuffle for cross-partition joins
- Distributed BFS/PageRank
- Multi-node graph analytics

## Integration with Sabot

### Streaming Graph Queries

Once Step 5 is complete, you'll be able to do:

```python
import sabot as sb

# Stream of edges
edge_stream = sb.Stream.from_kafka('edges')

# Continuous pattern matching
fraud_patterns = edge_stream.match_pattern(
    "(account:Account)-[:TRANSFER]->(suspicious:Account)",
    where="transfer.amount > 10000 AND suspicious.risk_score > 0.8"
)

# Output alerts
fraud_patterns.to_kafka('fraud_alerts')
```

### Batch Graph Analytics

```python
# Load graph from Parquet
vertices = sb.Stream.from_parquet('vertices.parquet')
edges = sb.Stream.from_parquet('edges.parquet')

# Create graph
graph = sb.Graph.from_streams(vertices, edges)

# Run PageRank
pagerank = graph.pagerank(iterations=10)

# Find top-ranked vertices
top_ranked = pagerank.sort_by('rank', descending=True).limit(10)
```

## Technical Details

### CSR Format

CSR (Compressed Sparse Row) stores graph adjacency efficiently:

```
Graph: 0 → [1, 2], 1 → [2], 2 → []

CSR:
  indptr:  [0, 2, 3, 3]  (offsets for each vertex)
  indices: [1, 2, 2]      (neighbor IDs)
```

**Memory**: ~16 bytes per edge (two int64 values)
**Lookup**: O(1) to find neighbors (indptr[v] to indptr[v+1])

### Dictionary Encoding

Strings (labels, types, IRIs) are dictionary-encoded to int32:

```
Labels: ['Person', 'Company', 'Person', 'Company']
→ dictionary: ['Person', 'Company']
→ indices: [0, 1, 0, 1]
```

**Benefits**:
- 4x smaller memory (int32 vs string)
- Faster comparisons (integer equality)
- SIMD-friendly operations

### Zero-Copy Operations

All operations avoid copying data:

```python
# Get neighbors (slice, not copy)
neighbors = csr.get_neighbors(0)  # Returns view into indices array

# Filter vertices (mask, not copy)
persons = vt.filter_by_label('Person')  # Returns filtered view

# Select columns (projection, not copy)
projected = vt.select(['id', 'name'])  # Returns column views
```

## References

- [Apache Arrow Documentation](https://arrow.apache.org/docs/)
- [Arrow Acero (Streaming Execution)](https://arrow.apache.org/docs/cpp/streaming_execution.html)
- [GraphBLAS Standard](https://graphblas.org/)
- [Property Graph Model](https://github.com/opencypher/openCypher)
- [RDF 1.1 Concepts](https://www.w3.org/TR/rdf11-concepts/)
- [SPARQL 1.1 Query Language](https://www.w3.org/TR/sparql11-query/)

---

**Status**: Step 1 Complete ✅
**Next**: Compile and test, then proceed to Step 2 (Traversal Kernels)
**Author**: Sabot Development Team
**Date**: October 2025
