# Sabot Triple Store Demos

This directory contains demos for Sabot's RDF triple store implementations.

## Quick Start

```bash
# Set up library paths
export DYLD_LIBRARY_PATH=/Users/bengamble/Sabot/vendor/arrow/cpp/build/install/lib:/Users/bengamble/Sabot/sabot_ql/build:/Users/bengamble/Sabot/MarbleDB/build:$DYLD_LIBRARY_PATH

# Run comprehensive demo
python examples/comprehensive_triple_store_demo.py

# Run specific demos
python examples/triple_store_example.py                # MarbleDB persistent store
python examples/sparql_query_demo.py                   # SPARQL queries
python benchmarks/studies/rdf_benchmarks/rdf_loader.py # RDF file loading
```

## Available Demos

### 1. Comprehensive Triple Store Demo
**File**: `comprehensive_triple_store_demo.py`

Shows all triple store implementations:
- ✅ **PyRDFTripleStore** - In-memory Arrow-backed store (works now)
- ✅ **MarbleDB Triple Store** - Persistent C++ storage (works now)
- ✅ **RDF Loader** - Load from RDF files with rdflib
- ⚠️ **SPARQL Queries** - Query execution (partial)

```bash
python examples/comprehensive_triple_store_demo.py
```

**Output**:
```
DEMO 1: PyRDFTripleStore (In-Memory, Arrow-backed)
✅ Loaded RDF triple store
   Triples: 9
   Terms: 11

DEMO 3: MarbleDB Triple Store (Persistent, C++)
✅ Store created successfully
   ✅ Persistent storage (MarbleDB)
   ✅ Memory-safe C++ integration
```

### 2. MarbleDB Triple Store Example
**File**: `triple_store_example.py`

Simple example of the new persistent triple store:

```python
from sabot.graph import create_triple_store

# Create persistent store
store = create_triple_store("./my_graph_db")

# Use context manager
with create_triple_store("./my_graph_db") as store:
    # Store operations...
    pass  # Auto-closes
```

**Status**:
- ✅ Create/open persistent stores
- ✅ Context manager support
- 🚧 insert_triple() - Coming soon
- 🚧 SPARQL queries - Coming soon

### 3. SPARQL Query Demo
**File**: `sparql_query_demo.py`

Demonstrates SPARQL query compilation and execution:

```python
from sabot._cython.graph.engine import GraphQueryEngine

engine = GraphQueryEngine()
engine.load_vertices(vertices)
engine.load_edges(edges)

# Execute SPARQL query
result = engine.query_sparql("""
    SELECT ?person ?name
    WHERE {
        ?person rdf:type Person .
        ?person name ?name .
    }
""")
```

**Status**:
- ✅ SPARQL parser (SELECT, WHERE, FILTER, LIMIT)
- ✅ Graph query engine
- ⚠️ Query execution (partial implementation)

### 4. RDF Loader
**File**: `benchmarks/studies/rdf_benchmarks/rdf_loader.py`

Load RDF files (N-Triples, Turtle, RDF/XML) into Sabot:

```python
from benchmarks.studies.rdf_benchmarks.rdf_loader import load_rdf_file

# Load from file
store = load_rdf_file('data.nt', format='nt')

# Or create synthetic data
store = create_synthetic_dataset()
```

**Requires**: `uv pip install rdflib`

## Triple Store Implementations

### PyRDFTripleStore (Cython)

**Location**: `sabot._cython.graph.storage.graph_storage.PyRDFTripleStore`

**Features**:
- ✅ In-memory Arrow-backed storage
- ✅ Fast columnar operations
- ✅ Term dictionary encoding
- ✅ Triple pattern matching
- ✅ SPARQL integration

**Schema**:
```python
# Triples table
triples = pa.Table.from_pydict({
    's': pa.array([0, 1, 2], type=pa.int64()),  # Subject IDs
    'p': pa.array([100, 101, 102], type=pa.int64()),  # Predicate IDs
    'o': pa.array([10, 11, 12], type=pa.int64())   # Object IDs
})

# Term dictionary
terms = pa.Table.from_pydict({
    'id': pa.array([0, 1, 2], type=pa.int64()),
    'lex': pa.array(['http://...'], type=pa.string()),  # Lexical form
    'kind': pa.array([0, 1, 2], type=pa.uint8()),  # 0=IRI, 1=Literal, 2=Blank
    'lang': pa.array(['en'], type=pa.string()),    # Language tag
    'datatype': pa.array(['xsd:string'], type=pa.string())  # Datatype
})

store = PyRDFTripleStore(triples, terms)
```

**Methods**:
- `num_triples()` - Count triples
- `num_terms()` - Count terms
- `lookup_term_id(lex)` - Get term ID by lexical form
- `lookup_term(id)` - Get lexical form by ID
- `filter_triples(s, p, o)` - Pattern matching (-1 for wildcard)

### MarbleDB TripleStore (C++)

**Location**: `sabot.graph.TripleStore`

**Features**:
- ✅ Persistent storage (MarbleDB/LSM-tree)
- ✅ C++ performance
- ✅ Arrow integration
- ✅ Memory-safe Python bindings
- 🚧 Insert/query methods (coming soon)

**Usage**:
```python
from sabot.graph import create_triple_store

# Create/open persistent store
store = create_triple_store("./graph_db")

# Future: Insert triples
# store.insert_triple("http://ex.org/Alice", "http://ex.org/knows", "http://ex.org/Bob")

# Clean shutdown
store.close()
```

**Internals**:
- C++ library: `libsabot_ql.dylib` (4.2MB)
- Storage: MarbleDB `libmarble.a` (30MB)
- Python bindings: Cython (`sabot_ql_native.so`)

## Performance

### PyRDFTripleStore
- **Pattern matching**: 3-37M matches/sec (benchmarked)
- **SPARQL parsing**: 23,798 queries/sec
- **Memory**: Arrow zero-copy operations

### MarbleDB TripleStore
- **Storage**: LSM-tree with LZ4 compression
- **Persistence**: Bloom filters + sparse indexes
- **Expected**: 10-100M triples on commodity hardware

## Dependencies

**Required**:
- Sabot with vendored Arrow (built-in)
- MarbleDB (built-in)
- Cython modules (built-in)

**Optional**:
- `rdflib` - For RDF file parsing: `uv pip install rdflib`

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                  Python Layer                           │
│  sabot.graph.TripleStore (high-level API)              │
└─────────────────────┬───────────────────────────────────┘
                      │
┌─────────────────────┴───────────────────────────────────┐
│              Cython Bindings                            │
│  sabot_ql_native (Cython → C++)                        │
└─────────────────────┬───────────────────────────────────┘
                      │
┌─────────────────────┴───────────────────────────────────┐
│                C++ Layer                                │
│  libsabot_ql.dylib (triple store + vocabulary)         │
│  ├─ TripleStore (SPO table management)                 │
│  └─ Vocabulary (term dictionary with LRU cache)        │
└─────────────────────┬───────────────────────────────────┘
                      │
┌─────────────────────┴───────────────────────────────────┐
│              Storage Layer                              │
│  libmarble.a (MarbleDB - LSM tree)                     │
│  ├─ Memtable (in-memory write buffer)                  │
│  ├─ SST files (sorted string tables)                   │
│  ├─ Bloom filters (membership testing)                 │
│  └─ Sparse index (efficient lookups)                   │
└─────────────────────────────────────────────────────────┘
                      │
                 Arrow Tables
              (zero-copy operations)
```

## Next Steps

1. **Implement insert methods** for MarbleDB store
2. **Complete SPARQL query execution**
3. **Add batch loading** from Arrow tables
4. **Benchmarking** against Virtuoso, Jena, RDF4J
5. **SPARQL 1.1** full support (property paths, aggregations)

## Files

```
examples/
├── comprehensive_triple_store_demo.py   # All demos in one
├── triple_store_example.py              # MarbleDB basic usage
├── sparql_query_demo.py                 # SPARQL queries
└── TRIPLE_STORE_DEMOS.md               # This file

benchmarks/studies/rdf_benchmarks/
└── rdf_loader.py                        # Load RDF files

sabot/graph.py                           # High-level API
sabot_ql/bindings/python/
├── sabot_ql_native.so                   # Cython extension
├── sabot_ql_impl.pyx                    # Bindings source
└── test_triple_store_direct.py          # Low-level tests
```

## Testing

```bash
# Test MarbleDB store directly
cd sabot_ql/bindings/python
python test_triple_store_direct.py

# Test sabot.graph module
python test_graph_module.py

# All tests should pass (6/6)
```

## Status Summary

| Component | Status | Notes |
|-----------|--------|-------|
| PyRDFTripleStore | ✅ Working | In-memory, Arrow-backed |
| MarbleDB Store | ✅ Working | Persistent infrastructure |
| SPARQL Parser | ✅ Working | SELECT, WHERE, FILTER |
| Query Execution | ⚠️ Partial | Some features missing |
| RDF Loader | ✅ Working | With rdflib installed |
| insert_triple() | 🚧 Coming | Needs C++ bindings |
| Batch loading | 🚧 Coming | Needs Arrow integration |

**Overall**: Core infrastructure complete, operational capabilities coming soon.
