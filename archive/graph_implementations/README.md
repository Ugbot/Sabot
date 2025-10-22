# Archived Graph Implementation Attempts

**Date Archived:** October 22, 2025
**Reason:** Redundant implementations superseded by working code in `sabot/_cython/graph/`

---

## Summary

This directory contains two abandoned attempts to implement graph query execution in Sabot:
1. **abandoned_kuzu_fork/** - Attempted vendor of Kuzu database (never completed)
2. **abandoned_cpp_bridge/** - C++ bridge wrapper (not implemented)

Both were **never actually used** in the working codebase. The real, working implementation is in:
```
sabot/_cython/graph/
```

All tests import from `sabot._cython.graph` and work correctly (19/19 tests passing as of archival).

---

## abandoned_kuzu_fork/ (sabot_cypher)

### What It Was

An attempt to vendor Kuzu database (similar to how sabot_sql vendors DuckDB):
- Full Kuzu source code in `vendored/sabot_cypher_core/`
- CMake build infrastructure
- Cython wrappers for Python integration
- Documentation claiming it was complete

### Why It Was Abandoned

1. **Never built** - CMake configuration exists but module was never compiled
2. **Not used** - Only 29 references in codebase, mostly old docs and theoretical benchmarks
3. **Redundant** - Pure Python/Cython implementation already existed and worked
4. **Massive** - Vendored Kuzu added ~50MB+ of unused C++ code
5. **Confusing** - Documentation claimed it was working when it wasn't

### What Was Real

- ✅ README and documentation (but misleading)
- ✅ CMakeLists.txt and build files
- ✅ Vendored Kuzu source code (complete but unused)
- ❌ Built native module (never compiled)
- ❌ Working Cython bindings
- ❌ Actual usage in tests or examples

### Size

- ~50MB of vendored Kuzu C++ source
- ~500+ files in vendored/sabot_cypher_core/
- Extensive documentation claiming completion

---

## abandoned_cpp_bridge/ (sabot_graph)

### What It Was

A minimal C++ bridge to wrap graph query execution:
- 3 C++ source files (sabot_graph_bridge.cpp, marble_graph_backend.cpp, graph_plan_translator.cpp)
- Python wrappers that raised NotImplementedError
- CMake build (did build successfully but doesn't do anything)

### Why It Was Abandoned

1. **Bridge to nowhere** - Was supposed to call sabot_cypher or sabot_ql engines
2. **Not implemented** - All methods raise NotImplementedError
3. **Only 9 references** - Barely mentioned in codebase
4. **Redundant** - GraphQueryEngine in `sabot/_cython/graph/` already works

### What Was Real

- ✅ C++ bridge structure (compiled but empty stubs)
- ✅ Python wrapper files
- ❌ Actual implementation (all TODOs and NotImplementedError)
- ❌ Connection to any real query engine
- ❌ Usage in tests or production code

### Size

- ~3 C++ files, ~500 lines total
- Build artifacts in build/
- Minimal Python wrappers

---

## The Real Implementation

**Location:** `sabot/_cython/graph/`

**What actually works:**
```python
from sabot._cython.graph.engine.query_engine import GraphQueryEngine
from sabot import cyarrow as pa

# Create engine
engine = GraphQueryEngine()

# Load graph
vertices = pa.table({
    'id': [0, 1, 2],
    'label': ['Person', 'Person', 'Account'],
    'name': ['Alice', 'Bob', 'Checking']
})
engine.load_vertices(vertices)

edges = pa.table({
    'source': [0, 1],
    'target': [2, 2],
    'label': ['OWNS', 'OWNS']
})
engine.load_edges(edges)

# Execute Cypher query
result = engine.query_cypher('MATCH (a:Person)-[:OWNS]->(b) RETURN a.name, b')
print(result.to_pandas())
```

**Test Coverage:** 19 comprehensive tests (100% passing)
- Basic edge patterns
- Node label filtering
- Property access
- 2-hop patterns
- Edge type filtering
- LIMIT clauses
- Empty graph handling

**Performance:**
- Parse: 23,798 queries/sec
- Execute: ~500 queries/sec (small graphs)
- Pattern matching: 3-37M matches/sec (large graphs)

**Components:**
```
sabot/_cython/graph/
├── compiler/
│   ├── cypher_parser.py       # Lark-based Cypher parser
│   ├── cypher_translator.py   # AST to execution plan
│   └── sparql_parser.py        # SPARQL parser
├── engine/
│   ├── query_engine.py         # GraphQueryEngine (main API)
│   └── state_manager.py        # State persistence
├── query/
│   └── pattern_match.pyx       # C++ pattern matching kernels
├── storage/
│   └── graph_storage.pyx       # PyPropertyGraph, Arrow storage
└── traversal/                  # BFS, PageRank, shortest paths, etc.
```

---

## Timeline

### Before Archival
- `sabot_cypher/` at root level
- `sabot_graph/` at root level
- Confusion about which implementation to use
- Documentation claiming both were working
- Tests importing from `sabot._cython.graph` (the real one)

### Why We Archived
1. Discovered 3 different "graph implementations"
2. Only `sabot/_cython/graph/` actually works
3. Tests prove it works (19/19 passing)
4. The other two were never used
5. Keeping them created confusion

### After Archival
- Clear single source of truth: `sabot/_cython/graph/`
- Removed ~50MB of unused Kuzu code from active tree
- Updated documentation to point to working implementation
- Preserved history in archive for reference

---

## Migration Guide

If you have old code referencing the archived implementations:

### OLD (archived, doesn't work):
```python
# DON'T USE - archived
from sabot_cypher import SabotCypherBridge
from sabot_graph import create_sabot_graph_bridge
```

### NEW (works):
```python
# USE THIS
from sabot._cython.graph.engine.query_engine import GraphQueryEngine
```

See `sabot/_cython/graph/MIGRATION.md` for complete migration guide.

---

## For Developers

**If you want to use vendored Kuzu:**
1. The complete Kuzu source is in `abandoned_kuzu_fork/vendored/sabot_cypher_core/`
2. Building it would require ~10-20 hours of integration work
3. Current Python implementation already works and is tested
4. Recommendation: Don't. Use what works.

**If you want to understand the history:**
- Read `abandoned_kuzu_fork/README.md` for the original vision
- Compare with actual working implementation in `sabot/_cython/graph/`
- Note that documentation claimed completion but code wasn't built

---

## References

**Working Implementation:**
- `sabot/_cython/graph/` - The real implementation
- `tests/integration/graph/` - Comprehensive test suite
- `CYPHER_WORKING.md` - Feature documentation
- `CYPHER_FEATURES_COMPLETE.md` - Implementation report

**Archived Documentation:**
- `abandoned_kuzu_fork/README.md` - Original sabot_cypher vision
- `abandoned_cpp_bridge/README.md` - Original sabot_graph vision

**Migration:**
- `sabot/_cython/graph/MIGRATION.md` - How to update old code

---

**Questions?** Check `sabot/_cython/graph/README.md` or run the tests:
```bash
.venv/bin/python tests/integration/graph/run_all_cypher_tests.py
```
