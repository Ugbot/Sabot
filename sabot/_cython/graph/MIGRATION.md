# Migration from sabot_cypher/sabot_graph to sabot._cython.graph

**Date:** October 22, 2025
**Reason:** Consolidation to single working implementation

---

## Summary

As of October 22, 2025, `sabot_cypher/` and `sabot_graph/` have been **archived** (not deleted). These directories contained attempts to vendor Kuzu and create C++ bridges, but were never completed or used.

The **real, working implementation** has always been in `sabot/_cython/graph/`, which is now the only supported graph query engine.

---

## What Changed

### Archived Directories

```bash
# OLD locations (now archived)
sabot_cypher/          → archive/graph_implementations/abandoned_kuzu_fork/
sabot_graph/           → archive/graph_implementations/abandoned_cpp_bridge/
```

### Active Implementation

```bash
# NEW location (always was the real one)
sabot/_cython/graph/   → The working implementation
```

---

## Code Migration

### Import Changes

**OLD (doesn't work, now archived):**
```python
from sabot_cypher import SabotCypherBridge
from sabot_cypher import create_cypher_engine
from sabot_graph import create_sabot_graph_bridge
from sabot_graph import SabotGraphOrchestrator
```

**NEW (works):**
```python
from sabot._cython.graph.engine.query_engine import GraphQueryEngine
```

---

## Usage Examples

### Basic Query Execution

**OLD:**
```python
# This never worked
from sabot_cypher import create_cypher_engine
engine = create_cypher_engine()
result = engine.execute(query)
```

**NEW:**
```python
# This works
from sabot._cython.graph.engine.query_engine import GraphQueryEngine
from sabot import cyarrow as pa

engine = GraphQueryEngine()

# Load graph data
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

# Execute query
result = engine.query_cypher('MATCH (a)-[:OWNS]->(b) RETURN a.name, b')
print(result.to_pandas())
```

---

### Graph Bridge

**OLD:**
```python
# This never worked
from sabot_graph import create_sabot_graph_bridge
bridge = create_sabot_graph_bridge()
result = bridge.execute_cypher(query)
```

**NEW:**
```python
# Use GraphQueryEngine directly
from sabot._cython.graph.engine.query_engine import GraphQueryEngine
engine = GraphQueryEngine()
result = engine.query_cypher(query)
```

---

### Streaming Operators

**OLD:**
```python
# This was planned but never implemented
from sabot_graph import create_streaming_graph_executor
executor = create_streaming_graph_executor()
executor.register_continuous_query(query)
```

**NEW:**
```python
# Streaming operators not yet implemented
# Use batch queries for now
from sabot._cython.graph.engine.query_engine import GraphQueryEngine
engine = GraphQueryEngine()
result = engine.query_cypher(query)
```

Note: Graph streaming operators (CypherOperator, SPARQLOperator) exist in `sabot/operators/graph_query.py` but raise `NotImplementedError`. Use GraphQueryEngine directly for now.

---

### Graph API Facade

**OLD:**
```python
# This tried to import sabot_cypher
from sabot.api.graph_facade import GraphAPI
engine = Sabot()
result = engine.graph.cypher(query)
```

**NEW (updated):**
```python
# Now uses GraphQueryEngine internally
from sabot.api.graph_facade import GraphAPI
engine = Sabot()
result = engine.graph.cypher(query)  # Works now!
```

The GraphAPI facade has been updated to use `GraphQueryEngine` internally.

---

## Feature Comparison

### What Works in sabot._cython.graph ✅

```python
from sabot._cython.graph.engine.query_engine import GraphQueryEngine

engine = GraphQueryEngine()

# Supported features:
✅ Basic edge patterns: (a)-[r]->(b)
✅ Edge type filtering: -[:KNOWS]->
✅ Node label filtering: (a:Person)
✅ Property access: RETURN a.name, b.age
✅ 2-hop patterns: (a)-[r1]->(b)-[r2]->(c)
✅ LIMIT clause
✅ Variable naming
✅ Empty graph handling
✅ In-memory storage
✅ State persistence (RocksDB, Tonbo)
```

**Test Coverage:** 19 comprehensive tests, 100% passing
**Performance:** 23,798 q/s parsing, 3-37M matches/sec

### What Didn't Work in sabot_cypher/sabot_graph ❌

```python
# Nothing actually worked
❌ Never built native modules
❌ All methods raised NotImplementedError or returned fake data
❌ No real query execution
❌ No storage integration
❌ Extensive documentation claiming completion but code didn't work
```

---

## Why the Change

### sabot_cypher (Abandoned Kuzu Fork)

**Goal:** Fork Kuzu database like sabot_sql forked DuckDB

**What happened:**
- Vendored complete Kuzu source code (~50MB)
- Created CMake build infrastructure
- Wrote extensive documentation claiming it was complete
- **Never actually built the native module**
- **Never actually used in any tests**
- All "tests" were theoretical or used fake data

**Why abandoned:**
- Pure Python/Cython implementation (`sabot._cython.graph`) already existed and worked
- Vendored Kuzu was unused dead weight
- Maintaining a fork is expensive
- Current implementation meets all requirements

### sabot_graph (Abandoned C++ Bridge)

**Goal:** Create C++ bridge to wrap Cypher/SPARQL execution

**What happened:**
- Created 3 C++ files with stub implementations
- Python wrappers raised NotImplementedError
- Built successfully but does nothing
- Was supposed to call sabot_cypher or sabot_ql (neither worked)

**Why abandoned:**
- Bridge to nowhere (nothing to bridge to)
- Python implementation works fine
- No performance benefit to C++ wrapper for this use case
- GraphQueryEngine already provides the needed API

---

## Testing

### OLD Tests (archived)

```bash
# These tests used fake data or didn't run
archive/graph_implementations/abandoned_kuzu_fork/benchmarks/
archive/graph_implementations/abandoned_cpp_bridge/examples/
```

### NEW Tests (working)

```bash
# These tests actually execute queries
tests/integration/graph/test_cypher_queries.py        # 6 tests
tests/integration/graph/test_property_access.py       # 6 tests
tests/integration/graph/test_2hop_patterns.py         # 7 tests

# Run all tests
.venv/bin/python tests/integration/graph/run_all_cypher_tests.py
# Result: 19/19 passing
```

---

## Performance

### Real Performance (sabot._cython.graph)

```
Parse: 23,798 queries/sec (Lark parser)
Execute: ~500 queries/sec (small graphs)
Pattern matching: 3-37M matches/sec (C++ kernels)
```

### Claimed Performance (sabot_cypher)

```
❌ No real benchmarks (theoretical only)
❌ Comparisons to Kuzu but using fake data
❌ No actual execution to measure
```

---

## Questions

### "Why was sabot_cypher created if it doesn't work?"

Someone started it with good intentions (mirroring sabot_sql's DuckDB fork), created extensive documentation, but never finished the implementation. The docs made it look complete, but the code wasn't built.

### "Why keep the archive instead of deleting?"

To preserve history and allow reference if needed. The vendored Kuzu code might be useful someday, and the documentation shows the original vision.

### "Will sabot_cypher/sabot_graph be revived?"

No current plans. The Python implementation works, is tested, and meets all requirements. Effort is better spent improving what works.

### "What about SPARQL?"

SPARQL support is partial. The parser exists but execution is not fully implemented. Focus is on Cypher for now.

### "What if I need features from Kuzu?"

The vendored Kuzu is available in the archive. Building it would require significant integration work. Consider whether current implementation can be enhanced instead.

---

## Help

### If you encounter old code:

1. **Check imports:** If it imports from `sabot_cypher` or `sabot_graph`, update to `sabot._cython.graph`
2. **Check tests:** Run tests to verify changes work
3. **Check docs:** Update any documentation referencing old paths

### If you need graph queries:

```python
from sabot._cython.graph.engine.query_engine import GraphQueryEngine
from sabot import cyarrow as pa

# See tests/ for examples
```

### If tests fail:

```bash
# Verify working implementation
.venv/bin/python tests/integration/graph/run_all_cypher_tests.py

# Check for old imports
grep -r "sabot_cypher\|sabot_graph" .
```

---

## Summary

- ✅ **Use:** `sabot._cython.graph.engine.query_engine.GraphQueryEngine`
- ❌ **Don't use:** `sabot_cypher` or `sabot_graph` (archived)
- ✅ **Tests:** 19/19 passing in tests/integration/graph/
- ✅ **Docs:** See CYPHER_WORKING.md and CYPHER_FEATURES_COMPLETE.md
- ✅ **Examples:** tests/integration/graph/*.py

The working implementation is simpler, tested, and ready to use!
