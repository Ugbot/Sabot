# SabotGraph

**Graph query execution (Cypher + SPARQL) for Sabot streaming pipelines.**

**STATUS: NOT YET IMPLEMENTED - Requires C++ bridge wiring**

---

## Overview

SabotGraph is designed to bring graph queries to Sabot as native stream operators. The architecture and API have been designed, but the C++ bridge implementation is not yet complete.

**Current State:**
- ⏳ C++ bridge structure exists but not wired to Python
- ⏳ Python API defined but raises NotImplementedError
- ✅ Pattern matching kernels exist in sabot_cypher
- ✅ SPARQL parser exists in sabot_ql
- ⏳ MarbleDB integration not yet implemented

---

## Architecture (Design)

**Pattern: Mirrors sabot_sql**

```
sabot_sql:
  SQL → DuckDB Parser → Logical Plan → Sabot Morsel Operators
  State: Tonbo/MarbleDB tables, RocksDB timers

sabot_graph (PLANNED):
  Cypher/SPARQL → Parser → Logical Plan → Sabot Morsel Operators
  State: MarbleDB vertices/edges/indexes
```

---

## Implementation Status

### What Exists

- ✅ **Parsers**: Cypher parser (Lark) and SPARQL parser (C++)
- ✅ **Pattern matching**: Cython kernels for graph pattern matching
- ✅ **API structure**: Python classes and method signatures defined
- ✅ **C++ structure**: SabotGraphBridge C++ class exists

### What's Missing (NOT IMPLEMENTED)

- ❌ **C++ bridge initialization**: Bridge not wired to Python
- ❌ **execute_cypher()**: Not implemented, raises NotImplementedError
- ❌ **execute_sparql()**: Not implemented, raises NotImplementedError
- ❌ **MarbleDB integration**: All storage operations are stubs
- ❌ **Batch execution**: execute_*_on_batch() not implemented
- ❌ **Kafka integration**: Streaming not implemented
- ❌ **Distributed execution**: Not implemented

---

## Files

### Python
- `sabot_graph_python.py` - Python bridge (NOT IMPLEMENTED, raises NotImplementedError)
- `sabot_graph_streaming.py` - Streaming integration (NOT IMPLEMENTED)

### C++
- `src/graph/sabot_graph_bridge.cpp` - C++ bridge (stubs, needs implementation)
- `src/state/marble_graph_backend.cpp` - MarbleDB integration (stubs)

---

## Next Steps

To make this functional:

1. **Build sabot_cypher native module**
   ```bash
   cd sabot_cypher
   cmake -S . -B build
   cmake --build build
   ```

2. **Implement C++ bridge initialization**
   - Wire SabotGraphBridge C++ class to Python
   - Initialize MarbleDB backend
   - Connect to sabot_cypher execution engine

3. **Implement execution methods**
   - ExecuteCypher() - call sabot_cypher engine
   - ExecuteSPARQL() - call sabot_ql engine
   - Batch execution methods

4. **Implement MarbleDB storage**
   - Replace TODO stubs in marble_graph_backend.cpp
   - Implement vertex/edge storage and retrieval
   - Implement indexing for fast lookups

---

## Related Components

- **sabot_cypher**: Cypher query engine (requires build)
- **sabot_ql**: SPARQL query engine (partial implementation)
- **MarbleDB**: State backend (not yet integrated)

---

## Contact

For implementation questions, see:
- GRAPH_QUERY_AUDIT_REPORT.md - Complete audit of what's real vs fake
- sabot_cypher/README.md - Cypher engine status
- sabot_ql/README.md - SPARQL engine status
