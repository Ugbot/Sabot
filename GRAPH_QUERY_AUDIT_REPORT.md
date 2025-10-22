# Graph Query Code Audit Report

**Date:** October 22, 2025
**Status:** Complete
**Severity:** HIGH - Multiple fake code paths found

## Executive Summary

Audit of graph query functionality in Sabot reveals **significant fake/demo code** that returns hardcoded values instead of executing actual queries. This affects both the Cypher (sabot_cypher/sabot_graph) and SPARQL (sabot_ql) query engines.

**Key Findings:**
- ❌ **SabotGraph Bridge**: Returns demo/hardcoded data
- ❌ **Graph operators**: Pass-through implementations
- ⚠️ **SPARQL execution**: Not implemented (TODO stubs)
- ⚠️ **Cypher execution**: Parser exists but engine not built
- ✅ **Test infrastructure**: Real, but tests use fake backends

---

## Critical Fake Code Paths

### 1. sabot_graph/sabot_graph_python.py

**File:** `sabot_graph/sabot_graph_python.py`

**Location:** Lines 65-103

**Issue:** `execute_cypher()` and `execute_sparql()` return hardcoded demo data

```python
def execute_cypher(self, query):
    # TODO: Call C++ bridge.ExecuteCypher()

    print(f"Executing Cypher: {query[:60]}...")

    # Return demo result
    return ca.table({
        'id': ca.array([1, 2, 3], type=ca.int64()),
        'name': ca.array(['Alice', 'Bob', 'Charlie'])
    })

def execute_sparql(self, query):
    # TODO: Call C++ bridge.ExecuteSPARQL()

    print(f"Executing SPARQL: {query[:60]}...")

    # Return demo result
    return ca.table({
        'subject': ca.array([1, 2, 3], type=ca.int64()),
        'object': ca.array([4, 5, 6], type=ca.int64())
    })
```

**Impact:** Any code using `SabotGraphBridge` gets fake results.

---

### 2. sabot_graph/sabot_graph_python.py - Batch Execution

**File:** `sabot_graph/sabot_graph_python.py`

**Location:** Lines 105-138

**Issue:** Batch execution methods just pass through input without processing

```python
def execute_cypher_on_batch(self, query, batch):
    # TODO: Call C++ bridge.ExecuteCypherOnBatch()

    print(f"Executing Cypher on batch of {batch.num_rows} rows")

    return batch  # Pass through for now

def execute_sparql_on_batch(self, query, batch):
    # TODO: Call C++ bridge.ExecuteSPARQLOnBatch()

    print(f"Executing SPARQL on batch of {batch.num_rows} rows")

    return batch  # Pass through for now
```

**Impact:** Streaming graph queries don't execute - they just return the input batch unchanged.

---

### 3. sabot_graph/sabot_graph_streaming.py

**File:** `sabot_graph/sabot_graph_streaming.py`

**Location:** Lines 106-126

**Issue:** Streaming execution not implemented, checkpoint returns fake data

```python
async def run(self):
    # TODO: Integrate with Sabot Kafka consumer
    # TODO: Process graph events
    # TODO: Execute continuous queries
    # TODO: Publish results to Kafka

    print(f"Streaming graph executor running...")
    print("  Kafka integration: TODO")
    print("  Continuous queries: TODO")

def checkpoint(self):
    # TODO: Create MarbleDB checkpoint

    print("Creating checkpoint...")

    return {"checkpoint_id": "ckpt_demo", "success": True}
```

**Impact:** Streaming graph queries cannot actually run.

---

### 4. sabot/operators/graph_query.py

**File:** `sabot/operators/graph_query.py`

**Location:** Line 159

**Issue:** `GraphEnrichOperator` doesn't enrich - just prints and passes through

```python
def _enrich_batch(self, batch):
    """Enrich batch with graph lookups."""
    if batch is None or batch.num_rows == 0:
        return batch

    # Extract vertex IDs
    vertex_ids = batch.column(self.vertex_column)

    # Batch graph lookups
    # TODO: Optimize with batch vertex lookup

    print(f"Enriching {batch.num_rows} rows with graph data")

    return batch  # For now, pass through
```

**Impact:** Graph enrichment in streaming pipelines doesn't work.

---

### 5. sabot_graph C++ Bridge

**File:** `sabot_graph/src/graph/sabot_graph_bridge.cpp`

**Location:** Lines 80-157

**Issue:** C++ bridge returns demo results instead of executing queries

```cpp
std::shared_ptr<arrow::Table> SabotGraphBridge::ExecuteCypher(
    const std::string& query) {

    // TODO: Integrate with actual Sabot executor

    // For now, return demo result
    auto schema = arrow::schema({
        arrow::field("id", arrow::int64()),
        arrow::field("name", arrow::utf8())
    });
    // ... creates hardcoded demo table
}

std::shared_ptr<arrow::RecordBatch> SabotGraphBridge::ExecuteCypherOnBatch(
    const std::string& query,
    const std::shared_ptr<arrow::RecordBatch>& batch) {

    // TODO: Integrate with SabotCypher for batch execution

    std::cout << "ExecuteCypherOnBatch: " << query << " on "
              << batch->num_rows() << " rows\n";

    return batch;  // Pass through for now
}
```

**Impact:** Even if Python calls C++, it gets fake data.

---

### 6. sabot_graph C++ State Backend

**File:** `sabot_graph/src/state/marble_graph_backend.cpp`

**Location:** Throughout - Lines 38-248

**Issue:** All MarbleDB operations are TODO stubs

```cpp
void MarbleGraphBackend::Open() {
    // TODO: Initialize MarbleDB instance
    // TODO: Create column families

    std::cout << "MarbleGraphBackend: Open at " << db_path_ << "\n";
}

void MarbleGraphBackend::PutVertex(const Vertex& vertex) {
    // TODO: Insert into graph_vertices_cf using MarbleDB

    std::cout << "PutVertex: id=" << vertex.id << "\n";
}

std::vector<Vertex> MarbleGraphBackend::GetVertex(int64_t vertex_id) {
    // TODO: Fast lookup from graph_vertices_cf (5-10μs via hot key cache)

    std::vector<Vertex> result;
    Vertex v;
    v.id = vertex_id;
    v.label = "DemoVertex";
    v.name = "Demo";
    result.push_back(v);
    return result;
}
```

**Impact:** Graph persistence doesn't work. All storage operations are fake.

---

### 7. sabot_cypher Module

**File:** `sabot_cypher/__init__.py`

**Location:** Lines 53-89

**Issue:** Native module not built, placeholder raises NotImplementedError

```python
class SabotCypherBridge:
    """
    Placeholder for SabotCypherBridge.

    The C++ native module is not yet built.
    """

    @staticmethod
    def create():
        raise NotImplementedError(
            f"SabotCypherBridge not yet built.\n"
            f"Import error: {_import_error}\n"
            f"Please build the C++ extension first."
        )

    def execute(self, query, params=None):
        raise NotImplementedError("execute() requires native extension")
```

**Impact:** sabot_cypher cannot execute queries without the native module being built.

---

### 8. sabot_cypher Wrapper

**File:** `sabot_cypher/sabot_cypher_wrapper.py`

**Location:** Lines 86-96

**Issue:** Without native module, only prints execution plan (doesn't execute)

```python
def execute(self, query: str, params: dict = None):
    # Step 1: Parse (would use Lark parser)
    plan = self._query_to_plan(query)

    # Step 2: Execute plan
    if self.use_native:
        result = self.bridge.execute_plan(plan)
        # ... returns actual result
    else:
        print("  ⚠️  Native execution not available")
        print(f"  Plan generated: {len(plan['operators'])} operators")
        for i, op in enumerate(plan['operators'], 1):
            print(f"    {i}. {op['type']}")
        return None  # Returns None!
```

**Impact:** Cypher queries print plans but return `None` without the native module.

---

### 9. sabot_cypher State Store

**File:** `sabot_cypher/src/state/unified_state_store.cpp`

**Location:** Throughout - Lines 31-400

**Issue:** All MarbleDB operations are TODO stubs, returns demo data

```cpp
void UnifiedStateStore::PutVertex(int64_t vertex_id, const Vertex& vertex) {
    // TODO: Serialize vertex to Arrow and store in MarbleDB

    std::cout << "PutVertex: " << vertex_id << "\n";
}

Vertex UnifiedStateStore::GetVertex(int64_t vertex_id) {
    // TODO: Fast lookup from MarbleDB (5-10 μs)

    Vertex vertex;
    vertex.id = vertex_id;
    vertex.name = "DemoVertex_" + std::to_string(vertex_id);
    return vertex;
}

Checkpoint UnifiedStateStore::GetLatestCheckpoint() {
    // TODO: Query latest checkpoint from workflow_checkpoints_cf

    Checkpoint checkpoint;
    checkpoint.checkpoint_id = "ckpt_demo";
    return checkpoint;
}
```

**Impact:** Cypher state management is entirely fake.

---

### 10. sabot_ql SPARQL Execution

**File:** `sabot_ql/bindings/python/sabot_ql.pyx`

**Location:** Lines 89-141

**Issue:** All execution methods are TODO stubs

```python
def __init__(self, db_path=":memory:"):
    # TODO: Initialize MarbleDB and create TripleStore/Vocabulary

    print(f"SabotQL created: {db_path}")

def add_triple(self, subject, predicate, object):
    # TODO: Convert to Term, add to vocabulary, insert triple

    print(f"add_triple: {subject} {predicate} {object}")

def execute_sparql(self, query):
    # TODO: Parse SPARQL, execute, return Arrow Table

    print(f"execute_sparql: {query}")

    # Return empty table
    return pa.table({
        'subject': pa.array([], type=pa.int64()),
        'predicate': pa.array([], type=pa.int64()),
        'object': pa.array([], type=pa.int64())
    })
```

**Impact:** SPARQL queries cannot execute - they return empty tables.

---

### 11. sabot_ql Join Operators

**File:** `sabot_ql/src/operators/join.cpp`

**Location:** Lines 380-445

**Issue:** Join implementations are placeholders

```cpp
std::shared_ptr<arrow::Table> MergeJoin::Execute(
    std::shared_ptr<arrow::Table> left,
    std::shared_ptr<arrow::Table> right) {

    // TODO: Implement merge join algorithm
    // This is a simplified placeholder

    return arrow::Table::Make(arrow::schema({}), {});
}

std::shared_ptr<arrow::Table> NestedLoopJoin::Execute(
    std::shared_ptr<arrow::Table> left,
    std::shared_ptr<arrow::Table> right) {

    // TODO: Implement nested loop join
    // For now, return not implemented

    throw std::runtime_error("NestedLoopJoin not implemented");
}
```

**Impact:** SPARQL queries with joins will fail or return empty results.

---

### 12. sabot_ql Triple Store

**File:** `sabot_ql/src/storage/triple_store_impl.cpp`

**Location:** Lines 284-446

**Issue:** ScanIndex not implemented, uses placeholder iteration

```cpp
std::shared_ptr<arrow::Table> TripleStoreImpl::ScanIndex(
    IndexType index,
    const std::optional<ValueId>& s,
    const std::optional<ValueId>& p,
    const std::optional<ValueId>& o) {

    // TODO: Once MarbleDB Iterator API is implemented, use range scans
    // For now, use placeholder iteration

    // TODO: Remove once MarbleDB Iterator API is implemented

    // Returns limited/incomplete results
}
```

**Impact:** SPARQL triple pattern matching is incomplete and inefficient.

---

## What IS Real

### ✅ Real Components

1. **Cypher Parser (Lark-based)**
   - Files: `sabot/_cython/graph/compiler/cypher_parser.py`
   - Status: Real parser that converts Cypher to AST
   - Evidence: 23,798 queries/sec benchmark (verified)

2. **SPARQL Parser**
   - Files: `sabot_ql/src/sparql/parser.cpp`
   - Status: Real C++ parser
   - Evidence: Builds, parses queries successfully

3. **Pattern Matching Kernels**
   - Files: `sabot/_cython/graph/executor/*.pyx`
   - Status: Real Cython kernels
   - Evidence: 3-37M matches/sec benchmarks (verified)

4. **GraphQueryEngine Framework**
   - Files: `sabot/_cython/graph/engine/query_engine.py`
   - Status: Real orchestration, calls real parsers
   - Evidence: Actual code that calls parsers/translators

5. **Test Infrastructure**
   - Files: `tests/unit/graph/*.py`, `tests/integration/sparql/*.py`
   - Status: Tests are real, they test the components
   - Evidence: Tests run, they just use the fake backends

---

## Benchmarks Using Fake Data

### ❌ Unreliable Benchmarks

1. **SabotCypher vs Kuzu Comparison**
   - File: `sabot_cypher/benchmarks/real_kuzu_comparison.py`
   - Issue: Uses `sabot_cypher.execute()` which depends on native module
   - Result: If native module not built, returns `None` or errors
   - Status: Benchmark infrastructure is real, but backend is not

2. **Kuzu Study Benchmarks**
   - File: `benchmarks/kuzu_study/run_benchmark.py`
   - Issue: Depends on query_cypher() which may use fake backend
   - Status: Benchmarks run, but measure fake execution if backend not ready

---

## API Facade Issues

### sabot/api/graph_facade.py

**File:** `sabot/api/graph_facade.py`

**Location:** Lines 141-182

**Issue:** Unimplemented methods raise NotImplementedError

```python
def load_graph(self, path: str, format: str = 'auto'):
    # TODO: Implement graph loading
    logger.warning("Graph loading not yet implemented")
    raise NotImplementedError("Graph loading coming soon")

def create_graph(self, name: str, schema: Optional[Dict] = None):
    # TODO: Implement graph creation
    logger.warning("Graph creation not yet implemented")
    raise NotImplementedError("Graph creation coming soon")

def register_graph(self, name: str, graph: Any):
    # TODO: Register graph in both Cypher and SPARQL engines
    logger.warning("Graph registration not yet implemented")
    raise NotImplementedError("Graph registration coming soon")
```

**Impact:** Graph API facade exists but core functionality is missing.

---

## Summary Tables

### Fake Returns by Component

| Component | File | Method | Returns |
|-----------|------|--------|---------|
| SabotGraphBridge | `sabot_graph_python.py:80-83` | `execute_cypher()` | Hardcoded demo table |
| SabotGraphBridge | `sabot_graph_python.py:100-103` | `execute_sparql()` | Hardcoded demo table |
| SabotGraphBridge | `sabot_graph_python.py:120` | `execute_cypher_on_batch()` | Input batch (pass-through) |
| SabotGraphBridge | `sabot_graph_python.py:137` | `execute_sparql_on_batch()` | Input batch (pass-through) |
| GraphEnrichOperator | `operators/graph_query.py:159` | `_enrich_batch()` | Input batch (pass-through) |
| StreamingGraphExecutor | `sabot_graph_streaming.py:126` | `checkpoint()` | `{"checkpoint_id": "ckpt_demo"}` |
| SabotQL | `sabot_ql.pyx:126` | `execute_sparql()` | Empty table |
| SabotCypherWrapper | `sabot_cypher_wrapper.py:96` | `execute()` | `None` (without native) |
| C++ Bridge | `sabot_graph_bridge.cpp:82-116` | `ExecuteCypher/SPARQL()` | Hardcoded demo tables |
| MarbleGraphBackend | `marble_graph_backend.cpp` | All methods | Fake/demo data |
| UnifiedStateStore | `unified_state_store.cpp` | All methods | Fake/demo data |

### TODO Stubs by Component

| Component | File | Count | Impact |
|-----------|------|-------|--------|
| sabot_graph C++ | `sabot_graph/src/**/*.cpp` | 50+ | All graph storage/execution |
| sabot_cypher C++ | `sabot_cypher/src/**/*.cpp` | 60+ | All Cypher state/execution |
| sabot_ql C++ | `sabot_ql/src/**/*.cpp` | 40+ | SPARQL joins, scanning, execution |
| Python bindings | `sabot_ql.pyx`, etc. | 15+ | All Python→C++ integration |

---

## Recommendations

### Priority 1: Critical (Blocking)

1. **Build sabot_cypher native module**
   - File: `sabot_cypher/CMakeLists.txt`
   - Command: `cmake --build build`
   - Blocks: All Cypher query execution

2. **Implement SabotGraphBridge execution**
   - File: `sabot_graph/sabot_graph_python.py`
   - Replace demo returns with actual C++ calls
   - Blocks: All graph query operations

3. **Implement MarbleDB integration**
   - Files: `sabot_graph/src/state/marble_graph_backend.cpp`
   - Replace TODO stubs with actual MarbleDB calls
   - Blocks: Graph persistence

### Priority 2: High

4. **Complete SPARQL execution**
   - File: `sabot_ql/bindings/python/sabot_ql.pyx`
   - Implement `execute_sparql()` to call C++ engine
   - Blocks: SPARQL query execution

5. **Implement join operators**
   - File: `sabot_ql/src/operators/join.cpp`
   - Complete MergeJoin and NestedLoopJoin
   - Blocks: Complex SPARQL queries

6. **Complete TripleStore scanning**
   - File: `sabot_ql/src/storage/triple_store_impl.cpp`
   - Implement ScanIndex with MarbleDB iterators
   - Blocks: Efficient triple pattern matching

### Priority 3: Medium

7. **Implement streaming graph operators**
   - File: `sabot_graph/sabot_graph_streaming.py`
   - Complete Kafka integration and continuous queries
   - Blocks: Real-time graph analytics

8. **Complete GraphEnrichOperator**
   - File: `sabot/operators/graph_query.py`
   - Implement actual graph lookups
   - Blocks: Stream enrichment with graph data

---

## Testing Status

### What Tests Reveal

All graph tests in `tests/unit/graph/` and `tests/integration/sparql/` are **structurally correct** but test against **fake backends**:

- ✅ Tests verify API contracts
- ✅ Tests verify data flow
- ❌ Tests don't verify actual execution (fake backends)
- ❌ Tests don't catch that results are hardcoded

**Example:** `test_graph_queries.py` tests `query_cypher()` but the backend returns demo data, so tests pass even though queries don't execute.

---

## Conclusion

**Current State:** Graph query functionality is **NOT PRODUCTION READY**

- **Parsers:** ✅ Real and working
- **Execution:** ❌ Fake/TODO stubs
- **Storage:** ❌ Fake/TODO stubs
- **API:** ⚠️ Real structure, fake backends
- **Tests:** ⚠️ Pass but test fake implementations

**To Make Real:**

1. Build native modules (sabot_cypher)
2. Implement MarbleDB integration
3. Complete C++ execution engines
4. Connect Python→C++ properly
5. Replace all demo/pass-through returns

**Estimated Work:** 2-4 weeks for Priority 1 items to get basic functionality working.

---

**Audit Complete**
**Report Generated:** October 22, 2025
