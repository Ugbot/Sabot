# SabotGraph Documentation Index

Quick navigation to all SabotGraph documentation.

---

## 📚 **Core Documentation**

### **Getting Started**
- **[README.md](README.md)** - Module overview, quick start, features
- **[STATUS.md](STATUS.md)** - Current implementation status
- **[IMPLEMENTATION_COMPLETE.md](IMPLEMENTATION_COMPLETE.md)** - Plan execution summary

### **Architecture**
- **[SABOT_GRAPH_COMPLETE.md](SABOT_GRAPH_COMPLETE.md)** - Complete architecture guide
- **[/SABOT_GRAPH_INTEGRATION_COMPLETE.md](../SABOT_GRAPH_INTEGRATION_COMPLETE.md)** - Integration summary
- **[/SABOT_GRAPH_FINAL_SUMMARY.md](../SABOT_GRAPH_FINAL_SUMMARY.md)** - Final summary
- **[/SABOT_GRAPH_MARBLEDB_COMPLETE.md](../SABOT_GRAPH_MARBLEDB_COMPLETE.md)** - MarbleDB integration

---

## 💻 **Code Reference**

### **C++ API**

**Headers:**
- `include/sabot_graph/graph/common_types.h` - GraphPlan, GraphOperatorDescriptor
- `include/sabot_graph/graph/sabot_graph_bridge.h` - Main bridge API
- `include/sabot_graph/graph/graph_plan_translator.h` - Logical → Morsel
- `include/sabot_graph/state/marble_graph_backend.h` - MarbleDB backend
- `include/sabot_graph/execution/batch_graph_operators.h` - Batch analytics

**Implementation:**
- `src/graph/sabot_graph_bridge.cpp`
- `src/graph/graph_plan_translator.cpp`
- `src/state/marble_graph_backend.cpp`
- `src/execution/batch_graph_operators.cpp`

**Tests:**
- `test_sabot_graph_bridge.cpp` - C++ integration tests

**Build:**
- `CMakeLists.txt` - Build configuration

### **Python API**

**Core:**
- `__init__.py` - Module exports
- `sabot_graph_python.py` - SabotGraphBridge, SabotGraphOrchestrator
- `sabot_graph_streaming.py` - StreamingGraphExecutor

**Integration:**
- `../sabot/operators/graph_query.py` - CypherOperator, SPARQLOperator, GraphEnrichOperator

---

## 📖 **Examples**

### **Basic Examples**
- `examples/graph_in_sabot_flow.py` - Graph queries with standard operators
- `examples/simple_graph_test.py` - Basic API test
- `examples/standalone_test.py` - Standalone test (no Sabot deps)

### **Advanced Examples**
- `examples/streaming_fraud_detection.py` - Real-time fraud detection with continuous Cypher queries

---

## 📊 **Benchmarks**

- `benchmarks/unified_store_benchmark.py` - Unified MarbleDB vs separate stores (106x faster, 3x less memory)

---

## 🔧 **Build & Test**

### **Build C++ Library**

```bash
cd sabot_graph/build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j8
```

### **Run C++ Tests**

```bash
cd sabot_graph/build
DYLD_LIBRARY_PATH=.:../../vendor/arrow/cpp/build/install/lib ./test_sabot_graph_bridge
```

### **Run Python Examples**

```bash
cd sabot_graph
python3 examples/standalone_test.py
python3 examples/streaming_fraud_detection.py
python3 benchmarks/unified_store_benchmark.py
```

---

## 🎯 **Quick Reference**

### **API Usage**

```python
from sabot_graph import create_sabot_graph_bridge

# Create bridge
graph = create_sabot_graph_bridge(state_backend='marbledb')

# Register graph
graph.register_graph(vertices, edges)

# Execute queries
result = graph.execute_cypher("MATCH (a)-[:KNOWS]->(b) RETURN a, b")
result = graph.execute_sparql("SELECT ?s ?o WHERE { ?s <knows> ?o }")
```

### **In Sabot Flows**

```python
from sabot.api.stream import Stream
from sabot_graph import create_sabot_graph_bridge

graph = create_sabot_graph_bridge()

(Stream.from_kafka('events')
    .filter(lambda b: b.column('amount') > 1000)
    .cypher("MATCH (a)-[:KNOWS]->(b) RETURN b.id", graph)
    .to_kafka('results'))
```

### **Streaming**

```python
from sabot_graph import StreamingGraphExecutor

executor = StreamingGraphExecutor(kafka_source='transactions')
executor.register_continuous_query(query, output_topic='alerts')
executor.start()
```

---

## 📈 **Performance**

- **Cypher**: 52.9x faster than Kuzu
- **SPARQL**: 23,798 q/s parser
- **MarbleDB**: 5-10μs lookups
- **Unified**: 106x faster than separate stores
- **Memory**: 3x less than separate stores

---

## 🔗 **Related Projects**

- **[sabot_cypher/](../sabot_cypher/)** - Cypher query engine (52.9x faster than Kuzu)
- **[sabot_ql/](../sabot_ql/)** - SPARQL query engine (23,798 q/s parser)
- **[sabot_sql/](../sabot_sql/)** - SQL query engine (pattern template)
- **[MarbleDB/](../MarbleDB/)** - State backend (5-10μs lookups)

---

## 📝 **Status**

**Current:** ✅ Core structure complete, all tests passing  
**Next:** Engine integration, MarbleDB CFs, Stream API extensions  
**Version:** 0.1.0  
**Date:** December 19, 2024

---

*For questions or issues, see STATUS.md or README.md*

