# SabotCypher

**A high-performance Cypher query engine built by hard-forking Kuzu's parser/binder/optimizer and replacing physical execution with Sabot's Arrow-based graph operators.**

## Origins and Architecture

### Hard Fork of Kuzu

SabotCypher is a **hard fork** of Kuzu, following the exact same pattern as sabot_sql (which forked DuckDB):

- **Parser/Binder/Planner/Optimizer**: Vendored from Kuzu for robust Cypher parsing and optimization
- **Physical Runtime**: Completely replaced with Sabot's Arrow-based graph operators
- **No Kuzu Execution**: Zero Kuzu physical operators; Sabot-only runtime

### Why Fork Kuzu?

Kuzu provides:
- Best-in-class Cypher parser (openCypher M23 compliant)
- Sophisticated query optimizer for graph queries
- Cost-based join ordering and pattern optimization
- Full logical plan infrastructure

SabotCypher adds:
- Zero-copy Arrow execution
- Pattern matching kernels (`match_2hop`, `match_3hop`, variable-length paths)
- Stream processing integration
- Distributed query execution

### Architecture Comparison

**Original Kuzu:**
```
Cypher Query → Parser → Binder → Optimizer → LogicalPlan
                                                    ↓
                            Physical Plan (Kuzu processors)
                                                    ↓
                            Storage Layer (Kuzu storage)
                                                    ↓
                                                Results
```

**SabotCypher:**
```
Cypher Query → Kuzu Parser → Kuzu Binder → Kuzu Optimizer → LogicalPlan
                                                                   ↓
                              Sabot Translator (LogicalPlan → ArrowPlan)
                                                                   ↓
                                    Arrow Executor (Pattern Matching + Operators)
                                                                   ↓
                                        Arrow Tables (Zero-Copy)
                                                                   ↓
                                                            Results
```

## Directory Structure

```
sabot_cypher/
├── vendored/sabot_cypher_core/  # Kuzu frontend ONLY (kuzu:: namespace KEPT)
│   ├── src/
│   │   ├── parser/              # Cypher parser
│   │   ├── binder/              # Query binder
│   │   ├── planner/             # Logical planner
│   │   ├── optimizer/           # Query optimizer
│   │   ├── catalog/             # Catalog management
│   │   ├── common/              # Common utilities
│   │   └── function/            # Function registry
│   └── include/                 # Kuzu headers
│   [NO processor/, NO storage/ - deleted]
├── include/sabot_cypher/        # NEW Sabot headers (sabot_cypher:: namespace)
│   ├── cypher/                  # Bridge, translator
│   │   ├── sabot_cypher_bridge.h
│   │   ├── logical_plan_translator.h
│   │   └── op_mapping.h
│   └── execution/
│       └── arrow_executor.h
├── src/                         # NEW Sabot implementation (sabot_cypher:: namespace)
│   ├── cypher/
│   │   ├── sabot_cypher_bridge.cpp
│   │   ├── logical_plan_translator.cpp
│   │   └── op_mapping.cpp
│   └── execution/
│       └── arrow_executor.cpp
├── bindings/
│   └── python/
│       └── pybind_module.cpp    # Python bindings
├── CMakeLists.txt               # Build frontend + Sabot translator
├── __init__.py                  # Python module
└── README.md                    # This file
```

## Namespace Strategy

**Critical Design Decision:** SabotCypher uses TWO COEXISTING NAMESPACES:

1. **`namespace kuzu`** - in `vendored/sabot_cypher_core/` (parser, binder, optimizer)
   - Kept EXACTLY as is from Kuzu
   - No renaming required
   - Isolated in vendored directory

2. **`namespace sabot_cypher`** - in `include/sabot_cypher/` and `src/` (translator, executor)
   - New Sabot code
   - Calls Kuzu APIs
   - Implements Arrow execution

This matches the sabot_sql pattern exactly:
- `namespace duckdb` in `vendored/sabot_sql_core/` (5214 occurrences)
- `namespace sabot_sql` in custom code (4951 occurrences)

## Features (Planned)

- **Cypher Query Support**: Full openCypher M23 syntax via Kuzu parser
- **Pattern Matching**: 2-hop, 3-hop, variable-length paths using Sabot kernels
- **Aggregation**: COUNT, SUM, AVG, MIN, MAX via Arrow compute
- **Joins**: Hash join on pattern variables
- **Filtering**: WHERE clauses with Arrow compute kernels
- **Ordering**: ORDER BY with Arrow sort
- **Limits**: LIMIT/SKIP with Arrow slice
- **Zero-Copy**: Pure Arrow data flow throughout

## Build Status

**Current Phase**: Skeleton Complete ✅

- ✅ Kuzu frontend copied to `vendored/sabot_cypher_core/`
- ✅ Physical execution deleted (processor/, storage/)
- ✅ Sabot translator headers created
- ✅ Stub implementations for bridge, translator, executor
- ✅ CMakeLists.txt skeleton
- ⏳ Kuzu frontend build integration (next)
- ⏳ Translator implementation (next)

## Quick Start (Planned)

### Build

```bash
cd sabot_cypher
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
```

### Usage

```python
from sabot_cypher import SabotCypherBridge

# Create bridge
bridge = SabotCypherBridge.create()

# Register graph
bridge.register_graph(vertices_table, edges_table)

# Execute Cypher
result = bridge.execute("MATCH (a)-[:KNOWS]->(b) RETURN a.name, b.name")
print(result.table)
```

## Development Roadmap

### Phase 1: Skeleton ✅ (Current)
- [x] Fork Kuzu to vendored/
- [x] Delete physical execution
- [x] Create Sabot translator headers
- [x] Create stub implementations
- [x] Write documentation

### Phase 2: Kuzu Frontend Integration (Next)
- [ ] Build Kuzu parser/binder/optimizer as library
- [ ] Link with Sabot translator
- [ ] Verify parse→bind→optimize pipeline works

### Phase 3: Basic Translation
- [ ] Scan translation (node/rel scans)
- [ ] Filter translation (WHERE clauses)
- [ ] Project translation (RETURN clauses)
- [ ] Limit translation (LIMIT/SKIP)

### Phase 4: Pattern Matching
- [ ] Integrate match_2hop kernel
- [ ] Integrate match_3hop kernel
- [ ] Variable-length path support

### Phase 5: Advanced Features
- [ ] Join translation (pattern joins)
- [ ] Aggregate translation (COUNT/SUM/AVG/MIN/MAX)
- [ ] ORDER BY translation
- [ ] Multiple MATCH + WITH support

### Phase 6: Python Bindings
- [ ] Pybind11 module
- [ ] PyArrow integration
- [ ] Python API

### Phase 7: Benchmarks
- [ ] Port Kuzu study benchmarks (Q1-Q9)
- [ ] Performance validation
- [ ] Result correctness verification

## Credits and Attribution

### Kuzu
SabotCypher vendors Kuzu's parser, binder, planner, and optimizer components:
- **Source**: https://github.com/kuzudb/kuzu
- **License**: MIT License
- **Usage**: Parser/binder/planner/optimizer only (no physical execution runtime)
- **Modifications**: Deleted processor/ and storage/ directories; no code changes to frontend

### Sabot
Execution engine and runtime:
- **All Physical Execution**: 100% Sabot Arrow + pattern matching operators
- **Arrow Integration**: Zero-copy data flow via Arrow tables
- **Pattern Matching**: Sabot's match_2hop, match_3hop, variable-length path kernels

## License

SabotCypher components: See [LICENSE](../LICENSE) in parent directory.

Vendored Kuzu components: MIT License (see `vendored/sabot_cypher_core/LICENSE`)

## Status

**⚠️ UNDER ACTIVE DEVELOPMENT ⚠️**

SabotCypher is currently in early development (skeleton phase). Not yet ready for use.

Target completion: 2-3 weeks for basic query execution (Q1-Q9 from benchmarks).

