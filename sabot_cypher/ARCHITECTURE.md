# SabotCypher Architecture

## Hard Fork Strategy

SabotCypher follows the **exact same pattern** as sabot_sql, which successfully forked DuckDB.

### The Fork Pattern

**sabot_sql (DuckDB Fork):**
```
sabot_sql/
├── vendored/sabot_sql_core/     # DuckDB frontend (namespace duckdb)
├── include/sabot_sql/           # NEW Sabot headers (namespace sabot_sql)
├── src/                         # NEW Sabot implementation
└── CMakeLists.txt               # Build both
```

**sabot_cypher (Kuzu Fork):**
```
sabot_cypher/
├── vendored/sabot_cypher_core/  # Kuzu frontend (namespace kuzu)
├── include/sabot_cypher/        # NEW Sabot headers (namespace sabot_cypher)
├── src/                         # NEW Sabot implementation
└── CMakeLists.txt               # Build both
```

**Key Insight:** Both approaches keep the original namespace (`duckdb::` or `kuzu::`) intact in the vendored directory and create NEW code in a separate namespace (`sabot_sql::` or `sabot_cypher::`).

## Component Layers

### Layer 1: Kuzu Frontend (Vendored, Unchanged)

**Location:** `vendored/sabot_cypher_core/src/`

**Kept:**
- `parser/` - Cypher text → AST (openCypher M23)
- `binder/` - AST → Bound statements (semantic analysis)
- `planner/` - Bound statements → Logical plan
- `optimizer/` - Logical plan optimization (cost-based, join ordering)
- `catalog/` - Schema catalog (node/rel types, properties)
- `common/` - Utilities (types, exceptions, data structures)
- `function/` - Function registry (scalar, aggregate, table functions)
- `main/` - Connection API (Database, Connection classes)

**Deleted:**
- `processor/` - Physical operators (DELETED - replaced by Sabot)
- `storage/` - Storage layer (DELETED - replaced by Sabot)
- `test/` - Tests (DELETED - references deleted code)
- `benchmark/` - Benchmarks (DELETED - references deleted code)

**Namespace:** `namespace kuzu` - kept EXACTLY as is

### Layer 2: Sabot Translator (New)

**Location:** `include/sabot_cypher/cypher/`, `src/cypher/`

**Components:**

**1. SabotCypherBridge** (`sabot_cypher_bridge.h/cpp`)
- Connection API (Create, ExecuteCypher, Explain, RegisterGraph)
- Owns Kuzu Database + Connection
- Orchestrates parse → bind → optimize → translate → execute pipeline
- Returns Arrow tables to caller

**2. LogicalPlanTranslator** (`logical_plan_translator.h/cpp`)
- Visits Kuzu `LogicalPlan` tree (from optimizer)
- Maps each `LogicalOperator` to `ArrowOperatorDesc`
- Produces `ArrowPlan` (list of Arrow operators)

**Operator Mapping Table:**

| Kuzu Logical Operator | Arrow Operator | Implementation |
|-----------------------|----------------|----------------|
| `LogicalScan` | Scan | Filter vertices/edges table by label/type |
| `LogicalFilter` | Filter | Arrow compute: `pc.equal`, `pc.less`, etc. |
| `LogicalProjection` | Project | Arrow select + late property materialization |
| `LogicalHashJoin` | Join | Arrow hash join on pattern variables |
| `LogicalAggregate` | Aggregate | Arrow group_by: COUNT/SUM/AVG/MIN/MAX |
| `LogicalOrder` | OrderBy | Arrow sort_indices (ASC/DESC) |
| `LogicalLimit` | Limit | Arrow slice (offset + limit) |
| `LogicalExtend` | Extend | Sabot pattern kernels: match_2hop, match_3hop |
| `LogicalPathPropertyProbe` | VarLenPath | Sabot variable-length path kernel |

**Namespace:** `namespace sabot_cypher`

### Layer 3: Arrow Executor (New)

**Location:** `include/sabot_cypher/execution/`, `src/execution/`

**Components:**

**ArrowExecutor** (`arrow_executor.h/cpp`)
- Executes `ArrowPlan` operator-by-operator
- Uses Arrow compute kernels for filters, aggregates, sorts
- Uses Sabot pattern matching kernels for graph traversals
- Returns Arrow table as result

**Execution Methods:**
- `ExecuteScan()` - scan vertices or edges with label filter
- `ExecuteFilter()` - apply WHERE predicate using Arrow compute
- `ExecuteProject()` - select columns, late-materialize properties
- `ExecuteJoin()` - Arrow hash join on pattern variables
- `ExecuteAggregate()` - Arrow group_by with aggregate functions
- `ExecuteOrderBy()` - Arrow sort_indices
- `ExecuteLimit()` - Arrow slice

**Namespace:** `namespace sabot_cypher::execution`

## Data Flow

### End-to-End Query Execution

```
┌─────────────────────────────────────────────────────────────────┐
│ User Code (Python)                                              │
│                                                                 │
│ bridge = SabotCypherBridge.create()                             │
│ bridge.register_graph(vertices, edges)  # Arrow tables         │
│ result = bridge.execute("MATCH (a)-[:KNOWS]->(b) RETURN ...")  │
└─────────────────────────────────────────────────────────────────┘
         │
         ↓
┌─────────────────────────────────────────────────────────────────┐
│ SabotCypherBridge (namespace sabot_cypher)                      │
│                                                                 │
│ 1. Create Kuzu Database (in-memory)                             │
│ 2. Create Kuzu Connection                                       │
│ 3. Parse query → kuzu::parser::Parser                           │
│ 4. Bind query → kuzu::binder::Binder                            │
│ 5. Plan query → kuzu::planner::Planner                          │
│ 6. Optimize → kuzu::optimizer::Optimizer                        │
│    Returns: kuzu::planner::LogicalPlan                          │
└─────────────────────────────────────────────────────────────────┘
         │
         ↓
┌─────────────────────────────────────────────────────────────────┐
│ LogicalPlanTranslator (namespace sabot_cypher)                  │
│                                                                 │
│ Visit LogicalPlan tree:                                          │
│   • LogicalScan → ArrowOperatorDesc{type="Scan", ...}          │
│   • LogicalExtend → ArrowOperatorDesc{type="Extend", ...}      │
│   • LogicalFilter → ArrowOperatorDesc{type="Filter", ...}      │
│   • LogicalAggregate → ArrowOperatorDesc{type="Aggregate", ...}│
│   • LogicalOrder → ArrowOperatorDesc{type="OrderBy", ...}      │
│   • LogicalLimit → ArrowOperatorDesc{type="Limit", ...}        │
│                                                                 │
│ Returns: ArrowPlan (vector of operators)                        │
└─────────────────────────────────────────────────────────────────┘
         │
         ↓
┌─────────────────────────────────────────────────────────────────┐
│ ArrowExecutor (namespace sabot_cypher::execution)               │
│                                                                 │
│ Execute operator pipeline:                                       │
│   table = vertices_table                                         │
│   for op in arrow_plan.operators:                               │
│     if op.type == "Scan":                                       │
│       table = ExecuteScan(op, vertices, edges)                  │
│     elif op.type == "Filter":                                   │
│       table = ExecuteFilter(op, table)                          │
│     elif op.type == "Extend":                                   │
│       table = match_2hop(edges, table)  # Sabot kernel          │
│     elif op.type == "Aggregate":                                │
│       table = ExecuteAggregate(op, table)                       │
│     # ... more operators                                        │
│                                                                 │
│ Returns: arrow::Table (results)                                 │
└─────────────────────────────────────────────────────────────────┘
         │
         ↓
┌─────────────────────────────────────────────────────────────────┐
│ Python (PyArrow)                                                │
│                                                                 │
│ result.table → pyarrow.Table                                    │
│ print(result.table.to_pandas())                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Namespace Coexistence

**Critical Design:** Two namespaces coexist in the same binary:

### Vendored Kuzu Code (`namespace kuzu`)

**Headers:** `vendored/sabot_cypher_core/src/include/`
- `#include "parser/parser.h"` → `namespace kuzu::parser`
- `#include "binder/binder.h"` → `namespace kuzu::binder`
- `#include "planner/planner.h"` → `namespace kuzu::planner`
- `#include "optimizer/optimizer.h"` → `namespace kuzu::optimizer`

**Source:** `vendored/sabot_cypher_core/src/`
- All Kuzu code uses `namespace kuzu`
- NO CHANGES to Kuzu code (except deletions)

### New Sabot Code (`namespace sabot_cypher`)

**Headers:** `include/sabot_cypher/`
- `#include "sabot_cypher/cypher/sabot_cypher_bridge.h"` → `namespace sabot_cypher::cypher`
- `#include "sabot_cypher/execution/arrow_executor.h"` → `namespace sabot_cypher::execution`

**Source:** `src/`
- All new code uses `namespace sabot_cypher`
- Calls Kuzu APIs: `kuzu::main::Connection`, `kuzu::planner::LogicalPlan`, etc.

**Example:**
```cpp
// sabot_cypher_bridge.cpp
namespace sabot_cypher {
namespace cypher {

arrow::Result<CypherResult> SabotCypherBridge::ExecuteCypher(...) {
    // Call Kuzu API (different namespace)
    auto kuzu_result = kuzu_conn_->query(query);  // kuzu::main::Connection
    
    // Get logical plan (different namespace)
    auto logical_plan = kuzu_result->getLogicalPlan();  // kuzu::planner::LogicalPlan
    
    // Translate using Sabot code (same namespace)
    auto translator = LogicalPlanTranslator::Create();  // sabot_cypher::cypher::LogicalPlanTranslator
    auto arrow_plan = translator->Translate(*logical_plan);
    
    // Execute using Sabot code (same namespace)
    auto executor = execution::ArrowExecutor::Create();  // sabot_cypher::execution::ArrowExecutor
    auto result_table = executor->Execute(*arrow_plan, vertices_, edges_);
    
    return CypherResult{result_table, ...};
}

}}  // namespace sabot_cypher::cypher
```

## Build System

### CMakeLists.txt Structure

```cmake
# Build Kuzu frontend as static library
add_subdirectory(vendored/sabot_cypher_core)
# Produces: libkuzu_core.a (parser, binder, planner, optimizer)

# Build Sabot translator + executor
add_library(sabot_cypher SHARED
    src/cypher/sabot_cypher_bridge.cpp
    src/cypher/logical_plan_translator.cpp
    src/execution/arrow_executor.cpp
)

# Link everything
target_link_libraries(sabot_cypher
    kuzu_core      # Kuzu frontend (static)
    Arrow::arrow_shared
    # Sabot pattern matching kernels (to be added)
)
```

## Pattern Matching Integration

### Sabot Graph Kernels

SabotCypher will use existing Sabot pattern matching kernels:

**2-Hop Pattern:** `(a)-[r]->(b)`
```cpp
// Located in: sabot/_cython/graph/
auto result = match_2hop(edges_table, pattern);
// Returns: Arrow table with columns [a_id, r_id, b_id]
```

**3-Hop Pattern:** `(a)-[r1]->(b)-[r2]->(c)`
```cpp
auto result = match_3hop(edges_table, pattern);
// Returns: Arrow table with columns [a_id, r1_id, b_id, r2_id, c_id]
```

**Variable-Length Path:** `(a)-[r*1..3]->(b)`
```cpp
auto result = match_variable_length_path(edges_table, min_hops, max_hops);
// Returns: Arrow table with path information
```

### Extend Translation

When translator sees `LogicalExtend` (pattern edge traversal):

```cpp
arrow::Status LogicalPlanTranslator::TranslateExtend(
    const kuzu::planner::LogicalOperator& op,
    ArrowPlan& plan) {
    
    auto extend_op = static_cast<const kuzu::planner::LogicalExtend&>(op);
    
    // Determine hop count from extend operator
    if (extend_op.isRecursive()) {
        // Variable-length path
        ArrowOperatorDesc desc;
        desc.type = "VarLenPath";
        desc.params["min_hops"] = std::to_string(extend_op.getLowerBound());
        desc.params["max_hops"] = std::to_string(extend_op.getUpperBound());
        plan.operators.push_back(desc);
    } else {
        // Fixed-length pattern
        ArrowOperatorDesc desc;
        desc.type = "Extend";
        desc.params["hops"] = "1";  // Single edge traversal
        plan.operators.push_back(desc);
    }
    
    return arrow::Status::OK();
}
```

## Comparison: sabot_sql vs sabot_cypher

| Aspect | sabot_sql (DuckDB) | sabot_cypher (Kuzu) |
|--------|-------------------|---------------------|
| **Vendored Frontend** | DuckDB parser/planner/optimizer | Kuzu parser/binder/optimizer |
| **Query Language** | SQL | Cypher |
| **Logical Plan** | `duckdb::LogicalOperator` | `kuzu::planner::LogicalOperator` |
| **Physical Execution** | Sabot morsel operators | Sabot Arrow + pattern matching |
| **Namespace Strategy** | `duckdb::` + `sabot_sql::` | `kuzu::` + `sabot_cypher::` |
| **Key Operators** | ASOF JOIN, SAMPLE BY, TUMBLE | Pattern matching, variable-length paths |
| **Status** | ✅ Production (12.2M rows) | ⏳ Skeleton (in development) |

## Development Status

### Completed ✅
- [x] Kuzu forked to `vendored/sabot_cypher_core/`
- [x] Physical execution deleted (processor/, storage/)
- [x] Sabot translator headers created
- [x] Stub implementations (bridge, translator, executor)
- [x] CMakeLists.txt skeleton
- [x] Documentation (README, ARCHITECTURE)

### In Progress ⏳
- [ ] Build Kuzu frontend as library
- [ ] Link Kuzu frontend with Sabot translator
- [ ] Implement translator visitor pattern
- [ ] Implement basic operator mappings (Scan, Filter, Project, Limit)

### Planned 📋
- [ ] Pattern matching integration (Extend → match_2hop/match_3hop)
- [ ] Aggregation support (COUNT/SUM/AVG/MIN/MAX)
- [ ] JOIN translation (pattern variable joins)
- [ ] ORDER BY translation (Arrow sort)
- [ ] Python bindings (pybind11)
- [ ] Benchmarks (Q1-Q9 from kuzu_study)

## Success Criteria

1. **Parse → Bind → Optimize**: Kuzu frontend produces LogicalPlan ✅
2. **Translate**: LogicalPlan → ArrowPlan mapping complete
3. **Execute**: ArrowPlan → Arrow Table execution complete
4. **Benchmarks**: Q1-Q9 from kuzu_study all execute correctly
5. **Performance**: Competitive with or faster than Kuzu on benchmark queries
6. **Python API**: `import sabot_cypher; bridge.execute(query)` works

Target: **2-3 weeks** for Q1-Q9 support.

