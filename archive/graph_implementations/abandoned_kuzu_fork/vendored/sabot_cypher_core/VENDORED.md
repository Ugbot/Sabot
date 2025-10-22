# Vendored Kuzu Frontend

This directory contains a **hard fork** of Kuzu's frontend components.

## Source

**Original Repository:** https://github.com/kuzudb/kuzu  
**Fork Date:** October 12, 2025  
**Fork Commit:** [commit hash from vendor/kuzu]  

## License

MIT License - see `LICENSE` file in this directory.

## What Was Kept

We vendored the following Kuzu components:

- **parser/** - Cypher text parser (openCypher M23)
- **binder/** - Semantic analysis and binding
- **planner/** - Logical query planning
- **optimizer/** - Cost-based query optimization
- **catalog/** - Schema catalog management
- **common/** - Utilities, types, data structures
- **function/** - Function registry (scalar, aggregate, table)
- **main/** - Connection API (Database, Connection classes)
- **include/** - All headers for above components
- **third_party/** - Dependencies (pybind11, re2, antlr4, etc.)

## What Was Deleted

We **deleted** the following Kuzu components:

- **processor/** - Physical operator execution (DELETED - replaced by Sabot Arrow executor)
- **storage/** - Storage layer (DELETED - Sabot uses Arrow tables)
- **test/** - Test suite (DELETED - references deleted components)
- **benchmark/** - Benchmarks (DELETED - references deleted components)
- **tools/** - Language bindings (DELETED - we build our own Python bindings)

## Modifications

**IMPORTANT:** We made **ZERO changes** to the Kuzu source code itself.

The only "modifications" were **deletions**:
1. Deleted `src/processor/` directory
2. Deleted `src/storage/` directory
3. Deleted `test/` directory
4. Deleted `benchmark/` directory

**Namespace:** The `namespace kuzu` is kept EXACTLY as is throughout all vendored code.

## Integration with SabotCypher

**How It Works:**

1. **Kuzu provides:** Parse → Bind → Plan → Optimize → `LogicalPlan`
2. **SabotCypher provides:** Translate `LogicalPlan` → `ArrowPlan` → Execute on Arrow

**Code Example:**
```cpp
// SabotCypher code (namespace sabot_cypher)
#include "parser/parser.h"        // From vendored Kuzu (namespace kuzu)
#include "planner/planner.h"      // From vendored Kuzu (namespace kuzu)

namespace sabot_cypher {
namespace cypher {

arrow::Result<CypherResult> SabotCypherBridge::ExecuteCypher(const std::string& query) {
    // Use Kuzu APIs (different namespace)
    auto kuzu_db = kuzu::main::Database::create(...);
    auto kuzu_conn = kuzu::main::Connection::create(kuzu_db);
    auto kuzu_result = kuzu_conn->query(query);
    auto logical_plan = kuzu_result->getLogicalPlan();  // kuzu::planner::LogicalPlan
    
    // Translate using SabotCypher (same namespace)
    auto arrow_plan = LogicalPlanTranslator::Translate(*logical_plan);
    
    // Execute using SabotCypher (same namespace)
    auto result = ArrowExecutor::Execute(*arrow_plan, vertices_, edges_);
    
    return CypherResult{result, ...};
}

}}  // namespace sabot_cypher::cypher
```

## Build System

This vendored directory is built as a **static library**:

```cmake
# In vendored/sabot_cypher_core/CMakeLists.txt
add_library(kuzu_core STATIC
    src/parser/*.cpp
    src/binder/*.cpp
    src/planner/*.cpp
    src/optimizer/*.cpp
    # ... other frontend components
)
```

Then linked into `libsabot_cypher.so`:

```cmake
# In sabot_cypher/CMakeLists.txt
add_library(sabot_cypher SHARED
    src/cypher/sabot_cypher_bridge.cpp
    src/cypher/logical_plan_translator.cpp
    src/execution/arrow_executor.cpp
)

target_link_libraries(sabot_cypher
    kuzu_core  # Static lib from vendored/
    Arrow::arrow_shared
)
```

## Upstream Tracking

**Do we track Kuzu upstream?**

Not actively. This is a **hard fork**, not a vendoring-with-updates strategy.

**Why?**

1. We only need the frontend (parser/binder/optimizer)
2. Kuzu's API is relatively stable for these components
3. We deleted large portions (processor/storage) that would conflict with updates
4. Our integration is via `LogicalPlan` interface, which is stable

**If we need Kuzu updates:**

We can selectively merge parser/binder/optimizer changes if needed, but this is not planned.

## Attribution

All code in this directory is:
- **Copyright:** Kuzu contributors
- **License:** MIT (see LICENSE file)
- **Original Author:** https://github.com/kuzudb/kuzu

SabotCypher gratefully acknowledges the Kuzu team for their excellent Cypher parser and query optimizer.

## Questions?

See `../ARCHITECTURE.md` for details on how Kuzu frontend integrates with Sabot execution.
