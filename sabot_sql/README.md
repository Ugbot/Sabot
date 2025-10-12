# SabotSQL: Distributed SQL Engine

**Status:** ✅ Implementation Complete

## Overview

SabotSQL is a distributed SQL query engine built on top of Sabot's morsel-driven execution framework. It combines DuckDB's best-in-class SQL parser and optimizer with Sabot's distributed agent-based execution model.

## Architecture

```
SQL Query
    ↓
DuckDB Parser → DuckDB Optimizer
    ↓
SQL-to-Operator Translator
    ↓
Sabot Morsel Operators
    ↓
Agent-Based Distributed Execution
```

## Key Features

- **DuckDB Integration**: Leverages DuckDB's excellent SQL parser and query optimizer
- **Distributed Execution**: Agent-based provisioning with morsel parallelism
- **Arrow Native**: Zero-copy columnar processing throughout
- **SQL Support**: SELECT, JOIN, GROUP BY, CTEs, subqueries, ORDER BY, LIMIT
- **Multiple Modes**: Local, local-parallel, and distributed execution

## Components

### C++ Layer

#### SQL Engine
- **DuckDB Bridge** (`sql/duckdb_bridge.{h,cpp}`): SQL parsing and optimization
- **Operator Translator** (`sql/sql_operator_translator.{h,cpp}`): Converts DuckDB plans to Sabot operators
- **Query Engine** (`sql/query_engine.{h,cpp}`): End-to-end SQL execution

#### SQL-Specific Operators
- **TableScanOperator** (`operators/table_scan.{h,cpp}`): Read from Arrow/CSV/Parquet
- **CTEOperator** (`operators/cte.{h,cpp}`): Common Table Expression materialization
- **SubqueryOperator** (`operators/subquery.{h,cpp}`): Scalar, EXISTS, IN, correlated subqueries

### Python Layer

Located in `sabot/sql/`:
- **SQLController** (`controller.py`): Agent provisioning and coordination
- **SQL Agents** (`agents.py`): Specialized agents for scan, join, aggregate
- **SQL API** (`../api/sql.py`): High-level Python interface

## Building

```bash
cd sabot_sql
mkdir build && cd build
cmake ..
make -j$(nproc)
```

**Dependencies:**
- Apache Arrow (vendored in `../vendor/arrow`)
- DuckDB (vendored in `../vendor/duckdb`)
- C++20 compiler (GCC 11+, Clang 14+)

## Usage

### Python API

```python
from sabot.api.sql import SQLEngine

# Create engine with distributed execution
engine = SQLEngine(
    num_agents=4,
    execution_mode="local_parallel"
)

# Register tables
engine.register_table("customers", customers_table)
engine.register_table("orders", orders_table)

# Execute SQL with CTEs
result = await engine.execute("""
    WITH high_value_orders AS (
        SELECT customer_id, SUM(amount) as total
        FROM orders
        GROUP BY customer_id
        HAVING total > 10000
    )
    SELECT c.name, h.total
    FROM customers c
    JOIN high_value_orders h ON c.id = h.customer_id
    ORDER BY h.total DESC
    LIMIT 10
""")

print(result.to_pandas())
```

### C++ API

```cpp
#include <sabot_sql/sql/query_engine.h>

auto engine = SQLQueryEngine::Create();
engine->RegisterTable("customers", customers_arrow_table);
engine->RegisterTable("orders", orders_arrow_table);

auto result = engine->Execute("SELECT * FROM customers WHERE total > 1000");
```

## Examples

- **SQL Pipeline Demo**: `examples/sql_pipeline_demo.py`
- Shows basic queries, joins, aggregations, and CTEs
- Demonstrates agent-based distributed execution

## Architecture Benefits

### Why This Design?

1. **DuckDB Brain**: Best-in-class SQL parsing and optimization
2. **Sabot Muscle**: Distributed morsel execution across agents
3. **Arrow Efficiency**: Zero-copy columnar processing
4. **Agent Scaling**: Dynamic resource provisioning

### Similar to Spark SQL

This architecture mirrors Spark SQL:
- **Spark SQL**: Catalyst optimizer → Spark execution engine
- **SabotSQL**: DuckDB optimizer → Sabot morsel operators

## Execution Modes

### Local
Single-threaded execution for small queries

### Local Parallel
Multi-threaded with morsel parallelism (default)

### Distributed
Agent-based execution across multiple nodes with Arrow Flight shuffle

## Performance Characteristics

- **Zero-copy**: Arrow columnar format throughout
- **Morsel parallelism**: Cache-friendly batch processing
- **Expected Performance**: Within 2x of DuckDB for local, linear scaling for distributed

## Integration with Sabot

SabotSQL is a standalone module that integrates with:
- **Sabot Agents**: Uses `AgentRuntime` and `DurableAgentManager`
- **Arrow Operators**: Leverages Sabot's morsel-driven operators
- **Stream API**: Compatible with Sabot's streaming infrastructure

## vs SabotQL

- **SabotQL**: RDF/SPARQL triple store (graph queries)
- **SabotSQL**: General-purpose SQL engine (relational queries)
- Both share the Arrow/morsel execution foundation

## License

Same as Sabot project.

---

**Status**: Implementation complete, ready for testing
**Last Updated**: October 12, 2025


