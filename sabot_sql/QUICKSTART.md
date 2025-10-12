# SabotSQL Quick Start

## What is SabotSQL?

SabotSQL is a SQL engine built by:
- **Forking DuckDB's parser/planner** for robust SQL parsing
- **Adding time-series SQL** (ASOF JOIN, SAMPLE BY, LATEST BY) inspired by QuestDB
- **Adding window functions** (TUMBLE, HOP, SESSION) inspired by Flink
- **Replacing execution** with Sabot's Arrow-based morsel and shuffle operators

**Key Point**: This is NOT DuckDB with extensions. It's a hard fork where:
- DuckDB provides parsing/planning only (no physical execution)
- Time-series features are core (not bolted on)
- All execution is pure Sabot operators

## Installation

### 1. Build SabotSQL

```bash
cd sabot_sql
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
```

### 2. Set Runtime Library Path

```bash
export DYLD_LIBRARY_PATH=/Users/bengamble/Sabot/sabot_sql/build:/Users/bengamble/Sabot/vendor/arrow/cpp/build/install/lib:$DYLD_LIBRARY_PATH
```

## Hello World

```python
from sabot_sql import create_sabot_sql_bridge
from sabot import cyarrow as ca

# Create bridge
bridge = create_sabot_sql_bridge()

# Create test data
data = ca.Table.from_pydict({
    'id': [1, 2, 3, 4, 5],
    'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
    'price': [10.5, 20.3, 30.7, 40.1, 50.9]
})

# Register table
bridge.register_table("products", data)

# Execute SQL
result = bridge.execute_sql("SELECT * FROM products WHERE price > 25")

print(f"Result: {result.num_rows} rows")
# Output: Result: 3 rows
```

## Time-Series Examples

### ASOF JOIN

```python
from sabot import cyarrow as ca

# Time-series aligned join
trades = ca.Table.from_pydict({
    'symbol': ['AAPL', 'AAPL', 'MSFT'],
    'ts': [100, 200, 150],
    'price': [150.0, 155.0, 300.0]
})

quotes = ca.Table.from_pydict({
    'symbol': ['AAPL', 'AAPL', 'MSFT'],
    'ts': [95, 195, 145],
    'bid': [149.0, 154.0, 299.0]
})

bridge.register_table("trades", trades)
bridge.register_table("quotes", quotes)

result = bridge.execute_sql("""
    SELECT * FROM trades ASOF JOIN quotes 
    ON trades.symbol = quotes.symbol AND trades.ts <= quotes.ts
""")
```

### SAMPLE BY (Window Aggregation)

```python
from sabot import cyarrow as ca

trades = ca.Table.from_pydict({
    'symbol': ['AAPL'] * 1000,
    'timestamp': list(range(1000)),
    'price': [150.0 + i*0.1 for i in range(1000)]
})

bridge.register_table("trades", trades)

result = bridge.execute_sql("""
    SELECT symbol, AVG(price), COUNT(*) 
    FROM trades 
    SAMPLE BY 1h
""")
```

### LATEST BY (Deduplication)

```python
from sabot import cyarrow as ca

quotes = ca.Table.from_pydict({
    'symbol': ['AAPL', 'AAPL', 'MSFT', 'MSFT'],
    'timestamp': [100, 200, 150, 250],
    'price': [150.0, 155.0, 300.0, 305.0]
})

bridge.register_table("quotes", quotes)

result = bridge.execute_sql("""
    SELECT symbol, price, timestamp 
    FROM quotes 
    LATEST BY symbol
""")
```

## Distributed Execution

```python
from sabot_sql import SabotSQLOrchestrator
from sabot import cyarrow as ca

# Create orchestrator
orch = SabotSQLOrchestrator()

# Add agents
for i in range(4):
    orch.add_agent(f"agent_{i+1}")

# Distribute data
large_table = ca.Table.from_pydict({
    'id': list(range(100000)),
    'value': list(range(100000))
})

orch.distribute_table("data", large_table, strategy="round_robin")

# Execute distributed query
results = orch.execute_distributed_query(
    "SELECT COUNT(*) FROM data"
)

# Check results
successful = sum(1 for r in results if r['status'] == 'success')
print(f"Successful agents: {successful}/4")
```

## Running Examples

### C++ Test
```bash
DYLD_LIBRARY_PATH=./build:../vendor/arrow/cpp/build/install/lib \
./examples/test_asof_and_windows
```

### Python Integration Test
```bash
python3 examples/test_sabot_sql_integrated_extensions.py
```

### Fintech Demo
```bash
cd ../examples/fintech_enrichment_demo
python3 sabot_sql_enrichment_demo.py --agents 4
```

## Next Steps

1. **Read**: [Architecture](docs/ARCHITECTURE.md) for technical details
2. **Test**: Run benchmarks in `benchmarks/`
3. **Deploy**: See [Production Status](docs/PRODUCTION_STATUS.md)

## Performance Tips

- Use Arrow IPC files instead of CSV (10-100x faster loading)
- Increase agent count for larger datasets
- Use LIMIT clauses for testing before full queries
- Monitor orchestrator stats with `get_orchestrator_stats()`

## Support

See full documentation in [docs/](docs/) directory.

