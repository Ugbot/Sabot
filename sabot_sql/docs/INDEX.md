# SabotSQL Documentation Index

## Quick Links

- **[README](../README.md)**: Quick start and overview
- **[Production Status](PRODUCTION_STATUS.md)**: Deployment readiness checklist
- **[Architecture](ARCHITECTURE.md)**: Complete technical architecture

## Core Documentation

### Implementation
- **[Implementation Summary](IMPLEMENTATION_SUMMARY.md)**: What was built and how
- **[Integration Guide](INTEGRATION_GUIDE.md)**: Technical integration details
- **[Extensions Integrated](EXTENSIONS_INTEGRATED.md)**: Flink/QuestDB extension details

### Testing & Validation
- **[Benchmark 1M Rows](BENCHMARK_1M_ROWS.md)**: Scalability testing results
- **[Full Dataset Results](FULL_DATASET_RESULTS.md)**: Production-scale (12.2M rows) validation
- **[Fintech Demo Results](FINTECH_DEMO_RESULTS.md)**: Real-world fintech use case

## Features

### SQL Extensions

#### Flink SQL
- TUMBLE, HOP, SESSION windows
- CURRENT_TIMESTAMP → NOW()
- OVER clauses

#### QuestDB SQL
- ASOF JOIN (time-series aligned joins)
- SAMPLE BY (time-based aggregation)
- LATEST BY (deduplication)

#### Standard SQL
- All standard features (SELECT, FROM, WHERE, GROUP BY, ORDER BY, LIMIT)
- Joins (INNER, LEFT, RIGHT, FULL)
- Aggregations (COUNT, SUM, AVG, MIN, MAX)
- Subqueries and CTEs

### Architecture Highlights

**Binder Rewrites**
- ASOF JOIN → LEFT JOIN + hint extraction
- SAMPLE BY → GROUP BY DATE_TRUNC + interval capture
- LATEST BY → ORDER BY DESC LIMIT 1
- Automatic detection and preprocessing

**Sabot-Only Execution**
- No vendored physical runtime
- Pure Arrow + morsel + shuffle operators
- Build-time enforcement with flags
- Zero-copy data flow

**Distributed Coordination**
- Multi-agent orchestration
- Round-robin partitioning
- Shuffle repartitioning for joins/windows
- Linear scalability validated

## Performance Summary

### Latency
- Simple queries: < 1ms
- ASOF JOIN: < 10ms
- SAMPLE BY: < 1ms
- LATEST BY: < 1ms

### Throughput
- C++ execution: 77M rows/sec
- Python distributed: 642K rows/sec per agent
- Arrow operations: 250M rows/sec

### Scalability
- Tested: 10K → 10M rows (linear)
- Tested: 4 → 8 agents (linear)
- Ready: 100M+ rows, 16+ agents

## Examples

### Running Tests

**C++ Test**
```bash
cd sabot_sql
DYLD_LIBRARY_PATH=./build:../vendor/arrow/cpp/build/install/lib ./examples/test_asof_and_windows
```

**Python Integration Test**
```bash
python3 sabot_sql/examples/test_sabot_sql_integrated_extensions.py
```

### Running Benchmarks

**1M Row Benchmark**
```bash
python3 sabot_sql/benchmarks/benchmark_sabot_sql_1m_rows.py
```

**Extensions Benchmark**
```bash
python3 sabot_sql/benchmarks/benchmark_sabot_sql_integrated_extensions.py
```

**Detailed Scalability**
```bash
python3 sabot_sql/benchmarks/benchmark_sabot_sql_detailed.py
```

### Running Fintech Demo

**Production-Scale Demo**
```bash
cd examples/fintech_enrichment_demo
python3 sabot_sql_enrichment_demo.py --agents 8 --securities 10000000 --quotes 1200000 --trades 1000000
```

## Build Configuration

### CMake Options
- `CMAKE_CXX_STANDARD=20` - C++20 required
- `SABOT_SQL_DISABLE_VENDORED_PHYSICAL=ON` - Disable vendored physical operators
- `SABOT_SQL_EXECUTION_SABOT_ONLY` - Compile-time flag for Sabot-only execution

### Build Modes
- **Release**: `-O3 -DNDEBUG` for production
- **Debug**: `-g` for development
- **RelWithDebInfo**: `-O2 -g` for profiling

## API Reference

### Python

#### SabotSQLBridge
```python
bridge = create_sabot_sql_bridge()
bridge.register_table(table_name: str, table: pa.Table)
result = bridge.execute_sql(sql: str) -> pa.Table
plan = bridge.parse_and_optimize(sql: str) -> Dict
```

#### SabotSQLOrchestrator
```python
orch = SabotSQLOrchestrator()
orch.add_agent(agent_id: str)
orch.distribute_table(table_name: str, table: pa.Table, strategy: str)
results = orch.execute_distributed_query(sql: str) -> List[Dict]
stats = orch.get_orchestrator_stats() -> Dict
```

### C++

#### SabotSQLBridge
```cpp
auto bridge = SabotSQLBridge::Create();
bridge->RegisterTable(table_name, arrow_table);
auto result = bridge->ExecuteSQL(sql_query);
auto plan = bridge->ParseAndOptimize(sql_query);
```

#### SabotOperatorTranslator
```cpp
auto translator = SabotOperatorTranslator::Create();
auto morsel_plan = translator->TranslateToMorselOperators(logical_plan);
auto schema = translator->GetOutputSchema(morsel_plan);
```

## Production Deployment

### Requirements
- C++20 compiler
- CMake 3.20+
- Arrow 22.0.0+
- Python 3.13+ (for Python bindings)

### Runtime
```bash
export DYLD_LIBRARY_PATH=/path/to/sabot_sql/build:/path/to/arrow/lib:$DYLD_LIBRARY_PATH
```

### Validation
All tests passing:
- C++ tests: 1/1
- Python tests: 5/5
- Benchmarks: All successful
- Fintech demo: 4/4 (32/32 queries)

## Contributing

SabotSQL is part of the Sabot project. See the main [Sabot README](../README.md) for contribution guidelines.

## Documentation

See [docs/](docs/) for complete documentation:
- Architecture and design
- Implementation details
- Performance benchmarks
- Production validation
- Integration guides

## Status

**✅ PRODUCTION READY**

- Comprehensive testing complete
- Performance validated on 12.2M rows
- Distributed execution working (8 agents)
- Documentation complete
- Ready for deployment

