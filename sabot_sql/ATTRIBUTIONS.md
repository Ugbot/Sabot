# SabotSQL - Credits and Attributions

## Overview

SabotSQL is a hybrid SQL engine combining the best of multiple open-source projects:

## Primary Components

### 1. DuckDB (Forked - MIT License)

**Source**: https://github.com/duckdb/duckdb  
**License**: MIT License  
**Usage**: Parser, Binder, Planner, Optimizer

**Vendored Components:**
- `sabot_sql/vendored/sabot_sql_core/` contains DuckDB's:
  - SQL parser (tokenizer, grammar, AST construction)
  - Binder (semantic analysis, type resolution)
  - Logical planner (query plan construction)
  - Optimizer (filter pushdown, join reordering, etc.)
  - Common utilities (types, vectors, serialization)

**Modifications:**
- Upgraded from C++11 to C++20
- Disabled all physical execution operators (`SABOT_SQL_DISABLE_VENDORED_PHYSICAL=ON`)
- Extended binder with time-series SQL rewrites
- Added custom logical plan metadata for Sabot execution hints
- Renamed symbols to `sabot_sql::` namespace where needed

**NOT Used:**
- DuckDB's physical operators (hash join, aggregate, etc.)
- DuckDB's execution engine
- DuckDB's storage layer
- DuckDB's transaction system

### 2. QuestDB (Inspired - Apache 2.0 License)

**Source**: https://github.com/questdb/questdb  
**License**: Apache License 2.0  
**Usage**: Time-series SQL syntax inspiration

**Features Inspired:**
- `ASOF JOIN` - Time-series aligned joins with time inequality
- `SAMPLE BY interval` - Time-based sampling and aggregation
- `LATEST BY key` - Get most recent record per partition key

**Implementation:**
- Custom SabotSQL binder rewrites (not QuestDB code)
- Custom Sabot operator pipelines for execution
- Syntax semantics matching QuestDB where possible

**NOT Used:**
- No QuestDB source code
- No QuestDB binary dependencies
- Independent implementation

### 3. Apache Flink (Inspired - Apache 2.0 License)

**Source**: https://github.com/apache/flink  
**License**: Apache License 2.0  
**Usage**: Window function syntax inspiration

**Features Inspired:**
- `TUMBLE(time_col, INTERVAL)` - Tumbling time windows
- `HOP(time_col, step, size)` - Sliding/hopping windows
- `SESSION(time_col, gap)` - Session windows with timeout
- `CURRENT_TIMESTAMP` function normalization

**Implementation:**
- Custom SabotSQL binder rewrites (not Flink code)
- Custom Sabot operator pipelines for execution
- Syntax semantics matching Flink SQL where possible

**NOT Used:**
- No Flink source code
- No Flink binary dependencies
- Independent implementation

### 4. Sabot (Original - Apache 2.0 License)

**Source**: https://github.com/bengamble/Sabot (this repository)  
**License**: Apache License 2.0  
**Usage**: Complete physical execution engine

**Components:**
- Morsel-driven parallel execution
- Arrow-based zero-copy data flow (cyarrow)
- Hash partitioning and shuffle coordination
- Distributed agent orchestration
- All physical operators (joins, aggregations, filters, etc.)

**SabotSQL Execution:**
- `CythonHashJoinOperator` - For joins (including time-aware ASOF)
- `CythonGroupByOperator` - For aggregations and windows
- `ShuffleOperator` - For distributed repartitioning
- `MorselDrivenOperator` - For parallel execution
- Zero-copy Arrow data flow throughout

## Architecture Summary

```
┌─────────────────────────────────────────────┐
│  SQL Query (Standard/QuestDB/Flink syntax)  │
└──────────────────┬──────────────────────────┘
                   │
┌──────────────────▼──────────────────────────┐
│  DuckDB Parser (vendored, MIT License)      │
│  - Tokenizer, grammar, AST                  │
└──────────────────┬──────────────────────────┘
                   │
┌──────────────────▼──────────────────────────┐
│  DuckDB Binder (vendored + extended)        │
│  + SabotSQL Rewrites (custom)               │
│  - ASOF → LEFT JOIN + hints                 │
│  - SAMPLE BY → GROUP BY DATE_TRUNC          │
│  - LATEST BY → ORDER BY DESC LIMIT 1        │
└──────────────────┬──────────────────────────┘
                   │
┌──────────────────▼──────────────────────────┐
│  DuckDB Planner (vendored, MIT License)     │
│  - Logical plan construction                │
└──────────────────┬──────────────────────────┘
                   │
┌──────────────────▼──────────────────────────┐
│  DuckDB Optimizer (vendored, MIT License)   │
│  - Filter pushdown, join reordering         │
└──────────────────┬──────────────────────────┘
                   │
┌──────────────────▼──────────────────────────┐
│  SabotSQL Translator (custom)               │
│  - DuckDB logical → Sabot physical          │
│  - Build operator descriptors with params   │
└──────────────────┬──────────────────────────┘
                   │
┌──────────────────▼──────────────────────────┐
│  Sabot Execution (Apache 2.0)               │
│  - Morsel-driven parallel execution         │
│  - Arrow zero-copy data flow                │
│  - Distributed shuffle coordination         │
└──────────────────┬──────────────────────────┘
                   │
               Arrow Table Result
```

## Code Attribution

### Vendored DuckDB Code
Location: `sabot_sql/vendored/sabot_sql_core/`

**DuckDB Components** (MIT License):
- `sql_common/` - Common types, vectors, utilities
- `sql_parser/` - SQL parser and tokenizer
- `sql_planner/` - Logical plan construction
- `sql_optimizer/` - Query optimization passes
- `catalog/` - Schema and type catalog
- `function/` - Scalar and aggregate function registry

**License**: See `vendored/sabot_sql_core/LICENSE` (DuckDB MIT License)

### Custom SabotSQL Code
Location: `sabot_sql/src/sql/`, `sabot_sql/include/sabot_sql/`

**SabotSQL Components** (Apache 2.0):
- `binder_rewrites.{h,cpp}` - Time-series SQL detection and rewriting
- `sabot_operator_translator.{h,cpp}` - DuckDB logical → Sabot physical mapping
- `sabot_sql_bridge.{h,cpp}` - Main entry point and coordination
- `common_types.h` - Extended metadata for Sabot execution
- `enums.h` - Time-series join and window types

**License**: Apache License 2.0 (Sabot license)

### Sabot Execution Engine
Location: `sabot/_cython/operators/`, `sabot/_cython/shuffle/`

**Sabot Components** (Apache 2.0):
- All physical operators (joins, aggregations, filters, etc.)
- Morsel-driven parallel execution
- Arrow-based shuffle and partitioning
- Distributed agent coordination

**License**: Apache License 2.0 (Sabot license)

## License Compliance

### DuckDB (MIT)
```
Copyright (c) DuckDB Labs
Permission is hereby granted, free of charge, to any person obtaining a copy...
```

Full DuckDB license: `sabot_sql/vendored/sabot_sql_core/LICENSE`

### SabotSQL Custom Code (Apache 2.0)
```
Copyright (c) Sabot Project
Licensed under the Apache License, Version 2.0...
```

Full Sabot license: `LICENSE` (repository root)

## Acknowledgments

We gratefully acknowledge:

- **DuckDB Team**: For creating an excellent SQL parser/optimizer that we forked
- **QuestDB Team**: For pioneering time-series SQL syntax (ASOF/SAMPLE BY/LATEST BY)
- **Apache Flink Team**: For defining streaming SQL window semantics (TUMBLE/HOP/SESSION)
- **Apache Arrow Team**: For the columnar data format that powers our execution

## Disclaimer

SabotSQL is:
- An **independent fork** of DuckDB's parser/planner (MIT License - allows forks)
- **Inspired by** QuestDB and Flink syntax (no code used, independent implementation)
- **Not affiliated with** DuckDB Labs, QuestDB, or Apache Flink
- **Compliant with** all applicable open-source licenses

## Questions?

- **DuckDB usage**: Legal under MIT License; parser/planner only, no execution
- **QuestDB/Flink inspiration**: Syntax semantics only, independent implementation
- **License compatibility**: MIT (DuckDB) + Apache 2.0 (Sabot) = compatible
- **Attribution**: Clearly documented here and in README

For license questions, see individual LICENSE files in:
- `sabot_sql/vendored/sabot_sql_core/LICENSE` (DuckDB MIT)
- `LICENSE` (Sabot Apache 2.0)

