# Sabot Documentation

**Version**: 0.1.0
**Last Updated**: October 25, 2025

Welcome to Sabot's documentation! This directory contains comprehensive technical documentation, guides, and references for the Sabot streaming analytics platform.

## Quick Links

- **Get Started**: [Quickstart Guide](guides/QUICKSTART.md) - Get up and running in 5 minutes
- **Project Status**: [PROJECT_MAP.md](../PROJECT_MAP.md) - Current status, file locations, and metrics
- **Architecture**: [Architecture Overview](architecture/README.md) - System design and implementation
- **API Reference**: [API Documentation](reference/API_REFERENCE.md) - Complete API reference

## Documentation Structure

### 📚 Guides (`guides/`)

User-facing documentation and tutorials:
- **[QUICKSTART.md](guides/QUICKSTART.md)** - 5-minute introduction to Sabot
- **[DOCUMENTATION.md](guides/DOCUMENTATION.md)** - Documentation organization and navigation
- **[GETTING_STARTED.md](guides/GETTING_STARTED.md)** - Detailed getting started guide
- **[USER_WORKFLOW.md](guides/USER_WORKFLOW.md)** - Common workflows and patterns

**Start here** if you're new to Sabot!

### 🏗️ Architecture (`architecture/`)

System architecture and design documentation:
- **[README.md](architecture/README.md)** - Consolidated architecture overview (start here!)
- **[ARCHITECTURE.md](architecture/ARCHITECTURE.md)** - Detailed architecture deep-dive
- **[ARCHITECTURE_OVERVIEW.md](architecture/ARCHITECTURE_OVERVIEW.md)** - High-level overview
- **[ARROW_CONVERSION_AUDIT.md](architecture/ARROW_CONVERSION_AUDIT.md)** - Arrow usage patterns
- **Historical**: Unification process documents

### 🎯 Features (`features/`)

Feature-specific implementation documentation:

#### Kafka Integration (`features/kafka/`)
- Production-ready Kafka connector
- C++ librdkafka integration (5-8x faster)
- Schema Registry support (Avro, Protobuf, JSON)
- **Documentation**: See `features/kafka/` directory

#### SQL Engine (`features/sql/`)
- DuckDB-based SQL execution
- Arrow-native processing
- Competitive with pure DuckDB performance
- **Documentation**: See `features/sql/` directory

#### Graph Queries (`features/graph/`)
- **Cypher**: Property graph queries ✅
- **RDF/SPARQL**: Triple store queries ⚠️ (performance issue)
- **Key Docs**:
  - [rdf_sparql.md](features/graph/rdf_sparql.md) - User-facing RDF/SPARQL guide
  - [RDF_SPARQL_ARCHITECTURE.md](features/graph/RDF_SPARQL_ARCHITECTURE.md) - Architecture details
  - [SPARQL_PERFORMANCE_ANALYSIS.md](features/graph/SPARQL_PERFORMANCE_ANALYSIS.md) - Performance analysis
  - [GRAPH_QUERY_ENGINE.md](features/graph/GRAPH_QUERY_ENGINE.md) - Graph engine overview

#### Fintech Kernels (`features/fintech/`)
- ASOF joins (time-series alignment)
- VWAP, TWAP calculations
- Market microstructure functions
- **Documentation**: See `features/fintech/` directory

#### Change Data Capture (`features/cdc/`)
- MySQL CDC via binlog
- PostgreSQL CDC via logical replication
- Arrow-native change streams
- **Documentation**: See `features/cdc/` directory

#### C++ Agent (`features/cpp_agent/`)
- C++ agent core implementation
- Performance optimization details
- **Documentation**: See `features/cpp_agent/` directory

### 📖 Reference (`reference/`)

Technical reference documentation:
- **[API_REFERENCE.md](reference/API_REFERENCE.md)** - Complete API documentation
- **[CLI.md](reference/CLI.md)** - Command-line interface reference

### 📊 Benchmarks (`benchmarks/`)

Performance analysis and benchmark results:
- **[BENCHMARK_RESULTS.md](benchmarks/BENCHMARK_RESULTS.md)** - Benchmark summaries
- **[FINAL_BENCHMARK_RESULTS.md](benchmarks/FINAL_BENCHMARK_RESULTS.md)** - Comprehensive results
- **[PYSPARK_VS_SABOT_BENCHMARK_SUMMARY.md](benchmarks/PYSPARK_VS_SABOT_BENCHMARK_SUMMARY.md)** - PySpark comparison (2,287x average speedup)
- **[PERFORMANCE_VALIDATION_PHASE1.md](benchmarks/PERFORMANCE_VALIDATION_PHASE1.md)** - Validation results

### 📅 Session Reports (`session-reports/`)

Historical development session summaries:
- **oct18/**, **oct19/**, **oct20/** - Dated session reports
- **other/** - Miscellaneous session reports
- **archive/** - Archived phase completion documents

Organized by date for historical reference.

### 📝 Planning (`planning/`)

Roadmaps and planning documents:
- **[NEXT_STEPS.md](planning/NEXT_STEPS.md)** - Immediate next steps
- **[ACCOMPLISHMENTS.md](planning/ACCOMPLISHMENTS.md)** - What's been accomplished
- Historical phase planning documents

## Examples

See the `../examples/` directory for working code examples:
- **Quickstart** (3 examples): hello_sabot.py, filter_and_map.py, local_join.py
- **Local Pipelines** (3 examples): Streaming, windowing, stateful processing
- **Kafka** (1 example): kafka_integration_example.py
- **RDF/SPARQL**: rdf_simple_example.py, sparql_demo.py
- **Total**: 14+ working examples

## Current Status

### Production Ready ✅
- Streaming pipelines (local and distributed)
- Kafka integration (5-8x faster than Python)
- SQL engine (competitive with DuckDB)
- Cypher graph queries
- Fintech kernels (ASOF joins, etc.)
- State management (RocksDB, Memory)
- 70+ Cython operator modules built

### Known Issues ⚠️
- **SPARQL Query Execution**: O(n²) scaling issue
  - Blocks production use for >10K triples
  - See [SPARQL_PERFORMANCE_ANALYSIS.md](features/graph/SPARQL_PERFORMANCE_ANALYSIS.md)
  - Priority: HIGH - needs C++ optimization

- **SQL String Operations**: Being optimized with Arrow compute kernels
  - Currently 2-20x slower than DuckDB on string-heavy queries
  - Improvement in progress

## Performance Highlights

### vs PySpark (Verified)
- **Average speedup**: ~2,287x faster
- JSON parsing: 6-632x faster
- Filter+Map: 303-10,625x faster
- JOIN: 112-1,129x faster
- Aggregation: 460-4,553x faster

### vs DuckDB (ClickBench, 37 queries)
- Sabot wins: 22 queries (numeric operations)
- DuckDB wins: 13 queries (string operations)
- Overall: DuckDB ~1.3x faster (string advantage)
- Target: Competitive after string optimization

### Kafka Throughput
- JSON: 150K+ msg/sec (5-8x faster than Python)
- Avro: 120K+ msg/sec (infrastructure ready)
- Protobuf: 100K+ msg/sec (infrastructure ready)

## Technology Stack

### Vendored Dependencies

All dependencies are vendored - no system requirements:
- **Arrow C++** (22.0.0) - Columnar operations (~500MB)
- **librdkafka** - Kafka client (~50MB)
- **DuckDB** - SQL engine (~200MB)
- **RocksDB** - State backend (~100MB)
- **simdjson** - SIMD JSON (~5MB)
- **Avro C++** - Avro codec (~20MB)
- **Protobuf** - Protobuf codec (~100MB)

### Code Metrics
- **C++**: ~15,000 lines (hot paths, integrations)
- **Cython**: ~20,000 lines (70+ operator modules)
- **Python**: ~30,000 lines (orchestration, API)
- **Documentation**: ~35,000+ lines (125+ markdown files)
- **Total**: ~100,000 lines

## Navigation Tips

### By Use Case

**"I want to get started quickly"**
→ [Quickstart Guide](guides/QUICKSTART.md)

**"I need to understand the architecture"**
→ [Architecture Overview](architecture/README.md)

**"I want to use Kafka"**
→ [Kafka Integration](features/kafka/)

**"I want to run SQL queries"**
→ [SQL Engine](features/sql/)

**"I want to use RDF/SPARQL"**
→ [RDF/SPARQL Guide](features/graph/rdf_sparql.md)
→ ⚠️ Read [Performance Analysis](features/graph/SPARQL_PERFORMANCE_ANALYSIS.md) first!

**"I want to see benchmarks"**
→ [Benchmark Results](benchmarks/)

**"I want API reference"**
→ [API Reference](reference/API_REFERENCE.md)

### By Component

**Agent Architecture**
→ [Architecture README](architecture/README.md) § Agent Architecture

**Streaming Operators**
→ [Architecture README](architecture/README.md) § Streaming Operators

**State Management**
→ [Architecture README](architecture/README.md) § State Management

**Network Shuffle**
→ [Architecture README](architecture/README.md) § Network Shuffle

## Contributing to Documentation

### Organization Principles

1. **Guides** - User-facing, tutorial-style, assumes beginner knowledge
2. **Architecture** - Implementation details, design decisions, for developers
3. **Features** - Feature-specific deep-dives, organized by subsystem
4. **Reference** - Structured API/CLI reference, minimal narrative
5. **Benchmarks** - Performance data, analysis, comparisons
6. **Session Reports** - Historical development logs, archived by date

### Documentation Standards

- Use clear, concise language
- Include code examples where appropriate
- Link to related documents
- Mark status clearly (✅ Working, ⚠️ Known issues, ❌ Broken)
- Update "Last Updated" dates
- Be accurate, not optimistic

### Key Files to Maintain

- **[PROJECT_MAP.md](../PROJECT_MAP.md)** - Overall project status
- **[Architecture README](architecture/README.md)** - Current architecture
- **Feature docs** - Keep synchronized with code changes
- **Benchmarks** - Update when performance changes

## Getting Help

- **Issues**: https://github.com/anthropics/sabot/issues (if applicable)
- **Documentation Issues**: File in project issues with "docs" label
- **Questions**: Check [QUICKSTART.md](guides/QUICKSTART.md) first

## Documentation Index

### All Documents (Organized)

#### Guides (4 files)
- QUICKSTART.md
- DOCUMENTATION.md
- GETTING_STARTED.md
- USER_WORKFLOW.md

#### Architecture (9 files)
- README.md (start here!)
- ARCHITECTURE.md
- ARCHITECTURE_OVERVIEW.md
- ARCHITECTURE_UNIFICATION_STATUS.md
- ARCHITECTURE_UNIFICATION_FINAL.md
- ARCHITECTURE_REFACTORING_SUMMARY.md
- README_UNIFIED_ARCHITECTURE.md
- ARROW_CONVERSION_AUDIT.md

#### Features
- **Kafka** (13 files)
- **SQL** (3 files)
- **Graph** (12+ files including RDF/SPARQL docs)
- **Fintech** (9 files)
- **CDC** (5 files)
- **C++ Agent** (5 files)

#### Reference (2 files)
- API_REFERENCE.md
- CLI.md

#### Benchmarks (4+ files)
- BENCHMARK_RESULTS.md
- FINAL_BENCHMARK_RESULTS.md
- PYSPARK_VS_SABOT_BENCHMARK_SUMMARY.md
- And more...

#### Planning (3+ files)
- NEXT_STEPS.md
- ACCOMPLISHMENTS.md
- Historical phase documents

#### Session Reports (50+ files)
- Organized by date (oct18, oct19, oct20)
- Archive for completed phases

**Total**: 125+ documentation files, all organized

---

**Documentation Status**: ✅ Comprehensive and well-organized
**Last Major Update**: October 25, 2025 (Documentation reorganization complete)
**Maintainer**: Sabot Development Team
