# Sabot ClickBench Benchmark

This directory contains a ClickBench benchmark implementation for Sabot SQL with distributed execution across multiple nodes.

## Overview

ClickBench is a standard benchmark for analytical databases that tests various SQL operations including:
- Aggregations (COUNT, SUM, AVG, MIN, MAX)
- Grouping and ordering
- String operations
- Date/time functions
- Complex joins and subqueries

The Sabot implementation supports:
- **Distributed execution** across multiple agents/nodes
- **Parquet file loading** with automatic type conversion
- **Performance metrics** collection and reporting
- **Multiple execution modes** (local, local_parallel, distributed)

## Files

- `sabot_clickbench.py` - Main benchmark runner
- `distributed_executor.py` - Distributed SQL execution engine
- `queries.sql` - ClickBench SQL queries (44 queries)
- `query.py` - Original DuckDB-based benchmark (for comparison)
- `README.md` - This documentation

## Usage

### Basic Usage

```bash
# Run with distributed execution (4 agents)
python sabot_clickbench.py --parquet-file hits.parquet --num-agents 4

# Run with local parallel execution
python sabot_clickbench.py --parquet-file hits.parquet --execution-mode local_parallel

# Run with custom parameters
python sabot_clickbench.py \
    --parquet-file hits.parquet \
    --num-agents 8 \
    --execution-mode distributed \
    --morsel-size-kb 128 \
    --benchmark-runs 5 \
    --output-file results.json
```

### Command Line Options

- `--parquet-file`: Path to hits.parquet file (required)
- `--num-agents`: Number of distributed agents (default: 4)
- `--execution-mode`: Execution mode - local, local_parallel, or distributed (default: distributed)
- `--morsel-size-kb`: Morsel size for parallel execution (default: 64)
- `--warmup-runs`: Number of warmup runs per query (default: 1)
- `--benchmark-runs`: Number of benchmark runs per query (default: 3)
- `--output-file`: Output file for results (JSON format)
- `--verbose`: Enable verbose logging

### Example Output

```
SABOT CLICKBENCH RESULTS
================================================================================

Query 1: 0.123s avg
  Times: ['0.120', '0.125', '0.124']
  SQL: SELECT COUNT(*) FROM hits...
  Rows: 1

Query 2: 0.456s avg
  Times: ['0.450', '0.460', '0.458']
  SQL: SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0...
  Rows: 1

...

================================================================================
SUMMARY:
  Total queries: 44
  Total time: 15.234s
  Average per query: 0.346s
  Execution mode: distributed
  Agents: 4
================================================================================

DISTRIBUTED EXECUTOR PERFORMANCE STATS
============================================================
Total queries executed: 44
Total execution time: 15.234s
Average time per query: 0.346s

Agent Performance:
  Agent 0:
    Queries: 44
    Total time: 3.812s
    Avg time: 0.087s
    Min time: 0.045s
    Max time: 0.234s
  Agent 1:
    Queries: 44
    Total time: 3.789s
    Avg time: 0.086s
    Min time: 0.043s
    Max time: 0.231s
  ...
============================================================
```

## Architecture

### Distributed Execution

The benchmark uses Sabot's distributed agent system:

1. **Data Distribution**: The hits table is partitioned across agents using round-robin
2. **Query Analysis**: Queries are analyzed to determine execution strategy:
   - `local_only`: Execute locally on each agent
   - `requires_aggregation`: Aggregate results across agents
   - `requires_shuffle`: Redistribute data between agents
3. **Parallel Execution**: Queries run in parallel across all agents
4. **Result Combination**: Results are combined using Arrow table concatenation

### Execution Modes

- **Local**: Single-threaded execution on one agent
- **Local Parallel**: Multi-threaded execution with morsel parallelism
- **Distributed**: Execution across multiple agents/nodes

### Performance Metrics

The benchmark collects detailed performance metrics:

- **Query-level metrics**: Execution time, row counts, min/max/avg times
- **Agent-level metrics**: Per-agent performance statistics
- **System-level metrics**: Total execution time, throughput
- **Distributed metrics**: Shuffle times, aggregation times

## Data Requirements

The benchmark expects a `hits.parquet` file with the following schema:

- `EventTime`: Timestamp (seconds since epoch)
- `EventDate`: Date (days since epoch)
- `UserID`: User identifier
- `CounterID`: Counter identifier
- `AdvEngineID`: Advertisement engine ID
- `SearchPhrase`: Search query string
- `URL`: Page URL
- `Title`: Page title
- `Referer`: Referrer URL
- `ResolutionWidth`: Screen width
- `ResolutionHeight`: Screen height
- `WindowClientWidth`: Window width
- `WindowClientHeight`: Window height
- `MobilePhone`: Mobile phone model
- `MobilePhoneModel`: Mobile phone model details
- `RegionID`: Geographic region
- `ClientIP`: Client IP address
- `WatchID`: Watch identifier
- `TraficSourceID`: Traffic source ID
- `SearchEngineID`: Search engine ID
- `URLHash`: URL hash
- `RefererHash`: Referrer hash
- `IsRefresh`: Refresh flag
- `IsLink`: Link flag
- `IsDownload`: Download flag
- `DontCountHits`: Don't count hits flag

## Comparison with Original

The original `query.py` uses DuckDB for execution. The Sabot version provides:

- **Distributed execution** across multiple nodes
- **Better scalability** for large datasets
- **Advanced optimization** with Sabot's morsel operators
- **Time-series features** (ASOF JOIN, SAMPLE BY, etc.)
- **Streaming capabilities** for real-time processing

## Performance Tuning

### Agent Configuration

- **Number of agents**: Should match available CPU cores
- **Morsel size**: Larger morsels reduce overhead but increase memory usage
- **Partitioning strategy**: Round-robin works well for most queries

### Query Optimization

- **Aggregation queries**: Benefit from distributed execution
- **Join queries**: May require data shuffling
- **Ordering queries**: Can be optimized with distributed sorting

### Memory Management

- **Table caching**: Tables are cached in memory for fast access
- **Result streaming**: Large results are streamed to avoid memory issues
- **Garbage collection**: Automatic cleanup of intermediate results

## Troubleshooting

### Common Issues

1. **Import errors**: Ensure Sabot SQL is properly installed
2. **Memory issues**: Reduce number of agents or morsel size
3. **Performance issues**: Check agent distribution and network latency
4. **Data loading errors**: Verify parquet file format and schema

### Debug Mode

Enable verbose logging for debugging:

```bash
python sabot_clickbench.py --parquet-file hits.parquet --verbose
```

### Performance Analysis

Use the output JSON file for detailed analysis:

```python
import json

with open('results.json') as f:
    results = json.load(f)

# Analyze performance trends
for query in results['results']:
    print(f"Query {query['query_id']}: {query['avg_time']:.3f}s")
```

## Future Enhancements

- **Dynamic partitioning**: Adaptive partitioning based on query patterns
- **Query caching**: Cache frequently used query results
- **Streaming execution**: Real-time query execution
- **Advanced optimizations**: Query plan optimization and cost estimation
- **Monitoring**: Real-time performance monitoring and alerting

