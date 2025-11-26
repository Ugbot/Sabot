# Sabot: High-Performance Python Data Processing

**⚠️ EXPERIMENTAL - ALPHA SOFTWARE ⚠️**

**The Python alternative to PySpark and Ray for high-performance columnar data processing.**

Sabot is a Python framework that brings Apache Arrow's columnar performance to data processing workflows. Unlike PySpark's JVM overhead or Ray's distributed complexity, Sabot provides zero-copy Arrow operations with Cython acceleration for massive throughput on single machines.

## 🎯 Unified Architecture (October 2025)

Sabot provides a **unified entry point** for data processing:

```python
from sabot import Sabot

# Create unified engine
engine = Sabot(mode='local')

# Stream processing
stream = engine.stream.from_kafka('topic').filter(lambda b: b.column('x') > 10)

# SQL processing (via DuckDB)
result = engine.sql("SELECT * FROM table WHERE x > 10")

# RDF/SPARQL (functional, basic queries)
results = engine.sparql("SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 10")

# Clean shutdown
engine.shutdown()
```

**See:** [Unified Architecture Guide](docs/architecture/README_UNIFIED_ARCHITECTURE.md)

## Classic API (Still Supported)

```python
from sabot import cyarrow as ca
from sabot.api.stream import Stream
from sabot.cyarrow import compute as pc

# Load 10M rows from Parquet file
data = ca.read_parquet('transactions.parquet')

# Create batch processing pipeline
stream = (Stream.from_table(data, batch_size=100_000)
    # Filter high-value transactions (SIMD-accelerated)
    .filter(lambda batch: pc.greater(batch.column('amount'), 10000))

    # Compute fraud score using auto-compiled Numba UDF
    .map(lambda batch: batch.append_column('fraud_score',
        compute_fraud_score(batch)))  # 10-100x Numba speedup

    # Select output columns
    .select('transaction_id', 'amount', 'fraud_score'))

# Execute: 104M rows/sec hash joins, 10-100x UDF speedup
for batch in stream:
    print(f"Processed {batch.num_rows} rows")

# Same code works for streaming (infinite) sources!
# stream = Stream.from_kafka('transactions')  # Never terminates
```

## Project Status

This is an experimental research project exploring the design space of:
- Zero-copy Arrow columnar processing in Python
- Cython acceleration for data-intensive operations
- High-performance batch processing alternatives
- Kafka streaming integration with columnar efficiency

**Current State (v0.1.0-alpha):**
- ✅ **Arrow Operations**: Zero-copy Arrow columnar processing
- ✅ **SQL Engine**: DuckDB-based SQL execution
- ⚠️ **RDF/SPARQL**: Functional for basic queries, rough around edges
- ⚠️ **Kafka Integration**: Basic source/sink working
- ❌ **Cypher/Graph**: Not functional (parser incomplete)
- ⚠️ Distributed features are experimental
- ⚠️ Test coverage is limited
- ⚠️ Not recommended for production use

## Design Philosophy

Sabot explores Arrow-native data processing in Python:

- **Arrow-first**: All data operations use Apache Arrow columnar format
- **SQL via DuckDB**: Leverage DuckDB's optimized SQL engine
- **Cython acceleration**: Performance-critical paths in Cython
- **Streaming experiments**: Kafka integration for stream processing (experimental)

## Design Goals

**Arrow-Native Processing**
- Zero-copy Arrow operations where possible
- DuckDB for SQL execution
- Cython for performance-critical code

**Streaming (Experimental)**
- Kafka source/sink integration
- Basic stream operators
- State management experiments

**Query Languages**
- SQL via DuckDB (working)
- SPARQL for RDF (basic)
- Cypher for graphs (not working)

## Quick Start

### 1. Install

**Prerequisites:** Python 3.9+, C++ compiler, CMake 3.16+

```bash
# Clone with vendored Arrow C++ submodule
git clone --recursive https://github.com/yourusername/sabot.git
cd sabot

# Option A: Quick install (builds Arrow C++ automatically, ~30-60 mins first time)
pip install -e .

# Option B: Build Arrow C++ manually first (recommended for development)
python build.py          # One-time Arrow build (~30-60 mins)
pip install -e .         # Fast install (<1 min)

# Verify vendored Arrow is working
python -c "from sabot import cyarrow; print(f'Vendored Arrow: {cyarrow.USING_ZERO_COPY}')"

# Note: Use sabot.cyarrow (our optimized Arrow), not pyarrow

# Dependencies Note: We vendor critical Cython dependencies (RocksDB, Arrow components)
# because they are tightly bound to native C/C++ libraries, not standard Python packages.
# This ensures consistent builds and performance across platforms.
```

### 2. Start Infrastructure

```bash
# Start Kafka, Postgres, Redis via Docker
docker compose up -d

# Check services
docker compose ps
```

### 3. Create Your First App

**`fraud_app.py`:**
```python
import sabot as sb
import json

# Create Sabot application
app = sb.App(
    'fraud-detection',
    broker='kafka://localhost:19092',
    value_serializer='json'
)

# Define fraud detector with state
detector = FraudDetector()  # Your fraud detection logic

@app.agent('bank-transactions')
async def detect_fraud(stream):
    """Process transactions and detect fraud patterns."""
    async for message in stream:
        # Deserialize message (stream yields raw bytes/str)
        if isinstance(message, bytes):
            txn = json.loads(message.decode('utf-8'))
        else:
            txn = json.loads(message) if isinstance(message, str) else message

        # Process transaction
        alerts = await detector.detect_fraud(txn)

        # Handle fraud alerts
        for alert in alerts:
            print(f"🚨 FRAUD: {alert['type']} - {alert['details']}")
```

### 4. Run with CLI

```bash
# Start worker (Faust-style)
sabot -A fraud_app:app worker --loglevel=INFO

# Or with concurrency
sabot -A fraud_app:app worker -c 4
```

## Building from Source

Sabot uses a unified build system that automatically builds all dependencies:

```bash
# Clone the repository
git clone https://github.com/sabot/sabot.git
cd sabot

# Initialize submodules (Arrow, Tonbo, etc.)
git submodule update --init --recursive

# Build everything (Arrow C++, vendored extensions, Cython modules)
python build.py

# The build system will:
# 1. Check dependencies (Arrow, RocksDB, Tonbo, Rust, hiredis)
# 2. Build Arrow C++ (20-60 min first time, then cached)
# 3. Build vendor extensions (CyRedis, RocksDB, Tonbo) if available
# 4. Auto-discover and build 56 Cython modules
# 5. Validate builds and report summary
```

**Build Commands:**
```bash
python build.py              # Build everything
python build.py --clean      # Remove .so/.c/.cpp artifacts
python build.py --clean-all  # Also remove Arrow/vendor builds
python build.py --skip-arrow # Don't rebuild Arrow (use existing)
python build.py --skip-vendor # Skip vendor extensions
```

**Dependencies:**
- **Required**: CMake, C++ compiler (clang++/g++), Python 3.11+, Cython, NumPy
- **Optional**: RocksDB (for persistent state), Rust toolchain (for Tonbo), hiredis (for CyRedis)

The build system automatically skips unavailable optional dependencies and reports what was built/skipped.

## Architecture

Sabot combines **Arrow's columnar performance** with **Python's ecosystem**:

```
┌─────────────────────────────────────────────────────────┐
│                    Application Layer                    │
│   @app.agent() decorators, Faust-style API            │
└─────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────┐
│              Sabot Core (Clean API)                     │
│   import sabot as sb                                    │
│   - sb.App, sb.agent()                                  │
│   - sb.Barrier, sb.BarrierTracker (checkpoints)        │
│   - sb.MemoryBackend, sb.ValueState (state)            │
│   - sb.WatermarkTracker, sb.Timers (time)              │
└─────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────┐
│         Cython-Accelerated Modules (10-100x faster)     │
│   - Checkpoint coordination (Chandy-Lamport)            │
│   - State management (RocksDB, memory)                  │
│   - Time/watermark tracking                             │
│   - Arrow batch processing (SIMD-accelerated)          │
└─────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────┐
│                  Infrastructure Layer                   │
│   Kafka, Redpanda | PostgreSQL | Redis | RocksDB       │
└─────────────────────────────────────────────────────────┘
```

### Core Modules

| Module | Description | Status |
|--------|-------------|--------|
| **cyarrow** | Zero-copy Arrow operations | ✅ Working |
| **sql** | DuckDB-based SQL execution | ✅ Working |
| **sparql** | RDF triple store queries | ⚠️ Basic |
| **kafka** | Kafka source/sink | ⚠️ Basic |
| **state** | Memory/RocksDB backends | ⚠️ Experimental |
| **checkpoint** | Distributed snapshots | ⚠️ Experimental |
| **graph/cypher** | Graph pattern matching | ❌ Not working |

### Additional Modules (Experimental)

These modules exist but are experimental or incomplete:

- **Feature Engineering** (`sabot/features/`) - Feature store concepts, not production-ready
- **State Backends** - Memory backend works, RocksDB/Tonbo experimental
- **Shuffle** - Network shuffle infrastructure exists, not fully tested

### Graph Processing (`sabot/_cython/graph/`)

**Status: ❌ Cypher parser incomplete - not functional**

Graph processing modules exist but the Cypher query language parser is incomplete. Basic graph storage structures are in place but query execution does not work.

**What exists (not working end-to-end):**
- Graph storage structures (CSR/CSC adjacency)
- Basic traversal algorithms (BFS, DFS)
- Pattern matching kernels (incomplete)

**What's needed:**
- Complete Cypher parser
- Query planner integration
- End-to-end testing

See [GRAPH_QUERY_ENGINE.md](docs/features/graph/GRAPH_QUERY_ENGINE.md) for architecture documentation.

## Example: Basic Usage

See `examples/00_quickstart/` for simple examples:

```python
from sabot.api.stream import Stream

# Basic stream operations
stream = (Stream.from_list([1, 2, 3, 4, 5])
    .filter(lambda x: x > 2)
    .map(lambda x: x * 2))

for item in stream:
    print(item)
```

For SQL queries:
```python
from sabot_sql import SabotSQL

sql = SabotSQL()
result = sql.query("SELECT * FROM 'data.parquet' WHERE value > 10")
```

For RDF/SPARQL (basic):
```python
from sabot.rdf import RDFStore

store = RDFStore()
store.load("data.nt")
results = store.query("SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 10")
```

## CLI Reference

Sabot provides a Faust-style CLI for production deployments:

```bash
# Start worker
sabot -A myapp:app worker

# With concurrency
sabot -A myapp:app worker -c 4

# Override broker
sabot -A myapp:app worker -b kafka://prod:9092

# Set log level
sabot -A myapp:app worker --loglevel=DEBUG

# Full options
sabot -A myapp:app worker \
  --concurrency 4 \
  --broker kafka://localhost:9092 \
  --loglevel INFO
```

## API Reference

### Creating an App

```python
import sabot as sb

app = sb.App(
    'my-app',
    broker='kafka://localhost:19092',      # Kafka broker URL
    value_serializer='json',                # 'json', 'arrow', 'avro'
    enable_distributed_state=True,          # Use Redis for state
    database_url='postgresql://localhost/sabot'  # For durable execution
)
```

### Defining Agents

```python
@app.agent('my-topic')
async def process_events(stream):
    """Stateful event processor."""
    async for event in stream:
        # Process event
        result = transform(event)
        # Yield to output
        yield result
```

### State Management

```python
# Memory backend (fast, not persistent)
config = sb.BackendConfig(
    backend_type="memory",
    max_size=100000,
    ttl_seconds=300.0
)
backend = sb.MemoryBackend(config)

# RocksDB backend (persistent, larger state)
rocksdb = sb.RocksDBBackend(
    sb.BackendConfig(backend_type="rocksdb", path="./state")
)

# State types
value_state = sb.ValueState(backend, "counter")      # Single value
map_state = sb.MapState(backend, "user_profiles")    # Key-value map
list_state = sb.ListState(backend, "events")         # Ordered list
```

### Checkpointing

```python
# Barrier tracking for distributed checkpoints
tracker = sb.BarrierTracker(num_channels=3)

# Register barrier
aligned = tracker.register_barrier(
    channel=0,
    checkpoint_id=1,
    total_inputs=3
)

# Checkpoint coordinator
coordinator = sb.Coordinator()
```

### Time & Watermarks

```python
# Track watermarks across partitions
watermark_tracker = sb.WatermarkTracker(num_partitions=3)
watermark_tracker.update_watermark(partition_id=0, timestamp=12345)

# Timer service for delayed processing
timers = sb.Timers()
```

## Installation Details

### System Requirements

- **Python**: 3.8+
- **OS**: Linux, macOS (Windows via WSL)
- **Memory**: 4GB+ recommended
- **Dependencies**: See `requirements.txt`

### Optional Dependencies

```bash
# GPU acceleration (RAFT)
pip install cudf cupy raft-dask pylibraft

# Kafka support
pip install confluent-kafka aiokafka

# Redis state backend
pip install redis hiredis

# RocksDB state backend
pip install rocksdb

# All optional features
pip install sabot[all]
```

### Building from Source

```bash
# Install Cython and dependencies
uv pip install cython numpy  # Use sabot.cyarrow, not pyarrow

# Build Cython extensions
python setup.py build_ext --inplace

# Install in development mode
pip install -e .
```

## Docker Compose Infrastructure

The included `docker-compose.yml` provides a complete streaming stack:

```yaml
services:
  redpanda:      # Kafka-compatible broker (port 19092)
  console:       # Redpanda web UI (port 8080)
  postgres:      # PostgreSQL for durable execution (port 5432)
  redis:         # Redis for distributed state (port 6379)
```

**Start all services:**
```bash
docker compose up -d
```

**Access Redpanda Console:**
```bash
open http://localhost:8080
```

**View logs:**
```bash
docker compose logs -f redpanda
```

**Stop all services:**
```bash
docker compose down
```

## Examples

| Example | Description | Status | Location |
|---------|-------------|--------|----------|
| **Quickstart** | Basic stream operations | ✅ Working | `examples/00_quickstart/` |
| **Local Pipelines** | Filter, map, window ops | ✅ Working | `examples/01_local_pipelines/` |
| **SQL Queries** | DuckDB-based SQL | ✅ Working | `examples/api/` |
| **RDF/SPARQL** | Triple store queries | ⚠️ Basic | `examples/sabot_ql_integration/` |
| **Kafka Streaming** | Source/sink integration | ⚠️ Basic | `examples/streaming/` |
| **Fintech Demo** | Data enrichment pipeline | ⚠️ Needs setup | `examples/fintech_enrichment_demo/` |

## Benchmark Results

Performance benchmarks have not been independently verified. Historical benchmark claims in this repository should be treated skeptically.

**What we can say:**
- Arrow IPC loading is faster than CSV (expected)
- DuckDB SQL execution is fast (it's DuckDB)
- Cython modules provide speedup over pure Python

See `docs/benchmarks/` for historical benchmark data (may be outdated).

## Documentation

### Guides
- **[QUICKSTART.md](docs/guides/QUICKSTART.md)** - Getting started guide
- **[GETTING_STARTED.md](docs/guides/GETTING_STARTED.md)** - Detailed setup instructions
- **[DOCUMENTATION.md](docs/guides/DOCUMENTATION.md)** - Full documentation index

### Architecture
- **[ARCHITECTURE.md](docs/architecture/ARCHITECTURE.md)** - System architecture overview
- **[README_UNIFIED_ARCHITECTURE.md](docs/architecture/README_UNIFIED_ARCHITECTURE.md)** - Unified API design
- **[PROJECT_MAP.md](PROJECT_MAP.md)** - Directory structure and module status

### Features
- **[RDF/SPARQL](docs/features/graph/rdf_sparql.md)** - RDF triple store (basic queries working)
- **[SQL Engine](docs/features/sql/)** - DuckDB-based SQL execution
- **[Kafka Integration](docs/features/kafka/)** - Basic Kafka source/sink
- **[Graph/Cypher](docs/features/graph/GRAPH_QUERY_ENGINE.md)** - Architecture docs (not functional)

## Comparison to Other Frameworks

| Feature | Sabot | PySpark | Ray | Apache Flink |
|---------|-------|---------|-----|--------------|
| **Language** | Python | Python (JVM) | Python | Java/Scala |
| **Columnar Processing** | ✅ Arrow-native | ⚠️ Arrow integration | ❌ No | ⚠️ Limited |
| **SQL** | ✅ DuckDB-based | ✅ SparkSQL | ⚠️ Limited | ✅ FlinkSQL |
| **Streaming** | ⚠️ Experimental | ✅ Structured Streaming | ✅ Ray Data | ✅ Production |
| **Graph Processing** | ❌ Not working | ⚠️ GraphX | ❌ No | ⚠️ Gelly |
| **RDF/SPARQL** | ⚠️ Basic | ❌ No | ❌ No | ❌ No |
| **Setup Complexity** | ✅ Simple | 🐌 JVM + cluster | 🐌 Distributed | 🐌 Cluster |
| **Production Ready** | ❌ Alpha | ✅ Yes | ✅ Yes | ✅ Yes |

## Roadmap

### Current Status (v0.1.0-alpha)

**Working:**
- ✅ Arrow columnar operations
- ✅ SQL via DuckDB integration
- ✅ Basic stream operators (filter, map, window)

**Partially Working:**
- ⚠️ RDF/SPARQL - basic queries work, needs polish
- ⚠️ Kafka integration - basic source/sink
- ⚠️ State backends - memory works, RocksDB experimental

**Not Working:**
- ❌ Cypher/Graph queries - parser incomplete
- ❌ Distributed execution - infrastructure only
- ❌ Production streaming - experimental only

### Needs Work
- Complete Cypher parser
- Polish SPARQL implementation
- Improve test coverage
- Better error handling
- Documentation updates

## Contributing

Sabot is an experimental research project and welcomes contributions! This is a learning-focused project exploring streaming architecture design.

**High-Impact Areas:**
1. **Testing**: Expand test coverage beyond current ~5%
2. **Agent Runtime**: Complete the execution layer implementation
3. **RocksDB Integration**: Finish state backend implementation
4. **Documentation**: Improve guides and examples
5. **Benchmarking**: Add comprehensive performance tests

**Before Contributing:**
- This is alpha software with many incomplete features
- Focus on learning and experimentation rather than production readiness
- Check existing issues and roadmap before starting major work
- Add tests for any new functionality

See GitHub issues for contribution opportunities.

## License

GNU Affero General Public License v3.0 (AGPL-3.0) - See [LICENSE](LICENSE) file for details.

## Credits

Inspired by:
- **Apache Arrow** - Zero-copy columnar data processing
- **PySpark** - DataFrame API and distributed processing concepts
- **Ray** - Actor model and distributed computing patterns
- **Faust** - Python streaming framework, CLI design, and agent patterns

Built with:
- **Cython** - High-performance compiled modules for Arrow acceleration
- **PyArrow** - SIMD-accelerated compute kernels and memory management
- **Redpanda** - Kafka-compatible streaming infrastructure
- **PostgreSQL** - Durable execution (DBOS-inspired)
- **RocksDB** - Embedded key-value store for state management

## Support

- **Issues**: [GitHub Issues](https://github.com/yourusername/sabot/issues)
- **Discussions**: [GitHub Discussions](https://github.com/yourusername/sabot/discussions)
- **Email**: team@sabot.io

---

## When to Choose Sabot

**Sabot might be useful if:**
- You want Arrow-native columnar processing in Python
- You need SQL queries via DuckDB
- You're experimenting with RDF/SPARQL in Python
- You want a simpler alternative to PySpark for local processing

**Choose PySpark/Ray/Flink when:**
- You need production-ready distributed processing
- You need battle-tested streaming
- You need reliable graph processing

**This is experimental alpha software.** Many features are incomplete or non-functional. Use for experimentation and learning, not production.

**Getting Started:**
- See `examples/00_quickstart/` for basic usage
- See `examples/sabot_ql_integration/` for RDF/SPARQL examples
