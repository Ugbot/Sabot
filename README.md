# Sabot: Python Streaming with Flink-Like Semantics

**⚠️ EXPERIMENTAL - ALPHA SOFTWARE ⚠️**

**If Faust is Kafka Streams in Python, Sabot aims to be Flink in Python.**

Sabot is an experimental streaming framework exploring Flink-inspired stream processing in Python with Cython acceleration. This project is in active development and not yet production-ready.

```python
import sabot as sb

# Create app with Kafka
app = sb.App('fraud-detection', broker='kafka://localhost:19092')

# Define streaming agent
@app.agent('transactions')
async def detect_fraud(stream):
    async for transaction in stream:
        if is_fraudulent(transaction):
            yield alert

# Deploy with CLI
# $ sabot -A myapp:app worker
```

## Project Status

This is an experimental research project exploring the design space of:
- Flink-style stream processing semantics in Python
- Cython acceleration for performance-critical paths
- Chandy-Lamport distributed checkpointing
- Arrow-based columnar processing

**Current State (v0.1.0-alpha):**
- ✅ Core architecture designed and documented (~60K LOC)
- ✅ Cython modules for checkpoint coordination, state management, time tracking
- ✅ Basic Kafka integration with schema registry support
- ✅ Faust-style CLI scaffolding
- ⚠️ Many components are work-in-progress or stubbed out
- ⚠️ Test coverage is limited (~5%)
- ⚠️ Not recommended for production use

## Measured Performance (Local Benchmarks)

**What Actually Works:**
- **Throughput**: 3,000-6,000 transactions/second (fraud detection benchmark, M1 Pro)
- **Checkpoint initiation**: <10μs (Cython barrier coordination)
- **State operations**: Sub-millisecond get/put with MemoryBackend
- **Memory footprint**: <500MB for multi-agent fraud detection demo

**Experimental Features (In Development):**
- Distributed agent runtime
- RocksDB state backend integration
- Arrow batch processing optimizations
- GPU acceleration via RAFT

## Design Goals

🚀 **Flink-Inspired Architecture**
- Event-time processing with watermarks
- Exactly-once semantics via distributed checkpointing
- Complex event processing (CEP) with pattern matching
- Iterative stream processing

⚡ **Performance Through Cython**
- Cython-accelerated checkpoint coordination
- Fast state backends (Memory, RocksDB)
- Watermark and timer tracking in C
- Arrow integration for columnar operations

🔧 **Pythonic API**
- Unified imports: `import sabot as sb`
- Decorator-based agents: `@app.agent()`
- Composable stream pipelines
- Faust-style CLI for familiarity

## Quick Start

### 1. Install

```bash
# Clone and install
git clone https://github.com/yourusername/sabot.git
cd sabot
pip install -e .

# Or from PyPI (coming soon)
pip install sabot
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

# Create Sabot application
app = sb.App(
    'fraud-detection',
    broker='kafka://localhost:19092'
)

# Define fraud detector with state
detector_state = sb.MemoryBackend(
    sb.BackendConfig(backend_type="memory")
)

@app.agent('bank-transactions')
async def detect_fraud(stream):
    """Process transactions and detect fraud patterns."""
    async for transaction in stream:
        # Check for suspicious patterns
        if transaction['amount'] > 10000:
            yield {
                'alert_type': 'high_amount',
                'transaction_id': transaction['id'],
                'amount': transaction['amount']
            }
```

### 4. Run with CLI

```bash
# Start worker (Faust-style)
sabot -A fraud_app:app worker --loglevel=INFO

# Or with concurrency
sabot -A fraud_app:app worker -c 4
```

## Architecture

Sabot combines **Flink's streaming model** with **Python's ecosystem**:

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
│   - Arrow batch processing                              │
└─────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────┐
│                  Infrastructure Layer                   │
│   Kafka, Redpanda | PostgreSQL | Redis | RocksDB       │
└─────────────────────────────────────────────────────────┘
```

### Core Modules

| Module | Description | Performance |
|--------|-------------|-------------|
| **checkpoint** | Distributed snapshots (Chandy-Lamport) | <10μs initiation |
| **state** | Managed state (Memory, RocksDB, Redis) | 1M+ ops/sec |
| **time** | Watermarks, timers, event-time | <5μs tracking |
| **agents** | Actor-based stream processors | 5K-10K txn/s |

## Example: Fraud Detection Demo

See the [Fraud Detection Demo](examples/FRAUD_DEMO_README.md) for an example processing banking transactions.

**Three-terminal setup:**

```bash
# Terminal 1: Infrastructure
docker compose up -d

# Terminal 2: Sabot Worker
sabot -A examples.fraud_app:app worker

# Terminal 3: Data Producer
python examples/flink_fraud_producer.py
```

**What this demonstrates:**
- Multi-pattern fraud detection logic
- Basic checkpointing coordination
- Event-time processing concepts
- Memory-backed state management
- Real-time metrics collection

**Measured results (M1 Pro laptop):**
- **Throughput**: 3,000-6,000 transactions/second
- **Latency**: <1ms p99 for fraud detection logic
- **Memory**: <500MB for 3 concurrent agents
- **Checkpoint coordination**: Sub-10μs barrier initiation

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
pip install cython numpy pyarrow

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

| Example | Description | Location |
|---------|-------------|----------|
| **Fraud Detection** | Real-time fraud detection on 200K transactions | `examples/fraud_app.py` |
| **Windowed Analytics** | Tumbling/sliding windows with aggregations | `examples/streaming/windowed_analytics.py` |
| **Multi-Agent Coordination** | Coordinated processing across multiple agents | `examples/streaming/multi_agent_coordination.py` |
| **Arrow Operations** | Zero-copy columnar processing | `examples/data/arrow_operations.py` |

## Benchmark Results

**Fraud Detection Demo (M1 Pro, local Kafka):**
- **Throughput**: 3,000-6,000 transactions/second
- **Latency p99**: <1ms per transaction
- **Memory**: <500MB for 3 concurrent agents
- **Checkpoint barrier initiation**: <10μs (Cython coordinator)

**State Backend Operations (MemoryBackend):**
- **Get/Put latency**: Sub-millisecond
- **Sustained throughput**: 1M+ operations/second (Cython implementation)

**Notes on Benchmarks:**
- Measured on consumer-grade hardware (M1 Pro, 16GB RAM)
- Local Redpanda broker (no network latency)
- Simple fraud detection patterns (no external API calls)
- Memory backend only (RocksDB integration experimental)
- Results may vary significantly with different workloads

## Documentation

- **[Project Map](PROJECT_MAP.md)** - Directory structure and module overview
- **[Getting Started Guide](docs/GETTING_STARTED.md)** - Step-by-step tutorial
- **[API Reference](docs/API_REFERENCE.md)** - API documentation
- **[Architecture](docs/ARCHITECTURE.md)** - Deep dive into internals
- **[Fraud Demo README](examples/FRAUD_DEMO_README.md)** - Example walkthrough
- **[CLI Guide](docs/CLI.md)** - Command-line reference

## Comparison to Other Frameworks

| Feature | Sabot | Faust | Apache Flink | Kafka Streams |
|---------|-------|-------|--------------|---------------|
| **Language** | Python | Python | Java/Scala | Java |
| **Maturity** | ⚠️ Alpha | ✅ Stable | ✅ Production | ✅ Production |
| **CLI Deployment** | 🚧 In Progress | ✅ Yes | ❌ No | ❌ No |
| **Checkpointing** | 🚧 Chandy-Lamport (experimental) | ⚠️ Basic | ✅ Async barriers | ✅ Log-based |
| **Event Time** | 🚧 Partial support | ⚠️ Limited | ✅ Full support | ✅ Full support |
| **State Backends** | 🚧 Memory (working), RocksDB (WIP) | ⚠️ RocksDB only | ✅ Multiple | ✅ RocksDB |
| **Performance** | ⚡ Cython-accelerated (partial) | 🐌 Pure Python | ⚡⚡ JVM | ⚡⚡ JVM |
| **Arrow/Columnar** | 🚧 Experimental | ❌ No | ⚠️ Limited | ❌ No |
| **Production Ready** | ❌ No | ✅ Yes | ✅ Yes | ✅ Yes |

## Roadmap

### Current Status (v0.1.0-alpha)
**Working:**
- ✅ Cython checkpoint coordinator (Chandy-Lamport barriers)
- ✅ Memory state backend with Cython acceleration
- ✅ Basic Kafka source/sink with schema registry
- ✅ Watermark tracking primitives
- ✅ CLI scaffolding (Faust-style)
- ✅ Fraud detection demo (3K-6K txn/s)

**In Progress:**
- 🚧 Agent runtime execution layer (partially stubbed)
- 🚧 RocksDB state backend integration
- 🚧 Arrow batch processing optimizations
- 🚧 Distributed coordination

**Known Limitations:**
- ⚠️ Test coverage ~5% (not production-safe)
- ⚠️ Many components are stubs/work-in-progress
- ⚠️ CLI uses mock implementations in places
- ⚠️ Limited error handling and recovery testing

### Planned (v0.2.0)
- 🎯 Complete agent runtime implementation
- 🎯 Comprehensive integration tests
- 🎯 RocksDB state backend completion
- 🎯 Improved error handling and recovery
- 🎯 Performance benchmarking suite
- 🎯 Production-ready checkpointing

### Future Ideas (v0.3.0+)
- 📋 GPU acceleration via RAFT
- 📋 Advanced CEP patterns
- 📋 SQL/Table API
- 📋 Web UI for monitoring
- 📋 S3/HDFS connectors
- 📋 Query optimizer

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

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines (if available).

## License

Apache License 2.0 - See [LICENSE](LICENSE) file for details.

## Credits

Inspired by:
- **Apache Flink** - Streaming architecture and semantics
- **Kafka Faust** - Python API and CLI design
- **Ray** - Distributed actor model
- **Apache Arrow** - Columnar data processing

Built with:
- **Cython** - High-performance compiled modules
- **Redpanda** - Kafka-compatible streaming
- **PostgreSQL** - Durable execution (DBOS-inspired)
- **RocksDB** - Embedded key-value store

## Support

- **Issues**: [GitHub Issues](https://github.com/yourusername/sabot/issues)
- **Discussions**: [GitHub Discussions](https://github.com/yourusername/sabot/discussions)
- **Email**: team@sabot.io

---

## Disclaimer

**This is experimental alpha software.** It is not production-ready and should be used for research, learning, and experimentation only. APIs may change, features may be incomplete, and bugs are expected. We welcome feedback and contributions to help improve the project.

**Ready to experiment?** Check out the [Project Map](PROJECT_MAP.md) to understand the codebase structure, then try the [Fraud Detection Demo](examples/FRAUD_DEMO_README.md)!
