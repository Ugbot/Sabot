# Sabot Examples

This directory contains practical examples demonstrating Sabot's capabilities, organized by functionality. All examples use the new **CLI-compatible agent model** with `@app.agent()` decorators.

## 🎯 Quick Start (3 Steps)

1. **Start Infrastructure:**
   ```bash
   docker compose up -d
   ```

2. **Run a Sabot Worker:**
   ```bash
   # Basic example
   sabot -A examples.core.basic_pipeline:app worker

   # Multi-agent coordination
   sabot -A examples.streaming.multi_agent_coordination:app worker

   # Fraud detection
   sabot -A examples.fraud_app:app worker
   ```

3. **Send Test Data** (separate terminal):
   ```bash
   # For basic_pipeline
   python examples/core/basic_pipeline.py  # Shows producer code

   # For fraud detection
   python examples/flink_fraud_producer.py
   ```

## 📁 Example Organization

Examples are organized into folders based on their primary functionality:

- **`api/`** - **NEW!** Modern Stream API examples (recommended)
- **`core/`** - Getting started and basic concepts
- **`streaming/`** - Stream processing and agent patterns
- **`data/`** - Data transformations and joins
- **`storage/`** - Persistence and state management
- **`cluster/`** - Distributed and cluster operations
- **`advanced/`** - Advanced features (GPU, specialized hardware)
- **`cli/`** - Command-line interface demonstrations

## 🏃 Running Examples

### CLI-Compatible Examples (Recommended)

These examples use the `@app.agent()` decorator and can be run with the CLI:

```bash
# Format: sabot -A <module.path>:app worker
sabot -A examples.core.basic_pipeline:app worker
sabot -A examples.streaming.windowed_analytics:app worker
sabot -A examples.streaming.multi_agent_coordination:app worker
sabot -A examples.fraud_app:app worker
```

**Benefits:**
- ✅ Production-ready deployment pattern
- ✅ Automatic Kafka consumer management
- ✅ Built-in monitoring and health checks
- ✅ Easy scaling with `--concurrency` flag

### Standalone Examples

Some examples run standalone for demonstration:

```bash
python examples/core/simplified_demo.py  # No Kafka required
```

## 🚀 Featured Examples

### 🎭 **No Installation Required**

#### [`core/simplified_demo.py`](core/simplified_demo.py)
**What it demonstrates:** All core Sabot concepts in a standalone demo
- Streaming pipelines and transformations
- Stateful agent processing
- Arrow columnar operations
- Windowed analytics
- Real-time data processing patterns

**Perfect for:** First-time users who want to understand Sabot concepts quickly

```bash
python examples/core/simplified_demo.py  # Works immediately!
```

#### [`cli/sabot_cli_demo.py`](cli/sabot_cli_demo.py)
**What it demonstrates:** Faust-style CLI for agent management and supervision
- Agent lifecycle management (start, stop, restart, scale)
- Real-time supervision with auto-restart on failure
- Live monitoring and health checks
- Worker process management
- Cluster status and overview

**Perfect for:** Understanding Faust-style agent management and CLI operations

```bash
# Cluster status
python examples/cli/sabot_cli_demo.py status

# List agents and workers
python examples/cli/sabot_cli_demo.py agents list
python examples/cli/sabot_cli_demo.py workers list

# Supervise agents (Faust-style)
python examples/cli/sabot_cli_demo.py agents supervise

# Monitor agents in real-time
python examples/cli/sabot_cli_demo.py agents monitor --alerts
```

---

## 📚 Full Examples by Category

### 🚀 **API Examples** (Modern Stream API - Recommended)

The new high-level Stream API provides a declarative, composable way to build streaming pipelines with Flink-level performance (0.5ns per row when Cython compiled).

#### [`api/basic_streaming.py`](api/basic_streaming.py) ⭐ **Start Here**
**What it demonstrates:** Basic stream operations with the modern API
- Filtering with SIMD-accelerated Arrow compute
- Map transformations (temperature conversions)
- Chained operations (filter → map → aggregate)
- Per-sensor analytics
- Declarative, zero-copy processing

```bash
python examples/api/basic_streaming.py
```

**Why use this:** Clean, declarative API vs imperative async agents. Same concepts as `core/basic_pipeline.py` but simpler and faster.

#### [`api/windowed_aggregations.py`](api/windowed_aggregations.py)
**What it demonstrates:** Time-based windowing with the Window API
- Tumbling windows (non-overlapping, fixed intervals)
- Sliding windows (overlapping, rolling calculations)
- Session windows (gap-based, dynamic)
- Real-time dashboard metrics
- Per-endpoint window analytics

```bash
python examples/api/windowed_aggregations.py
```

**Why use this:** Declarative window specifications vs manual window management. Same concepts as `streaming/windowed_analytics.py`.

#### [`api/arrow_transforms.py`](api/arrow_transforms.py)
**What it demonstrates:** Zero-copy Arrow operations and columnar processing
- SIMD-accelerated filtering (10-100x faster than Python loops)
- Stream-table joins with Arrow
- Arrow compute kernels
- Multi-table joins
- Zero-copy slicing and memory efficiency

```bash
python examples/api/arrow_transforms.py
```

**Why use this:** Shows full power of Arrow columnar format. Same concepts as `data/arrow_operations.py` but with Stream API.

#### [`api/stateful_joins.py`](api/stateful_joins.py)
**What it demonstrates:** Stateful processing and join patterns with State API
- Stream-table joins using ValueState
- Per-user event counters with ValueState
- ListState for event accumulation
- MapState for feature stores
- Stream-stream joins
- Real-time fraud detection

```bash
python examples/api/stateful_joins.py
```

**Why use this:** Clean state abstractions (ValueState, ListState, MapState) vs manual state management. Same concepts as `data/joins_demo.py`.

#### [`api/stream_processing_demo.py`](api/stream_processing_demo.py)
**What it demonstrates:** Comprehensive API demo with 7 examples
- All API features in one place
- Performance validation (100K rows)
- Real-world use cases

```bash
python examples/api/stream_processing_demo.py
```

**Quick Start:** See [`api/API_QUICKSTART.md`](api/API_QUICKSTART.md) for installation and API reference.

**API Benefits:**
- ✅ Declarative, composable pipelines
- ✅ Zero-copy performance (0.5ns/row vs 2000ns/row)
- ✅ Type-safe with Arrow schemas
- ✅ Automatic batching optimization
- ✅ Clean state management (ValueState, ListState, MapState)
- ✅ SIMD-accelerated operations

---

### 🏠 **Core Examples**

#### [`core/basic_pipeline.py`](core/basic_pipeline.py) ✅ CLI-Compatible
**What it demonstrates:** Simple streaming pipeline with filtering and mapping
- Creating a Sabot app with `@app.agent()` decorator
- Processing sensor data streams
- Filtering and transforming data
- Real-time temperature alerts

```bash
# Start worker
sabot -A examples.core.basic_pipeline:app worker

# Send test data (see file for producer code)
python examples/core/basic_pipeline.py  # Shows producer
```

---

### 🌊 **Streaming Examples**

#### [`streaming/windowed_analytics.py`](streaming/windowed_analytics.py) ✅ CLI-Compatible
**What it demonstrates:** Windowed analytics with tumbling and sliding windows
- Tumbling windows (10-second intervals)
- Sliding windows (30s window, 10s slide)
- Per-endpoint analytics
- Real-time dashboard metrics

```bash
# Start worker (runs 3 agents simultaneously)
sabot -A examples.streaming.windowed_analytics:app worker

# Send test data (see file for producer code)
```

#### [`streaming/multi_agent_coordination.py`](streaming/multi_agent_coordination.py) ✅ CLI-Compatible
**What it demonstrates:** Multi-agent coordination and distributed processing
- Four agents working together in a pipeline:
  1. Data Ingestion
  2. Validation
  3. Enrichment
  4. Analytics
- Inter-agent communication via Kafka
- Real-time processing stats and coordination

```bash
# Start worker (all 4 agents run together)
sabot -A examples.streaming.multi_agent_coordination:app worker

# Send test data (see file for producer code)
```

#### [`streaming/windowed_analytics.py`](streaming/windowed_analytics.py)
**What it demonstrates:** Time-windowed analytics
- Tumbling windows (periodic aggregations)
- Sliding windows (rolling calculations)
- Real-time dashboard metrics
- Time-based data processing

```bash
python examples/streaming/windowed_analytics.py
```

#### [`streaming/windowing_demo.py`](streaming/windowing_demo.py)
**What it demonstrates:** Advanced windowing techniques
- Hopping windows
- Session windows
- Custom window functions
- Complex time-based aggregations

```bash
python examples/streaming/windowing_demo.py
```

---

### 📊 **Data Processing Examples**

#### [`data/arrow_operations.py`](data/arrow_operations.py)
**What it demonstrates:** Arrow-native columnar operations
- Arrow table creation and manipulation
- Native Arrow joins (SIMD accelerated)
- Zero-copy data operations
- Columnar data processing

```bash
python examples/data/arrow_operations.py
```

#### [`data/arrow_joins_demo.py`](data/arrow_joins_demo.py)
**What it demonstrates:** Arrow-based join operations
- Arrow Table.join() operations
- Arrow Dataset.join() operations
- As-of joins with Arrow
- Performance comparisons

```bash
python examples/data/arrow_joins_demo.py
```

#### [`data/flink_joins_demo.py`](data/flink_joins_demo.py)
**What it demonstrates:** Flink-style join patterns
- Stream-table joins
- Stream-stream joins
- Interval joins
- Temporal joins

```bash
python examples/data/flink_joins_demo.py
```

#### [`data/joins_demo.py`](data/joins_demo.py)
**What it demonstrates:** Comprehensive join operations
- All join types supported by Sabot
- Performance benchmarks
- Memory usage analysis
- Real-world join patterns

```bash
python examples/data/joins_demo.py
```

---

### 💾 **Storage Examples**

#### [`storage/materialized_views.py`](storage/materialized_views.py)
**What it demonstrates:** Real-time materialized views
- RocksDB-backed persistent views
- Automatic view maintenance
- Live query capabilities
- State management across restarts

```bash
python examples/storage/materialized_views.py
```

#### [`storage/debezium_materialized_views.py`](storage/debezium_materialized_views.py)
**What it demonstrates:** Debezium-powered change data capture
- CDC event processing
- Real-time view updates
- Database change streaming
- Materialized view maintenance

```bash
python examples/storage/debezium_materialized_views.py
```

#### [`storage/pluggable_backends.py`](storage/pluggable_backends.py)
**What it demonstrates:** Pluggable storage backends
- Multiple storage options
- Backend switching
- Performance comparisons
- Custom storage implementations

```bash
python examples/storage/pluggable_backends.py
```

---

### ☁️ **Cluster Examples**

#### [`cluster/cluster_distributed_agents.py`](cluster/cluster_distributed_agents.py)
**What it demonstrates:** Distributed agent capabilities
- Multi-node agent deployment
- Cross-node communication
- Load balancing
- Fault tolerance

```bash
python examples/cluster/cluster_distributed_agents.py
```

#### [`cluster/cluster_dbos_parallelism.py`](cluster/cluster_dbos_parallelism.py)
**What it demonstrates:** DBOS orchestration of workers/agents
- Intelligent task distribution
- Resource management
- Parallel processing coordination
- Cluster-level optimization

```bash
python examples/cluster/cluster_dbos_parallelism.py
```

#### [`cluster/cluster_fault_tolerance.py`](cluster/cluster_fault_tolerance.py)
**What it demonstrates:** Fault tolerance and recovery
- Agent failure simulation
- Automatic recovery
- State persistence
- High availability patterns

```bash
python examples/cluster/cluster_fault_tolerance.py
```

#### [`cluster/durable_agents.py`](cluster/durable_agents.py)
**What it demonstrates:** Durable agent execution
- Transactional agent state
- Recovery from failures
- Deterministic processing
- Persistent agent coordination

```bash
python examples/cluster/durable_agents.py
```

#### [`kafka_bridge_demo.py`](../kafka_bridge_demo.py)
**What it demonstrates:** Kafka as bridge between jobs/systems and source/sink
- Job-to-job communication via Kafka
- External system integration
- Kafka + Tonbo complementary usage
- End-to-end streaming pipelines
- Performance comparison between backends

**Perfect for:** Understanding when to use Kafka vs Tonbo in your architecture

```bash
# Requires Kafka running on localhost:9092
python examples/kafka_bridge_demo.py
```

---

### ⚡ **Advanced Examples**

#### [`advanced/gpu_accelerated.py`](advanced/gpu_accelerated.py)
**What it demonstrates:** GPU-accelerated ML on streaming data
- RAFT-powered GPU acceleration
- Real-time anomaly detection
- K-means clustering on GPU
- Performance comparison (CPU vs GPU)

```bash
# Requires CUDA and RAPIDS AI
python examples/advanced/gpu_accelerated.py
```

#### [`advanced/gpu_raft_clustering.py`](advanced/gpu_raft_clustering.py)
**What it demonstrates:** GPU-accelerated clustering algorithms
- RAFT clustering on GPU
- Real-time cluster analysis
- Streaming data clustering
- GPU memory management

```bash
python examples/advanced/gpu_raft_clustering.py
```

#### [`advanced/gpu_raft_neighbors.py`](advanced/gpu_raft_neighbors.py)
**What it demonstrates:** GPU-accelerated nearest neighbors
- RAFT k-nearest neighbors on GPU
- Real-time similarity search
- Streaming neighbor analysis
- Performance optimization

```bash
python examples/advanced/gpu_raft_neighbors.py
```

---

### 💻 **CLI Examples**

#### [`cli/sabot_cli.py`](cli/sabot_cli.py)
**What it demonstrates:** Command-line interface usage
- CLI command structure
- Script automation
- Batch processing
- Administrative tasks

```bash
python examples/cli/sabot_cli.py --help
```

## 🎯 Learning Path

### 🚀 Recommended Path (Modern Stream API)

**For New Users - Start Here:**
1. ⭐ **Modern API basics** → [`api/basic_streaming.py`](api/basic_streaming.py) - Declarative streaming
2. **Windowing** → [`api/windowed_aggregations.py`](api/windowed_aggregations.py) - Time-based analytics
3. **Arrow operations** → [`api/arrow_transforms.py`](api/arrow_transforms.py) - Zero-copy processing
4. **Stateful processing** → [`api/stateful_joins.py`](api/stateful_joins.py) - State & joins

**For Intermediate Users:**
5. **Comprehensive demo** → [`api/stream_processing_demo.py`](api/stream_processing_demo.py) - All features
6. **Compare approaches** → Compare API examples with their agent-based equivalents
7. **Kafka integration** → Use `Stream.from_kafka()` and `stream.to_kafka()`

### 📚 Alternative Path (Agent-Based API)

**For New Users:**
1. **Start here** → [`core/simplified_demo.py`](core/simplified_demo.py) - All concepts in one demo
2. **CLI basics** → [`cli/sabot_cli_demo.py`](cli/sabot_cli_demo.py) - Learn Faust-style CLI
3. **First pipeline** → [`core/basic_pipeline.py`](core/basic_pipeline.py) - Simple streaming
4. **Agent processing** → [`streaming/agent_processing.py`](streaming/agent_processing.py) - Stateful agents

**For Intermediate Users:**
5. **Data operations** → [`data/arrow_operations.py`](data/arrow_operations.py) - Arrow columnar processing
6. **Windowed analytics** → [`streaming/windowed_analytics.py`](streaming/windowed_analytics.py) - Time-based aggregations
7. **Multi-agent coordination** → [`streaming/multi_agent_coordination.py`](streaming/multi_agent_coordination.py) - Agent collaboration

**For Advanced Users:**
8. **Storage & persistence** → [`storage/materialized_views.py`](storage/materialized_views.py) - Real-time views
9. **Join operations** → [`data/joins_demo.py`](data/joins_demo.py) - All join types
10. **Cluster deployment** → [`cluster/cluster_distributed_agents.py`](cluster/cluster_distributed_agents.py) - Distributed processing

### 🤔 Which Path Should I Choose?

**Use Stream API (Recommended) if:**
- ✅ You want simple, declarative pipelines
- ✅ You need maximum performance (0.5ns/row)
- ✅ You prefer functional/composable style
- ✅ You're building new applications

**Use Agent API if:**
- ✅ You need Kafka consumer management
- ✅ You want Faust-style async agents
- ✅ You're migrating from Faust
- ✅ You need CLI deployment (`sabot worker`)

## Prerequisites

### Basic Requirements
- Python 3.8+
- pip package manager

### Full Sabot Installation (Recommended)
```bash
# Install from source (development mode)
cd /path/to/sabot
pip install -e .

# Or install specific features
pip install -e ".[kafka]"     # With Kafka support
pip install -e ".[gpu]"       # With GPU acceleration
pip install -e ".[all]"       # All optional dependencies
```

### GPU Example Requirements
For GPU-accelerated examples in `advanced/`, you'll need:
- NVIDIA GPU with CUDA support
- CUDA Toolkit 11.8+
- RAPIDS AI libraries: `cudf`, `cuml`, `raft-dask`
- Full Sabot installation with GPU support

## Running Examples

### With Full Sabot (Recommended)
```bash
# Activate virtual environment (if using one)
# source /path/to/sabot/.venv/bin/activate

# Run examples from their folders
python examples/core/basic_pipeline.py
python examples/streaming/agent_processing.py
python examples/data/arrow_operations.py
# ... etc
```

### Without Full Sabot (Fallback Mode)
All examples include automatic fallback to simulation mode if Sabot isn't fully installed:

```bash
# These examples work immediately without installation
python examples/core/simplified_demo.py      # ✅ Always works
python examples/cli/sabot_cli_demo.py status # ✅ CLI demo works

# Other examples will automatically use simulation mode
python examples/core/basic_pipeline.py       # 🔄 Runs with simulation
```

When running in simulation mode, examples will:
- ✅ Show "🔄 Running with simulation" message
- ✅ Demonstrate core concepts and patterns
- ✅ Generate realistic sample data
- ⚠️ Use mock classes instead of real Sabot components

## 📝 Contributing Examples

When adding new examples:

1. **Choose the right folder** based on the example's primary focus
2. **Include simulation fallback** for users without full Sabot installation
3. **Add comprehensive docstrings** explaining what the example demonstrates
4. **Update this README** with the new example
5. **Test both modes**: full Sabot and simulation mode

### Example Template

```python
#!/usr/bin/env python3
"""
Example: [Brief description]

This example demonstrates:
- Feature 1
- Feature 2
- Use case

Requirements:
- Sabot with [optional features]
"""

import sys
import os

# Add examples directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

try:
    import sabot as sb
    SIMULATION_MODE = False
except ImportError:
    # Fallback for demonstration
    print("🔄 Running with simulation - install Sabot for full functionality")
    SIMULATION_MODE = True

    class MockApp:
        # Mock implementation for demo

def main():
    if SIMULATION_MODE:
        # Simulation implementation
        print("📊 Simulating [feature]...")
        # Demo logic here
    else:
        # Real Sabot implementation
        app = sb.create_app(id="example-app")
        # Real logic here

if __name__ == "__main__":
    main()
```

## 📄 License

All examples are part of the Sabot project and follow the same Apache 2.0 license.

**Ready to explore?** Pick an example and start experimenting! 🚀
