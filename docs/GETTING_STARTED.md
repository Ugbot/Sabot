# Getting Started with Sabot

This guide will walk you through setting up Sabot and building your first streaming application.

## Prerequisites

- **Python 3.8+** installed
- **Docker** and Docker Compose installed
- **4GB+ RAM** available
- **macOS or Linux** (Windows via WSL2)

## Installation

### Step 1: Clone and Install

```bash
# Clone the repository
git clone https://github.com/yourusername/sabot.git
cd sabot

# Create virtual environment (recommended)
python3 -m venv .venv
source .venv/bin/activate

# Install Sabot with dependencies
pip install -e .
```

### Step 2: Build Cython Extensions

```bash
# Build high-performance Cython modules
python setup.py build_ext --inplace

# Verify installation
python -c "import sabot as sb; print('✅ Sabot installed:', sb.__version__)"
```

### Step 3: Start Infrastructure

```bash
# Start Kafka, Postgres, Redis via Docker Compose
docker compose up -d

# Wait for services to be healthy (30-60 seconds)
docker compose ps

# You should see all services as "running (healthy)"
```

### Step 4: Verify Infrastructure

```bash
# Check Redpanda (Kafka)
docker compose logs redpanda | grep "successfully started"

# Access Redpanda Console
open http://localhost:8080

# Test Kafka connectivity
python -c "from confluent_kafka import Producer; print('✅ Kafka client works')"
```

## Your First Streaming App

Let's build a simple word count application.

### Create `wordcount_app.py`

```python
import sabot as sb

# Create Sabot application
app = sb.App(
    'wordcount',
    broker='kafka://localhost:19092',
    value_serializer='json'
)

# Word counter state
word_counts = {}

@app.agent('sentences')
async def count_words(stream):
    """Count words from incoming sentences."""
    global word_counts

    async for sentence in stream:
        # Split sentence into words
        words = sentence.get('text', '').lower().split()

        # Update counts
        for word in words:
            word_counts[word] = word_counts.get(word, 0) + 1

        # Yield top 10 words
        top_words = sorted(
            word_counts.items(),
            key=lambda x: x[1],
            reverse=True
        )[:10]

        yield {
            'top_words': dict(top_words),
            'total_words': len(word_counts)
        }
```

### Run with CLI

```bash
# Terminal 1: Start the Sabot worker
sabot -A wordcount_app:app worker --loglevel=INFO
```

### Send Test Data

```bash
# Terminal 2: Send test sentences to Kafka
python -c "
from confluent_kafka import Producer
import json

producer = Producer({'bootstrap.servers': 'localhost:19092'})

sentences = [
    {'text': 'hello world from sabot'},
    {'text': 'hello streaming world'},
    {'text': 'sabot is streaming in python'},
]

for sentence in sentences:
    producer.produce(
        'sentences',
        value=json.dumps(sentence).encode('utf-8')
    )
    producer.flush()
    print(f'✅ Sent: {sentence}')
"
```

You should see word counts being processed in Terminal 1!

## Adding State Management

Let's enhance our app with proper state management:

```python
import sabot as sb

app = sb.App('wordcount', broker='kafka://localhost:19092')

# Create persistent state backend
config = sb.BackendConfig(
    backend_type="memory",
    max_size=100000,
    ttl_seconds=3600.0  # 1 hour TTL
)
state_backend = sb.MemoryBackend(config)

@app.agent('sentences')
async def count_words(stream):
    """Count words with proper state management."""

    async for sentence in stream:
        words = sentence.get('text', '').lower().split()

        for word in words:
            # Use state backend for persistence
            count_key = f"word:{word}"
            current_count = await state_backend.get(count_key) or 0
            await state_backend.set(count_key, current_count + 1)

        yield {'message': f'Processed {len(words)} words'}
```

## Real-World Example: Fraud Detection

Now let's build something more realistic. See the complete [Fraud Detection Demo](../examples/FRAUD_DEMO_README.md).

### Key Concepts Demonstrated

1. **Multi-pattern detection** - Velocity, amount anomaly, geographic
2. **Stateful processing** - Track account history and statistics
3. **Distributed checkpointing** - Exactly-once semantics
4. **Event-time processing** - Watermarks and late data handling

### Quick Setup

```bash
# Terminal 1: Infrastructure
docker compose up -d

# Terminal 2: Sabot Worker
sabot -A examples.fraud_app:app worker

# Terminal 3: Data Producer
python examples/flink_fraud_producer.py
```

Watch real-time fraud alerts appear in Terminal 2!

## Understanding the CLI

### Basic Commands

```bash
# Start worker
sabot -A myapp:app worker

# With concurrency (4 workers)
sabot -A myapp:app worker -c 4

# Override broker
sabot -A myapp:app worker -b kafka://prod:9092

# Set log level
sabot -A myapp:app worker --loglevel=DEBUG
```

### CLI Options

| Flag | Description | Example |
|------|-------------|---------|
| `-A, --app` | App module path | `-A examples.fraud_app:app` |
| `-c, --concurrency` | Number of workers | `-c 4` |
| `-b, --broker` | Override broker URL | `-b kafka://prod:9092` |
| `-l, --loglevel` | Log level | `-l DEBUG` |

## Working with State

### Memory Backend (Fast, Non-Persistent)

```python
import sabot as sb

# Configure memory backend
config = sb.BackendConfig(
    backend_type="memory",
    max_size=100000,      # Max entries
    ttl_seconds=300.0      # 5 minute TTL
)
backend = sb.MemoryBackend(config)

# Store and retrieve
await backend.set("user:123", {"name": "Alice", "score": 100})
user_data = await backend.get("user:123")
```

### RocksDB Backend (Persistent)

```python
# Configure RocksDB (for larger state)
rocksdb_config = sb.BackendConfig(
    backend_type="rocksdb",
    path="./state/rocksdb"
)
rocksdb = sb.RocksDBBackend(rocksdb_config)

# Same API as memory backend
await rocksdb.set("key", "value")
value = await rocksdb.get("key")
```

### State Types

```python
# Value state (single value)
value_state = sb.ValueState(backend, "counter")
await value_state.update(42)
count = await value_state.value()

# Map state (key-value mapping)
map_state = sb.MapState(backend, "user_profiles")
await map_state.put("user:123", {"name": "Alice"})
profile = await map_state.get("user:123")

# List state (ordered list)
list_state = sb.ListState(backend, "events")
await list_state.add({"event": "login", "user": "alice"})
events = await list_state.get()
```

## Checkpointing & Fault Tolerance

Sabot provides distributed checkpointing for exactly-once semantics:

```python
import sabot as sb

# Create barrier tracker (for distributed coordination)
tracker = sb.BarrierTracker(num_channels=3)

# In your agent
@app.agent('transactions')
async def process_with_checkpoints(stream):
    checkpoint_id = 0

    async for transaction in stream:
        # Process transaction
        result = process(transaction)

        # Trigger checkpoint every 1000 messages
        if transaction['id'] % 1000 == 0:
            checkpoint_id += 1

            # Register barrier
            aligned = tracker.register_barrier(
                channel=0,
                checkpoint_id=checkpoint_id,
                total_inputs=3
            )

            if aligned:
                print(f"✅ Checkpoint {checkpoint_id} complete")

        yield result
```

## Next Steps

### 1. Explore Examples

- **[Fraud Detection](../examples/fraud_app.py)** - Production-ready example
- **[Windowed Analytics](../examples/streaming/windowed_analytics.py)** - Time windows
- **[Multi-Agent](../examples/streaming/multi_agent_coordination.py)** - Agent coordination

### 2. Read Documentation

- **[API Reference](API_REFERENCE.md)** - Complete API docs
- **[Architecture](ARCHITECTURE.md)** - Deep dive into internals
- **[CLI Guide](CLI.md)** - Command-line reference

### 3. Try Advanced Features

```python
# GPU acceleration (if available)
app = sb.App('ml-pipeline', enable_gpu=True)

# Redis distributed state
app = sb.App('app',
    enable_distributed_state=True,
    redis_host='localhost',
    redis_port=6379
)

# PostgreSQL durable execution
app = sb.App('app',
    database_url='postgresql://localhost/sabot'
)
```

## Troubleshooting

### "Kafka connection refused"

```bash
# Check if Redpanda is running
docker compose ps

# View logs
docker compose logs redpanda

# Restart if needed
docker compose restart redpanda
```

### "Module not found: confluent_kafka"

```bash
# Install Kafka client
pip install confluent-kafka
```

### "Cannot import name 'X' from sabot"

```bash
# Rebuild Cython extensions
python setup.py build_ext --inplace

# Reinstall
pip install -e .
```

### "Port 19092 already in use"

```bash
# Stop existing Kafka/Redpanda
docker compose down

# Check what's using the port
lsof -i :19092

# Kill if needed
kill -9 <PID>
```

## Getting Help

- **Documentation**: See [docs/](.)
- **Examples**: See [examples/](../examples)
- **Issues**: [GitHub Issues](https://github.com/yourusername/sabot/issues)
- **Discussions**: [GitHub Discussions](https://github.com/yourusername/sabot/discussions)

---

**Next**: Try the [Fraud Detection Demo](../examples/FRAUD_DEMO_README.md) for a production-ready example!
