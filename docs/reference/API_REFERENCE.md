# Sabot API Reference

Complete API documentation for Sabot's clean Python API.

## Import Convention

```python
import sabot as sb
```

All examples use this convention. You can also import specific classes:

```python
from sabot import App, MemoryBackend, BackendConfig
```

---

## Core Application

### `sb.App`

Create a Sabot streaming application.

**Signature:**
```python
sb.App(
    id: str,
    *,
    broker: Optional[str] = None,
    value_serializer: str = "arrow",
    key_serializer: str = "raw",
    enable_gpu: bool = False,
    gpu_device: int = 0,
    redis_host: str = "localhost",
    redis_port: int = 6379,
    enable_distributed_state: bool = True,
    database_url: str = "postgresql://localhost/sabot"
) -> App
```

**Parameters:**
- `id` (str): Unique application identifier
- `broker` (str, optional): Kafka broker URL (e.g., `kafka://localhost:19092`)
- `value_serializer` (str): Serialization format: `'json'`, `'arrow'`, `'avro'`
- `key_serializer` (str): Key serialization: `'raw'`, `'json'`
- `enable_gpu` (bool): Enable GPU acceleration (requires RAFT)
- `redis_host` (str): Redis host for distributed state
- `redis_port` (int): Redis port
- `enable_distributed_state` (bool): Use Redis for state
- `database_url` (str): PostgreSQL URL for durable execution

**Example:**
```python
# Basic app
app = sb.App('my-app', broker='kafka://localhost:19092')

# With PostgreSQL for durable execution
app = sb.App(
    'fraud-detection',
    broker='kafka://localhost:19092',
    database_url='postgresql://localhost/sabot'
)

# With Redis distributed state
app = sb.App(
    'distributed-app',
    broker='kafka://localhost:19092',
    enable_distributed_state=True,
    redis_host='redis.prod',
    redis_port=6379
)
```

---

## Agents

### `@app.agent()`

Decorator to define a streaming agent (stateful processor).

**Signature:**
```python
@app.agent(
    topic: Union[str, TopicT],
    *,
    name: Optional[str] = None,
    concurrency: int = 1,
    max_concurrency: int = 10,
    enabled: bool = True
) -> Callable
```

**Parameters:**
- `topic` (str | Topic): Kafka topic name or Topic object
- `name` (str, optional): Agent name (defaults to function name)
- `concurrency` (int): Number of concurrent processors
- `max_concurrency` (int): Maximum concurrency limit
- `enabled` (bool): Whether agent is enabled

**Example:**
```python
@app.agent('transactions')
async def process_transactions(stream):
    """Process transaction stream."""
    async for transaction in stream:
        # Process transaction
        result = validate(transaction)
        # Yield result (optional)
        yield result

# With options
@app.agent('events', concurrency=4, name='event_processor')
async def process_events(stream):
    async for event in stream:
        yield process(event)
```

---

## State Management

### `sb.BackendConfig`

Configuration for state backends.

**Signature:**
```python
sb.BackendConfig(
    backend_type: str,
    path: Optional[str] = None,
    max_size: int = 1000000,
    ttl_seconds: float = 0.0,
    compression: str = "none"
)
```

**Parameters:**
- `backend_type` (str): Backend type: `'memory'`, `'rocksdb'`
- `path` (str, optional): Path for persistent backends (RocksDB)
- `max_size` (int): Maximum number of entries
- `ttl_seconds` (float): Time-to-live in seconds (0 = no expiration)
- `compression` (str): Compression: `'none'`, `'snappy'`, `'lz4'`

**Example:**
```python
# Memory backend (fast, not persistent)
memory_config = sb.BackendConfig(
    backend_type="memory",
    max_size=100000,
    ttl_seconds=300.0  # 5 minutes
)

# RocksDB backend (persistent)
rocksdb_config = sb.BackendConfig(
    backend_type="rocksdb",
    path="./state/rocksdb",
    compression="snappy"
)
```

### `sb.MemoryBackend`

High-performance in-memory state backend (Cython-accelerated).

**Signature:**
```python
sb.MemoryBackend(config: BackendConfig)
```

**Methods:**
- `async get(key: str) -> Any`: Get value by key
- `async set(key: str, value: Any)`: Set key-value pair
- `async delete(key: str)`: Delete key
- `async clear()`: Clear all entries
- `async items() -> List[Tuple[str, Any]]`: Get all key-value pairs

**Example:**
```python
# Create backend
backend = sb.MemoryBackend(
    sb.BackendConfig(backend_type="memory")
)

# Store and retrieve
await backend.set("user:123", {"name": "Alice", "balance": 1000})
user = await backend.get("user:123")  # {'name': 'Alice', 'balance': 1000}

# Delete
await backend.delete("user:123")
```

**Performance:**
- **Get/Set**: 1M+ operations/second
- **Memory overhead**: <10% vs pure Python
- **Cython-accelerated**: 10-100x faster than pure Python

### `sb.RocksDBBackend`

Persistent state backend using RocksDB.

**Signature:**
```python
sb.RocksDBBackend(config: BackendConfig)
```

**Same API as MemoryBackend** but with persistence:

```python
# Create persistent backend
rocksdb = sb.RocksDBBackend(
    sb.BackendConfig(
        backend_type="rocksdb",
        path="./state/rocksdb"
    )
)

# Use same API
await rocksdb.set("key", "value")
value = await rocksdb.get("key")
```

**When to use:**
- State > 1GB
- Need persistence across restarts
- Slower than memory but still fast (<1ms latency)

### State Types

#### `sb.ValueState`

Single-value state.

**Signature:**
```python
sb.ValueState(backend: StoreBackend, key: str)
```

**Methods:**
- `async value() -> Any`: Get current value
- `async update(value: Any)`: Update value
- `async clear()`: Clear value

**Example:**
```python
counter = sb.ValueState(backend, "event_counter")

await counter.update(0)
await counter.update(await counter.value() + 1)
count = await counter.value()  # 1
```

#### `sb.MapState`

Key-value map state.

**Signature:**
```python
sb.MapState(backend: StoreBackend, key: str)
```

**Methods:**
- `async get(key: str) -> Any`: Get value by key
- `async put(key: str, value: Any)`: Put key-value pair
- `async remove(key: str)`: Remove key
- `async keys() -> List[str]`: Get all keys
- `async items() -> Dict`: Get all items
- `async clear()`: Clear all items

**Example:**
```python
profiles = sb.MapState(backend, "user_profiles")

await profiles.put("alice", {"age": 30, "city": "NYC"})
await profiles.put("bob", {"age": 25, "city": "SF"})

alice = await profiles.get("alice")
all_users = await profiles.items()
```

#### `sb.ListState`

Ordered list state.

**Signature:**
```python
sb.ListState(backend: StoreBackend, key: str)
```

**Methods:**
- `async get() -> List`: Get all items
- `async add(value: Any)`: Append item
- `async update(values: List)`: Replace all items
- `async clear()`: Clear list

**Example:**
```python
events = sb.ListState(backend, "recent_events")

await events.add({"type": "login", "user": "alice"})
await events.add({"type": "purchase", "user": "alice"})

recent = await events.get()  # List of events
```

---

## Checkpointing

### `sb.Barrier`

Checkpoint barrier for distributed snapshots.

**Signature:**
```python
sb.Barrier(checkpoint_id: int, source_id: int)
```

**Parameters:**
- `checkpoint_id` (int): Unique checkpoint ID
- `source_id` (int): Source/partition ID

**Example:**
```python
barrier = sb.Barrier(checkpoint_id=1, source_id=0)
```

### `sb.BarrierTracker`

Track checkpoint barriers across multiple channels (Chandy-Lamport algorithm).

**Signature:**
```python
sb.BarrierTracker(num_channels: int)
```

**Methods:**
- `register_barrier(channel: int, checkpoint_id: int, total_inputs: int) -> bool`:
  Register barrier and check if aligned
- `get_barrier_stats(checkpoint_id: int) -> Dict`: Get checkpoint statistics
- `reset_barrier(checkpoint_id: int)`: Reset barrier after completion

**Example:**
```python
# Create tracker for 3 partitions
tracker = sb.BarrierTracker(num_channels=3)

# Agent 0 receives barrier
aligned_0 = tracker.register_barrier(
    channel=0,
    checkpoint_id=1,
    total_inputs=3
)  # False - waiting for others

# Agent 1 receives barrier
aligned_1 = tracker.register_barrier(
    channel=1,
    checkpoint_id=1,
    total_inputs=3
)  # False - waiting for agent 2

# Agent 2 receives barrier
aligned_2 = tracker.register_barrier(
    channel=2,
    checkpoint_id=1,
    total_inputs=3
)  # True - all aligned!

# Get stats
stats = tracker.get_barrier_stats(1)
# {'aligned': True, 'received_count': 3, 'total_inputs': 3}

# Reset for next checkpoint
tracker.reset_barrier(1)
```

**Performance:**
- **Registration**: <10μs per barrier
- **Memory**: O(num_checkpoints × num_channels)

### `sb.Coordinator`

Checkpoint coordinator for managing distributed snapshots.

**Signature:**
```python
sb.Coordinator(
    max_concurrent_checkpoints: int = 10,
    max_operators: int = 100
)
```

**Methods:**
- `trigger_checkpoint() -> int`: Trigger new checkpoint, returns checkpoint ID
- `complete_checkpoint(checkpoint_id: int)`: Mark checkpoint as complete
- `get_latest_checkpoint() -> int`: Get latest completed checkpoint ID

**Example:**
```python
coordinator = sb.Coordinator()

# Trigger checkpoint
checkpoint_id = coordinator.trigger_checkpoint()

# ... agents process barriers ...

# Complete checkpoint
coordinator.complete_checkpoint(checkpoint_id)

# Get latest
latest = coordinator.get_latest_checkpoint()
```

---

## Time & Watermarks

### `sb.WatermarkTracker`

Track event-time watermarks across partitions.

**Signature:**
```python
sb.WatermarkTracker(num_partitions: int)
```

**Methods:**
- `update_watermark(partition_id: int, timestamp: int)`: Update partition watermark
- `get_global_watermark() -> int`: Get minimum watermark across all partitions
- `get_partition_watermark(partition_id: int) -> int`: Get specific partition watermark

**Example:**
```python
tracker = sb.WatermarkTracker(num_partitions=3)

# Update watermarks from different partitions
tracker.update_watermark(0, 100)  # Partition 0 at time 100
tracker.update_watermark(1, 150)  # Partition 1 at time 150
tracker.update_watermark(2, 120)  # Partition 2 at time 120

# Global watermark is minimum (handles out-of-order data)
global_wm = tracker.get_global_watermark()  # 100
```

**Use case:**
- Event-time processing
- Handling out-of-order events
- Triggering time-based windows

### `sb.Timers`

Timer service for delayed processing.

**Signature:**
```python
sb.Timers()
```

**Example:**
```python
timers = sb.Timers()
# Timer functionality (advanced usage)
```

### `sb.EventTime`

Event-time utilities.

```python
event_time = sb.EventTime()
```

### `sb.TimeService`

Unified time service.

```python
time_service = sb.TimeService()
```

---

## Utilities

### `sb.create_app()`

Factory function to create an App (same as `sb.App`).

```python
app = sb.create_app(
    'my-app',
    broker='kafka://localhost:19092'
)
```

---

## Advanced Features

### Redis Client (Optional)

If Redis is available:

```python
if sb.REDIS_AVAILABLE:
    client = sb.RedisClient(host='localhost', port=6379)
    async_redis = sb.AsyncRedis(host='localhost', port=6379)
```

### GPU Acceleration (Optional)

Enable GPU acceleration:

```python
app = sb.App(
    'ml-pipeline',
    enable_gpu=True,
    gpu_device=0  # GPU device ID
)
```

---

## Type Annotations

Sabot provides type hints for better IDE support:

```python
from sabot import (
    AgentT,        # Agent type
    AppT,          # App type
    StreamT,       # Stream type
    TopicT,        # Topic type
    RecordBatch,   # Arrow RecordBatch
    Schema,        # Arrow Schema
    Table,         # Arrow Table
)
```

---

## Best Practices

### 1. Use Type Hints

```python
from sabot import App, MemoryBackend
from typing import Dict, Any

async def process(transaction: Dict[str, Any]) -> Dict[str, Any]:
    return {"status": "processed"}

@app.agent('transactions')
async def agent(stream):
    async for txn in stream:
        result = await process(txn)
        yield result
```

### 2. Handle Errors Gracefully

```python
@app.agent('events')
async def process_events(stream):
    async for event in stream:
        try:
            result = validate(event)
            yield result
        except ValidationError as e:
            print(f"Invalid event: {e}")
            # Log to dead letter queue
            continue
```

### 3. Use Checkpointing for Exactly-Once

```python
tracker = sb.BarrierTracker(num_channels=3)

@app.agent('transactions')
async def process_with_checkpoints(stream):
    checkpoint_interval = 1000
    processed = 0

    async for txn in stream:
        result = process(txn)
        processed += 1

        # Checkpoint every 1000 messages
        if processed % checkpoint_interval == 0:
            checkpoint_id = processed // checkpoint_interval
            aligned = tracker.register_barrier(0, checkpoint_id, 3)
            if aligned:
                print(f"✅ Checkpoint {checkpoint_id}")

        yield result
```

### 4. Choose Right State Backend

```python
# Small state (<100MB), fast access → MemoryBackend
if state_size_mb < 100:
    backend = sb.MemoryBackend(config)

# Large state (>1GB), persistence → RocksDBBackend
else:
    backend = sb.RocksDBBackend(config)
```

---

## Migration Guide

### From Faust

```python
# Faust
import faust
app = faust.App('app', broker='kafka://localhost')

@app.agent('topic')
async def process(stream):
    async for value in stream:
        yield value

# Sabot (nearly identical!)
import sabot as sb
app = sb.App('app', broker='kafka://localhost:19092')

@app.agent('topic')
async def process(stream):
    async for value in stream:
        yield value

# Run with CLI
# Faust: faust -A myapp:app worker
# Sabot: sabot -A myapp:app worker
```

---

**Next**: See [Architecture](ARCHITECTURE.md) for implementation details.
