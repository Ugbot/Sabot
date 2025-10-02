# Sabot Kafka Integration

Complete Kafka integration for Sabot with support for all major serialization formats and Schema Registry.

---

## 🚀 Features

### ✅ **All Kafka Serialization Formats**
- **JSON** - Human-readable, widely supported
- **Avro** - Schema evolution, Confluent Schema Registry
- **Protobuf** - Efficient binary, Google's format (C++ accelerated)
- **JSON Schema** - JSON with validation
- **MessagePack** - Compact binary, faster than JSON
- **String** - Plain UTF-8 text
- **Bytes** - Raw binary data

### ✅ **Schema Registry Integration**
- Confluent Schema Registry client
- Automatic schema registration and caching
- Confluent wire format: `[magic_byte(1)][schema_id(4)][payload]`
- Schema evolution and compatibility checking

### ✅ **High-Level APIs**
- **KafkaSource** - Read from Kafka topics
- **KafkaSink** - Write to Kafka topics
- **Stream API** - `Stream.from_kafka()` and `stream.to_kafka()`
- Seamless async/sync integration

### ✅ **Production Features**
- Consumer group management
- Offset management (earliest, latest, committed)
- Compression (lz4, gzip, snappy, zstd)
- Idempotent writes
- Batching and backpressure
- Automatic serialization/deserialization

---

## 📦 Installation

```bash
# Core Kafka support
pip install sabot[kafka]

# Or install individual components
pip install aiokafka avro protobuf jsonschema msgpack httpx
```

For development (with Cython optimization):
```bash
pip install sabot[kafka,dev]
```

---

## 🎯 Quick Start

### Reading from Kafka (JSON)

```python
from sabot.kafka import from_kafka
import asyncio

# Create Kafka source
source = from_kafka(
    bootstrap_servers="localhost:9092",
    topic="transactions",
    group_id="my-consumer-group",
    codec_type="json"
)

# Consume messages
async def consume():
    await source.start()
    async for message in source.stream():
        print(f"Received: {message}")
    await source.stop()

asyncio.run(consume())
```

### Writing to Kafka (JSON)

```python
from sabot.kafka import to_kafka
import asyncio

# Create Kafka sink
sink = to_kafka(
    bootstrap_servers="localhost:9092",
    topic="output",
    codec_type="json"
)

# Produce messages
async def produce():
    await sink.start()

    messages = [
        {"user_id": "123", "action": "purchase"},
        {"user_id": "456", "action": "view"}
    ]

    for msg in messages:
        await sink.send(msg)

    await sink.stop()

asyncio.run(produce())
```

### Stream API Integration

```python
from sabot.api.stream import Stream

# Read from Kafka, transform, write back
stream = Stream.from_kafka(
    bootstrap_servers="localhost:9092",
    topic="input-topic",
    group_id="stream-processor",
    codec_type="json"
)

# Apply transformations
result = (stream
    .filter(lambda batch: ...)  # Filter records
    .map(lambda batch: ...)     # Transform records
)

# Write to output topic
result.to_kafka(
    bootstrap_servers="localhost:9092",
    topic="output-topic",
    codec_type="json"
)
```

---

## 📖 Detailed Examples

### Avro with Schema Registry

```python
from sabot.kafka import from_kafka, to_kafka, SchemaRegistryClient

# Define Avro schema
SCHEMA = """
{
  "type": "record",
  "name": "Transaction",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "amount", "type": "double"},
    {"name": "timestamp", "type": "long"}
  ]
}
"""

# Register schema
registry = SchemaRegistryClient("http://localhost:8081")
schema_id = registry.register_schema("transactions-value", SCHEMA, "AVRO")

# Create Avro source
source = from_kafka(
    bootstrap_servers="localhost:9092",
    topic="transactions",
    group_id="avro-consumer",
    codec_type="avro",
    codec_options={
        'schema_registry_url': 'http://localhost:8081',
        'subject': 'transactions-value'
    }
)

# Create Avro sink
sink = to_kafka(
    bootstrap_servers="localhost:9092",
    topic="processed-transactions",
    codec_type="avro",
    codec_options={
        'schema_registry_url': 'http://localhost:8081',
        'subject': 'processed-transactions-value',
        'schema': SCHEMA
    }
)
```

### Protobuf with Schema Registry

```python
from sabot.kafka import to_kafka

# Assuming you have a compiled Protobuf message class
from my_protos import Transaction

sink = to_kafka(
    bootstrap_servers="localhost:9092",
    topic="transactions-proto",
    codec_type="protobuf",
    codec_options={
        'schema_registry_url': 'http://localhost:8081',
        'message_class': Transaction,
        'subject': 'transactions-proto-value'
    }
)

# Send Protobuf message
transaction = Transaction()
transaction.id = "TX-123"
transaction.amount = 99.99

await sink.send(transaction)
```

### JSON Schema with Validation

```python
from sabot.kafka import to_kafka

# Define JSON Schema
SCHEMA = {
    "type": "object",
    "properties": {
        "user_id": {"type": "string"},
        "age": {"type": "integer", "minimum": 0}
    },
    "required": ["user_id", "age"]
}

sink = to_kafka(
    bootstrap_servers="localhost:9092",
    topic="validated-users",
    codec_type="json_schema",
    codec_options={
        'schema_registry_url': 'http://localhost:8081',
        'schema': SCHEMA,
        'subject': 'users-value'
    }
)

# Valid message
await sink.send({"user_id": "123", "age": 30})

# Invalid message (raises validation error)
# await sink.send({"user_id": "456"})  # Missing required 'age'
```

### MessagePack (High Performance)

```python
from sabot.kafka import from_kafka, to_kafka

# MessagePack is more compact and faster than JSON
source = from_kafka(
    bootstrap_servers="localhost:9092",
    topic="high-throughput",
    group_id="msgpack-consumer",
    codec_type="msgpack"
)

sink = to_kafka(
    bootstrap_servers="localhost:9092",
    topic="msgpack-output",
    codec_type="msgpack",
    compression_type="lz4"
)

# Process at wire speed
async for message in source.stream():
    await sink.send(process(message))
```

### Multi-Codec Pipeline

```python
# Read Avro, write JSON (for downstream consumers)
source = from_kafka(
    bootstrap_servers="localhost:9092",
    topic="transactions-avro",
    group_id="codec-converter",
    codec_type="avro",
    codec_options={
        'schema_registry_url': 'http://localhost:8081',
        'subject': 'transactions-value'
    }
)

sink = to_kafka(
    bootstrap_servers="localhost:9092",
    topic="transactions-json",
    codec_type="json"
)

# Convert Avro → JSON
await source.start()
await sink.start()

async for transaction in source.stream():
    await sink.send(transaction)  # Automatic conversion
```

---

## 🔧 Configuration

### KafkaSource Configuration

```python
from sabot.kafka import KafkaSourceConfig, KafkaSource

config = KafkaSourceConfig(
    # Connection
    bootstrap_servers="localhost:9092",
    topic="my-topic",
    group_id="my-group",

    # Codec
    codec_type="json",  # json, avro, protobuf, json_schema, msgpack, string, bytes
    codec_options={},   # Codec-specific options

    # Consumer settings
    auto_offset_reset="latest",  # earliest, latest
    enable_auto_commit=True,
    auto_commit_interval_ms=5000,
    max_poll_records=500,
    fetch_min_bytes=1,
    fetch_max_wait_ms=500,

    # Additional Kafka config
    consumer_config={
        'session_timeout_ms': 30000,
        'heartbeat_interval_ms': 3000
    }
)

source = KafkaSource(config)
```

### KafkaSink Configuration

```python
from sabot.kafka import KafkaSinkConfig, KafkaSink, PartitionStrategy

config = KafkaSinkConfig(
    # Connection
    bootstrap_servers="localhost:9092",
    topic="output-topic",

    # Codec
    codec_type="json",
    codec_options={},

    # Partitioning
    partition_strategy=PartitionStrategy.KEY_HASH,
    partition_key_extractor=lambda msg: msg['user_id'].encode('utf-8'),

    # Producer settings
    compression_type="lz4",  # gzip, snappy, lz4, zstd, None
    acks="all",              # 0, 1, all
    retries=3,
    max_in_flight_requests=5,
    linger_ms=10,           # Batching delay
    batch_size=16384,       # 16KB

    # Reliability
    enable_idempotence=True,
    transactional_id=None,  # Set for exactly-once

    # Additional Kafka config
    producer_config={
        'request_timeout_ms': 30000
    }
)

sink = KafkaSink(config)
```

---

## 🎭 Advanced Usage

### Consumer with Metadata

```python
source = from_kafka(...)
await source.start()

# Get full message metadata
async for metadata in source.stream_with_metadata():
    print(f"Topic: {metadata['topic']}")
    print(f"Partition: {metadata['partition']}")
    print(f"Offset: {metadata['offset']}")
    print(f"Timestamp: {metadata['timestamp']}")
    print(f"Key: {metadata['key']}")
    print(f"Value: {metadata['value']}")
    print(f"Headers: {metadata['headers']}")
```

### Manual Offset Management

```python
source = from_kafka(
    bootstrap_servers="localhost:9092",
    topic="my-topic",
    group_id="manual-commit",
    enable_auto_commit=False  # Manual commit
)

await source.start()

async for message in source.stream():
    process(message)

    # Commit after processing
    await source.commit()
```

### Seek Operations

```python
source = from_kafka(...)
await source.start()

# Seek to beginning
await source.seek_to_beginning()

# Seek to end
await source.seek_to_end()

# Then start consuming
async for message in source.stream():
    ...
```

### Key Extraction for Partitioning

```python
sink = to_kafka(
    bootstrap_servers="localhost:9092",
    topic="user-events",
    codec_type="json",
    key_extractor=lambda msg: msg['user_id'].encode('utf-8')
)

# Messages with same user_id go to same partition
await sink.send({"user_id": "123", "action": "login"})
await sink.send({"user_id": "123", "action": "purchase"})  # Same partition
```

### Batch Sending

```python
sink = to_kafka(...)
await sink.start()

# Send batch (more efficient)
messages = [
    {"id": 1, "data": "a"},
    {"id": 2, "data": "b"},
    {"id": 3, "data": "c"}
]

await sink.send_batch(messages)
await sink.flush()
```

---

## 📊 Performance

### Phase 1 (Current - Pure Python)
- **JSON**: 50K-100K msgs/sec
- **MessagePack**: 80K-150K msgs/sec
- **Avro**: 30K-80K msgs/sec
- **Protobuf**: 100K-200K msgs/sec (C++ backend)

### Phase 2 (Planned - Cython Optimization)
- **All formats**: 500K-1M msgs/sec
- Lock-free schema caching
- Zero-copy serialization paths
- SIMD-accelerated validation

### Optimization Tips

1. **Use MessagePack** for high throughput binary data
2. **Enable compression** (lz4 recommended for speed)
3. **Batch messages** when possible
4. **Use Protobuf** for structured data (already C++ accelerated)
5. **Tune batch_size and linger_ms** for your workload

---

## 🧪 Testing

Run comprehensive tests:

```bash
# Requires running Kafka + Schema Registry
pytest tests/test_kafka_integration.py --run-kafka -v

# Without Kafka (unit tests only)
pytest tests/test_kafka_integration.py -v
```

Run the demo:

```bash
python examples/kafka_etl_demo.py
```

---

## 🏗️ Architecture

### Confluent Wire Format

All Schema Registry codecs use Confluent wire format:

```
┌─────────────┬──────────────┬─────────────────┐
│ Magic Byte  │  Schema ID   │    Payload      │
│   (1 byte)  │  (4 bytes)   │  (variable)     │
└─────────────┴──────────────┴─────────────────┘
     0x00         Big-endian      Avro/Proto/JSON
```

### Schema Caching

```
┌──────────────────────────────────────────────┐
│          SchemaRegistryClient                │
├──────────────────────────────────────────────┤
│  _schemas_by_id: Dict[int, RegisteredSchema] │
│  _schemas_by_subject: Dict[str, Dict]        │
│  _latest_versions: Dict[str, int]            │
└──────────────────────────────────────────────┘
         │
         ├─ Phase 1: Python dict (lock-based)
         └─ Phase 2: C++ unordered_map (lock-free)
```

### Codec Hierarchy

```
Codec (ABC)
├─ JSONCodec (orjson or stdlib)
├─ MsgPackCodec (msgpack-python)
├─ ArrowCodec (pyarrow)
├─ AvroCodec (avro-python3 → Phase 2: C++)
├─ ProtobufCodec (google.protobuf - C++ backend)
├─ JSONSchemaCodec (jsonschema → Phase 2: C++)
├─ StringCodec (UTF-8)
└─ BytesCodec (no-op)
```

---

## 🔮 Roadmap

### Phase 1: Python Implementation ✅ **COMPLETE**
- ✅ All 7 codec types
- ✅ Schema Registry client
- ✅ KafkaSource and KafkaSink
- ✅ Stream API integration
- ✅ Comprehensive tests and examples

### Phase 2: Cython Optimization (Planned)
- 🔄 Lock-free schema cache (C++ unordered_map)
- 🔄 Cythonized Avro encoding/decoding
- 🔄 Cythonized JSON Schema validation
- 🔄 Zero-copy serialization paths
- 🔄 SIMD-accelerated operations
- 🔄 Target: 500K-1M msgs/sec

### Phase 3: Advanced Features (Future)
- 🔄 Exactly-once semantics (EOS)
- 🔄 Transactional writes
- 🔄 Confluent Cloud integration
- 🔄 Kafka Streams-style operators
- 🔄 Stateful processing with Tonbo

---

## 🤝 Contributing

The Kafka integration is designed for easy extension:

1. **Add new codec**: Implement `Codec` ABC in `sabot/types/codecs.py`
2. **Optimize hot paths**: Mark with `# TODO: Cythonize` comments
3. **Add tests**: Extend `tests/test_kafka_integration.py`
4. **Update docs**: This file!

---

## 📚 Resources

- [Confluent Schema Registry Docs](https://docs.confluent.io/platform/current/schema-registry/index.html)
- [Apache Avro Specification](https://avro.apache.org/docs/current/spec.html)
- [Protocol Buffers](https://developers.google.com/protocol-buffers)
- [JSON Schema](https://json-schema.org/)
- [MessagePack Format](https://msgpack.org/)

---

## 📝 License

Sabot Kafka integration is part of the Sabot project.

---

**Built for production. Optimized for performance. Ready for scale.**
