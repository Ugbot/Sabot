# Kafka Quick Start Guide

**5-minute guide to using Kafka with Sabot**

---

## Installation

```bash
pip install sabot[kafka]
```

---

## 1. Read from Kafka (JSON)

```python
from sabot.kafka import from_kafka
import asyncio

async def consume():
    source = from_kafka(
        "localhost:9092",      # Kafka brokers
        "my-topic",            # Topic
        "my-group",            # Consumer group
        codec_type="json"      # JSON codec
    )

    await source.start()
    async for message in source.stream():
        print(message)
    await source.stop()

asyncio.run(consume())
```

---

## 2. Write to Kafka (JSON)

```python
from sabot.kafka import to_kafka
import asyncio

async def produce():
    sink = to_kafka(
        "localhost:9092",
        "output-topic",
        codec_type="json"
    )

    await sink.start()
    await sink.send({"user_id": "123", "action": "purchase"})
    await sink.stop()

asyncio.run(produce())
```

---

## 3. Stream API (Easiest)

```python
from sabot.api.stream import Stream

# Read from Kafka
stream = Stream.from_kafka(
    "localhost:9092",
    "input-topic",
    "my-group"
)

# Transform
result = (stream
    .filter(lambda b: ...)
    .map(lambda b: ...)
)

# Write to Kafka
result.to_kafka("localhost:9092", "output-topic")
```

---

## 4. Avro with Schema Registry

```python
from sabot.kafka import from_kafka

SCHEMA = """
{
  "type": "record",
  "name": "User",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "age", "type": "int"}
  ]
}
"""

source = from_kafka(
    "localhost:9092",
    "users-avro",
    "my-group",
    codec_type="avro",
    codec_options={
        'schema_registry_url': 'http://localhost:8081',
        'subject': 'users-value'
    }
)
```

---

## 5. All Supported Codecs

```python
# JSON (human-readable)
codec_type="json"

# Avro (with schema evolution)
codec_type="avro"
codec_options={'schema_registry_url': 'http://localhost:8081', 'subject': 'topic-value'}

# Protobuf (efficient, C++ backend)
codec_type="protobuf"
codec_options={'schema_registry_url': 'http://localhost:8081', 'message_class': MyMessage}

# JSON Schema (with validation)
codec_type="json_schema"
codec_options={'schema': {...}}

# MessagePack (compact binary)
codec_type="msgpack"

# String (plain text)
codec_type="string"

# Bytes (raw binary)
codec_type="bytes"
```

---

## 6. Common Patterns

### ETL Pipeline
```python
# Read Avro, write JSON
source = from_kafka(..., codec_type="avro")
sink = to_kafka(..., codec_type="json")

async for message in source.stream():
    await sink.send(transform(message))
```

### Batch Processing
```python
sink = to_kafka(...)
await sink.start()

messages = [{"id": i} for i in range(1000)]
await sink.send_batch(messages)
await sink.flush()
```

### With Metadata
```python
source = from_kafka(...)
async for metadata in source.stream_with_metadata():
    print(f"Offset: {metadata['offset']}")
    print(f"Value: {metadata['value']}")
```

---

## 7. Configuration

```python
from sabot.kafka import KafkaSourceConfig, KafkaSource

config = KafkaSourceConfig(
    bootstrap_servers="localhost:9092",
    topic="my-topic",
    group_id="my-group",
    codec_type="json",
    auto_offset_reset="earliest",  # or "latest"
    enable_auto_commit=True,
    max_poll_records=500
)

source = KafkaSource(config)
```

---

## 8. Error Handling

```python
try:
    await source.start()
    async for message in source.stream():
        process(message)
except Exception as e:
    logger.error(f"Kafka error: {e}")
finally:
    await source.stop()
```

---

## Next Steps

📖 **Full Documentation**: See `docs/KAFKA_INTEGRATION.md`
🧪 **Examples**: Run `python examples/kafka_etl_demo.py`
🧰 **Tests**: Run `pytest tests/test_kafka_integration.py --run-kafka`

---

**That's it! You're ready to use Kafka with Sabot.** 🚀
