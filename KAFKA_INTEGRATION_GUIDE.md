# Kafka Integration Guide

## Overview

Sabot provides a complete Kafka integration with support for multiple serialization formats (JSON, Avro, Protobuf, JSON Schema, MessagePack, String, Bytes), consumer groups, offset management, and error handling.

## Current API

### Stream.from_kafka()

Read from a Kafka topic into a Sabot stream:

```python
Stream.from_kafka(
    bootstrap_servers: str,      # Kafka brokers (e.g., "localhost:9092")
    topic: str,                  # Topic to consume from
    group_id: str,               # Consumer group ID
    codec_type: str = "json",    # Codec: json, avro, protobuf, json_schema, msgpack, string, bytes
    codec_options: Optional[Dict[str, Any]] = None,  # Codec-specific options
    batch_size: int = 1000,      # Messages per batch
    **consumer_kwargs            # Additional Kafka consumer config
) -> Stream
```

### Stream.to_kafka()

Write a stream to a Kafka topic:

```python
stream.to_kafka(
    bootstrap_servers: str,      # Kafka brokers (e.g., "localhost:9092")
    topic: str,                  # Topic to produce to
    codec_type: str = "json",    # Codec: json, avro, protobuf, json_schema, msgpack, string, bytes
    codec_options: Optional[Dict[str, Any]] = None,  # Codec-specific options
    compression_type: str = "lz4",  # Compression: gzip, snappy, lz4, zstd
    key_extractor: Optional[Callable[[Dict], bytes]] = None,  # Extract key from message
    **producer_kwargs            # Additional Kafka producer config
) -> OutputStream
```

## Examples

### Example 1: Simple JSON Stream

```python
from sabot.api.stream import Stream
import pyarrow.compute as pc

# Read from Kafka
stream = Stream.from_kafka(
    bootstrap_servers="localhost:9092",
    topic="transactions",
    group_id="fraud-detector"
)

# Process stream
processed = (stream
    .filter(lambda b: pc.greater(b['amount'], 1000.0))
    .map(lambda b: b.append_column('flagged', pc.scalar(True)))
)

# Write to Kafka
processed.to_kafka(
    bootstrap_servers="localhost:9092",
    topic="flagged-transactions"
)
```

### Example 2: Avro with Schema Registry

```python
from sabot.api.stream import Stream

# Define Avro schema
transaction_schema = {
    "type": "record",
    "name": "Transaction",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "amount", "type": "double"},
        {"name": "timestamp", "type": "long"}
    ]
}

# Read Avro from Kafka
stream = Stream.from_kafka(
    bootstrap_servers="localhost:9092",
    topic="transactions",
    group_id="processor",
    codec_type="avro",
    codec_options={
        'schema_registry_url': 'http://localhost:8081',
        'subject': 'transactions-value'
    }
)

# Process and write Avro to Kafka
stream.to_kafka(
    bootstrap_servers="localhost:9092",
    topic="processed-transactions",
    codec_type="avro",
    codec_options={
        'schema_registry_url': 'http://localhost:8081',
        'subject': 'processed-transactions-value',
        'schema': transaction_schema
    }
)
```

### Example 3: Multiple Topics with Routing

```python
from sabot.api.stream import Stream
import pyarrow.compute as pc

# Read from Kafka
stream = Stream.from_kafka(
    bootstrap_servers="localhost:9092",
    topic="orders",
    group_id="order-router"
)

# Route high-value orders to one topic
high_value = stream.filter(lambda b: pc.greater(b['amount'], 10000.0))
high_value.to_kafka("localhost:9092", "high-value-orders")

# Route low-value orders to another topic
low_value = stream.filter(lambda b: pc.less_equal(b['amount'], 10000.0))
low_value.to_kafka("localhost:9092", "low-value-orders")
```

### Example 4: Error Handling

```python
from sabot.api.stream import Stream

# Read with error handling
stream = Stream.from_kafka(
    bootstrap_servers="localhost:9092",
    topic="transactions",
    group_id="processor",
    # Consumer will skip malformed messages
    auto_offset_reset="earliest",
    enable_auto_commit=True
)

# Process with error handling
try:
    processed = stream.map(lambda b: process_batch(b))
    processed.to_kafka("localhost:9092", "processed")
except Exception as e:
    logger.error(f"Stream processing error: {e}")
```

### Example 5: Custom Partitioning

```python
from sabot.api.stream import Stream

# Read from Kafka
stream = Stream.from_kafka(
    bootstrap_servers="localhost:9092",
    topic="transactions",
    group_id="processor"
)

# Write with custom key extraction (for partitioning)
def extract_customer_id(message: dict) -> bytes:
    return str(message.get('customer_id', '')).encode('utf-8')

stream.to_kafka(
    bootstrap_servers="localhost:9092",
    topic="customer-transactions",
    key_extractor=extract_customer_id
)
```

## Configuration Options

### KafkaSourceConfig

```python
from sabot.kafka.source import KafkaSourceConfig, KafkaSource

config = KafkaSourceConfig(
    # Required
    bootstrap_servers="localhost:9092",
    topic="my-topic",
    group_id="my-group",
    
    # Codec
    codec_type="json",  # json, avro, protobuf, json_schema, msgpack, string, bytes
    codec_options={},
    
    # Consumer settings
    auto_offset_reset="latest",  # earliest, latest
    enable_auto_commit=True,
    auto_commit_interval_ms=5000,
    max_poll_records=500,
    fetch_min_bytes=1,
    fetch_max_wait_ms=500,
    
    # Error handling
    error_handling_policy="skip",  # skip, fail, retry, dead_letter
    max_retry_attempts=3,
    retry_backoff_seconds=1.0,
    dead_letter_topic="my-topic-dlq",
    error_sample_rate=0.1,
    
    # Additional config
    consumer_config={}
)

# Use directly
source = KafkaSource(config)
async for message in source.stream():
    print(message)
```

## Codec Support

### JSON (default)

```python
stream = Stream.from_kafka(
    "localhost:9092",
    "topic",
    "group",
    codec_type="json"
)
```

### Avro with Schema Registry

```python
stream = Stream.from_kafka(
    "localhost:9092",
    "topic",
    "group",
    codec_type="avro",
    codec_options={
        'schema_registry_url': 'http://localhost:8081',
        'subject': 'topic-value'
    }
)
```

### Protobuf

```python
stream = Stream.from_kafka(
    "localhost:9092",
    "topic",
    "group",
    codec_type="protobuf",
    codec_options={
        'schema_registry_url': 'http://localhost:8081',
        'subject': 'topic-value'
    }
)
```

### JSON Schema

```python
stream = Stream.from_kafka(
    "localhost:9092",
    "topic",
    "group",
    codec_type="json_schema",
    codec_options={
        'schema_registry_url': 'http://localhost:8081',
        'subject': 'topic-value'
    }
)
```

### MessagePack

```python
stream = Stream.from_kafka(
    "localhost:9092",
    "topic",
    "group",
    codec_type="msgpack"
)
```

### String

```python
stream = Stream.from_kafka(
    "localhost:9092",
    "topic",
    "group",
    codec_type="string"
)
```

### Raw Bytes

```python
stream = Stream.from_kafka(
    "localhost:9092",
    "topic",
    "group",
    codec_type="bytes"
)
```

## Error Handling

### Skip Errors (Default)

```python
stream = Stream.from_kafka(
    "localhost:9092",
    "topic",
    "group",
    error_handling_policy="skip"
)
```

### Fail on Errors

```python
stream = Stream.from_kafka(
    "localhost:9092",
    "topic",
    "group",
    error_handling_policy="fail"
)
```

### Retry on Errors

```python
stream = Stream.from_kafka(
    "localhost:9092",
    "topic",
    "group",
    error_handling_policy="retry",
    max_retry_attempts=3,
    retry_backoff_seconds=1.0
)
```

### Dead Letter Queue

```python
stream = Stream.from_kafka(
    "localhost:9092",
    "topic",
    "group",
    error_handling_policy="dead_letter",
    dead_letter_topic="topic-dlq"
)
```

## Compression

```python
stream.to_kafka(
    "localhost:9092",
    "topic",
    compression_type="lz4"  # gzip, snappy, lz4, zstd
)
```

## Common Patterns

### Pattern 1: ETL Pipeline

```python
# Extract from Kafka
source = Stream.from_kafka("localhost:9092", "raw-data", "etl")

# Transform
transformed = (source
    .filter(lambda b: pc.is_not_null(b['id']))
    .map(lambda b: enrich_data(b))
    .map(lambda b: validate_data(b))
)

# Load to Kafka
transformed.to_kafka("localhost:9092", "enriched-data")
```

### Pattern 2: Fan-out

```python
# One source, multiple sinks
stream = Stream.from_kafka("localhost:9092", "events", "processor")

# Route to different topics
stream.filter(lambda b: b['type'] == 'A').to_kafka("localhost:9092", "type-a")
stream.filter(lambda b: b['type'] == 'B').to_kafka("localhost:9092", "type-b")
stream.filter(lambda b: b['type'] == 'C').to_kafka("localhost:9092", "type-c")
```

### Pattern 3: Aggregation

```python
from sabot.api.stream import Stream
import pyarrow.compute as pc

# Read from Kafka
stream = Stream.from_kafka("localhost:9092", "transactions", "aggregator")

# Aggregate and write results
(stream
    .window(size_seconds=60)
    .aggregate({
        'count': ('*', 'count'),
        'sum': ('amount', 'sum'),
        'avg': ('amount', 'mean')
    })
    .to_kafka("localhost:9092", "transaction-stats")
)
```

## Migration from Old API

### Old API (Deprecated)

```python
# OLD - Will show deprecation warning
@app.agent('topic-name')
async def process(stream):
    async for event in stream:
        yield event
```

### New API (Recommended)

```python
# NEW - Use Stream API directly
stream = Stream.from_kafka(
    bootstrap_servers="localhost:9092",
    topic="topic-name",
    group_id="my-group"
)

processed = stream.map(lambda b: process_batch(b))
processed.to_kafka("localhost:9092", "output-topic")
```

## Testing Without Kafka

For local development and testing without a running Kafka instance:

```python
from sabot.api.stream import Stream
import pyarrow as pa

# Create test batches
batches = [
    pa.RecordBatch.from_pydict({'id': [1, 2], 'amount': [100.0, 200.0]}),
    pa.RecordBatch.from_pydict({'id': [3, 4], 'amount': [300.0, 400.0]})
]

# Use from_batches instead of from_kafka
stream = Stream.from_batches(batches)

# Process normally
processed = stream.filter(lambda b: pc.greater(b['amount'], 150.0))

# Collect results
result = processed.collect()
print(f"Filtered: {result.num_rows} rows")
```

## Performance Tips

1. **Batch Size**: Increase `batch_size` for higher throughput
   ```python
   stream = Stream.from_kafka(
       "localhost:9092",
       "topic",
       "group",
       batch_size=10000  # Process 10K messages per batch
   )
   ```

2. **Compression**: Use LZ4 or Snappy for best performance
   ```python
   stream.to_kafka(
       "localhost:9092",
       "topic",
       compression_type="lz4"
   )
   ```

3. **Consumer Config**: Tune Kafka consumer settings
   ```python
   stream = Stream.from_kafka(
       "localhost:9092",
       "topic",
       "group",
       fetch_min_bytes=1024 * 1024,  # 1MB
       max_poll_records=5000
   )
   ```

4. **Parallel Processing**: Use multiple consumer groups
   ```python
   # Consumer group 1
   stream1 = Stream.from_kafka("localhost:9092", "topic", "group-1")
   
   # Consumer group 2
   stream2 = Stream.from_kafka("localhost:9092", "topic", "group-2")
   ```

## Troubleshooting

### Issue: Stream hangs

**Solution**: Check Kafka broker connectivity and topic existence

```python
# Add timeout
stream = Stream.from_kafka(
    "localhost:9092",
    "topic",
    "group",
    fetch_max_wait_ms=5000  # 5 second timeout
)
```

### Issue: Offset management

**Solution**: Configure auto-commit or manual commit

```python
stream = Stream.from_kafka(
    "localhost:9092",
    "topic",
    "group",
    enable_auto_commit=True,
    auto_commit_interval_ms=5000
)
```

### Issue: Message deserialization errors

**Solution**: Use error handling policy

```python
stream = Stream.from_kafka(
    "localhost:9092",
    "topic",
    "group",
    error_handling_policy="skip",  # Skip bad messages
    error_sample_rate=1.0  # Log all errors for debugging
)
```

## Next Steps

- See `examples/api/basic_streaming.py` for complete examples
- See `sabot/kafka/source.py` for implementation details
- See `sabot/kafka/sink.py` for producer details
- See `STREAMING_SQL.md` for Kafka + SQL integration
