# Sabot Channel System Design

## Overview

Based on analysis of Faust's channel system, here's the design for Sabot's channel architecture that integrates Arrow-native processing with the streaming agent paradigm.

## Faust Channel Analysis

### Key Components
1. **Channel Class**: Core in-memory channel with queue buffering
2. **ChannelT Interface**: Abstract type definitions for channel implementations
3. **Subscriber Pattern**: Multiple consumers can read from same channel
4. **Iterator Isolation**: Channels clone themselves when iterated over
5. **Schema Support**: Serialization/deserialization with configurable schemas

### Core Features
- **Async Iteration**: `__aiter__` and `__anext__` support
- **Message Buffering**: Configurable queue size with flow control
- **Send Operations**: `send()`, `send_soon()`, `publish_message()`
- **Queue Operations**: `put()`, `get()`, `empty()`
- **Error Handling**: Decode error callbacks
- **Channel Cloning**: Iterator isolation and configuration inheritance

### Architecture Flow
```
Agent <-- Stream <-- Channel <-- Transport (optional)
```

## Sabot Channel System Design

### Core Principles
1. **Arrow-Native**: All data processing uses Apache Arrow for efficiency
2. **Zero-Copy**: Minimize data copying between components
3. **Cython Optimized**: Performance-critical paths in Cython
4. **Transport Agnostic**: Channels work with any backend (memory, Kafka, Redis, etc.)
5. **Subscriber Pattern**: Multiple agents can consume from same channel

### Channel Types

#### 1. Base Channel (Abstract)
```python
class ChannelT(AsyncIterator[EventT[T]], ABC):
    """Abstract channel interface for Sabot."""

    app: AppT
    schema: SchemaT
    key_type: Optional[ModelArg]
    value_type: Optional[ModelArg]
    maxsize: Optional[int]
    active_partitions: Optional[Set[TP]]

    # Core async iteration
    def __aiter__(self) -> ChannelT[T]: ...
    async def __anext__(self) -> EventT[T]: ...

    # Message operations
    async def send(self, *, key=None, value=None, **kwargs) -> RecordMetadata: ...
    async def put(self, event: EventT) -> None: ...
    async def get(self, *, timeout=None) -> EventT[T]: ...

    # Channel management
    def clone(self, **kwargs) -> ChannelT[T]: ...
    def stream(self, **kwargs) -> StreamT[T]: ...
    def derive(self, **kwargs) -> ChannelT[T]: ...

    # Subscriber management
    @property
    def subscriber_count(self) -> int: ...
```

#### 2. In-Memory Channel
```python
class Channel(ChannelT[T]):
    """In-memory channel with Arrow RecordBatch buffering."""

    def __init__(self, app, *, maxsize=None, schema=None, **kwargs):
        self.app = app
        self.maxsize = maxsize or app.conf.stream_buffer_maxsize
        self._queue = ThrowableQueue(maxsize=self.maxsize)
        self._subscribers = WeakSet()
        self.schema = schema or self._get_default_schema()
        # Arrow-specific: pre-allocate RecordBatch builder
        self._batch_builder = pa.RecordBatchBuilder()

    async def put(self, event: EventT) -> None:
        """Put event into channel queue."""
        # Convert to Arrow format if needed
        arrow_event = self._to_arrow_event(event)
        await self._queue.put(arrow_event)

        # Broadcast to subscribers
        for subscriber in self._subscribers:
            await subscriber._queue.put(arrow_event)

    async def get(self, *, timeout=None) -> EventT[T]:
        """Get next event from queue."""
        if timeout:
            arrow_event = await asyncio.wait_for(self._queue.get(), timeout)
        else:
            arrow_event = await self._queue.get()
        return self._from_arrow_event(arrow_event)
```

#### 3. Transport-Backed Channels
```python
class TopicChannel(ChannelT[T]):
    """Channel backed by external transport (Kafka, Redis, etc.)."""

    def __init__(self, app, topic_name, *, partitions=1, **kwargs):
        super().__init__(app, **kwargs)
        self.topic_name = topic_name
        self.partitions = partitions
        self._producer = app.producer
        self._consumer = app.consumer

    async def send(self, *, key=None, value=None, **kwargs) -> RecordMetadata:
        """Send message via transport."""
        # Arrow serialization
        arrow_key, arrow_value = self._serialize_arrow(key, value)

        # Send via transport
        return await self._producer.send_and_wait(
            self.topic_name,
            key=arrow_key,
            value=arrow_value,
            **kwargs
        )

    async def deliver(self, message: Message) -> None:
        """Deliver message from consumer to channel queue."""
        # Deserialize from transport format to Arrow
        arrow_event = self._deserialize_arrow(message)
        await self._queue.put(arrow_event)
```

### Key Design Decisions

#### 1. Arrow Integration
- **RecordBatch Buffering**: Use Arrow RecordBatch for efficient in-memory storage
- **Zero-Copy Operations**: Share Arrow buffers between components
- **Schema Enforcement**: Arrow schemas ensure type safety
- **Vectorized Processing**: Enable SIMD operations on buffered data

#### 2. Subscriber Pattern
```python
class Channel:
    def __init__(self):
        self._subscribers = WeakSet()  # Weak references to prevent cycles
        self._root = None

    def clone(self, *, is_iterator=False, **kwargs):
        """Create channel clone for iteration."""
        clone = type(self)(**self._clone_args(), **kwargs)
        if is_iterator and self._root:
            self._root._subscribers.add(clone)
        return clone

    async def put(self, event):
        """Broadcast to all subscribers."""
        for subscriber in self._root._subscribers:
            await subscriber._queue.put(event)
```

#### 3. Cython Optimizations
```cython
# _cython/channels.pyx
cdef class FastChannel:
    cdef:
        object _queue
        object _subscribers
        object _batch_builder
        object _schema

    cdef inline object _to_arrow_record(self, object event):
        # Fast Arrow conversion
        return self._batch_builder.append_record(event)

    cdef inline object _from_arrow_record(self, object arrow_record):
        # Fast Arrow to Python conversion
        return self._record_converter(arrow_record)
```

#### 4. Routing and Distribution
```python
class ChannelRouter:
    """Routes messages between channels based on rules."""

    def __init__(self, app):
        self.app = app
        self._routes = {}  # pattern -> channel mapping

    def add_route(self, pattern, channel):
        """Add routing rule."""
        self._routes[pattern] = channel

    async def route_message(self, message):
        """Route message to appropriate channel(s)."""
        for pattern, channel in self._routes.items():
            if self._matches_pattern(message, pattern):
                await channel.put(message)
```

### Integration with Sabot Architecture

#### Stream Creation
```python
# Channels create streams
channel = app.channel(schema=arrow_schema, maxsize=1000)
stream = channel.stream(buffer_size=100)

# Agents consume from streams
@app.agent(stream)
async def process_data(stream):
    async for event in stream:
        # Process Arrow RecordBatch
        result = process_arrow_batch(event.value)
        yield result
```

#### Multi-Transport Support
```python
# In-memory channel
memory_channel = app.channel()

# Kafka-backed channel
kafka_channel = app.topic("data-stream", partitions=3)

# Redis-backed channel
redis_channel = app.redis_channel("cache-stream")

# All implement same interface
async def fan_out(channel1, channel2, channel3):
    async for event in input_channel:
        await channel1.put(event)
        await channel2.put(event)
        await channel3.put(event)
```

### Performance Optimizations

#### 1. Buffer Management
- **Pre-allocated Buffers**: Reuse Arrow RecordBatch builders
- **Memory Pooling**: Share Arrow memory pools across channels
- **Batch Operations**: Buffer multiple messages before processing

#### 2. Zero-Copy Operations
- **Buffer Sharing**: Arrow buffers shared between producer/consumer
- **View Creation**: Create views instead of copying data
- **Reference Counting**: Efficient memory management

#### 3. Concurrent Processing
- **Async Queues**: Non-blocking message passing
- **Flow Control**: Backpressure when queues full
- **Subscriber Batching**: Batch messages to multiple subscribers

### Error Handling
```python
class Channel:
    async def on_decode_error(self, exc, message):
        """Handle Arrow deserialization errors."""
        await self.app.log.error(
            "Failed to decode Arrow message",
            exc=exc,
            message=message
        )
        # Optionally send to dead letter queue
        await self._dead_letter_queue.put(message)
```

### Testing Strategy
```python
def test_channel_buffering():
    """Test Arrow RecordBatch buffering."""
    channel = app.channel(maxsize=10)

    # Send Arrow RecordBatch
    batch = pa.RecordBatch.from_arrays([pa.array([1, 2, 3])], names=['data'])
    await channel.put(batch)

    # Receive and verify
    received = await channel.get()
    assert received.equals(batch)

def test_subscriber_pattern():
    """Test multiple consumers from same channel."""
    channel = app.channel()
    subscriber1 = channel.clone(is_iterator=True)
    subscriber2 = channel.clone(is_iterator=True)

    # Both should receive same messages
    await channel.put(event1)
    assert await subscriber1.get() == event1
    assert await subscriber2.get() == event1
```

This design provides a solid foundation for Sabot's channel system that builds on Faust's proven architecture while adding Arrow-native processing capabilities and Cython optimizations for maximum performance.
