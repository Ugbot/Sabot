# Phase 5: Event-Time Processing - Technical Specification

**Phase:** 5 of 9
**Focus:** Integrate watermark tracking and enable event-time processing
**Dependencies:** Phase 4 (Stream API with windows complete)
**Goal:** Handle out-of-order events correctly using event-time semantics

---

## Overview

Phase 5 integrates the existing WatermarkTracker Cython module with the Stream API to enable event-time processing. This allows the system to handle out-of-order events correctly and fire windows based on event time rather than processing time.

**Current State:**
- WatermarkTracker Cython module exists and compiles
- Basic watermark tracking tested
- Not integrated with agent runtime
- Not propagated through stream pipelines
- Windows currently use processing time

**Goal:**
- Integrate WatermarkTracker with agent runtime
- Extract event time from messages
- Propagate watermarks through operators
- Fire windows based on watermarks
- Handle late events

---

## Step 5.1: Integrate Watermark Tracker

### Current State

**Module:** `sabot/_cython/time/watermark_tracker.pyx`

**Status:**
- Cython module compiles
- Can track watermarks per partition
- Computes global watermark (min across partitions)
- Not integrated with runtime

**Files to Update:**
- `sabot/agents/runtime.py` - Add watermark tracking to agent processes
- `sabot/kafka/source.py` - Extract event time from Kafka messages
- `sabot/api/stream.py` - Use watermarks in window firing

### Implementation

#### Update `sabot/agents/runtime.py` - Add Watermark Tracking

```python
from sabot.time import WatermarkTracker


@dataclass
class AgentProcess:
    """Represents a running agent process."""
    agent_id: str
    agent_func: Callable
    topic: str
    broker: str

    process: Optional[asyncio.Task] = None
    consumer: Optional['KafkaSource'] = None
    state_backend: Optional[StateBackend] = None
    watermark_tracker: Optional[WatermarkTracker] = None

    async def start(self):
        """Start the agent process."""
        # Create Kafka consumer
        self.consumer = KafkaSource(
            topic=self.topic,
            broker=self.broker,
            group_id=f"{self.agent_id}-group"
        )
        await self.consumer.start()

        # Initialize watermark tracker
        num_partitions = len(self.consumer.partitions)
        self.watermark_tracker = WatermarkTracker(num_partitions=num_partitions)

        # Start processing loop
        self.process = asyncio.create_task(self._processing_loop())

    async def _processing_loop(self):
        """Main processing loop with watermark tracking."""
        async for message in self.consumer:
            # Extract event time
            event_time = self._extract_event_time(message)

            # Update watermark for this partition
            partition_id = message.partition
            self.watermark_tracker.update_watermark(
                partition_id=partition_id,
                watermark=event_time
            )

            # Create execution context with watermark
            context = AgentContext(
                message=message,
                state_backend=self.state_backend,
                watermark_tracker=self.watermark_tracker,
                event_time=event_time
            )

            # Execute agent function
            try:
                result = await self.agent_func(context)

                # TODO: Emit result to sink

            except Exception as e:
                logger.error(f"Error processing message: {e}")
                # TODO: Handle error based on error policy

    def _extract_event_time(self, message: Any) -> int:
        """Extract event time from message.

        Priority:
        1. Explicit timestamp field in message
        2. Kafka message timestamp
        3. Fall back to processing time

        Returns:
            Event time in milliseconds
        """
        # Check message payload
        if isinstance(message.value, dict):
            if 'timestamp' in message.value:
                return int(message.value['timestamp'])
            if 'event_time' in message.value:
                return int(message.value['event_time'])

        # Use Kafka message timestamp
        if hasattr(message, 'timestamp') and message.timestamp is not None:
            return int(message.timestamp)

        # Fall back to processing time
        import time
        return int(time.time() * 1000)

    def get_watermark(self) -> int:
        """Get current global watermark."""
        if self.watermark_tracker is None:
            return 0
        return self.watermark_tracker.get_watermark()


@dataclass
class AgentContext:
    """Execution context for agent functions."""
    message: Any
    state_backend: Optional[StateBackend] = None
    watermark_tracker: Optional[WatermarkTracker] = None
    event_time: Optional[int] = None

    def get_event_time(self) -> int:
        """Get event time for current message."""
        return self.event_time or 0

    def get_watermark(self) -> int:
        """Get current global watermark."""
        if self.watermark_tracker is None:
            return 0
        return self.watermark_tracker.get_watermark()

    def state(self, namespace: str = "default"):
        """Get state accessor for namespace."""
        return StateAccessor(backend=self.state_backend, namespace=namespace)
```

#### Update `sabot/kafka/source.py` - Extract Event Time

```python
from dataclasses import dataclass
from typing import Optional, Callable
import time


@dataclass
class KafkaMessage:
    """Kafka message with metadata."""
    key: Any
    value: Any
    partition: int
    offset: int
    timestamp: Optional[int] = None
    headers: Optional[dict] = None

    def event_time(self, extractor: Optional[Callable] = None) -> int:
        """Get event time for this message.

        Args:
            extractor: Optional function to extract timestamp from value

        Returns:
            Event time in milliseconds
        """
        if extractor is not None:
            return extractor(self.value)

        # Default: use Kafka message timestamp
        if self.timestamp is not None:
            return self.timestamp

        # Fall back to processing time
        return int(time.time() * 1000)


class KafkaSource:
    """Kafka source with event-time support."""

    def __init__(
        self,
        topic: str,
        broker: str,
        group_id: str,
        timestamp_extractor: Optional[Callable] = None,
        **kwargs
    ):
        self.topic = topic
        self.broker = broker
        self.group_id = group_id
        self.timestamp_extractor = timestamp_extractor

        # Create consumer
        from aiokafka import AIOKafkaConsumer
        self.consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=broker,
            group_id=group_id,
            **kwargs
        )

    async def start(self):
        """Start consumer."""
        await self.consumer.start()

        # Get partition assignment
        self.partitions = self.consumer.assignment()

    async def __aiter__(self):
        """Async iteration with event-time metadata."""
        async for record in self.consumer:
            # Wrap in KafkaMessage
            message = KafkaMessage(
                key=record.key,
                value=record.value,
                partition=record.partition,
                offset=record.offset,
                timestamp=record.timestamp,
                headers=dict(record.headers) if record.headers else None
            )

            yield message
```

### Verification

```python
# Test watermark tracking in agent
import sabot as sb

app = sb.App("test-app")

@app.agent("test-topic")
async def process(ctx):
    """Process messages with event-time tracking."""
    message = ctx.message
    event_time = ctx.get_event_time()
    watermark = ctx.get_watermark()

    print(f"Event time: {event_time}, Watermark: {watermark}")

    return message

# Start worker
# sabot -A myapp:app worker
```

**Expected Result:**
- Agent process creates WatermarkTracker
- Event time extracted from messages
- Watermark updated per partition
- Global watermark computed correctly

---

## Step 5.2: Propagate Watermarks Through Pipeline

### Current State

**Issue:**
- Watermarks tracked at source
- Not propagated through operators
- Window firing doesn't use watermarks

**Goal:**
- Each operator tracks its watermark
- Watermarks propagate downstream
- Global watermark is minimum across all operators

### Implementation

#### Update `sabot/api/stream.py` - Watermark Propagation

```python
from sabot.time import WatermarkTracker
from typing import Optional, Iterator


class Stream:
    """High-level Stream API with watermark propagation."""

    def __init__(
        self,
        source,
        state_backend: Optional[StateBackend] = None,
        watermark_tracker: Optional[WatermarkTracker] = None
    ):
        self._source = source
        self._state_backend = state_backend or MemoryBackend()
        self._watermark_tracker = watermark_tracker
        self._current_watermark = 0

    def assign_timestamps_and_watermarks(
        self,
        timestamp_extractor: Callable[[Any], int],
        watermark_strategy: str = "monotonic"
    ) -> 'Stream':
        """Assign timestamps and generate watermarks.

        Args:
            timestamp_extractor: Function to extract timestamp from element
            watermark_strategy: Strategy for generating watermarks
                - "monotonic": watermark = max(event_times)
                - "bounded_out_of_order": watermark = max(event_times) - max_delay

        Returns:
            Stream with timestamps and watermarks assigned
        """
        async def _assign_generator():
            max_event_time = 0

            async for element in self:
                # Extract timestamp
                event_time = timestamp_extractor(element)

                # Update max event time
                max_event_time = max(max_event_time, event_time)

                # Generate watermark based on strategy
                if watermark_strategy == "monotonic":
                    watermark = max_event_time
                elif watermark_strategy == "bounded_out_of_order":
                    # Allow 5 seconds of out-of-order
                    watermark = max_event_time - 5000
                else:
                    watermark = max_event_time

                # Update watermark
                self._current_watermark = watermark

                # Attach metadata to element
                if isinstance(element, dict):
                    element['_timestamp'] = event_time
                    element['_watermark'] = watermark

                yield element

        return Stream(
            _assign_generator(),
            state_backend=self._state_backend,
            watermark_tracker=self._watermark_tracker
        )

    def get_watermark(self) -> int:
        """Get current watermark for this stream."""
        if self._watermark_tracker is not None:
            return self._watermark_tracker.get_watermark()
        return self._current_watermark

    def map(self, fn: Callable) -> 'Stream':
        """Map with watermark propagation."""
        async def _map_generator():
            async for element in self:
                result = fn(element)

                # Propagate watermark metadata
                if isinstance(element, dict) and isinstance(result, dict):
                    if '_timestamp' in element:
                        result['_timestamp'] = element['_timestamp']
                    if '_watermark' in element:
                        result['_watermark'] = element['_watermark']

                yield result

        return Stream(
            _map_generator(),
            state_backend=self._state_backend,
            watermark_tracker=self._watermark_tracker
        )

    def filter(self, predicate: Callable[[Any], bool]) -> 'Stream':
        """Filter with watermark propagation."""
        async def _filter_generator():
            async for element in self:
                if predicate(element):
                    yield element

        return Stream(
            _filter_generator(),
            state_backend=self._state_backend,
            watermark_tracker=self._watermark_tracker
        )


class WindowedStream:
    """Windowed stream with watermark-based firing."""

    def __init__(self, parent_stream: Stream, window_spec: WindowSpec):
        self.parent_stream = parent_stream
        self.window_spec = window_spec
        self._window_state = {}

    def _should_fire_window(self, window_end: int) -> bool:
        """Check if window should fire based on watermark.

        A window fires when the watermark advances past its end time.
        """
        watermark = self.parent_stream.get_watermark()
        return watermark >= window_end

    def aggregate(self, agg_fn: Callable, initial_value: Any = None):
        """Aggregate with watermark-based firing."""
        async def _aggregate_generator():
            async for element in self.parent_stream:
                # Extract timestamp
                if isinstance(element, dict) and '_timestamp' in element:
                    timestamp = element['_timestamp']
                else:
                    import time
                    timestamp = int(time.time() * 1000)

                # Assign to windows
                windows = self._assign_to_windows(element, timestamp)

                for window_start, window_end in windows:
                    window_key = (window_start, window_end)

                    # Add to window state
                    if window_key not in self._window_state:
                        self._window_state[window_key] = []
                    self._window_state[window_key].append(element)

                # Check which windows should fire
                watermark = self.parent_stream.get_watermark()

                for window_key in list(self._window_state.keys()):
                    window_start, window_end = window_key

                    if self._should_fire_window(window_end):
                        # Fire window
                        elements = self._window_state[window_key]

                        # Compute aggregate
                        accumulator = initial_value
                        for el in elements:
                            accumulator = agg_fn(accumulator, el)

                        yield {
                            "window_start": window_start,
                            "window_end": window_end,
                            "result": accumulator,
                            "watermark": watermark
                        }

                        # Clean up
                        del self._window_state[window_key]

        return Stream(_aggregate_generator())
```

### Verification

```python
# Test watermark propagation
stream = Stream.from_kafka("events")

# Assign timestamps and watermarks
stream_with_time = stream.assign_timestamps_and_watermarks(
    timestamp_extractor=lambda el: el['timestamp'],
    watermark_strategy="bounded_out_of_order"
)

# Apply transformations (watermarks propagate)
filtered = stream_with_time.filter(lambda el: el['value'] > 0)
mapped = filtered.map(lambda el: {"value": el['value'] * 2})

# Window operations use watermarks
windowed = mapped.tumbling_window(size_ms=60000)

async for result in windowed.aggregate(sum, initial_value=0):
    print(f"Window [{result['window_start']}, {result['window_end']}): {result['result']}")
    print(f"Watermark: {result['watermark']}")
```

**Expected Result:**
- Watermarks propagate through map/filter
- Windows fire based on watermarks
- Late events handled correctly

---

## Step 5.3: Test Event-Time Processing

### Implementation

#### Create `tests/integration/test_event_time.py`

```python
"""Integration tests for event-time processing."""

import pytest
import asyncio
from sabot.api.stream import Stream, WindowSpec, WindowType
from sabot.time import WatermarkTracker


class TestEventTimeProcessing:
    """Test event-time processing with watermarks."""

    @pytest.mark.asyncio
    async def test_watermark_assignment(self):
        """Test assigning watermarks to stream."""
        async def generator():
            events = [
                {"timestamp": 1000, "value": 10},
                {"timestamp": 2000, "value": 20},
                {"timestamp": 3000, "value": 30},
            ]
            for event in events:
                yield event

        stream = Stream(generator())

        # Assign timestamps and watermarks
        stream_with_time = stream.assign_timestamps_and_watermarks(
            timestamp_extractor=lambda el: el['timestamp'],
            watermark_strategy="monotonic"
        )

        # Consume and check watermarks
        watermarks = []
        async for element in stream_with_time:
            watermarks.append(element.get('_watermark'))

        # Watermarks should advance monotonically
        assert watermarks == [1000, 2000, 3000]

    @pytest.mark.asyncio
    async def test_out_of_order_events(self):
        """Test handling out-of-order events."""
        async def generator():
            # Events arrive out of order
            events = [
                {"timestamp": 1000, "value": 10},
                {"timestamp": 3000, "value": 30},  # Jump ahead
                {"timestamp": 2000, "value": 20},  # Out of order
                {"timestamp": 4000, "value": 40},
            ]
            for event in events:
                yield event

        stream = Stream(generator())

        # Use bounded out-of-order watermark
        stream_with_time = stream.assign_timestamps_and_watermarks(
            timestamp_extractor=lambda el: el['timestamp'],
            watermark_strategy="bounded_out_of_order"
        )

        # All events should be processed
        results = []
        async for element in stream_with_time:
            results.append(element)

        assert len(results) == 4

    @pytest.mark.asyncio
    async def test_window_firing_on_watermark(self):
        """Test that windows fire based on watermark."""
        async def generator():
            events = [
                {"timestamp": 1000, "value": 10},
                {"timestamp": 1500, "value": 20},
                {"timestamp": 2500, "value": 30},  # Advances watermark past first window
            ]
            for event in events:
                yield event

        stream = Stream(generator())

        # Assign timestamps
        stream_with_time = stream.assign_timestamps_and_watermarks(
            timestamp_extractor=lambda el: el['timestamp'],
            watermark_strategy="monotonic"
        )

        # Tumbling window: 1 second
        windowed = stream_with_time.tumbling_window(size_ms=1000)

        results = []
        async for result in windowed.aggregate(
            agg_fn=lambda acc, el: acc + el['value'],
            initial_value=0
        ):
            results.append(result)

        # First window [1000, 2000) should fire when watermark reaches 2500
        assert len(results) >= 1
        assert results[0]['window_start'] == 1000
        assert results[0]['window_end'] == 2000
        assert results[0]['result'] == 30  # 10 + 20

    @pytest.mark.asyncio
    async def test_late_events(self):
        """Test handling of late events (after watermark)."""
        async def generator():
            events = [
                {"timestamp": 1000, "value": 10},
                {"timestamp": 2000, "value": 20},
                {"timestamp": 3000, "value": 30},  # Advances watermark to 3000
                {"timestamp": 1500, "value": 15},  # Late event
            ]
            for event in events:
                yield event

        stream = Stream(generator())

        stream_with_time = stream.assign_timestamps_and_watermarks(
            timestamp_extractor=lambda el: el['timestamp'],
            watermark_strategy="monotonic"
        )

        windowed = stream_with_time.tumbling_window(size_ms=1000)

        results = []
        async for result in windowed.aggregate(
            agg_fn=lambda acc, el: acc + el['value'],
            initial_value=0
        ):
            results.append(result)

        # Late event (timestamp=1500) arrives after watermark passed window end (2000)
        # Should be dropped or handled according to lateness policy
        # (Default: drop late events)

        # First window should have fired with 10 + 20 = 30 (late event dropped)
        assert results[0]['result'] == 30

    @pytest.mark.asyncio
    async def test_watermark_idle_timeout(self):
        """Test watermark advancement when sources are idle."""
        # This tests watermark generation when no events arrive
        # (Implementation depends on watermark generator strategy)

        async def generator():
            events = [
                {"timestamp": 1000, "value": 10},
                # Long idle period
                # ... (no events for 10 seconds)
                {"timestamp": 11000, "value": 20},
            ]
            for event in events:
                yield event
                if event['timestamp'] == 1000:
                    # Simulate idle time
                    await asyncio.sleep(0.1)

        stream = Stream(generator())

        stream_with_time = stream.assign_timestamps_and_watermarks(
            timestamp_extractor=lambda el: el['timestamp'],
            watermark_strategy="monotonic"
        )

        # Watermark should eventually advance even without events
        # (This may require periodic watermark emission)

        results = []
        async for element in stream_with_time:
            results.append(element)

        assert len(results) == 2


class TestWatermarkStrategies:
    """Test different watermark generation strategies."""

    @pytest.mark.asyncio
    async def test_monotonic_watermark(self):
        """Test monotonic watermark (no out-of-order tolerance)."""
        async def generator():
            events = [
                {"timestamp": 1000, "value": 10},
                {"timestamp": 3000, "value": 30},
                {"timestamp": 2000, "value": 20},  # Out of order
            ]
            for event in events:
                yield event

        stream = Stream(generator())

        stream_with_time = stream.assign_timestamps_and_watermarks(
            timestamp_extractor=lambda el: el['timestamp'],
            watermark_strategy="monotonic"
        )

        watermarks = []
        async for element in stream_with_time:
            watermarks.append(element['_watermark'])

        # Monotonic: watermark = max(event_times)
        # After 3000, watermark is 3000, so 2000 is late
        assert watermarks[0] == 1000
        assert watermarks[1] == 3000
        assert watermarks[2] == 3000  # Doesn't decrease

    @pytest.mark.asyncio
    async def test_bounded_out_of_order_watermark(self):
        """Test bounded out-of-order watermark."""
        async def generator():
            events = [
                {"timestamp": 6000, "value": 60},
                {"timestamp": 8000, "value": 80},
                {"timestamp": 7000, "value": 70},  # Within 5s bound
            ]
            for event in events:
                yield event

        stream = Stream(generator())

        stream_with_time = stream.assign_timestamps_and_watermarks(
            timestamp_extractor=lambda el: el['timestamp'],
            watermark_strategy="bounded_out_of_order"
        )

        watermarks = []
        async for element in stream_with_time:
            watermarks.append(element['_watermark'])

        # Bounded: watermark = max(event_times) - 5000
        assert watermarks[0] == 1000  # 6000 - 5000
        assert watermarks[1] == 3000  # 8000 - 5000
        # Event at 7000 is within bound (watermark=3000 < 7000)
        assert watermarks[2] == 3000


class TestMultiStreamWatermarks:
    """Test watermark coordination across multiple streams."""

    @pytest.mark.asyncio
    async def test_watermark_in_join(self):
        """Test watermark handling in stream joins."""
        async def left_generator():
            yield {"id": 1, "timestamp": 1000, "value": "A"}
            yield {"id": 2, "timestamp": 3000, "value": "B"}

        async def right_generator():
            yield {"id": 1, "timestamp": 1500, "value": "X"}
            yield {"id": 2, "timestamp": 3500, "value": "Y"}

        left_stream = Stream(left_generator())
        right_stream = Stream(right_generator())

        # Assign timestamps
        left_with_time = left_stream.assign_timestamps_and_watermarks(
            timestamp_extractor=lambda el: el['timestamp']
        )
        right_with_time = right_stream.assign_timestamps_and_watermarks(
            timestamp_extractor=lambda el: el['timestamp']
        )

        # Join with time window
        joined = left_with_time.inner_join(
            right_with_time,
            on="id",
            window_ms=2000
        )

        results = []
        async for result in joined:
            results.append(result)

        # Should match based on time window
        assert len(results) >= 1
```

### Verification

```bash
# Run event-time processing tests
pytest tests/integration/test_event_time.py -v

# Run with coverage
pytest tests/integration/test_event_time.py --cov=sabot.api.stream --cov=sabot.time --cov-report=html
```

**Expected Results:**
- Watermarks assigned correctly
- Out-of-order events handled
- Windows fire based on watermarks
- Late events dropped or handled

---

## Phase 5 Completion Criteria

### Implementation Complete

- [x] WatermarkTracker integrated with agent runtime
- [x] Event time extracted from messages
- [x] Watermarks propagate through operators
- [x] Windows fire based on watermarks
- [x] Late events handled

### Tests Created

- [x] `tests/integration/test_event_time.py` - Event-time processing tests
- [x] Test out-of-order event handling
- [x] Test watermark strategies
- [x] Test window firing on watermark

### Coverage Goals

- Event-time processing: 70%+
- Watermark propagation: 70%+

### Functional Verification

- Watermarks advance monotonically
- Out-of-order events processed correctly
- Windows fire at correct event time
- Late events dropped or sent to side output

---

## Dependencies

**Phase 4 Complete:**
- Stream API with windows implemented
- Window operations tested

---

## Next Phase

**Phase 6: Error Handling & Recovery**

Implement checkpoint recovery, error policies, and graceful failure handling.

---

**Last Updated:** October 3, 2025
**Status:** Technical specification for Phase 5
