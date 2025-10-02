# Sabot → Flink Parity: Detailed Implementation Roadmap
## Achieving Enterprise-Grade Stream Processing in Python

---

## 🎯 **Mission: Match Apache Flink's Capabilities in Python**

Apache Flink is the gold standard for enterprise stream processing. To compete, Sabot needs to match Flink's:
- **Processing Guarantees**: Exactly-once semantics with distributed snapshots
- **State Management**: Fault-tolerant, scalable state with incremental checkpoints
- **Time Handling**: Event time, processing time, watermarks, late data handling
- **SQL Support**: Full SQL interface for stream processing
- **Performance**: Sub-second latency at millions of events/sec
- **Operations**: Web UI, metrics, savepoints, job management

**Current Status:** 60% implementation, strong foundation but missing critical Flink features

---

## 📊 **Flink Feature Comparison Matrix**

| Flink Feature | Sabot Current | Sabot Target | Priority | Effort |
|---------------|---------------|--------------|----------|---------|
| **Core Processing** |
| Exactly-Once Semantics | Framework only | ✅ Full | 🔴 P0 | 3 weeks |
| Event Time Processing | ❌ None | ✅ Full | 🔴 P0 | 2 weeks |
| Watermarks | ❌ None | ✅ Full | 🔴 P0 | 1 week |
| Checkpointing | Basic | ✅ Full | 🔴 P0 | 2 weeks |
| Savepoints | ❌ None | ✅ Full | 🟡 P1 | 1 week |
| **State Management** |
| Keyed State | Interface | ✅ Full | 🔴 P0 | 2 weeks |
| Operator State | ❌ None | ✅ Full | 🟡 P1 | 1 week |
| State Backends (RocksDB) | 45% | ✅ Full | 🔴 P0 | 2 weeks |
| Incremental Checkpoints | ❌ None | ✅ Full | 🟡 P1 | 1 week |
| Queryable State | ❌ None | ✅ Full | 🟠 P2 | 2 weeks |
| **Windowing** |
| Tumbling Windows | Basic | ✅ Full | 🟡 P1 | 1 week |
| Sliding Windows | Basic | ✅ Full | 🟡 P1 | 1 week |
| Session Windows | Basic | ✅ Full | 🟡 P1 | 1 week |
| Global Windows | ❌ None | ✅ Full | 🟠 P2 | 3 days |
| Custom Windows | ❌ None | ✅ Full | 🟠 P2 | 1 week |
| **Joins** |
| Stream-Stream Join | Interface | ✅ Full | 🟡 P1 | 2 weeks |
| Stream-Table Join | Interface | ✅ Full | 🟡 P1 | 1 week |
| Interval Join | ❌ None | ✅ Full | 🟡 P1 | 1 week |
| Temporal Join | ❌ None | ✅ Full | 🟠 P2 | 1 week |
| **SQL Interface** |
| SQL Parser | ❌ None | ✅ Full | 🟡 P1 | 2 weeks |
| Table API | ❌ None | ✅ Full | 🟡 P1 | 2 weeks |
| Catalogs | ❌ None | ✅ Full | 🟠 P2 | 1 week |
| **Connectors** |
| Kafka | Mocked | ✅ Full | 🔴 P0 | 1 week |
| File Systems | Basic | ✅ Full | 🟡 P1 | 1 week |
| JDBC | ❌ None | ✅ Full | 🟠 P2 | 1 week |
| Elasticsearch | ❌ None | ✅ Full | 🟠 P2 | 1 week |
| **Operations** |
| Web UI | Basic | ✅ Full | 🟡 P1 | 2 weeks |
| Metrics Dashboard | Partial | ✅ Full | 🟡 P1 | 1 week |
| Job Management | Basic | ✅ Full | 🔴 P0 | 1 week |
| Deployment (K8s) | ❌ None | ✅ Full | 🟡 P1 | 2 weeks |

**Total Estimated Effort:** ~40-45 weeks (single developer)
**With 2-3 developers:** ~15-20 weeks (4-5 months)

---

## 🚨 **Critical Missing Features (P0 - Must Have)**

### **1. EXACTLY-ONCE SEMANTICS**
**Status:** Framework exists, no actual implementation
**Flink Implementation:** Distributed snapshots (Chandy-Lamport algorithm)
**Impact:** This is THE defining feature of enterprise stream processing

#### **What Flink Does:**
```java
// Flink automatically ensures exactly-once processing
stream
    .keyBy(value -> value.userId)
    .process(new ProcessFunction() {
        @Override
        void processElement(Value value, Context ctx, Collector<Result> out) {
            // Flink guarantees this runs exactly once per element
            state.update(value);
            out.collect(process(value));
        }
    });
```

#### **What Sabot Needs to Implement:**

##### **A. Checkpoint Coordinator** (`sabot/core/checkpoint_coordinator.py`)
```python
class CheckpointCoordinator:
    """
    Distributed snapshot coordinator using Chandy-Lamport algorithm.

    Flink's approach:
    1. JobManager triggers checkpoint
    2. Checkpoint barriers flow through topology
    3. Operators snapshot state when barrier arrives
    4. Coordinator collects all snapshots
    5. Checkpoint confirmed when all operators complete
    """

    async def trigger_checkpoint(self, checkpoint_id: int) -> CheckpointResult:
        """
        Trigger distributed snapshot across all operators.

        Implementation:
        1. Generate checkpoint ID
        2. Inject barrier into all source streams
        3. Wait for all operators to complete snapshot
        4. Persist metadata to DFS/S3
        5. Confirm checkpoint success/failure
        """

    async def inject_barrier(self, checkpoint_id: int, source: str) -> None:
        """Inject checkpoint barrier into stream."""

    async def await_checkpoint_completion(self, checkpoint_id: int,
                                         timeout: float) -> bool:
        """Wait for all operators to complete checkpoint."""

    async def recover_from_checkpoint(self, checkpoint_id: int) -> None:
        """Restore all operator state from checkpoint."""
```

**Key Components:**
- **Barrier Injection**: Insert checkpoint markers into streams
- **Barrier Alignment**: Operators wait for barriers from all inputs
- **Snapshot Atomicity**: All-or-nothing snapshot success
- **Two-Phase Commit**: For external systems (Kafka, databases)

**Files to Create:**
```
sabot/core/
├── checkpoint_coordinator.py      # Main coordinator (800 LOC)
├── checkpoint_barrier.py          # Barrier injection/alignment (400 LOC)
├── checkpoint_storage.py          # Persistent storage (500 LOC)
└── two_phase_commit.py           # 2PC for external systems (600 LOC)
```

**Implementation Time:** 3 weeks
**Complexity:** 🔴 Very High - This is the hardest part

---

##### **B. Checkpoint Barriers** (`sabot/core/checkpoint_barrier.py`)
```python
@dataclass
class CheckpointBarrier:
    """
    Checkpoint barrier that flows through streams.

    Flink: Barriers are special records that separate epochs.
    All records before barrier belong to checkpoint N.
    All records after barrier belong to checkpoint N+1.
    """
    checkpoint_id: int
    timestamp: int
    is_cancellation: bool = False

class BarrierTracker:
    """
    Tracks barriers from multiple input channels.

    Flink: Operator must see barrier from ALL inputs before
    taking snapshot (barrier alignment).
    """

    def register_barrier(self, channel: int, barrier: CheckpointBarrier) -> bool:
        """Returns True when barriers from all channels received."""

    def should_block_channel(self, channel: int) -> bool:
        """Block fast channels until slow channels catch up."""
```

**Barrier Alignment Strategies:**
- **Exactly-Once**: Block fast inputs until all barriers arrive
- **At-Least-Once**: Don't block, process immediately (higher throughput)
- **Unaligned Checkpoints**: Buffer in-flight data (Flink 1.11+)

---

##### **C. State Backend with Snapshots** (`sabot/state/snapshotting_backend.py`)
```python
class SnapshotableStateBackend:
    """
    State backend that supports atomic snapshots.

    Flink supports:
    - MemoryStateBackend (for testing)
    - FsStateBackend (files)
    - RocksDBStateBackend (incremental checkpoints)
    """

    async def snapshot(self, checkpoint_id: int) -> StateSnapshot:
        """
        Create atomic snapshot of all state.

        RocksDB: Use native checkpoints for efficiency
        Memory: Deep copy all state
        """

    async def restore(self, snapshot: StateSnapshot) -> None:
        """Restore state from snapshot."""

    async def incremental_snapshot(self, checkpoint_id: int,
                                   base_checkpoint: int) -> DeltaSnapshot:
        """
        Incremental checkpoint (Flink RocksDB backend).

        Only snapshot changed data since last checkpoint.
        Dramatically reduces checkpoint time.
        """
```

**Files to Create/Enhance:**
```
sabot/state/
├── snapshotting_backend.py       # Snapshot interface (600 LOC)
├── rocksdb_snapshots.py          # RocksDB incremental (800 LOC)
├── fs_backend.py                 # File system backend (500 LOC)
└── state_handle.py               # State references (300 LOC)
```

**Implementation Time:** 2 weeks

---

### **2. EVENT TIME & WATERMARKS**
**Status:** Not implemented at all
**Flink Implementation:** Event time processing with watermark generators
**Impact:** Critical for correct time-based operations (windows, joins)

#### **What Flink Does:**
```java
// Flink uses event time from data, not wall clock
stream
    .assignTimestampsAndWatermarks(
        WatermarkStrategy
            .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
            .withTimestampAssigner((event, timestamp) -> event.timestamp)
    )
    .keyBy(e -> e.key)
    .window(TumblingEventTimeWindows.of(Time.seconds(60)))
    .reduce(new SumReducer());
```

#### **What Sabot Needs:**

##### **A. Time Characteristics** (`sabot/core/time.py`)
```python
class TimeCharacteristic(Enum):
    """Time semantics for stream processing."""
    PROCESSING_TIME = "processing_time"  # Wall clock time
    EVENT_TIME = "event_time"            # Time from data
    INGESTION_TIME = "ingestion_time"    # Time when entering system

class TimestampAssigner:
    """
    Extract timestamps from events.

    Flink: Users specify how to get timestamp from data.
    """

    def extract_timestamp(self, record: RecordBatch,
                         record_index: int) -> int:
        """
        Extract timestamp (milliseconds) from record.

        Example:
        - JSON: record['timestamp']
        - Arrow: record['event_time'].as_py()
        """

class WatermarkGenerator:
    """
    Generate watermarks for event time processing.

    Watermark(t) means: "No more events with timestamp < t will arrive"
    """

    def on_event(self, timestamp: int) -> Optional[Watermark]:
        """Called for each event. May emit watermark."""

    def on_periodic_emit(self) -> Optional[Watermark]:
        """Called periodically to emit watermark."""
```

**Watermark Strategies:**
```python
class BoundedOutOfOrdernessWatermarks(WatermarkGenerator):
    """
    Flink's most common watermark strategy.

    Accounts for late events up to max_out_of_orderness.
    Watermark = max_seen_timestamp - max_out_of_orderness
    """

    def __init__(self, max_out_of_orderness: timedelta):
        self.max_timestamp = 0
        self.max_delay = max_out_of_orderness

    def on_event(self, timestamp: int) -> Optional[Watermark]:
        self.max_timestamp = max(self.max_timestamp, timestamp)
        return Watermark(self.max_timestamp - self.max_delay.total_seconds() * 1000)

class MonotonousTimestampsWatermarks(WatermarkGenerator):
    """
    For strictly ordered streams.
    Watermark = current timestamp
    """
    pass

class PeriodicWatermarks(WatermarkGenerator):
    """
    Emit watermarks at fixed intervals (e.g., every second).
    """
    pass
```

##### **B. Watermark Handling** (`sabot/core/watermark.py`)
```python
@dataclass
class Watermark:
    """
    Watermark indicating progress of event time.

    timestamp: Milliseconds since epoch
    """
    timestamp: int

    @property
    def is_max(self) -> bool:
        """Special watermark indicating stream end."""
        return self.timestamp == sys.maxsize

class WatermarkTracker:
    """
    Track watermarks from multiple input streams.

    Flink rule: Output watermark = min(input watermarks)
    """

    def update_watermark(self, channel: int, watermark: Watermark) -> None:
        """Update watermark for input channel."""

    def get_current_watermark(self) -> Watermark:
        """Get minimum watermark across all inputs."""

    def is_late(self, timestamp: int) -> bool:
        """Check if event is late (timestamp < watermark)."""
```

##### **C. Late Data Handling** (`sabot/core/late_data.py`)
```python
class LateDataStrategy(Enum):
    """How to handle late events."""
    DROP = "drop"              # Discard late events
    SIDE_OUTPUT = "side_output"  # Send to separate stream
    ALLOWED_LATENESS = "allowed"  # Accept within lateness window

class LateDataHandler:
    """
    Handle events arriving after watermark.

    Flink: Windows can specify allowed lateness.
    Events within allowed lateness update window results.
    """

    def __init__(self, allowed_lateness: timedelta,
                 strategy: LateDataStrategy):
        self.allowed_lateness = allowed_lateness
        self.strategy = strategy

    def handle_late_event(self, event: Any, watermark: Watermark) -> LateAction:
        """Decide what to do with late event."""
```

**Files to Create:**
```
sabot/core/
├── time.py                    # Time characteristics (500 LOC)
├── watermark.py              # Watermark handling (400 LOC)
├── late_data.py              # Late data strategies (300 LOC)
└── timestamp_extractors.py   # Common extractors (200 LOC)
```

**Implementation Time:** 2 weeks
**Complexity:** 🟡 Medium-High

---

### **3. KEYED STATE MANAGEMENT**
**Status:** Interfaces exist, no real implementation
**Flink Implementation:** Partitioned state with efficient access
**Impact:** Essential for stateful processing (counting, aggregating, etc.)

#### **What Flink Provides:**

```java
// Flink provides typed state primitives
public class MyProcessFunction extends KeyedProcessFunction<String, Event, Result> {

    // Value state: Single value per key
    private ValueState<Long> count;

    // List state: List of values per key
    private ListState<Event> events;

    // Map state: Key-value map per key
    private MapState<String, Integer> userStats;

    @Override
    public void open(Configuration config) {
        // State is automatically partitioned by key
        count = getRuntimeContext().getState(
            new ValueStateDescriptor<>("count", Long.class)
        );
    }

    @Override
    public void processElement(Event event, Context ctx, Collector<Result> out) {
        // Access state for current key
        Long currentCount = count.value();
        count.update(currentCount + 1);
    }
}
```

#### **What Sabot Needs:**

##### **A. State Primitives** (`sabot/state/state_primitives.py`)
```python
class ValueState(Generic[T]):
    """
    Single value per key.

    Flink: Most common state type.
    Efficient for single values (counters, flags, etc.)
    """

    async def value(self) -> Optional[T]:
        """Get current value for current key."""

    async def update(self, value: T) -> None:
        """Set value for current key."""

    async def clear(self) -> None:
        """Clear value for current key."""

class ListState(Generic[T]):
    """
    List of values per key.

    Flink: For accumulating multiple values per key.
    """

    async def get(self) -> List[T]:
        """Get all values for current key."""

    async def add(self, value: T) -> None:
        """Append value to list for current key."""

    async def update(self, values: List[T]) -> None:
        """Replace entire list for current key."""

    async def add_all(self, values: List[T]) -> None:
        """Append multiple values."""

class MapState(Generic[K, V]):
    """
    Key-value map per key.

    Flink: For maintaining complex state structures.
    """

    async def get(self, key: K) -> Optional[V]:
        """Get value for map key."""

    async def put(self, key: K, value: V) -> None:
        """Set value for map key."""

    async def contains(self, key: K) -> bool:
        """Check if map contains key."""

    async def remove(self, key: K) -> None:
        """Remove key from map."""

    async def entries(self) -> AsyncIterator[Tuple[K, V]]:
        """Iterate over all entries."""

class ReducingState(Generic[T]):
    """
    State that aggregates values using reduce function.

    Flink: Memory-efficient aggregation.
    """

    async def add(self, value: T) -> None:
        """Add value, apply reduce function."""

    async def get(self) -> Optional[T]:
        """Get reduced value."""

class AggregatingState(Generic[IN, OUT]):
    """
    State that aggregates with custom aggregator.

    Flink: More flexible than ReducingState.
    """

    async def add(self, value: IN) -> None:
        """Add value to aggregator."""

    async def get(self) -> OUT:
        """Get aggregated result."""
```

##### **B. State Backend** (`sabot/state/keyed_state_backend.py`)
```python
class KeyedStateBackend:
    """
    Backend for keyed state storage.

    Flink: Abstracts storage (memory, RocksDB, etc.)
    """

    def __init__(self, key_serializer: TypeSerializer,
                 state_backend: StateBackend):
        self.current_key: Optional[Any] = None
        self.key_serializer = key_serializer
        self.backend = state_backend

    def set_current_key(self, key: Any) -> None:
        """Set key for current context."""
        self.current_key = key

    async def get_value_state(self, name: str,
                             serializer: TypeSerializer) -> ValueState:
        """Create value state handle."""

    async def get_list_state(self, name: str,
                            serializer: TypeSerializer) -> ListState:
        """Create list state handle."""

    async def snapshot_keyed_state(self, checkpoint_id: int) -> KeyedSnapshot:
        """Snapshot all state for all keys."""
```

##### **C. State Descriptor** (`sabot/state/state_descriptor.py`)
```python
@dataclass
class StateDescriptor(Generic[T]):
    """
    Describes state structure.

    Flink: Declares state before use.
    """
    name: str
    type_info: TypeInfo[T]
    default_value: Optional[T] = None
    ttl_config: Optional[StateTtlConfig] = None

@dataclass
class StateTtlConfig:
    """
    Time-to-live configuration for state.

    Flink: Automatically clean up old state.
    """
    ttl: timedelta
    update_type: TtlUpdateType  # OnCreateAndWrite, OnReadAndWrite
    state_visibility: TtlStateVisibility  # NeverReturnExpired, ReturnExpiredIfNotCleanedUp
    cleanup_strategy: TtlCleanupStrategy
```

**Files to Create:**
```
sabot/state/
├── state_primitives.py       # State types (800 LOC)
├── keyed_state_backend.py    # Backend impl (1000 LOC)
├── state_descriptor.py       # Descriptors (300 LOC)
├── state_ttl.py             # TTL handling (400 LOC)
└── state_serializers.py     # Efficient serialization (500 LOC)
```

**Implementation Time:** 2 weeks
**Complexity:** 🟡 Medium

---

### **4. REAL KAFKA CONNECTOR**
**Status:** Mocked/simulated
**Flink Implementation:** Full Kafka producer/consumer with exactly-once
**Impact:** Can't process real data without this

#### **What Flink Provides:**

```java
// Flink Kafka Consumer with exactly-once
FlinkKafkaConsumer<Event> consumer = new FlinkKafkaConsumer<>(
    "my-topic",
    new JsonDeserializationSchema<>(Event.class),
    properties
);
consumer.setStartFromEarliest();
consumer.setCommitOffsetsOnCheckpoints(true);  // Exactly-once

// Flink Kafka Producer with exactly-once
FlinkKafkaProducer<Event> producer = new FlinkKafkaProducer<>(
    "output-topic",
    new JsonSerializationSchema<>(Event.class),
    properties,
    FlinkKafkaProducer.Semantic.EXACTLY_ONCE  // Requires Kafka 0.11+
);
```

#### **What Sabot Needs:**

##### **A. Kafka Source** (`sabot/connectors/kafka/source.py`)
```python
class KafkaSource:
    """
    Kafka consumer integrated with checkpointing.

    Must support:
    - Partition assignment and rebalancing
    - Offset management with checkpoints
    - Exactly-once semantics
    """

    def __init__(self,
                 bootstrap_servers: str,
                 topic: str,
                 group_id: str,
                 value_deserializer: Deserializer,
                 start_from: StartPosition = StartPosition.LATEST):
        self.consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            enable_auto_commit=False,  # Manual commit for exactly-once
            auto_offset_reset=start_from.value
        )

    async def read_batches(self) -> AsyncIterator[RecordBatch]:
        """
        Read batches from Kafka.

        Implementation:
        1. Poll messages from Kafka
        2. Convert to Arrow RecordBatch
        3. Track offsets for checkpoint
        """

    async def snapshot_offsets(self, checkpoint_id: int) -> OffsetSnapshot:
        """
        Snapshot current offsets for checkpoint.

        Flink: Called when checkpoint barrier arrives.
        """

    async def restore_offsets(self, snapshot: OffsetSnapshot) -> None:
        """Restore offsets from checkpoint."""

    async def commit_offsets(self, checkpoint_id: int) -> None:
        """
        Commit offsets to Kafka after checkpoint completes.

        Flink: Only commit after checkpoint is confirmed.
        """
```

##### **B. Kafka Sink** (`sabot/connectors/kafka/sink.py`)
```python
class KafkaSink:
    """
    Kafka producer with exactly-once semantics.

    Uses Kafka transactions (requires 0.11+).
    """

    def __init__(self,
                 bootstrap_servers: str,
                 topic: str,
                 value_serializer: Serializer,
                 semantic: DeliveryGuarantee = DeliveryGuarantee.EXACTLY_ONCE):
        self.producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            enable_idempotence=True,  # For exactly-once
            transactional_id=f"sabot-{uuid.uuid4()}"  # For transactions
        )
        self.semantic = semantic

    async def write_batch(self, batch: RecordBatch) -> None:
        """Write batch to Kafka."""

    async def snapshot_state(self, checkpoint_id: int) -> SinkSnapshot:
        """
        Snapshot producer state.

        For exactly-once: Begin Kafka transaction.
        """

    async def commit_transaction(self, checkpoint_id: int) -> None:
        """
        Commit Kafka transaction.

        Flink: Only commit after checkpoint completes.
        """

    async def abort_transaction(self, checkpoint_id: int) -> None:
        """Abort transaction on checkpoint failure."""
```

##### **C. Offset Management** (`sabot/connectors/kafka/offsets.py`)
```python
@dataclass
class OffsetSnapshot:
    """Snapshot of Kafka consumer offsets."""
    checkpoint_id: int
    offsets: Dict[TopicPartition, int]
    timestamp: int

class OffsetManager:
    """
    Manage Kafka offsets for exactly-once processing.

    Flink: Offsets are checkpointed and only committed
    after checkpoint completes successfully.
    """

    def __init__(self, group_id: str):
        self.pending_commits: Dict[int, OffsetSnapshot] = {}

    async def prepare_commit(self, checkpoint_id: int,
                            offsets: Dict[TopicPartition, int]) -> None:
        """Store offsets to commit after checkpoint."""

    async def commit_checkpoint(self, checkpoint_id: int) -> None:
        """Commit offsets for completed checkpoint."""

    async def abort_checkpoint(self, checkpoint_id: int) -> None:
        """Discard offsets for failed checkpoint."""
```

**Files to Create:**
```
sabot/connectors/kafka/
├── source.py                 # Kafka source (800 LOC)
├── sink.py                   # Kafka sink (700 LOC)
├── offsets.py               # Offset management (400 LOC)
├── partitioner.py           # Custom partitioning (300 LOC)
└── exactly_once.py          # Exactly-once coordinator (500 LOC)
```

**Implementation Time:** 1 week
**Complexity:** 🟡 Medium

---

## 🟡 **Important Missing Features (P1 - Should Have)**

### **5. ADVANCED WINDOWING**
**Status:** Basic framework only
**Flink Implementation:** Complete windowing with triggers and evictors

#### **What Flink Provides:**

```java
// Flink has sophisticated windowing
stream
    .keyBy(e -> e.key)
    .window(TumblingEventTimeWindows.of(Time.seconds(60)))
    .allowedLateness(Time.minutes(1))  // Handle late data
    .sideOutputLateData(lateDataTag)   // Capture late data
    .trigger(ContinuousEventTimeTrigger.of(Time.seconds(10)))  // Early results
    .reduce(new SumReducer());
```

#### **What Sabot Needs:**

##### **A. Window Implementation** (`sabot/windowing/windows.py`)
```python
class Window(ABC):
    """Base class for all window types."""

    @abstractmethod
    def assign_window(self, timestamp: int) -> List[WindowId]:
        """Assign element to window(s)."""

class TumblingEventTimeWindow(Window):
    """
    Non-overlapping fixed-size windows.

    [0-60) [60-120) [120-180) ...
    """

    def __init__(self, size: timedelta):
        self.size_ms = int(size.total_seconds() * 1000)

    def assign_window(self, timestamp: int) -> List[WindowId]:
        window_start = (timestamp // self.size_ms) * self.size_ms
        return [WindowId(window_start, window_start + self.size_ms)]

class SlidingEventTimeWindow(Window):
    """
    Overlapping windows with slide interval.

    Size=60s, Slide=30s:
    [0-60) [30-90) [60-120) [90-150) ...
    """

    def __init__(self, size: timedelta, slide: timedelta):
        self.size_ms = int(size.total_seconds() * 1000)
        self.slide_ms = int(slide.total_seconds() * 1000)

    def assign_window(self, timestamp: int) -> List[WindowId]:
        """Element belongs to multiple windows."""
        windows = []
        last_start = (timestamp // self.slide_ms) * self.slide_ms
        for start in range(last_start - self.size_ms + self.slide_ms,
                          last_start + 1,
                          self.slide_ms):
            if start <= timestamp < start + self.size_ms:
                windows.append(WindowId(start, start + self.size_ms))
        return windows

class SessionWindow(Window):
    """
    Dynamic windows based on activity gaps.

    Gap=30s: [0-45) [60-95) [120-180) ...
    (windows end after 30s of inactivity)
    """

    def __init__(self, gap: timedelta):
        self.gap_ms = int(gap.total_seconds() * 1000)

    def merge_windows(self, windows: List[WindowId]) -> List[WindowId]:
        """Merge overlapping session windows."""

class GlobalWindow(Window):
    """Single window containing all elements."""

    def assign_window(self, timestamp: int) -> List[WindowId]:
        return [WindowId.GLOBAL]
```

##### **B. Window Triggers** (`sabot/windowing/triggers.py`)
```python
class Trigger(ABC):
    """
    Determines when to evaluate window.

    Flink: Triggers decide when to "fire" window computation.
    """

    @abstractmethod
    async def on_element(self, element: Any, timestamp: int,
                        window: WindowId, ctx: TriggerContext) -> TriggerResult:
        """Called for each element."""

    @abstractmethod
    async def on_event_time(self, time: int, window: WindowId,
                           ctx: TriggerContext) -> TriggerResult:
        """Called when event time timer fires."""

    @abstractmethod
    async def on_processing_time(self, time: int, window: WindowId,
                                ctx: TriggerContext) -> TriggerResult:
        """Called when processing time timer fires."""

class TriggerResult(Enum):
    """What to do when trigger fires."""
    CONTINUE = "continue"      # Do nothing
    FIRE = "fire"             # Evaluate window
    PURGE = "purge"           # Clear window state
    FIRE_AND_PURGE = "fire_and_purge"  # Both

class EventTimeTrigger(Trigger):
    """
    Fires when watermark passes window end.

    Flink default for event time windows.
    """

    async def on_element(self, element, timestamp, window, ctx):
        # Register timer for window end
        await ctx.register_event_time_timer(window.max_timestamp)
        return TriggerResult.CONTINUE

    async def on_event_time(self, time, window, ctx):
        if time >= window.max_timestamp:
            return TriggerResult.FIRE

class ProcessingTimeTrigger(Trigger):
    """Fires based on wall clock."""
    pass

class CountTrigger(Trigger):
    """Fires after N elements."""
    pass

class ContinuousEventTimeTrigger(Trigger):
    """
    Fires repeatedly at intervals (early results).

    Flink: For getting early results before watermark.
    """
    pass
```

##### **C. Window Operators** (`sabot/windowing/operators.py`)
```python
class WindowOperator:
    """
    Operator that applies windows to keyed stream.

    Flink: Core windowing logic.
    """

    def __init__(self,
                 window_assigner: Window,
                 trigger: Trigger,
                 function: WindowFunction,
                 allowed_lateness: timedelta = timedelta(0)):
        self.window_assigner = window_assigner
        self.trigger = trigger
        self.function = function
        self.allowed_lateness_ms = int(allowed_lateness.total_seconds() * 1000)

        # Window state per key
        self.window_buffers: Dict[Tuple[Key, WindowId], List[Any]] = {}
        self.watermark = Watermark(0)

    async def process_element(self, element: Any, timestamp: int) -> None:
        """
        Process element with windowing.

        Steps:
        1. Assign to window(s)
        2. Add to window buffer
        3. Check trigger
        4. Fire if needed
        """

    async def process_watermark(self, watermark: Watermark) -> None:
        """
        Advance watermark and fire ready windows.

        Steps:
        1. Fire all windows with end < watermark
        2. Purge windows outside allowed lateness
        """

    async def fire_window(self, key: Any, window: WindowId) -> None:
        """Evaluate window function and emit results."""
```

**Files to Create:**
```
sabot/windowing/
├── windows.py               # Window types (600 LOC)
├── triggers.py              # Trigger logic (800 LOC)
├── operators.py             # Window operators (1000 LOC)
├── evictors.py             # Evictor policies (300 LOC)
└── late_data_handling.py   # Late data logic (400 LOC)
```

**Implementation Time:** 3 weeks
**Complexity:** 🟡 Medium-High

---

### **6. STREAM JOINS**
**Status:** Interfaces only, no implementation
**Flink Implementation:** Multiple join types with time constraints

#### **What Flink Provides:**

```java
// Stream-Stream Join (windowed)
DataStream<Result> joined = stream1
    .join(stream2)
    .where(e1 -> e1.key)
    .equalTo(e2 -> e2.key)
    .window(TumblingEventTimeWindows.of(Time.seconds(60)))
    .apply((e1, e2) -> new Result(e1, e2));

// Interval Join (time-bounded)
DataStream<Result> intervalJoined = stream1
    .keyBy(e -> e.key)
    .intervalJoin(stream2.keyBy(e -> e.key))
    .between(Time.seconds(-5), Time.seconds(5))  // Join within 5s window
    .process(new ProcessJoinFunction());

// Stream-Table Join (lookup)
DataStream<Enriched> enriched = stream
    .join(table)
    .where(e -> e.userId)
    .equalTo(u -> u.id)
    .apply((event, user) -> new Enriched(event, user));
```

#### **What Sabot Needs:**

##### **A. Windowed Join** (`sabot/joins/windowed_join.py`)
```python
class WindowedStreamJoin:
    """
    Join two streams within same windows.

    Flink: Both streams must be windowed with same window assigner.
    """

    def __init__(self,
                 window: Window,
                 join_type: JoinType,
                 key_selector_left: Callable,
                 key_selector_right: Callable):
        self.window = window
        self.join_type = join_type
        self.left_key = key_selector_left
        self.right_key = key_selector_right

        # Buffer elements by key and window
        self.left_buffer: Dict[Tuple[Key, WindowId], List] = {}
        self.right_buffer: Dict[Tuple[Key, WindowId], List] = {}

    async def process_left(self, element: Any, timestamp: int) -> None:
        """Buffer left stream element."""

    async def process_right(self, element: Any, timestamp: int) -> None:
        """Buffer right stream element."""

    async def on_watermark(self, watermark: Watermark) -> AsyncIterator[Tuple]:
        """
        Fire windows and emit join results.

        Steps:
        1. Identify complete windows
        2. Perform join (nested loop, hash join, etc.)
        3. Emit results
        4. Clean up buffers
        """

    def _hash_join(self, left_elements: List, right_elements: List) -> List[Tuple]:
        """Efficient hash join implementation."""
```

##### **B. Interval Join** (`sabot/joins/interval_join.py`)
```python
class IntervalJoin:
    """
    Join streams within time interval.

    Flink: Most efficient for stream-stream joins.
    Only maintains relevant time window of data.
    """

    def __init__(self,
                 lower_bound: timedelta,
                 upper_bound: timedelta,
                 key_selector_left: Callable,
                 key_selector_right: Callable):
        self.lower_ms = int(lower_bound.total_seconds() * 1000)
        self.upper_ms = int(upper_bound.total_seconds() * 1000)

        # Sorted buffer by timestamp
        self.left_buffer: Dict[Key, SortedList[TimestampedElement]] = {}
        self.right_buffer: Dict[Key, SortedList[TimestampedElement]] = {}

    async def process_left(self, element: Any, timestamp: int) -> AsyncIterator[Tuple]:
        """
        Process left element and join with right.

        For each right element where:
        left_timestamp + lower_bound <= right_timestamp <= left_timestamp + upper_bound
        """

    async def process_right(self, element: Any, timestamp: int) -> AsyncIterator[Tuple]:
        """Process right element and join with left."""

    async def cleanup_old_data(self, watermark: Watermark) -> None:
        """Remove elements outside join interval."""
```

##### **C. Stream-Table Join** (`sabot/joins/stream_table_join.py`)
```python
class StreamTableJoin:
    """
    Join stream with static/slowly-changing table.

    Flink: Table is typically cached or from external DB.
    """

    def __init__(self,
                 table: Table,
                 key_selector_stream: Callable,
                 key_selector_table: Callable,
                 join_type: JoinType = JoinType.INNER):
        self.table = table
        self.stream_key = key_selector_stream
        self.table_key = key_selector_table
        self.join_type = join_type

        # Table data cache
        self.table_data: Dict[Key, Any] = {}

    async def process_stream(self, element: Any) -> Optional[Tuple]:
        """
        Join stream element with table.

        Steps:
        1. Extract key from stream element
        2. Lookup key in table
        3. Emit joined result (if join matches)
        """
```

**Files to Create:**
```
sabot/joins/
├── windowed_join.py         # Windowed joins (800 LOC)
├── interval_join.py         # Interval joins (700 LOC)
├── stream_table_join.py     # Stream-table joins (500 LOC)
├── temporal_join.py         # Temporal joins (600 LOC)
└── join_strategies.py       # Hash/sort-merge join (600 LOC)
```

**Implementation Time:** 2 weeks
**Complexity:** 🟡 Medium

---

### **7. TABLE API & SQL**
**Status:** Not implemented
**Flink Implementation:** Full SQL support with query optimizer
**Impact:** Major usability feature for analysts

#### **What Flink Provides:**

```java
// Flink Table API
Table events = tableEnv.fromDataStream(stream, $("userId"), $("amount"), $("timestamp"));

Table result = events
    .window(Tumble.over(lit(60).seconds()).on($("timestamp")).as("w"))
    .groupBy($("userId"), $("w"))
    .select($("userId"), $("amount").sum().as("total"));

// Flink SQL
tableEnv.executeSql("""
    CREATE TABLE events (
        userId STRING,
        amount BIGINT,
        ts TIMESTAMP(3),
        WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'events',
        'properties.bootstrap.servers' = 'localhost:9092'
    )
""");

Table result = tableEnv.sqlQuery("""
    SELECT
        userId,
        TUMBLE_END(ts, INTERVAL '60' SECOND) as window_end,
        SUM(amount) as total
    FROM events
    GROUP BY userId, TUMBLE(ts, INTERVAL '60' SECOND)
""");
```

#### **What Sabot Needs:**

##### **A. SQL Parser** (`sabot/sql/parser.py`)
```python
class SQLParser:
    """
    Parse SQL into execution plan.

    Options:
    1. Use existing parser (sqlglot, sqlparse)
    2. Build custom parser for streaming SQL
    """

    def parse(self, sql: str) -> SQLStatement:
        """Parse SQL string into AST."""

    def validate(self, statement: SQLStatement) -> List[ValidationError]:
        """Validate SQL semantics."""

class StreamingSQLExtensions:
    """
    Streaming-specific SQL syntax.

    Flink extensions:
    - WATERMARK clause
    - TUMBLE, HOP, SESSION window functions
    - FOR SYSTEM_TIME AS OF (temporal joins)
    """

    def parse_watermark_clause(self, clause: str) -> WatermarkStrategy:
        """WATERMARK FOR ts AS ts - INTERVAL '5' SECOND"""

    def parse_window_function(self, func: str) -> WindowFunction:
        """TUMBLE(ts, INTERVAL '60' SECOND)"""
```

##### **B. Query Planner** (`sabot/sql/planner.py`)
```python
class QueryPlanner:
    """
    Convert SQL to physical execution plan.

    Steps:
    1. Logical plan (from AST)
    2. Optimization (push filters, etc.)
    3. Physical plan (actual operators)
    """

    def create_logical_plan(self, statement: SQLStatement) -> LogicalPlan:
        """Create logical operator tree."""

    def optimize(self, logical: LogicalPlan) -> LogicalPlan:
        """Apply optimization rules."""

    def create_physical_plan(self, logical: LogicalPlan) -> PhysicalPlan:
        """Convert to executable operators."""

class OptimizationRule(ABC):
    """Base class for query optimizations."""

    @abstractmethod
    def apply(self, plan: LogicalPlan) -> Optional[LogicalPlan]:
        """Apply optimization rule."""

class FilterPushdownRule(OptimizationRule):
    """Push filters closer to source."""
    pass

class ProjectionPushdownRule(OptimizationRule):
    """Push projections to source."""
    pass

class JoinReorderRule(OptimizationRule):
    """Reorder joins for efficiency."""
    pass
```

##### **C. Table API** (`sabot/table/table_api.py`)
```python
class Table:
    """
    Programmatic table API.

    Flink: Fluent API for building queries.
    """

    def select(self, *fields: str) -> 'Table':
        """Select columns."""

    def where(self, condition: str) -> 'Table':
        """Filter rows."""

    def group_by(self, *keys: str) -> 'GroupedTable':
        """Group by keys."""

    def join(self, other: 'Table') -> 'JoinedTable':
        """Join with another table."""

    def window(self, window: WindowSpec) -> 'WindowedTable':
        """Apply windowing."""

    def to_datastream(self) -> DataStream:
        """Convert to DataStream."""

class GroupedTable:
    """Table grouped by keys."""

    def select(self, *aggregations: str) -> Table:
        """Apply aggregations."""

class WindowedTable:
    """Table with windowing."""

    def group_by(self, *keys: str) -> 'WindowedGroupedTable':
        """Group by keys within windows."""
```

**Files to Create:**
```
sabot/sql/
├── parser.py                # SQL parsing (1200 LOC)
├── planner.py              # Query planning (1500 LOC)
├── optimizer.py            # Query optimization (1000 LOC)
├── executor.py             # Plan execution (800 LOC)
└── catalog.py              # Table metadata (600 LOC)

sabot/table/
├── table_api.py            # Table API (800 LOC)
├── expressions.py          # Expression DSL (600 LOC)
└── functions.py            # Built-in functions (1000 LOC)
```

**Implementation Time:** 4 weeks
**Complexity:** 🔴 High

---

### **8. WEB UI & OPERATIONS**
**Status:** Basic CLI, no web UI
**Flink Implementation:** Comprehensive web dashboard
**Impact:** Essential for operations and debugging

#### **What Flink Provides:**

- Job submission and management
- Real-time metrics and monitoring
- Job graph visualization
- Task manager details
- Checkpoint statistics
- Watermark progress
- Backpressure monitoring
- Log viewing

#### **What Sabot Needs:**

##### **A. REST API** (`sabot/web/api.py`)
```python
@dataclass
class JobInfo:
    """Job information."""
    job_id: str
    name: str
    state: JobState
    start_time: int
    parallelism: int
    vertices: List[VertexInfo]

class SabotAPI:
    """
    REST API for Sabot cluster.

    Flink endpoints to implement:
    - GET /jobs - List jobs
    - POST /jobs - Submit job
    - GET /jobs/:id - Job details
    - PATCH /jobs/:id - Cancel/stop job
    - GET /jobs/:id/metrics - Job metrics
    - GET /jobs/:id/checkpoints - Checkpoint history
    - GET /taskmanagers - List task managers
    - GET /overview - Cluster overview
    """

    @app.get("/jobs")
    async def list_jobs(self) -> List[JobInfo]:
        """List all jobs."""

    @app.post("/jobs")
    async def submit_job(self, jar: bytes, config: JobConfig) -> JobId:
        """Submit new job."""

    @app.get("/jobs/{job_id}")
    async def get_job(self, job_id: str) -> JobInfo:
        """Get job details."""

    @app.get("/jobs/{job_id}/metrics")
    async def get_job_metrics(self, job_id: str) -> JobMetrics:
        """Get job metrics."""
```

##### **B. Web Frontend** (`sabot/web/frontend/`)
```typescript
// React/Vue.js dashboard
// Pages needed:
// - Job overview
// - Job details with graph visualization
// - Metrics dashboard
// - Checkpoint monitoring
// - Task manager view
// - Configuration view
```

##### **C. Job Management** (`sabot/web/job_manager.py`)
```python
class JobManager:
    """
    Manage job lifecycle.

    Flink: JobManager coordinates job execution.
    """

    async def submit_job(self, job: JobGraph, config: JobConfig) -> JobId:
        """Submit job to cluster."""

    async def cancel_job(self, job_id: str, mode: CancelMode) -> None:
        """Cancel running job."""

    async def stop_job(self, job_id: str, savepoint: bool = True) -> None:
        """Stop with savepoint."""

    async def restart_job(self, job_id: str) -> None:
        """Restart job."""
```

**Files to Create:**
```
sabot/web/
├── api.py                   # REST API (1200 LOC)
├── job_manager.py          # Job management (800 LOC)
├── metrics_api.py          # Metrics endpoints (600 LOC)
└── frontend/               # Web UI (5000+ LOC)
    ├── components/
    ├── pages/
    └── services/
```

**Implementation Time:** 3 weeks (backend) + 2 weeks (frontend)
**Complexity:** 🟡 Medium (backend), 🟡 Medium-High (frontend)

---

## 🟠 **Nice-to-Have Features (P2 - Can Wait)**

### **9. QUERYABLE STATE**
Allow external systems to query current state

### **10. SAVEPOINTS**
User-triggered checkpoints for upgrades/migration

### **11. SIDE OUTPUTS**
Send elements to multiple outputs

### **12. ASYNC I/O**
Asynchronous external lookups

### **13. CEP (Complex Event Processing)**
Pattern matching on streams

---

## 📅 **Detailed Implementation Timeline**

### **Month 1-2: Core Foundation (P0 Features)**
```
Week 1-3: Exactly-Once Semantics
├── Checkpoint coordinator (800 LOC)
├── Barrier injection/alignment (400 LOC)
├── Snapshotting backends (1500 LOC)
└── Testing & validation

Week 4-5: Event Time & Watermarks
├── Time characteristics (500 LOC)
├── Watermark generators (400 LOC)
├── Late data handling (300 LOC)
└── Testing & validation

Week 6-7: Keyed State
├── State primitives (800 LOC)
├── State backend (1000 LOC)
├── State descriptors (300 LOC)
└── Testing & validation

Week 8: Kafka Connector
├── Source implementation (800 LOC)
├── Sink implementation (700 LOC)
├── Exactly-once integration (500 LOC)
└── Testing & validation
```

### **Month 3-4: Advanced Features (P1 Features)**
```
Week 9-11: Windowing
├── Window types (600 LOC)
├── Triggers (800 LOC)
├── Window operators (1000 LOC)
└── Testing & validation

Week 12-13: Stream Joins
├── Windowed join (800 LOC)
├── Interval join (700 LOC)
├── Stream-table join (500 LOC)
└── Testing & validation

Week 14-17: SQL Support
├── SQL parser (1200 LOC)
├── Query planner (1500 LOC)
├── Query optimizer (1000 LOC)
├── Table API (800 LOC)
└── Testing & validation

Week 18-20: Web UI & Operations
├── REST API (1200 LOC)
├── Job management (800 LOC)
├── Web frontend (5000 LOC)
└── Testing & validation
```

### **Month 5: Integration & Polish**
```
Week 21-22: Integration
├── Wire all components together
├── End-to-end testing
└── Performance tuning

Week 23-24: Documentation & Examples
├── Complete API documentation
├── Tutorial examples
├── Migration guides
└── Deployment guides
```

---

## 📊 **Success Metrics**

### **Performance Benchmarks**
| Metric | Flink (Java) | Sabot Target | Validation |
|--------|--------------|--------------|------------|
| Throughput | 1M msg/sec | 500K msg/sec | Kafka benchmark |
| Latency (p99) | 50ms | 100ms | End-to-end test |
| Checkpoint Time | 1-5s | 2-10s | State snapshot |
| State Size | 100GB+ | 50GB+ | RocksDB backend |
| Recovery Time | 10-30s | 20-60s | Failure test |

### **Feature Completeness**
- ✅ Exactly-once semantics: Full implementation
- ✅ Event time: Complete watermark support
- ✅ State management: All state types
- ✅ Windowing: All window types
- ✅ Joins: All join types
- ✅ SQL: Basic SQL support
- ✅ Operations: Web UI & management

---

## 🎯 **Implementation Priority Order**

1. **Phase 1 (Weeks 1-8): Foundation - Must Have**
   - Exactly-once semantics
   - Event time & watermarks
   - Keyed state management
   - Real Kafka connector

2. **Phase 2 (Weeks 9-17): Advanced Features - Should Have**
   - Complete windowing
   - Stream joins
   - SQL support

3. **Phase 3 (Weeks 18-20): Operations - Should Have**
   - Web UI
   - Job management
   - Monitoring dashboard

4. **Phase 4 (Weeks 21-24): Polish - Must Have**
   - Integration testing
   - Performance optimization
   - Documentation
   - Examples

---

## 🚀 **Getting Started Checklist**

### **Immediate Next Steps (This Week)**
- [ ] Set up development environment for Flink parity work
- [ ] Create feature branches for each major component
- [ ] Write detailed design docs for checkpoint coordinator
- [ ] Implement checkpoint barrier proto (proof of concept)
- [ ] Set up integration test framework

### **This Month**
- [ ] Complete checkpoint coordinator implementation
- [ ] Add watermark support to stream engine
- [ ] Implement basic keyed state primitives
- [ ] Create Kafka connector (read-only first)
- [ ] Write comprehensive tests for each component

### **Next Quarter**
- [ ] Achieve exactly-once semantics
- [ ] Complete state management system
- [ ] Implement all window types
- [ ] Add basic SQL support
- [ ] Build web UI foundation

---

## 💡 **Key Design Decisions**

### **1. Checkpoint Algorithm**
**Decision:** Use Flink's distributed snapshot (Chandy-Lamport)
**Rationale:** Proven algorithm, handles complex topologies
**Alternative:** Periodic state dumps (simpler but less flexible)

### **2. State Backend**
**Decision:** RocksDB as primary backend
**Rationale:** Handles large state, incremental checkpoints
**Alternative:** Pure memory (faster but limited scale)

### **3. Watermark Generation**
**Decision:** Per-partition watermark generation
**Rationale:** Better handling of slow partitions
**Alternative:** Global watermark (simpler but less accurate)

### **4. SQL Engine**
**Decision:** Use sqlglot for parsing, custom planner
**Rationale:** Fast integration, full control over optimization
**Alternative:** Build from scratch (more work, more flexible)

### **5. Web UI**
**Decision:** React frontend with REST API
**Rationale:** Modern, responsive, good ecosystem
**Alternative:** Server-side rendering (simpler but less interactive)

---

## 📚 **References & Learning Resources**

### **Flink Documentation**
- [Flink Architecture](https://flink.apache.org/flink-architecture.html)
- [Checkpointing](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/checkpointing/)
- [Event Time & Watermarks](https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/time/)
- [State Backends](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/state_backends/)

### **Academic Papers**
- "Lightweight Asynchronous Snapshots for Distributed Dataflows" (Flink checkpoint algorithm)
- "MillWheel: Fault-Tolerant Stream Processing at Internet Scale" (Google's approach)
- "The Dataflow Model" (Beam/Flink foundations)

### **Implementation Guides**
- Flink source code (especially flink-streaming-java)
- Kafka Streams implementation
- Apache Beam runners

---

**This roadmap transforms Sabot from a good streaming framework into a Flink-competitive enterprise platform. Total effort: 5-6 months with a dedicated team.**