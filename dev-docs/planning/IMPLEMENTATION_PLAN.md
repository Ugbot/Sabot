# Sabot Implementation Plan
**Date:** October 2, 2025
**Status:** Sequential steps based on current reality

---

## Current State

### Working Components ✅
- Cython build system (31 modules compile)
- Checkpoint coordination primitives (Chandy-Lamport in Cython)
- State backends: Memory and RocksDB backends functional
- Watermark tracking primitives (Cython WatermarkTracker)
- Basic Kafka consumer (JSON/Avro deserialization)
- Arrow C++ integration (`sabot/_c/arrow_core.pyx`)
- Fraud detection example (measured 3K-6K txn/s)

### Not Working
- CLI loads mock App instead of real applications
- Agent runtime not wired to Kafka consumers
- sabot/arrow.py contains 32 NotImplementedError stubs
- Stream API has 7 NotImplementedError
- Test coverage at 5%
- Execution layer designed but not integrated
- Event-time processing not integrated

---

## Implementation Steps (Sequential)

### Phase 1: Fix Critical Blockers

#### Step 1.1: Fix CLI App Loading
**File:** `sabot/cli.py`
**Current:** Mock App class at lines 45-61
**Change:**
```python
def load_app_from_spec(app_spec: str):
    """Load App from 'module:attribute' format."""
    module_name, attr_name = app_spec.split(':')
    module = importlib.import_module(module_name)
    return getattr(module, attr_name)
```
**Verify:** `sabot -A examples.fraud_app:app worker` launches fraud demo

---

#### Step 1.2: Test CLI with Fraud Demo
**Create:** `tests/integration/test_cli_fraud_demo.py`
**Test:**
1. CLI parses app spec correctly
2. App loads from module
3. Worker starts without errors
4. Can stop gracefully

**Verify:** Integration test passes

---

#### Step 1.3: Fix Channel Creation
**File:** `sabot/app.py` line 381
**Current:** Raises NotImplementedError for non-memory channels
**Options:**
- Add `async_channel()` method for async channel creation
- Use sync wrapper with event loop detection
- Create channels lazily on first use

**Implementation:** Choose one approach and implement
**Verify:** Can create Kafka channels from App

---

#### Step 1.4: Test Channel Creation
**Create:** `tests/unit/test_app_channels.py`
**Test:**
1. Memory channel creation
2. Kafka channel creation
3. Redis channel creation (if supported)
4. Channel configuration options

**Verify:** All channel types can be created

---

### Phase 2: Agent Runtime Integration

#### Step 2.1: Wire Agent to Kafka Consumer
**Files:** `sabot/agents/runtime.py`, `sabot/kafka/source.py`
**Change:**
```python
class AgentRuntime:
    async def start_kafka_agent(self, agent_spec, topic):
        """Start agent consuming from Kafka topic."""
        consumer = KafkaSource(topic, ...)
        async for message in consumer:
            await agent_spec.function(message)
```
**Verify:** Agent receives messages from Kafka

---

#### Step 2.2: Test Agent Kafka Integration
**Create:** `tests/integration/test_agent_kafka.py`
**Test:**
1. Agent starts and connects to Kafka
2. Agent receives messages
3. Agent processes messages
4. Agent handles errors

**Verify:** Integration test passes

---

#### Step 2.3: Connect State Backend to Agents
**Files:** `sabot/agents/runtime.py`, `sabot/state/`
**Change:**
```python
class AgentProcess:
    def __init__(self, agent_spec, state_backend):
        self.state = state_backend
        # Agent can access state during processing
```
**Verify:** Agents can read/write state

---

#### Step 2.4: Test Agent State Integration
**Create:** `tests/integration/test_agent_state.py`
**Test:**
1. Agent writes to state
2. Agent reads from state
3. State persists across restarts
4. Multiple agents share state correctly

**Verify:** Integration test passes

---

#### Step 2.5: Connect Checkpoint Coordinator
**Files:** `sabot/agents/runtime.py`, `sabot/_cython/checkpoint/`
**Change:**
```python
from sabot.checkpoint import Coordinator

class AgentRuntime:
    def __init__(self):
        self.checkpoint_coordinator = Coordinator()

    async def trigger_checkpoint(self):
        checkpoint_id = self.checkpoint_coordinator.trigger()
        # Coordinate barrier across all agents
```
**Verify:** Checkpoints can be triggered

---

#### Step 2.6: Test Checkpoint Integration
**Create:** `tests/integration/test_checkpoint_e2e.py`
**Test:**
1. Checkpoint triggered
2. All agents receive barrier
3. State snapshot created
4. Checkpoint completes

**Verify:** End-to-end checkpoint works

---

### Phase 3: Increase Test Coverage

#### Step 3.1: Unit Test Cython Checkpoint Modules
**Create:** `tests/unit/test_cython_checkpoint.py`
**Test:**
- `barrier.pyx` - Barrier creation and serialization
- `barrier_tracker.pyx` - Barrier registration and alignment
- `coordinator.pyx` - Checkpoint triggering
- `storage.pyx` - Snapshot persistence
- `recovery.pyx` - Recovery from snapshot

**Verify:** All Cython checkpoint modules have unit tests

---

#### Step 3.2: Unit Test Cython State Modules
**Create:** `tests/unit/test_cython_state.py`
**Test:**
- Memory backend CRUD operations
- RocksDB backend CRUD operations
- ValueState, ListState, MapState primitives
- ReducingState, AggregatingState

**Verify:** All Cython state modules have unit tests

---

#### Step 3.3: Unit Test Cython Time Modules
**Create:** `tests/unit/test_cython_time.py`
**Test:**
- WatermarkTracker updates and retrieval
- Timers registration and firing
- EventTime utilities

**Verify:** All Cython time modules have unit tests

---

#### Step 3.4: Unit Test Arrow Integration
**Create:** `tests/unit/test_arrow_core.py`
**Test:**
- `arrow_core.pyx` buffer access
- `batch_processor.pyx` transformations
- `join_processor.pyx` join operations
- `window_processor.pyx` windowing

**Verify:** Arrow Cython modules have unit tests

---

#### Step 3.5: Fraud Demo End-to-End Test
**Create:** `tests/integration/test_fraud_demo_e2e.py`
**Test:**
1. Start Kafka broker
2. Create fraud detection topic
3. Send 1000 test transactions
4. Verify fraud alerts generated
5. Check state consistency
6. Validate checkpoint creation

**Verify:** Fraud demo works end-to-end with real Kafka

---

### Phase 4: Stream API Completion

#### Step 4.1: Identify Stream API Stubs
**File:** `sabot/api/stream.py`
**Action:** List all 7 NotImplementedError locations
**Document:** What each method should do

---

#### Step 4.2: Implement Window Operations
**File:** `sabot/api/stream.py`
**Implement:**
- `tumbling()` window function
- `sliding()` window function
- `session()` window function
- Window triggers
- Window eviction

**Verify:** Window operations work with test streams

---

#### Step 4.3: Test Window Operations
**Create:** `tests/unit/test_stream_windows.py`
**Test:**
1. Tumbling windows with time-based boundaries
2. Sliding windows with overlap
3. Session windows with inactivity gaps
4. Early/late triggers
5. Window state management

**Verify:** All window types work correctly

---

#### Step 4.4: Implement Join Operations
**File:** `sabot/api/stream.py`
**Implement:**
- Stream-to-stream join
- Stream-to-table join
- Join buffering
- Join state management

**Verify:** Joins produce correct results

---

#### Step 4.5: Test Join Operations
**Create:** `tests/unit/test_stream_joins.py`
**Test:**
1. Inner join
2. Left outer join
3. Right outer join
4. Full outer join
5. Window-based join
6. Interval join

**Verify:** All join types work correctly

---

#### Step 4.6: Wire State to Stream API
**File:** `sabot/api/stream.py`
**Change:**
```python
class Stream:
    def with_state(self, backend):
        self._state_backend = backend
        return self

    def map_with_state(self, fn):
        # fn has access to state
        pass
```
**Verify:** Stream transformations can access state

---

### Phase 5: Event-Time Processing

#### Step 5.1: Integrate Watermark Tracker
**Files:** `sabot/agents/runtime.py`, `sabot/_cython/time/watermark_tracker.pyx`
**Change:**
```python
class AgentProcess:
    def __init__(self):
        self.watermark_tracker = WatermarkTracker(num_partitions=...)

    async def process_message(self, msg):
        # Extract event time
        self.watermark_tracker.update_watermark(partition, event_time)
```
**Verify:** Watermarks update as messages processed

---

#### Step 5.2: Propagate Watermarks Through Pipeline
**Files:** `sabot/agents/runtime.py`, `sabot/api/stream.py`
**Implement:**
1. Extract watermark from each operator
2. Compute global watermark (min across partitions)
3. Propagate watermark downstream
4. Trigger window computations based on watermark

**Verify:** Watermarks flow through entire pipeline

---

#### Step 5.3: Test Event-Time Processing
**Create:** `tests/integration/test_event_time.py`
**Test:**
1. Out-of-order events processed correctly
2. Watermarks advance properly
3. Late events handled
4. Windows fire at correct event time

**Verify:** Event-time processing works end-to-end

---

### Phase 6: Error Handling & Recovery

#### Step 6.1: Implement Checkpoint Recovery
**File:** `sabot/_cython/checkpoint/recovery.pyx`
**Implement:**
1. Load latest checkpoint metadata
2. Restore state from snapshot
3. Resume from checkpoint offset
4. Verify no data loss or duplication

**Verify:** Can recover from checkpoint

---

#### Step 6.2: Test Checkpoint Recovery
**Create:** `tests/integration/test_checkpoint_recovery.py`
**Test:**
1. Process 100 events
2. Create checkpoint at event 50
3. Simulate failure at event 75
4. Recover from checkpoint
5. Verify events 51-75 reprocessed
6. Verify events 76-100 processed once

**Verify:** Exactly-once semantics maintained

---

#### Step 6.3: Implement Error Policies
**File:** `sabot/kafka/source.py`
**Implement:**
- SKIP: Log error and continue
- FAIL: Stop processing
- RETRY: Retry N times
- DEAD_LETTER: Send to DLQ

**Verify:** Each error policy works

---

#### Step 6.4: Test Error Handling
**Create:** `tests/integration/test_error_handling.py`
**Test:**
1. Kafka deserialization errors
2. Agent processing errors
3. State backend errors
4. Network failures
5. Resource exhaustion

**Verify:** Errors handled gracefully

---

### Phase 7: Performance & Benchmarking

#### Step 7.1: Benchmark Checkpoint Performance
**Create:** `benchmarks/bench_checkpoint.py`
**Measure:**
- Barrier initiation latency
- Barrier propagation time
- Snapshot creation time
- Recovery time

**Verify:** Meets performance claims (<10μs barrier)

---

#### Step 7.2: Benchmark State Backend Performance
**Create:** `benchmarks/bench_state.py`
**Measure:**
- Get/Put latency (Memory backend)
- Get/Put latency (RocksDB backend)
- Throughput (ops/sec)
- Memory usage

**Verify:** Meets performance claims (1M+ ops/sec for Memory)

---

#### Step 7.3: Benchmark Arrow Operations
**Create:** `benchmarks/bench_arrow.py`
**Measure:**
- Batch transformation throughput
- Join performance
- Window processing performance
- Compare Cython vs pure Python

**Verify:** Cython faster than pure Python

---

#### Step 7.4: Benchmark End-to-End
**Create:** `benchmarks/bench_fraud_demo.py`
**Measure:**
- Throughput (transactions/sec)
- Latency (p50, p95, p99)
- Memory usage
- CPU usage

**Verify:** Meets fraud demo performance (3K-6K txn/s)

---

### Phase 8: Documentation & Cleanup

#### Step 8.1: Document sabot/arrow.py Status
**File:** `sabot/arrow.py`
**Add header:**
```python
"""
Arrow Compatibility Layer

This module provides a fallback to pyarrow. The internal implementation
contains stubs (32 NotImplementedError) and is not complete.

For production use:
- Use pyarrow directly: `import pyarrow as pa`
- Use Cython bindings: `from sabot._c.arrow_core import ...`

This module may be removed in a future version.
"""
```
**Verify:** Users understand module status

---

#### Step 8.2: Update README.md
**File:** `README.md`
**Changes:**
- Accurate LOC count
- Document pyarrow dependency
- Mark experimental features
- Update "What Works" section
- Update performance claims with measured values

**Verify:** README matches reality

---

#### Step 8.3: Update PROJECT_MAP.md
**File:** `PROJECT_MAP.md`
**Changes:**
- Correct line counts for all files
- Update module status (working/partial/stub)
- Document NotImplementedError counts
- Link to REALITY_CHECK.md

**Verify:** PROJECT_MAP accurate

---

#### Step 8.4: Update CLAUDE.md
**File:** `CLAUDE.md`
**Changes:**
- Document pyarrow dependency (not vendored Arrow)
- Remove incorrect claims
- Add accurate implementation status

**Verify:** Instructions match reality

---

### Phase 9: Additional Features (After Core Works)

#### Step 9.1: Implement Process Supervision
**File:** `sabot/agents/supervisor.py`
**Implement:**
- ONE_FOR_ONE strategy
- ONE_FOR_ALL strategy
- REST_FOR_ONE strategy
- Health monitoring
- Auto-restart

**Verify:** Failed agents restart correctly

---

#### Step 9.2: Implement Resource Monitoring
**File:** `sabot/agents/resources.py`
**Implement:**
- CPU usage tracking
- Memory usage tracking
- Backpressure detection
- Resource limit enforcement

**Verify:** Resources monitored and limits enforced

---

#### Step 9.3: Add Metrics Collection
**Files:** `sabot/core/metrics.py`, `sabot/observability/`
**Implement:**
- Throughput metrics
- Latency metrics
- Error rate metrics
- State size metrics
- Checkpoint metrics

**Verify:** Metrics collected and exportable

---

#### Step 9.4: Add Savepoints
**Create:** `sabot/checkpoint/savepoints.py`
**Implement:**
1. Trigger savepoint (checkpoint with retention)
2. Store savepoint metadata
3. Resume from savepoint
4. Delete old savepoints

**Verify:** Can manually create and restore savepoints

---

## Testing Checklist

### Unit Tests
- [ ] Cython checkpoint modules
- [ ] Cython state modules
- [ ] Cython time modules
- [ ] Cython Arrow modules
- [ ] Stream API operations
- [ ] App configuration
- [ ] Channel creation

### Integration Tests
- [ ] CLI loads real apps
- [ ] Agent receives Kafka messages
- [ ] Agent uses state backend
- [ ] Checkpoint coordination
- [ ] Checkpoint recovery
- [ ] Event-time processing
- [ ] Window operations
- [ ] Join operations
- [ ] Error handling
- [ ] Fraud demo end-to-end

### Benchmarks
- [ ] Checkpoint performance
- [ ] State backend performance
- [ ] Arrow operation performance
- [ ] End-to-end throughput
- [ ] Latency percentiles

### Documentation
- [ ] README accurate
- [ ] PROJECT_MAP accurate
- [ ] CLAUDE.md accurate
- [ ] API documentation
- [ ] Example documentation

---

## Success Criteria

### Phase 1 Complete When:
- CLI loads real applications
- Channels can be created (all types)
- Tests verify functionality

### Phase 2 Complete When:
- Agents consume from Kafka
- Agents use state backends
- Checkpoints can be triggered
- Integration tests pass

### Phase 3 Complete When:
- Test coverage reaches 30%+
- All Cython modules have tests
- Fraud demo has E2E test

### Phase 4 Complete When:
- Stream API NotImplementedError resolved
- Window operations work
- Join operations work
- State integrated with Stream API

### Phase 5 Complete When:
- Watermarks propagate through pipeline
- Event-time processing works
- Out-of-order events handled correctly

### Phase 6 Complete When:
- Checkpoint recovery tested
- Error policies implemented
- System handles failures gracefully

### Phase 7 Complete When:
- Performance benchmarks run
- Results documented
- Claims verified with measurements

### Phase 8 Complete When:
- Documentation accurate
- sabot/arrow.py status clear
- No misleading claims

### Phase 9 Complete When:
- Supervision strategies work
- Resources monitored
- Metrics collected
- Savepoints functional

---

## Not Included (Defer)

The following are not in this implementation plan:

- SQL interface / Table API
- Distributed cluster coordination
- Web UI
- K8s deployment automation
- GPU acceleration
- CEP (Complex Event Processing)
- Additional connectors (beyond Kafka)
- Custom window implementations
- Execution layer wiring
- Multi-node coordination

These features can be added after core streaming functionality is working and well-tested.

---

**Last Updated:** October 2, 2025
**Status:** Active implementation plan
**Review:** Update after each phase completion
