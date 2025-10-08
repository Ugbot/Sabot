# 01_local_pipelines - Local Stream Processing

**Time to complete:** 30-45 minutes
**Prerequisites:** Completed 00_quickstart

This directory teaches core streaming concepts using local execution (no distributed agents, no infrastructure).

---

## Learning Objectives

After completing these examples, you will understand:

✅ Batch vs streaming processing
✅ How streaming = infinite batching
✅ Tumbling window operations
✅ Stateful stream processing
✅ State backends (MemoryBackend)
✅ Event-time vs processing-time

---

## Key Concept: Everything is Batches

**Core insight:** Streaming is just processing infinite batches

```
Batch Processing:     [Batch 1] → [Batch 2] → [Batch 3] → DONE
Streaming Processing: [Batch 1] → [Batch 2] → [Batch 3] → ... → ∞
```

**Sabot processes both the same way:**
- Read batch
- Transform batch
- Write batch
- Repeat

---

## Examples

### 1. **batch_processing.py** - Parquet to Parquet Pipeline ✅ **READY**

Learn batch processing with files.

```bash
python examples/01_local_pipelines/batch_processing.py
```

**What you'll learn:**
- Reading from Parquet files
- Applying transformations (filter, map, select)
- Writing results to Parquet
- Building JobGraph for batch workloads

**Pipeline:**
```
Read Parquet (1,000 rows)
    ↓
Filter (amount > 500) → 500 rows
    ↓
Map (add tax column)
    ↓
Select (4 columns)
    ↓
Write Parquet
```

**Output:**
```
✅ Pipeline completed successfully!
Final output: 500 rows, 4 columns
```

**Runtime:** ~5 seconds

---

### 2. **streaming_simulation.py** - Simulated Streaming ✅ **READY**

Learn streaming concepts with a generator.

```bash
python examples/01_local_pipelines/streaming_simulation.py
```

**What you'll learn:**
- Streaming = infinite batches
- Batch-at-a-time processing
- Generator pattern for streaming
- Simulating real-time data

**Pipeline:**
```
Generator (10 batches × 100 events)
    ↓
Filter (value > 50)
    ↓
Map (add doubled_value)
    ↓
Aggregate (stats per batch)
```

**Output:**
```
Batch 1/10: Generated 100 events
  Batch 1:
    Input: 100 rows
    Filtered: 50 rows (value > 50)
    Avg value: 75.00
...
✅ Streaming simulation complete!
```

**Key insight:** Each batch is processed independently (stateless)

**Runtime:** ~5 seconds

---

### 3. **window_aggregation.py** - Tumbling Windows ✅ **READY**

Learn windowed aggregations.

```bash
python examples/01_local_pipelines/window_aggregation.py
```

**What you'll learn:**
- Tumbling windows (non-overlapping)
- Event-time processing
- Window-based aggregations (count, sum, avg, min, max)
- When windows emit results

**Pipeline:**
```
Event Stream (50 events)
    ↓
Window (5 events per window)
    ↓
Aggregate (compute stats)
    ↓
Emit window results
```

**Output:**
```
🪟 Window 1 complete:
   Events: 0 → 4
   Count: 5
   Sum: 100.00
   Avg: 20.00
...
Total windows: 10
```

**Window types explained:**
```
Tumbling:  [----] [----] [----]  (non-overlapping)
Sliding:   [----]
            [----]
             [----]               (overlapping)
Session:   [--] ... [-----]      (gap-based)
```

**Runtime:** ~5 seconds

---

### 4. **stateful_processing.py** - Running Totals ✅ **READY**

Learn stateful stream processing.

```bash
python examples/01_local_pipelines/stateful_processing.py
```

**What you'll learn:**
- Stateful processing (maintaining state across batches)
- Using MemoryBackend for state storage
- Per-key state management
- Running aggregations (cumulative sum, count, average)

**Pipeline:**
```
Transaction Stream (10 batches × 10 txns)
    ↓
Update State (per account)
    ↓
Emit Current Totals
```

**Output:**
```
Batch 2 - Current State:
  ACC_0: Count=7, Total=$630.00, Avg=$90.00
  ACC_1: Count=7, Total=$700.00, Avg=$100.00
  ACC_2: Count=6, Total=$540.00, Avg=$90.00
...
Final State Summary:
Account      Txn Count    Total Amount    Avg Amount
ACC_0        34           $5610.00        $165.00
ACC_1        33           $5445.00        $165.00
ACC_2        33           $5280.00        $160.00
```

**Key insight:** State persists across batch boundaries

**Runtime:** ~5 seconds

---

## Concepts Explained

### Batch vs Streaming

| Aspect | Batch | Streaming |
|--------|-------|-----------|
| Input | Finite dataset | Infinite stream |
| Processing | All at once | Continuous |
| State | Entire dataset in memory | Incremental state |
| Latency | Minutes to hours | Milliseconds to seconds |
| Use case | Historical analysis | Real-time processing |

**Sabot handles both!** Same API, different data sources.

---

### Event-Time vs Processing-Time

**Event-time:**
- When the event actually happened
- Timestamp in the event data
- Used for windows, joins, aggregations
- Handles out-of-order events

**Processing-time:**
- When the system processes the event
- Wall-clock time
- Simpler, but can't handle late events

**Best practice:** Use event-time for correctness

---

### State Backends

Sabot uses a hybrid storage architecture:

| Backend | Purpose | Max Size | Latency | Durability |
|---------|---------|----------|---------|------------|
| MemoryBackend | Hot paths, temporary state | <1GB | <1μs | Volatile |
| TonboBackend | Columnar data (aggregations, joins, windows) | >100GB | ~50μs | Persistent |
| RocksDBBackend | Metadata (checkpoints, timers, barriers) | <100MB | ~100μs | Persistent |
| RedisBackend | Distributed state coordination | No limit | ~1ms | Distributed |

**Default:** MemoryBackend (used in stateful_processing.py)

**When to use:**
- **MemoryBackend:** Fast, small state (<1GB), can afford to lose state
- **TonboBackend:** Large columnar data (aggregations, joins) - automatic
- **RocksDBBackend:** System metadata only - automatic
- **RedisBackend:** Multi-agent coordination, shared state across processes

---

## Common Patterns

### Pattern 1: Stateless Transformation

```python
# No state maintained
stream → filter → map → sink
```

**Examples:**
- ETL pipelines
- Data enrichment (with static reference data)
- Format conversion

---

### Pattern 2: Windowed Aggregation

```python
# State maintained within window
stream → window(5 minutes) → aggregate → sink
```

**Examples:**
- Real-time dashboards (5-minute metrics)
- SLA monitoring (hourly p95 latency)
- Traffic analytics (daily page views)

---

### Pattern 3: Stateful Processing

```python
# State maintained across all events
stream → update_state(per_key) → emit_results
```

**Examples:**
- Running totals (account balances)
- User sessions (click streams)
- Fraud detection (transaction history)

---

## Performance Characteristics

### Batch Processing

| Operation | Throughput | Latency |
|-----------|------------|---------|
| Read Parquet | 500MB/sec | <10ms per file |
| Filter | 100M rows/sec | <1ms per batch |
| Map | 50M rows/sec | <5ms per batch |
| Write Parquet | 200MB/sec | <50ms per file |

---

### Streaming Processing

| Operation | Throughput | Latency |
|-----------|------------|---------|
| Generator | 1M events/sec | <1ms per batch |
| Filter | 100M rows/sec | <1ms per batch |
| Window aggregate | 10M events/sec | <10ms per window |
| Stateful update | 5M events/sec | <10ms per batch |

**Note:** All measurements on M1 Pro, single-threaded

---

### State Backend Performance

| Backend | Read Latency | Write Latency | Throughput | Data Type |
|---------|--------------|---------------|------------|-----------|
| Memory | <1μs | <1μs | 10M ops/sec | Any |
| Tonbo | ~50μs | ~100μs | 1M rows/sec | Columnar (Arrow) |
| RocksDB | ~100μs | ~200μs | 100K ops/sec | Metadata only |
| Redis | ~1ms | ~1ms | 10K ops/sec | Distributed |

---

## Best Practices

### 1. Choose Right Processing Mode

**Use batch when:**
- Historical data analysis
- Latency not critical (minutes/hours OK)
- Entire dataset fits in storage

**Use streaming when:**
- Real-time requirements (<1 second)
- Unbounded data (never ends)
- Need incremental results

---

### 2. Pick Right Window Type

**Tumbling windows:**
- Non-overlapping
- Each event in one window
- Example: 5-minute dashboards

**Sliding windows:**
- Overlapping
- Each event in multiple windows
- Example: 10-minute window, 1-minute slide

**Session windows:**
- Gap-based (timeout defines window boundary)
- Variable size
- Example: User sessions with 30-minute timeout

---

### 3. Select Right State Backend

```python
# Small state (<1GB), fast
state_backend = MemoryBackend()

# Large columnar data (aggregations, joins) - automatic
# Sabot automatically uses TonboBackend for columnar operations

# Distributed, shared state
state_backend = RedisBackend('redis://localhost:6379')
```

**Rule of thumb:**
- Start with MemoryBackend for user state
- TonboBackend and RocksDBBackend are used automatically (you don't configure them)
- Use Redis for multi-process coordination

---

### 4. Handle Late Events

```python
# Allow 1 hour of lateness
window(size='5min', allowed_lateness='1h')
```

**Why:** Real-world events can arrive out-of-order

**Tradeoff:** Longer lateness = more state to maintain

---

## Debugging Tips

### Check Pipeline Structure

```python
graph = JobGraph(job_name="my_pipeline")
# ... add operators ...

# Print execution order
for op in graph.topological_sort():
    print(f"{op.name} ({op.operator_type.value})")
```

---

### Inspect State

```python
state_backend = MemoryBackend()

# Put state
state_backend.put("key1", 100)

# Get state
value = state_backend.get("key1")
print(f"Value: {value}")

# List all keys
for key in state_backend._store.keys():
    print(f"{key}: {state_backend.get(key)}")
```

---

### Monitor Batch Sizes

```python
for batch in stream:
    print(f"Batch size: {batch.num_rows} rows, {batch.nbytes / 1024 / 1024:.2f} MB")
```

**Tip:** Batch size affects throughput/latency tradeoff

---

## Common Issues

### Issue 1: Out of Memory

**Symptom:** Process crashes with OOM error

**Causes:**
- Batch size too large
- State growing unbounded
- Too many windows open

**Solutions:**
- Reduce batch size
- Configure state TTL (time-to-live)
- Set max window count
- Tonbo automatically handles large columnar state (>1GB)

---

### Issue 2: High Latency

**Symptom:** Results delayed

**Causes:**
- Large batch size (more data to process)
- Expensive transformations
- Blocking I/O operations

**Solutions:**
- Reduce batch size
- Optimize transformations (use Numba)
- Use async I/O
- Profile with benchmarks

---

### Issue 3: Incorrect Window Results

**Symptom:** Window aggregates are wrong

**Causes:**
- Using processing-time instead of event-time
- Window size too small
- Not handling late events

**Solutions:**
- Use event-time timestamps
- Increase window size
- Configure allowed_lateness

---

## Next Steps

**Completed local pipelines?** Great! Next:

**Option A: Optimization**
→ `../02_optimization/` - See automatic optimization (2-5x speedup)

**Option B: Distributed Execution**
→ `../03_distributed_basics/` - Run pipelines across multiple agents

**Option C: Production Patterns**
→ `../04_production_patterns/` - Real-world streaming patterns

---

**Prev:** `../00_quickstart/README.md`
**Next:** `../02_optimization/README.md`
