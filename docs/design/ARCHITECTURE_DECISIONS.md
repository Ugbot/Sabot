# Architecture Decision Record: Unified Streaming/Batch Model

**Date**: October 6, 2025
**Status**: Approved
**Context**: Unifying Sabot's operator model for streaming and batch processing

---

## Decision

**Streaming and batch are the same.** All operators process `RecordBatch` objects identically, whether the source is finite (batch) or infinite (streaming).

---

## Key Principles

### 1. Everything is Batches
- All operators: `RecordBatch` → `RecordBatch`
- No per-record processing in data plane (Cython/C++)
- "Per-record" iteration only at user API layer (syntactic sugar)

### 2. Streaming = Infinite Batching
- **Batch mode**: Finite source → `for batch in op` → terminates when exhausted
- **Streaming mode**: Infinite source → `async for batch in op` → runs forever
- **Same operator code** for both modes

### 3. Sources Know Boundedness, Operators Don't
- **Batch sources** (Parquet, CSV): iterate and stop
- **Streaming sources** (Kafka, sockets): loop forever
- **All downstream operators**: agnostic to boundedness

### 4. Data Plane vs Control Plane
- **Data Plane (C++/Cython)**: Operators, shuffle, morsels, state - from vendored libs
- **Control Plane (Python/DBOS)**: Orchestration, job management, health tracking

### 5. Agents = Worker Nodes
- Agents are cluster nodes that execute tasks
- NOT user code (that's the dataflow DAG)
- JobManager deploys operator tasks to agents

### 6. Auto-Numba for UDFs
- User functions JIT-compiled automatically
- AST analysis chooses `@njit` (scalar) vs `@vectorize` (array)
- Transparent 10-100x speedup

### 7. Morsel-Driven Parallelism
- Batches split into morsels for work-stealing execution
- Applies to both operators and shuffle
- NUMA-aware scheduling

---

## Example: Same Code, Different Modes

```python
def create_pipeline(source):
    """Works for both batch and streaming"""
    return (source
        .filter(lambda b: pc.greater(b['amount'], 1000))
        .window(tumbling(seconds=60))
        .aggregate({'total': ('amount', 'sum')})
    )

# Batch mode (finite)
batch_result = create_pipeline(Stream.from_parquet('data.parquet'))
for batch in batch_result:  # Terminates when file exhausted
    process(batch)

# Streaming mode (infinite)
stream_result = create_pipeline(Stream.from_kafka('transactions'))
async for batch in stream_result:  # Runs forever
    process(batch)
```

---

## Consequences

### Positive
- **Simplicity**: One operator model, not two
- **Testability**: Test on batch (fast), deploy to streaming
- **Performance**: Unified optimization path
- **Maintainability**: Less code to maintain

### Negative
- Must handle async/sync sources uniformly
- Watermark handling differs between modes
- Checkpointing required for streaming, optional for batch

### Mitigations
- `BaseOperator` supports both `__iter__()` and `__aiter__()`
- Watermarks implicit in batch (max timestamp), explicit in streaming
- Checkpointing enabled/disabled based on source type

---

## References

- Full design: `docs/design/UNIFIED_BATCH_ARCHITECTURE.md`
- Similar approach: Apache Flink (batch is bounded streaming)
- Inspiration: Apache Beam (unified model)
