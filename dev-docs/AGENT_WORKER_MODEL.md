# Agent Worker Model

## Overview

**Agents are worker nodes, not user code.**

This document explains the fundamental shift in Sabot's architecture: agents are no longer user-defined functions, but infrastructure components that execute operator tasks. Users define dataflows using the Stream API, which get compiled to tasks and deployed to agent worker nodes.

## Architecture

### Before (Phase 4 and earlier)
```
User Code → @app.agent → Agent Function → Stream Processing
```

### After (Phase 5 and later)
```
User Code → @app.dataflow → JobManager → Agent Worker Nodes → Tasks
```

### New Architecture
```
┌─────────────────────────────────────────────────────────┐
│  User: Define Dataflow DAG                               │
│  @app.dataflow("pipeline")                               │
│  def my_pipeline():                                      │
│      return (Stream.from_kafka('input')                  │
│          .map(transform)                                 │
│          .filter(predicate)                              │
│          .to_kafka('output'))                            │
└─────────────────────────────────────────────────────────┘
                                 │
┌─────────────────────────────────────────────────────────┐
│  JobManager: Compile & Deploy                           │
│  - Optimize DAG                                         │
│  - Generate execution graph                             │
│  - Assign tasks to agents                               │
│  - Deploy via DBOS workflows                            │
└─────────────────────────────────────────────────────────┘
                                 │
┌──────────────┬──────────────┬──────────────────────────┐
│  Agent 1     │  Agent 2     │  Agent N                 │
│  (Worker)    │  (Worker)    │  (Worker)                │
│              │              │                          │
│  Task 1.0    │  Task 2.0    │  Task N.0                │
│  Task 1.1    │  Task 2.1    │  Task N.1                │
└──────────────┴──────────────┴──────────────────────────┘
```

## User Code vs Runtime

### OLD (deprecated): @app.agent
```python
@app.agent("transactions")
async def fraud_detector(transaction):
    """Process one transaction at a time"""
    if transaction['amount'] > 10000:
        yield {'alert': 'high_value', 'txn': transaction}
```

**Problems:**
- User thinks they're writing "agents" (active components)
- Actually just defining stream transformations
- Confusing naming: agents aren't autonomous
- Record-at-a-time processing (inefficient)

### NEW (recommended): @app.dataflow
```python
@app.dataflow("fraud_detection")
def fraud_pipeline():
    """Define operator DAG for fraud detection"""
    import pyarrow.compute as pc

    return (Stream.from_kafka('transactions')
        .filter(lambda b: pc.greater(b['amount'], 10000))
        .map(lambda b: b.append_column('alert',
            pa.array(['high_value'] * b.num_rows)))
        .to_kafka('fraud_alerts'))
```

**Benefits:**
- Clear separation: dataflow defines what, agents execute how
- Batch processing (Arrow columnar format)
- Declarative DAG definition
- Automatic optimization and parallelization

## Deployment Model

### Local Mode (Development)
- SQLite database backend
- Single agent process
- In-memory execution
- No network communication

```python
# Local execution
app = App(id="local")
job_manager = JobManager()  # Uses SQLite automatically

job_id = await job_manager.submit_job(job_graph_json)
```

### Distributed Mode (Production)
- PostgreSQL database backend
- Multiple agent processes
- Network shuffle communication
- Fault tolerance and scaling

```python
# Distributed execution
app = App(id="production")
job_manager = JobManager(dbos_url='postgresql://...')

job_id = await job_manager.submit_job(job_graph_json)
```

## Task Execution

### How Agents Execute Tasks

1. **Receive Task Assignment**: JobManager assigns tasks to agents
2. **Load Operator**: Deserialize operator configuration
3. **Initialize Morsel Processor**: Set up parallel execution
4. **Execute Batches**: Process data in morsel-driven parallelism
5. **Handle Shuffle**: Send/receive data via Arrow Flight if needed
6. **Report Metrics**: Update task status and performance metrics

### Morsel-Driven Parallelism

Agents automatically split large batches into cache-friendly morsels and process them in parallel using work-stealing:

```
Large Batch (100K rows)
    ↓
Split into Morsels (64KB each)
    ↓
Worker Threads Process in Parallel
    ↓
Reassemble Results
```

### Shuffle Integration

For stateful operators (joins, aggregations), agents participate in network shuffle:

```
Agent 1          Agent 2          Agent 3
   ↓               ↓               ↓
Partition ──── Shuffle ─────── Partition
   ↓       Network Transfer      ↓
Reassemble ──── Combine ────── Reassemble
```

## DBOS Integration

Sabot uses [DBOS](https://docs.dbos.dev/) for durable, fault-tolerant job orchestration:

### Local Mode (Development)
```python
from sabot import App
from sabot.job_manager import JobManager

app = App(id="local")
job_manager = JobManager()  # Uses SQLite automatically

# Submit jobs - durable even in local mode
job_id = await job_manager.submit_job(job_graph_json)
```

### Distributed Mode (Production)
```python
from sabot import App
from sabot.job_manager import JobManager

app = App(id="production")
job_manager = JobManager(dbos_url='postgresql://...')  # Uses PostgreSQL

# Submit jobs with full DBOS durability
job_id = await job_manager.submit_job(job_graph_json)
```

### DBOS Features Used

1. **@workflow**: Job submission survives crashes and resumes
2. **@transaction**: Database operations are atomic
3. **@communicator**: Agent deployment calls are retryable
4. **Exactly-once**: Workflows run exactly once even with failures

## Migration Guide

### Step 1: Replace @app.agent with @app.dataflow

**Before:**
```python
@app.agent("orders")
async def process_orders(order):
    if order['status'] == 'pending':
        yield order
```

**After:**
```python
@app.dataflow("order_processing")
def order_pipeline():
    return (Stream.from_kafka('orders')
        .filter(lambda b: pc.equal(b['status'], 'pending'))
        .to_kafka('pending_orders'))
```

### Step 2: Convert Record Processing to Batch Processing

**Before (record-at-a-time):**
```python
async def process(record):
    result = record['value'] * 1.1
    yield {'result': result}
```

**After (batch processing):**
```python
def process_batch(batch):
    return batch.append_column('result',
        pc.multiply(batch['value'], 1.1))
```

### Step 3: Update Imports and Dependencies

**Before:**
```python
from sabot import App

app = App(id="my_app")
```

**After:**
```python
from sabot import App
from sabot.api.stream import Stream
import pyarrow.compute as pc

app = App(id="my_app")
```

### Step 4: Change Deployment Pattern

**Before:**
```python
# Agents started implicitly
await app.start()
```

**After:**
```python
# Dataflows started explicitly
await my_dataflow.start(parallelism=4)
```

## Backward Compatibility

The old `@app.agent` decorator still works but shows a deprecation warning:

```python
@app.agent("topic")  # ⚠️  DeprecationWarning
async def my_agent(record):
    yield record
```

Internally, it gets converted to a dataflow, so existing code continues to work during migration.

## Benefits

### For Users
- **Clearer Mental Model**: Dataflows define what, agents execute how
- **Better Performance**: Batch processing instead of record-at-a-time
- **Automatic Optimization**: DAG optimization and parallelization
- **Future-Proof**: Aligns with modern stream processing architectures

### For Operations
- **Scalability**: Linear scaling with cluster size
- **Fault Tolerance**: DBOS-backed durable execution
- **Monitoring**: Rich metrics and health tracking
- **Resource Efficiency**: Morsel parallelism and work-stealing

## Troubleshooting

### Common Issues

**Issue**: "DBOS not available" error in local mode
**Solution**: DBOS is optional for local mode. The system falls back to basic execution.

**Issue**: Dataflow doesn't start
**Solution**: Check that SQLite database is initialized:
```bash
python -m sabot.init_db
```

**Issue**: Performance worse than expected
**Solution**: Ensure you're using batch operations, not record-at-a-time processing.

### Debugging

**Check Job Status:**
```python
job_manager = JobManager()
status = await job_manager.get_job_status(job_id)
print(status)
```

**Check Agent Health:**
```python
jobs = await job_manager.list_jobs()
for job in jobs:
    print(f"Job {job['job_id']}: {job['status']}")
```

## Future Enhancements

- **Live Rescaling**: Dynamically adjust parallelism without downtime
- **State Migration**: Move operator state between agents during rescaling
- **Advanced Scheduling**: NUMA-aware task placement and resource management
- **Multi-Cluster**: Support for geo-distributed deployments

---

## Related Documentation

- [Phase 5 Implementation Plan](PHASE5_AGENT_WORKER_NODE.md)
- [Phase 6 DBOS Control Plane](PHASE6_DBOS_CONTROL_PLANE.md)
- [Unified Batch Architecture](design/UNIFIED_BATCH_ARCHITECTURE.md)
- [Stream API Reference](../sabot/api/stream.py)

---

**Last Updated**: October 2025
**Status**: Active Documentation
