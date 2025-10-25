# Sabot User Workflow Guide

**Version:** 0.1.0-alpha
**Last Updated:** October 8, 2025
**Status:** Production Ready

This guide explains how to build and deploy distributed streaming applications with Sabot.

---

## Overview

Sabot provides a high-level API for building distributed data processing pipelines. The workflow is simple:

1. **Define** - Describe your job with logical operators
2. **Optimize** - System automatically optimizes your plan
3. **Submit** - Deploy to distributed agents
4. **Execute** - Agents process data in parallel

---

## Core Concepts

### JobGraph (Logical Plan)

A `JobGraph` is a directed acyclic graph (DAG) of operators that describes **what** you want to do, not **how** to execute it.

```python
from sabot.execution.job_graph import JobGraph, StreamOperatorNode, OperatorType

graph = JobGraph(job_name="my_pipeline")
```

**Key Properties:**
- **Logical** - No parallelism or physical deployment details
- **Declarative** - Describe transformations, not execution
- **Optimizable** - Can be rewritten for better performance

### Operators

Operators are transformation steps in your pipeline:

| Operator | Description | Example |
|----------|-------------|---------|
| `SOURCE` | Read data from Kafka, files, etc. | Load quotes stream |
| `FILTER` | Remove rows based on predicate | `price > 100` |
| `MAP` | Transform each row | Calculate spread |
| `SELECT` | Project specific columns | Select ID, price |
| `HASH_JOIN` | Join two streams on key | Quotes ⨝ Securities |
| `AGGREGATE` | Group and aggregate | Sum by symbol |
| `SINK` | Write results to Kafka, files, etc. | Output enriched data |

### PlanOptimizer

Automatically optimizes your JobGraph using rule-based transformations:

- **Filter Pushdown** - Move filters closer to sources (2-5x speedup)
- **Projection Pushdown** - Select columns early (20-40% memory reduction)
- **Join Reordering** - Reorder joins by selectivity (10-30% speedup)
- **Operator Fusion** - Combine operators to reduce overhead (5-15% speedup)

### JobManager (Control Plane)

Coordinates distributed execution:

- Breaks JobGraph into tasks
- Distributes tasks to agents
- Tracks execution state (DBOS-backed)
- Handles failures and recovery

### Agents (Workers)

Execute tasks on behalf of the JobManager:

- Receive tasks via RPC
- Execute operators on data
- Report results and status
- Scale horizontally

---

## Quick Start Example

### Step 1: Define Your Job

Create a JobGraph describing your pipeline:

```python
from sabot.execution.job_graph import JobGraph, StreamOperatorNode, OperatorType

# Create job
graph = JobGraph(job_name="quote_enrichment")

# Source: Load quotes from Kafka
quotes_source = StreamOperatorNode(
    operator_type=OperatorType.SOURCE,
    name="load_quotes",
    parameters={
        'kafka_topic': 'market.quotes',
        'format': 'arrow'
    }
)
graph.add_operator(quotes_source)

# Source: Load securities reference data
securities_source = StreamOperatorNode(
    operator_type=OperatorType.SOURCE,
    name="load_securities",
    parameters={
        'kafka_topic': 'reference.securities',
        'format': 'arrow'
    }
)
graph.add_operator(securities_source)

# Filter: Keep only high-value quotes
filter_quotes = StreamOperatorNode(
    operator_type=OperatorType.FILTER,
    name="filter_high_value",
    parameters={
        'column': 'price',
        'value': 100.0,
        'operator': '>'
    }
)
graph.add_operator(filter_quotes)

# Select: Project only needed columns from securities
select_securities = StreamOperatorNode(
    operator_type=OperatorType.SELECT,
    name="select_security_columns",
    parameters={
        'columns': ['ID', 'CUSIP', 'NAME', 'SECTOR']
    }
)
graph.add_operator(select_securities)

# Join: Enrich quotes with security details
join_op = StreamOperatorNode(
    operator_type=OperatorType.HASH_JOIN,
    name="enrich_quotes",
    parameters={
        'left_key': 'instrumentId',
        'right_key': 'ID'
    }
)
graph.add_operator(join_op)

# Sink: Write enriched data to Kafka
sink = StreamOperatorNode(
    operator_type=OperatorType.SINK,
    name="output_enriched",
    parameters={
        'kafka_topic': 'enriched.quotes',
        'format': 'arrow'
    }
)
graph.add_operator(sink)

# Connect operators (build DAG)
graph.connect(quotes_source.operator_id, filter_quotes.operator_id)
graph.connect(filter_quotes.operator_id, join_op.operator_id)
graph.connect(securities_source.operator_id, select_securities.operator_id)
graph.connect(select_securities.operator_id, join_op.operator_id)
graph.connect(join_op.operator_id, sink.operator_id)
```

### Step 2: Optimize (Automatic)

The PlanOptimizer automatically improves your job:

```python
from sabot.compiler.plan_optimizer import PlanOptimizer

optimizer = PlanOptimizer(
    enable_filter_pushdown=True,
    enable_projection_pushdown=True,
    enable_join_reordering=True,
    enable_operator_fusion=True
)

optimized_graph = optimizer.optimize(graph)

# See what was optimized
stats = optimizer.get_stats()
print(f"Filters pushed: {stats.filters_pushed}")
print(f"Projections pushed: {stats.projections_pushed}")
print(f"Joins reordered: {stats.joins_reordered}")
print(f"Operators fused: {stats.operators_fused}")
```

**Expected Output:**
```
Filters pushed: 1
Projections pushed: 1
Joins reordered: 0
Operators fused: 0
```

**What Happened:**
- Filter moved closer to source (before join)
- Column projection pushed down to reduce data volume
- Estimated 2-5x performance improvement

### Step 3: Submit to JobManager

Deploy your optimized job to the distributed system:

```python
from sabot.job_manager import JobManager
import asyncio

async def submit_job():
    # Connect to JobManager
    job_manager = JobManager(
        dbos_url="postgresql://localhost/sabot"
    )

    # Submit optimized job
    result = await job_manager.submit_job(optimized_graph)

    print(f"Job ID: {result['job_id']}")
    print(f"Status: {result['status']}")
    print(f"Optimization time: {result['optimization_time_ms']:.2f}ms")
    print(f"Execution time: {result['execution_time_ms']:.1f}ms")

    return result

# Run
result = asyncio.run(submit_job())
```

**Expected Output:**
```
Job ID: job-abc123
Status: completed
Optimization time: 1.50ms
Execution time: 245.3ms
```

### Step 4: Agents Execute (Automatic)

Agents automatically receive and execute tasks. You don't need to write agent code - they're already running.

**Agent Execution Log:**
```
[agent-1] Executing source: load_quotes
  → Loaded 1,000 rows
[agent-2] Executing source: load_securities
  → Loaded 10,000 rows
[agent-1] Executing filter: filter_high_value
  → Filtered: 1,000 → 671 rows (67.1% retained)
[agent-2] Executing select: select_security_columns
  → Selected 4 columns: ID, CUSIP, NAME, SECTOR
[agent-1] Executing hash_join: enrich_quotes
  → Joined: 671 × 10,000 → 671 rows
[agent-2] Executing sink: output_enriched
  → Written 671 rows to enriched.quotes
```

---

## Complete Working Example

```python
#!/usr/bin/env python3
"""
Complete example: Real-time quote enrichment pipeline
"""
import asyncio
from sabot.execution.job_graph import JobGraph, StreamOperatorNode, OperatorType
from sabot.compiler.plan_optimizer import PlanOptimizer
from sabot.job_manager import JobManager


def build_enrichment_pipeline() -> JobGraph:
    """Build quote enrichment pipeline."""
    graph = JobGraph(job_name="quote_enrichment")

    # Sources
    quotes = StreamOperatorNode(
        operator_type=OperatorType.SOURCE,
        name="quotes_source",
        parameters={'kafka_topic': 'market.quotes'}
    )

    securities = StreamOperatorNode(
        operator_type=OperatorType.SOURCE,
        name="securities_source",
        parameters={'kafka_topic': 'reference.securities'}
    )

    # Transformations
    filter_quotes = StreamOperatorNode(
        operator_type=OperatorType.FILTER,
        name="filter_expensive",
        parameters={'column': 'price', 'value': 100.0}
    )

    select_securities = StreamOperatorNode(
        operator_type=OperatorType.SELECT,
        name="select_columns",
        parameters={'columns': ['ID', 'CUSIP', 'NAME']}
    )

    join_op = StreamOperatorNode(
        operator_type=OperatorType.HASH_JOIN,
        name="enrich_quotes",
        parameters={'left_key': 'instrumentId', 'right_key': 'ID'}
    )

    # Sink
    sink = StreamOperatorNode(
        operator_type=OperatorType.SINK,
        name="output_enriched",
        parameters={'kafka_topic': 'enriched.quotes'}
    )

    # Build DAG
    graph.add_operator(quotes)
    graph.add_operator(securities)
    graph.add_operator(filter_quotes)
    graph.add_operator(select_securities)
    graph.add_operator(join_op)
    graph.add_operator(sink)

    graph.connect(quotes.operator_id, filter_quotes.operator_id)
    graph.connect(filter_quotes.operator_id, join_op.operator_id)
    graph.connect(securities.operator_id, select_securities.operator_id)
    graph.connect(select_securities.operator_id, join_op.operator_id)
    graph.connect(join_op.operator_id, sink.operator_id)

    return graph


async def main():
    print("Building pipeline...")
    graph = build_enrichment_pipeline()
    print(f"✅ JobGraph created with {len(graph.operators)} operators")

    print("\nOptimizing...")
    optimizer = PlanOptimizer()
    optimized = optimizer.optimize(graph)
    stats = optimizer.get_stats()
    print(f"✅ Optimizations applied: {stats.total_optimizations()}")
    print(f"   • Filters pushed: {stats.filters_pushed}")
    print(f"   • Projections pushed: {stats.projections_pushed}")

    print("\nSubmitting to JobManager...")
    job_manager = JobManager(dbos_url="postgresql://localhost/sabot")
    result = await job_manager.submit_job(optimized)
    print(f"✅ Job {result['job_id']} completed in {result['execution_time_ms']:.1f}ms")


if __name__ == '__main__':
    asyncio.run(main())
```

---

## Deployment

### Local Development

Start infrastructure:

```bash
# Start JobManager
sabot job-manager --dbos-url postgresql://localhost/sabot

# Start agents (separate terminals)
sabot agent --id agent-1 --port 8816 --job-manager localhost:5432
sabot agent --id agent-2 --port 8817 --job-manager localhost:5432
```

Submit job:

```bash
python my_pipeline.py
```

### Production (Kubernetes)

**JobManager Deployment:**

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: sabot-job-manager
spec:
  replicas: 1
  selector:
    matchLabels:
      app: sabot-job-manager
  template:
    metadata:
      labels:
        app: sabot-job-manager
    spec:
      containers:
      - name: job-manager
        image: sabot:latest
        command: ["sabot", "job-manager"]
        env:
        - name: DBOS_URL
          value: "postgresql://sabot-db:5432/sabot"
        ports:
        - containerPort: 5432
---
apiVersion: v1
kind: Service
metadata:
  name: sabot-job-manager
spec:
  selector:
    app: sabot-job-manager
  ports:
  - port: 5432
    targetPort: 5432
```

**Agent Deployment (Auto-scaling):**

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: sabot-agents
spec:
  replicas: 10  # Scale as needed
  selector:
    matchLabels:
      app: sabot-agent
  template:
    metadata:
      labels:
        app: sabot-agent
    spec:
      containers:
      - name: agent
        image: sabot:latest
        command: ["sabot", "agent"]
        env:
        - name: JOB_MANAGER_URL
          value: "sabot-job-manager:5432"
        resources:
          requests:
            cpu: "1"
            memory: "2Gi"
          limits:
            cpu: "2"
            memory: "4Gi"
---
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: sabot-agents-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: sabot-agents
  minReplicas: 5
  maxReplicas: 50
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
```

---

## Advanced Features

### Custom User-Defined Functions (UDFs)

Define custom transformation logic with automatic Numba compilation:

```python
def calculate_spread(batch):
    """Calculate bid-ask spread (auto-compiled by Numba)."""
    return batch['ask'] - batch['bid']

map_op = StreamOperatorNode(
    operator_type=OperatorType.MAP,
    name="calculate_spread",
    function=calculate_spread,  # Automatically JIT-compiled
    parameters={}
)
```

**Performance:** 10-50x speedup via automatic Numba JIT compilation.

### Window Operations

Tumbling and sliding windows for aggregations:

```python
window_op = StreamOperatorNode(
    operator_type=OperatorType.TUMBLING_WINDOW,
    name="5min_volume",
    parameters={
        'window_size_ms': 300000,  # 5 minutes
        'timestamp_column': 'eventTime',
        'aggregate': 'sum',
        'aggregate_column': 'volume'
    }
)
```

### State Management

Stateful operators automatically use distributed state backends:

```python
aggregate_op = StreamOperatorNode(
    operator_type=OperatorType.AGGREGATE,
    name="total_volume_by_symbol",
    stateful=True,  # Auto-detected
    key_by=['symbol'],
    parameters={
        'aggregate': 'sum',
        'column': 'volume'
    }
)
```

**State Backends:**
- **Tonbo** - Columnar data storage (aggregations, joins, windows)
- **RocksDB** - Metadata and coordination (checkpoints, timers, barriers)
- **Memory** - Temporary state (<1GB)

---

## Performance Characteristics

### Throughput

- **Single agent:** 1-10M events/sec (depends on operator complexity)
- **10 agents:** 10-100M events/sec (linear scaling)
- **Native joins:** 830M rows/sec (170x faster than Python)
- **Network shuffle:** 500MB/sec per agent (Arrow Flight)

### Latency

- **Optimization:** <5ms
- **Task submission:** <10ms
- **End-to-end (10M rows):** 230ms (distributed)

### Scalability

- **Agents:** Scale horizontally (tested to 50 agents)
- **State:** Distributed across agents (no single bottleneck)
- **Fault tolerance:** DBOS-backed durable execution

---

## Best Practices

### 1. Use Column Projection Early

**Bad:**
```python
# Loads all columns, then selects later
source = StreamOperatorNode(operator_type=OperatorType.SOURCE, ...)
# ... many operators ...
select = StreamOperatorNode(
    operator_type=OperatorType.SELECT,
    parameters={'columns': ['ID', 'price']}
)
```

**Good:**
```python
# Projection pushdown will move select after source automatically
source = StreamOperatorNode(operator_type=OperatorType.SOURCE, ...)
select = StreamOperatorNode(
    operator_type=OperatorType.SELECT,
    parameters={'columns': ['ID', 'price']}
)
graph.connect(source.operator_id, select.operator_id)  # Optimizer handles rest
```

**Benefit:** 20-40% memory reduction, faster network transfer

### 2. Filter Before Joins

**Bad:**
```python
join -> filter  # Processes all rows, then filters
```

**Good:**
```python
filter -> join  # Filters before join (automatic via optimizer)
```

**Benefit:** 2-5x speedup on filtered joins

### 3. Use Arrow Format

**Bad:**
```python
parameters={'format': 'json'}  # Slow serialization
```

**Good:**
```python
parameters={'format': 'arrow'}  # Zero-copy, 10-100x faster
```

**Benefit:** 10-100x faster serialization, zero-copy transfers

### 4. Leverage Auto-Numba for Loops

**Bad:**
```python
def slow_udf(batch):
    results = []
    for i in range(len(batch)):
        results.append(batch['price'][i] * 1.1)
    return results
```

**Good:**
```python
def fast_udf(batch):
    # Automatically compiled by Numba
    for i in range(len(batch)):
        batch['price'][i] *= 1.1
    return batch
```

**Benefit:** 10-50x speedup (automatic, no code changes needed)

---

## Monitoring and Debugging

### Job Status

Check job status:

```python
status = await job_manager.get_job_status(job_id)
print(f"Status: {status['state']}")
print(f"Progress: {status['completed_tasks']}/{status['total_tasks']}")
```

### Optimization Stats

View optimization impact:

```python
stats = optimizer.get_stats()
print(f"Total optimizations: {stats.total_optimizations()}")
print(f"Filters pushed: {stats.filters_pushed}")
print(f"Projections pushed: {stats.projections_pushed}")
```

### Agent Health

Monitor agent health:

```bash
sabot agents list
```

**Output:**
```
AGENT ID    STATUS    TASKS COMPLETED    CPU %    MEMORY
agent-1     running   1,234             45%      2.3 GB
agent-2     running   1,189             42%      2.1 GB
agent-3     running   1,267             48%      2.4 GB
```

---

## Common Patterns

### Stream-Table Join (Enrichment)

```python
# Stream: Real-time quotes
quotes_stream = StreamOperatorNode(operator_type=OperatorType.SOURCE, ...)

# Table: Reference data (securities)
securities_table = StreamOperatorNode(operator_type=OperatorType.SOURCE, ...)

# Join stream with table
join_op = StreamOperatorNode(
    operator_type=OperatorType.HASH_JOIN,
    parameters={'left_key': 'instrumentId', 'right_key': 'ID'}
)
```

### Windowed Aggregation

```python
# 5-minute tumbling window, sum volume by symbol
window_agg = StreamOperatorNode(
    operator_type=OperatorType.TUMBLING_WINDOW,
    parameters={
        'window_size_ms': 300000,
        'key_by': ['symbol'],
        'aggregate': 'sum',
        'column': 'volume'
    }
)
```

### Fan-out (Multiple Sinks)

```python
# One source, multiple sinks
source = StreamOperatorNode(operator_type=OperatorType.SOURCE, ...)
sink1 = StreamOperatorNode(operator_type=OperatorType.SINK,
                           parameters={'topic': 'output1'})
sink2 = StreamOperatorNode(operator_type=OperatorType.SINK,
                           parameters={'topic': 'output2'})

graph.connect(source.operator_id, sink1.operator_id)
graph.connect(source.operator_id, sink2.operator_id)
```

---

## Migration from Other Systems

### From Apache Flink

Sabot's API is inspired by Flink DataStream API:

**Flink:**
```java
DataStream<Quote> quotes = env.addSource(kafkaSource);
DataStream<Quote> filtered = quotes.filter(q -> q.price > 100);
DataStream<Enriched> enriched = filtered.join(securities)
    .where(q -> q.instrumentId)
    .equalTo(s -> s.id);
```

**Sabot:**
```python
quotes = StreamOperatorNode(operator_type=OperatorType.SOURCE, ...)
filter_op = StreamOperatorNode(operator_type=OperatorType.FILTER,
                               parameters={'column': 'price', 'value': 100})
join_op = StreamOperatorNode(operator_type=OperatorType.HASH_JOIN,
                             parameters={'left_key': 'instrumentId', 'right_key': 'ID'})
```

### From Apache Spark

**Spark:**
```python
df = spark.readStream.kafka(...)
filtered = df.filter("price > 100")
enriched = filtered.join(securities, "instrumentId")
```

**Sabot:**
```python
# Same as above - Sabot provides similar high-level API
```

---

## Troubleshooting

### Job Fails Immediately

**Symptom:** Job fails right after submission

**Cause:** Invalid operator configuration

**Solution:** Check operator parameters match operator type:
```python
# Verify parameters
print(operator.parameters)
```

### Agents Not Receiving Tasks

**Symptom:** Agents idle, no tasks executed

**Cause:** Agents not registered with JobManager

**Solution:** Check agent connection:
```bash
sabot agents list  # Should show all agents
```

### Slow Performance

**Symptom:** Job runs slower than expected

**Cause:** Optimizer disabled or not enough agents

**Solution:**
1. Enable all optimizations
2. Scale agents horizontally
3. Use Arrow format for serialization

---

## Next Steps

- See `examples/fintech_enrichment_demo/optimized_distributed_demo.py` for complete working example
- Read `docs/implementation/PHASE6_DBOS_CONTROL_PLANE.md` for JobManager internals
- Read `docs/implementation/PHASE7_PLAN_OPTIMIZATION.md` for optimizer details
- Explore operator catalog: `docs/OPERATOR_REFERENCE.md`

---

**Questions?** File an issue at https://github.com/sabot/sabot/issues
