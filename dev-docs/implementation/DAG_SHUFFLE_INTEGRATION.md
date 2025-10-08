# DAG and DBOS Integration for Shuffle-based Operator Parallelism

## Overview

Integrated shuffle operations into Sabot's DAG execution and DBOS-controlled parallelism framework, enabling true operator parallelism with intelligent work distribution across distributed agents.

## Architecture Integration

### 1. Execution Graph Extensions

**File: `sabot/execution/execution_graph.py`**

Added shuffle-aware execution graph with:

#### `ShuffleEdgeType` Enum
```python
class ShuffleEdgeType:
    FORWARD = "forward"          # 1:1, no shuffle (operator chaining)
    HASH = "hash"                # Hash partition by key
    BROADCAST = "broadcast"      # Replicate to all
    REBALANCE = "rebalance"      # Round-robin
    RANGE = "range"              # Range partition
    CUSTOM = "custom"            # User-defined
```

#### `ShuffleEdge` Dataclass
Tracks data redistribution between N upstream and M downstream tasks:
- Shuffle ID and type
- Partition keys
- Task mappings (N×M connections)
- Metrics (rows/bytes shuffled)

#### Key Methods Added to `ExecutionGraph`:
- `add_shuffle_edge(shuffle_edge)` - Register shuffle operation
- `get_shuffle_edges_for_operator(operator_id)` - Query shuffles
- `detect_shuffle_edge_type(...)` - Automatic shuffle type detection

**Detection Logic:**
```python
# Same parallelism → FORWARD (operator chaining)
if len(upstream_tasks) == len(downstream_tasks):
    return ShuffleEdgeType.FORWARD

# Different parallelism + stateful with keys → HASH
if downstream_stateful and downstream_key_by:
    return ShuffleEdgeType.HASH

# Different parallelism + stateless → REBALANCE
return ShuffleEdgeType.REBALANCE
```

### 2. Job Graph Enhancements

**File: `sabot/execution/job_graph.py`**

Modified `to_execution_graph()` method to:

1. **Detect shuffle requirements** based on operator characteristics
2. **Create shuffle edges** automatically
3. **Connect tasks** according to shuffle type

```python
# Automatic shuffle edge creation
shuffle_type = exec_graph.detect_shuffle_edge_type(
    operator.operator_id,
    downstream_op_id,
    downstream_op.stateful,
    downstream_op.key_by
)

shuffle_edge = ShuffleEdge(
    shuffle_id=f"shuffle_{upstream}_{downstream}_{uuid}",
    edge_type=shuffle_type,
    partition_keys=downstream_op.key_by,
    num_partitions=len(downstream_tasks),
    ...
)
```

**Task Connection Strategy:**
- `FORWARD`: 1:1 mapping (no network shuffle)
- `HASH`: All-to-all (N×M connections, runtime partitioning)
- `REBALANCE`: Round-robin distribution
- `BROADCAST`: All-to-all (replicate data)

### 3. Shuffle Execution Coordinator

**File: `sabot/execution/shuffle_coordinator.py`**

DBOS-aware coordinator managing shuffle lifecycle:

#### `ShuffleExecutionPlan`
Concrete execution plan for each shuffle:
- Upstream/downstream task-to-agent mappings
- Partition count and keys
- Metrics tracking

#### `ShuffleCoordinator`
Orchestrates shuffle execution:

```python
coordinator = ShuffleCoordinator(execution_graph)

# Step 1: Initialize with agent assignments
await coordinator.initialize(agent_assignments)

# Step 2: Register with agent shuffle managers (Cython)
await coordinator.register_shuffles_with_agents(shuffle_managers)

# Step 3: Configure task locations for data fetching
await coordinator.set_task_locations(shuffle_managers)
```

**Key Features:**
- Maps logical shuffles to physical agent locations
- Registers shuffles with Cython ShuffleManager on each agent
- Configures upstream task locations for downstream reads
- Tracks shuffle metrics across distributed execution

### 4. Integration Points

#### With DBOS Parallel Controller

```python
from sabot.dbos_parallel_controller import DBOSParallelController

# DBOS controller decides parallelism levels
controller = DBOSParallelController(max_workers=64)

# Job graph uses parallelism hints
operator.parallelism = controller.calculate_optimal_worker_count(workload)

# Execution graph creates shuffle edges automatically
exec_graph = job_graph.to_execution_graph()

# Shuffle coordinator manages distributed execution
shuffle_coord = ShuffleCoordinator(exec_graph)
```

#### With Agent Runtime

Each agent runs:
1. **ShuffleManager** (Cython) - Local shuffle operations
2. **Task executor** - Processes data
3. **Shuffle transport** - Arrow Flight server/client

Coordinator tells each agent:
- Which shuffles to participate in
- Where upstream tasks are located
- How to partition outgoing data

#### With Cython Shuffle System

Coordinator bridges Python DAG execution and Cython shuffle implementation:

```python
# Python → Cython
manager.register_shuffle(
    operator_id=shuffle_id.encode('utf-8'),
    num_partitions=plan.num_partitions,
    partition_keys=[k.encode('utf-8') for k in keys],
    edge_type=cython_shuffle_type,
    schema=arrow_schema
)

# Cython handles actual data transfer
manager.write_partition(shuffle_id, partition_id, batch)
batches = manager.read_partition(shuffle_id, partition_id, upstream_agents)
```

## Example: Multi-Stage Pipeline with Shuffles

### User Code
```python
from sabot import App
from sabot.execution.job_graph import JobGraph, StreamOperatorNode, OperatorType

app = App("fraud_detection")
job = JobGraph(job_id="fraud_pipeline")

# Operator 1: Read transactions (parallelism=10)
source = StreamOperatorNode(
    operator_type=OperatorType.SOURCE,
    name="transactions_source",
    parallelism=10
)
job.add_operator(source)

# Operator 2: Enrich with user data (parallelism=10)
enrich = StreamOperatorNode(
    operator_type=OperatorType.MAP,
    name="enrich_user_data",
    parallelism=10,
    stateful=False
)
job.add_operator(enrich)
job.connect(source.operator_id, enrich.operator_id)

# Operator 3: Group by user_id for fraud detection (parallelism=5)
fraud_check = StreamOperatorNode(
    operator_type=OperatorType.GROUP_BY,
    name="fraud_detection",
    parallelism=5,
    stateful=True,
    key_by=["user_id"]  # ← Triggers HASH shuffle
)
job.add_operator(fraud_check)
job.connect(enrich.operator_id, fraud_check.operator_id)

# Operator 4: Alert sink (parallelism=2)
alerts = StreamOperatorNode(
    operator_type=OperatorType.SINK,
    name="alert_sink",
    parallelism=2,
    stateful=False
)
job.add_operator(alerts)
job.connect(fraud_check.operator_id, alerts.operator_id)
```

### Generated Execution Graph

**Operators → Tasks:**
- `transactions_source`: 10 tasks
- `enrich_user_data`: 10 tasks
- `fraud_detection`: 5 tasks
- `alert_sink`: 2 tasks
- **Total**: 27 tasks

**Shuffle Edges:**

1. **source → enrich** (10 → 10):
   - Type: `FORWARD` (same parallelism, stateless)
   - Connections: 1:1 (operator chaining, no network shuffle)

2. **enrich → fraud_check** (10 → 5):
   - Type: `HASH` (different parallelism, stateful with key)
   - Partition key: `user_id`
   - Connections: 10×5 = 50 (all-to-all)
   - **Network shuffle required**

3. **fraud_check → alerts** (5 → 2):
   - Type: `REBALANCE` (different parallelism, stateless)
   - Connections: Round-robin distribution
   - **Network shuffle required**

### Physical Execution Flow

```
┌─────────────────────────────────────────────────────────────┐
│ Agent 1: tasks 0-8                                          │
│                                                             │
│  source_task_0 → enrich_task_0 ─┐                          │
│  source_task_1 → enrich_task_1 ─┤                          │
│  ...                             ├─► [HASH SHUFFLE]        │
│  source_task_8 → enrich_task_8 ─┘    by user_id           │
│                                      ↓                      │
└─────────────────────────────────────────────────────────────┘
                                      │
                       ┌──────────────┼──────────────┐
                       ↓              ↓              ↓
         ┌─────────────────┬────────────────┬──────────────────┐
         │ Agent 2         │ Agent 3        │ Agent 4          │
         │ fraud_task_0    │ fraud_task_1   │ fraud_task_2-4   │
         │ (partition 0)   │ (partition 1)  │ (partitions 2-4) │
         └─────────────────┴────────────────┴──────────────────┘
                       │              │              │
                       └──────────────┼──────────────┘
                                      ↓
                            [REBALANCE SHUFFLE]
                                      │
                          ┌───────────┴───────────┐
                          ↓                       ↓
                 ┌────────────────┐      ┌────────────────┐
                 │ Agent 5        │      │ Agent 6        │
                 │ alert_task_0   │      │ alert_task_1   │
                 └────────────────┘      └────────────────┘
```

## DBOS Integration Points

### 1. Parallelism Decision

```python
from sabot.dbos_parallel_controller import DBOSParallelController

controller = DBOSParallelController()

# DBOS decides optimal parallelism based on:
# - Current system load
# - Data volume
# - Historical performance
parallelism = controller.plan_optimal_parallelism(
    operator_type="GROUP_BY",
    estimated_cardinality=1000000,
    current_load=0.6
)

operator.parallelism = parallelism
```

### 2. Work Distribution

```python
# DBOS adaptive load balancer assigns tasks to agents
distribution = controller.load_balancer.get_load_distribution_plan(tasks)

# Maps tasks to agents based on:
# - Current queue depths
# - Historical performance
# - Resource availability
agent_assignments = {
    task_id: agent_address
    for agent_id, task_list in distribution.items()
    for task_id in task_list
}

# Shuffle coordinator uses these assignments
await shuffle_coordinator.initialize(agent_assignments)
```

### 3. Dynamic Rescaling

```python
# DBOS monitors performance and triggers rescaling
if controller.should_scale_up(metrics):
    new_parallelism = controller.calculate_scale_target()

    # Execution graph supports live rescaling
    exec_graph.rescale_operator(
        operator_id="fraud_detection",
        new_parallelism=new_parallelism
    )

    # Shuffle coordinator reconfigures for new topology
    await shuffle_coordinator.reinitialize(new_agent_assignments)
```

## File Structure

```
sabot/
├── execution/
│   ├── job_graph.py                    # [MODIFIED] Shuffle edge generation
│   ├── execution_graph.py              # [MODIFIED] Shuffle tracking
│   └── shuffle_coordinator.py          # [NEW] DBOS-aware coordination
├── _cython/
│   └── shuffle/                        # Cython shuffle implementation
│       ├── shuffle_manager.pyx         # Per-agent shuffle management
│       ├── shuffle_transport.pyx       # Arrow Flight transport
│       └── partitioner.pyx             # Hash/range/RR partitioning
└── dbos_parallel_controller.py         # DBOS parallelism decisions
```

## Usage Example

### Complete End-to-End

```python
from sabot.execution.job_graph import JobGraph
from sabot.execution.shuffle_coordinator import ShuffleCoordinator
from sabot.dbos_parallel_controller import DBOSParallelController

# 1. Create job graph
job = JobGraph(job_id="my_job")
# ... add operators with parallelism hints ...

# 2. Convert to execution graph (shuffle edges auto-created)
exec_graph = job.to_execution_graph()

# 3. Initialize DBOS controller
dbos = DBOSParallelController(max_workers=64)

# 4. Get task-to-agent assignments
agent_assignments = dbos.assign_tasks_to_agents(exec_graph.tasks)

# 5. Create shuffle coordinator
shuffle_coord = ShuffleCoordinator(exec_graph)
await shuffle_coord.initialize(agent_assignments)

# 6. Create shuffle managers on each agent (Cython)
shuffle_managers = {}
for agent_id in agents:
    from sabot._cython.shuffle.shuffle_manager import ShuffleManager
    shuffle_managers[agent_id] = ShuffleManager(
        agent_id=agent_id.encode('utf-8'),
        agent_host=agent_host.encode('utf-8'),
        agent_port=shuffle_port
    )
    await shuffle_managers[agent_id].start()

# 7. Register shuffles with agents
await shuffle_coord.register_shuffles_with_agents(shuffle_managers)
await shuffle_coord.set_task_locations(shuffle_managers)

# 8. Execute tasks
# ... task execution with shuffle support ...

# 9. Monitor metrics
metrics = shuffle_coord.get_shuffle_metrics()
print(f"Total rows shuffled: {metrics['total_rows_shuffled']}")
```

## Benefits

### 1. Automatic Shuffle Detection
- No manual shuffle configuration
- Inferred from operator characteristics (stateful, key_by, parallelism)
- Transparent to user

### 2. DBOS-Controlled Parallelism
- Intelligent parallelism decisions based on system state
- Adaptive load balancing
- Dynamic rescaling support

### 3. Type-Safe Integration
- Python DAG management
- Cython shuffle execution (performance)
- Clear separation of concerns

### 4. Distributed Coordination
- Task-to-agent mapping
- Location awareness for data fetching
- Centralized metrics tracking

## Performance Characteristics

### Shuffle Type Selection

| Scenario | Shuffle Type | Network Overhead | Use Case |
|----------|--------------|------------------|----------|
| Same parallelism | FORWARD | None | Operator chaining |
| Stateful + keys | HASH | N×M connections | Joins, GroupBy |
| Stateless | REBALANCE | Round-robin | Load balancing |
| Small dimension | BROADCAST | Replicate | Dimension tables |

### Scalability

- **N upstream tasks × M downstream tasks**
- HASH shuffle: N×M network connections
- Memory bounded by buffer pools (Cython)
- Natural backpressure when buffers full

## Next Steps

1. **Agent runtime integration** - Wire shuffle managers into agent lifecycle
2. **Testing** - End-to-end DAG execution with shuffles
3. **Metrics** - Track shuffle performance across execution
4. **Optimization** - Operator chaining detection refinement

## Summary

Successfully integrated shuffle operations into Sabot's DAG execution framework:
- ✅ Automatic shuffle edge detection and generation
- ✅ DBOS-aware task assignment and coordination
- ✅ Cython shuffle manager integration
- ✅ Type-safe Python ↔ Cython bridge
- ✅ Distributed execution coordination
- ⏳ Agent runtime integration (next)
- ⏳ End-to-end testing (next)

This provides a complete foundation for operator parallelism in Sabot with intelligent work distribution managed by DBOS controller and efficient data shuffling via Cython/Arrow Flight.
