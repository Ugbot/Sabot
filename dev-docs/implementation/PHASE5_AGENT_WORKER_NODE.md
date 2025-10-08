# Phase 5: Agent as Worker Node - Implementation Plan

**Version**: 1.0
**Date**: October 2025
**Status**: Design Specification
**Prerequisites**: Phase 4 (Network Shuffle) complete

---

## Executive Summary

This phase redefines "agent" from a user-facing code construct to a worker node in the cluster. Users write dataflow DAGs using operators. The JobManager compiles these DAGs into physical execution graphs and deploys tasks to agent worker nodes. Agents execute operator tasks using morsel-driven parallelism.

**Key Changes**:
- Agent = worker node that executes tasks (NOT user code)
- Users write dataflow DAGs, not `@app.agent` decorators
- JobManager handles compilation and deployment
- TaskExecutor runs operator tasks on agents
- Morsel processor provides work-stealing parallelism

**Migration Path**: `@app.agent` deprecated in favor of `@app.dataflow` with backward compatibility layer.

---

## Part 1: Detailed Task Breakdown

### Task 1.1: Create Agent Class (Worker Node)
**File**: `/Users/bengamble/Sabot/sabot/agent.py` (NEW)
**Estimated Effort**: 8 hours

**Purpose**: Implement Agent as a worker node that executes operator tasks.

**Requirements**:
- Agent runs on cluster nodes (physical or virtual machines)
- Manages task execution slots (configurable parallelism)
- Integrates with morsel processor for local parallelism
- Connects to shuffle transport for network data transfer
- Reports health and metrics to JobManager

**Implementation**:

```python
# sabot/agent.py
"""
Agent: A worker node that executes operator tasks.

Agents are NOT user code. They are runtime infrastructure that:
- Execute tasks assigned by JobManager
- Use morsel-driven parallelism locally
- Shuffle data via Arrow Flight
- Report health to JobManager
"""

import asyncio
import logging
from typing import Dict, List, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class AgentConfig:
    """Configuration for agent worker node"""
    agent_id: str
    host: str
    port: int
    num_slots: int = 8              # How many tasks can run concurrently
    workers_per_slot: int = 4       # Morsel workers per task
    memory_mb: int = 16384          # Total memory for agent
    cpu_cores: int = 8              # Total CPU cores


class Agent:
    """
    Worker node that executes streaming operator tasks.

    An Agent is NOT user code - it's a runtime component.
    """

    def __init__(self, config: AgentConfig):
        self.config = config
        self.agent_id = config.agent_id

        # Task execution
        self.active_tasks: Dict[str, TaskExecutor] = {}
        self.slots: List[Slot] = [
            Slot(i, config.memory_mb // config.num_slots)
            for i in range(config.num_slots)
        ]

        # Morsel parallelism
        from .dbos_cython_parallel import DBOSCythonParallelProcessor
        self.morsel_processor = DBOSCythonParallelProcessor(
            max_workers=config.num_slots * config.workers_per_slot,
            morsel_size_kb=64
        )

        # Shuffle
        from ._cython.shuffle.shuffle_transport import ShuffleServer, ShuffleClient
        self.shuffle_server = ShuffleServer()
        self.shuffle_client = ShuffleClient()

        # State
        self.running = False

        logger.info(f"Agent initialized: {self.agent_id}")

    async def start(self):
        """Start agent services"""
        if self.running:
            return

        self.running = True

        # Start morsel processor
        await self.morsel_processor.start()

        # Start shuffle server (Arrow Flight)
        await self.shuffle_server.start(self.config.host, self.config.port)

        logger.info(f"Agent started: {self.agent_id} at {self.config.host}:{self.config.port}")

    async def stop(self):
        """Stop agent and cleanup"""
        if not self.running:
            return

        self.running = False

        # Stop tasks
        for task_id in list(self.active_tasks.keys()):
            await self.stop_task(task_id)

        # Stop services
        await self.morsel_processor.stop()
        await self.shuffle_server.stop()

        logger.info(f"Agent stopped: {self.agent_id}")

    async def deploy_task(self, task: 'TaskVertex'):
        """Deploy and start a task (operator instance)"""
        # Allocate slot
        slot = self._allocate_slot(task)
        if slot is None:
            raise RuntimeError(f"No available slots for task {task.task_id}")

        # Create task executor
        executor = TaskExecutor(
            task=task,
            slot=slot,
            morsel_processor=self.morsel_processor,
            shuffle_client=self.shuffle_client
        )

        # Start execution
        await executor.start()

        self.active_tasks[task.task_id] = executor
        logger.info(f"Deployed task {task.task_id} to slot {slot.slot_id}")

    async def stop_task(self, task_id: str):
        """Stop and remove a task"""
        if task_id not in self.active_tasks:
            return

        executor = self.active_tasks[task_id]
        await executor.stop()

        # Free slot
        executor.slot.release()

        del self.active_tasks[task_id]
        logger.info(f"Stopped task {task_id}")

    def get_status(self) -> dict:
        """Get agent status for health reporting"""
        return {
            'agent_id': self.agent_id,
            'running': self.running,
            'active_tasks': len(self.active_tasks),
            'available_slots': sum(1 for s in self.slots if s.is_free()),
            'total_slots': len(self.slots),
        }

    def _allocate_slot(self, task) -> Optional['Slot']:
        """Find free slot for task"""
        for slot in self.slots:
            if slot.is_free() and slot.can_fit(task):
                slot.assign(task)
                return slot
        return None
```

**Testing Strategy**:
- Unit test: Agent initialization and configuration
- Unit test: Slot allocation and release
- Integration test: Deploy simple task to agent
- Integration test: Multi-task execution
- Integration test: Graceful shutdown

**Success Criteria**:
- Agent can start/stop cleanly
- Agent can allocate slots for tasks
- Agent integrates with morsel processor
- Agent integrates with shuffle transport
- Health status reporting works

---

### Task 1.2: Create TaskExecutor
**File**: `/Users/bengamble/Sabot/sabot/agent.py` (extend)
**Estimated Effort**: 12 hours

**Purpose**: Execute a single TaskVertex (operator instance) on an agent.

**Requirements**:
- Pull batches from input stream
- Execute operator on batches via morsel parallelism
- Push results to output stream (potentially via shuffle)
- Handle operator-specific logic (stateless vs stateful)
- Report metrics

**Implementation**:

```python
# sabot/agent.py (continued)

@dataclass
class Slot:
    """Execution slot for task"""
    slot_id: int
    memory_mb: int
    assigned_task: Optional[str] = None

    def is_free(self) -> bool:
        return self.assigned_task is None

    def can_fit(self, task) -> bool:
        return task.memory_mb <= self.memory_mb

    def assign(self, task):
        self.assigned_task = task.task_id

    def release(self):
        self.assigned_task = None


class TaskExecutor:
    """
    Executes a single TaskVertex (operator instance).

    Responsibilities:
    - Pull batches from input stream
    - Execute operator on batches (via morsel parallelism)
    - Push results to output stream (potentially via shuffle)
    """

    def __init__(self, task, slot, morsel_processor, shuffle_client):
        self.task = task
        self.slot = slot
        self.morsel_processor = morsel_processor
        self.shuffle_client = shuffle_client

        # Create operator instance
        self.operator = self._create_operator()

        # Execution state
        self.running = False
        self.execution_task = None

    def _create_operator(self):
        """Instantiate operator from task specification"""
        # This would deserialize operator from task.operator_spec
        # For now, placeholder
        return self.task.operator

    async def start(self):
        """Start task execution"""
        if self.running:
            return

        self.running = True
        self.execution_task = asyncio.create_task(self._execution_loop())

    async def stop(self):
        """Stop task execution"""
        if not self.running:
            return

        self.running = False
        if self.execution_task:
            self.execution_task.cancel()
            try:
                await self.execution_task
            except asyncio.CancelledError:
                pass

    async def _execution_loop(self):
        """
        Main execution loop: process batches via operator.

        For operators requiring shuffle:
        1. Receive batches from shuffle server
        2. Process via operator
        3. Send results to downstream via shuffle client

        For local operators:
        1. Receive batches from upstream task (same agent)
        2. Process via operator
        3. Send results to downstream task
        """
        try:
            if self.operator.requires_shuffle():
                await self._execute_with_shuffle()
            else:
                await self._execute_local()

        except Exception as e:
            logger.error(f"Task execution failed: {e}")
            raise

    async def _execute_with_shuffle(self):
        """Execute operator with network shuffle"""
        # Receive partitions assigned to this task
        async for batch in self.shuffle_server.receive_batches(self.task.task_id):
            # Process batch (via morsel parallelism)
            result = await self._process_batch_with_morsels(batch)

            # Send result downstream (may require re-shuffling)
            if result is not None:
                await self._send_downstream(result)

    async def _execute_local(self):
        """Execute operator without shuffle"""
        # Process batches from upstream
        async for batch in self.task.input_stream:
            result = await self._process_batch_with_morsels(batch)

            if result is not None:
                await self.task.output_stream.send(result)

    async def _process_batch_with_morsels(self, batch):
        """
        Process batch using morsel-driven parallelism.

        Splits batch into morsels, processes in parallel via workers.
        """
        # Create morsels from batch
        morsels = self.morsel_processor.create_morsels(batch)

        # Process morsels in parallel
        results = await self.morsel_processor.process_morsels(
            morsels,
            process_fn=self.operator.process_morsel
        )

        # Reassemble batch from morsel results
        if results:
            return self._reassemble_batch(results)
        return None

    def _reassemble_batch(self, morsel_results):
        """Combine morsel results back into single batch"""
        batches = [m.data for m in morsel_results if m.data is not None]
        if batches:
            import pyarrow as pa
            return pa.concat_batches(batches)
        return None

    async def _send_downstream(self, batch):
        """Send batch to downstream tasks (via shuffle if needed)"""
        # Determine target tasks based on partitioning
        # For now, simplified
        await self.task.output_stream.send(batch)
```

**Testing Strategy**:
- Unit test: TaskExecutor initialization
- Unit test: Morsel splitting and reassembly
- Integration test: Execute stateless operator (map)
- Integration test: Execute stateful operator (groupBy with shuffle)
- Integration test: Handle operator errors gracefully

**Success Criteria**:
- TaskExecutor can start/stop cleanly
- Batches processed correctly via morsels
- Local execution works (no shuffle)
- Shuffled execution works (with network transfer)
- Error handling and recovery works

---

### Task 1.3: Integrate with Morsel Processor
**File**: `/Users/bengamble/Sabot/sabot/_cython/operators/base_operator.pyx` (modify)
**Estimated Effort**: 6 hours

**Purpose**: Add morsel-aware interface to all operators.

**Requirements**:
- All operators support `process_morsel()` method
- Default implementation delegates to `process_batch()`
- Operators can optimize for morsel-level execution

**Implementation**:

```cython
# sabot/_cython/operators/base_operator.pyx (extend)

cdef class BaseOperator:
    """
    Base class for all streaming operators.

    NEW: Morsel-aware interface for parallel execution.
    """

    # ... existing code ...

    # ========================================================================
    # MORSEL-AWARE INTERFACE (for parallelism)
    # ========================================================================

    cpdef object process_morsel(self, Morsel morsel):
        """
        Process a morsel (chunk of batch for parallel execution).

        Default: just process the batch inside the morsel.
        Override for morsel-specific optimizations (e.g., NUMA placement).

        Args:
            morsel: Morsel containing RecordBatch data

        Returns:
            Processed Morsel or RecordBatch
        """
        result_batch = self.process_batch(morsel.data)
        if result_batch is not None:
            morsel.data = result_batch
            morsel.mark_processed()
            return morsel
        return None

    cpdef int get_parallelism_hint(self):
        """
        Suggest parallelism for this operator.

        Used by scheduler to determine number of parallel tasks.
        """
        return self._parallelism_hint
```

**Testing Strategy**:
- Unit test: Default morsel processing delegates to batch
- Unit test: Morsel metadata preserved correctly
- Integration test: Parallel morsel execution
- Performance test: Morsel vs batch-only execution

**Success Criteria**:
- All operators support morsel interface
- Default implementation works correctly
- Performance improvement measurable

---

### Task 1.4: Update App Class - Deprecate @app.agent
**File**: `/Users/bengamble/Sabot/sabot/app.py` (modify)
**Estimated Effort**: 10 hours

**Purpose**: Deprecate `@app.agent` and introduce `@app.dataflow` as replacement.

**Requirements**:
- `@app.agent` still works but logs deprecation warning
- `@app.agent` maps to `@app.dataflow` internally for compatibility
- New `@app.dataflow` decorator for defining operator DAGs
- Clear migration path documented

**Implementation**:

```python
# sabot/app.py (modify)

class App(AppT):
    """Main Sabot application class - unified batch/streaming runtime."""

    # ... existing code ...

    def agent(self, topic: Union[str, TopicT], **kwargs) -> Callable:
        """
        DEPRECATED: Create an agent.

        Use @app.dataflow instead.

        This decorator is maintained for backward compatibility.
        It internally maps to @app.dataflow.
        """
        import warnings
        warnings.warn(
            "@app.agent is deprecated. Use @app.dataflow instead. "
            "Agents are now worker nodes, not user code.",
            DeprecationWarning,
            stacklevel=2
        )

        def decorator(func: AgentFun) -> AgentT:
            # Map to dataflow internally
            topic_name = topic if isinstance(topic, str) else topic.name

            # Create a simple dataflow wrapper
            # This maintains compatibility while using new system
            return self._create_legacy_agent_wrapper(
                func, topic_name, **kwargs
            )

        return decorator

    def _create_legacy_agent_wrapper(self, func, topic_name, **kwargs):
        """
        Internal: Create legacy agent wrapper for backward compatibility.

        This creates a dataflow that wraps the old agent function.
        """
        # Create operator from function
        from .api.stream import Stream

        # Build dataflow
        stream = Stream.from_kafka(topic_name)

        # Apply function as map operator
        # (simplified - real implementation would handle async generators)
        result_stream = stream.map(func)

        # Register as dataflow
        agent_name = kwargs.get('name', f"{func.__name__}_{topic_name}")
        return self._register_dataflow(agent_name, result_stream)

    def dataflow(self, name: Optional[str] = None) -> Callable:
        """
        Create a dataflow (operator DAG).

        This is the NEW way to define stream processing logic.

        Example:
            @app.dataflow("process_orders")
            def order_pipeline():
                return (Stream.from_kafka('orders')
                    .filter(lambda b: pc.greater(b['amount'], 1000))
                    .window(tumbling(seconds=60))
                    .aggregate({'total': ('amount', 'sum')})
                    .to_kafka('processed_orders'))

        Args:
            name: Dataflow name (auto-generated if None)

        Returns:
            Dataflow decorator
        """
        def decorator(func: Callable) -> 'Dataflow':
            dataflow_name = name or func.__name__

            # Execute function to get operator DAG
            operator_dag = func()

            # Register dataflow
            return self._register_dataflow(dataflow_name, operator_dag)

        return decorator

    def _register_dataflow(self, name: str, operator_dag) -> 'Dataflow':
        """
        Internal: Register a dataflow with the JobManager.

        Args:
            name: Dataflow name
            operator_dag: Operator DAG (from Stream API)

        Returns:
            Dataflow instance
        """
        # Create dataflow object
        dataflow = Dataflow(
            name=name,
            operator_dag=operator_dag,
            app=self
        )

        # Store for later deployment
        if not hasattr(self, '_dataflows'):
            self._dataflows = {}
        self._dataflows[name] = dataflow

        logger.info(f"Registered dataflow: {name}")
        return dataflow


@dataclass
class Dataflow:
    """
    A dataflow is a user-defined operator DAG.

    Dataflows are compiled to physical execution graphs
    and deployed to agent worker nodes by the JobManager.
    """
    name: str
    operator_dag: Any  # Stream API DAG
    app: App
    job_id: Optional[str] = None

    async def start(self, parallelism: int = 4):
        """
        Start the dataflow by submitting to JobManager.

        Args:
            parallelism: Default parallelism for operators
        """
        # Submit to JobManager
        from .job_manager import JobManager

        job_manager = JobManager(database_url=self.app.database_url)
        self.job_id = await job_manager.submit_job(
            self.operator_dag,
            parallelism=parallelism
        )

        logger.info(f"Started dataflow {self.name} as job {self.job_id}")

    async def stop(self):
        """Stop the dataflow"""
        if not self.job_id:
            logger.warning(f"Dataflow {self.name} not started")
            return

        # Cancel job via JobManager
        from .job_manager import JobManager

        job_manager = JobManager(database_url=self.app.database_url)
        await job_manager.cancel_job(self.job_id)

        logger.info(f"Stopped dataflow {self.name}")
```

**Testing Strategy**:
- Unit test: `@app.dataflow` registration
- Unit test: Deprecation warning for `@app.agent`
- Integration test: Legacy agent still works
- Integration test: New dataflow works
- Migration test: Convert agent to dataflow

**Success Criteria**:
- `@app.dataflow` works correctly
- `@app.agent` still works with deprecation warning
- Clear error messages guide migration
- Examples demonstrate new pattern

---

### Task 1.5: Create Agent Worker Model Documentation
**File**: `/Users/bengamble/Sabot/docs/AGENT_WORKER_MODEL.md` (NEW)
**Estimated Effort**: 4 hours

**Purpose**: Document the new agent-as-worker model and migration guide.

**Outline**:

```markdown
# Agent Worker Model

## Overview

Agents are worker nodes, not user code.

## Architecture

[Diagram of JobManager → Agents → Tasks]

## User Code vs Runtime

**OLD (deprecated)**:
```python
@app.agent("orders")
async def process_orders(order):
    yield order.total
```

**NEW (recommended)**:
```python
@app.dataflow("process_orders")
def order_pipeline():
    return (Stream.from_kafka('orders')
        .map(lambda b: pc.multiply(b['total'], 1.1))
        .to_kafka('processed_orders'))
```

## Migration Guide

Step-by-step conversion from @app.agent to @app.dataflow

## Deployment Model

How JobManager deploys dataflows to agents

## Task Execution

How agents execute tasks with morsel parallelism

## Troubleshooting

Common issues and solutions
```

---

### Task 1.6: Create JobManager (DBOS-backed)
**File**: `/Users/bengamble/Sabot/sabot/job_manager.py` (NEW)
**Estimated Effort**: 16 hours

**Purpose**: Orchestrate job submission, compilation, and deployment.

**Requirements**:
- Accept dataflow DAGs from users
- Optimize DAG (filter pushdown, etc.)
- Compile to physical execution graph
- Assign tasks to agents
- Deploy tasks via agent HTTP/gRPC API
- Monitor execution
- Handle failures and rescaling

**Implementation Outline**:

```python
# sabot/job_manager.py (NEW)

class JobManager:
    """
    Manages distributed streaming jobs across cluster.

    Uses DBOS for:
    - Durable job state (survives crashes)
    - Workflow orchestration (multi-step deployment)
    - Transaction guarantees (atomic task assignment)
    - Event logging (audit trail)
    """

    def __init__(self, dbos_url='postgresql://localhost/sabot'):
        # Initialize DBOS
        # Initialize optimizer
        # Initialize cluster state
        pass

    @DBOS.workflow
    async def submit_job(self, dataflow_dag, parallelism=4):
        """
        Submit streaming job (DBOS workflow - durable across failures).

        Steps:
        1. Optimize DAG (transaction)
        2. Compile to physical plan (transaction)
        3. Assign tasks to agents (transaction)
        4. Deploy to agents (communicator)
        5. Monitor execution (workflow)
        """
        pass

    @DBOS.transaction
    async def _optimize_dag_step(self, job_id, dag):
        """Optimize operator DAG (DBOS transaction - atomic)"""
        pass

    @DBOS.transaction
    async def _compile_execution_graph_step(self, job_id, dag, parallelism):
        """Compile logical DAG to physical execution graph"""
        pass

    @DBOS.transaction
    async def _assign_tasks_step(self, job_id, exec_graph):
        """Assign tasks to agents (DBOS transaction - atomic)"""
        pass

    @DBOS.communicator
    async def _deploy_tasks_step(self, job_id, assignments):
        """Deploy tasks to agents (DBOS communicator - external calls)"""
        pass

    @DBOS.workflow
    async def rescale_operator(self, job_id, operator_id, new_parallelism):
        """Dynamically rescale operator (DBOS workflow - durable)"""
        pass
```

**Testing Strategy**:
- Unit test: DAG optimization
- Unit test: Physical plan compilation
- Integration test: Job submission end-to-end
- Integration test: Task deployment to agents
- Integration test: Rescaling workflow
- Fault tolerance test: JobManager crash recovery

**Success Criteria**:
- Jobs can be submitted and deployed
- DBOS workflows persist state correctly
- Failures trigger automatic recovery
- Rescaling works without downtime

---

### Task 1.7: Slot Management
**File**: `/Users/bengamble/Sabot/sabot/agent.py` (extend)
**Estimated Effort**: 4 hours

**Purpose**: Manage task execution slots on agents.

**Requirements**:
- Slots track resource allocation (memory, CPU)
- Slot assignment considers resource requirements
- Slot release frees resources
- Health checks monitor slot usage

**Implementation**: Already included in Agent class above (Slot dataclass).

**Testing Strategy**:
- Unit test: Slot allocation and release
- Unit test: Resource constraint checking
- Integration test: Multi-slot execution
- Stress test: Slot exhaustion handling

---

### Task 1.8: Integration Tests
**File**: `/Users/bengamble/Sabot/tests/integration/test_agent_worker_model.py` (NEW)
**Estimated Effort**: 8 hours

**Purpose**: End-to-end integration tests for agent-as-worker.

**Test Cases**:

1. **Deploy Simple Dataflow**:
   - Start agent
   - Submit dataflow (map only)
   - Verify task deployed
   - Verify data processed

2. **Deploy Stateful Dataflow**:
   - Submit dataflow with shuffle (groupBy)
   - Verify shuffle occurs
   - Verify results correct

3. **Multi-Agent Deployment**:
   - Start 3 agents
   - Submit dataflow with parallelism=6
   - Verify tasks distributed across agents
   - Verify shuffle works across agents

4. **Task Failure and Recovery**:
   - Kill task mid-execution
   - Verify supervisor restarts task
   - Verify data continuity

5. **Agent Failure and Rescaling**:
   - Kill agent
   - Verify tasks reassigned to other agents
   - Verify job continues

**Success Criteria**:
- All integration tests pass
- No data loss on failures
- Performance meets benchmarks

---

## Part 2: Migration Guide for Existing Code

### Step 1: Identify Agents to Migrate

Find all uses of `@app.agent`:

```bash
grep -r "@app.agent" sabot/ examples/
```

### Step 2: Convert Agent to Dataflow

**Before (deprecated)**:

```python
@app.agent("transactions")
async def fraud_detector(transaction):
    if transaction['amount'] > 10000:
        yield {'alert': 'high_value', 'txn': transaction}
```

**After (recommended)**:

```python
@app.dataflow("fraud_detection")
def fraud_pipeline():
    import pyarrow.compute as pc

    return (Stream.from_kafka('transactions')
        .filter(lambda b: pc.greater(b['amount'], 10000))
        .map(lambda b: b.append_column('alert', pa.array(['high_value'] * b.num_rows)))
        .to_kafka('fraud_alerts'))
```

**Key Changes**:
1. Decorator: `@app.agent` → `@app.dataflow`
2. Function signature: `async def func(record)` → `def func()` (returns DAG)
3. Logic: Per-record processing → Batch processing (Arrow compute)
4. Output: `yield` → `.to_kafka()` sink

### Step 3: Update Tests

**Before**:

```python
async def test_fraud_agent():
    app = App(id="test")

    @app.agent("txns")
    async def detect(txn):
        if txn['amount'] > 1000:
            yield txn

    # Simulate sending
    await detect.send({'amount': 5000})
```

**After**:

```python
async def test_fraud_dataflow():
    app = App(id="test")

    @app.dataflow("fraud_detection")
    def fraud_pipeline():
        return (Stream.from_kafka('txns')
            .filter(lambda b: pc.greater(b['amount'], 1000))
            .to_kafka('alerts'))

    # Start dataflow
    dataflow = fraud_pipeline
    await dataflow.start(parallelism=1)

    # Send test data
    # ... use Kafka producer ...
```

### Step 4: Update Deployment

**Before**: Agents started implicitly with app.

**After**: Dataflows submitted to JobManager explicitly:

```python
app = App(id="production")

@app.dataflow("pipeline")
def pipeline():
    return (...)

# Submit to JobManager
await pipeline.start(parallelism=8)
```

---

## Part 3: Testing Strategy

### Unit Tests (40 hours total)

1. **Agent Tests** (8 hours):
   - Agent initialization
   - Slot allocation
   - Task deployment
   - Health reporting

2. **TaskExecutor Tests** (8 hours):
   - Task execution
   - Morsel processing
   - Shuffle integration
   - Error handling

3. **JobManager Tests** (12 hours):
   - DAG optimization
   - Physical plan compilation
   - Task assignment
   - DBOS workflow persistence

4. **Dataflow API Tests** (8 hours):
   - Dataflow registration
   - Backward compatibility with @app.agent
   - Migration helpers

5. **Integration Tests** (4 hours):
   - Covered in Task 1.8 above

### Performance Tests (16 hours)

1. **Throughput Benchmark** (4 hours):
   - Measure records/sec with agent-as-worker
   - Compare to old agent model
   - Target: No degradation (same or better)

2. **Latency Benchmark** (4 hours):
   - Measure end-to-end latency
   - Target: <100ms p99 for simple pipeline

3. **Scalability Test** (4 hours):
   - Deploy 10 agents, 40 tasks
   - Measure throughput scaling
   - Target: Linear scaling to 8 agents

4. **Fault Tolerance Test** (4 hours):
   - Kill agents during execution
   - Measure recovery time
   - Verify no data loss

---

## Part 4: Success Criteria

### Functional Requirements

- [ ] Agent runs as standalone worker process
- [ ] TaskExecutor executes operator tasks correctly
- [ ] Morsel processor integrates with operators
- [ ] Shuffle transport works for stateful operators
- [ ] JobManager compiles and deploys dataflows
- [ ] DBOS workflows persist job state
- [ ] @app.dataflow API works correctly
- [ ] @app.agent backward compatibility maintained

### Performance Requirements

- [ ] Throughput: ≥50K records/sec (single agent, map operator)
- [ ] Latency: p99 <100ms (simple pipeline)
- [ ] Scalability: Linear to 8 agents
- [ ] Resource efficiency: <500MB memory per agent

### Reliability Requirements

- [ ] Agent failures trigger task reassignment
- [ ] Task failures trigger local restarts
- [ ] JobManager crash recovery works
- [ ] No data loss on failures
- [ ] Graceful shutdown preserves state

### Usability Requirements

- [ ] Clear deprecation warnings for @app.agent
- [ ] Migration guide with examples
- [ ] Error messages guide users to correct API
- [ ] Documentation complete and accurate

---

## Part 5: Estimated Effort Summary

| Task | Estimated Hours |
|------|----------------|
| 1.1 Create Agent Class | 8 |
| 1.2 Create TaskExecutor | 12 |
| 1.3 Integrate Morsel Processor | 6 |
| 1.4 Update App Class | 10 |
| 1.5 Documentation | 4 |
| 1.6 Create JobManager | 16 |
| 1.7 Slot Management | 4 |
| 1.8 Integration Tests | 8 |
| Unit Tests | 40 |
| Performance Tests | 16 |
| Migration & Documentation | 8 |
| **Total** | **132 hours** |

**Estimated Calendar Time**: 3-4 weeks (1 developer full-time)

---

## Part 6: Implementation Order

### Step 1: Core Agent Infrastructure (Days 1-5)
1. Create Agent class (Task 1.1)
2. Create Slot management (Task 1.7)
3. Create TaskExecutor basic (Task 1.2, no shuffle)
4. Unit tests for Agent and Slot

**Milestone**: Agent can execute simple tasks locally

### Step 2: Morsel Integration (Days 6-8)
1. Add morsel interface to operators (Task 1.3)
2. Integrate TaskExecutor with morsel processor
3. Unit tests for morsel processing

**Milestone**: Tasks execute with morsel parallelism

### Step 3: Shuffle Integration (Days 9-11)
1. Extend TaskExecutor for shuffle (Task 1.2, complete)
2. Integration tests with shuffle transport
3. Multi-agent shuffle tests

**Milestone**: Stateful operators work across agents

### Step 4: JobManager and Orchestration (Days 12-16)
1. Create JobManager skeleton (Task 1.6)
2. Implement DAG compilation
3. Implement task assignment
4. Implement DBOS workflows
5. Integration tests for deployment

**Milestone**: JobManager can deploy jobs to agents

### Step 5: User API and Migration (Days 17-19)
1. Update App class (Task 1.4)
2. Create @app.dataflow decorator
3. Add backward compatibility for @app.agent
4. Create migration examples

**Milestone**: Users can write dataflows

### Step 6: Testing and Documentation (Days 20-22)
1. Complete integration tests (Task 1.8)
2. Performance benchmarks
3. Write documentation (Task 1.5)
4. Create migration guide
5. Update examples

**Milestone**: Phase 5 complete and documented

---

## Part 7: Risks and Mitigations

### Risk 1: Performance Regression
**Impact**: High
**Probability**: Medium
**Mitigation**:
- Benchmark early and often
- Compare to baseline (old agent model)
- Profile critical paths (morsel processing, shuffle)
- Optimize hot paths with Cython

### Risk 2: Complex Migration Path
**Impact**: Medium
**Probability**: High
**Mitigation**:
- Maintain backward compatibility with @app.agent
- Provide automated migration tool (future)
- Create comprehensive migration guide
- Offer example conversions for common patterns

### Risk 3: DBOS Integration Issues
**Impact**: High
**Probability**: Low
**Mitigation**:
- Test DBOS workflows early
- Have fallback to basic JobManager (no DBOS)
- Document DBOS setup clearly
- Provide Docker setup for DBOS dependencies

### Risk 4: Shuffle Complexity
**Impact**: High
**Probability**: Medium
**Mitigation**:
- Leverage existing shuffle transport (Phase 4)
- Test shuffle thoroughly in isolation
- Start with simple hash partitioning
- Add advanced partitioning later

---

## Part 8: Dependencies

### Prerequisites (must be complete)

1. **Phase 4: Network Shuffle**
   - Shuffle transport working
   - Arrow Flight integration
   - Lock-free data structures

2. **Morsel Processor**
   - Work-stealing queues
   - Parallel processing
   - NUMA awareness

3. **Execution Graph**
   - TaskVertex definition
   - Shuffle edge types
   - State tracking

### Optional Dependencies

1. **DBOS**: For durable workflows (can work without)
2. **PostgreSQL**: For DBOS backend (can use SQLite)
3. **Metrics System**: For monitoring (can use basic logging)

---

## Part 9: Future Enhancements (Post-Phase 5)

### Dynamic Rescaling
- Automatic scaling based on load
- Rebalancing without downtime
- State migration for stateful operators

### Advanced Scheduling
- NUMA-aware task placement
- Co-location of related tasks
- Resource-aware scheduling

### Checkpointing Integration
- Distributed snapshots
- Exactly-once processing
- State recovery

### Metrics and Monitoring
- Real-time dashboards
- Performance profiling
- Anomaly detection

---

## Part 10: References

### Design Documents
- `/Users/bengamble/Sabot/docs/design/UNIFIED_BATCH_ARCHITECTURE.md`
- Flink Execution Model
- Spark Physical Plan

### Related Code
- `/Users/bengamble/Sabot/sabot/app.py` (current agent decorator)
- `/Users/bengamble/Sabot/sabot/agents/runtime.py` (agent runtime)
- `/Users/bengamble/Sabot/sabot/morsel_parallelism.py` (morsel processor)
- `/Users/bengamble/Sabot/sabot/execution/execution_graph.py` (execution graph)
- `/Users/bengamble/Sabot/sabot/_cython/shuffle/` (shuffle transport)

---

**Last Updated**: October 6, 2025
**Version**: 1.0
**Status**: Ready for implementation
