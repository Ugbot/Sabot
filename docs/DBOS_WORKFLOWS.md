# DBOS Workflows in Sabot

**Version:** 1.0
**Last Updated:** October 2025
**Status:** Production

---

## Table of Contents

1. [Overview](#overview)
2. [DBOS Concepts](#dbos-concepts)
3. [Job Submission Workflow](#job-submission-workflow)
4. [Rescaling Workflow](#rescaling-workflow)
5. [Recovery and Resumption](#recovery-and-resumption)
6. [Transaction Guarantees](#transaction-guarantees)
7. [Best Practices](#best-practices)

---

## Overview

Sabot uses DBOS (Database-Oriented Operating System) workflows for durable, fault-tolerant job orchestration. All orchestration state lives in Postgres, enabling:

- **Crash Recovery**: Workflows survive process crashes and resume automatically
- **Exactly-Once Execution**: Each workflow step executes exactly once
- **Transaction Safety**: Atomic operations with rollback on failure
- **Audit Trail**: Complete history of all orchestration events

---

## DBOS Concepts

### Workflow

A multi-step operation that coordinates distributed execution:

```python
@workflow
async def submit_job(self, job_graph_json: str, job_name: str) -> str:
    """
    DBOS workflow for job submission.

    - Survives crashes
    - Resumes from last completed step
    - Exactly-once execution
    """
    job_id = str(uuid.uuid4())

    # Each step is durable
    await self._persist_job_definition(job_id, job_name, job_graph_json)
    await self._optimize_job_dag(job_id, job_graph_json)
    await self._compile_execution_plan(job_id, optimized_dag)
    await self._assign_tasks_to_agents(job_id, exec_graph)
    await self._deploy_job_to_agents(job_id, assignments)

    return job_id
```

### Transaction

An atomic database operation:

```python
@transaction
async def _persist_job_definition(self, job_id: str, job_name: str, dag_json: str):
    """
    Atomic database transaction.

    - All-or-nothing execution
    - Automatic rollback on failure
    - Isolation from concurrent transactions
    """
    await self.dbos.execute(
        "INSERT INTO jobs (job_id, job_name, job_graph_json) VALUES ($1, $2, $3)",
        job_id, job_name, dag_json
    )
```

### Communicator

An external HTTP/RPC call that can be retried:

```python
@communicator
async def _deploy_job_to_agents(self, job_id: str, assignments: Dict):
    """
    External communication step.

    - Can be retried on failure
    - Idempotent operations
    - Survives network failures
    """
    async with aiohttp.ClientSession() as session:
        for task_id, agent_id in assignments.items():
            agent = await self._get_agent_info(agent_id)
            await session.post(f"http://{agent.host}:{agent.port}/deploy_task", ...)
```

---

## Job Submission Workflow

### Workflow Diagram

```
┌─────────────────────────────────────────────────────────────┐
│  submit_job() - DBOS Workflow                                │
└─────────────────────────────────────────────────────────────┘
                            │
                            ↓
┌─────────────────────────────────────────────────────────────┐
│  Step 1: _persist_job_definition() - @transaction           │
│  - INSERT INTO jobs                                          │
│  - Store job_graph_json                                      │
└─────────────────────────────────────────────────────────────┘
                            │
                            ↓
┌─────────────────────────────────────────────────────────────┐
│  Step 2: _optimize_job_dag() - @transaction                 │
│  - Run PlanOptimizer                                         │
│  - UPDATE jobs SET optimized_dag_json                        │
└─────────────────────────────────────────────────────────────┘
                            │
                            ↓
┌─────────────────────────────────────────────────────────────┐
│  Step 3: _compile_execution_plan() - @transaction           │
│  - Convert DAG to ExecutionGraph                             │
│  - INSERT INTO execution_graphs                              │
│  - INSERT INTO tasks (for each task)                         │
└─────────────────────────────────────────────────────────────┘
                            │
                            ↓
┌─────────────────────────────────────────────────────────────┐
│  Step 4: _assign_tasks_to_agents() - @transaction           │
│  - SELECT available agents                                   │
│  - UPDATE tasks SET agent_id (for each task)                 │
│  - UPDATE agents SET available_slots (decrement)             │
└─────────────────────────────────────────────────────────────┘
                            │
                            ↓
┌─────────────────────────────────────────────────────────────┐
│  Step 5: _deploy_job_to_agents() - @communicator            │
│  - POST /deploy_task to each agent (HTTP)                    │
│  - UPDATE tasks SET state = 'deploying'                      │
└─────────────────────────────────────────────────────────────┘
                            │
                            ↓
┌─────────────────────────────────────────────────────────────┐
│  Step 6: _activate_job_execution() - @transaction           │
│  - UPDATE jobs SET status = 'running', started_at = NOW()    │
└─────────────────────────────────────────────────────────────┘
```

### Example Execution

```python
# Submit job
job_manager = JobManager(dbos_url='postgresql://localhost/sabot')

job_graph = JobGraph(...)
job_id = await job_manager.submit_job(job_graph.to_json(), "my-streaming-job")

# If JobManager crashes at any step, workflow resumes on restart
```

---

## Rescaling Workflow

### Live Operator Rescaling

```python
@workflow
async def rescale_operator(self, job_id: str, operator_id: str, new_parallelism: int):
    """
    Rescale operator with zero data loss.

    Steps:
    1. Pause operator tasks
    2. Drain in-flight data
    3. Create new tasks
    4. Redistribute state (if stateful)
    5. Deploy new tasks
    6. Cancel old tasks
    """
    rescale_id = str(uuid.uuid4())

    # Step 1: Initialize rescaling
    old_parallelism, stateful = await self._init_rescale_step(
        rescale_id, job_id, operator_id, new_parallelism
    )

    # Step 2: Pause tasks
    await self._pause_operator_tasks_step(rescale_id, job_id, operator_id)

    # Step 3: Drain in-flight data
    await self._drain_operator_tasks_step(rescale_id, job_id, operator_id)

    # Step 4: Create new tasks
    new_task_ids = await self._create_rescaled_tasks_step(
        rescale_id, job_id, operator_id, new_parallelism
    )

    # Step 5: Redistribute state (if stateful)
    if stateful:
        await self._redistribute_state_step(rescale_id, job_id, operator_id, new_task_ids)

    # Step 6: Deploy new tasks
    await self._deploy_rescaled_tasks_step(rescale_id, job_id, new_task_ids)

    # Step 7: Cancel old tasks
    await self._cancel_old_tasks_step(rescale_id, job_id, operator_id)

    logger.info(f"Rescaled {operator_id}: {old_parallelism} → {new_parallelism}")
```

---

## Recovery and Resumption

### Crash Recovery Example

**Scenario:** JobManager crashes during job submission

1. **Before Crash:**
   ```python
   # JobManager executing submit_job workflow
   await self._persist_job_definition(job_id, ...)  # ✅ Completed
   await self._optimize_job_dag(job_id, ...)       # ✅ Completed
   await self._compile_execution_plan(job_id, ...) # ✅ Completed
   await self._assign_tasks_to_agents(job_id, ...) # ❌ CRASH HERE
   ```

2. **After Restart:**
   ```python
   # DBOS automatically detects incomplete workflow
   # Reads workflow_state from database
   # Resumes from last completed step

   await self._assign_tasks_to_agents(job_id, ...) # ✅ Resume here
   await self._deploy_job_to_agents(job_id, ...)
   await self._activate_job_execution(job_id)
   ```

### Workflow State Persistence

```sql
-- workflow_state table tracks execution progress
SELECT * FROM workflow_state WHERE workflow_id = 'submit-job-abc123';

┌──────────────────┬───────────────────┬──────────────────────┬────────────┐
│ workflow_id      │ current_step      │ completed_steps      │ status     │
├──────────────────┼───────────────────┼──────────────────────┼────────────┤
│ submit-job-abc123│ assign_tasks      │ [persist, optimize,  │ running    │
│                  │                   │  compile]            │            │
└──────────────────┴───────────────────┴──────────────────────┴────────────┘
```

---

## Transaction Guarantees

### Atomic Task Assignment

```python
@transaction
async def _assign_tasks_to_agents(self, job_id: str, exec_graph_json: str):
    """
    All tasks assigned atomically.

    Guarantees:
    - All tasks assigned OR none assigned (rollback)
    - No partial assignment
    - Isolation from concurrent assignments
    """
    available_agents = await self.dbos.query(
        "SELECT agent_id, available_slots FROM agents WHERE status = 'alive'"
    )

    if not available_agents:
        raise RuntimeError("No agents available")

    # Assign all tasks in single transaction
    for task in exec_graph.tasks.values():
        agent = self._select_agent(available_agents)

        # UPDATE tasks (atomic)
        await self.dbos.execute(
            "UPDATE tasks SET agent_id = $1, state = 'scheduled' WHERE task_id = $2",
            agent.agent_id, task.task_id
        )

        # UPDATE agents (atomic)
        await self.dbos.execute(
            "UPDATE agents SET available_slots = available_slots - 1 WHERE agent_id = $1",
            agent.agent_id
        )

    # Transaction commits here - all updates succeed or all rollback
```

### Idempotent Operations

All workflow steps are **idempotent** - safe to execute multiple times:

```python
@transaction
async def _persist_job_definition(self, job_id: str, ...):
    """
    Idempotent INSERT.

    If row exists (workflow resumed), this is a no-op.
    """
    await self.dbos.execute(
        """
        INSERT INTO jobs (job_id, job_name, job_graph_json)
        VALUES ($1, $2, $3)
        ON CONFLICT (job_id) DO NOTHING
        """,
        job_id, job_name, job_graph_json
    )
```

---

## Best Practices

### 1. Design Workflows as Transactions + Communicators

**Good:**
```python
@workflow
async def deploy_job(self, job_id):
    # Transaction: atomic database ops
    await self._create_tasks(job_id)

    # Communicator: external HTTP calls
    await self._notify_agents(job_id)
```

**Bad:**
```python
@workflow
async def deploy_job(self, job_id):
    # ❌ Mixing database and external calls without separation
    await self.dbos.execute(...)
    await aiohttp.post(...)  # Should be @communicator
```

### 2. Make All Steps Idempotent

```python
@transaction
async def _create_task(self, task_id):
    # ✅ Idempotent: ON CONFLICT DO NOTHING
    await self.dbos.execute(
        "INSERT INTO tasks (task_id, ...) VALUES ($1, ...) ON CONFLICT DO NOTHING",
        task_id
    )
```

### 3. Use Transactions for State Mutations

```python
# ✅ Correct: Atomic state update
@transaction
async def _update_job_status(self, job_id, status):
    await self.dbos.execute(
        "UPDATE jobs SET status = $1, updated_at = NOW() WHERE job_id = $2",
        status, job_id
    )
```

### 4. Keep Workflow Steps Small

```python
# ✅ Good: Small, focused steps
@workflow
async def submit_job(self, job_graph):
    await self._persist(job_graph)
    await self._optimize(job_graph)
    await self._compile(job_graph)
    await self._deploy(job_graph)

# ❌ Bad: Monolithic step
@transaction
async def _do_everything(self, job_graph):
    # Too much in one transaction
```

---

## References

- **DBOS Project:** https://github.com/dbos-inc/dbos-transact
- **Phase 6 Implementation:** `/Users/bengamble/Sabot/docs/implementation/PHASE6_DBOS_CONTROL_PLANE.md`
- **Database Schema:** `/Users/bengamble/Sabot/sabot/dbos_schema.sql`
- **JobManager Implementation:** `/Users/bengamble/Sabot/sabot/job_manager.py`

---

**Last Updated:** October 2025
**Status:** Production Ready
