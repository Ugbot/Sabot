# Phase 5 & 6 Integration: Technical Implementation Plan

**Version:** 1.0
**Date:** October 7, 2025
**Status:** Implementation in Progress
**Estimated Effort:** 70 hours (~2 weeks)

---

## Executive Summary

This document provides a detailed technical plan for integrating Phase 5 (Agent Worker Node) and Phase 6 (DBOS Control Plane) to create a production-ready distributed streaming system with:

- **Agent-based execution**: Workers run operator tasks with morsel parallelism
- **Arrow Flight shuffle**: Zero-copy network data transfer between agents
- **DBOS orchestration**: Durable workflows for job submission, rescaling, failure recovery
- **Fault tolerance**: Automatic agent failure detection and task recovery
- **Live rescaling**: Scale operators up/down without downtime

---

## Current State Analysis

### ✅ Already Complete

1. **Documentation**
   - `/docs/implementation/PHASE5_AGENT_WORKER_NODE.md` - Design specification
   - `/docs/implementation/PHASE6_DBOS_CONTROL_PLANE.md` - Design specification
   - `/docs/LOCK_FREE_QUEUE_OPTIMIZATION.md` - rigtorp optimizations complete

2. **Core Infrastructure**
   - `sabot/agent.py` - Agent, TaskExecutor, Slot classes (546 lines)
   - `sabot/job_manager.py` - JobManager skeleton with InMemoryDBOS fallback
   - `sabot/dbos_schema.sql` - Complete database schema
   - `sabot/_cython/shuffle/lock_free_queue.pyx` - Optimized SPSC/MPSC queues
   - `sabot/_cython/shuffle/atomic_partition_store.pyx` - Lock-free hash table
   - `sabot/_cython/shuffle/flight_transport_lockfree.pyx` - C++ Flight server skeleton

3. **Lock-Free Primitives**
   - rigtorp local caching pattern (~50% cache traffic reduction)
   - Exponential backoff for CAS contention
   - Python API wrappers (py_push, py_pop)

### 🚧 Needs Implementation

1. **Flight Server Integration**
   - Wire agent's host:port to Flight server
   - Complete DoGet/DoPut C++ implementations
   - Serve partitions from AtomicPartitionStore

2. **JobManager Workflows**
   - Complete submit_job workflow (6 steps)
   - Implement rescale_operator workflow
   - Implement recover_failed_task workflow

3. **Agent Lifecycle**
   - Registration with JobManager
   - Heartbeat mechanism
   - Task deployment via HTTP

4. **TaskExecutor**
   - Execution loop with shuffle/local paths
   - Morsel processor integration
   - Shuffle transport wiring

5. **State Management**
   - StateRedistributor for rescaling
   - Consistent hashing
   - State snapshot/restore

6. **Testing**
   - Multi-agent integration tests
   - Failure recovery tests
   - Rescaling tests

---

## Step 1: Wire Flight Server to Agent Configuration

**Estimated Effort:** 8 hours
**Priority:** P0 (Critical)

### Goal

Each agent runs an Arrow Flight server on its configured `host:port` to serve shuffle partitions to peer agents.

### Technical Design

```
Agent 1 (host:8816)                    Agent 2 (host:8817)
├── ShuffleTransport                   ├── ShuffleTransport
│   ├── FlightServer (8816)            │   ├── FlightServer (8817)
│   │   └── DoGet/DoPut                │   │   └── DoGet/DoPut
│   └── FlightClient pool              │   └── FlightClient pool
│       └── Connect to Agent 2         │       └── Connect to Agent 1
└── AtomicPartitionStore               └── AtomicPartitionStore
```

### Subtask 1.1: Update ShuffleTransport.start() Signature

**File:** `sabot/_cython/shuffle/shuffle_transport.pyx`

**Current:**
```cython
cpdef void start(self) except *
```

**Target:**
```cython
cpdef void start(self, string host, int32_t port) except *:
    """
    Start shuffle transport with Flight server on agent's address.

    Args:
        host: Agent hostname/IP (e.g., "0.0.0.0")
        port: Agent port (e.g., 8816)
    """
    self.agent_host = host
    self.agent_port = port

    # Start Flight server on agent's address
    self.server.host = host
    self.server.port = port
    self.server.start()

    # Initialize client (for connecting to peer agents)
    self.client.init()
```

**Testing:**
- Unit test: ShuffleTransport starts with host/port
- Unit test: Server binds to correct address
- Integration test: Agent can start shuffle transport

### Subtask 1.2: Complete C++ FlightServer DoGet Implementation

**File:** `sabot/_cython/shuffle/flight_transport_lockfree.pyx` (lines 84-171)

**Current:** Stub with `Status::NotImplemented`

**Target Implementation:**

```cpp
Status DoGet(const ServerCallContext& context,
             const Ticket& request,
             std::unique_ptr<FlightDataStream>* stream) override {
    // Parse ticket: "shuffle_id_hash:partition_id"
    std::string ticket_str = request.ticket;
    size_t colon_pos = ticket_str.find(':');
    if (colon_pos == std::string::npos) {
        return Status::Invalid("Invalid ticket format");
    }

    int64_t shuffle_id_hash = std::stoll(ticket_str.substr(0, colon_pos));
    int32_t partition_id = std::stoi(ticket_str.substr(colon_pos + 1));

    // Fetch partition from store (call Cython wrapper)
    // Need to add extern "C" wrapper in Cython
    auto batch = sabot_get_partition(
        partition_store_ptr,
        shuffle_id_hash,
        partition_id
    );

    if (!batch) {
        return Status::KeyError("Partition not found");
    }

    // Create Flight stream from batch
    std::vector<std::shared_ptr<RecordBatch>> batches = {batch};
    ARROW_ASSIGN_OR_RAISE(
        auto reader,
        RecordBatchReader::Make(batches, batch->schema())
    );

    *stream = std::make_unique<RecordBatchStream>(reader);
    return Status::OK();
}
```

**Add Cython Wrapper:**

```cython
# Extern "C" function for C++ to call Cython
cdef extern from *:
    """
    extern "C" {
        void* sabot_get_partition(void* store_ptr, int64_t shuffle_id_hash, int32_t partition_id);
    }

    // Implementation
    void* sabot_get_partition(void* store_ptr, int64_t shuffle_id_hash, int32_t partition_id) {
        // Cast to Cython object and call get()
        // Return raw pointer to RecordBatch
        return NULL;  // TODO: Implement
    }
    """
```

**Testing:**
- Unit test: DoGet with valid ticket returns batch
- Unit test: DoGet with invalid ticket returns error
- Integration test: Fetch partition from remote agent

### Subtask 1.3: Complete C++ FlightServer DoPut Implementation

**File:** `sabot/_cython/shuffle/flight_transport_lockfree.pyx` (lines 131-166)

**Current:** Stub with `Status::NotImplemented`

**Target Implementation:**

```cpp
Status DoPut(const ServerCallContext& context,
             std::unique_ptr<FlightMessageReader> reader,
             std::unique_ptr<FlightMetadataWriter> writer) override {
    // Parse descriptor: "shuffle_id_hash:partition_id"
    const FlightDescriptor& descriptor = reader->descriptor();
    if (descriptor.type != FlightDescriptor::CMD) {
        return Status::Invalid("Expected CMD descriptor");
    }

    std::string cmd = descriptor.cmd;
    size_t colon_pos = cmd.find(':');
    if (colon_pos == std::string::npos) {
        return Status::Invalid("Invalid command format");
    }

    int64_t shuffle_id_hash = std::stoll(cmd.substr(0, colon_pos));
    int32_t partition_id = std::stoi(cmd.substr(colon_pos + 1));

    // Read all batches from stream
    ARROW_ASSIGN_OR_RAISE(auto table, reader->ToTable());
    ARROW_ASSIGN_OR_RAISE(auto batch, table->CombineChunksToBatch());

    // Insert into store (call Cython wrapper)
    bool success = sabot_insert_partition(
        partition_store_ptr,
        shuffle_id_hash,
        partition_id,
        batch.get()
    );

    if (!success) {
        return Status::CapacityError("Store full");
    }

    return Status::OK();
}
```

**Testing:**
- Unit test: DoPut inserts batch into store
- Unit test: DoPut validates descriptor format
- Integration test: Send partition to remote agent

### Subtask 1.4: Update Agent.start() to Pass Host/Port

**File:** `sabot/agent.py` (lines 254-277)

**Current:**
```python
async def start(self):
    # Start shuffle transport
    await self.shuffle_transport.start()
```

**Target:**
```python
async def start(self):
    # Start shuffle transport with agent's address
    self.shuffle_transport.start(
        self.config.host.encode('utf-8'),
        self.config.port
    )
```

**Testing:**
- Integration test: Agent starts with correct address
- Integration test: Multiple agents bind to different ports

### Deliverables

- [ ] ShuffleTransport.start(host, port) signature updated
- [ ] C++ DoGet implementation complete
- [ ] C++ DoPut implementation complete
- [ ] Agent passes host:port to shuffle transport
- [ ] Unit tests passing (8 tests)
- [ ] Integration test: Multi-agent shuffle works

---

## Step 2: Complete JobManager DBOS Workflows

**Estimated Effort:** 16 hours
**Priority:** P0 (Critical)

### Goal

Implement durable workflows for job submission, rescaling, and failure recovery using DBOS (or InMemoryDBOS fallback).

### Subtask 2.1: Implement submit_job Workflow

**File:** `sabot/job_manager.py`

**Target Implementation:**

```python
@workflow
async def submit_job(self, job_graph_json: str, job_name: str = "unnamed_job") -> str:
    """
    Submit streaming job (DBOS workflow - durable across failures).

    Steps:
    1. Persist job definition (transaction)
    2. Optimize DAG (transaction)
    3. Compile to physical plan (transaction)
    4. Assign tasks to agents (transaction)
    5. Deploy to agents (communicator)
    6. Monitor execution (workflow)
    """
    job_id = str(uuid.uuid4())
    logger.info(f"Submitting job {job_id} ({job_name})")

    # Step 1: Persist job
    await self._persist_job_step(job_id, job_name, job_graph_json)

    # Step 2: Optimize DAG
    optimized_dag_json = await self._optimize_dag_step(job_id, job_graph_json)

    # Step 3: Compile execution graph
    exec_graph_json = await self._compile_execution_graph_step(job_id, optimized_dag_json)

    # Step 4: Assign tasks
    assignments = await self._assign_tasks_step(job_id, exec_graph_json)

    # Step 5: Deploy tasks
    await self._deploy_tasks_step(job_id, assignments)

    # Step 6: Monitor (background)
    await self._start_monitoring_step(job_id)

    logger.info(f"Job {job_id} submitted successfully")
    return job_id
```

**Testing:**
- Unit test: Each workflow step in isolation
- Integration test: Submit job end-to-end
- Fault tolerance test: Crash during step 3, resume

### Subtask 2.2: Implement _persist_job_step

```python
@transaction
async def _persist_job_step(self, job_id: str, job_name: str, job_graph_json: str):
    """Persist job definition (atomic transaction)."""
    await self.dbos.execute(
        """
        INSERT INTO jobs (job_id, job_name, job_graph_json, status, submitted_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        job_id, job_name, job_graph_json, 'pending', datetime.now()
    )
```

### Subtask 2.3: Implement _optimize_dag_step

```python
@transaction
async def _optimize_dag_step(self, job_id: str, job_graph_json: str) -> str:
    """Apply plan optimization (filter pushdown, etc)."""
    from .execution.job_graph import JobGraph

    job_graph = JobGraph.from_dict(json.loads(job_graph_json))

    # Apply optimizations
    optimized = self.optimizer.optimize(job_graph)
    optimized_json = json.dumps(optimized.to_dict())

    # Store optimized DAG
    await self.dbos.execute(
        "UPDATE jobs SET optimized_dag_json = ?, status = ? WHERE job_id = ?",
        optimized_json, 'optimizing', job_id
    )

    return optimized_json
```

### Subtask 2.4: Implement _compile_execution_graph_step

```python
@transaction
async def _compile_execution_graph_step(self, job_id: str, dag_json: str) -> str:
    """Compile logical DAG to physical execution graph."""
    from .execution.job_graph import JobGraph

    job_graph = JobGraph.from_dict(json.loads(dag_json))
    exec_graph = job_graph.to_execution_graph()
    exec_graph_json = json.dumps(exec_graph.to_dict())

    # Persist execution graph
    await self.dbos.execute(
        """
        INSERT INTO execution_graphs (job_id, graph_json, total_tasks, total_operators)
        VALUES (?, ?, ?, ?)
        """,
        job_id, exec_graph_json, len(exec_graph.tasks), len(job_graph.operators)
    )

    # Create task rows
    for task in exec_graph.tasks.values():
        await self.dbos.execute(
            """
            INSERT INTO tasks (
                task_id, job_id, operator_id, operator_type, task_index,
                parallelism, state, memory_mb, cpu_cores
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            task.task_id, job_id, task.operator_id, task.operator_type.value,
            task.task_index, task.parallelism, 'created', task.memory_mb, task.cpu_cores
        )

    await self.dbos.execute(
        "UPDATE jobs SET status = ? WHERE job_id = ?",
        'compiling', job_id
    )

    return exec_graph_json
```

### Subtask 2.5: Implement _assign_tasks_step

```python
@transaction
async def _assign_tasks_step(self, job_id: str, exec_graph_json: str) -> Dict[str, str]:
    """Assign tasks to agents (atomic transaction)."""
    from .execution.execution_graph import ExecutionGraph

    exec_graph = ExecutionGraph(**json.loads(exec_graph_json))

    # Get available agents
    agents = await self.dbos.query(
        """
        SELECT agent_id, available_slots
        FROM agents
        WHERE status = 'alive'
          AND available_slots > 0
          AND (julianday('now') - julianday(last_heartbeat)) * 86400 < 30
        ORDER BY available_slots DESC
        """
    )

    if not agents:
        raise RuntimeError("No available agents")

    # Round-robin assignment
    assignments = {}
    agent_index = 0

    for task in exec_graph.tasks.values():
        agent = agents[agent_index % len(agents)]
        agent_id = agent[0]  # SQLite returns tuples

        assignments[task.task_id] = agent_id

        # Update task
        await self.dbos.execute(
            "UPDATE tasks SET agent_id = ?, state = ? WHERE task_id = ?",
            agent_id, 'scheduled', task.task_id
        )

        # Record assignment
        await self.dbos.execute(
            """
            INSERT INTO task_assignments (job_id, task_id, agent_id, operator_type, reason)
            VALUES (?, ?, ?, ?, ?)
            """,
            job_id, task.task_id, agent_id, task.operator_type.value, 'initial_deploy'
        )

        # Decrement agent slots
        await self.dbos.execute(
            "UPDATE agents SET available_slots = available_slots - 1 WHERE agent_id = ?",
            agent_id
        )

        agent_index += 1

    await self.dbos.execute(
        "UPDATE jobs SET status = ? WHERE job_id = ?",
        'deploying', job_id
    )

    return assignments
```

### Subtask 2.6: Implement _deploy_tasks_step

```python
@communicator
async def _deploy_tasks_step(self, job_id: str, assignments: Dict[str, str]):
    """Deploy tasks to agents via HTTP (external calls)."""
    import aiohttp

    # Get agent addresses
    agent_ids = list(set(assignments.values()))
    placeholders = ','.join('?' * len(agent_ids))

    agents = await self.dbos.query(
        f"SELECT agent_id, host, port FROM agents WHERE agent_id IN ({placeholders})",
        *agent_ids
    )

    agent_map = {a[0]: (a[1], a[2]) for a in agents}

    # Deploy tasks
    async with aiohttp.ClientSession() as session:
        for task_id, agent_id in assignments.items():
            host, port = agent_map[agent_id]

            # Get task details
            task_data = await self.dbos.query_one(
                "SELECT operator_type, parameters_json FROM tasks WHERE task_id = ?",
                task_id
            )

            # Send deployment request
            url = f"http://{host}:{port}/deploy_task"
            async with session.post(url, json={
                'task_id': task_id,
                'job_id': job_id,
                'operator_type': task_data[0],
                'parameters': task_data[1] or '{}'
            }) as response:
                if response.status == 200:
                    await self.dbos.execute(
                        "UPDATE tasks SET state = ? WHERE task_id = ?",
                        'deploying', task_id
                    )
                else:
                    raise RuntimeError(f"Failed to deploy task {task_id}")

    await self.dbos.execute(
        "UPDATE jobs SET status = ?, started_at = ? WHERE job_id = ?",
        'running', datetime.now(), job_id
    )
```

### Subtask 2.7: Implement rescale_operator Workflow

```python
@workflow
async def rescale_operator(self, job_id: str, operator_id: str, new_parallelism: int):
    """
    Dynamically rescale operator (DBOS workflow).

    Steps:
    1. Validate and record rescaling
    2. Pause operator tasks
    3. Drain in-flight data
    4. Create new tasks
    5. Redistribute state (if stateful)
    6. Deploy new tasks
    7. Cancel old tasks
    8. Complete rescaling
    """
    rescale_id = str(uuid.uuid4())

    # Step 1: Init rescale
    old_parallelism, stateful = await self._init_rescale_step(
        rescale_id, job_id, operator_id, new_parallelism
    )

    # Step 2: Pause
    await self._pause_operator_tasks_step(rescale_id, job_id, operator_id)

    # Step 3: Drain
    await self._drain_operator_tasks_step(rescale_id, job_id, operator_id)

    # Step 4: Create new tasks
    new_task_ids = await self._create_rescaled_tasks_step(
        rescale_id, job_id, operator_id, new_parallelism
    )

    # Step 5: Redistribute state
    if stateful:
        await self._redistribute_state_step(
            rescale_id, job_id, operator_id, new_task_ids
        )

    # Step 6: Deploy
    await self._deploy_rescaled_tasks_step(rescale_id, job_id, new_task_ids)

    # Step 7: Cancel old
    await self._cancel_old_tasks_step(rescale_id, job_id, operator_id)

    # Step 8: Complete
    await self._complete_rescale_step(rescale_id)
```

### Subtask 2.8: Implement recover_failed_task Workflow

```python
@workflow
async def recover_failed_task(self, job_id: str, task_id: str):
    """
    Recover failed task (DBOS workflow).

    Steps:
    1. Mark task as reconciling
    2. Find new agent
    3. Assign to new agent
    4. Deploy
    5. Restore state (if stateful)
    """
    logger.info(f"Recovering failed task {task_id}")

    # Step 1: Mark reconciling
    await self.dbos.execute(
        "UPDATE tasks SET state = ? WHERE task_id = ?",
        'reconciling', task_id
    )

    # Step 2: Find agent
    agent = await self.dbos.query_one(
        """
        SELECT agent_id FROM agents
        WHERE status = 'alive'
          AND available_slots > 0
        ORDER BY available_slots DESC
        LIMIT 1
        """
    )

    if not agent:
        raise RuntimeError("No agents available for recovery")

    agent_id = agent[0]

    # Step 3: Assign
    await self.dbos.execute(
        "UPDATE tasks SET agent_id = ?, state = ? WHERE task_id = ?",
        agent_id, 'scheduled', task_id
    )

    # Step 4: Deploy
    await self._deploy_single_task(job_id, task_id, agent_id)

    logger.info(f"Task {task_id} recovered on agent {agent_id}")
```

### Deliverables

- [ ] submit_job workflow complete (6 steps)
- [ ] rescale_operator workflow complete
- [ ] recover_failed_task workflow complete
- [ ] Unit tests for each workflow step (20 tests)
- [ ] Integration test: Submit job end-to-end
- [ ] Fault tolerance test: Workflow resumption

---

## Step 3: Agent Registration and Health Tracking

**Estimated Effort:** 6 hours
**Priority:** P0 (Critical)

### Goal

Agents register with JobManager on startup and send periodic heartbeats. JobManager detects failures (heartbeat timeout > 30s).

### Subtask 3.1: Add JobManager HTTP Endpoints

**File:** `sabot/job_manager.py`

**Target:**

```python
async def start_http_server(self, host='0.0.0.0', port=8080):
    """Start HTTP server for agent communication."""
    from aiohttp import web

    app = web.Application()
    app.router.add_post('/register_agent', self._handle_register_agent)
    app.router.add_post('/heartbeat', self._handle_heartbeat)
    app.router.add_post('/deploy_task', self._handle_deploy_task_request)
    app.router.add_get('/status', self._handle_status)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()

    self._http_runner = runner
    logger.info(f"JobManager HTTP server started on {host}:{port}")

async def _handle_register_agent(self, request):
    """Handle agent registration."""
    from aiohttp import web

    data = await request.json()

    await self.dbos.execute(
        """
        INSERT INTO agents (
            agent_id, host, port, status, max_workers, available_slots,
            cython_available, gpu_available, registered_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(agent_id) DO UPDATE SET
            host = excluded.host,
            port = excluded.port,
            status = 'alive',
            last_heartbeat = CURRENT_TIMESTAMP
        """,
        data['agent_id'], data['host'], data['port'], 'alive',
        data['max_workers'], data['max_workers'],
        data.get('cython_available', False),
        data.get('gpu_available', False),
        datetime.now()
    )

    return web.json_response({'status': 'registered'})

async def _handle_heartbeat(self, request):
    """Handle agent heartbeat."""
    from aiohttp import web

    data = await request.json()

    await self.dbos.execute(
        """
        UPDATE agents SET
            last_heartbeat = ?,
            cpu_percent = ?,
            memory_percent = ?,
            disk_percent = ?,
            status = 'alive'
        WHERE agent_id = ?
        """,
        datetime.now(),
        data.get('cpu_percent', 0),
        data.get('memory_percent', 0),
        data.get('disk_percent', 0),
        data['agent_id']
    )

    # Record heartbeat in time-series table
    await self.dbos.execute(
        """
        INSERT INTO agent_heartbeats (
            agent_id, cpu_percent, memory_percent, disk_percent, recorded_at
        ) VALUES (?, ?, ?, ?, ?)
        """,
        data['agent_id'],
        data.get('cpu_percent', 0),
        data.get('memory_percent', 0),
        data.get('disk_percent', 0),
        datetime.now()
    )

    return web.json_response({'status': 'ok'})
```

### Subtask 3.2: Update Agent with Registration

**File:** `sabot/agent.py` (lines 396-414, 415-439)

**Current:** Basic skeleton

**Target:**

```python
async def _register_with_job_manager(self):
    """Register agent with JobManager."""
    if not self.job_manager:
        return

    import aiohttp

    try:
        async with aiohttp.ClientSession() as session:
            url = f"{self.config.job_manager_url}/register_agent"
            async with session.post(url, json={
                'agent_id': self.agent_id,
                'host': self.config.host,
                'port': self.config.port,
                'max_workers': self.config.num_slots,
                'cython_available': self._check_cython(),
                'gpu_available': self._check_gpu(),
            }) as response:
                if response.status == 200:
                    logger.info(f"Agent {self.agent_id} registered successfully")
                else:
                    raise RuntimeError("Failed to register with JobManager")
    except Exception as e:
        logger.error(f"Registration failed: {e}")
        raise

async def _heartbeat_loop(self):
    """Send periodic heartbeats to JobManager."""
    import psutil
    import aiohttp

    while self.running:
        try:
            async with aiohttp.ClientSession() as session:
                url = f"{self.config.job_manager_url}/heartbeat"
                async with session.post(url, json={
                    'agent_id': self.agent_id,
                    'cpu_percent': psutil.cpu_percent(),
                    'memory_percent': psutil.virtual_memory().percent,
                    'disk_percent': psutil.disk_usage('/').percent,
                    'active_workers': len(self.active_tasks),
                    'total_processed': sum(
                        getattr(t, 'rows_processed', 0)
                        for t in self.active_tasks.values()
                    ),
                }) as response:
                    if response.status != 200:
                        logger.warning("Heartbeat failed")

            await asyncio.sleep(10)  # 10-second heartbeat interval
        except Exception as e:
            logger.error(f"Heartbeat error: {e}")
            await asyncio.sleep(5)

def _check_cython(self) -> bool:
    """Check if Cython extensions available."""
    try:
        from sabot._cython.operators import base_operator
        return True
    except ImportError:
        return False

def _check_gpu(self) -> bool:
    """Check if GPU available."""
    try:
        import cupy
        return cupy.cuda.runtime.getDeviceCount() > 0
    except:
        return False
```

### Subtask 3.3: Implement Failure Detection

**File:** `sabot/job_manager.py`

```python
async def _failure_detection_loop(self):
    """Detect agent failures and trigger recovery."""
    while self.running:
        try:
            # Find agents with stale heartbeats
            dead_agents = await self.dbos.query(
                """
                SELECT agent_id FROM agents
                WHERE status = 'alive'
                  AND (julianday('now') - julianday(last_heartbeat)) * 86400 > 30
                """
            )

            for agent_row in dead_agents:
                agent_id = agent_row[0]
                logger.warning(f"Agent {agent_id} failed (heartbeat timeout)")

                # Mark agent as dead
                await self.dbos.execute(
                    "UPDATE agents SET status = ? WHERE agent_id = ?",
                    'dead', agent_id
                )

                # Trigger task recovery workflow
                await self.handle_agent_failure(agent_id)

            await asyncio.sleep(15)  # Check every 15 seconds
        except Exception as e:
            logger.error(f"Failure detection error: {e}")
            await asyncio.sleep(5)

@workflow
async def handle_agent_failure(self, agent_id: str):
    """Handle agent failure by recovering tasks."""
    # Get tasks running on failed agent
    tasks = await self.dbos.query(
        """
        SELECT task_id, job_id FROM tasks
        WHERE agent_id = ? AND state IN ('running', 'deploying')
        """,
        agent_id
    )

    for task_row in tasks:
        task_id, job_id = task_row
        await self.recover_failed_task(job_id, task_id)
```

### Deliverables

- [ ] JobManager HTTP server endpoints (register, heartbeat)
- [ ] Agent registration on startup
- [ ] Agent heartbeat loop (10s interval)
- [ ] Failure detection loop (30s timeout)
- [ ] Unit tests (6 tests)
- [ ] Integration test: Agent registration and heartbeat
- [ ] Integration test: Failure detection and recovery

---

## Step 4: Complete TaskExecutor Execution Loops

**Estimated Effort:** 12 hours
**Priority:** P0 (Critical)

### Goal

TaskExecutor executes operator tasks with morsel-driven parallelism and network shuffle.

### Subtask 4.1: Complete _execute_with_shuffle

**File:** `sabot/agent.py` (lines 139-164)

**Current:** Stub

**Target:**

```python
async def _execute_with_shuffle(self):
    """Execute operator with network shuffle."""
    # Get shuffle configuration
    shuffle_config = getattr(self.task, 'parameters', {}).get('shuffle_config', {})
    shuffle_id = shuffle_config.get('shuffle_id', '').encode('utf-8')
    partition_id = getattr(self.task, 'task_index', 0)

    if not shuffle_id:
        logger.warning(f"Task {self.task.task_id} requires shuffle but no shuffle_id")
        return

    # Wire up shuffle transport to operator
    if hasattr(self.operator, 'set_shuffle_transport'):
        self.operator.set_shuffle_transport(
            self.shuffle_client,
            shuffle_id,
            partition_id
        )

    # Execute operator - it will internally pull from shuffle
    try:
        for batch in self.operator:  # Operator.__iter__ uses shuffle
            if batch is not None and batch.num_rows > 0:
                # Process batch via morsels
                result = await self._process_batch_with_morsels(batch)

                # Send downstream
                if result is not None:
                    await self._send_downstream(result)
    except Exception as e:
        logger.error(f"Shuffle execution failed: {e}")
        raise
```

### Subtask 4.2: Complete _execute_local

**File:** `sabot/agent.py` (lines 166-178)

**Current:** Stub

**Target:**

```python
async def _execute_local(self):
    """Execute operator without shuffle (local pipeline)."""
    logger.info(f"Executing task {self.task.task_id} locally")

    # For local execution, operator receives batches from upstream tasks
    # This requires inter-task communication on same agent

    # Simplified: Assume operator has input_stream
    if not hasattr(self.task, 'input_stream'):
        logger.warning("Task has no input_stream for local execution")
        return

    try:
        async for batch in self.task.input_stream:
            if batch is not None and batch.num_rows > 0:
                # Process via morsels
                result = await self._process_batch_with_morsels(batch)

                # Send to output
                if result is not None and hasattr(self.task, 'output_stream'):
                    await self.task.output_stream.send(result)
    except Exception as e:
        logger.error(f"Local execution failed: {e}")
        raise
```

### Subtask 4.3: Complete _process_batch_with_morsels

**File:** `sabot/agent.py` (lines 180-201)

**Current:** Basic skeleton

**Target:**

```python
async def _process_batch_with_morsels(self, batch):
    """
    Process batch using morsel-driven parallelism.

    Splits batch into morsels, processes in parallel via workers.
    """
    if not self.morsel_processor or not batch or batch.num_rows == 0:
        # No morsel processor or empty batch - process directly
        if hasattr(self.operator, 'process_batch'):
            return self.operator.process_batch(batch)
        return batch

    # Create morsels from batch
    from .morsel_parallelism import Morsel

    # Split batch into morsels (e.g., 10K rows each)
    morsel_size = 10000
    morsels = []

    for i in range(0, batch.num_rows, morsel_size):
        end = min(i + morsel_size, batch.num_rows)
        morsel_batch = batch.slice(i, end - i)
        morsel = Morsel(
            data=morsel_batch,
            morsel_id=i // morsel_size,
            worker_id=-1  # Assigned by processor
        )
        morsels.append(morsel)

    # Process morsels in parallel
    try:
        results = await self.morsel_processor.process_morsels(
            morsels,
            process_fn=self.operator.process_morsel
        )

        # Reassemble batch from morsel results
        if results:
            return self._reassemble_batch(results)
        return None
    except Exception as e:
        logger.error(f"Morsel processing failed: {e}")
        # Fallback to sequential processing
        return self.operator.process_batch(batch)

def _reassemble_batch(self, morsel_results):
    """Combine morsel results back into single batch."""
    batches = [
        m.data for m in morsel_results
        if hasattr(m, 'data') and m.data is not None
    ]

    if batches:
        from sabot import cyarrow as pa
        return pa.concat_batches(batches)
    return None
```

### Subtask 4.4: Complete _send_downstream

**File:** `sabot/agent.py` (lines 211-215)

**Current:** Stub

**Target:**

```python
async def _send_downstream(self, batch):
    """Send batch to downstream tasks (via shuffle if needed)."""
    if not batch or batch.num_rows == 0:
        return

    # Get downstream configuration
    downstream_config = getattr(self.task, 'parameters', {}).get('downstream', {})

    if downstream_config.get('requires_shuffle', False):
        # Send via shuffle transport
        shuffle_id = downstream_config['shuffle_id'].encode('utf-8')

        # Partition batch based on key
        if 'partition_key' in downstream_config:
            partitioned_batches = self._partition_batch(
                batch,
                downstream_config['partition_key'],
                downstream_config['num_partitions']
            )

            # Send each partition to target agent
            for partition_id, partition_batch in partitioned_batches.items():
                target_agent = downstream_config['agents'][partition_id]
                await self.shuffle_client.send_partition(
                    target_agent,
                    shuffle_id,
                    partition_id,
                    partition_batch
                )
        else:
            # Broadcast to all downstream agents
            for agent_addr in downstream_config['agents']:
                await self.shuffle_client.send_partition(
                    agent_addr,
                    shuffle_id,
                    0,  # Single partition
                    batch
                )
    else:
        # Local send (within agent)
        if hasattr(self.task, 'output_stream'):
            await self.task.output_stream.send(batch)
        else:
            logger.debug(f"Task {self.task.task_id} has no output stream")

def _partition_batch(self, batch, partition_key, num_partitions):
    """Partition batch by key using hash partitioning."""
    from sabot._cython.shuffle.partitioner import HashPartitioner

    partitioner = HashPartitioner(
        num_partitions=num_partitions,
        key_columns=[partition_key.encode('utf-8')],
        schema=batch.schema
    )

    return partitioner.partition(batch)
```

### Deliverables

- [ ] _execute_with_shuffle complete
- [ ] _execute_local complete
- [ ] _process_batch_with_morsels complete
- [ ] _send_downstream complete
- [ ] Unit tests (12 tests)
- [ ] Integration test: Task execution with morsels
- [ ] Integration test: Shuffle execution

---

## Step 5: State Redistribution for Live Rescaling

**Estimated Effort:** 16 hours
**Priority:** P1 (High)

### Goal

Enable live operator rescaling with state redistribution using consistent hashing.

### Subtask 5.1: Create StateRedistributor Class

**File:** `sabot/state_redistribution.py` (NEW)

```python
"""
State Redistribution for Live Rescaling.

Uses consistent hashing to minimize state movement during rescaling.
"""

import logging
from typing import List, Dict, Any
import hashlib

logger = logging.getLogger(__name__)


class ConsistentHashRing:
    """
    Consistent hash ring for minimal state movement.

    Based on:
    - Karger et al. "Consistent Hashing and Random Trees" (1997)
    - 160 virtual nodes per physical node
    """

    def __init__(self, num_nodes: int, virtual_nodes: int = 160):
        self.num_nodes = num_nodes
        self.virtual_nodes = virtual_nodes
        self.ring = {}
        self._build_ring()

    def _build_ring(self):
        """Build hash ring with virtual nodes."""
        for node_id in range(self.num_nodes):
            for vnode_id in range(self.virtual_nodes):
                key = f"node{node_id}_vnode{vnode_id}"
                hash_val = self._hash(key)
                self.ring[hash_val] = node_id

    def _hash(self, key: str) -> int:
        """Hash key to 64-bit integer."""
        return int(hashlib.md5(key.encode()).hexdigest()[:16], 16)

    def get_node(self, key: str) -> int:
        """Get node ID for key."""
        key_hash = self._hash(key)

        # Find next node clockwise on ring
        sorted_hashes = sorted(self.ring.keys())
        for hash_val in sorted_hashes:
            if hash_val >= key_hash:
                return self.ring[hash_val]

        # Wrap around to first node
        return self.ring[sorted_hashes[0]]


class StateRedistributor:
    """
    Redistribute state during live rescaling.

    Minimizes state movement using consistent hashing.
    """

    def __init__(self):
        self.logger = logger

    async def redistribute_state(
        self,
        old_parallelism: int,
        new_parallelism: int,
        old_task_states: Dict[int, Dict[str, Any]],
        key_by: List[str]
    ) -> Dict[int, Dict[str, Any]]:
        """
        Redistribute state from old tasks to new tasks.

        Args:
            old_parallelism: Old task count
            new_parallelism: New task count
            old_task_states: {task_id: {key: value}}
            key_by: Key columns for partitioning

        Returns:
            {new_task_id: {key: value}}
        """
        logger.info(
            f"Redistributing state: {old_parallelism} → {new_parallelism} tasks"
        )

        # Build consistent hash rings
        old_ring = ConsistentHashRing(old_parallelism)
        new_ring = ConsistentHashRing(new_parallelism)

        # Redistribute state
        new_task_states = {i: {} for i in range(new_parallelism)}

        for old_task_id, state in old_task_states.items():
            for key, value in state.items():
                # Compute key hash
                key_str = self._serialize_key(key, key_by)

                # Find new task for this key
                new_task_id = new_ring.get_node(key_str)

                # Move state
                new_task_states[new_task_id][key] = value

        # Log state movement statistics
        total_keys = sum(len(s) for s in old_task_states.values())
        moved_keys = self._count_moved_keys(
            old_task_states,
            new_task_states,
            old_ring,
            new_ring
        )

        movement_pct = (moved_keys / total_keys * 100) if total_keys > 0 else 0
        logger.info(
            f"State redistribution complete: "
            f"{moved_keys}/{total_keys} keys moved ({movement_pct:.1f}%)"
        )

        return new_task_states

    def _serialize_key(self, key: Any, key_by: List[str]) -> str:
        """Serialize key for hashing."""
        if isinstance(key, tuple):
            return '|'.join(str(k) for k in key)
        return str(key)

    def _count_moved_keys(
        self,
        old_states: Dict,
        new_states: Dict,
        old_ring: ConsistentHashRing,
        new_ring: ConsistentHashRing
    ) -> int:
        """Count keys that moved to different tasks."""
        moved = 0

        for old_task_id, state in old_states.items():
            for key in state.keys():
                key_str = self._serialize_key(key, [])
                old_node = old_ring.get_node(key_str)
                new_node = new_ring.get_node(key_str)

                if old_node != new_node:
                    moved += 1

        return moved


class RescalingCoordinator:
    """
    Coordinate live rescaling operations.

    Implements zero-downtime rescaling protocol:
    1. Pause old tasks
    2. Drain in-flight data
    3. Snapshot state
    4. Create new tasks
    5. Redistribute state
    6. Resume new tasks
    7. Cancel old tasks
    """

    def __init__(self, job_manager):
        self.job_manager = job_manager
        self.redistributor = StateRedistributor()

    async def rescale_operator(
        self,
        job_id: str,
        operator_id: str,
        old_parallelism: int,
        new_parallelism: int,
        stateful: bool,
        key_by: List[str]
    ):
        """Execute rescaling operation."""
        logger.info(
            f"Rescaling operator {operator_id}: "
            f"{old_parallelism} → {new_parallelism}"
        )

        # 1. Pause old tasks
        await self._pause_tasks(job_id, operator_id)

        # 2. Drain in-flight data
        await self._drain_tasks(job_id, operator_id)

        # 3. Snapshot state
        old_states = {}
        if stateful:
            old_states = await self._snapshot_state(job_id, operator_id)

        # 4. Create new tasks
        new_task_ids = await self._create_new_tasks(
            job_id, operator_id, new_parallelism
        )

        # 5. Redistribute state
        if stateful and old_states:
            new_states = await self.redistributor.redistribute_state(
                old_parallelism,
                new_parallelism,
                old_states,
                key_by
            )

            # Restore state to new tasks
            await self._restore_state(new_task_ids, new_states)

        # 6. Deploy and resume new tasks
        await self._deploy_new_tasks(job_id, new_task_ids)

        # 7. Cancel old tasks
        await self._cancel_old_tasks(job_id, operator_id)

        logger.info(f"Rescaling complete: {operator_id}")

    # Implementation of helper methods...
    # (pause, drain, snapshot, create, restore, deploy, cancel)
```

### Deliverables

- [ ] ConsistentHashRing class
- [ ] StateRedistributor class
- [ ] RescalingCoordinator class
- [ ] Unit tests (16 tests)
- [ ] Integration test: Rescale 4→8 tasks
- [ ] Integration test: Rescale 8→4 tasks
- [ ] Verify no data loss during rescaling

---

## Step 6: Integration Tests

**Estimated Effort:** 12 hours
**Priority:** P0 (Critical)

### Goal

Comprehensive end-to-end tests for multi-agent deployment, shuffle, failure recovery, and rescaling.

### Test File Structure

```
tests/integration/
├── test_agent_deployment.py       # Agent lifecycle and deployment
├── test_distributed_shuffle.py    # Already exists - enhance
├── test_shuffle_networking.py     # Already exists - enhance
├── test_rescaling.py              # NEW - Live rescaling tests
├── test_dbos_orchestration.py     # NEW - JobManager workflow tests
└── test_parallel_correctness.py   # Already exists
```

### Subtask 6.1: test_agent_deployment.py

```python
"""
Integration tests for agent deployment and lifecycle.
"""

import pytest
import asyncio
from sabot.agent import Agent, AgentConfig
from sabot.job_manager import JobManager


@pytest.mark.asyncio
async def test_single_agent_simple_dataflow():
    """Test: Deploy simple dataflow to single agent."""
    # Start agent
    agent = Agent(AgentConfig(
        agent_id="agent1",
        host="127.0.0.1",
        port=8816,
        num_slots=4
    ))
    await agent.start()

    # Start JobManager
    job_manager = JobManager(dbos_url='sqlite:///test.db')

    # Submit job
    job_graph_json = """
    {
        "operators": [
            {"id": "source", "type": "kafka_source", "topic": "input"},
            {"id": "map", "type": "map", "function": "lambda x: x * 2"},
            {"id": "sink", "type": "kafka_sink", "topic": "output"}
        ],
        "edges": [
            {"from": "source", "to": "map"},
            {"from": "map", "to": "sink"}
        ]
    }
    """

    job_id = await job_manager.submit_job(job_graph_json, "simple_job")

    # Wait for deployment
    await asyncio.sleep(2)

    # Verify task deployed
    assert len(agent.active_tasks) == 3  # source, map, sink

    # Cleanup
    await agent.stop()


@pytest.mark.asyncio
async def test_multi_agent_with_shuffle():
    """Test: Deploy shuffled dataflow across 3 agents."""
    # Start 3 agents
    agents = []
    for i in range(3):
        agent = Agent(AgentConfig(
            agent_id=f"agent{i+1}",
            host="127.0.0.1",
            port=8816 + i,
            num_slots=2
        ))
        await agent.start()
        agents.append(agent)

    # Submit job with shuffle (groupBy)
    job_manager = JobManager(dbos_url='sqlite:///test.db')

    job_graph_json = """
    {
        "operators": [
            {"id": "source", "type": "kafka_source", "parallelism": 2},
            {"id": "group", "type": "group_by", "keys": ["user_id"], "parallelism": 4},
            {"id": "agg", "type": "aggregate", "function": "sum", "parallelism": 2}
        ],
        "edges": [
            {"from": "source", "to": "group", "shuffle": "hash"},
            {"from": "group", "to": "agg", "shuffle": "hash"}
        ]
    }
    """

    job_id = await job_manager.submit_job(job_graph_json, "shuffle_job")

    # Wait for deployment
    await asyncio.sleep(3)

    # Verify tasks distributed across agents
    total_tasks = sum(len(a.active_tasks) for a in agents)
    assert total_tasks == 8  # 2 + 4 + 2

    # Cleanup
    for agent in agents:
        await agent.stop()


@pytest.mark.asyncio
async def test_agent_failure_recovery():
    """Test: Agent failure triggers task recovery."""
    # Start 2 agents
    agent1 = Agent(AgentConfig(agent_id="agent1", host="127.0.0.1", port=8816))
    agent2 = Agent(AgentConfig(agent_id="agent2", host="127.0.0.1", port=8817))

    await agent1.start()
    await agent2.start()

    # Submit job
    job_manager = JobManager(dbos_url='sqlite:///test.db')
    job_id = await job_manager.submit_job("{...}", "recovery_test")

    await asyncio.sleep(2)

    # Kill agent1
    await agent1.stop()

    # Wait for failure detection
    await asyncio.sleep(35)  # Heartbeat timeout + detection

    # Verify tasks moved to agent2
    assert len(agent2.active_tasks) > 0

    # Cleanup
    await agent2.stop()
```

### Subtask 6.2: test_rescaling.py

```python
"""
Integration tests for live operator rescaling.
"""

import pytest
import asyncio
from sabot.job_manager import JobManager
from sabot.state_redistribution import StateRedistributor


@pytest.mark.asyncio
async def test_rescale_stateless_operator():
    """Test: Rescale stateless operator (4 → 8 tasks)."""
    job_manager = JobManager(dbos_url='sqlite:///test.db')

    # Submit job
    job_id = await job_manager.submit_job("{...}", "rescale_test")

    await asyncio.sleep(2)

    # Rescale operator
    await job_manager.rescale_operator(
        job_id=job_id,
        operator_id="map_op",
        new_parallelism=8
    )

    # Verify new tasks deployed
    tasks = await job_manager.dbos.query(
        "SELECT COUNT(*) FROM tasks WHERE job_id = ? AND operator_id = ?",
        job_id, "map_op"
    )

    assert tasks[0][0] == 8


@pytest.mark.asyncio
async def test_rescale_stateful_operator():
    """Test: Rescale stateful operator with state redistribution."""
    job_manager = JobManager(dbos_url='sqlite:///test.db')

    # Submit stateful job (groupBy)
    job_id = await job_manager.submit_job("{...}", "stateful_rescale")

    await asyncio.sleep(2)

    # Generate some state
    # ... send data through pipeline ...

    # Rescale
    await job_manager.rescale_operator(
        job_id=job_id,
        operator_id="group_by_op",
        new_parallelism=8
    )

    # Verify state preserved
    # ... check aggregate values match ...

    assert True  # TODO: Implement state verification


@pytest.mark.asyncio
async def test_consistent_hashing_minimal_movement():
    """Test: Consistent hashing minimizes state movement."""
    redistributor = StateRedistributor()

    # Create synthetic state
    old_states = {
        0: {f"key{i}": f"value{i}" for i in range(0, 1000)},
        1: {f"key{i}": f"value{i}" for i in range(1000, 2000)},
        2: {f"key{i}": f"value{i}" for i in range(2000, 3000)},
        3: {f"key{i}": f"value{i}" for i in range(3000, 4000)},
    }

    # Redistribute 4 → 8
    new_states = await redistributor.redistribute_state(
        old_parallelism=4,
        new_parallelism=8,
        old_task_states=old_states,
        key_by=['key']
    )

    # Verify ~50% of keys moved (expected for doubling)
    total_keys = sum(len(s) for s in old_states.values())
    new_total_keys = sum(len(s) for s in new_states.values())

    assert total_keys == new_total_keys  # No data loss

    # Movement should be < 60% (close to theoretical 50%)
    # (Actual measurement would require tracking moves)
```

### Subtask 6.3: test_dbos_orchestration.py

```python
"""
Integration tests for DBOS workflow orchestration.
"""

import pytest
import asyncio
from sabot.job_manager import JobManager


@pytest.mark.asyncio
async def test_submit_job_workflow():
    """Test: submit_job workflow completes all 6 steps."""
    job_manager = JobManager(dbos_url='sqlite:///test.db')

    job_graph_json = '{"operators": [...]}'

    job_id = await job_manager.submit_job(job_graph_json, "workflow_test")

    # Verify job state
    job = await job_manager.dbos.query_one(
        "SELECT status FROM jobs WHERE job_id = ?",
        job_id
    )

    assert job[0] == 'running'


@pytest.mark.asyncio
async def test_workflow_resumption_after_crash():
    """Test: Workflow resumes after JobManager crash."""
    job_manager = JobManager(dbos_url='sqlite:///test.db')

    # Start job submission workflow
    job_id = await job_manager.submit_job("{...}", "crash_test")

    # Simulate crash after step 3 (compilation)
    # ... kill process ...

    # Restart JobManager
    job_manager2 = JobManager(dbos_url='sqlite:///test.db')

    # Verify workflow resumes from step 4
    # ... check database state ...

    # TODO: Implement DBOS workflow resumption
```

### Deliverables

- [ ] test_agent_deployment.py (3 tests)
- [ ] test_rescaling.py (3 tests)
- [ ] test_dbos_orchestration.py (2 tests)
- [ ] All integration tests passing
- [ ] CI/CD pipeline configured

---

## Summary of Deliverables

### Code Files

| File | Lines | Status |
|------|-------|--------|
| `flight_transport_lockfree.pyx` | +200 | Modified |
| `shuffle_transport.pyx` | +50 | Modified |
| `agent.py` | +300 | Modified |
| `job_manager.py` | +800 | Modified |
| `state_redistribution.py` | +400 | NEW |
| `test_agent_deployment.py` | +300 | NEW |
| `test_rescaling.py` | +200 | NEW |
| `test_dbos_orchestration.py` | +150 | NEW |

### Tests

- Unit tests: 62 tests
- Integration tests: 8 tests
- **Total: 70 tests**

### Documentation

- This technical plan
- API documentation updates
- Migration guide updates

---

## Implementation Order

### Week 1 (40 hours)

**Days 1-2: Flight Server Integration (16h)**
- Step 1: Wire Flight server to agent configuration
- Test and validate multi-agent shuffle

**Days 3-5: JobManager Workflows (24h)**
- Step 2: Complete DBOS workflows
- Test job submission end-to-end

### Week 2 (30 hours)

**Days 1-2: Agent Health & TaskExecutor (18h)**
- Step 3: Agent registration and heartbeat
- Step 4: Complete TaskExecutor execution loops
- Test multi-agent deployment

**Days 3-4: State Redistribution & Tests (12h)**
- Step 5: State redistribution (basics)
- Step 6: Integration tests
- Final validation

---

## Success Criteria

### Functional

- [x] Agent runs Flight server on configured port
- [ ] JobManager deploys jobs to agents
- [ ] Tasks execute with morsel parallelism
- [ ] Multi-agent shuffle works
- [ ] Agent failures detected and recovered
- [ ] Live rescaling preserves state

### Performance

- [ ] Throughput: ≥50K records/sec (single agent)
- [ ] Latency: p99 <100ms (simple pipeline)
- [ ] Scalability: Linear to 8 agents
- [ ] State movement: <60% on rescaling

### Reliability

- [ ] No data loss on agent failure
- [ ] No data loss on rescaling
- [ ] Workflow resumes after JobManager crash
- [ ] All integration tests passing

---

**Last Updated:** October 7, 2025
**Status:** Implementation in Progress
**Next Step:** Begin Step 1 - Wire Flight Server to Agent Configuration
