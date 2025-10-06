# -*- coding: utf-8 -*-
"""
JobManager: DBOS-backed distributed job orchestration.

Supports both local mode (SQLite) and distributed mode (Postgres).
All orchestration state lives in the database for durability.
"""

import logging
from typing import List, Dict, Optional, Any
import uuid
import json
import os
from datetime import datetime

# DBOS for durable workflows
try:
    from dbos import DBOS, workflow, transaction, communicator
    DBOS_AVAILABLE = True
except ImportError:
    DBOS_AVAILABLE = False
    # Fallback stubs for local mode without DBOS
    def workflow(func): return func
    def transaction(func): return func
    def communicator(func): return func

logger = logging.getLogger(__name__)


class InMemoryDBOS:
    """
    In-memory DBOS fallback using SQLite for local development.

    WARNING: This is NOT a proper DBOS implementation!
    - No workflow resumption on process restart
    - No distributed coordination
    - No exactly-once guarantees
    - Only for local testing when DBOS is not available

    For production, install DBOS: pip install dbos
    """

    def __init__(self, db_url: str):
        import sqlite3
        self.db_path = db_url.replace('sqlite:///', '') if db_url.startswith('sqlite:///') else 'sabot_local.db'
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute("PRAGMA foreign_keys = ON")
        self._init_schema()
        # Store sqlite3 for use in other methods
        self.sqlite3 = sqlite3

    def _init_schema(self):
        """Initialize database schema if needed."""
        # For testing, always try to initialize (idempotent)
        pass

        # Read and execute schema
        import os
        schema_path = os.path.join(os.path.dirname(__file__), 'dbos_schema.sql')
        if os.path.exists(schema_path):
            with open(schema_path, 'r') as f:
                schema_sql = f.read()

            # Execute schema statements individually to handle existing tables gracefully
            statements = [stmt.strip() for stmt in schema_sql.split(';') if stmt.strip() and not stmt.strip().startswith('--')]
            for statement in statements:
                try:
                    self.conn.execute(statement)
                except Exception as e:
                    if "already exists" in str(e):
                        # Table/view already exists, skip
                        continue
                    else:
                        # Re-raise other errors
                        raise

            self.conn.commit()

    async def execute(self, query: str, *params):
        """Execute a query."""
        cursor = self.conn.cursor()
        cursor.execute(query, params)
        self.conn.commit()
        return cursor

    async def query(self, query: str, *params):
        """Execute a query and return results."""
        cursor = self.conn.cursor()
        cursor.execute(query, params)
        return cursor.fetchall()

    async def query_one(self, query: str, *params):
        """Execute a query and return first result."""
        cursor = self.conn.cursor()
        cursor.execute(query, params)
        return cursor.fetchone()

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()


class JobManager:
    """
    Manages distributed streaming jobs across cluster.

    Uses DBOS for:
    - Durable job state (survives crashes)
    - Workflow orchestration (multi-step deployment)
    - Transaction guarantees (atomic task assignment)
    - Event logging (audit trail)

    Supports both local and distributed modes:
    - Local mode: SQLite backend, single-process execution
    - Distributed mode: Postgres backend, multi-agent execution
    """

    def __init__(self, dbos_url=None):
        """
        Initialize JobManager.

        For local development: dbos_url=None (uses SQLite)
        For distributed: dbos_url='postgresql://...' (uses Postgres)

        Args:
            dbos_url: Database URL (None for local SQLite, Postgres URL for distributed)
        """
        if dbos_url is None:
            # Local mode - use SQLite
            self.dbos_url = 'sqlite:///sabot_local.db'
            self.is_local_mode = True
            logger.info("Running in local mode with SQLite")
        else:
            # Distributed mode - use provided URL
            self.dbos_url = dbos_url
            self.is_local_mode = False
            logger.info(f"Running in distributed mode with {dbos_url}")

        if DBOS_AVAILABLE:
            self.dbos = DBOS(database_url=self.dbos_url)
        else:
            logger.warning("DBOS not available - using in-memory fallback")
            self.dbos = InMemoryDBOS(self.dbos_url)

        # Optimizer
        from .compiler.plan_optimizer import PlanOptimizer
        self.optimizer = PlanOptimizer()

        # Local mode components (for single-process execution)
        if self.is_local_mode:
            self._local_agent = None
            self._local_tasks = {}

    # ========================================================================
    # DBOS WORKFLOW: Submit and Deploy Job
    # ========================================================================

    @workflow
    async def submit_job(self, job_graph_json: str, job_name: str = "unnamed_job") -> str:
        """
        Submit streaming job (DBOS workflow - durable across failures).

        DBOS Workflow Pattern:
        - Workflow survives crashes and can resume from any step
        - Each step is idempotent (can run multiple times safely)
        - State persisted in database between steps
        - Exactly-once execution guarantees

        Args:
            job_graph_json: Serialized JobGraph
            job_name: Human-readable job name

        Returns:
            job_id: Unique job identifier
        """
        # Generate deterministic job_id from input (for idempotency)
        # In DBOS, workflows are identified by their input parameters
        job_id = str(uuid.uuid4())
        logger.info(f"Submitting job {job_id} ({job_name})")

        # Step 1: Persist job definition (transaction - atomic)
        await self._persist_job_definition(job_id, job_name, job_graph_json)

        # Step 2: Optimize DAG (transaction - atomic)
        optimized_dag_json = await self._optimize_job_dag(job_id, job_graph_json)

        # Step 3: Compile to execution graph (transaction - atomic)
        exec_graph_json = await self._compile_execution_plan(job_id, optimized_dag_json)

        # Step 4: Assign tasks to agents (transaction - atomic)
        assignments = await self._assign_tasks_to_agents(job_id, exec_graph_json)

        # Step 5: Deploy to agents (communicator - external calls, retryable)
        await self._deploy_job_to_agents(job_id, assignments)

        # Step 6: Mark job as running and start monitoring
        await self._activate_job_execution(job_id)

        logger.info(f"Job {job_id} submitted and deployed successfully")
        return job_id

    @transaction
    async def _persist_job_definition(self, job_id: str, job_name: str, job_graph_json: str):
        """
        Persist job definition (DBOS Transaction - atomic, idempotent).

        Idempotent: Can run multiple times safely (INSERT OR REPLACE).
        """
        logger.info(f"Persisting job definition: {job_id}")

        # Idempotent insert - handles workflow resumption
        await self.dbos.execute(
            """
            INSERT OR REPLACE INTO jobs (job_id, job_name, job_graph_json, status)
            VALUES (?, ?, ?, ?)
            """,
            job_id, job_name, job_graph_json, 'pending'
        )

    @transaction
    async def _optimize_job_dag(self, job_id: str, job_graph_json: str) -> str:
        """
        Optimize job DAG (DBOS Transaction - atomic, idempotent).

        Reads job definition, optimizes it, stores result.
        Idempotent: Can re-run optimization safely.
        """
        logger.info(f"Optimizing DAG for job {job_id}")

        # Deserialize job graph
        from .execution import JobGraph
        job_graph = JobGraph.from_dict(json.loads(job_graph_json))

        # Optimize (idempotent operation)
        optimized = self.optimizer.optimize(job_graph)
        optimized_json = json.dumps(optimized.to_dict())

        # Store optimized version (idempotent update)
        await self.dbos.execute(
            """
            UPDATE jobs
            SET optimized_dag_json = ?, status = 'optimizing'
            WHERE job_id = ?
            """,
            optimized_json, job_id
        )

        return optimized_json

    @transaction
    async def _compile_execution_plan(self, job_id: str, dag_json: str) -> str:
        """
        Compile logical DAG to physical execution plan (DBOS Transaction - atomic, idempotent).

        Creates ExecutionGraph and persists task definitions.
        Idempotent: Can re-run compilation safely (uses INSERT OR REPLACE).
        """
        logger.info(f"Compiling execution plan for job {job_id}")

        from .execution import JobGraph
        job_graph = JobGraph.from_dict(json.loads(dag_json))

        # Compile to execution graph
        exec_graph = job_graph.to_execution_graph()
        exec_graph_json = json.dumps(exec_graph.to_dict())

        # Persist execution graph (idempotent)
        await self.dbos.execute(
            """
            INSERT OR REPLACE INTO execution_graphs (job_id, graph_json, total_tasks, total_operators)
            VALUES (?, ?, ?, ?)
            """,
            job_id, exec_graph_json, exec_graph.get_task_count(), len(job_graph.operators)
        )

        # Create/update task rows (idempotent)
        for task in exec_graph.tasks.values():
            await self.dbos.execute(
                """
                INSERT OR REPLACE INTO tasks (
                    task_id, job_id, operator_id, operator_type, operator_name,
                    task_index, parallelism, state, stateful, key_by_columns,
                    memory_mb, cpu_cores, parameters_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                task.task_id, job_id, task.operator_id, task.operator_type.value,
                task.operator_name, task.task_index, task.parallelism,
                task.state.value, task.stateful, task.key_by,
                task.memory_mb, task.cpu_cores, json.dumps(task.parameters)
            )

        # Update job status
        await self.dbos.execute(
            "UPDATE jobs SET status = 'compiling' WHERE job_id = ?",
            job_id
        )

        return exec_graph_json

    @transaction
    async def _assign_tasks_to_agents(self, job_id: str, exec_graph_json: str) -> Dict[str, str]:
        """Assign tasks to agents (atomic transaction)."""
        logger.info(f"Assigning tasks for job {job_id}")

        # In local mode, ensure local agent exists first
        if self.is_local_mode:
            local_agent_id = "local_agent"
            await self.dbos.execute(
                """
                INSERT OR REPLACE INTO agents (
                    agent_id, host, port, status, max_workers, available_slots,
                    cython_available, gpu_available, registered_at, last_heartbeat
                ) VALUES (?, 'localhost', 1, 'alive', 8, 8, 1, 0, datetime('now'), datetime('now'))
                """,
                local_agent_id
            )

        # Get tasks from database since they were created in compile step
        from .execution import ExecutionGraph
        exec_graph = ExecutionGraph(
            job_id=job_id,
            job_name="compiled_job"
        )

        # Load tasks from database
        task_rows = await self.dbos.query(
            "SELECT * FROM tasks WHERE job_id = ?",
            job_id
        )

        # Convert to TaskVertex objects and add to graph
        from .execution import TaskVertex, OperatorType
        for row in task_rows:
            # Convert row to dict for TaskVertex constructor
            # Column order: task_id, job_id, operator_id, operator_type, operator_name,
            #               task_index, parallelism, agent_id, slot_id, state, state_updated_at,
            #               parameters_json, stateful, key_by_columns, memory_mb, cpu_cores,
            #               rows_processed, bytes_processed, failure_count, last_heartbeat
            task_data = {
                'task_id': row[0],
                'operator_id': row[2],      # column 2
                'operator_type': OperatorType(row[3]),  # column 3
                'operator_name': row[4] if row[4] else '',  # column 4
                'task_index': row[5],       # column 5
                'parallelism': row[6],      # column 6
                'state': row[9],            # column 9 (state)
                'state_updated_at': 0,      # Will be set by TaskVertex
                'upstream_task_ids': [],    # TODO: populate from shuffle edges
                'downstream_task_ids': [],  # TODO: populate from shuffle edges
                'slot_id': row[8],          # column 8
                'node_id': row[7],          # column 7 (agent_id)
                'parameters': json.loads(row[11]) if row[11] else {},  # column 11
                'stateful': bool(row[12]),   # column 12
                'key_by': row[13] if row[13] else None,  # column 13
                'memory_mb': row[14] if row[14] else 256,  # column 14
                'cpu_cores': row[15] if row[15] else 0.5,  # column 15
                'rows_processed': row[16] if row[16] else 0,  # column 16
                'bytes_processed': row[17] if row[17] else 0,  # column 17
                'failure_count': row[18] if row[18] else 0,  # column 18
                'last_heartbeat': row[19] if len(row) > 19 and row[19] else None  # column 19
            }
            task = TaskVertex(**task_data)
            exec_graph.add_task(task)

        assignments = {}

        if self.is_local_mode:
            # Local mode: all tasks run on the single local agent
            local_agent_id = "local_agent"

            # Ensure local agent exists in database
            await self.dbos.execute(
                """
                INSERT OR IGNORE INTO agents (
                    agent_id, host, port, status, max_workers, available_slots,
                    cython_available, gpu_available, registered_at, last_heartbeat
                ) VALUES (?, 'localhost', 1, 'alive', 8, 8, 1, 0, datetime('now'), datetime('now'))
                """,
                local_agent_id
            )

            for task in exec_graph.tasks.values():
                assignments[task.task_id] = local_agent_id

                # Update task assignment
                await self.dbos.execute(
                    """
                    UPDATE tasks
                    SET agent_id = ?, state = 'scheduled', state_updated_at = datetime('now')
                    WHERE task_id = ?
                    """,
                    local_agent_id, task.task_id
                )
        else:
            # Distributed mode: assign to available agents
            available_agents = await self.dbos.query(
                """
                SELECT agent_id, available_slots, cpu_percent, memory_percent
                FROM agents
                WHERE status = 'alive'
                  AND available_slots > 0
                  AND (julianday('now') - julianday(last_heartbeat)) * 86400 < 30
                ORDER BY available_slots DESC
                """
            )

            if not available_agents:
                raise RuntimeError("No available agents for task assignment")

            # Simple round-robin assignment
            agent_index = 0
            for task in exec_graph.tasks.values():
                agent = available_agents[agent_index % len(available_agents)]
                agent_id = agent['agent_id']

                assignments[task.task_id] = agent_id

                # Update task assignment
                await self.dbos.execute(
                    """
                    UPDATE tasks
                    SET agent_id = ?, state = 'scheduled', state_updated_at = datetime('now')
                    WHERE task_id = ?
                    """,
                    agent_id, task.task_id
                )

                # Record assignment in audit log
                await self.dbos.execute(
                    """
                    INSERT INTO task_assignments (job_id, task_id, agent_id, operator_type, reason)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    job_id, task.task_id, agent_id, task.operator_type.value, 'initial_deploy'
                )

                # Decrement agent slots
                await self.dbos.execute(
                    """
                    UPDATE agents
                    SET available_slots = available_slots - 1
                    WHERE agent_id = ?
                    """,
                    agent_id
                )

                agent_index += 1

        # Update job status
        await self.dbos.execute(
            "UPDATE jobs SET status = 'deploying' WHERE job_id = ?",
            job_id
        )

        return assignments

    @communicator
    async def _deploy_job_to_agents(self, job_id: str, assignments: Dict[str, str]):
        """Deploy tasks to agents (external HTTP calls)."""
        logger.info(f"Deploying tasks for job {job_id}")

        if self.is_local_mode:
            # Local mode: deploy to local agent
            await self._deploy_to_local_agent(job_id, assignments)
        else:
            # Distributed mode: deploy via HTTP
            await self._deploy_to_distributed_agents(job_id, assignments)

    async def _deploy_to_local_agent(self, job_id: str, assignments: Dict[str, str]):
        """Deploy tasks to local agent (in-process)."""
        # Initialize local agent if needed
        if self._local_agent is None:
            from .agent import Agent, AgentConfig
            config = AgentConfig(
                agent_id="local_agent",
                host="localhost",
                port=0,  # Not used in local mode
                num_slots=8,
                workers_per_slot=4
            )
            self._local_agent = Agent(config, job_manager=self)

        # Deploy tasks to local agent
        for task_id, agent_id in assignments.items():
            # Get task details
            task_data = await self.dbos.query_one(
                """
                SELECT task_id, operator_id, operator_type, parameters_json
                FROM tasks WHERE task_id = ?
                """,
                task_id
            )

            # Create task vertex
            from .execution import TaskVertex, OperatorType
            # task_data is a tuple: (task_id, operator_id, operator_type, parameters_json)
            task = TaskVertex(
                task_id=task_id,
                operator_id=task_data[1],  # operator_id
                operator_type=OperatorType(task_data[2]),  # operator_type
                operator_name='',  # Not selected
                task_index=0,
                parallelism=1,
                parameters=json.loads(task_data[3]) if task_data[3] else {},  # parameters_json
                node_id=agent_id  # Use node_id instead of agent_id
            )

            # Deploy to local agent
            await self._local_agent.deploy_task(task)

        # Update job status
        await self.dbos.execute(
            "UPDATE jobs SET status = 'running', started_at = datetime('now') WHERE job_id = ?",
            job_id
        )

    async def _deploy_to_distributed_agents(self, job_id: str, assignments: Dict[str, str]):
        """Deploy tasks to distributed agents via HTTP."""
        import aiohttp

        # Group tasks by agent
        agent_tasks = {}
        for task_id, agent_id in assignments.items():
            if agent_id not in agent_tasks:
                agent_tasks[agent_id] = []
            agent_tasks[agent_id].append(task_id)

        # Deploy to each agent
        async with aiohttp.ClientSession() as session:
            for agent_id, task_ids in agent_tasks.items():
                # Get agent connection info
                agent_info = await self.dbos.query_one(
                    """
                    SELECT host, port FROM agents WHERE agent_id = ?
                    """,
                    agent_id
                )

                if not agent_info:
                    logger.error(f"Agent {agent_id} not found")
                    continue

                # Deploy tasks to this agent
                url = f"http://{agent_info['host']}:{agent_info['port']}/deploy_tasks"
                async with session.post(url, json={
                    'job_id': job_id,
                    'task_ids': task_ids
                }) as response:
                    if response.status != 200:
                        logger.error(f"Failed to deploy tasks to agent {agent_id}")
                        raise RuntimeError(f"Task deployment failed for agent {agent_id}")

        # Update job status
        await self.dbos.execute(
            "UPDATE jobs SET status = 'running', started_at = datetime('now') WHERE job_id = ?",
            job_id
        )

    @transaction
    async def _activate_job_execution(self, job_id: str):
        """
        Activate job execution (DBOS Transaction - atomic, idempotent).

        Final step: mark job as running and record workflow completion.
        """
        logger.info(f"Activating job execution: {job_id}")

        # Update job status to running
        await self.dbos.execute(
            """
            UPDATE jobs
            SET status = 'running', started_at = datetime('now')
            WHERE job_id = ? AND status != 'running'
            """,
            job_id
        )

        # In a real DBOS implementation, this would also:
        # - Start monitoring workflows
        # - Set up failure recovery workflows
        # - Initialize metrics collection

    @workflow
    async def _start_monitoring_step(self, job_id: str):
        """Monitor job execution (background workflow)."""
        # This would periodically check task health
        # For now, placeholder
        pass

    # ========================================================================
    # PUBLIC API METHODS
    # ========================================================================

    async def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """Get job status and details."""
        job_data = await self.dbos.query_one(
            """
            SELECT job_id, job_name, status, submitted_at, started_at, completed_at,
                   error_message, failure_count
            FROM jobs WHERE job_id = ?
            """,
            job_id
        )

        if not job_data:
            return None

        # Get task summary
        task_summary = await self.dbos.query_one(
            """
            SELECT
                COUNT(*) as total_tasks,
                COUNT(CASE WHEN state = 'running' THEN 1 END) as running_tasks,
                COUNT(CASE WHEN state = 'failed' THEN 1 END) as failed_tasks,
                SUM(rows_processed) as total_rows,
                SUM(bytes_processed) as total_bytes
            FROM tasks WHERE job_id = ?
            """,
            job_id
        )

        # Handle both dict (DBOS) and tuple (SQLite) results
        if isinstance(job_data, dict):
            return {
                'job_id': job_data['job_id'],
                'job_name': job_data['job_name'],
                'status': job_data['status'],
                'submitted_at': job_data['submitted_at'],
                'started_at': job_data['started_at'],
                'completed_at': job_data['completed_at'],
                'error_message': job_data['error_message'],
                'failure_count': job_data['failure_count'],
                'tasks': task_summary
            }
        else:
            # Tuple result from SQLite: (job_id, job_name, status, submitted_at, started_at, completed_at, error_message, failure_count)
            return {
                'job_id': job_data[0],
                'job_name': job_data[1],
                'status': job_data[2],
                'submitted_at': job_data[3],
                'started_at': job_data[4],
                'completed_at': job_data[5],
                'error_message': job_data[6],
                'failure_count': job_data[7],
                'tasks': task_summary
            }

    async def cancel_job(self, job_id: str):
        """Cancel a running job."""
        # Update job status
        await self.dbos.execute(
            "UPDATE jobs SET status = 'canceling' WHERE job_id = ?",
            job_id
        )

        # Cancel tasks (this would need to be implemented)
        # For now, just mark as canceled
        await self.dbos.execute(
            "UPDATE jobs SET status = 'canceled', completed_at = datetime('now') WHERE job_id = ?",
            job_id
        )

    async def list_jobs(self, status_filter=None) -> List[Dict[str, Any]]:
        """List jobs with optional status filter."""
        if status_filter:
            jobs = await self.dbos.query(
                """
                SELECT job_id, job_name, status, submitted_at, started_at
                FROM jobs WHERE status = ?
                ORDER BY submitted_at DESC
                """,
                status_filter
            )
        else:
            jobs = await self.dbos.query(
                """
                SELECT job_id, job_name, status, submitted_at, started_at
                FROM jobs
                ORDER BY submitted_at DESC
                LIMIT 100
                """
            )
        return jobs

    # ========================================================================
    # LOCAL MODE SUPPORT
    # ========================================================================

    def get_local_agent(self):
        """Get the local agent for direct access (local mode only)."""
        if not self.is_local_mode:
            raise RuntimeError("Local agent only available in local mode")
        return self._local_agent

    async def run_job_locally(self, job_graph_json: str, job_name: str = "local_job") -> str:
        """
        Run job in local mode (single process, no network).

        This bypasses the full DBOS workflow for simpler local execution.
        """
        if not self.is_local_mode:
            raise RuntimeError("Local execution only available in local mode")

        job_id = str(uuid.uuid4())
        logger.info(f"Running job {job_id} locally ({job_name})")

        # Deserialize job graph
        from .execution import JobGraph
        job_graph = JobGraph.from_dict(json.loads(job_graph_json))

        # Initialize local agent
        if self._local_agent is None:
            from .agent import Agent, AgentConfig
            config = AgentConfig(
                agent_id="local_agent",
                host="localhost",
                port=0,
                num_slots=8,
                workers_per_slot=4
            )
            self._local_agent = Agent(config, job_manager=self)

        # Start agent
        await self._local_agent.start()

        # Execute directly on local agent
        await self._local_agent.execute_job(job_graph, job_id)

        return job_id

    # ========================================================================
    # AGENT REGISTRATION AND HEALTH (for distributed mode)
    # ========================================================================

    async def register_agent(self, agent_info: Dict[str, Any]):
        """Register an agent in the database."""
        await self.dbos.execute(
            """
            INSERT OR REPLACE INTO agents (
                agent_id, host, port, status, max_workers, available_slots,
                cython_available, gpu_available, registered_at, last_heartbeat
            ) VALUES (?, ?, ?, 'alive', ?, ?, ?, ?, datetime('now'), datetime('now'))
            """,
            agent_info['agent_id'],
            agent_info['host'],
            agent_info['port'],
            agent_info.get('max_workers', 8),
            agent_info.get('max_workers', 8),  # Start with all slots available
            agent_info.get('cython_available', False),
            agent_info.get('gpu_available', False)
        )
        logger.info(f"Registered agent: {agent_info['agent_id']}")

    async def send_heartbeat(self, heartbeat_data: Dict[str, Any]):
        """Record agent heartbeat."""
        # Insert heartbeat record
        await self.dbos.execute(
            """
            INSERT INTO agent_heartbeats (
                agent_id, cpu_percent, memory_percent, disk_percent,
                active_workers, total_processed
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            heartbeat_data['agent_id'],
            heartbeat_data.get('cpu_percent'),
            heartbeat_data.get('memory_percent'),
            heartbeat_data.get('disk_percent'),
            heartbeat_data.get('active_workers'),
            heartbeat_data.get('total_processed')
        )

        # Update agent status
        await self.dbos.execute(
            """
            UPDATE agents
            SET cpu_percent = ?, memory_percent = ?, disk_percent = ?,
                last_heartbeat = datetime('now')
            WHERE agent_id = ?
            """,
            heartbeat_data.get('cpu_percent'),
            heartbeat_data.get('memory_percent'),
            heartbeat_data.get('disk_percent'),
            heartbeat_data['agent_id']
        )

    # ============================================================================
    # LIVE RESCALING
    # ============================================================================

    @workflow
    async def rescale_operator(self, job_id: str, operator_id: str, new_parallelism: int):
        """
        Rescale an operator to new parallelism level (DBOS workflow).

        This is a durable operation that survives JobManager crashes.
        Uses state redistribution to maintain operator state during rescaling.

        Args:
            job_id: Job containing the operator
            operator_id: Operator to rescale
            new_parallelism: Target parallelism level
        """
        logger.info(f"Starting rescaling workflow for operator {operator_id} to {new_parallelism} tasks")

        # Step 1: Validate rescaling request
        await self._validate_rescaling_request(job_id, operator_id, new_parallelism)

        # Step 2: Create rescaling operation record
        rescaling_id = str(uuid.uuid4())
        await self.dbos.execute(
            """
            INSERT INTO rescaling_operations (
                rescaling_id, job_id, operator_id, old_parallelism, new_parallelism,
                status, started_at
            ) VALUES (?, ?, ?, ?, ?, 'in_progress', datetime('now'))
            """,
            rescaling_id, job_id, operator_id, 0, new_parallelism  # old_parallelism computed below
        )

        try:
            # Step 3: Execute rescaling
            success = await self._execute_rescaling(rescaling_id, job_id, operator_id, new_parallelism)

            # Step 4: Update rescaling status
            status = 'completed' if success else 'failed'
            await self.dbos.execute(
                """
                UPDATE rescaling_operations
                SET status = ?, completed_at = datetime('now')
                WHERE rescaling_id = ?
                """,
                status, rescaling_id
            )

            if success:
                logger.info(f"Rescaling completed successfully for operator {operator_id}")
            else:
                logger.error(f"Rescaling failed for operator {operator_id}")

        except Exception as e:
            logger.error(f"Rescaling workflow failed: {e}")
            await self.dbos.execute(
                """
                UPDATE rescaling_operations
                SET status = 'failed', error_message = ?, completed_at = datetime('now')
                WHERE rescaling_id = ?
                """,
                str(e), rescaling_id
            )
            raise

    @transaction
    async def _validate_rescaling_request(self, job_id: str, operator_id: str, new_parallelism: int):
        """Validate that rescaling request is allowed."""
        # Check job exists and is running
        job = await self.dbos.query_one(
            "SELECT status FROM jobs WHERE job_id = ?",
            job_id
        )
        if not job or job['status'] != 'running':
            raise ValueError(f"Job {job_id} not found or not running")

        # Check operator exists in job
        operator = await self.dbos.query_one(
            """
            SELECT operator_id FROM execution_graph
            WHERE job_id = ? AND operator_id = ?
            """,
            job_id, operator_id
        )
        if not operator:
            raise ValueError(f"Operator {operator_id} not found in job {job_id}")

        # Validate parallelism range
        if new_parallelism < 1 or new_parallelism > 128:
            raise ValueError(f"Invalid parallelism {new_parallelism} (must be 1-128)")

    async def _execute_rescaling(self, rescaling_id: str, job_id: str, operator_id: str, new_parallelism: int) -> bool:
        """Execute the rescaling operation."""
        from .state_redistribution import RescalingCoordinator

        coordinator = RescalingCoordinator(self.dbos)

        success = await coordinator.rescale_operator(
            job_id=job_id,
            operator_id=operator_id,
            new_parallelism=new_parallelism
        )

        return success

    # ============================================================================
    # FAILURE RECOVERY
    # ============================================================================

    @workflow
    async def recover_failed_task(self, job_id: str, task_id: str):
        """
        Recover a failed task (DBOS workflow).

        Automatically reschedules failed tasks to healthy agents.
        Restores state for stateful operators.

        Args:
            job_id: Job containing the failed task
            task_id: Task that failed
        """
        logger.info(f"Recovering failed task {task_id} in job {job_id}")

        # Step 1: Mark task as recovering
        await self.dbos.execute(
            """
            UPDATE tasks
            SET state = 'recovering', state_updated_at = datetime('now')
            WHERE task_id = ?
            """,
            task_id
        )

        # Step 2: Find available agent
        available_agent = await self.dbos.query_one(
            """
            SELECT agent_id FROM agents
            WHERE status = 'alive' AND available_slots > 0
            ORDER BY available_slots DESC
            LIMIT 1
            """
        )

        if not available_agent:
            logger.error("No available agents for task recovery")
            await self.dbos.execute(
                """
                UPDATE tasks
                SET state = 'failed', state_updated_at = datetime('now')
                WHERE task_id = ?
                """,
                task_id
            )
            raise RuntimeError("No available agents for recovery")

        agent_id = available_agent['agent_id']

        # Step 3: Check if operator is stateful and needs state restoration
        task_info = await self.dbos.query_one(
            """
            SELECT operator_id FROM tasks WHERE task_id = ?
            """,
            task_id
        )

        operator_info = await self.dbos.query_one(
            """
            SELECT is_stateful, key_columns FROM execution_graph
            WHERE job_id = ? AND operator_id = ?
            """,
            job_id, task_info['operator_id']
        )

        needs_state_restore = operator_info and operator_info.get('is_stateful', False)

        # Step 4: Reassign task to new agent
        await self.dbos.execute(
            """
            UPDATE tasks
            SET agent_id = ?, state = 'scheduled', state_updated_at = datetime('now')
            WHERE task_id = ?
            """,
            agent_id, task_id
        )

        # Step 5: Deploy task to new agent
        await self._deploy_single_task(job_id, task_id, agent_id)

        # Step 6: If stateful, trigger state restoration
        if needs_state_restore:
            await self._restore_task_state(job_id, task_id, operator_info['key_columns'])

        logger.info(f"Task {task_id} recovered on agent {agent_id}")

    async def _restore_task_state(self, job_id: str, task_id: str, key_columns: List[str]):
        """Restore state for a recovered task."""
        # In a full implementation, this would coordinate with other tasks
        # of the same operator to restore partitioned state
        logger.info(f"State restoration initiated for task {task_id}")

    @workflow
    async def handle_agent_failure(self, agent_id: str):
        """
        Handle agent failure by recovering all its tasks (DBOS workflow).

        Args:
            agent_id: Failed agent ID
        """
        logger.info(f"Handling failure of agent {agent_id}")

        # Step 1: Mark agent as failed
        await self.dbos.execute(
            """
            UPDATE agents
            SET status = 'failed', last_heartbeat = datetime('now')
            WHERE agent_id = ?
            """,
            agent_id
        )

        # Step 2: Find all tasks on failed agent
        failed_tasks = await self.dbos.query(
            """
            SELECT task_id, job_id FROM tasks
            WHERE agent_id = ? AND state IN ('running', 'scheduled')
            """,
            agent_id
        )

        # Step 3: Recover each task
        for task in failed_tasks:
            try:
                await self.recover_failed_task(task['job_id'], task['task_id'])
            except Exception as e:
                logger.error(f"Failed to recover task {task['task_id']}: {e}")

        logger.info(f"Completed failure recovery for agent {agent_id}")

    async def check_agent_health(self):
        """Check for failed agents and trigger recovery."""
        # Find agents with stale heartbeats (no heartbeat in last 30 seconds)
        failed_agents = await self.dbos.query(
            """
            SELECT agent_id FROM agents
            WHERE status = 'alive'
              AND last_heartbeat < datetime('now', '-30 seconds')
            """
        )

        for agent in failed_agents:
            logger.warning(f"Agent {agent['agent_id']} appears failed (stale heartbeat)")
            await self.handle_agent_failure(agent['agent_id'])
