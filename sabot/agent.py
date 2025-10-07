# -*- coding: utf-8 -*-
"""
Agent: A worker node that executes operator tasks.

Agents are NOT user code. They are runtime infrastructure that:
- Execute tasks assigned by JobManager
- Use morsel-driven parallelism locally
- Shuffle data via Arrow Flight
- Report health to JobManager

Supports both local mode (single process) and distributed mode (multi-agent).
"""

import asyncio
import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
import threading
import json

logger = logging.getLogger(__name__)


@dataclass
class AgentConfig:
    """Configuration for agent worker node"""
    agent_id: str
    host: str = "0.0.0.0"
    port: int = 8816
    num_slots: int = 8              # How many tasks can run concurrently
    workers_per_slot: int = 4       # Morsel workers per task
    memory_mb: int = 16384          # Total memory for agent
    cpu_cores: int = 8              # Total CPU cores
    job_manager_url: Optional[str] = None  # For distributed mode


@dataclass
class Slot:
    """Execution slot for task"""
    slot_id: int
    memory_mb: int
    assigned_task: Optional[str] = None

    def is_free(self) -> bool:
        return self.assigned_task is None

    def can_fit(self, task) -> bool:
        # For now, simple memory check
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

    def __init__(self, task, slot, morsel_processor, shuffle_client, is_local_mode=False):
        self.task = task
        self.slot = slot
        self.morsel_processor = morsel_processor
        self.shuffle_client = shuffle_client
        self.is_local_mode = is_local_mode

        # Create operator instance
        self.operator = self._create_operator()

        # Execution state
        self.running = False
        self.execution_task = None

    def _create_operator(self):
        """Instantiate operator from task specification"""
        # This would deserialize operator from task.operator_spec
        # For now, create a simple placeholder
        if hasattr(self.task, 'operator'):
            return self.task.operator
        else:
            # Create operator based on task type
            from .api.stream import Stream
            # This is a simplified version - real implementation would
            # deserialize the operator from task parameters
            return None

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
            if self.operator and self.operator.requires_shuffle():
                await self._execute_with_shuffle()
            else:
                await self._execute_local()
        except Exception as e:
            logger.error(f"Task execution failed: {e}")
            raise

    async def _execute_with_shuffle(self):
        """Execute operator with network shuffle"""
        # Get shuffle configuration from task
        shuffle_config = getattr(self.task, 'parameters', {}).get('shuffle_config', {})
        shuffle_id = shuffle_config.get('shuffle_id', '').encode('utf-8')
        partition_id = getattr(self.task, 'task_index', 0)

        if not shuffle_id:
            logger.warning(f"Task {self.task.task_id} requires shuffle but no shuffle_id configured")
            return

        # Receive partitions for this task from shuffle transport
        # The operator's __iter__ will use shuffle_transport.receive_partitions()
        try:
            # Execute operator - it will internally use shuffle transport to receive data
            for batch in self.operator:
                if batch is not None and batch.num_rows > 0:
                    # Process batch (via morsel parallelism if configured)
                    result = await self._process_batch_with_morsels(batch)

                    # Send result downstream
                    if result is not None:
                        await self._send_downstream(result)
        except Exception as e:
            logger.error(f"Shuffle execution failed for task {self.task.task_id}: {e}")
            raise

    async def _execute_local(self):
        """Execute operator without shuffle"""
        # For local mode, we need to execute the operator directly
        # This is a simplified implementation
        logger.info(f"Executing task {self.task.task_id} locally")

        # In a real implementation, this would:
        # 1. Get input batches from the task's input sources
        # 2. Process them through the operator
        # 3. Send results to output destinations

        # For now, just mark as completed
        await asyncio.sleep(1)  # Simulate some work

    async def _process_batch_with_morsels(self, batch):
        """
        Process batch using morsel-driven parallelism.

        Splits batch into morsels, processes in parallel via workers.
        """
        if not self.morsel_processor or not batch:
            return batch

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
        batches = [m.data for m in morsel_results if hasattr(m, 'data') and m.data is not None]
        if batches:
            import pyarrow as pa
            return pa.concat_batches(batches)
        return None

    async def _send_downstream(self, batch):
        """Send batch to downstream tasks (via shuffle if needed)"""
        # Implementation depends on the specific shuffle transport
        # For now, this is a placeholder
        logger.debug(f"Sending batch downstream: {len(batch) if batch else 0} rows")


class Agent:
    """
    Worker node that executes streaming operator tasks.

    An Agent is NOT user code - it's a runtime component.
    """

    def __init__(self, config: AgentConfig, job_manager=None):
        self.config = config
        self.agent_id = config.agent_id
        self.job_manager = job_manager

        # Task execution
        self.active_tasks: Dict[str, TaskExecutor] = {}
        self.slots: List[Slot] = [
            Slot(i, config.memory_mb // config.num_slots)
            for i in range(config.num_slots)
        ]

        # Morsel parallelism
        from .morsel_parallelism import ParallelProcessor
        self.morsel_processor = ParallelProcessor(
            num_workers=config.num_slots * config.workers_per_slot,
            morsel_size_kb=64
        )

        # Shuffle transport
        from ._cython.shuffle.shuffle_transport import ShuffleTransport
        self.shuffle_transport = ShuffleTransport()

        # State
        self.running = False
        self.is_local_mode = job_manager.is_local_mode if job_manager else True

        logger.info(f"Agent initialized: {self.agent_id} (local_mode={self.is_local_mode})")

    async def start(self):
        """Start agent services"""
        if self.running:
            return

        self.running = True

        # Start morsel processor
        await self.morsel_processor.start()

        # Start shuffle transport with agent's address
        self.shuffle_transport.start(
            self.config.host.encode('utf-8'),
            self.config.port
        )

        if not self.is_local_mode:
            # Start HTTP server for task deployment
            await self._start_http_server()

            # Register with JobManager (distributed mode)
            await self._register_with_job_manager()

            # Start heartbeat loop
            asyncio.create_task(self._heartbeat_loop())

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
        self.shuffle_transport.stop()  # Synchronous Cython method

        logger.info(f"Agent stopped: {self.agent_id}")

    async def deploy_task(self, task):
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
            shuffle_client=self.shuffle_transport,
            is_local_mode=self.is_local_mode
        )

        # Configure shuffle if operator requires it
        if hasattr(task, 'parameters') and task.parameters.get('shuffle_config'):
            shuffle_config = task.parameters['shuffle_config']
            if hasattr(executor.operator, 'set_shuffle_config'):
                # Wire up shuffle transport to operator
                shuffle_id = shuffle_config['shuffle_id'].encode('utf-8')
                task_id = task.task_index if hasattr(task, 'task_index') else 0
                num_partitions = shuffle_config['num_partitions']

                # Get agent addresses from config
                upstream_agents = shuffle_config.get('upstream_agents', [])
                downstream_agents = shuffle_config.get('downstream_agents', [])

                # Start shuffle in transport with agent addresses
                self.shuffle_transport.start_shuffle(
                    shuffle_id,
                    num_partitions,
                    [a.encode('utf-8') if isinstance(a, str) else a for a in downstream_agents],
                    [a.encode('utf-8') if isinstance(a, str) else a for a in upstream_agents]
                )

                executor.operator.set_shuffle_config(
                    self.shuffle_transport,
                    shuffle_id,
                    task_id,
                    num_partitions
                )
                logger.info(
                    f"Configured shuffle for task {task.task_id}: "
                    f"shuffle_id={shuffle_id}, partition={task_id}, "
                    f"upstream={len(upstream_agents)}, downstream={len(downstream_agents)}"
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

    async def execute_job(self, job_graph, job_id: str):
        """Execute a job directly (local mode only)"""
        if not self.is_local_mode:
            raise RuntimeError("Direct job execution only available in local mode")

        logger.info(f"Executing job {job_id} locally")

        # Compile job graph to execution graph
        exec_graph = job_graph.to_execution_graph()

        # Create tasks and deploy them
        for task_vertex in exec_graph.tasks.values():
            await self.deploy_task(task_vertex)

        # Wait for all tasks to complete (simplified)
        # In a real implementation, this would track task completion
        await asyncio.sleep(5)

        logger.info(f"Job {job_id} execution completed")

    def get_status(self) -> dict:
        """Get agent status for health reporting"""
        return {
            'agent_id': self.agent_id,
            'running': self.running,
            'active_tasks': len(self.active_tasks),
            'available_slots': sum(1 for s in self.slots if s.is_free()),
            'total_slots': len(self.slots),
            'is_local_mode': self.is_local_mode
        }

    async def _register_with_job_manager(self):
        """Register agent in database via JobManager API."""
        if not self.job_manager:
            return

        # In distributed mode, this would make an HTTP call
        # For now, register directly with the JobManager
        try:
            await self.job_manager.register_agent({
                'agent_id': self.agent_id,
                'host': self.config.host,
                'port': self.config.port,
                'max_workers': self.config.num_slots,
                'cython_available': True,  # Assume for now
                'gpu_available': False,
            })
        except Exception as e:
            logger.error(f"Failed to register with JobManager: {e}")

    async def _heartbeat_loop(self):
        """Send periodic heartbeats to JobManager."""
        import psutil

        while self.running:
            try:
                # Collect resource usage
                heartbeat_data = {
                    'agent_id': self.agent_id,
                    'cpu_percent': psutil.cpu_percent(),
                    'memory_percent': psutil.virtual_memory().percent,
                    'disk_percent': psutil.disk_usage('/').percent,
                    'active_workers': len(self.active_tasks),
                    'total_processed': sum(
                        getattr(t, 'rows_processed', 0) for t in self.active_tasks.values()
                    ),
                }

                # Send heartbeat
                await self.job_manager.send_heartbeat(heartbeat_data)

                await asyncio.sleep(10)  # Heartbeat every 10 seconds
            except Exception as e:
                logger.error(f"Heartbeat error: {e}")
                await asyncio.sleep(5)

    def _allocate_slot(self, task) -> Optional[Slot]:
        """Find free slot for task"""
        for slot in self.slots:
            if slot.is_free() and slot.can_fit(task):
                slot.assign(task)
                return slot
        return None

    # ========================================================================
    # HTTP API for Task Deployment (Distributed Mode)
    # ========================================================================

    async def _start_http_server(self):
        """Start HTTP server for receiving task deployments from JobManager"""
        from aiohttp import web

        app = web.Application()
        app.router.add_post('/deploy_task', self._handle_deploy_task)
        app.router.add_post('/heartbeat', self._handle_heartbeat_request)
        app.router.add_post('/stop_task', self._handle_stop_task_request)
        app.router.add_get('/status', self._handle_status_request)

        # Start server
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, self.config.host, self.config.port)
        await site.start()

        self._http_runner = runner
        logger.info(f"Agent HTTP server started on {self.config.host}:{self.config.port}")

    async def _handle_deploy_task(self, request):
        """Handle task deployment request from JobManager"""
        from aiohttp import web

        try:
            data = await request.json()
            task_id = data.get('task_id')
            job_id = data.get('job_id')
            operator_type = data.get('operator_type')
            parameters = data.get('parameters', {})

            logger.info(f"Received deploy_task request: {task_id}")

            # Create task object from parameters
            from .execution import TaskVertex, OperatorType
            task = TaskVertex(
                task_id=task_id,
                operator_id=data.get('operator_id', task_id),
                operator_type=OperatorType(operator_type),
                operator_name=data.get('operator_name', ''),
                task_index=data.get('task_index', 0),
                parallelism=data.get('parallelism', 1),
                state='scheduled',
                stateful=data.get('stateful', False),
                key_by=data.get('key_by_columns', []),
                memory_mb=data.get('memory_mb', 256),
                cpu_cores=data.get('cpu_cores', 0.5),
                parameters=parameters
            )

            # Deploy task
            await self.deploy_task(task)

            return web.json_response({'status': 'success', 'task_id': task_id})

        except Exception as e:
            logger.error(f"Failed to deploy task: {e}")
            return web.json_response(
                {'status': 'error', 'message': str(e)},
                status=500
            )

    async def _handle_heartbeat_request(self, request):
        """Handle heartbeat request (just respond with OK)"""
        from aiohttp import web
        return web.json_response({'status': 'alive', 'agent_id': self.agent_id})

    async def _handle_stop_task_request(self, request):
        """Handle stop task request from JobManager"""
        from aiohttp import web

        try:
            data = await request.json()
            task_id = data.get('task_id')

            logger.info(f"Received stop_task request: {task_id}")

            await self.stop_task(task_id)

            return web.json_response({'status': 'success', 'task_id': task_id})

        except Exception as e:
            logger.error(f"Failed to stop task: {e}")
            return web.json_response(
                {'status': 'error', 'message': str(e)},
                status=500
            )

    async def _handle_status_request(self, request):
        """Handle status request"""
        from aiohttp import web

        status = self.get_status()
        return web.json_response(status)
