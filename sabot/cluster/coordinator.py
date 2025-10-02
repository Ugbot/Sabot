#!/usr/bin/env python3
"""
Cluster Coordinator - Core of Sabot's distributed cluster management.

Manages cluster lifecycle, node coordination, and work distribution across
multiple nodes with fault tolerance and automatic scaling.
"""

import asyncio
import json
import logging
import time
import uuid
from typing import Dict, List, Set, Optional, Any, Callable, Tuple
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict, deque
import threading
import socket
import aiohttp
from aiohttp import web
import psutil

from ..monitoring import MetricsCollector, HealthChecker
from .types import (
    ClusterNode, ClusterWork, ClusterConfig,
    NodeStatus, NodeRole
)
from .balancer import LoadBalancer, WorkloadStrategy
from .discovery import NodeDiscovery
from .health import ClusterHealthMonitor
from .fault_tolerance import FailureDetector

# Job execution layer
from ..execution import (
    JobGraph, ExecutionGraph, StreamOperatorNode,
    TaskVertex, TaskState, OperatorType
)
from ..execution.slot_pool import SlotPool, SlotRequest, Slot

logger = logging.getLogger(__name__)


class ClusterCoordinator:
    """
    Core cluster coordinator for distributed Sabot deployments.

    Manages cluster membership, work distribution, health monitoring,
    and fault tolerance across multiple nodes.
    """

    def __init__(self, config: ClusterConfig):
        self.config = config
        self.node_id = config.node_id

        # Cluster state
        self.nodes: Dict[str, ClusterNode] = {}
        self.work_queue: deque = deque(maxlen=config.work_queue_size)
        self.active_work: Dict[str, ClusterWork] = {}

        # Component managers
        self.load_balancer = LoadBalancer(WorkloadStrategy.LEAST_LOADED)
        self.node_discovery = NodeDiscovery(config.discovery_endpoints)
        self.health_monitor = ClusterHealthMonitor()
        self.failure_detector = FailureDetector(self.config.node_timeout)

        # Job execution layer (JobManager-style)
        self.slot_pool = SlotPool()
        self.active_jobs: Dict[str, ExecutionGraph] = {}  # job_id -> ExecutionGraph
        self.job_graphs: Dict[str, JobGraph] = {}  # job_id -> JobGraph (logical)
        self.task_deployments: Dict[str, str] = {}  # task_id -> node_id

        # Monitoring integration
        self.metrics: Optional[MetricsCollector] = None
        self.health_checker: Optional[HealthChecker] = None

        # Web server for cluster communication
        self.app = web.Application()
        self.runner: Optional[web.AppRunner] = None
        self.site: Optional[web.TCPSite] = None

        # Control flags
        self.running = False
        self.is_coordinator = (config.role == NodeRole.COORDINATOR)

        # Setup routes
        self._setup_routes()

        # Initialize self as first node
        self._register_self()

        logger.info(f"ClusterCoordinator initialized with node_id={self.node_id}")

    def _register_self(self) -> None:
        """Register this node in the cluster."""
        self_node = ClusterNode(
            node_id=self.node_id,
            host=self.config.host,
            port=self.config.port,
            role=self.config.role,
            status=NodeStatus.ACTIVE,
            cpu_cores=psutil.cpu_count(),
            memory_gb=psutil.virtual_memory().total / (1024**3)
        )
        self.nodes[self.node_id] = self_node

        # Register slots for this node
        num_slots = self.config.max_workers_per_node
        self.slot_pool.register_node(
            node_id=self.node_id,
            num_slots=num_slots,
            cpu_per_slot=1.0,
            memory_per_slot_mb=1024
        )

    def _setup_routes(self) -> None:
        """Setup HTTP routes for cluster communication."""
        self.app.router.add_get('/health', self._handle_health)
        self.app.router.add_get('/nodes', self._handle_get_nodes)
        self.app.router.add_post('/nodes/join', self._handle_node_join)
        self.app.router.add_post('/nodes/leave', self._handle_node_leave)
        self.app.router.add_post('/work/submit', self._handle_submit_work)
        self.app.router.add_get('/work/status/{work_id}', self._handle_work_status)
        self.app.router.add_post('/heartbeat', self._handle_heartbeat)

        # Job management routes
        self.app.router.add_post('/jobs/submit', self._handle_submit_job)
        self.app.router.add_get('/jobs/{job_id}', self._handle_get_job)
        self.app.router.add_post('/jobs/{job_id}/rescale', self._handle_rescale_job)
        self.app.router.add_post('/jobs/{job_id}/cancel', self._handle_cancel_job)
        self.app.router.add_get('/jobs', self._handle_list_jobs)
        self.app.router.add_get('/slots', self._handle_get_slots)

    async def start(self) -> None:
        """Start the cluster coordinator."""
        if self.running:
            return

        self.running = True
        logger.info("Starting ClusterCoordinator...")

        # Start web server
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, self.config.host, self.config.port)
        await self.site.start()

        # Start background tasks
        asyncio.create_task(self._heartbeat_loop())
        asyncio.create_task(self._health_check_loop())
        asyncio.create_task(self._work_distribution_loop())
        asyncio.create_task(self._node_discovery_loop())

        # Start failure detection
        await self.failure_detector.start()

        logger.info(f"ClusterCoordinator started on {self.config.host}:{self.config.port}")

    async def stop(self) -> None:
        """Stop the cluster coordinator."""
        if not self.running:
            return

        self.running = False
        logger.info("Stopping ClusterCoordinator...")

        # Stop failure detection
        await self.failure_detector.stop()

        # Stop web server
        if self.site:
            await self.site.stop()
        if self.runner:
            await self.runner.cleanup()

        logger.info("ClusterCoordinator stopped")

    async def _heartbeat_loop(self) -> None:
        """Send heartbeats to other nodes."""
        while self.running:
            try:
                await self._send_heartbeats()
                await asyncio.sleep(self.config.heartbeat_interval)
            except Exception as e:
                logger.error(f"Error in heartbeat loop: {e}")
                await asyncio.sleep(5)

    async def _send_heartbeats(self) -> None:
        """Send heartbeat to all known nodes."""
        heartbeat_data = {
            'node_id': self.node_id,
            'timestamp': time.time(),
            'status': 'alive',
            'active_tasks': len([w for w in self.active_work.values() if w.assigned_to == self.node_id])
        }

        for node in self.nodes.values():
            if node.node_id != self.node_id and node.status == NodeStatus.ACTIVE:
                try:
                    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5)) as session:
                        url = f"http://{node.address}/heartbeat"
                        async with session.post(url, json=heartbeat_data) as response:
                            if response.status == 200:
                                # Update node's last heartbeat
                                node.last_heartbeat = time.time()
                except Exception as e:
                    logger.warning(f"Failed to send heartbeat to {node.node_id}: {e}")

    async def _health_check_loop(self) -> None:
        """Perform periodic health checks on cluster nodes."""
        while self.running:
            try:
                await self._perform_health_checks()
                await asyncio.sleep(30)  # Health check every 30 seconds
            except Exception as e:
                logger.error(f"Error in health check loop: {e}")
                await asyncio.sleep(10)

    async def _perform_health_checks(self) -> None:
        """Perform health checks on all nodes."""
        for node in self.nodes.values():
            try:
                # Perform health check
                health_score = await self._check_node_health(node)

                # Update node health
                node.health_score = health_score
                node.last_heartbeat = time.time()

                # Update status based on health
                if health_score > 0.8:
                    node.status = NodeStatus.ACTIVE
                    node.consecutive_failures = 0
                elif health_score > 0.5:
                    node.status = NodeStatus.DEGRADED
                else:
                    node.status = NodeStatus.FAILED
                    node.consecutive_failures += 1

                # Mark node as failed if too many consecutive failures
                if node.consecutive_failures >= 3:
                    node.status = NodeStatus.FAILED
                    await self._handle_node_failure(node)

            except Exception as e:
                logger.error(f"Health check failed for node {node.node_id}: {e}")
                node.consecutive_failures += 1

    async def _check_node_health(self, node: ClusterNode) -> float:
        """Check health of a specific node."""
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                url = f"http://{node.address}/health"
                async with session.get(url) as response:
                    if response.status == 200:
                        data = await response.json()
                        # Return health score from node's health endpoint
                        return data.get('health_score', 0.5)
                    else:
                        return 0.0
        except Exception:
            return 0.0

    async def _work_distribution_loop(self) -> None:
        """Distribute work to available nodes."""
        while self.running:
            try:
                await self._distribute_pending_work()
                await asyncio.sleep(5)  # Distribute work every 5 seconds
            except Exception as e:
                logger.error(f"Error in work distribution loop: {e}")
                await asyncio.sleep(10)

    async def _distribute_pending_work(self) -> None:
        """Distribute pending work to available nodes."""
        if not self.work_queue:
            return

        # Get available nodes
        available_nodes = [
            node for node in self.nodes.values()
            if node.status == NodeStatus.ACTIVE and node.active_tasks < self.config.max_workers_per_node
        ]

        if not available_nodes:
            return

        # Distribute work using load balancer
        work_assigned = 0
        while self.work_queue and available_nodes and work_assigned < 10:  # Limit assignments per cycle
            work = self.work_queue.popleft()

            # Find best node for this work
            best_node = self.load_balancer.select_node(available_nodes, work)

            if best_node:
                await self._assign_work_to_node(work, best_node)
                work_assigned += 1

    async def _assign_work_to_node(self, work: ClusterWork, node: ClusterNode) -> None:
        """Assign work to a specific node."""
        try:
            work.assigned_to = node.node_id
            work.assigned_at = time.time()
            work.status = "assigned"

            # Send work to node
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
                url = f"http://{node.address}/work/execute"
                payload = {
                    'work_id': work.work_id,
                    'work_type': work.work_type,
                    'payload': work.payload
                }

                async with session.post(url, json=payload) as response:
                    if response.status == 200:
                        node.active_tasks += 1
                        self.active_work[work.work_id] = work

                        # Update metrics
                        if self.metrics:
                            self.metrics.increment_counter('sabot_cluster_work_assigned')
                    else:
                        # Re-queue work if assignment failed
                        work.assigned_to = None
                        work.status = "pending"
                        self.work_queue.appendleft(work)

        except Exception as e:
            logger.error(f"Failed to assign work {work.work_id} to node {node.node_id}: {e}")
            # Re-queue work
            work.assigned_to = None
            work.status = "pending"
            self.work_queue.appendleft(work)

    async def _node_discovery_loop(self) -> None:
        """Discover new nodes in the cluster."""
        while self.running:
            try:
                await self._discover_nodes()
                await asyncio.sleep(60)  # Discover nodes every minute
            except Exception as e:
                logger.error(f"Error in node discovery loop: {e}")
                await asyncio.sleep(30)

    async def _discover_nodes(self) -> None:
        """Discover new nodes using configured discovery mechanisms."""
        discovered_nodes = await self.node_discovery.discover_nodes()

        for node_info in discovered_nodes:
            if node_info['node_id'] not in self.nodes:
                # New node discovered
                node = ClusterNode(
                    node_id=node_info['node_id'],
                    host=node_info['host'],
                    port=node_info['port'],
                    status=NodeStatus.JOINING
                )
                self.nodes[node.node_id] = node

                # Attempt to join the node
                await self._join_node(node)

    async def _join_node(self, node: ClusterNode) -> None:
        """Attempt to join a node to the cluster."""
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                url = f"http://{node.address}/cluster/join"
                join_data = {
                    'coordinator_node_id': self.node_id,
                    'coordinator_host': self.config.host,
                    'coordinator_port': self.config.port,
                    'cluster_nodes': [n.to_dict() for n in self.nodes.values()]
                }

                async with session.post(url, json=join_data) as response:
                    if response.status == 200:
                        node.status = NodeStatus.ACTIVE
                        logger.info(f"Node {node.node_id} joined the cluster")

                        # Register slots for new node
                        num_slots = self.config.max_workers_per_node
                        self.slot_pool.register_node(
                            node_id=node.node_id,
                            num_slots=num_slots,
                            cpu_per_slot=1.0,
                            memory_per_slot_mb=1024
                        )
                    else:
                        node.status = NodeStatus.FAILED
                        logger.warning(f"Node {node.node_id} failed to join cluster")

        except Exception as e:
            logger.error(f"Failed to join node {node.node_id}: {e}")
            node.status = NodeStatus.FAILED

    async def _handle_node_failure(self, node: ClusterNode) -> None:
        """Handle node failure by reassigning work and updating cluster state."""
        logger.warning(f"Node {node.node_id} marked as failed")

        # Unregister node's slots from slot pool
        self.slot_pool.unregister_node(node.node_id)

        # Reschedule tasks that were running on failed node
        failed_tasks = [
            task_id for task_id, node_id in self.task_deployments.items()
            if node_id == node.node_id
        ]

        for task_id in failed_tasks:
            # Find task in active jobs
            for exec_graph in self.active_jobs.values():
                if task_id in exec_graph.tasks:
                    task = exec_graph.tasks[task_id]
                    task.update_state(TaskState.FAILED)
                    task.failure_count += 1

                    # Reschedule task if below max retries
                    if task.failure_count < 3:
                        logger.info(f"Rescheduling task {task_id} after node failure")
                        asyncio.create_task(self._allocate_and_deploy_task(task, exec_graph))
                    else:
                        logger.error(f"Task {task_id} failed too many times, marking as permanently failed")

        # Reassign active work from failed node (legacy work queue)
        failed_work = [
            work for work in self.active_work.values()
            if work.assigned_to == node.node_id
        ]

        for work in failed_work:
            if work.retries < work.max_retries:
                # Re-queue work for retry
                work.retries += 1
                work.assigned_to = None
                work.status = "pending"
                self.work_queue.appendleft(work)
            else:
                # Mark work as permanently failed
                work.status = "failed"
                work.completed_at = time.time()

        # Update metrics
        if self.metrics:
            self.metrics.increment_counter('sabot_cluster_node_failures')
            self.metrics.set_gauge('sabot_cluster_active_nodes', len([
                n for n in self.nodes.values() if n.status == NodeStatus.ACTIVE
            ]))

    # HTTP handlers

    async def _handle_health(self, request: web.Request) -> web.Response:
        """Handle health check requests."""
        health_data = {
            'status': 'healthy',
            'node_id': self.node_id,
            'cluster_size': len(self.nodes),
            'active_nodes': len([n for n in self.nodes.values() if n.status == NodeStatus.ACTIVE]),
            'pending_work': len(self.work_queue),
            'active_work': len(self.active_work),
            'timestamp': time.time()
        }
        return web.json_response(health_data)

    async def _handle_get_nodes(self, request: web.Request) -> web.Response:
        """Handle request to get cluster nodes."""
        nodes_data = [node.to_dict() for node in self.nodes.values()]
        return web.json_response({'nodes': nodes_data})

    async def _handle_node_join(self, request: web.Request) -> web.Response:
        """Handle node join requests."""
        try:
            data = await request.json()
            node = ClusterNode.from_dict(data)
            node.status = NodeStatus.JOINING

            self.nodes[node.node_id] = node
            await self._join_node(node)

            return web.json_response({'status': 'joined', 'node_id': node.node_id})
        except Exception as e:
            return web.json_response({'error': str(e)}, status=400)

    async def _handle_node_leave(self, request: web.Request) -> web.Response:
        """Handle node leave requests."""
        try:
            data = await request.json()
            node_id = data['node_id']

            if node_id in self.nodes:
                node = self.nodes[node_id]
                node.status = NodeStatus.LEAVING
                # Handle graceful shutdown
                await self._handle_node_failure(node)

            return web.json_response({'status': 'left', 'node_id': node_id})
        except Exception as e:
            return web.json_response({'error': str(e)}, status=400)

    async def _handle_submit_work(self, request: web.Request) -> web.Response:
        """Handle work submission."""
        try:
            data = await request.json()
            work = ClusterWork(
                work_id=str(uuid.uuid4()),
                work_type=data['work_type'],
                payload=data['payload'],
                priority=data.get('priority', 1)
            )

            self.work_queue.append(work)

            # Update metrics
            if self.metrics:
                self.metrics.increment_counter('sabot_cluster_work_submitted')

            return web.json_response({
                'work_id': work.work_id,
                'status': 'submitted',
                'queue_position': len(self.work_queue)
            })
        except Exception as e:
            return web.json_response({'error': str(e)}, status=400)

    async def _handle_work_status(self, request: web.Request) -> web.Response:
        """Handle work status requests."""
        work_id = request.match_info['work_id']

        if work_id in self.active_work:
            work = self.active_work[work_id]
        else:
            # Check if work was completed
            work = None
            for w in list(self.active_work.values()):
                if w.work_id == work_id and w.completed_at:
                    work = w
                    break

        if work:
            return web.json_response({
                'work_id': work.work_id,
                'status': work.status,
                'assigned_to': work.assigned_to,
                'created_at': work.created_at,
                'assigned_at': work.assigned_at,
                'completed_at': work.completed_at
            })
        else:
            return web.json_response({'error': 'Work not found'}, status=404)

    async def _handle_heartbeat(self, request: web.Request) -> web.Response:
        """Handle heartbeat from other nodes."""
        try:
            data = await request.json()
            node_id = data['node_id']

            if node_id in self.nodes:
                node = self.nodes[node_id]
                node.last_heartbeat = data.get('timestamp', time.time())
                node.active_tasks = data.get('active_tasks', 0)

            return web.json_response({'status': 'received'})
        except Exception as e:
            return web.json_response({'error': str(e)}, status=400)

    # Public API methods

    def submit_work(self, work_type: str, payload: Dict[str, Any], priority: int = 1) -> str:
        """Submit work to the cluster."""
        work = ClusterWork(
            work_id=str(uuid.uuid4()),
            work_type=work_type,
            payload=payload,
            priority=priority
        )

        self.work_queue.append(work)
        return work.work_id

    def get_work_status(self, work_id: str) -> Optional[Dict[str, Any]]:
        """Get status of submitted work."""
        if work_id in self.active_work:
            work = self.active_work[work_id]
            return {
                'work_id': work.work_id,
                'status': work.status,
                'assigned_to': work.assigned_to,
                'created_at': work.created_at,
                'assigned_at': work.assigned_at
            }
        return None

    def get_cluster_status(self) -> Dict[str, Any]:
        """Get overall cluster status."""
        active_nodes = [n for n in self.nodes.values() if n.status == NodeStatus.ACTIVE]
        total_cpu = sum(n.cpu_cores for n in active_nodes)
        total_memory = sum(n.memory_gb for n in active_nodes)

        return {
            'coordinator_node': self.node_id,
            'total_nodes': len(self.nodes),
            'active_nodes': len(active_nodes),
            'total_cpu_cores': total_cpu,
            'total_memory_gb': total_memory,
            'pending_work': len(self.work_queue),
            'active_work': len(self.active_work),
            'nodes': [node.to_dict() for node in self.nodes.values()]
        }

    def set_metrics_collector(self, metrics: MetricsCollector) -> None:
        """Set metrics collector for monitoring."""
        self.metrics = metrics

    def set_health_checker(self, health_checker: HealthChecker) -> None:
        """Set health checker for cluster health monitoring."""
        self.health_checker = health_checker

    # ==================== Job Management API (JobManager-style) ====================

    async def submit_stream_job(self, job_graph: JobGraph) -> str:
        """
        Submit a streaming job for execution.

        This is the main entry point for job submission. It:
        1. Converts JobGraph to ExecutionGraph (logical -> physical)
        2. Allocates slots for all tasks
        3. Deploys tasks to worker nodes
        4. Starts execution

        Args:
            job_graph: Logical job graph to execute

        Returns:
            Job ID
        """
        # Validate job graph
        job_graph.validate()

        # Store logical graph
        self.job_graphs[job_graph.job_id] = job_graph

        # Convert to execution graph (expand parallelism)
        exec_graph = job_graph.to_execution_graph()
        self.active_jobs[job_graph.job_id] = exec_graph

        logger.info(
            f"Submitted job {job_graph.job_id} ({job_graph.job_name}): "
            f"{exec_graph.get_task_count()} tasks from {len(job_graph.operators)} operators"
        )

        # Deploy job asynchronously
        asyncio.create_task(self._deploy_job(exec_graph))

        return job_graph.job_id

    async def _deploy_job(self, exec_graph: ExecutionGraph) -> None:
        """
        Deploy execution graph to cluster.

        Steps:
        1. Allocate slots for all tasks
        2. Deploy tasks to assigned slots
        3. Start task execution
        """
        exec_graph.start_time = time.time()

        # Get all tasks in topological order (sources first)
        source_tasks = exec_graph.get_source_tasks()

        # Allocate slots for all tasks
        for task in exec_graph.tasks.values():
            await self._allocate_and_deploy_task(task, exec_graph)

        logger.info(f"Deployed job {exec_graph.job_id} with {exec_graph.get_task_count()} tasks")

    async def _allocate_and_deploy_task(self, task: TaskVertex, exec_graph: ExecutionGraph) -> bool:
        """
        Allocate slot and deploy a single task.

        Args:
            task: Task to deploy
            exec_graph: Parent execution graph

        Returns:
            True if successfully deployed
        """
        # Create slot request
        request = SlotRequest(
            task_id=task.task_id,
            operator_id=task.operator_id,
            cpu_cores=task.cpu_cores,
            memory_mb=task.memory_mb,
            sharing_group=exec_graph.job_id,  # Tasks from same job can share slots
        )

        # Try to allocate slot
        slot = self.slot_pool.request_slot(request)

        if not slot:
            logger.warning(f"No available slot for task {task.task_id}, will retry")
            task.update_state(TaskState.SCHEDULED)
            return False

        # Assign task to slot
        task.slot_id = slot.slot_id
        task.node_id = slot.node_id
        self.task_deployments[task.task_id] = slot.node_id

        # Deploy task to worker node
        success = await self._deploy_task_to_node(task, slot.node_id)

        if success:
            task.update_state(TaskState.RUNNING)
            self.slot_pool.activate_slot(task.task_id)
            logger.info(f"Deployed task {task.task_id} to node {slot.node_id}")
        else:
            # Release slot if deployment failed
            self.slot_pool.release_slot(task.task_id)
            task.update_state(TaskState.FAILED)
            logger.error(f"Failed to deploy task {task.task_id}")

        return success

    async def _deploy_task_to_node(self, task: TaskVertex, node_id: str) -> bool:
        """
        Send task deployment request to worker node.

        Args:
            task: Task to deploy
            node_id: Worker node ID

        Returns:
            True if deployment succeeded
        """
        if node_id not in self.nodes:
            logger.error(f"Node {node_id} not found")
            return False

        node = self.nodes[node_id]

        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
                url = f"http://{node.address}/tasks/deploy"
                payload = task.to_dict()

                async with session.post(url, json=payload) as response:
                    return response.status == 200

        except Exception as e:
            logger.error(f"Failed to deploy task {task.task_id} to node {node_id}: {e}")
            return False

    async def rescale_operator(self, job_id: str, operator_id: str, new_parallelism: int) -> Dict[str, Any]:
        """
        Live rescale an operator to new parallelism (zero-downtime).

        This implements the core rescaling algorithm:
        1. Create new task instances at new parallelism
        2. Allocate slots for new tasks
        3. Deploy new tasks alongside old tasks
        4. Redistribute state from old tasks to new tasks (if stateful)
        5. Switch traffic to new tasks
        6. Cancel old tasks

        Args:
            job_id: Job ID
            operator_id: Operator to rescale
            new_parallelism: New parallelism level

        Returns:
            Rescaling plan dictionary
        """
        if job_id not in self.active_jobs:
            raise ValueError(f"Job {job_id} not found")

        exec_graph = self.active_jobs[job_id]

        # Perform rescaling in execution graph
        rescale_plan = exec_graph.rescale_operator(operator_id, new_parallelism)

        if rescale_plan['status'] == 'no_change':
            return rescale_plan

        logger.info(
            f"Rescaling job {job_id} operator {operator_id}: "
            f"{rescale_plan['old_parallelism']} -> {rescale_plan['new_parallelism']}"
        )

        # Deploy new tasks
        new_task_ids = rescale_plan['new_task_ids']
        for task_id in new_task_ids:
            task = exec_graph.tasks[task_id]
            await self._allocate_and_deploy_task(task, exec_graph)

        # If stateful, redistribute state
        if rescale_plan['requires_state_redistribution']:
            await self._redistribute_state(
                exec_graph,
                rescale_plan['old_task_ids'],
                rescale_plan['new_task_ids']
            )

        # Cancel old tasks after new tasks are running
        old_task_ids = rescale_plan['old_task_ids']
        for task_id in old_task_ids:
            await self._cancel_task(exec_graph, task_id)

        logger.info(f"Rescaling complete for operator {operator_id}")

        return rescale_plan

    async def _redistribute_state(self, exec_graph: ExecutionGraph,
                                   old_task_ids: List[str], new_task_ids: List[str]) -> None:
        """
        Redistribute state from old tasks to new tasks.

        This is a placeholder for state redistribution logic. In a full implementation:
        1. Read state from old tasks (via state backend)
        2. Repartition state based on new key distribution
        3. Write state to new tasks

        Args:
            exec_graph: Execution graph
            old_task_ids: Old task IDs
            new_task_ids: New task IDs
        """
        logger.info(
            f"Redistributing state from {len(old_task_ids)} old tasks "
            f"to {len(new_task_ids)} new tasks"
        )

        # TODO: Implement state redistribution via Tonbo state backend
        # For now, log a warning
        logger.warning("State redistribution not yet implemented - new tasks will start with empty state")

    async def _cancel_task(self, exec_graph: ExecutionGraph, task_id: str) -> None:
        """
        Cancel a running task.

        Args:
            exec_graph: Execution graph
            task_id: Task ID to cancel
        """
        if task_id not in exec_graph.tasks:
            logger.warning(f"Task {task_id} not found in execution graph")
            return

        task = exec_graph.tasks[task_id]

        if task.node_id and task.node_id in self.nodes:
            node = self.nodes[task.node_id]

            try:
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                    url = f"http://{node.address}/tasks/{task_id}/cancel"
                    async with session.post(url) as response:
                        if response.status == 200:
                            logger.info(f"Canceled task {task_id} on node {task.node_id}")
                        else:
                            logger.error(f"Failed to cancel task {task_id}")
            except Exception as e:
                logger.error(f"Error canceling task {task_id}: {e}")

        # Release slot
        if task.slot_id:
            self.slot_pool.release_slot(task_id)

        # Update task state
        task.update_state(TaskState.CANCELED)

        # Remove from deployments
        if task_id in self.task_deployments:
            del self.task_deployments[task_id]

    async def enable_auto_rebalance(self, job_id: str, interval_seconds: int = 60,
                                     imbalance_threshold: float = 0.3) -> None:
        """
        Enable automatic load rebalancing for a job.

        Monitors task performance and automatically rebalances morsels when
        load imbalance exceeds threshold.

        Args:
            job_id: Job ID
            interval_seconds: Rebalancing check interval
            imbalance_threshold: Imbalance threshold (0.0 to 1.0)
        """
        if job_id not in self.active_jobs:
            raise ValueError(f"Job {job_id} not found")

        logger.info(
            f"Enabling auto-rebalance for job {job_id} "
            f"(interval={interval_seconds}s, threshold={imbalance_threshold})"
        )

        # Start rebalancing loop
        asyncio.create_task(self._auto_rebalance_loop(job_id, interval_seconds, imbalance_threshold))

    async def _auto_rebalance_loop(self, job_id: str, interval_seconds: int,
                                    imbalance_threshold: float) -> None:
        """
        Automatic rebalancing loop.

        Args:
            job_id: Job ID
            interval_seconds: Check interval
            imbalance_threshold: Imbalance threshold
        """
        while job_id in self.active_jobs:
            try:
                await asyncio.sleep(interval_seconds)

                # Check if job still exists
                if job_id not in self.active_jobs:
                    break

                exec_graph = self.active_jobs[job_id]

                # Calculate load imbalance across tasks
                imbalance = self._calculate_load_imbalance(exec_graph)

                if imbalance > imbalance_threshold:
                    logger.info(
                        f"Load imbalance detected for job {job_id}: {imbalance:.2%} "
                        f"(threshold {imbalance_threshold:.2%})"
                    )

                    # Trigger rebalancing
                    await self._rebalance_job(exec_graph)

            except Exception as e:
                logger.error(f"Error in auto-rebalance loop for job {job_id}: {e}")

    def _calculate_load_imbalance(self, exec_graph: ExecutionGraph) -> float:
        """
        Calculate load imbalance across tasks.

        Returns:
            Imbalance score (0.0 = perfectly balanced, 1.0 = maximally imbalanced)
        """
        tasks = [t for t in exec_graph.tasks.values() if t.is_running()]

        if not tasks:
            return 0.0

        # Use rows processed as load metric
        loads = [t.rows_processed for t in tasks]

        if not loads or sum(loads) == 0:
            return 0.0

        avg_load = sum(loads) / len(loads)
        max_load = max(loads)

        # Imbalance = (max - avg) / max
        imbalance = (max_load - avg_load) / max_load if max_load > 0 else 0.0

        return imbalance

    async def _rebalance_job(self, exec_graph: ExecutionGraph) -> None:
        """
        Rebalance load across tasks by migrating morsels.

        This is a placeholder for morsel migration logic.

        Args:
            exec_graph: Execution graph
        """
        logger.info(f"Rebalancing job {exec_graph.job_id}")

        # TODO: Implement morsel migration via DBOSParallelController
        # For now, just log
        logger.warning("Morsel migration not yet implemented")

    # HTTP handlers for job management

    async def _handle_submit_job(self, request: web.Request) -> web.Response:
        """Handle job submission requests."""
        try:
            data = await request.json()

            # Reconstruct job graph from JSON
            job_graph = JobGraph.from_dict(data)

            # Submit job
            job_id = await self.submit_stream_job(job_graph)

            return web.json_response({
                'job_id': job_id,
                'status': 'submitted',
                'task_count': self.active_jobs[job_id].get_task_count()
            })

        except Exception as e:
            logger.error(f"Error submitting job: {e}")
            return web.json_response({'error': str(e)}, status=400)

    async def _handle_get_job(self, request: web.Request) -> web.Response:
        """Handle job status requests."""
        job_id = request.match_info['job_id']

        if job_id not in self.active_jobs:
            return web.json_response({'error': 'Job not found'}, status=404)

        exec_graph = self.active_jobs[job_id]

        return web.json_response({
            'job_id': job_id,
            'job_name': exec_graph.job_name,
            'status': exec_graph.to_dict(),
            'metrics': exec_graph.get_metrics_summary(),
        })

    async def _handle_rescale_job(self, request: web.Request) -> web.Response:
        """Handle operator rescaling requests."""
        try:
            job_id = request.match_info['job_id']
            data = await request.json()

            operator_id = data['operator_id']
            new_parallelism = data['parallelism']

            # Perform rescaling
            rescale_plan = await self.rescale_operator(job_id, operator_id, new_parallelism)

            return web.json_response(rescale_plan)

        except Exception as e:
            logger.error(f"Error rescaling job: {e}")
            return web.json_response({'error': str(e)}, status=400)

    async def _handle_cancel_job(self, request: web.Request) -> web.Response:
        """Handle job cancellation requests."""
        try:
            job_id = request.match_info['job_id']

            if job_id not in self.active_jobs:
                return web.json_response({'error': 'Job not found'}, status=404)

            exec_graph = self.active_jobs[job_id]

            # Cancel all tasks
            for task_id in list(exec_graph.tasks.keys()):
                await self._cancel_task(exec_graph, task_id)

            # Remove job
            del self.active_jobs[job_id]
            if job_id in self.job_graphs:
                del self.job_graphs[job_id]

            logger.info(f"Canceled job {job_id}")

            return web.json_response({'status': 'canceled', 'job_id': job_id})

        except Exception as e:
            logger.error(f"Error canceling job: {e}")
            return web.json_response({'error': str(e)}, status=400)

    async def _handle_list_jobs(self, request: web.Request) -> web.Response:
        """Handle list jobs requests."""
        jobs = []

        for job_id, exec_graph in self.active_jobs.items():
            jobs.append({
                'job_id': job_id,
                'job_name': exec_graph.job_name,
                'task_count': exec_graph.get_task_count(),
                'metrics': exec_graph.get_metrics_summary(),
            })

        return web.json_response({'jobs': jobs})

    async def _handle_get_slots(self, request: web.Request) -> web.Response:
        """Handle slot pool status requests."""
        return web.json_response(self.slot_pool.to_dict())
