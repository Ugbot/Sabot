# -*- coding: utf-8 -*-
"""Distributed DBOS Coordinator for multi-node Sabot deployments."""

import asyncio
import json
import logging
import time
import uuid
from typing import Dict, List, Set, Optional, Any, Tuple
from dataclasses import dataclass, field
from collections import defaultdict, deque
import aiohttp
import aiohttp.web
from aiohttp import web
import psutil
import threading

from .dbos_parallel_controller import DBOSParallelController, get_system_resource_summary

logger = logging.getLogger(__name__)

@dataclass
class NodeInfo:
    """Information about a cluster node."""
    node_id: str
    host: str
    port: int
    status: str = "unknown"  # unknown, alive, dead, joining, leaving
    last_heartbeat: float = field(default_factory=time.time)
    capabilities: Dict[str, Any] = field(default_factory=dict)
    active_workers: int = 0
    total_processed: int = 0
    system_resources: Dict[str, Any] = field(default_factory=dict)

    @property
    def is_alive(self) -> bool:
        """Check if node is alive based on heartbeat."""
        return (time.time() - self.last_heartbeat) < 30.0  # 30 second timeout

    @property
    def health_score(self) -> float:
        """Calculate node health score (0.0 to 1.0)."""
        if not self.is_alive:
            return 0.0

        # Base score on resource availability
        cpu_available = max(0, 100 - self.system_resources.get('cpu_percent', 0))
        memory_available = max(0, 100 - self.system_resources.get('memory_percent', 0))

        # Weighted average
        score = (cpu_available * 0.6 + memory_available * 0.4) / 100.0
        return min(1.0, score)

@dataclass
class DistributedJob:
    """A job that can be distributed across nodes."""
    job_id: str
    data: Any
    processor_func: callable
    priority: int = 1
    created_at: float = field(default_factory=time.time)
    status: str = "pending"  # pending, running, completed, failed
    assigned_node: Optional[str] = None
    result: Any = None
    error: Optional[str] = None

class DistributedCoordinator:
    """Central coordinator for distributed Sabot deployments.

    Manages multiple Sabot nodes, distributes work, and provides
    fault tolerance and load balancing across the cluster.
    """

    def __init__(self,
                 host: str = "0.0.0.0",
                 port: int = 8080,
                 cluster_name: str = "sabot-cluster",
                 node_timeout: float = 30.0):
        self.host = host
        self.port = port
        self.cluster_name = cluster_name
        self.node_timeout = node_timeout

        # Node management
        self.local_node_id = str(uuid.uuid4())
        self.nodes: Dict[str, NodeInfo] = {}
        self.node_lock = threading.RLock()

        # Job management
        self.jobs: Dict[str, DistributedJob] = {}
        self.pending_jobs: deque = deque()
        self.job_lock = threading.RLock()

        # HTTP components
        self.app = None
        self.runner = None
        self.site = None

        # Control components
        self.running = False
        self.heartbeat_thread: Optional[threading.Thread] = None
        self.coordination_thread: Optional[threading.Thread] = None

        # Local Sabot instance (for single-node mode)
        self.local_sabot = None

        logger.info(f"Initialized Distributed Coordinator (node_id={self.local_node_id})")

    async def start(self):
        """Start the distributed coordinator."""
        if self.running:
            return

        self.running = True
        logger.info(f"Starting Distributed Coordinator on {self.host}:{self.port}")

        # Register local node
        self._register_local_node()

        # Start HTTP server
        await self._start_http_server()

        # Start background threads
        self._start_background_threads()

        logger.info("Distributed Coordinator started successfully")

    async def stop(self):
        """Stop the distributed coordinator."""
        if not self.running:
            return

        logger.info("Stopping Distributed Coordinator")
        self.running = False

        # Stop background threads
        self._stop_background_threads()

        # Stop HTTP server
        await self._stop_http_server()

        logger.info("Distributed Coordinator stopped")

    def _register_local_node(self):
        """Register this node in the cluster."""
        with self.node_lock:
            self.nodes[self.local_node_id] = NodeInfo(
                node_id=self.local_node_id,
                host=self.host,
                port=self.port,
                status="alive",
                capabilities={
                    "cython_available": self._check_cython_available(),
                    "gpu_available": self._check_gpu_available(),
                    "max_workers": psutil.cpu_count(),
                },
                system_resources=get_system_resource_summary()
            )

    def _check_cython_available(self) -> bool:
        """Check if Cython optimizations are available."""
        try:
            from ._cython import morsel_parallelism
            return True
        except ImportError:
            return False

    def _check_gpu_available(self) -> bool:
        """Check if GPU resources are available."""
        try:
            import cudf  # cuDF indicates GPU availability
            return True
        except ImportError:
            return False

    async def _start_http_server(self):
        """Start the HTTP server for node communication."""
        self.app = web.Application()

        # API routes
        self.app.router.add_get('/health', self._handle_health)
        self.app.router.add_post('/register', self._handle_register_node)
        self.app.router.add_post('/heartbeat', self._handle_heartbeat)
        self.app.router.add_post('/submit_job', self._handle_submit_job)
        self.app.router.add_get('/job/{job_id}', self._handle_get_job)
        self.app.router.add_get('/nodes', self._handle_get_nodes)
        self.app.router.add_get('/stats', self._handle_get_stats)

        self.runner = web.AppRunner(self.app)
        await self.runner.setup()

        self.site = web.TCPSite(self.runner, self.host, self.port)
        await self.site.start()

        logger.info(f"HTTP server started on http://{self.host}:{self.port}")

    async def _stop_http_server(self):
        """Stop the HTTP server."""
        if self.site:
            await self.site.stop()
        if self.runner:
            await self.runner.cleanup()

    def _start_background_threads(self):
        """Start background monitoring and coordination threads."""
        self.heartbeat_thread = threading.Thread(
            target=self._heartbeat_monitor_loop,
            name="heartbeat-monitor",
            daemon=True
        )
        self.heartbeat_thread.start()

        self.coordination_thread = threading.Thread(
            target=self._coordination_loop,
            name="coordination",
            daemon=True
        )
        self.coordination_thread.start()

    def _stop_background_threads(self):
        """Stop background threads."""
        # Threads will stop when self.running becomes False
        pass

    def _heartbeat_monitor_loop(self):
        """Monitor node heartbeats and mark dead nodes."""
        while self.running:
            try:
                current_time = time.time()

                with self.node_lock:
                    dead_nodes = []
                    for node_id, node in self.nodes.items():
                        if node_id != self.local_node_id and not node.is_alive:
                            dead_nodes.append(node_id)
                            logger.warning(f"Node {node_id} marked as dead")

                    # Remove dead nodes
                    for node_id in dead_nodes:
                        del self.nodes[node_id]

                time.sleep(5.0)  # Check every 5 seconds

            except Exception as e:
                logger.error(f"Heartbeat monitor error: {e}")
                time.sleep(5.0)

    def _coordination_loop(self):
        """Main coordination loop for job distribution."""
        while self.running:
            try:
                self._coordinate_jobs()
                time.sleep(1.0)  # Coordinate every second

            except Exception as e:
                logger.error(f"Coordination loop error: {e}")
                time.sleep(1.0)

    def _coordinate_jobs(self):
        """Coordinate job distribution across nodes."""
        with self.job_lock:
            # Get pending jobs
            if not self.pending_jobs:
                return

            # Get available nodes sorted by health score
            available_nodes = self._get_available_nodes()
            if not available_nodes:
                return

            # Distribute jobs to nodes
            jobs_assigned = 0
            for job in list(self.pending_jobs):
                if not available_nodes:
                    break

                # Select best node for job
                best_node = self._select_node_for_job(job, available_nodes)
                if best_node:
                    self._assign_job_to_node(job, best_node)
                    self.pending_jobs.remove(job)
                    jobs_assigned += 1

            if jobs_assigned > 0:
                logger.debug(f"Assigned {jobs_assigned} jobs to nodes")

    def _get_available_nodes(self) -> List[NodeInfo]:
        """Get list of available nodes sorted by health score."""
        with self.node_lock:
            available = [node for node in self.nodes.values() if node.is_alive and node.status == "alive"]
            return sorted(available, key=lambda n: n.health_score, reverse=True)

    def _select_node_for_job(self, job: DistributedJob, nodes: List[NodeInfo]) -> Optional[NodeInfo]:
        """Select the best node for a job."""
        if not nodes:
            return None

        # Simple selection: choose node with highest health score
        # In a real implementation, this could consider:
        # - Job requirements vs node capabilities
        # - Data locality
        # - Load balancing
        # - Network proximity

        return nodes[0]

    def _assign_job_to_node(self, job: DistributedJob, node: NodeInfo):
        """Assign a job to a node."""
        job.assigned_node = node.node_id
        job.status = "running"

        # In a real implementation, this would send the job to the node
        # For now, we'll simulate local processing
        logger.info(f"Assigned job {job.job_id} to node {node.node_id}")

    # HTTP Handlers

    async def _handle_health(self, request):
        """Health check endpoint."""
        return web.json_response({
            "status": "healthy",
            "node_id": self.local_node_id,
            "timestamp": time.time()
        })

    async def _handle_register_node(self, request):
        """Handle node registration."""
        data = await request.json()

        node_info = NodeInfo(
            node_id=data['node_id'],
            host=data['host'],
            port=data['port'],
            status="alive",
            capabilities=data.get('capabilities', {}),
            system_resources=data.get('system_resources', {})
        )

        with self.node_lock:
            self.nodes[node_info.node_id] = node_info

        logger.info(f"Registered node {node_info.node_id} at {node_info.host}:{node_info.port}")

        return web.json_response({"status": "registered"})

    async def _handle_heartbeat(self, request):
        """Handle node heartbeat."""
        data = await request.json()
        node_id = data['node_id']

        with self.node_lock:
            if node_id in self.nodes:
                node = self.nodes[node_id]
                node.last_heartbeat = time.time()
                node.system_resources = data.get('system_resources', {})
                node.active_workers = data.get('active_workers', 0)
                node.total_processed = data.get('total_processed', 0)

        return web.json_response({"status": "ok"})

    async def _handle_submit_job(self, request):
        """Handle job submission."""
        data = await request.json()

        job = DistributedJob(
            job_id=str(uuid.uuid4()),
            data=data['data'],
            processor_func=data['processor_func'],  # In real impl, this would be serialized
            priority=data.get('priority', 1)
        )

        with self.job_lock:
            self.jobs[job.job_id] = job
            self.pending_jobs.append(job)

        logger.info(f"Submitted job {job.job_id}")

        return web.json_response({
            "job_id": job.job_id,
            "status": "submitted"
        })

    async def _handle_get_job(self, request):
        """Get job status."""
        job_id = request.match_info['job_id']

        with self.job_lock:
            job = self.jobs.get(job_id)
            if not job:
                return web.json_response({"error": "Job not found"}, status=404)

            return web.json_response({
                "job_id": job.job_id,
                "status": job.status,
                "assigned_node": job.assigned_node,
                "result": job.result,
                "error": job.error,
                "created_at": job.created_at
            })

    async def _handle_get_nodes(self, request):
        """Get cluster node information."""
        with self.node_lock:
            nodes_info = []
            for node in self.nodes.values():
                nodes_info.append({
                    "node_id": node.node_id,
                    "host": node.host,
                    "port": node.port,
                    "status": node.status,
                    "health_score": node.health_score,
                    "capabilities": node.capabilities,
                    "active_workers": node.active_workers,
                    "total_processed": node.total_processed,
                    "system_resources": node.system_resources
                })

            return web.json_response({
                "cluster_name": self.cluster_name,
                "nodes": nodes_info,
                "total_nodes": len(nodes_info),
                "alive_nodes": len([n for n in nodes_info if n["status"] == "alive"])
            })

    async def _handle_get_stats(self, request):
        """Get coordinator statistics."""
        with self.job_lock:
            pending_count = len(self.pending_jobs)
            running_count = len([j for j in self.jobs.values() if j.status == "running"])
            completed_count = len([j for j in self.jobs.values() if j.status == "completed"])
            failed_count = len([j for j in self.jobs.values() if j.status == "failed"])

        with self.node_lock:
            node_count = len(self.nodes)
            alive_count = len([n for n in self.nodes.values() if n.is_alive])

        return web.json_response({
            "cluster_name": self.cluster_name,
            "coordinator_node_id": self.local_node_id,
            "nodes": {
                "total": node_count,
                "alive": alive_count
            },
            "jobs": {
                "total": len(self.jobs),
                "pending": pending_count,
                "running": running_count,
                "completed": completed_count,
                "failed": failed_count
            },
            "uptime": time.time() - psutil.boot_time() if hasattr(psutil, 'boot_time') else 0
        })

    # Public API

    async def submit_job(self, data: Any, processor_func: callable, priority: int = 1) -> str:
        """Submit a job for distributed processing."""
        job = DistributedJob(
            job_id=str(uuid.uuid4()),
            data=data,
            processor_func=processor_func,
            priority=priority
        )

        with self.job_lock:
            self.jobs[job.job_id] = job
            self.pending_jobs.append(job)

        logger.info(f"Submitted job {job.job_id}")
        return job.job_id

    async def get_job_result(self, job_id: str, timeout: float = 30.0) -> Any:
        """Get job result with timeout."""
        start_time = time.time()

        while time.time() - start_time < timeout:
            with self.job_lock:
                job = self.jobs.get(job_id)
                if job:
                    if job.status == "completed":
                        return job.result
                    elif job.status == "failed":
                        raise Exception(f"Job failed: {job.error}")

            await asyncio.sleep(0.1)

        raise TimeoutError(f"Job {job_id} did not complete within {timeout} seconds")

    def get_cluster_stats(self) -> Dict[str, Any]:
        """Get comprehensive cluster statistics."""
        with self.node_lock:
            node_stats = []
            for node in self.nodes.values():
                node_stats.append({
                    "node_id": node.node_id,
                    "status": node.status,
                    "health_score": node.health_score,
                    "active_workers": node.active_workers,
                    "total_processed": node.total_processed,
                    "capabilities": node.capabilities
                })

        with self.job_lock:
            job_stats = {
                "total_jobs": len(self.jobs),
                "pending_jobs": len(self.pending_jobs),
                "completed_jobs": len([j for j in self.jobs.values() if j.status == "completed"]),
                "failed_jobs": len([j for j in self.jobs.values() if j.status == "failed"])
            }

        return {
            "cluster_name": self.cluster_name,
            "coordinator_node_id": self.local_node_id,
            "nodes": node_stats,
            "jobs": job_stats,
            "system_resources": get_system_resource_summary()
        }

class SabotWorkerNode:
    """A worker node that joins a distributed Sabot cluster."""

    def __init__(self,
                 coordinator_host: str,
                 coordinator_port: int,
                 local_host: str = "0.0.0.0",
                 local_port: int = 0):  # 0 = auto-assign
        self.coordinator_host = coordinator_host
        self.coordinator_port = coordinator_port
        self.local_host = local_host
        self.local_port = local_port

        self.node_id = str(uuid.uuid4())
        self.registered = False
        self.running = False

        # Local Sabot instance
        self.local_sabot = None

        # HTTP session for coordinator communication
        self.session: Optional[aiohttp.ClientSession] = None

        # Background tasks
        self.heartbeat_task: Optional[asyncio.Task] = None

        logger.info(f"Initialized Worker Node (node_id={self.node_id})")

    async def start(self):
        """Start the worker node and join the cluster."""
        if self.running:
            return

        self.running = True
        self.session = aiohttp.ClientSession()

        # Register with coordinator
        await self._register_with_coordinator()

        # Start local Sabot instance
        await self._start_local_sabot()

        # Start heartbeat
        self.heartbeat_task = asyncio.create_task(self._heartbeat_loop())

        logger.info(f"Worker node started and joined cluster at {self.coordinator_host}:{self.coordinator_port}")

    async def stop(self):
        """Stop the worker node."""
        if not self.running:
            return

        self.running = False

        # Stop heartbeat
        if self.heartbeat_task:
            self.heartbeat_task.cancel()

        # Stop local Sabot
        if self.local_sabot:
            await self.local_sabot.stop()

        # Close session
        if self.session:
            await self.session.close()

        logger.info("Worker node stopped")

    async def _register_with_coordinator(self):
        """Register this node with the coordinator."""
        coordinator_url = f"http://{self.coordinator_host}:{self.coordinator_port}/register"

        registration_data = {
            "node_id": self.node_id,
            "host": self.local_host,
            "port": self.local_port,
            "capabilities": {
                "cython_available": self._check_cython_available(),
                "gpu_available": self._check_gpu_available(),
                "max_workers": psutil.cpu_count(),
            },
            "system_resources": get_system_resource_summary()
        }

        for attempt in range(5):  # Retry registration
            try:
                async with self.session.post(coordinator_url, json=registration_data) as response:
                    if response.status == 200:
                        self.registered = True
                        logger.info("Successfully registered with coordinator")
                        return
                    else:
                        logger.warning(f"Registration failed: {response.status}")
            except Exception as e:
                logger.warning(f"Registration attempt {attempt + 1} failed: {e}")

            await asyncio.sleep(2 ** attempt)  # Exponential backoff

        raise Exception("Failed to register with coordinator after 5 attempts")

    async def _start_local_sabot(self):
        """Start local Sabot instance."""
        # Import here to avoid circular imports
        from .dbos_cython_parallel import DBOSCythonParallelProcessor

        self.local_sabot = DBOSCythonParallelProcessor(
            max_workers=psutil.cpu_count(),
            morsel_size_kb=64,
            target_utilization=0.8
        )

        await self.local_sabot.start()

    def _check_cython_available(self) -> bool:
        """Check if Cython optimizations are available."""
        try:
            from ._cython import morsel_parallelism
            return True
        except ImportError:
            return False

    def _check_gpu_available(self) -> bool:
        """Check if GPU resources are available."""
        try:
            import cudf
            return True
        except ImportError:
            return False

    async def _heartbeat_loop(self):
        """Send periodic heartbeats to coordinator."""
        while self.running:
            try:
                await self._send_heartbeat()
                await asyncio.sleep(10.0)  # Heartbeat every 10 seconds
            except Exception as e:
                logger.error(f"Heartbeat error: {e}")
                await asyncio.sleep(5.0)

    async def _send_heartbeat(self):
        """Send heartbeat to coordinator."""
        if not self.session or not self.registered:
            return

        heartbeat_url = f"http://{self.coordinator_host}:{self.coordinator_port}/heartbeat"

        # Get local stats
        stats = {}
        if self.local_sabot:
            stats = self.local_sabot.get_stats()

        heartbeat_data = {
            "node_id": self.node_id,
            "system_resources": get_system_resource_summary(),
            "active_workers": stats.get('dbos_controller', {}).get('active_workers', 0),
            "total_processed": stats.get('dbos_controller', {}).get('total_processed', 0)
        }

        try:
            async with self.session.post(heartbeat_url, json=heartbeat_data) as response:
                if response.status != 200:
                    logger.warning(f"Heartbeat failed: {response.status}")
        except Exception as e:
            logger.error(f"Heartbeat send failed: {e}")

# Convenience functions

def create_distributed_coordinator(host: str = "0.0.0.0", port: int = 8080) -> DistributedCoordinator:
    """Create a distributed coordinator."""
    return DistributedCoordinator(host, port)

def create_worker_node(coordinator_host: str, coordinator_port: int) -> SabotWorkerNode:
    """Create a worker node that joins a cluster."""
    return SabotWorkerNode(coordinator_host, coordinator_port)

async def submit_distributed_job(coordinator: DistributedCoordinator,
                               data: Any,
                               processor_func: callable) -> str:
    """Submit a job to the distributed cluster."""
    return await coordinator.submit_job(data, processor_func)

async def get_distributed_job_result(coordinator: DistributedCoordinator,
                                   job_id: str,
                                   timeout: float = 30.0) -> Any:
    """Get result of a distributed job."""
    return await coordinator.get_job_result(job_id, timeout)
