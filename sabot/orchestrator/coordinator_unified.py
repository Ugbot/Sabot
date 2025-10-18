#!/usr/bin/env python3
"""
Unified Coordinator

Consolidates JobManager, ClusterCoordinator, and DistributedCoordinator into single coordinator.
Provides both programmatic API and HTTP/REST interface.

Design:
- JobManager logic (job lifecycle, task scheduling) - Core
- ClusterCoordinator features (node management, health) - Merged in
- DistributedCoordinator HTTP API (REST endpoints) - Separate layer

Performance: C++ → Cython → Python layering maintained.
"""

import logging
import asyncio
from typing import Optional, Dict, Any, List

logger = logging.getLogger(__name__)


class UnifiedCoordinator:
    """
    Unified coordinator for all Sabot orchestration.
    
    Consolidates:
    - JobManager: Job lifecycle, task scheduling, DBOS workflows
    - ClusterCoordinator: Node management, slot allocation, health monitoring  
    - DistributedCoordinator: HTTP API, REST endpoints, cluster stats
    
    Provides:
    - Programmatic API (for embedded use)
    - HTTP/REST API (for distributed clusters)
    - Unified resource management
    - Single shuffle service
    
    Example:
        # Programmatic API
        coordinator = UnifiedCoordinator(mode='embedded')
        job_id = await coordinator.submit_job(job_graph)
        
        # HTTP API (separate server)
        api_server = CoordinatorAPIServer(coordinator)
        await api_server.start()
    """
    
    def __init__(
        self,
        mode: str = 'embedded',
        state_backend = None,
        shuffle_service = None,
        config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize unified coordinator.
        
        Args:
            mode: 'embedded' (programmatic) or 'server' (HTTP API)
            state_backend: State backend for durable state
            shuffle_service: Shuffle service instance
            config: Configuration options
        """
        self.mode = mode
        self.state_backend = state_backend
        self.shuffle_service = shuffle_service
        self.config = config or {}
        
        # Core components
        self._jobs: Dict[str, Any] = {}
        self._nodes: Dict[str, Any] = {}
        self._slots: Dict[str, Any] = {}
        
        # Initialize based on mode
        if mode == 'embedded':
            self._init_embedded()
        elif mode == 'server':
            self._init_server()
        else:
            raise ValueError(f"Unknown mode: {mode}. Use 'embedded' or 'server'")
        
        logger.info(f"Initialized UnifiedCoordinator in {mode} mode")
    
    def _init_embedded(self):
        """Initialize for embedded/programmatic use."""
        # Embedded mode uses JobManager logic
        logger.debug("Initializing embedded coordinator (JobManager-style)")
        
        # TODO: Import and wrap JobManager functionality
        # For now, basic structure
        pass
    
    def _init_server(self):
        """Initialize for HTTP server mode."""
        # Server mode uses DistributedCoordinator HTTP API
        logger.debug("Initializing server coordinator (HTTP API)")
        
        # TODO: Import HTTP server components
        pass
    
    # Job Management (from JobManager)
    
    async def submit_job(self, job_graph, job_id: Optional[str] = None) -> str:
        """
        Submit job for execution.
        
        Args:
            job_graph: Job graph definition
            job_id: Optional job ID (auto-generated if None)
            
        Returns:
            Job ID
        """
        # TODO: Delegate to JobManager logic
        import uuid
        job_id = job_id or str(uuid.uuid4())
        
        self._jobs[job_id] = {
            'job_id': job_id,
            'status': 'submitted',
            'graph': job_graph
        }
        
        logger.info(f"Submitted job {job_id}")
        return job_id
    
    async def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """Get job status."""
        if job_id not in self._jobs:
            raise ValueError(f"Job {job_id} not found")
        
        return self._jobs[job_id]
    
    async def cancel_job(self, job_id: str):
        """Cancel running job."""
        if job_id not in self._jobs:
            raise ValueError(f"Job {job_id} not found")
        
        self._jobs[job_id]['status'] = 'cancelled'
        logger.info(f"Cancelled job {job_id}")
    
    # Node Management (from ClusterCoordinator)
    
    async def register_node(self, node_id: str, node_info: Dict[str, Any]):
        """Register cluster node."""
        self._nodes[node_id] = node_info
        logger.info(f"Registered node {node_id}")
    
    async def unregister_node(self, node_id: str):
        """Unregister cluster node."""
        if node_id in self._nodes:
            del self._nodes[node_id]
            logger.info(f"Unregistered node {node_id}")
    
    async def get_cluster_stats(self) -> Dict[str, Any]:
        """Get cluster statistics."""
        return {
            'num_nodes': len(self._nodes),
            'num_jobs': len(self._jobs),
            'active_jobs': sum(1 for j in self._jobs.values() if j['status'] == 'running'),
            'shuffle_service': self.shuffle_service is not None
        }
    
    # Resource Management (from ClusterCoordinator + JobManager)
    
    async def allocate_slot(self, task_id: str, requirements: Dict[str, Any]) -> Optional[str]:
        """Allocate slot for task."""
        # TODO: Implement slot allocation
        slot_id = f"slot_{len(self._slots)}"
        self._slots[slot_id] = {
            'task_id': task_id,
            'requirements': requirements
        }
        logger.debug(f"Allocated slot {slot_id} for task {task_id}")
        return slot_id
    
    async def release_slot(self, slot_id: str):
        """Release allocated slot."""
        if slot_id in self._slots:
            del self._slots[slot_id]
            logger.debug(f"Released slot {slot_id}")
    
    # Shuffle Integration
    
    def get_shuffle_service(self):
        """Get shuffle service instance."""
        return self.shuffle_service
    
    # Cleanup
    
    async def shutdown(self):
        """Shutdown coordinator and cleanup resources."""
        logger.info(f"Shutting down coordinator ({len(self._jobs)} jobs, {len(self._nodes)} nodes)")
        
        # Cancel all running jobs
        for job_id in list(self._jobs.keys()):
            if self._jobs[job_id]['status'] == 'running':
                await self.cancel_job(job_id)
        
        # Shutdown shuffle service
        if self.shuffle_service:
            self.shuffle_service.shutdown()
        
        logger.info("Coordinator shutdown complete")


class CoordinatorAPIServer:
    """
    HTTP/REST API server for UnifiedCoordinator.
    
    Provides REST endpoints for:
    - Job submission (/jobs/submit)
    - Job status (/jobs/{job_id})
    - Cluster stats (/cluster/stats)
    - Node registration (/nodes/register)
    
    Based on DistributedCoordinator HTTP API.
    
    Example:
        coordinator = UnifiedCoordinator(mode='embedded')
        api_server = CoordinatorAPIServer(coordinator, host='0.0.0.0', port=8080)
        await api_server.start()
    """
    
    def __init__(
        self,
        coordinator: UnifiedCoordinator,
        host: str = '0.0.0.0',
        port: int = 8080
    ):
        """
        Initialize API server.
        
        Args:
            coordinator: UnifiedCoordinator instance
            host: Server host
            port: Server port
        """
        self.coordinator = coordinator
        self.host = host
        self.port = port
        self._app = None
        self._runner = None
        self._site = None
    
    async def start(self):
        """Start HTTP API server."""
        try:
            from aiohttp import web
            
            self._app = web.Application()
            
            # Register routes
            self._register_routes()
            
            # Start server
            self._runner = web.AppRunner(self._app)
            await self._runner.setup()
            
            self._site = web.TCPSite(self._runner, self.host, self.port)
            await self._site.start()
            
            logger.info(f"Coordinator API server started on {self.host}:{self.port}")
            
        except ImportError as e:
            logger.error(f"Failed to start API server (aiohttp not available): {e}")
            raise
    
    def _register_routes(self):
        """Register HTTP routes."""
        # Job management
        self._app.router.add_post('/jobs/submit', self._handle_submit_job)
        self._app.router.add_get('/jobs/{job_id}', self._handle_get_job)
        self._app.router.add_post('/jobs/{job_id}/cancel', self._handle_cancel_job)
        
        # Cluster management
        self._app.router.add_get('/cluster/stats', self._handle_cluster_stats)
        self._app.router.add_post('/nodes/register', self._handle_register_node)
        
        # Health check
        self._app.router.add_get('/health', self._handle_health)
    
    async def _handle_submit_job(self, request):
        """Handle job submission."""
        from aiohttp import web
        
        try:
            data = await request.json()
            job_id = await self.coordinator.submit_job(data.get('job_graph'))
            
            return web.json_response({'job_id': job_id, 'status': 'submitted'})
            
        except Exception as e:
            logger.error(f"Job submission failed: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    async def _handle_get_job(self, request):
        """Handle job status request."""
        from aiohttp import web
        
        job_id = request.match_info['job_id']
        
        try:
            status = await self.coordinator.get_job_status(job_id)
            return web.json_response(status)
            
        except ValueError as e:
            return web.json_response({'error': str(e)}, status=404)
        except Exception as e:
            return web.json_response({'error': str(e)}, status=500)
    
    async def _handle_cancel_job(self, request):
        """Handle job cancellation."""
        from aiohttp import web
        
        job_id = request.match_info['job_id']
        
        try:
            await self.coordinator.cancel_job(job_id)
            return web.json_response({'status': 'cancelled'})
            
        except Exception as e:
            return web.json_response({'error': str(e)}, status=500)
    
    async def _handle_cluster_stats(self, request):
        """Handle cluster stats request."""
        from aiohttp import web
        
        try:
            stats = await self.coordinator.get_cluster_stats()
            return web.json_response(stats)
            
        except Exception as e:
            return web.json_response({'error': str(e)}, status=500)
    
    async def _handle_register_node(self, request):
        """Handle node registration."""
        from aiohttp import web
        
        try:
            data = await request.json()
            await self.coordinator.register_node(data['node_id'], data)
            return web.json_response({'status': 'registered'})
            
        except Exception as e:
            return web.json_response({'error': str(e)}, status=500)
    
    async def _handle_health(self, request):
        """Handle health check."""
        from aiohttp import web
        
        return web.json_response({
            'status': 'healthy',
            'mode': self.coordinator.mode,
            'nodes': len(self.coordinator._nodes),
            'jobs': len(self.coordinator._jobs)
        })
    
    async def stop(self):
        """Stop API server."""
        if self._site:
            await self._site.stop()
        
        if self._runner:
            await self._runner.cleanup()
        
        logger.info("API server stopped")


# Convenience functions

def create_unified_coordinator(
    mode: str = 'embedded',
    state_backend=None,
    shuffle_service=None,
    config: Optional[Dict[str, Any]] = None
) -> UnifiedCoordinator:
    """
    Create unified coordinator.
    
    Args:
        mode: 'embedded' or 'server'
        state_backend: State backend instance
        shuffle_service: Shuffle service instance
        config: Configuration
        
    Returns:
        UnifiedCoordinator
    """
    return UnifiedCoordinator(mode, state_backend, shuffle_service, config)


async def create_coordinator_with_api(
    host: str = '0.0.0.0',
    port: int = 8080,
    state_backend=None,
    shuffle_service=None,
    config: Optional[Dict[str, Any]] = None
) -> tuple:
    """
    Create coordinator with HTTP API server.
    
    Args:
        host: API server host
        port: API server port
        state_backend: State backend
        shuffle_service: Shuffle service
        config: Configuration
        
    Returns:
        (coordinator, api_server) tuple
    """
    coordinator = UnifiedCoordinator('embedded', state_backend, shuffle_service, config)
    api_server = CoordinatorAPIServer(coordinator, host, port)
    
    await api_server.start()
    
    return coordinator, api_server

