# -*- coding: utf-8 -*-
"""Composable launcher for Sabot - supports single-node, multi-node, and Kubernetes deployments."""

import asyncio
import logging
import os
import sys
from typing import Optional, Dict, Any
from enum import Enum
import signal

from .distributed_coordinator import DistributedCoordinator, SabotWorkerNode
from .dbos_cython_parallel import DBOSCythonParallelProcessor

logger = logging.getLogger(__name__)

class SabotMode(Enum):
    """Sabot deployment modes."""
    SINGLE_NODE = "single-node"      # Single process, single machine
    COORDINATOR = "coordinator"      # Distributed coordinator
    WORKER = "worker"                # Distributed worker node
    AUTO = "auto"                    # Auto-detect based on environment

class ComposableLauncher:
    """Composable launcher that adapts Sabot to different deployment scenarios.

    Supports:
    - Single-node development/testing
    - Multi-node distributed processing
    - Kubernetes cluster deployments
    - Docker containerized deployments
    """

    def __init__(self):
        self.mode = self._detect_mode()
        self.coordinator = None
        self.worker_node = None
        self.local_processor = None
        self.running = False

        # Configuration from environment
        self.config = self._load_config()

        logger.info(f"Initialized Composable Launcher in {self.mode.value} mode")

    def _detect_mode(self) -> SabotMode:
        """Auto-detect deployment mode from environment."""
        # Check environment variables
        mode_env = os.getenv("SABOT_MODE", "").lower()

        if mode_env == "coordinator":
            return SabotMode.COORDINATOR
        elif mode_env == "worker":
            return SabotMode.WORKER
        elif mode_env == "single-node":
            return SabotMode.SINGLE_NODE

        # Check for Kubernetes
        if os.getenv("KUBERNETES_SERVICE_HOST"):
            # In Kubernetes - check if we're the coordinator or a worker
            pod_name = os.getenv("HOSTNAME", "")
            if "coordinator" in pod_name:
                return SabotMode.COORDINATOR
            else:
                return SabotMode.WORKER

        # Check for distributed coordinator
        coordinator_host = os.getenv("COORDINATOR_HOST")
        if coordinator_host:
            return SabotMode.WORKER

        # Default to single-node
        return SabotMode.SINGLE_NODE

    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from environment variables."""
        return {
            "host": os.getenv("SABOT_HOST", "0.0.0.0"),
            "port": int(os.getenv("SABOT_PORT", "8080")),
            "coordinator_host": os.getenv("COORDINATOR_HOST", "localhost"),
            "coordinator_port": int(os.getenv("COORDINATOR_PORT", "8080")),
            "cluster_name": os.getenv("CLUSTER_NAME", "sabot-cluster"),
            "log_level": os.getenv("LOG_LEVEL", "INFO"),
            "max_workers": int(os.getenv("MAX_WORKERS", "0")),  # 0 = auto-detect
            "morsel_size_kb": int(os.getenv("MORSEL_SIZE_KB", "64")),
            "target_utilization": float(os.getenv("TARGET_UTILIZATION", "0.8")),
        }

    async def start(self):
        """Start Sabot in the appropriate mode."""
        if self.running:
            return

        self.running = True

        # Setup logging
        logging.basicConfig(
            level=getattr(logging, self.config["log_level"]),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

        logger.info(f"Starting Sabot in {self.mode.value} mode")

        try:
            if self.mode == SabotMode.SINGLE_NODE:
                await self._start_single_node()
            elif self.mode == SabotMode.COORDINATOR:
                await self._start_coordinator()
            elif self.mode == SabotMode.WORKER:
                await self._start_worker()
            else:
                raise ValueError(f"Unsupported mode: {self.mode}")

            # Setup signal handlers
            self._setup_signal_handlers()

            logger.info("Sabot started successfully")

            # Keep running
            while self.running:
                await asyncio.sleep(1.0)

        except Exception as e:
            logger.error(f"Failed to start Sabot: {e}")
            raise
        finally:
            await self.stop()

    async def stop(self):
        """Stop Sabot."""
        if not self.running:
            return

        logger.info("Stopping Sabot")
        self.running = False

        # Stop components in reverse order
        if self.worker_node:
            await self.worker_node.stop()
        if self.coordinator:
            await self.coordinator.stop()
        if self.local_processor:
            await self.local_processor.stop()

        logger.info("Sabot stopped")

    async def _start_single_node(self):
        """Start Sabot in single-node mode."""
        logger.info("Starting single-node Sabot")

        self.local_processor = DBOSCythonParallelProcessor(
            max_workers=self.config["max_workers"] or None,
            morsel_size_kb=self.config["morsel_size_kb"],
            target_utilization=self.config["target_utilization"]
        )

        await self.local_processor.start()

        # Expose API on configured port
        await self._start_local_api_server()

    async def _start_coordinator(self):
        """Start Sabot coordinator."""
        logger.info(f"Starting coordinator on {self.config['host']}:{self.config['port']}")

        self.coordinator = DistributedCoordinator(
            host=self.config["host"],
            port=self.config["port"],
            cluster_name=self.config["cluster_name"]
        )

        await self.coordinator.start()

    async def _start_worker(self):
        """Start Sabot worker node."""
        logger.info(f"Starting worker node connecting to {self.config['coordinator_host']}:{self.config['coordinator_port']}")

        self.worker_node = SabotWorkerNode(
            coordinator_host=self.config["coordinator_host"],
            coordinator_port=self.config["coordinator_port"],
            local_host=self.config["host"],
            local_port=self.config["port"]
        )

        await self.worker_node.start()

    async def _start_local_api_server(self):
        """Start local API server for single-node mode."""
        from aiohttp import web
        import json

        async def process_data_handler(request):
            """Handle data processing requests."""
            try:
                data = await request.json()
                input_data = data["data"]

                # Get processor function (simplified - in real impl would be more robust)
                processor_func_name = data.get("processor", "default")
                if processor_func_name == "default":
                    async def default_processor(item):
                        # Simulate CPU-intensive work
                        import math
                        result = 0
                        for i in range(1000):
                            result += item * math.sin(i)
                        await asyncio.sleep(0.001)
                        return result
                    processor_func = default_processor
                else:
                    # Would load actual function
                    processor_func = default_processor

                # Process data
                results = await self.local_processor.process_data(
                    input_data, processor_func
                )

                return web.json_response({
                    "status": "success",
                    "results": results,
                    "processed_count": len(results)
                })

            except Exception as e:
                return web.json_response({
                    "status": "error",
                    "error": str(e)
                }, status=500)

        async def health_handler(request):
            """Health check."""
            stats = self.local_processor.get_stats() if self.local_processor else {}
            return web.json_response({
                "status": "healthy",
                "mode": "single-node",
                "stats": stats
            })

        async def stats_handler(request):
            """Get statistics."""
            if self.local_processor:
                stats = self.local_processor.get_stats()
                return web.json_response(stats)
            else:
                return web.json_response({"error": "No processor available"})

        app = web.Application()
        app.router.add_post('/process', process_data_handler)
        app.router.add_get('/health', health_handler)
        app.router.add_get('/stats', stats_handler)

        runner = web.AppRunner(app)
        await runner.setup()

        site = web.TCPSite(runner, self.config["host"], self.config["port"])
        await site.start()

        logger.info(f"Local API server started on http://{self.config['host']}:{self.config['port']}")

    def _setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown."""
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, shutting down")
            asyncio.create_task(self.stop())

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    # Public API methods

    async def process_data(self, data: Any, processor_func=None):
        """Process data (works in all modes)."""
        if self.mode == SabotMode.SINGLE_NODE:
            if not self.local_processor:
                raise RuntimeError("Single-node processor not started")
            return await self.local_processor.process_data(data, processor_func)
        elif self.mode in (SabotMode.COORDINATOR, SabotMode.WORKER):
            if not self.coordinator:
                raise RuntimeError("Coordinator not available in worker mode")
            job_id = await self.coordinator.submit_job(data, processor_func)
            return await self.coordinator.get_job_result(job_id)
        else:
            raise RuntimeError(f"Processing not supported in {self.mode.value} mode")

    def get_stats(self) -> Dict[str, Any]:
        """Get current statistics."""
        if self.mode == SabotMode.SINGLE_NODE and self.local_processor:
            return self.local_processor.get_stats()
        elif self.mode == SabotMode.COORDINATOR and self.coordinator:
            return self.coordinator.get_cluster_stats()
        elif self.mode == SabotMode.WORKER and self.worker_node:
            return {"mode": "worker", "status": "running"}
        else:
            return {"mode": self.mode.value, "status": "unknown"}

# Convenience functions

def create_composable_launcher() -> ComposableLauncher:
    """Create a composable launcher that auto-detects the deployment mode."""
    return ComposableLauncher()

async def launch_sabot():
    """Launch Sabot with automatic mode detection."""
    launcher = create_composable_launcher()
    await launcher.start()

def main():
    """Main entry point for running Sabot."""
    # Parse command line arguments if needed
    # For now, rely on environment variables

    launcher = create_composable_launcher()
    asyncio.run(launcher.start())

if __name__ == "__main__":
    main()
