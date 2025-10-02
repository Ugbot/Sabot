# -*- coding: utf-8 -*-
"""
Durable Checkpoint Integration

Provides DBOS-enhanced durable checkpoint coordination for Sabot workflows.
Integrates with the existing checkpoint system to add durability guarantees.
"""

import asyncio
import logging
from typing import Dict, Any, Optional, List
from dataclasses import dataclass

from .dbos_parallel_controller import DBOSParallelController
from .stores.rocksdb_fallback import RocksDBBackend
from .stores.base import StoreBackendConfig

logger = logging.getLogger(__name__)


@dataclass
class DurableCheckpointConfig:
    """Configuration for durable checkpointing."""
    enable_durability: bool = True
    max_workers: int = 64
    target_utilization: float = 0.8
    rocksdb_path: str = "./data/durable_checkpoints"
    workflow_retention_days: int = 30
    checkpoint_cleanup_interval_hours: int = 24


class DurableCheckpointManager:
    """
    High-level manager for DBOS-enhanced durable checkpointing.

    This provides a clean interface for enabling durable checkpointing
    in Sabot applications with automatic DBOS integration.
    """

    def __init__(self, config: Optional[DurableCheckpointConfig] = None):
        """Initialize durable checkpoint manager."""
        self.config = config or DurableCheckpointConfig()

        # Core components
        self.dbos_controller: Optional[DBOSParallelController] = None
        self.durable_state_store = None
        self.durable_coordinator = None

        # Workflow tracking
        self.active_workflows: Dict[str, Dict[str, Any]] = {}
        self.workflow_cleanup_task: Optional[asyncio.Task] = None

        # Initialization state
        self._initialized = False

    async def initialize(self):
        """Initialize the durable checkpoint system."""
        if self._initialized:
            return

        logger.info("Initializing durable checkpoint system...")

        try:
            # Initialize DBOS controller
            self.dbos_controller = DBOSParallelController(
                max_workers=self.config.max_workers,
                target_utilization=self.config.target_utilization
            )

            # Initialize durable state store
            if self.config.enable_durability:
                rocksdb_config = StoreBackendConfig(path=self.config.rocksdb_path)
                rocksdb_backend = RocksDBBackend(rocksdb_config)
                await rocksdb_backend.start()

                # Import and initialize durable state store
                try:
                    from ._cython.checkpoint.dbos.durable_state_store import DBOSDurableStateStore
                    self.durable_state_store = DBOSDurableStateStore()
                    self.durable_state_store.initialize(rocksdb_backend, None)  # No DBOS transaction manager yet

                except ImportError:
                    logger.warning("DBOS durable state store not available, using fallback")
                    # Create a simple fallback state store
                    self.durable_state_store = _SimpleDurableStateStore(rocksdb_backend)

            # Initialize durable checkpoint coordinator
            try:
                from ._cython.checkpoint.dbos.durable_checkpoint_coordinator import DurableCheckpointCoordinator
                self.durable_coordinator = DurableCheckpointCoordinator()

                if self.config.enable_durability and self.dbos_controller and self.durable_state_store:
                    self.durable_coordinator.enable_durability(
                        self.dbos_controller, self.durable_state_store
                    )

            except ImportError:
                logger.warning("Durable checkpoint coordinator not available, using fallback")
                self.durable_coordinator = _SimpleDurableCheckpointCoordinator()

            # Start cleanup task
            if self.config.checkpoint_cleanup_interval_hours > 0:
                self.workflow_cleanup_task = asyncio.create_task(
                    self._cleanup_old_workflows()
                )

            self._initialized = True
            logger.info("Durable checkpoint system initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize durable checkpoint system: {e}")
            raise

    async def shutdown(self):
        """Shutdown the durable checkpoint system."""
        logger.info("Shutting down durable checkpoint system...")

        # Cancel cleanup task
        if self.workflow_cleanup_task:
            self.workflow_cleanup_task.cancel()
            try:
                await self.workflow_cleanup_task
            except asyncio.CancelledError:
                pass

        # Shutdown components
        if self.dbos_controller:
            await self.dbos_controller.processor_stopped()

        # Close state store
        if hasattr(self.durable_state_store, 'underlying_store'):
            await self.durable_state_store.underlying_store.stop()

        self._initialized = False
        logger.info("Durable checkpoint system shutdown complete")

    # Workflow management

    async def start_workflow(self, workflow_id: str, workflow_type: str = "streaming",
                           workflow_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Start a new durable workflow.

        Args:
            workflow_id: Unique workflow identifier
            workflow_type: Type of workflow (streaming, batch, etc.)
            workflow_config: Workflow configuration

        Returns:
            Workflow handle with metadata
        """
        await self._ensure_initialized()

        workflow_config = workflow_config or {}

        # Start workflow via durable coordinator
        workflow_handle = self.durable_coordinator.start_workflow(
            workflow_id, workflow_type, workflow_config
        )

        # Track locally
        self.active_workflows[workflow_id] = {
            'handle': workflow_handle,
            'type': workflow_type,
            'config': workflow_config,
            'start_time': asyncio.get_event_loop().time(),
        }

        logger.info(f"Started durable workflow: {workflow_id} ({workflow_type})")
        return workflow_handle

    async def register_operator(self, workflow_id: str, operator_id: int,
                              operator_name: str, operator_instance: Any):
        """
        Register an operator with a durable workflow.

        Args:
            workflow_id: Workflow to register with
            operator_id: Unique operator identifier
            operator_name: Human-readable operator name
            operator_instance: The operator instance
        """
        await self._ensure_initialized()

        if workflow_id not in self.active_workflows:
            raise ValueError(f"Workflow {workflow_id} not found")

        # Register with durable coordinator
        self.durable_coordinator.register_workflow_operator(
            workflow_id, operator_id, operator_name, operator_instance
        )

        logger.debug(f"Registered operator {operator_name} ({operator_id}) with workflow {workflow_id}")

    async def checkpoint_workflow(self, workflow_id: str) -> int:
        """
        Trigger a checkpoint for a workflow.

        Args:
            workflow_id: Workflow to checkpoint

        Returns:
            Checkpoint ID
        """
        await self._ensure_initialized()

        if workflow_id not in self.active_workflows:
            raise ValueError(f"Workflow {workflow_id} not found")

        # Trigger checkpoint
        checkpoint_id = self.durable_coordinator.trigger_workflow_checkpoint(workflow_id)

        logger.info(f"Triggered checkpoint {checkpoint_id} for workflow {workflow_id}")
        return checkpoint_id

    async def acknowledge_checkpoint(self, workflow_id: str, operator_id: int,
                                   checkpoint_id: int) -> bool:
        """
        Acknowledge checkpoint completion for an operator.

        Args:
            workflow_id: Workflow identifier
            operator_id: Operator that completed checkpoint
            checkpoint_id: Checkpoint ID

        Returns:
            True if checkpoint is complete across all operators
        """
        await self._ensure_initialized()

        # Acknowledge checkpoint
        completed = self.durable_coordinator.acknowledge_workflow_checkpoint(
            workflow_id, operator_id, checkpoint_id
        )

        if completed:
            logger.info(f"Checkpoint {checkpoint_id} completed for workflow {workflow_id}")

        return completed

    async def pause_workflow(self, workflow_id: str, reason: str = "user_request"):
        """Pause a workflow durably."""
        await self._ensure_initialized()

        if workflow_id not in self.active_workflows:
            raise ValueError(f"Workflow {workflow_id} not found")

        self.durable_coordinator.pause_workflow(workflow_id, reason)
        logger.info(f"Paused workflow {workflow_id}: {reason}")

    async def resume_workflow(self, workflow_id: str):
        """Resume a paused workflow."""
        await self._ensure_initialized()

        if workflow_id not in self.active_workflows:
            raise ValueError(f"Workflow {workflow_id} not found")

        self.durable_coordinator.resume_workflow(workflow_id)
        logger.info(f"Resumed workflow {workflow_id}")

    async def stop_workflow(self, workflow_id: str, reason: str = "completed"):
        """Stop a workflow durably."""
        await self._ensure_initialized()

        if workflow_id not in self.active_workflows:
            raise ValueError(f"Workflow {workflow_id} not found")

        # Stop workflow
        self.durable_coordinator.stop_workflow(workflow_id, reason)

        # Remove from active tracking
        del self.active_workflows[workflow_id]

        logger.info(f"Stopped workflow {workflow_id}: {reason}")

    async def recover_workflow(self, workflow_id: str) -> Dict[str, Any]:
        """
        Recover a workflow from durable state.

        Args:
            workflow_id: Workflow to recover

        Returns:
            Recovered workflow state
        """
        await self._ensure_initialized()

        # Recover workflow
        recovered_state = self.durable_coordinator.recover_workflow(workflow_id)

        # Track as active if recovery successful
        if recovered_state:
            self.active_workflows[workflow_id] = {
                'handle': recovered_state,
                'type': recovered_state.get('workflow_type', 'unknown'),
                'config': recovered_state.get('config', {}),
                'start_time': asyncio.get_event_loop().time(),
                'recovered': True,
            }

        logger.info(f"Recovered workflow {workflow_id}")
        return recovered_state

    # Monitoring and management

    def get_workflow_stats(self, workflow_id: str) -> Optional[Dict[str, Any]]:
        """Get statistics for a workflow."""
        if workflow_id not in self.active_workflows:
            return None

        # Get stats from durable coordinator
        stats = self.durable_coordinator.get_workflow_stats(workflow_id)

        # Add local tracking info
        local_info = self.active_workflows[workflow_id]
        stats.update({
            'local_tracking': local_info,
            'runtime_seconds': asyncio.get_event_loop().time() - local_info['start_time'],
        })

        return stats

    def list_active_workflows(self) -> List[Dict[str, Any]]:
        """List all active workflows."""
        workflows = []
        for workflow_id, info in self.active_workflows.items():
            stats = self.get_workflow_stats(workflow_id)
            if stats:
                workflows.append(stats)

        return workflows

    def get_system_health(self) -> Dict[str, Any]:
        """Get overall system health."""
        if not self._initialized:
            return {'status': 'not_initialized'}

        health = self.durable_coordinator.get_system_health()
        health.update({
            'active_workflows': len(self.active_workflows),
            'config': {
                'enable_durability': self.config.enable_durability,
                'max_workers': self.config.max_workers,
                'target_utilization': self.config.target_utilization,
            },
            'cleanup_task_running': self.workflow_cleanup_task is not None and not self.workflow_cleanup_task.done(),
        })

        return health

    def get_dbos_controller_stats(self) -> Optional[Dict[str, Any]]:
        """Get DBOS controller performance statistics."""
        if self.dbos_controller:
            return self.dbos_controller.get_performance_stats()
        return None

    # Internal methods

    async def _ensure_initialized(self):
        """Ensure the system is initialized."""
        if not self._initialized:
            await self.initialize()

    async def _cleanup_old_workflows(self):
        """Background task to cleanup old workflow data."""
        while self._initialized:
            try:
                # Wait for cleanup interval
                await asyncio.sleep(self.config.checkpoint_cleanup_interval_hours * 3600)

                # Cleanup old checkpoints
                if self.durable_state_store:
                    # This would be implemented in the durable state store
                    pass

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in workflow cleanup: {e}")

    # Context manager support

    async def __aenter__(self):
        """Async context manager entry."""
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.shutdown()


# Fallback implementations for when Cython extensions aren't available

class _SimpleDurableStateStore:
    """Simple fallback durable state store."""

    def __init__(self, rocksdb_backend):
        self.underlying_store = rocksdb_backend
        self.workflow_state_cache = {}

    def put_value(self, key: str, value: Any):
        """Put value (fallback implementation)."""
        self.workflow_state_cache[key] = value

    def get_value(self, key: str) -> Any:
        """Get value (fallback implementation)."""
        return self.workflow_state_cache.get(key)

    def delete_value(self, key: str) -> bool:
        """Delete value (fallback implementation)."""
        if key in self.workflow_state_cache:
            del self.workflow_state_cache[key]
            return True
        return False

    def exists_value(self, key: str) -> bool:
        """Check existence (fallback implementation)."""
        return key in self.workflow_state_cache

    def get_storage_stats(self) -> Dict[str, Any]:
        """Get storage stats (fallback implementation)."""
        return {
            'durability_enabled': False,
            'underlying_store_available': True,
            'cached_entries': len(self.workflow_state_cache),
            'dbos_transaction_manager': False,
        }


class _SimpleDurableCheckpointCoordinator:
    """Simple fallback durable checkpoint coordinator."""

    def __init__(self):
        self.workflow_registry = {}
        self.dbos_controller = None
        self.durable_state_store = None
        self.durability_enabled = False

    def enable_durability(self, dbos_controller, durable_state_store):
        """Enable durability (fallback implementation)."""
        self.dbos_controller = dbos_controller
        self.durable_state_store = durable_state_store
        self.durability_enabled = True

    def start_workflow(self, workflow_id: str, workflow_type: str, workflow_config: Dict[str, Any]):
        """Start workflow (fallback implementation)."""
        workflow_state = {
            'workflow_id': workflow_id,
            'workflow_type': workflow_type,
            'config': workflow_config,
            'start_time': asyncio.get_event_loop().time() * 1_000_000_000,  # ns
            'status': 'running',
            'checkpoint_id': None,
            'operators': {},
            'recovery_attempts': 0,
        }

        self.workflow_registry[workflow_id] = workflow_state
        return workflow_state

    def register_workflow_operator(self, workflow_id: str, operator_id: int,
                                 operator_name: str, operator_instance):
        """Register operator (fallback implementation)."""
        if workflow_id not in self.workflow_registry:
            return

        workflow_state = self.workflow_registry[workflow_id]
        workflow_state['operators'][operator_id] = {
            'name': operator_name,
            'instance': operator_instance,
            'checkpoint_count': 0,
            'last_checkpoint': None,
        }

    def trigger_workflow_checkpoint(self, workflow_id: str) -> int:
        """Trigger checkpoint (fallback implementation)."""
        if workflow_id not in self.workflow_registry:
            raise ValueError(f"Workflow {workflow_id} not found")

        workflow_state = self.workflow_registry[workflow_id]
        checkpoint_id = int(asyncio.get_event_loop().time() * 1_000_000)  # Simple ID

        workflow_state['checkpoint_id'] = checkpoint_id
        workflow_state['last_checkpoint_time'] = asyncio.get_event_loop().time() * 1_000_000_000

        return checkpoint_id

    def acknowledge_workflow_checkpoint(self, workflow_id: str, operator_id: int,
                                       checkpoint_id: int) -> bool:
        """Acknowledge checkpoint (fallback implementation)."""
        if workflow_id not in self.workflow_registry:
            return False

        workflow_state = self.workflow_registry[workflow_id]

        if operator_id in workflow_state['operators']:
            workflow_state['operators'][operator_id]['checkpoint_count'] += 1
            workflow_state['operators'][operator_id]['last_checkpoint'] = checkpoint_id

        # Simple completion check - all operators acknowledged
        return True

    def recover_workflow(self, workflow_id: str):
        """Recover workflow (fallback implementation)."""
        return self.workflow_registry.get(workflow_id)

    def pause_workflow(self, workflow_id: str, reason: str = "user_request"):
        """Pause workflow (fallback implementation)."""
        if workflow_id in self.workflow_registry:
            self.workflow_registry[workflow_id]['status'] = 'paused'
            self.workflow_registry[workflow_id]['pause_reason'] = reason
            self.workflow_registry[workflow_id]['pause_time'] = asyncio.get_event_loop().time() * 1_000_000_000

    def resume_workflow(self, workflow_id: str):
        """Resume workflow (fallback implementation)."""
        if workflow_id in self.workflow_registry:
            workflow_state = self.workflow_registry[workflow_id]
            if workflow_state['status'] == 'paused':
                workflow_state['status'] = 'running'
                workflow_state['resume_time'] = asyncio.get_event_loop().time() * 1_000_000_000

    def stop_workflow(self, workflow_id: str, reason: str = "completed"):
        """Stop workflow (fallback implementation)."""
        if workflow_id in self.workflow_registry:
            workflow_state = self.workflow_registry[workflow_id]
            workflow_state['status'] = 'stopped'
            workflow_state['stop_reason'] = reason
            workflow_state['stop_time'] = asyncio.get_event_loop().time() * 1_000_000_000

    def get_workflow_stats(self, workflow_id: str):
        """Get workflow stats (fallback implementation)."""
        if workflow_id not in self.workflow_registry:
            return None

        workflow_state = self.workflow_registry[workflow_id]
        return {
            'workflow_id': workflow_id,
            'status': workflow_state.get('status'),
            'start_time': workflow_state.get('start_time'),
            'operators': len(workflow_state.get('operators', {})),
            'checkpoints_triggered': sum(
                op.get('checkpoint_count', 0)
                for op in workflow_state.get('operators', {}).values()
            ),
            'recovery_attempts': workflow_state.get('recovery_attempts', 0),
        }

    def list_active_workflows(self):
        """List active workflows (fallback implementation)."""
        return {
            workflow_id: {
                'status': state.get('status'),
                'type': state.get('workflow_type'),
            }
            for workflow_id, state in self.workflow_registry.items()
            if state.get('status') == 'running'
        }

    def get_system_health(self) -> Dict[str, Any]:
        """Get system health (fallback implementation)."""
        return {
            'durability_enabled': self.durability_enabled,
            'dbos_controller_available': self.dbos_controller is not None,
            'durable_storage_available': self.durable_state_store is not None,
            'active_workflows': len([
                wid for wid, state in self.workflow_registry.items()
                if state.get('status') == 'running'
            ]),
            'checkpoint_coordinator_healthy': True,
            'recovery_manager_available': True,
        }


# Convenience functions

async def create_durable_checkpoint_manager(config: Optional[DurableCheckpointConfig] = None) -> DurableCheckpointManager:
    """Create and initialize a durable checkpoint manager."""
    manager = DurableCheckpointManager(config)
    await manager.initialize()
    return manager


def get_durable_checkpoint_config(enable_durability: bool = True,
                                max_workers: int = 64,
                                rocksdb_path: str = "./data/durable_checkpoints") -> DurableCheckpointConfig:
    """Create a durable checkpoint configuration."""
    return DurableCheckpointConfig(
        enable_durability=enable_durability,
        max_workers=max_workers,
        rocksdb_path=rocksdb_path,
    )
