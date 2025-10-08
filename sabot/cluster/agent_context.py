"""
Agent Context - Shared State for Agent Process

Each agent process has:
- ONE TaskSlotManager (shared worker pool for all operators)
- ONE ShuffleTransport (Arrow Flight server/client)
- ONE AgentRegistry (cluster membership)

This ensures:
1. All operators on same agent share same worker pool
2. Elastic capacity management at agent level
3. Unified local + network morsel processing
"""

import os
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class AgentContext:
    """
    Singleton context for agent process.

    Provides shared infrastructure:
    - Task slot manager (worker pool)
    - Shuffle transport (Flight RPC)
    - Agent registry (cluster membership)
    """

    _instance: Optional['AgentContext'] = None
    _initialized: bool = False

    def __init__(self):
        """Private constructor - use get_instance()."""
        self.agent_id: Optional[str] = None
        self.task_slot_manager = None
        self.shuffle_transport = None
        self.agent_registry = None

        # Metrics
        self.total_morsels_processed: int = 0
        self.total_bytes_shuffled: int = 0

    @classmethod
    def get_instance(cls) -> 'AgentContext':
        """Get singleton instance."""
        if cls._instance is None:
            cls._instance = AgentContext()
        return cls._instance

    def initialize(
        self,
        agent_id: str,
        num_slots: Optional[int] = None,
        host: Optional[str] = None,
        port: Optional[int] = None
    ):
        """
        Initialize agent with task slots and network transport.

        Args:
            agent_id: Unique agent identifier
            num_slots: Number of task slots (default: CPU count)
            host: Flight server host (default: 0.0.0.0)
            port: Flight server port (default: 8816)
        """
        if self._initialized:
            logger.warning(f"Agent {self.agent_id} already initialized")
            return

        self.agent_id = agent_id

        # Auto-detect parameters
        num_slots = num_slots or os.cpu_count() or 4
        host = host or os.getenv('SABOT_AGENT_HOST', '0.0.0.0')
        port = port or int(os.getenv('SABOT_AGENT_PORT', '8816'))

        logger.info(f"Initializing agent {agent_id} with {num_slots} task slots")

        # Create shared task slot manager
        from sabot._c.task_slot_manager import TaskSlotManager
        self.task_slot_manager = TaskSlotManager(num_slots)

        # Create shuffle transport
        from sabot._cython.shuffle.shuffle_transport import ShuffleTransport
        self.shuffle_transport = ShuffleTransport()

        # Start Flight server
        host_bytes = host.encode('utf-8')
        self.shuffle_transport.start(host_bytes, port)

        # Wire task slot manager to shuffle transport (unified processing)
        self.shuffle_transport.set_task_slot_manager(self.task_slot_manager)

        logger.info(f"Agent {agent_id} Flight server started on {host}:{port}")
        logger.info(f"Agent {agent_id} task slots wired to shuffle transport")

        # Join cluster (if registry available)
        try:
            from sabot.cluster.agent_registry import AgentRegistry
            self.agent_registry = AgentRegistry()
            self.agent_registry.register_agent(agent_id, host, port)
            logger.info(f"Agent {agent_id} registered in cluster")
        except Exception as e:
            logger.warning(f"Agent registry not available: {e}")
            self.agent_registry = None

        self._initialized = True

    def get_slot_stats(self) -> list:
        """
        Get statistics for all task slots.

        Returns:
            List of slot stat dicts
        """
        if self.task_slot_manager is None:
            return []

        return self.task_slot_manager.get_slot_stats()

    def scale_slots(self, target_slots: int):
        """
        Elastically scale task slots.

        Args:
            target_slots: Desired number of slots
        """
        if self.task_slot_manager is None:
            logger.error("Cannot scale: task slot manager not initialized")
            return

        current = self.task_slot_manager.num_slots

        if target_slots > current:
            # Scale up
            delta = target_slots - current
            self.task_slot_manager.add_slots(delta)
            logger.info(f"Scaled UP: {current} → {target_slots} task slots")

        elif target_slots < current:
            # Scale down (graceful)
            delta = current - target_slots
            self.task_slot_manager.remove_slots(delta)
            logger.info(f"Scaled DOWN: {current} → {target_slots} task slots")

    def get_metrics(self) -> dict:
        """
        Get agent-level metrics.

        Returns:
            Dict of metrics
        """
        metrics = {
            'agent_id': self.agent_id,
            'initialized': self._initialized,
            'total_morsels_processed': self.total_morsels_processed,
            'total_bytes_shuffled': self.total_bytes_shuffled,
        }

        if self.task_slot_manager:
            metrics.update({
                'num_slots': self.task_slot_manager.num_slots,
                'available_slots': self.task_slot_manager.available_slots,
                'queue_depth': self.task_slot_manager.queue_depth,
            })

        return metrics

    def shutdown(self):
        """Shutdown agent infrastructure."""
        if not self._initialized:
            return

        logger.info(f"Shutting down agent {self.agent_id}")

        # Shutdown task slots
        if self.task_slot_manager:
            self.task_slot_manager.shutdown()

        # Shutdown shuffle transport
        if self.shuffle_transport:
            self.shuffle_transport.stop()

        # Deregister from cluster
        if self.agent_registry:
            try:
                self.agent_registry.deregister_agent(self.agent_id)
            except:
                pass

        self._initialized = False


# ============================================================================
# Global Accessors
# ============================================================================

def get_or_create_slot_manager(num_slots: Optional[int] = None):
    """
    Get agent's shared task slot manager.

    Auto-initializes if needed.

    Args:
        num_slots: Number of slots (if creating new manager)

    Returns:
        TaskSlotManager instance
    """
    ctx = AgentContext.get_instance()

    if ctx.task_slot_manager is None:
        # Auto-initialize with default agent ID
        agent_id = f"agent_{os.getpid()}"
        ctx.initialize(agent_id=agent_id, num_slots=num_slots)

    return ctx.task_slot_manager


def get_agent_context() -> AgentContext:
    """Get agent context singleton."""
    return AgentContext.get_instance()


def initialize_agent(
    agent_id: str,
    num_slots: Optional[int] = None,
    host: Optional[str] = None,
    port: Optional[int] = None
):
    """
    Initialize agent infrastructure.

    Call this once at agent startup.

    Args:
        agent_id: Unique agent identifier
        num_slots: Number of task slots
        host: Flight server host
        port: Flight server port
    """
    ctx = AgentContext.get_instance()
    ctx.initialize(agent_id, num_slots, host, port)
