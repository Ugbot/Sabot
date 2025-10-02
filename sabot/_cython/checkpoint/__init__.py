# -*- coding: utf-8 -*-
"""
Cython Checkpoint Coordination

Provides distributed checkpointing with exactly-once semantics
using the Chandy-Lamport algorithm.
"""

__all__ = [
    'CheckpointCoordinator',
    'BarrierTracker',
    'Barrier',
    'CheckpointStorage',
    'RecoveryManager',
    # DBOS-enhanced components
    'DurableCheckpointCoordinator',
    'DBOSDurableStateStore',
]

# Import Cython extensions (will be available after compilation)
try:
    from .coordinator import CheckpointCoordinator
    from .barrier_tracker import BarrierTracker
    from .barrier import Barrier
    from .storage import CheckpointStorage
    from .recovery import RecoveryManager

    # DBOS-enhanced components
    try:
        from .dbos.durable_checkpoint_coordinator import DurableCheckpointCoordinator
        from .dbos.durable_state_store import DBOSDurableStateStore
        DBOS_CHECKPOINT_AVAILABLE = True
    except ImportError:
        DBOS_CHECKPOINT_AVAILABLE = False

        class DurableCheckpointCoordinator:
            """Fallback DurableCheckpointCoordinator interface."""
            pass

        class DBOSDurableStateStore:
            """Fallback DBOSDurableStateStore interface."""
            pass

    CYTHON_CHECKPOINT_AVAILABLE = True

except ImportError:
    CYTHON_CHECKPOINT_AVAILABLE = False
    DBOS_CHECKPOINT_AVAILABLE = False

    # Provide fallback interfaces
    class CheckpointCoordinator:
        """Fallback CheckpointCoordinator interface."""
        pass

    class BarrierTracker:
        """Fallback BarrierTracker interface."""
        pass

    class Barrier:
        """Fallback Barrier interface."""
        pass

    class CheckpointStorage:
        """Fallback CheckpointStorage interface."""
        pass

    class RecoveryManager:
        """Fallback RecoveryManager interface."""
        pass

    class DurableCheckpointCoordinator:
        """Fallback DurableCheckpointCoordinator interface."""
        pass

    class DBOSDurableStateStore:
        """Fallback DBOSDurableStateStore interface."""
        pass
