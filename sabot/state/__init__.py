"""
Sabot State Management

Unified state backend interface and implementations.
"""

from .interface import (
    StateBackend,
    BackendType,
    TransactionalStateBackend,
    Transaction,
    DistributedStateBackend
)

from .manager import (
    StateManager,
    create_state_manager
)

__all__ = [
    # Interfaces
    'StateBackend',
    'BackendType',
    'TransactionalStateBackend',
    'Transaction',
    'DistributedStateBackend',
    
    # Manager
    'StateManager',
    'create_state_manager',
]
