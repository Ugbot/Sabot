# -*- coding: utf-8 -*-
"""Pluggable store backends for Sabot tables."""

from .base import StoreBackend, StoreBackendConfig
from .memory import MemoryBackend
from .redis import RedisBackend

# Import new components
try:
    from .rocksdb import RocksDBBackend
    ROCKSDB_AVAILABLE = True
except ImportError:
    ROCKSDB_AVAILABLE = False
    RocksDBBackend = None

from .checkpoint import CheckpointManager, CheckpointConfig
from .manager import StateStoreManager, StateStoreConfig, StateTable, BackendType, StateIsolation

# Conditional imports for optional dependencies
try:
    from .arrow_files import ArrowFileBackend
    ARROW_AVAILABLE = True
except ImportError:
    ARROW_AVAILABLE = False
    ArrowFileBackend = None

try:
    from .tonbo import TonboBackend
    TONBO_AVAILABLE = True
except ImportError:
    TONBO_AVAILABLE = False
    TonboBackend = None

# Try to import Cython implementations for performance
try:
    from .._cython.stores_base import create_fast_backend, FastStoreBackend as FastStoreBackend
    from .._cython.stores_memory import UltraFastMemoryBackend
    CYTHON_AVAILABLE = True
except ImportError:
    CYTHON_AVAILABLE = False
    FastStoreBackend = None
    create_fast_backend = None
    UltraFastMemoryBackend = None

__all__ = [
    'StoreBackend',
    'StoreBackendConfig',
    'MemoryBackend',
    'RedisBackend',
    'CheckpointManager',
    'CheckpointConfig',
    'StateStoreManager',
    'StateStoreConfig',
    'StateTable',
    'BackendType',
    'StateIsolation',
    'create_backend_auto',
    'CYTHON_AVAILABLE',
    'ARROW_AVAILABLE',
    'TONBO_AVAILABLE',
    'ROCKSDB_AVAILABLE',
]

# Conditionally add optional backends to __all__
if ARROW_AVAILABLE:
    __all__.append('ArrowFileBackend')

if TONBO_AVAILABLE:
    __all__.append('TonboBackend')

if ROCKSDB_AVAILABLE:
    __all__.append('RocksDBBackend')

if CYTHON_AVAILABLE:
    __all__.extend([
        'FastStoreBackend',
        'UltraFastMemoryBackend',
        'create_fast_backend',
    ])


def create_backend_auto(backend_spec: str, use_cython: bool = True, **kwargs):
    """Create a backend with automatic Cython fallback.

    Args:
        backend_spec: Backend specification (e.g., "memory://", "redis://")
        use_cython: Whether to prefer Cython implementations
        **kwargs: Additional backend configuration

    Returns:
        Backend instance (Cython if available and requested, otherwise Python)
    """
    from .base import create_backend_from_string

    # Try Cython first if requested and available
    if use_cython and CYTHON_AVAILABLE:
        try:
            return create_fast_backend(backend_spec, **kwargs)
        except Exception as e:
            # Log and fall back to Python implementation
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Cython backend failed, falling back to Python: {e}")

    # Fall back to Python implementation
    return create_backend_from_string(backend_spec, **kwargs)
