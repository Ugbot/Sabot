# -*- coding: utf-8 -*-
"""Base store backend interface for Sabot tables.

This module provides the abstract base class for all storage backends.
For performance-critical operations, use the Cython implementations in _cython/.
"""

import abc
from typing import Any, Dict, List, Optional, Union, Iterator
from dataclasses import dataclass
from pathlib import Path


@dataclass
class StoreBackendConfig:
    """Configuration for store backends."""
    backend_type: str = "memory"
    path: Optional[Path] = None
    max_size: Optional[int] = None
    ttl_seconds: Optional[int] = None
    compression: Optional[str] = None
    options: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        if self.options is None:
            self.options = {}


class StoreBackend(abc.ABC):
    """Abstract base class for pluggable store backends.

    This interface defines the contract that all store backends must implement
    to be used as table storage in Sabot. Backends can range from in-memory
    storage to persistent databases like Tonbo or Arrow files.
    """

    def __init__(self, config: StoreBackendConfig):
        self.config = config

    @abc.abstractmethod
    async def start(self) -> None:
        """Initialize the backend."""
        pass

    @abc.abstractmethod
    async def stop(self) -> None:
        """Clean up resources."""
        pass

    @abc.abstractmethod
    async def get(self, key: Any) -> Optional[Any]:
        """Get a value by key."""
        pass

    @abc.abstractmethod
    async def set(self, key: Any, value: Any) -> None:
        """Set a value by key."""
        pass

    @abc.abstractmethod
    async def delete(self, key: Any) -> bool:
        """Delete a value by key. Returns True if deleted."""
        pass

    @abc.abstractmethod
    async def exists(self, key: Any) -> bool:
        """Check if a key exists."""
        pass

    @abc.abstractmethod
    async def keys(self, prefix: Optional[str] = None) -> List[Any]:
        """Get all keys, optionally filtered by prefix."""
        pass

    @abc.abstractmethod
    async def values(self) -> List[Any]:
        """Get all values."""
        pass

    @abc.abstractmethod
    async def items(self, prefix: Optional[str] = None) -> List[tuple]:
        """Get all key-value pairs, optionally filtered by prefix."""
        pass

    @abc.abstractmethod
    async def clear(self) -> None:
        """Clear all data."""
        pass

    @abc.abstractmethod
    async def size(self) -> int:
        """Get number of items stored."""
        pass

    @abc.abstractmethod
    async def scan(
        self,
        start_key: Optional[Any] = None,
        end_key: Optional[Any] = None,
        prefix: Optional[str] = None,
        limit: Optional[int] = None
    ) -> Iterator[tuple]:
        """Scan/range query over keys."""
        pass

    @abc.abstractmethod
    async def batch_set(self, items: Dict[Any, Any]) -> None:
        """Set multiple key-value pairs in a batch."""
        pass

    @abc.abstractmethod
    async def batch_delete(self, keys: List[Any]) -> int:
        """Delete multiple keys in a batch. Returns number deleted."""
        pass

    @abc.abstractmethod
    async def get_stats(self) -> Dict[str, Any]:
        """Get backend statistics."""
        pass

    @abc.abstractmethod
    async def backup(self, path: Path) -> None:
        """Create a backup at the given path."""
        pass

    @abc.abstractmethod
    async def restore(self, path: Path) -> None:
        """Restore from backup at the given path."""
        pass

    # Optional transactional methods
    async def begin_transaction(self) -> 'Transaction':
        """Begin a transaction (optional)."""
        raise NotImplementedError("Transactions not supported by this backend")

    async def commit_transaction(self, transaction: 'Transaction') -> None:
        """Commit a transaction (optional)."""
        raise NotImplementedError("Transactions not supported by this backend")

    async def rollback_transaction(self, transaction: 'Transaction') -> None:
        """Rollback a transaction (optional)."""
        raise NotImplementedError("Transactions not supported by this backend")


class Transaction(abc.ABC):
    """Abstract transaction interface."""

    @abc.abstractmethod
    async def get(self, key: Any) -> Optional[Any]:
        """Get within transaction."""
        pass

    @abc.abstractmethod
    async def set(self, key: Any, value: Any) -> None:
        """Set within transaction."""
        pass

    @abc.abstractmethod
    async def delete(self, key: Any) -> bool:
        """Delete within transaction."""
        pass


def create_backend(config: StoreBackendConfig) -> StoreBackend:
    """Factory function to create a backend instance."""
    backend_type = config.backend_type.lower()

    if backend_type == "memory":
        from .memory import MemoryBackend
        return MemoryBackend(config)
    elif backend_type == "redis":
        from .redis import RedisBackend
        return RedisBackend(config)
    elif backend_type == "arrow_files":
        from .arrow_files import ArrowFileBackend
        return ArrowFileBackend(config)
    elif backend_type == "tonbo":
        from .tonbo import TonboBackend
        return TonboBackend(config)
    else:
        raise ValueError(f"Unknown backend type: {backend_type}")


def create_backend_from_string(backend_spec: str, **kwargs) -> StoreBackend:
    """Create a backend from a string specification like 'memory://' or 'tonbo://path/to/db'."""
    if "://" in backend_spec:
        backend_type, path = backend_spec.split("://", 1)
        config = StoreBackendConfig(backend_type=backend_type, **kwargs)
        if path:
            config.path = Path(path)
        return create_backend(config)
    else:
        # Just a backend type
        return create_backend(StoreBackendConfig(backend_type=backend_spec, **kwargs))
