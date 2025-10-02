# -*- coding: utf-8 -*-
"""In-memory store backend for Sabot tables."""

import asyncio
from typing import Any, Dict, List, Optional, Iterator
from collections import OrderedDict

from .base import StoreBackend, StoreBackendConfig, Transaction


class MemoryTransaction(Transaction):
    """In-memory transaction implementation."""

    def __init__(self, backend: 'MemoryBackend'):
        self.backend = backend
        self.changes: Dict[Any, Any] = {}
        self.deletions: set = set()

    async def get(self, key: Any) -> Optional[Any]:
        """Get within transaction."""
        if key in self.deletions:
            return None
        if key in self.changes:
            return self.changes[key]
        return await self.backend.get(key)

    async def set(self, key: Any, value: Any) -> None:
        """Set within transaction."""
        self.changes[key] = value
        if key in self.deletions:
            self.deletions.remove(key)

    async def delete(self, key: Any) -> bool:
        """Delete within transaction."""
        if key in self.backend._data:
            self.deletions.add(key)
            if key in self.changes:
                del self.changes[key]
            return True
        return False


class MemoryBackend(StoreBackend):
    """In-memory store backend using Python dict.

    Fast but not persistent. Good for testing and development.
    """

    def __init__(self, config: StoreBackendConfig):
        super().__init__(config)
        self._data: Dict[Any, Any] = {}
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        """Initialize the memory backend."""
        # Nothing to do for in-memory
        pass

    async def stop(self) -> None:
        """Clean up the memory backend."""
        async with self._lock:
            self._data.clear()

    async def get(self, key: Any) -> Optional[Any]:
        """Get a value by key."""
        async with self._lock:
            return self._data.get(key)

    async def set(self, key: Any, value: Any) -> None:
        """Set a value by key."""
        async with self._lock:
            self._data[key] = value

    async def delete(self, key: Any) -> bool:
        """Delete a value by key."""
        async with self._lock:
            if key in self._data:
                del self._data[key]
                return True
            return False

    async def exists(self, key: Any) -> bool:
        """Check if a key exists."""
        async with self._lock:
            return key in self._data

    async def keys(self, prefix: Optional[str] = None) -> List[Any]:
        """Get all keys, optionally filtered by prefix."""
        async with self._lock:
            keys = list(self._data.keys())
            if prefix is not None:
                keys = [k for k in keys if str(k).startswith(prefix)]
            return keys

    async def values(self) -> List[Any]:
        """Get all values."""
        async with self._lock:
            return list(self._data.values())

    async def items(self, prefix: Optional[str] = None) -> List[tuple]:
        """Get all key-value pairs, optionally filtered by prefix."""
        async with self._lock:
            items = list(self._data.items())
            if prefix is not None:
                items = [(k, v) for k, v in items if str(k).startswith(prefix)]
            return items

    async def clear(self) -> None:
        """Clear all data."""
        async with self._lock:
            self._data.clear()

    async def size(self) -> int:
        """Get number of items stored."""
        async with self._lock:
            return len(self._data)

    async def scan(
        self,
        start_key: Optional[Any] = None,
        end_key: Optional[Any] = None,
        prefix: Optional[str] = None,
        limit: Optional[int] = None
    ) -> Iterator[tuple]:
        """Scan/range query over keys."""
        async with self._lock:
            # Sort keys for range queries
            sorted_keys = sorted(self._data.keys())
            results = []

            for key in sorted_keys:
                # Apply prefix filter
                if prefix and not str(key).startswith(prefix):
                    continue

                # Apply range filter
                if start_key is not None and key < start_key:
                    continue
                if end_key is not None and key >= end_key:
                    continue

                results.append((key, self._data[key]))

                # Apply limit
                if limit and len(results) >= limit:
                    break

            return iter(results)

    async def batch_set(self, items: Dict[Any, Any]) -> None:
        """Set multiple key-value pairs in a batch."""
        async with self._lock:
            self._data.update(items)

    async def batch_delete(self, keys: List[Any]) -> int:
        """Delete multiple keys in a batch."""
        async with self._lock:
            deleted = 0
            for key in keys:
                if key in self._data:
                    del self._data[key]
                    deleted += 1
            return deleted

    async def get_stats(self) -> Dict[str, Any]:
        """Get backend statistics."""
        async with self._lock:
            return {
                'backend_type': 'memory',
                'size': len(self._data),
                'memory_usage_bytes': sum(len(str(k)) + len(str(v)) for k, v in self._data.items()),
                'max_size': self.config.max_size,
                'ttl_seconds': self.config.ttl_seconds,
            }

    async def backup(self, path) -> None:
        """Create a backup (not really applicable for memory)."""
        # Memory backend can't be backed up to disk
        raise NotImplementedError("Memory backend cannot be backed up to disk")

    async def restore(self, path) -> None:
        """Restore from backup (not really applicable for memory)."""
        # Memory backend can't be restored from disk
        raise NotImplementedError("Memory backend cannot be restored from disk")

    async def begin_transaction(self) -> Transaction:
        """Begin a transaction."""
        return MemoryTransaction(self)

    async def commit_transaction(self, transaction: MemoryTransaction) -> None:
        """Commit a transaction."""
        async with self._lock:
            # Apply changes
            for key, value in transaction.changes.items():
                self._data[key] = value

            # Apply deletions
            for key in transaction.deletions:
                self._data.pop(key, None)

    async def rollback_transaction(self, transaction: MemoryTransaction) -> None:
        """Rollback a transaction (no-op for memory)."""
        # Changes are only in the transaction object, so just discard it
        pass
