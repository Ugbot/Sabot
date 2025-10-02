# -*- coding: utf-8 -*-
"""Redis store backend for Sabot tables using FastRedis."""

import asyncio
import json
import pickle
from typing import Any, Dict, List, Optional, Iterator

from .base import StoreBackend, StoreBackendConfig, Transaction


class RedisTransaction(Transaction):
    """Redis transaction implementation."""

    def __init__(self, backend: 'RedisBackend'):
        self.backend = backend
        self.operations: List[tuple] = []  # (op_type, key, value) tuples

    async def get(self, key: Any) -> Optional[Any]:
        """Get within transaction (simulated)."""
        # For Redis transactions, we can't read uncommitted changes
        # So we just delegate to the backend
        return await self.backend.get(key)

    async def set(self, key: Any, value: Any) -> None:
        """Set within transaction."""
        self.operations.append(('set', key, value))

    async def delete(self, key: Any) -> bool:
        """Delete within transaction."""
        self.operations.append(('delete', key, None))
        return True  # Assume success


class RedisBackend(StoreBackend):
    """Redis store backend using FastRedis.

    Persistent, distributed, and fast. Good for production use.
    """

    def __init__(self, config: StoreBackendConfig):
        super().__init__(config)
        self.redis_client = None
        self.namespace = config.options.get('namespace', 'sabot')
        self.serializer = config.options.get('serializer', 'json')  # 'json' or 'pickle'
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        """Initialize the Redis backend."""
        try:
            from fastredis import HighPerformanceRedis
            self.redis_client = HighPerformanceRedis(
                host=self.config.options.get('host', 'localhost'),
                port=self.config.options.get('port', 6379),
                max_connections=self.config.options.get('max_connections', 10),
                use_uvloop=True
            )
        except ImportError:
            raise RuntimeError("FastRedis not available. Install with: pip install fastredis")

    async def stop(self) -> None:
        """Clean up the Redis backend."""
        if self.redis_client:
            # FastRedis client cleanup if needed
            pass

    def _make_key(self, key: Any) -> str:
        """Create a namespaced Redis key."""
        return f"{self.namespace}:{key}"

    def _serialize(self, value: Any) -> str:
        """Serialize value for Redis storage."""
        if self.serializer == 'json':
            return json.dumps(value, default=str)
        elif self.serializer == 'pickle':
            return pickle.dumps(value).hex()
        else:
            return str(value)

    def _deserialize(self, value: str) -> Any:
        """Deserialize value from Redis storage."""
        if self.serializer == 'json':
            return json.loads(value)
        elif self.serializer == 'pickle':
            return pickle.loads(bytes.fromhex(value))
        else:
            return value

    async def get(self, key: Any) -> Optional[Any]:
        """Get a value by key."""
        if not self.redis_client:
            return None

        async with self._lock:
            redis_key = self._make_key(key)
            try:
                value = await self.redis_client.get(redis_key)
                if value is None:
                    return None
                return self._deserialize(value)
            except Exception as e:
                # Log error but don't crash
                return None

    async def set(self, key: Any, value: Any) -> None:
        """Set a value by key."""
        if not self.redis_client:
            return

        async with self._lock:
            redis_key = self._make_key(key)
            serialized_value = self._serialize(value)

            # Handle TTL if configured
            if self.config.ttl_seconds:
                await self.redis_client.setex(redis_key, self.config.ttl_seconds, serialized_value)
            else:
                await self.redis_client.set(redis_key, serialized_value)

    async def delete(self, key: Any) -> bool:
        """Delete a value by key."""
        if not self.redis_client:
            return False

        async with self._lock:
            redis_key = self._make_key(key)
            try:
                result = await self.redis_client.delete(redis_key)
                return result > 0
            except Exception:
                return False

    async def exists(self, key: Any) -> bool:
        """Check if a key exists."""
        if not self.redis_client:
            return False

        async with self._lock:
            redis_key = self._make_key(key)
            try:
                return await self.redis_client.exists(redis_key)
            except Exception:
                return False

    async def keys(self, prefix: Optional[str] = None) -> List[Any]:
        """Get all keys, optionally filtered by prefix."""
        if not self.redis_client:
            return []

        async with self._lock:
            try:
                pattern = f"{self.namespace}:{prefix or ''}*"
                redis_keys = await self.redis_client.keys(pattern)

                # Extract original keys from namespaced keys
                keys = []
                prefix_len = len(f"{self.namespace}:")
                for redis_key in redis_keys:
                    if redis_key.startswith(f"{self.namespace}:"):
                        original_key = redis_key[prefix_len:]
                        keys.append(original_key)

                return keys
            except Exception:
                return []

    async def values(self) -> List[Any]:
        """Get all values."""
        keys = await self.keys()
        values = []
        for key in keys:
            value = await self.get(key)
            if value is not None:
                values.append(value)
        return values

    async def items(self, prefix: Optional[str] = None) -> List[tuple]:
        """Get all key-value pairs, optionally filtered by prefix."""
        keys = await self.keys(prefix)
        items = []
        for key in keys:
            value = await self.get(key)
            if value is not None:
                items.append((key, value))
        return items

    async def clear(self) -> None:
        """Clear all data."""
        if not self.redis_client:
            return

        async with self._lock:
            try:
                pattern = f"{self.namespace}:*"
                keys = await self.redis_client.keys(pattern)
                if keys:
                    await self.redis_client.delete(*keys)
            except Exception:
                pass

    async def size(self) -> int:
        """Get number of items stored."""
        keys = await self.keys()
        return len(keys)

    async def scan(
        self,
        start_key: Optional[Any] = None,
        end_key: Optional[Any] = None,
        prefix: Optional[str] = None,
        limit: Optional[int] = None
    ) -> Iterator[tuple]:
        """Scan/range query over keys."""
        # Redis doesn't have efficient range queries, so we do basic filtering
        all_items = await self.items(prefix)

        # Sort by key for range queries
        all_items.sort(key=lambda x: x[0])

        results = []
        for key, value in all_items:
            # Apply range filter
            if start_key is not None and key < start_key:
                continue
            if end_key is not None and key >= end_key:
                continue

            results.append((key, value))

            # Apply limit
            if limit and len(results) >= limit:
                break

        return iter(results)

    async def batch_set(self, items: Dict[Any, Any]) -> None:
        """Set multiple key-value pairs in a batch."""
        if not self.redis_client:
            return

        async with self._lock:
            # Use Redis pipeline/mset for efficiency
            redis_items = {}
            for key, value in items.items():
                redis_key = self._make_key(key)
                redis_items[redis_key] = self._serialize(value)

            if redis_items:
                await self.redis_client.mset(redis_items)

    async def batch_delete(self, keys: List[Any]) -> int:
        """Delete multiple keys in a batch."""
        if not self.redis_client:
            return 0

        async with self._lock:
            redis_keys = [self._make_key(key) for key in keys]
            try:
                result = await self.redis_client.delete(*redis_keys)
                return result
            except Exception:
                return 0

    async def get_stats(self) -> Dict[str, Any]:
        """Get backend statistics."""
        size = await self.size()
        return {
            'backend_type': 'redis',
            'size': size,
            'namespace': self.namespace,
            'serializer': self.serializer,
            'host': self.config.options.get('host', 'localhost'),
            'port': self.config.options.get('port', 6379),
            'ttl_seconds': self.config.ttl_seconds,
        }

    async def backup(self, path) -> None:
        """Create a backup (Redis handles persistence internally)."""
        # Redis has its own persistence mechanisms (RDB/AOF)
        # For backup, we could export all data to a file
        raise NotImplementedError("Redis backup not implemented yet")

    async def restore(self, path) -> None:
        """Restore from backup."""
        # Restore all data from a backup file
        raise NotImplementedError("Redis restore not implemented yet")

    async def begin_transaction(self) -> Transaction:
        """Begin a transaction."""
        return RedisTransaction(self)

    async def commit_transaction(self, transaction: RedisTransaction) -> None:
        """Commit a transaction using Redis MULTI/EXEC."""
        if not self.redis_client or not transaction.operations:
            return

        async with self._lock:
            # Use Redis transactions for atomicity
            async with self.redis_client.pipeline() as pipe:
                for op_type, key, value in transaction.operations:
                    redis_key = self._make_key(key)
                    if op_type == 'set':
                        pipe.set(redis_key, self._serialize(value))
                    elif op_type == 'delete':
                        pipe.delete(redis_key)

                await pipe.execute()

    async def rollback_transaction(self, transaction: RedisTransaction) -> None:
        """Rollback a transaction (no-op since operations aren't applied until commit)."""
        # Redis transactions are not applied until EXEC, so rollback is automatic
        pass
