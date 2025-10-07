"""
Feature Store - CyRedis-backed feature storage and retrieval.
"""

import asyncio
import time
from typing import Dict, List, Optional, Any, Union
import json


class FeatureStore:
    """
    High-performance feature store backed by CyRedis.

    Provides fast feature storage and retrieval with TTL support,
    batch operations, and automatic key management.
    """

    def __init__(self, redis_url: str = "localhost:6379", db: int = 0):
        """
        Initialize feature store.

        Args:
            redis_url: Redis connection URL (host:port)
            db: Redis database number
        """
        self.redis_url = redis_url
        self.db = db
        self._client = None
        self._initialized = False

    async def initialize(self) -> None:
        """Initialize async Redis connection via CyRedis."""
        if self._initialized:
            return

        try:
            # Import CyRedis client
            from vendor.cyredis import AsyncRedisClient

            host, port = self.redis_url.split(':')
            self._client = AsyncRedisClient(
                host=host,
                port=int(port),
                db=self.db
            )
            await self._client.connect()
            self._initialized = True
        except ImportError:
            # Fallback to basic redis-py if CyRedis not available
            import redis.asyncio as redis_async
            host, port = self.redis_url.split(':')
            self._client = redis_async.Redis(
                host=host,
                port=int(port),
                db=self.db,
                decode_responses=True
            )
            self._initialized = True

    async def close(self) -> None:
        """Close Redis connection."""
        if self._client and hasattr(self._client, 'close'):
            await self._client.close()
        self._initialized = False

    def _make_key(self, entity_id: str, feature_name: str,
                   timestamp: Optional[int] = None) -> str:
        """
        Create Redis key for a feature.

        Format: feature:{entity_id}:{feature_name}:{timestamp}
        """
        if timestamp is None:
            timestamp = int(time.time())
        return f"feature:{entity_id}:{feature_name}:{timestamp}"

    async def get_feature(self, entity_id: str, feature_name: str,
                          timestamp: Optional[int] = None) -> Optional[float]:
        """
        Get a single feature value.

        Args:
            entity_id: Entity identifier (e.g., "BTC-USD")
            feature_name: Name of the feature
            timestamp: Optional timestamp (defaults to current time)

        Returns:
            Feature value or None if not found
        """
        if not self._initialized:
            await self.initialize()

        key = self._make_key(entity_id, feature_name, timestamp)
        value = await self._client.get(key)

        if value is None:
            return None

        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    async def get_features(self, entity_id: str, feature_names: List[str],
                           timestamp: Optional[int] = None) -> Dict[str, Optional[float]]:
        """
        Get multiple features at once (batch operation).

        Args:
            entity_id: Entity identifier
            feature_names: List of feature names
            timestamp: Optional timestamp

        Returns:
            Dictionary mapping feature names to values
        """
        if not self._initialized:
            await self.initialize()

        # Build keys
        keys = [self._make_key(entity_id, fname, timestamp)
                for fname in feature_names]

        # Batch get
        values = await self._client.mget(keys)

        # Parse results
        result = {}
        for fname, value in zip(feature_names, values):
            if value is not None:
                try:
                    result[fname] = float(value)
                except (ValueError, TypeError):
                    result[fname] = None
            else:
                result[fname] = None

        return result

    async def set_feature(self, entity_id: str, feature_name: str,
                          value: float, ttl: Optional[int] = None,
                          timestamp: Optional[int] = None) -> None:
        """
        Set a single feature value.

        Args:
            entity_id: Entity identifier
            feature_name: Name of the feature
            value: Feature value
            ttl: Time-to-live in seconds (None = no expiration)
            timestamp: Optional timestamp
        """
        if not self._initialized:
            await self.initialize()

        key = self._make_key(entity_id, feature_name, timestamp)

        if ttl is not None:
            await self._client.setex(key, ttl, str(value))
        else:
            await self._client.set(key, str(value))

    async def set_features(self, entity_id: str,
                           features: Dict[str, float],
                           ttl: Optional[int] = None,
                           timestamp: Optional[int] = None) -> None:
        """
        Set multiple features at once (batch operation).

        Args:
            entity_id: Entity identifier
            features: Dictionary mapping feature names to values
            ttl: Time-to-live in seconds
            timestamp: Optional timestamp
        """
        if not self._initialized:
            await self.initialize()

        # Use pipeline for batch writes
        pipe = self._client.pipeline()

        for fname, value in features.items():
            key = self._make_key(entity_id, fname, timestamp)
            if ttl is not None:
                pipe.setex(key, ttl, str(value))
            else:
                pipe.set(key, str(value))

        await pipe.execute()

    async def delete_features(self, entity_id: str, feature_names: List[str],
                              timestamp: Optional[int] = None) -> int:
        """
        Delete multiple features.

        Returns:
            Number of features deleted
        """
        if not self._initialized:
            await self.initialize()

        keys = [self._make_key(entity_id, fname, timestamp)
                for fname in feature_names]
        return await self._client.delete(*keys)

    async def exists(self, entity_id: str, feature_name: str,
                     timestamp: Optional[int] = None) -> bool:
        """Check if a feature exists in the store."""
        if not self._initialized:
            await self.initialize()

        key = self._make_key(entity_id, feature_name, timestamp)
        return bool(await self._client.exists(key))

    async def get_stats(self) -> Dict[str, Any]:
        """Get feature store statistics."""
        if not self._initialized:
            await self.initialize()

        info = await self._client.info()
        return {
            "used_memory": info.get("used_memory_human", "unknown"),
            "total_keys": await self._client.dbsize(),
            "connected_clients": info.get("connected_clients", 0),
        }
