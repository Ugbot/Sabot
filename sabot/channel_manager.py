# -*- coding: utf-8 -*-
"""Channel Manager - Abstraction layer for channels with multiple backends."""

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Type, Union, Callable
from dataclasses import dataclass
from enum import Enum

from .types import ChannelT, AppT
from .channels import Channel

logger = logging.getLogger(__name__)


class ChannelBackend(Enum):
    """Available channel backend types."""
    MEMORY = "memory"
    KAFKA = "kafka"
    REDIS = "redis"
    FLIGHT = "flight"
    ROCKSDB = "rocksdb"


class ChannelPolicy(Enum):
    """Channel placement and backend selection policies."""
    PERFORMANCE = "performance"  # Lowest latency, highest throughput
    DURABILITY = "durability"    # Persistent storage, survives restarts
    SCALABILITY = "scalability"  # Handles high volume, distributed
    COST = "cost"               # Most cost-effective option
    LOCAL = "local"            # Same process/machine only
    GLOBAL = "global"          # Cross-cluster, network transport


@dataclass
class ChannelConfig:
    """Configuration for channel creation."""
    name: str
    backend: ChannelBackend
    policy: ChannelPolicy
    maxsize: Optional[int] = None
    partitions: int = 1
    replication_factor: int = 1
    retention_hours: Optional[int] = None
    compression: Optional[str] = None
    schema: Optional[Any] = None
    key_type: Optional[Any] = None
    value_type: Optional[Any] = None
    location_hint: Optional[str] = None  # Preferred location/cluster
    durability_level: str = "memory"  # memory, disk, replicated


class ChannelBackendFactory(ABC):
    """Abstract factory for creating channel backends."""

    @abstractmethod
    async def create_channel(self, app: AppT, config: ChannelConfig) -> ChannelT:
        """Create a channel with the specific backend."""
        pass

    @abstractmethod
    def supports_policy(self, policy: ChannelPolicy) -> bool:
        """Check if this backend supports the given policy."""
        pass

    @property
    @abstractmethod
    def backend_type(self) -> ChannelBackend:
        """Return the backend type."""
        pass


class MemoryChannelFactory(ChannelBackendFactory):
    """Factory for in-memory channels with Cython optimization."""

    @property
    def backend_type(self) -> ChannelBackend:
        return ChannelBackend.MEMORY

    def supports_policy(self, policy: ChannelPolicy) -> bool:
        return policy in [ChannelPolicy.PERFORMANCE, ChannelPolicy.LOCAL, ChannelPolicy.COST]

    async def create_channel(self, app: AppT, config: ChannelConfig) -> ChannelT:
        """Create in-memory channel with optional Cython optimization."""
        # Try to use Cython-optimized version first
        try:
            from ._cython.channels import create_fast_channel
            return create_fast_channel(
                app=app,
                maxsize=config.maxsize,
                schema=config.schema,
                key_type=config.key_type,
                value_type=config.value_type,
            )
        except ImportError:
            # Fall back to pure Python implementation
            from .channels import Channel
            return Channel(
                app=app,
                maxsize=config.maxsize,
                schema=config.schema,
                key_type=config.key_type,
                value_type=config.value_type,
            )


class KafkaChannelFactory(ChannelBackendFactory):
    """Factory for Kafka-backed channels."""

    @property
    def backend_type(self) -> ChannelBackend:
        return ChannelBackend.KAFKA

    def supports_policy(self, policy: ChannelPolicy) -> bool:
        return policy in [ChannelPolicy.DURABILITY, ChannelPolicy.SCALABILITY, ChannelPolicy.GLOBAL]

    async def create_channel(self, app: AppT, config: ChannelConfig) -> ChannelT:
        """Create Kafka-backed channel."""
        try:
            from .channels_kafka import KafkaChannel
        except ImportError:
            raise ImportError("Kafka channel backend not available. Install with: pip install sabot[kafka]")

        return KafkaChannel(
            app=app,
            topic_name=config.name,
            partitions=config.partitions,
            replication_factor=config.replication_factor,
            retention_hours=config.retention_hours,
            compression=config.compression,
            schema=config.schema,
            key_type=config.key_type,
            value_type=config.value_type,
            maxsize=config.maxsize,
        )


class RedisChannelFactory(ChannelBackendFactory):
    """Factory for Redis-backed channels."""

    @property
    def backend_type(self) -> ChannelBackend:
        return ChannelBackend.REDIS

    def supports_policy(self, policy: ChannelPolicy) -> bool:
        return policy in [ChannelPolicy.PERFORMANCE, ChannelPolicy.SCALABILITY, ChannelPolicy.COST]

    async def create_channel(self, app: AppT, config: ChannelConfig) -> ChannelT:
        """Create Redis-backed channel."""
        try:
            from .channels_redis import RedisChannel
        except ImportError:
            raise ImportError("Redis channel backend not available. Install with: pip install sabot[redis]")

        return RedisChannel(
            app=app,
            channel_name=config.name,
            maxsize=config.maxsize,
            schema=config.schema,
            key_type=config.key_type,
            value_type=config.value_type,
        )


class FlightChannelFactory(ChannelBackendFactory):
    """Factory for Arrow Flight-backed channels."""

    @property
    def backend_type(self) -> ChannelBackend:
        return ChannelBackend.FLIGHT

    def supports_policy(self, policy: ChannelPolicy) -> bool:
        return policy in [ChannelPolicy.PERFORMANCE, ChannelPolicy.GLOBAL, ChannelPolicy.SCALABILITY]

    async def create_channel(self, app: AppT, config: ChannelConfig) -> ChannelT:
        """Create Arrow Flight-backed channel."""
        try:
            from .channels_flight import FlightChannel
        except ImportError:
            raise ImportError("Flight channel backend not available. Install with: pip install sabot[flight]")

        return FlightChannel(
            app=app,
            location=config.location_hint or "grpc://localhost:8815",
            path=f"/sabot/channels/{config.name}",
            schema=config.schema,
            key_type=config.key_type,
            value_type=config.value_type,
            maxsize=config.maxsize,
        )


class RocksDBChannelFactory(ChannelBackendFactory):
    """Factory for RocksDB-backed channels."""

    @property
    def backend_type(self) -> ChannelBackend:
        return ChannelBackend.ROCKSDB

    def supports_policy(self, policy: ChannelPolicy) -> bool:
        return policy in [ChannelPolicy.DURABILITY, ChannelPolicy.COST]

    async def create_channel(self, app: AppT, config: ChannelConfig) -> ChannelT:
        """Create RocksDB-backed channel."""
        try:
            from .channels_rocksdb import RocksDBChannel
        except ImportError:
            raise ImportError("RocksDB channel backend not available. Install with: pip install sabot[rocksdb]")

        return RocksDBChannel(
            app=app,
            name=config.name,
            path=config.location_hint,
            schema=config.schema,
            key_type=config.key_type,
            value_type=config.value_type,
            maxsize=config.maxsize,
        )


class ChannelManager:
    """Manages channel creation and backend selection using DBOS guidance."""

    def __init__(self, app: AppT):
        self.app = app
        self._backends: Dict[ChannelBackend, ChannelBackendFactory] = {}
        self._policies: Dict[str, ChannelPolicy] = {}
        self._channels: Dict[str, ChannelT] = {}

        # Register default backends
        self.register_backend(MemoryChannelFactory())
        self.register_backend(KafkaChannelFactory())
        self.register_backend(RedisChannelFactory())

        # Try to register optional backends
        try:
            self.register_backend(FlightChannelFactory())
        except ImportError:
            logger.debug("Flight channel backend not available")

        try:
            self.register_backend(RocksDBChannelFactory())
        except ImportError:
            logger.debug("RocksDB channel backend not available")

    def register_backend(self, factory: ChannelBackendFactory) -> None:
        """Register a channel backend factory."""
        self._backends[factory.backend_type] = factory
        logger.info(f"Registered channel backend: {factory.backend_type.value}")

    def set_policy(self, channel_pattern: str, policy: ChannelPolicy) -> None:
        """Set policy for channels matching a pattern."""
        self._policies[channel_pattern] = policy

    async def create_channel(
        self,
        name: str,
        *,
        backend: Optional[ChannelBackend] = None,
        policy: Optional[ChannelPolicy] = None,
        **kwargs: Any
    ) -> ChannelT:
        """Create a channel using DBOS-guided backend selection."""

        # Check if channel already exists
        if name in self._channels:
            return self._channels[name]

        # Determine backend using DBOS logic
        if backend is None:
            backend = await self._select_backend_dbos(name, policy, **kwargs)

        # Get backend factory
        if backend not in self._backends:
            raise ValueError(f"Backend {backend} not available. Registered: {list(self._backends.keys())}")

        factory = self._backends[backend]

        # Create configuration
        config = ChannelConfig(
            name=name,
            backend=backend,
            policy=policy or ChannelPolicy.PERFORMANCE,
            **kwargs
        )

        # Create channel
        channel = await factory.create_channel(self.app, config)
        self._channels[name] = channel

        logger.info(f"Created channel '{name}' with backend '{backend.value}'")
        return channel

    async def _select_backend_dbos(
        self,
        name: str,
        policy: Optional[ChannelPolicy],
        **kwargs: Any
    ) -> ChannelBackend:
        """Use DBOS to select the appropriate backend for a channel."""

        # Get DBOS guidance if available
        dbos_recommendation = await self._get_dbos_recommendation(name, policy, **kwargs)

        if dbos_recommendation:
            return dbos_recommendation

        # Fallback logic based on policies and requirements
        if policy:
            return self._select_backend_by_policy(policy, **kwargs)

        # Default to memory for local development
        if self.app.conf.is_development:
            return ChannelBackend.MEMORY

        # Production defaults based on channel name patterns
        if any(pattern in name for pattern in ['events', 'stream', 'topic']):
            return ChannelBackend.KAFKA
        elif any(pattern in name for pattern in ['cache', 'temp', 'session']):
            return ChannelBackend.REDIS
        else:
            return ChannelBackend.MEMORY

    async def _get_dbos_recommendation(
        self,
        name: str,
        policy: Optional[ChannelPolicy],
        **kwargs: Any
    ) -> Optional[ChannelBackend]:
        """Get backend recommendation from DBOS system."""

        try:
            # Try to get DBOS agent manager
            agent_manager = self.app.agent_manager

            if hasattr(agent_manager, 'recommend_channel_backend'):
                return await agent_manager.recommend_channel_backend(
                    name=name,
                    policy=policy,
                    **kwargs
                )

        except Exception as e:
            logger.debug(f"DBOS recommendation not available: {e}")

        return None

    def _select_backend_by_policy(
        self,
        policy: ChannelPolicy,
        **kwargs: Any
    ) -> ChannelBackend:
        """Select backend based on policy requirements."""

        # Find backends that support this policy
        candidates = [
            backend for backend, factory in self._backends.items()
            if factory.supports_policy(policy)
        ]

        if not candidates:
            raise ValueError(f"No backends support policy: {policy}")

        # Select based on policy preferences
        if policy == ChannelPolicy.PERFORMANCE:
            # Prefer memory, then redis, then kafka
            for pref in [ChannelBackend.MEMORY, ChannelBackend.REDIS, ChannelBackend.KAFKA]:
                if pref in candidates:
                    return pref

        elif policy == ChannelPolicy.DURABILITY:
            # Prefer kafka, then rocksdb
            for pref in [ChannelBackend.KAFKA, ChannelBackend.ROCKSDB]:
                if pref in candidates:
                    return pref

        elif policy == ChannelPolicy.SCALABILITY:
            # Prefer kafka for scalability
            if ChannelBackend.KAFKA in candidates:
                return ChannelBackend.KAFKA

        elif policy == ChannelPolicy.COST:
            # Prefer memory, then rocksdb
            for pref in [ChannelBackend.MEMORY, ChannelBackend.ROCKSDB]:
                if pref in candidates:
                    return pref

        elif policy == ChannelPolicy.LOCAL:
            # Only memory for local
            if ChannelBackend.MEMORY in candidates:
                return ChannelBackend.MEMORY

        elif policy == ChannelPolicy.GLOBAL:
            # Prefer kafka for global distribution
            if ChannelBackend.KAFKA in candidates:
                return ChannelBackend.KAFKA

        # Default to first available
        return candidates[0]

    def get_channel(self, name: str) -> Optional[ChannelT]:
        """Get an existing channel by name."""
        return self._channels.get(name)

    def list_channels(self) -> List[str]:
        """List all created channel names."""
        return list(self._channels.keys())

    def list_backends(self) -> List[ChannelBackend]:
        """List available backends."""
        return list(self._backends.keys())

    async def close_all(self) -> None:
        """Close all channels."""
        for channel in self._channels.values():
            # Channels don't have explicit close methods yet
            # This would be added to the ChannelT interface
            pass
        self._channels.clear()


# Global channel manager instance
_channel_manager: Optional[ChannelManager] = None


def get_channel_manager(app: AppT) -> ChannelManager:
    """Get or create the global channel manager."""
    global _channel_manager
    if _channel_manager is None:
        _channel_manager = ChannelManager(app)
    return _channel_manager
