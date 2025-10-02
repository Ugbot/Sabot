# -*- coding: utf-8 -*-
"""Redis-backed channel implementation for Sabot."""

import asyncio
import logging
from typing import Any, Optional

from .channels import SerializedChannel
from .types import AppT, ChannelT

logger = logging.getLogger(__name__)


class RedisChannel(SerializedChannel):
    """Redis-backed channel for fast pub/sub messaging."""

    def __init__(
        self,
        app: AppT,
        channel_name: str,
        *,
        maxsize: Optional[int] = None,
        **kwargs
    ):
        self.channel_name = channel_name

        # Try to get Redis client from app
        self._redis = getattr(app, '_redis_client', None)
        if self._redis is None:
            # Fallback: try to create Redis client
            try:
                import redis.asyncio as redis
                redis_url = getattr(app.conf, 'redis_url', 'redis://localhost:6379')
                self._redis = redis.from_url(redis_url)
            except ImportError:
                raise ImportError("Redis channel requires redis-py. Install with: pip install redis")

        super().__init__(app, maxsize=maxsize, **kwargs)

    async def send(self, *, key=None, value=None, **kwargs):
        """Send message to Redis channel."""
        # Serialize the message
        final_key, headers = self.prepare_key(key, self.key_serializer, headers={})
        final_value, headers = self.prepare_value(value, self.value_serializer, headers=headers)

        # Publish to Redis channel
        await self._redis.publish(self.channel_name, final_value)

        # Return mock RecordMetadata
        from .types import RecordMetadata, TP
        return RecordMetadata(
            topic=self.channel_name,
            partition=0,
            topic_partition=TP(self.channel_name, 0),
            offset=-1,
            timestamp=asyncio.get_event_loop().time(),
            timestamp_type=0,
        )

    async def deliver(self, message):
        """Deliver message from Redis subscription."""
        # Redis channels use pub/sub, so delivery happens through subscription
        await super().deliver(message)

    def derive(self, **kwargs: Any) -> ChannelT:
        """Derive new channel from this Redis channel."""
        return RedisChannel(
            self.app,
            self.channel_name,
            maxsize=self.maxsize,
            **{**self._clone_args(), **kwargs}
        )

    def __str__(self) -> str:
        return f"redis:{self.channel_name}"
