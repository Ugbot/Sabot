# -*- coding: utf-8 -*-
"""Kafka-backed channel implementation for Sabot."""

import asyncio
import logging
from typing import Any, Optional

from .channels import SerializedChannel
from .types import AppT, ChannelT, Message

logger = logging.getLogger(__name__)


class KafkaChannel(SerializedChannel):
    """Kafka-backed channel using Faust's topic infrastructure."""

    def __init__(
        self,
        app: AppT,
        topic_name: str,
        *,
        partitions: int = 1,
        replication_factor: int = 1,
        retention_hours: Optional[int] = None,
        compression: Optional[str] = None,
        **kwargs
    ):
        # Use the topic as the underlying channel
        # For now, delegate to Faust's topic system
        # In a full implementation, this would create a proper Kafka-backed channel
        self.topic_name = topic_name
        self.partitions = partitions
        self.replication_factor = replication_factor
        self.retention_hours = retention_hours
        self.compression = compression

        # Create the topic through the app
        self._topic = app.topic(
            topic_name,
            partitions=partitions,
            replication_factor=replication_factor,
            retention=retention_hours * 3600 * 1000 if retention_hours else None,  # Convert to ms
            compression=compression,
            **kwargs
        )

        # Initialize as SerializedChannel with topic as the underlying channel
        super().__init__(app, **kwargs)

        # Override the queue to use the topic
        self._queue = None  # Topics handle their own queuing

    @property
    def queue(self):
        """Return topic as the queue interface."""
        return self._topic

    def get_topic_name(self) -> str:
        """Get the topic name."""
        return self.topic_name

    async def send(self, *, key=None, value=None, partition=None, **kwargs):
        """Send message to Kafka topic."""
        return await self._topic.send(key=key, value=value, partition=partition, **kwargs)

    async def deliver(self, message: Message) -> None:
        """Deliver message from Kafka consumer."""
        # Kafka topics handle delivery through their own consumer
        await super().deliver(message)

    def derive(self, **kwargs: Any) -> ChannelT:
        """Derive new channel from this Kafka channel."""
        # For Kafka channels, derivation creates a new topic with modified settings
        return KafkaChannel(
            self.app,
            self.topic_name,
            partitions=self.partitions,
            replication_factor=self.replication_factor,
            retention_hours=self.retention_hours,
            compression=self.compression,
            **{**self._clone_args(), **kwargs}
        )

    def __str__(self) -> str:
        return f"kafka:{self.topic_name}"
