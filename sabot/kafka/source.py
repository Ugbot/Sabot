#!/usr/bin/env python3
"""
Kafka Source for Sabot Stream API

Reads from Kafka topics into Stream pipelines with full codec support.

Phase 1: Pure Python using aiokafka
Phase 2: Will be optimized with Cython for high-throughput production workloads

Features:
- All Kafka serialization formats (Avro, Protobuf, JSON Schema, JSON, MessagePack, String, Bytes)
- Consumer group management
- Offset management (earliest, latest, committed)
- Automatic deserialization via codecs
- Backpressure handling
- Graceful shutdown
"""

import asyncio
import logging
from typing import Optional, Dict, Any, List, Callable, AsyncGenerator
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class KafkaSourceConfig:
    """Configuration for Kafka source."""

    # Kafka connection
    bootstrap_servers: str
    topic: str
    group_id: str

    # Codec configuration
    codec_type: str = "json"  # json, avro, protobuf, json_schema, msgpack, string, bytes
    codec_options: Optional[Dict[str, Any]] = None

    # Consumer configuration
    auto_offset_reset: str = "latest"  # earliest, latest
    enable_auto_commit: bool = True
    auto_commit_interval_ms: int = 5000
    max_poll_records: int = 500
    fetch_min_bytes: int = 1
    fetch_max_wait_ms: int = 500

    # Additional Kafka consumer config
    consumer_config: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        if self.codec_options is None:
            self.codec_options = {}
        if self.consumer_config is None:
            self.consumer_config = {}


class KafkaSource:
    """
    Kafka source for streaming data into Sabot.

    Reads from Kafka topics and deserializes using configured codec.
    Designed for integration with Stream API via from_kafka().

    Example:
        >>> source = KafkaSource(config)
        >>> async for message in source.stream():
        >>>     print(message)
    """

    def __init__(self, config: KafkaSourceConfig):
        """
        Initialize Kafka source.

        Args:
            config: Kafka source configuration
        """
        self.config = config
        self._consumer = None
        self._codec = None
        self._running = False

        # Import aiokafka
        try:
            from aiokafka import AIOKafkaConsumer
            self._aiokafka = AIOKafkaConsumer
        except ImportError:
            raise RuntimeError("aiokafka is required for KafkaSource. Install with: pip install aiokafka")

        # Create codec
        from ..types.codecs import CodecType, CodecArg, create_codec

        # Map string codec type to CodecType enum
        codec_type_map = {
            'json': CodecType.JSON,
            'msgpack': CodecType.MSGPACK,
            'arrow': CodecType.ARROW,
            'avro': CodecType.AVRO,
            'protobuf': CodecType.PROTOBUF,
            'json_schema': CodecType.JSON_SCHEMA,
            'string': CodecType.STRING,
            'bytes': CodecType.BYTES,
        }

        codec_type = codec_type_map.get(config.codec_type.lower())
        if not codec_type:
            raise ValueError(f"Unsupported codec type: {config.codec_type}")

        codec_arg = CodecArg(codec_type=codec_type, options=config.codec_options)
        self._codec = create_codec(codec_arg)

        logger.info(f"KafkaSource initialized: topic={config.topic}, group={config.group_id}, codec={config.codec_type}")

    async def start(self):
        """Start the Kafka consumer."""
        if self._running:
            return

        # Build consumer config
        consumer_config = {
            'bootstrap_servers': self.config.bootstrap_servers,
            'group_id': self.config.group_id,
            'auto_offset_reset': self.config.auto_offset_reset,
            'enable_auto_commit': self.config.enable_auto_commit,
            'auto_commit_interval_ms': self.config.auto_commit_interval_ms,
            'max_poll_records': self.config.max_poll_records,
            'fetch_min_bytes': self.config.fetch_min_bytes,
            'fetch_max_wait_ms': self.config.fetch_max_wait_ms,
            **self.config.consumer_config
        }

        # Create consumer
        self._consumer = self._aiokafka(**consumer_config)

        # Start consumer
        await self._consumer.start()

        # Subscribe to topic
        self._consumer.subscribe([self.config.topic])

        self._running = True
        logger.info(f"KafkaSource started: topic={self.config.topic}")

    async def stop(self):
        """Stop the Kafka consumer."""
        if not self._running:
            return

        self._running = False

        if self._consumer:
            await self._consumer.stop()
            self._consumer = None

        logger.info(f"KafkaSource stopped: topic={self.config.topic}")

    async def stream(self) -> AsyncGenerator[Any, None]:
        """
        Stream messages from Kafka.

        Yields:
            Deserialized messages (type depends on codec)
        """
        if not self._running:
            await self.start()

        try:
            async for msg in self._consumer:
                try:
                    # Deserialize message value
                    if msg.value is not None:
                        decoded = self._codec.decode(msg.value)
                        yield decoded
                    else:
                        # Null value (tombstone)
                        yield None

                except Exception as e:
                    logger.error(f"Failed to decode message at offset {msg.offset}: {e}")
                    # Skip bad message (TODO: Add error handling policy)
                    continue

        except asyncio.CancelledError:
            logger.info("KafkaSource stream cancelled")
            await self.stop()
            raise

        except Exception as e:
            logger.error(f"KafkaSource stream error: {e}")
            await self.stop()
            raise

    async def stream_with_metadata(self) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Stream messages from Kafka with metadata.

        Yields:
            Dict with:
                - value: Deserialized message
                - key: Message key (bytes)
                - topic: Topic name
                - partition: Partition number
                - offset: Message offset
                - timestamp: Message timestamp
                - headers: Message headers
        """
        if not self._running:
            await self.start()

        try:
            async for msg in self._consumer:
                try:
                    # Deserialize message value
                    decoded_value = self._codec.decode(msg.value) if msg.value is not None else None

                    yield {
                        'value': decoded_value,
                        'key': msg.key,
                        'topic': msg.topic,
                        'partition': msg.partition,
                        'offset': msg.offset,
                        'timestamp': msg.timestamp,
                        'headers': dict(msg.headers) if msg.headers else {}
                    }

                except Exception as e:
                    logger.error(f"Failed to decode message at offset {msg.offset}: {e}")
                    continue

        except asyncio.CancelledError:
            logger.info("KafkaSource stream_with_metadata cancelled")
            await self.stop()
            raise

        except Exception as e:
            logger.error(f"KafkaSource stream_with_metadata error: {e}")
            await self.stop()
            raise

    async def commit(self):
        """Manually commit current offsets."""
        if self._consumer:
            await self._consumer.commit()

    async def seek_to_beginning(self):
        """Seek to the beginning of all assigned partitions."""
        if self._consumer:
            await self._consumer.seek_to_beginning()

    async def seek_to_end(self):
        """Seek to the end of all assigned partitions."""
        if self._consumer:
            await self._consumer.seek_to_end()

    def __aenter__(self):
        """Context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        await self.stop()


# Convenience function for creating Kafka sources
def from_kafka(
    bootstrap_servers: str,
    topic: str,
    group_id: str,
    codec_type: str = "json",
    codec_options: Optional[Dict[str, Any]] = None,
    **consumer_kwargs
) -> KafkaSource:
    """
    Create a Kafka source.

    Args:
        bootstrap_servers: Kafka brokers (e.g., "localhost:9092")
        topic: Topic to consume from
        group_id: Consumer group ID
        codec_type: Codec type (json, avro, protobuf, json_schema, msgpack, string, bytes)
        codec_options: Codec-specific options (e.g., schema_registry_url for Avro)
        **consumer_kwargs: Additional Kafka consumer config

    Returns:
        KafkaSource instance

    Example:
        >>> # Simple JSON source
        >>> source = from_kafka("localhost:9092", "my-topic", "my-group")
        >>>
        >>> # Avro source with Schema Registry
        >>> source = from_kafka(
        >>>     "localhost:9092",
        >>>     "transactions",
        >>>     "fraud-detector",
        >>>     codec_type="avro",
        >>>     codec_options={
        >>>         'schema_registry_url': 'http://localhost:8081',
        >>>         'subject': 'transactions-value'
        >>>     }
        >>> )
    """
    config = KafkaSourceConfig(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        group_id=group_id,
        codec_type=codec_type,
        codec_options=codec_options,
        consumer_config=consumer_kwargs
    )

    return KafkaSource(config)
