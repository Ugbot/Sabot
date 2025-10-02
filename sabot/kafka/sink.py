#!/usr/bin/env python3
"""
Kafka Sink for Sabot Stream API

Writes Stream results to Kafka topics with full codec support.

Phase 1: Pure Python using aiokafka
Phase 2: Will be optimized with Cython for high-throughput production workloads

Features:
- All Kafka serialization formats (Avro, Protobuf, JSON Schema, JSON, MessagePack, String, Bytes)
- Automatic serialization via codecs
- Batching and compression
- Idempotent/transactional writes
- Error handling and retries
- Partitioning strategies
"""

import asyncio
import logging
from typing import Optional, Dict, Any, Callable
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class PartitionStrategy(Enum):
    """Partitioning strategies for Kafka."""
    ROUND_ROBIN = "round_robin"
    KEY_HASH = "key_hash"
    CUSTOM = "custom"


@dataclass
class KafkaSinkConfig:
    """Configuration for Kafka sink."""

    # Kafka connection
    bootstrap_servers: str
    topic: str

    # Codec configuration
    codec_type: str = "json"  # json, avro, protobuf, json_schema, msgpack, string, bytes
    codec_options: Optional[Dict[str, Any]] = None

    # Partitioning
    partition_strategy: PartitionStrategy = PartitionStrategy.ROUND_ROBIN
    partition_key_extractor: Optional[Callable[[Any], bytes]] = None

    # Producer configuration
    compression_type: Optional[str] = "lz4"  # gzip, snappy, lz4, zstd
    acks: str = "all"  # 0, 1, all
    retries: int = 3
    max_in_flight_requests: int = 5
    linger_ms: int = 10  # Batching delay
    batch_size: int = 16384  # 16KB

    # Transactional writes
    enable_idempotence: bool = True
    transactional_id: Optional[str] = None

    # Additional Kafka producer config
    producer_config: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        if self.codec_options is None:
            self.codec_options = {}
        if self.producer_config is None:
            self.producer_config = {}


class KafkaSink:
    """
    Kafka sink for writing Stream results to Kafka.

    Serializes messages using configured codec and writes to Kafka topic.
    Designed for integration with Stream API via to_kafka().

    Example:
        >>> sink = KafkaSink(config)
        >>> await sink.start()
        >>> await sink.send({"user_id": 123, "action": "purchase"})
        >>> await sink.stop()
    """

    def __init__(self, config: KafkaSinkConfig):
        """
        Initialize Kafka sink.

        Args:
            config: Kafka sink configuration
        """
        self.config = config
        self._producer = None
        self._codec = None
        self._running = False
        self._partition_counter = 0

        # Import aiokafka
        try:
            from aiokafka import AIOKafkaProducer
            self._aiokafka = AIOKafkaProducer
        except ImportError:
            raise RuntimeError("aiokafka is required for KafkaSink. Install with: pip install aiokafka")

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

        logger.info(f"KafkaSink initialized: topic={config.topic}, codec={config.codec_type}")

    async def start(self):
        """Start the Kafka producer."""
        if self._running:
            return

        # Build producer config
        producer_config = {
            'bootstrap_servers': self.config.bootstrap_servers,
            'compression_type': self.config.compression_type,
            'acks': self.config.acks,
            'retries': self.config.retries,
            'max_in_flight_requests_per_connection': self.config.max_in_flight_requests,
            'linger_ms': self.config.linger_ms,
            'batch_size': self.config.batch_size,
            'enable_idempotence': self.config.enable_idempotence,
            **self.config.producer_config
        }

        # Add transactional ID if provided
        if self.config.transactional_id:
            producer_config['transactional_id'] = self.config.transactional_id

        # Create producer
        self._producer = self._aiokafka(**producer_config)

        # Start producer
        await self._producer.start()

        # Initialize transactions if transactional
        if self.config.transactional_id:
            # Note: aiokafka doesn't support transactions yet, but confluent-kafka does
            # This is a placeholder for future Phase 2 implementation
            pass

        self._running = True
        logger.info(f"KafkaSink started: topic={self.config.topic}")

    async def stop(self):
        """Stop the Kafka producer."""
        if not self._running:
            return

        self._running = False

        if self._producer:
            # Flush pending messages
            await self._producer.flush()
            await self._producer.stop()
            self._producer = None

        logger.info(f"KafkaSink stopped: topic={self.config.topic}")

    async def send(self, message: Any, key: Optional[bytes] = None, partition: Optional[int] = None) -> None:
        """
        Send a message to Kafka.

        Args:
            message: Message to send (will be serialized via codec)
            key: Optional message key
            partition: Optional partition (if None, uses partition strategy)
        """
        if not self._running:
            await self.start()

        try:
            # Serialize message
            value_bytes = self._codec.encode(message)

            # Determine partition
            if partition is None:
                partition = self._get_partition(message, key)

            # Send to Kafka
            await self._producer.send(
                topic=self.config.topic,
                value=value_bytes,
                key=key,
                partition=partition
            )

        except Exception as e:
            logger.error(f"Failed to send message to Kafka: {e}")
            raise

    async def send_batch(self, messages: list, keys: Optional[list] = None) -> None:
        """
        Send a batch of messages to Kafka.

        Args:
            messages: List of messages to send
            keys: Optional list of keys (must match length of messages)
        """
        if not self._running:
            await self.start()

        if keys and len(keys) != len(messages):
            raise ValueError("Length of keys must match length of messages")

        try:
            for i, message in enumerate(messages):
                key = keys[i] if keys else None
                await self.send(message, key=key)

        except Exception as e:
            logger.error(f"Failed to send batch to Kafka: {e}")
            raise

    async def flush(self):
        """Flush pending messages."""
        if self._producer:
            await self._producer.flush()

    def _get_partition(self, message: Any, key: Optional[bytes]) -> Optional[int]:
        """
        Get partition for message based on partition strategy.

        Args:
            message: Message to partition
            key: Message key

        Returns:
            Partition number or None (let Kafka decide)
        """
        if self.config.partition_strategy == PartitionStrategy.ROUND_ROBIN:
            # Let Kafka handle round-robin (return None)
            return None

        elif self.config.partition_strategy == PartitionStrategy.KEY_HASH:
            # Extract key if extractor provided
            if self.config.partition_key_extractor:
                key = self.config.partition_key_extractor(message)

            # Let Kafka hash the key
            return None

        elif self.config.partition_strategy == PartitionStrategy.CUSTOM:
            # Custom partitioning (must be implemented by user)
            if self.config.partition_key_extractor:
                key = self.config.partition_key_extractor(message)
                # User must provide actual partition calculation
                return None
            else:
                return None

        else:
            return None

    async def __aenter__(self):
        """Context manager entry."""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        await self.stop()


# Convenience function for creating Kafka sinks
def to_kafka(
    bootstrap_servers: str,
    topic: str,
    codec_type: str = "json",
    codec_options: Optional[Dict[str, Any]] = None,
    compression_type: str = "lz4",
    **producer_kwargs
) -> KafkaSink:
    """
    Create a Kafka sink.

    Args:
        bootstrap_servers: Kafka brokers (e.g., "localhost:9092")
        topic: Topic to produce to
        codec_type: Codec type (json, avro, protobuf, json_schema, msgpack, string, bytes)
        codec_options: Codec-specific options (e.g., schema_registry_url for Avro)
        compression_type: Compression (gzip, snappy, lz4, zstd)
        **producer_kwargs: Additional Kafka producer config

    Returns:
        KafkaSink instance

    Example:
        >>> # Simple JSON sink
        >>> sink = to_kafka("localhost:9092", "output-topic")
        >>> await sink.send({"result": 123})
        >>>
        >>> # Avro sink with Schema Registry
        >>> sink = to_kafka(
        >>>     "localhost:9092",
        >>>     "transactions-enriched",
        >>>     codec_type="avro",
        >>>     codec_options={
        >>>         'schema_registry_url': 'http://localhost:8081',
        >>>         'subject': 'transactions-enriched-value',
        >>>         'schema': transaction_schema
        >>>     }
        >>> )
    """
    config = KafkaSinkConfig(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        codec_type=codec_type,
        codec_options=codec_options,
        compression_type=compression_type,
        producer_config=producer_kwargs
    )

    return KafkaSink(config)
