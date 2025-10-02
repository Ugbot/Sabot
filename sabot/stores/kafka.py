# -*- coding: utf-8 -*-
"""Kafka store backend for Sabot - bridging jobs/systems and source/sink operations."""

import asyncio
import json
from typing import Any, Dict, List, Optional, Iterator, AsyncIterator
from pathlib import Path

from .base import StoreBackend, StoreBackendConfig, Transaction

# Kafka imports - optional dependency
try:
    from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
    from aiokafka.admin import AIOKafkaAdminClient, NewTopic
    from aiokafka.errors import KafkaError, TopicAlreadyExistsError
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    AIOKafkaProducer = None
    AIOKafkaConsumer = None
    AIOKafkaAdminClient = None
    NewTopic = None
    KafkaError = None
    TopicAlreadyExistsError = None


class KafkaTransaction(Transaction):
    """Kafka transaction implementation - using Kafka's transaction support."""

    def __init__(self, backend: 'KafkaBackend', producer):
        self.backend = backend
        self.producer = producer
        self.messages: List[tuple] = []  # (topic, key, value) tuples

    async def get(self, key: Any) -> Optional[Any]:
        """Get is not directly supported in Kafka - would require external indexing."""
        # Kafka is primarily append-only, not a KV store
        # For KV-like access, we'd need to maintain an external index
        self.backend.logger.warning("get() not supported on Kafka backend - use scan() for topic consumption")
        return None

    async def set(self, key: Any, value: Any) -> None:
        """Queue message for sending within transaction."""
        topic = self.backend._get_topic_name(key)
        key_bytes = self.backend._serialize_key(key)
        value_bytes = self.backend._serialize_value(value)
        self.messages.append((topic, key_bytes, value_bytes))

    async def delete(self, key: Any) -> bool:
        """Queue tombstone message for deletion."""
        topic = self.backend._get_topic_name(key)
        key_bytes = self.backend._serialize_key(key)
        # Send null value as tombstone
        self.messages.append((topic, key_bytes, None))
        return True


class KafkaBackend(StoreBackend):
    """
    Kafka store backend for bridging jobs/systems and source/sink operations.

    Unlike traditional KV stores, Kafka provides:
    - Distributed messaging for job/system bridging
    - Durable event streams as sources/sinks
    - Ordered, partitioned event processing
    - Exactly-once semantics with transactions
    """

    def __init__(self, config: StoreBackendConfig):
        super().__init__(config)
        self.producer: Optional[AIOKafkaProducer] = None
        self.consumer: Optional[AIOKafkaConsumer] = None
        self.admin_client: Optional[AIOKafkaAdminClient] = None
        self._topics_created: set = set()
        self._lock = asyncio.Lock()

        # Configuration
        self.bootstrap_servers = config.options.get('bootstrap_servers', 'localhost:9092')
        self.group_id = config.options.get('group_id', 'sabot-consumer')
        self.topic_prefix = config.options.get('topic_prefix', 'sabot.')
        self.replication_factor = config.options.get('replication_factor', 1)
        self.num_partitions = config.options.get('num_partitions', 1)
        self.auto_offset_reset = config.options.get('auto_offset_reset', 'earliest')
        self.enable_auto_commit = config.options.get('enable_auto_commit', False)

    async def start(self) -> None:
        """Initialize Kafka producer, consumer, and admin client."""
        if not KAFKA_AVAILABLE:
            raise RuntimeError(
                "Kafka backend requires aiokafka. "
                "Install with: pip install aiokafka"
            )

        try:
            # Initialize admin client for topic management
            self.admin_client = AIOKafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers
            )

            # Initialize producer for writing
            self.producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=self._serialize_value,
                key_serializer=self._serialize_key,
                enable_idempotence=True,  # Exactly-once semantics
                transactional_id=f"sabot-{id(self)}" if self.config.options.get('enable_transactions', False) else None
            )

            # Initialize consumer for reading
            self.consumer = AIOKafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                auto_offset_reset=self.auto_offset_reset,
                enable_auto_commit=self.enable_auto_commit,
                value_deserializer=self._deserialize_value,
                key_deserializer=self._deserialize_key
            )

            # Start all clients
            await self.admin_client.start()
            await self.producer.start()
            await self.consumer.start()

            self.logger.info(f"Kafka backend initialized with brokers: {self.bootstrap_servers}")

        except Exception as e:
            await self._cleanup()
            raise RuntimeError(f"Kafka backend initialization failed: {e}")

    async def stop(self) -> None:
        """Clean up Kafka resources."""
        await self._cleanup()

    async def _cleanup(self):
        """Clean up all Kafka clients."""
        async with self._lock:
            if self.consumer:
                await self.consumer.stop()
                self.consumer = None

            if self.producer:
                await self.producer.stop()
                self.producer = None

            if self.admin_client:
                await self.admin_client.close()
                self.admin_client = None

    def _get_topic_name(self, key_or_table: Any) -> str:
        """Generate topic name from key or table name."""
        if isinstance(key_or_table, str) and not key_or_table.startswith(self.topic_prefix):
            return f"{self.topic_prefix}{key_or_table}"
        elif hasattr(key_or_table, '__name__'):
            return f"{self.topic_prefix}{key_or_table.__name__}"
        else:
            return f"{self.topic_prefix}default"

    def _serialize_key(self, key: Any) -> bytes:
        """Serialize key to bytes."""
        if isinstance(key, bytes):
            return key
        elif isinstance(key, str):
            return key.encode('utf-8')
        else:
            return str(key).encode('utf-8')

    def _serialize_value(self, value: Any) -> bytes:
        """Serialize value to bytes (JSON by default)."""
        if isinstance(value, bytes):
            return value
        elif isinstance(value, str):
            return value.encode('utf-8')
        else:
            return json.dumps(value, default=str).encode('utf-8')

    def _deserialize_key(self, key_bytes: bytes) -> Any:
        """Deserialize key from bytes."""
        if key_bytes is None:
            return None
        try:
            return key_bytes.decode('utf-8')
        except UnicodeDecodeError:
            return key_bytes

    def _deserialize_value(self, value_bytes: bytes) -> Any:
        """Deserialize value from bytes."""
        if value_bytes is None:
            return None  # Tombstone
        try:
            decoded = value_bytes.decode('utf-8')
            return json.loads(decoded)
        except (UnicodeDecodeError, json.JSONDecodeError):
            return value_bytes

    async def _ensure_topic_exists(self, topic: str):
        """Ensure topic exists, creating it if necessary."""
        if topic in self._topics_created:
            return

        if not self.admin_client:
            return

        try:
            # Check if topic exists
            topics = await self.admin_client.list_topics()
            if topic not in topics:
                # Create topic
                topic_config = NewTopic(
                    name=topic,
                    num_partitions=self.num_partitions,
                    replication_factor=self.replication_factor
                )
                await self.admin_client.create_topics([topic_config])
                self.logger.info(f"Created Kafka topic: {topic}")

            self._topics_created.add(topic)

        except TopicAlreadyExistsError:
            self._topics_created.add(topic)
        except Exception as e:
            self.logger.warning(f"Could not create topic {topic}: {e}")

    async def get(self, key: Any) -> Optional[Any]:
        """Get is not directly supported - Kafka is append-only."""
        self.logger.warning("get() not supported on Kafka backend - use consume_topic() for reading")
        return None

    async def set(self, key: Any, value: Any) -> None:
        """Send message to Kafka topic."""
        if not self.producer:
            raise RuntimeError("Kafka producer not available")

        topic = self._get_topic_name(key)
        await self._ensure_topic_exists(topic)

        key_bytes = self._serialize_key(key)
        value_bytes = self._serialize_value(value)

        try:
            await self.producer.send_and_wait(
                topic=topic,
                key=key_bytes,
                value=value_bytes
            )
        except Exception as e:
            self.logger.error(f"Failed to send message to Kafka: {e}")
            raise

    async def delete(self, key: Any) -> bool:
        """Send tombstone message (null value) to Kafka."""
        if not self.producer:
            raise RuntimeError("Kafka producer not available")

        topic = self._get_topic_name(key)
        await self._ensure_topic_exists(topic)

        key_bytes = self._serialize_key(key)

        try:
            await self.producer.send_and_wait(
                topic=topic,
                key=key_bytes,
                value=None  # Tombstone
            )
            return True
        except Exception as e:
            self.logger.error(f"Failed to send tombstone to Kafka: {e}")
            return False

    async def exists(self, key: Any) -> bool:
        """Cannot efficiently check existence in Kafka."""
        return False  # Kafka doesn't support efficient existence checks

    async def keys(self, prefix: Optional[str] = None) -> List[Any]:
        """Cannot efficiently get all keys from Kafka."""
        return []  # Kafka doesn't support efficient key listing

    async def values(self) -> List[Any]:
        """Cannot efficiently get all values from Kafka."""
        return []  # Kafka doesn't support efficient value listing

    async def items(self, prefix: Optional[str] = None) -> List[tuple]:
        """Cannot efficiently get all items from Kafka."""
        return []  # Kafka doesn't support efficient item listing

    async def clear(self) -> None:
        """Cannot clear Kafka topics efficiently."""
        pass  # Not applicable for Kafka

    async def size(self) -> int:
        """Cannot efficiently get size from Kafka."""
        return 0  # Not applicable for Kafka

    async def scan(
        self,
        start_key: Optional[Any] = None,
        end_key: Optional[Any] = None,
        prefix: Optional[str] = None,
        limit: Optional[int] = None
    ) -> Iterator[tuple]:
        """Not supported for Kafka - use consume_topic() instead."""
        return iter([])

    async def consume_topic(self, topic: str, timeout_ms: int = 1000) -> AsyncIterator[tuple]:
        """
        Consume messages from a Kafka topic.

        This is the primary way to read from Kafka in Sabot.
        Yields (key, value, metadata) tuples.
        """
        if not self.consumer:
            raise RuntimeError("Kafka consumer not available")

        # Subscribe to topic
        await self.consumer.subscribe([topic])

        try:
            async for message in self.consumer:
                if message:
                    key = self._deserialize_key(message.key)
                    value = self._deserialize_value(message.value)

                    # Skip tombstones
                    if value is None:
                        continue

                    # Create metadata dict
                    metadata = {
                        'topic': message.topic,
                        'partition': message.partition,
                        'offset': message.offset,
                        'timestamp': message.timestamp,
                        'headers': dict(message.headers) if message.headers else {}
                    }

                    yield (key, value, metadata)

                    # Check limit
                    if limit and self.consumer.committed(message.topic_partition):
                        break

        except Exception as e:
            self.logger.error(f"Error consuming from Kafka topic {topic}: {e}")

    async def send_to_topic(self, topic: str, key: Any, value: Any, headers: Dict = None) -> None:
        """
        Send message to specific Kafka topic.

        This provides direct topic-level control for job bridging.
        """
        if not self.producer:
            raise RuntimeError("Kafka producer not available")

        await self._ensure_topic_exists(topic)

        key_bytes = self._serialize_key(key)
        value_bytes = self._serialize_value(value)

        try:
            await self.producer.send_and_wait(
                topic=topic,
                key=key_bytes,
                value=value_bytes,
                headers=headers
            )
        except Exception as e:
            self.logger.error(f"Failed to send to topic {topic}: {e}")
            raise

    async def batch_set(self, items: Dict[Any, Any]) -> None:
        """Send batch of messages to Kafka."""
        if not self.producer:
            raise RuntimeError("Kafka producer not available")

        # Group by topic
        topic_batches = {}
        for key, value in items.items():
            topic = self._get_topic_name(key)
            await self._ensure_topic_exists(topic)

            if topic not in topic_batches:
                topic_batches[topic] = []
            topic_batches[topic].append((key, value))

        # Send batches
        for topic, messages in topic_batches.items():
            try:
                batch = [
                    {
                        'topic': topic,
                        'key': self._serialize_key(key),
                        'value': self._serialize_value(value)
                    }
                    for key, value in messages
                ]

                # Send batch (aiokafka supports batch sending)
                for msg in batch:
                    await self.producer.send_and_wait(**msg)

            except Exception as e:
                self.logger.error(f"Failed to send batch to topic {topic}: {e}")
                raise

    async def batch_delete(self, keys: List[Any]) -> int:
        """Send tombstones for batch deletion."""
        deleted = 0
        for key in keys:
            if await self.delete(key):
                deleted += 1
        return deleted

    async def get_stats(self) -> Dict[str, Any]:
        """Get Kafka backend statistics."""
        stats = {
            'backend_type': 'kafka',
            'bootstrap_servers': self.bootstrap_servers,
            'group_id': self.group_id,
            'topic_prefix': self.topic_prefix,
            'topics_created': list(self._topics_created),
            'kafka_available': KAFKA_AVAILABLE,
        }

        if self.admin_client:
            try:
                # Get cluster info
                cluster_info = await self.admin_client.describe_cluster()
                stats.update({
                    'cluster_id': cluster_info.cluster_id,
                    'controller_id': cluster_info.controller_id,
                    'brokers': len(cluster_info.brokers)
                })
            except Exception as e:
                stats['cluster_error'] = str(e)

        return stats

    async def begin_transaction(self) -> Transaction:
        """Begin Kafka transaction."""
        if not self.producer:
            raise RuntimeError("Kafka producer not available")

        try:
            # Start transaction if supported
            if hasattr(self.producer, 'begin_transaction'):
                await self.producer.begin_transaction()

            return KafkaTransaction(self, self.producer)
        except Exception as e:
            self.logger.error(f"Failed to begin Kafka transaction: {e}")
            raise

    async def commit_transaction(self, transaction: KafkaTransaction) -> None:
        """Commit Kafka transaction."""
        if not self.producer:
            return

        try:
            # Send all queued messages
            for topic, key_bytes, value_bytes in transaction.messages:
                await self.producer.send_and_wait(
                    topic=topic,
                    key=key_bytes,
                    value=value_bytes
                )

            # Commit transaction
            if hasattr(self.producer, 'commit_transaction'):
                await self.producer.commit_transaction()

        except Exception as e:
            self.logger.error(f"Failed to commit Kafka transaction: {e}")
            # Try to abort transaction
            if hasattr(self.producer, 'abort_transaction'):
                await self.producer.abort_transaction()
            raise

    async def rollback_transaction(self, transaction: KafkaTransaction) -> None:
        """Rollback Kafka transaction."""
        if self.producer and hasattr(self.producer, 'abort_transaction'):
            try:
                await self.producer.abort_transaction()
            except Exception as e:
                self.logger.error(f"Failed to abort Kafka transaction: {e}")
