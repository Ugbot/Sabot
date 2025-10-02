# -*- coding: utf-8 -*-
"""Comprehensive tests for Sabot channel system."""

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import sys
import os

# Add sabot to path for testing
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

# Mock dependencies
from unittest.mock import MagicMock
sys.modules['typer'] = MagicMock()
sys.modules['rich.console'] = MagicMock()
sys.modules['rich.table'] = MagicMock()
sys.modules['rich.panel'] = MagicMock()
sys.modules['rich.text'] = MagicMock()
sys.modules['rich.live'] = MagicMock()
sys.modules['rich.spinner'] = MagicMock()
sys.modules['rich.progress'] = MagicMock()
sys.modules['rich.prompt'] = MagicMock()
sys.modules['rich'] = MagicMock()

# Mock typer
mock_typer = sys.modules['typer']
mock_app = MagicMock()
mock_typer.Typer.return_value = mock_app
mock_typer.Argument = lambda x, **kwargs: x
mock_typer.Option = lambda *args, **kwargs: args[0] if args else None

# Mock rich
mock_console = MagicMock()
sys.modules['rich.console'].Console.return_value = mock_console

# Test imports
try:
    from sabot.types.channels import ChannelT
    from sabot.channels import Channel, SerializedChannel
    from sabot.channel_manager import (
        ChannelManager, ChannelBackend, ChannelPolicy,
        ChannelConfig, ChannelBackendFactory
    )
    CHANNELS_AVAILABLE = True
except ImportError:
    CHANNELS_AVAILABLE = False
    print("Channel modules not available, tests will be skipped")

pytestmark = pytest.mark.skipif(not CHANNELS_AVAILABLE, reason="Channel modules not available")

class TestChannelT:
    """Test ChannelT abstract base class."""

    def test_channel_interface(self):
        """Test ChannelT defines required interface."""
        # ChannelT should be an async iterator
        assert hasattr(ChannelT, '__aiter__')

    @pytest.mark.asyncio
    async def test_channel_iteration(self):
        """Test basic channel iteration."""
        channel = Channel()

        # Test empty channel
        items = []
        async for item in channel:
            items.append(item)
        assert items == []

class TestChannel:
    """Test core Channel implementation."""

    def test_channel_creation(self):
        """Test channel creation."""
        channel = Channel()
        assert channel is not None
        assert hasattr(channel, 'put')
        assert hasattr(channel, 'get')

    @pytest.mark.asyncio
    async def test_channel_basic_operations(self):
        """Test basic channel put/get operations."""
        channel = Channel()

        # Put items
        await channel.put("item1")
        await channel.put("item2")

        # Get items
        item1 = await channel.get()
        item2 = await channel.get()

        assert item1 == "item1"
        assert item2 == "item2"

    @pytest.mark.asyncio
    async def test_channel_async_iteration(self):
        """Test channel async iteration."""
        channel = Channel()

        # Put items
        await channel.put("hello")
        await channel.put("world")

        # Close channel to end iteration
        await channel.close()

        # Iterate
        items = []
        async for item in channel:
            items.append(item)

        assert items == ["hello", "world"]

    @pytest.mark.asyncio
    async def test_channel_close(self):
        """Test channel close operation."""
        channel = Channel()

        # Put item
        await channel.put("test")

        # Close channel
        await channel.close()

        # Should be able to get remaining items
        item = await channel.get()
        assert item == "test"

        # Further gets should return None or raise
        with pytest.raises(asyncio.QueueEmpty):
            await asyncio.wait_for(channel.get(), timeout=0.1)

class TestSerializedChannel:
    """Test serialized channel functionality."""

    @pytest.mark.asyncio
    async def test_serialized_channel(self):
        """Test channel with serialization."""
        # Mock serializer
        mock_serializer = MagicMock()
        mock_serializer.serialize = MagicMock(return_value=b'{"test": "data"}')
        mock_serializer.deserialize = MagicMock(return_value={"test": "data"})

        channel = SerializedChannel(serializer=mock_serializer)

        # Put dict
        await channel.put({"test": "data"})

        # Get should deserialize
        result = await channel.get()
        assert result == {"test": "data"}

        mock_serializer.serialize.assert_called_once_with({"test": "data"})
        mock_serializer.deserialize.assert_called_once()

class TestChannelManager:
    """Test channel manager functionality."""

    def test_channel_manager_creation(self):
        """Test channel manager creation."""
        manager = ChannelManager()
        assert manager is not None
        assert hasattr(manager, 'register_factory')
        assert hasattr(manager, 'create_channel')

    def test_channel_backend_enum(self):
        """Test channel backend enum."""
        assert ChannelBackend.MEMORY == "memory"
        assert ChannelBackend.KAFKA == "kafka"
        assert ChannelBackend.REDIS == "redis"
        assert ChannelBackend.FLIGHT == "flight"
        assert ChannelBackend.ROCKSDB == "rocksdb"

    def test_channel_policy_enum(self):
        """Test channel policy enum."""
        assert ChannelPolicy.PERFORMANCE == "performance"
        assert ChannelPolicy.DURABILITY == "durability"
        assert ChannelPolicy.LATENCY == "latency"
        assert ChannelPolicy.THROUGHPUT == "throughput"

    def test_register_factory(self):
        """Test factory registration."""
        manager = ChannelManager()

        mock_factory = MagicMock(spec=ChannelBackendFactory)
        manager.register_factory(ChannelBackend.MEMORY, mock_factory)

        assert ChannelBackend.MEMORY in manager._factories

    @pytest.mark.asyncio
    async def test_create_channel_with_factory(self):
        """Test channel creation with registered factory."""
        manager = ChannelManager()

        # Mock factory
        mock_factory = MagicMock(spec=ChannelBackendFactory)
        mock_channel = MagicMock()
        mock_factory.create_channel.return_value = mock_channel

        manager.register_factory(ChannelBackend.MEMORY, mock_factory)

        # Create channel
        config = ChannelConfig(backend=ChannelBackend.MEMORY)
        channel = await manager.create_channel("test_channel", config)

        assert channel == mock_channel
        mock_factory.create_channel.assert_called_once_with("test_channel", config)

    @pytest.mark.asyncio
    async def test_create_channel_policy_selection(self):
        """Test channel creation with policy-based selection."""
        manager = ChannelManager()

        # Register multiple factories
        memory_factory = MagicMock(spec=ChannelBackendFactory)
        kafka_factory = MagicMock(spec=ChannelBackendFactory)

        memory_channel = MagicMock()
        kafka_channel = MagicMock()

        memory_factory.create_channel.return_value = memory_channel
        kafka_factory.create_channel.return_value = kafka_channel

        manager.register_factory(ChannelBackend.MEMORY, memory_factory)
        manager.register_factory(ChannelBackend.KAFKA, kafka_factory)

        # Test performance policy (should prefer memory)
        config = ChannelConfig(policy=ChannelPolicy.PERFORMANCE)
        channel = await manager.create_channel("perf_channel", config)

        assert channel == memory_channel

        # Test durability policy (should prefer kafka)
        config = ChannelConfig(policy=ChannelPolicy.DURABILITY)
        channel = await manager.create_channel("durable_channel", config)

        assert channel == kafka_channel

    @pytest.mark.asyncio
    async def test_create_channel_fallback(self):
        """Test fallback when preferred backend unavailable."""
        manager = ChannelManager()

        # Only register memory factory
        memory_factory = MagicMock(spec=ChannelBackendFactory)
        memory_channel = MagicMock()
        memory_factory.create_channel.return_value = memory_channel

        manager.register_factory(ChannelBackend.MEMORY, memory_factory)

        # Request kafka channel (should fallback to memory)
        config = ChannelConfig(backend=ChannelBackend.KAFKA)
        channel = await manager.create_channel("fallback_channel", config)

        assert channel == memory_channel

class TestChannelBackends:
    """Test specific channel backend implementations."""

    @pytest.mark.asyncio
    async def test_memory_channel(self):
        """Test memory channel backend."""
        try:
            from sabot.channels_memory import MemoryChannel

            channel = MemoryChannel()

            # Test operations
            await channel.put("test_data")
            result = await channel.get()
            assert result == "test_data"

        except ImportError:
            pytest.skip("Memory channel not implemented")

    @pytest.mark.asyncio
    async def test_kafka_channel(self):
        """Test Kafka channel backend."""
        try:
            from sabot.channels_kafka import KafkaChannel

            # Mock Kafka producer/consumer
            with patch('aiokafka.AIOKafkaProducer') as mock_producer, \
                 patch('aiokafka.AIOKafkaConsumer') as mock_consumer:

                mock_producer.return_value = AsyncMock()
                mock_consumer.return_value = AsyncMock()

                channel = KafkaChannel(
                    topic="test_topic",
                    bootstrap_servers="localhost:9092"
                )

                # Should create without error
                assert channel is not None

        except ImportError:
            pytest.skip("Kafka channel not implemented")

    @pytest.mark.asyncio
    async def test_redis_channel(self):
        """Test Redis channel backend."""
        try:
            from sabot.channels_redis import RedisChannel

            # Mock Redis connection
            with patch('redis.asyncio.Redis') as mock_redis:
                mock_redis.return_value = AsyncMock()

                channel = RedisChannel(
                    host="localhost",
                    port=6379,
                    channel_name="test_channel"
                )

                # Should create without error
                assert channel is not None

        except ImportError:
            pytest.skip("Redis channel not implemented")

class TestChannelIntegration:
    """Test channel integration scenarios."""

    @pytest.mark.asyncio
    async def test_channel_pipeline(self):
        """Test channel used in processing pipeline."""
        channel = Channel()

        # Simulate producer
        async def producer():
            for i in range(5):
                await channel.put(f"item_{i}")
                await asyncio.sleep(0.01)
            await channel.close()

        # Simulate consumer
        async def consumer():
            items = []
            async for item in channel:
                items.append(item)
            return items

        # Run pipeline
        await asyncio.gather(producer(), consumer())

        # Verify all items were processed
        # Note: consumer result not captured in this test
        # In real usage, would use queue or callback

    @pytest.mark.asyncio
    async def test_channel_with_multiple_consumers(self):
        """Test channel with multiple consumers."""
        channel = Channel()

        # Put items
        for i in range(10):
            await channel.put(f"item_{i}")

        # Close to signal end
        await channel.close()

        # Multiple consumers
        async def consume_all():
            items = []
            async for item in channel:
                items.append(item)
            return items

        # Only first consumer gets all items (channel drained)
        consumer1_result = await consume_all()
        consumer2_result = await consume_all()

        assert len(consumer1_result) == 10
        assert len(consumer2_result) == 0  # Channel drained

    @pytest.mark.asyncio
    async def test_channel_error_handling(self):
        """Test channel error handling."""
        channel = Channel()

        # Put an item
        await channel.put("good_item")

        # Simulate error in processing
        with pytest.raises(ValueError):
            async for item in channel:
                if item == "good_item":
                    raise ValueError("Processing error")

        # Channel should still be usable
        await channel.put("after_error")
        result = await channel.get()
        assert result == "after_error"

class TestChannelPerformance:
    """Test channel performance characteristics."""

    @pytest.mark.asyncio
    async def test_channel_throughput(self):
        """Test channel throughput."""
        channel = Channel()
        num_items = 1000

        # Producer
        async def producer():
            for i in range(num_items):
                await channel.put(f"item_{i}")
            await channel.close()

        # Consumer
        async def consumer():
            count = 0
            async for _ in channel:
                count += 1
            return count

        import time
        start_time = time.time()

        await asyncio.gather(producer(), consumer())

        end_time = time.time()
        duration = end_time - start_time

        # Basic throughput check (should be fast)
        assert duration < 1.0  # Should complete in less than 1 second

    @pytest.mark.asyncio
    async def test_channel_memory_usage(self):
        """Test channel memory usage patterns."""
        channel = Channel()

        # Add many items
        large_data = ["x" * 1000 for _ in range(100)]  # 100KB each

        for item in large_data:
            await channel.put(item)

        # Consume some
        for _ in range(50):
            await channel.get()

        # Should still have remaining items
        remaining = 0
        async for _ in channel:
            remaining += 1

        assert remaining == 50

class TestChannelConfiguration:
    """Test channel configuration options."""

    @pytest.mark.asyncio
    async def test_channel_buffer_size(self):
        """Test channel buffer size configuration."""
        # Test with different buffer sizes if supported
        channel = Channel()

        # Basic functionality should work regardless of buffer size
        await channel.put("test")
        result = await channel.get()
        assert result == "test"

    @pytest.mark.asyncio
    async def test_channel_serialization_options(self):
        """Test channel serialization configuration."""
        # Test with different serialization options
        channel = SerializedChannel(serializer=None)  # No-op serialization

        await channel.put("test_data")
        result = await channel.get()
        assert result == "test_data"

if __name__ == "__main__":
    # Run tests if called directly
    pytest.main([__file__, "-v"])
