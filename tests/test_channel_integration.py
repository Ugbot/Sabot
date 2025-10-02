# -*- coding: utf-8 -*-
"""Integration tests for channel system."""

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

try:
    from sabot.channels import Channel
    from sabot.channel_manager import ChannelManager, ChannelBackend, ChannelConfig
    CHANNELS_AVAILABLE = True
except ImportError:
    CHANNELS_AVAILABLE = False

pytestmark = pytest.mark.skipif(not CHANNELS_AVAILABLE, reason="Channel modules not available")

class TestChannelIntegration:
    """Integration tests for channel system."""

    @pytest.mark.asyncio
    async def test_end_to_end_channel_flow(self):
        """Test complete channel data flow."""
        # Create channel
        channel = Channel()

        # Producer: send data
        producer_data = [f"message_{i}" for i in range(10)]

        async def producer():
            for data in producer_data:
                await channel.put(data)
                await asyncio.sleep(0.001)  # Simulate processing time
            await channel.close()

        # Consumer: receive data
        async def consumer():
            received = []
            async for data in channel:
                received.append(data)
            return received

        # Run producer and consumer concurrently
        consumer_task = asyncio.create_task(consumer())
        await producer()

        received_data = await consumer_task

        # Verify all data was received
        assert received_data == producer_data

    @pytest.mark.asyncio
    async def test_channel_with_processing_pipeline(self):
        """Test channel in a processing pipeline."""
        input_channel = Channel()
        output_channel = Channel()

        # Stage 1: Transform data
        async def stage1():
            async for data in input_channel:
                # Transform: convert to uppercase
                transformed = data.upper()
                await output_channel.put(transformed)

        # Stage 2: Filter data
        async def stage2():
            results = []
            async for data in output_channel:
                # Filter: only keep items with even length
                if len(data) % 2 == 0:
                    results.append(data)
            return results

        # Input data
        input_data = ["hello", "world", "test", "data", "pipeline"]

        # Feed input
        async def feed_input():
            for data in input_data:
                await input_channel.put(data)
            await input_channel.close()

        # Run pipeline
        stage2_task = asyncio.create_task(stage2())
        stage1_task = asyncio.create_task(stage1())
        await feed_input()

        # Wait for stage 1 to complete
        await stage1_task

        # Close output channel
        await output_channel.close()

        # Get results
        results = await stage2_task

        # Verify: "HELLO" (5), "WORLD" (5), "TEST" (4), "DATA" (4), "PIPELINE" (8)
        # Even length: "TEST", "DATA", "PIPELINE"
        expected = ["TEST", "DATA", "PIPELINE"]
        assert results == expected

    @pytest.mark.asyncio
    async def test_channel_manager_integration(self):
        """Test channel manager with multiple backends."""
        try:
            from sabot.channel_manager import ChannelManager, ChannelBackend

            manager = ChannelManager()

            # Register memory backend (assuming it's available)
            try:
                from sabot.channels_memory import MemoryChannelFactory
                manager.register_factory(ChannelBackend.MEMORY, MemoryChannelFactory())
                has_memory = True
            except ImportError:
                has_memory = False

            if has_memory:
                # Create multiple channels
                channels = []
                for i in range(5):
                    config = ChannelConfig(backend=ChannelBackend.MEMORY)
                    channel = await manager.create_channel(f"test_channel_{i}", config)
                    channels.append(channel)

                # Test that all channels work
                for i, channel in enumerate(channels):
                    await channel.put(f"data_{i}")
                    result = await channel.get()
                    assert result == f"data_{i}"
            else:
                pytest.skip("Memory backend not available")

        except ImportError:
            pytest.skip("Channel manager not available")

    @pytest.mark.asyncio
    async def test_channel_error_recovery(self):
        """Test channel error recovery."""
        channel = Channel()

        # Put some data
        await channel.put("good_data_1")
        await channel.put("good_data_2")

        # Simulate error in processing
        async def failing_consumer():
            items = []
            try:
                async for data in channel:
                    if data == "good_data_2":
                        raise ValueError("Processing failed")
                    items.append(data)
            except ValueError:
                pass  # Expected error
            return items

        # Run failing consumer
        items = await failing_consumer()

        # Should have received first item
        assert items == ["good_data_1"]

        # Channel should still be usable
        await channel.put("after_error")
        result = await channel.get()
        assert result == "after_error"

    @pytest.mark.asyncio
    async def test_channel_concurrent_access(self):
        """Test concurrent access to channels."""
        channel = Channel()

        # Multiple producers
        async def producer(producer_id, count):
            for i in range(count):
                await channel.put(f"p{producer_id}_item_{i}")

        # Single consumer
        async def consumer(expected_count):
            items = []
            for _ in range(expected_count):
                item = await channel.get()
                items.append(item)
            return items

        # Run 3 producers and 1 consumer
        num_producers = 3
        items_per_producer = 10
        total_items = num_producers * items_per_producer

        producer_tasks = [
            producer(i, items_per_producer)
            for i in range(num_producers)
        ]

        consumer_task = consumer(total_items)

        # Run all concurrently
        await asyncio.gather(*producer_tasks, consumer_task)

        # Close channel
        await channel.close()

        # Verify all items were consumed
        remaining_count = 0
        async for _ in channel:
            remaining_count += 1

        assert remaining_count == 0

    @pytest.mark.asyncio
    async def test_channel_buffering(self):
        """Test channel buffering behavior."""
        channel = Channel()

        # Fill channel beyond typical buffer
        large_dataset = [f"item_{i}" for i in range(1000)]

        # Producer
        async def produce_all():
            for item in large_dataset:
                await channel.put(item)
            await channel.close()

        # Consumer
        async def consume_all():
            items = []
            async for item in channel:
                items.append(item)
            return items

        # Run concurrently
        consumer_task = asyncio.create_task(consume_all())
        await produce_all()

        received_items = await consumer_task

        # Verify all items received
        assert len(received_items) == len(large_dataset)
        assert received_items == large_dataset

class TestChannelPerformanceIntegration:
    """Performance integration tests."""

    @pytest.mark.asyncio
    async def test_high_throughput_channel(self):
        """Test channel with high throughput."""
        channel = Channel()
        num_messages = 10000

        # Producer
        async def producer():
            for i in range(num_messages):
                await channel.put(f"msg_{i}")
            await channel.close()

        # Consumer
        async def consumer():
            count = 0
            async for _ in channel:
                count += 1
            return count

        import time
        start_time = time.time()

        # Run concurrently
        consumer_task = asyncio.create_task(consumer())
        await producer()

        received_count = await consumer_task
        end_time = time.time()

        duration = end_time - start_time

        # Verify all messages received
        assert received_count == num_messages

        # Check throughput (should be reasonable)
        throughput = num_messages / duration
        assert throughput > 1000  # At least 1000 msg/sec

        print(".2f")

    @pytest.mark.asyncio
    async def test_channel_memory_efficiency(self):
        """Test channel memory efficiency."""
        channel = Channel()

        # Create large messages
        large_message = "x" * 10000  # 10KB each
        num_messages = 100  # 1MB total

        # Put large messages
        for i in range(num_messages):
            await channel.put(large_message)

        # Consume and verify
        received_count = 0
        async for received in channel:
            assert len(received) == len(large_message)
            received_count += 1
            if received_count >= num_messages:
                break

        assert received_count == num_messages

class TestChannelSystemIntegration:
    """Test integration with broader system."""

    @pytest.mark.asyncio
    async def test_channel_with_app_integration(self):
        """Test channel integration with app (mocked)."""
        # This would test integration with the broader Sabot app
        # For now, just test basic channel functionality
        channel = Channel()

        await channel.put("integrated_test")
        result = await channel.get()
        assert result == "integrated_test"

    @pytest.mark.asyncio
    async def test_channel_lifecycle(self):
        """Test complete channel lifecycle."""
        channel = Channel()

        # Create
        assert channel is not None

        # Use
        await channel.put("lifecycle_test")
        result = await channel.get()
        assert result == "lifecycle_test"

        # Close
        await channel.close()

        # Verify closed behavior
        with pytest.raises(asyncio.QueueEmpty):
            await asyncio.wait_for(channel.get(), timeout=0.1)

if __name__ == "__main__":
    # Run tests if called directly
    pytest.main([__file__, "-v"])
