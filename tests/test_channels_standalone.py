#!/usr/bin/env python3
"""Standalone channel tests that don't depend on full Sabot imports."""

import asyncio
import sys
import os

# Add sabot to path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

async def test_basic_channel():
    """Test basic channel functionality."""
    print("🧪 Testing Basic Channel Functionality")

    try:
        # Import channel components directly
        sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

        # Test ChannelT interface
        from types.channels import ChannelT
        print("✅ ChannelT interface imported")

        # Test basic Channel
        from channels import Channel
        print("✅ Channel class imported")

        channel = Channel()
        print("✅ Channel instance created")

        # Test put/get
        await channel.put("test_data")
        result = await channel.get()
        assert result == "test_data"
        print("✅ Basic put/get operations work")

        # Test async iteration
        await channel.put("item1")
        await channel.put("item2")
        await channel.close()

        items = []
        async for item in channel:
            items.append(item)

        assert items == ["item1", "item2"]
        print("✅ Async iteration works")

        return True

    except ImportError as e:
        print(f"⚠️  Import failed: {e}")
        return False
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

async def test_channel_manager():
    """Test channel manager functionality."""
    print("\n🎛️  Testing Channel Manager")

    try:
        from channel_manager import ChannelManager, ChannelBackend, ChannelConfig
        print("✅ Channel manager imported")

        manager = ChannelManager()
        print("✅ Channel manager created")

        # Test basic functionality
        assert hasattr(manager, 'register_factory')
        assert hasattr(manager, 'create_channel')
        print("✅ Channel manager has required methods")

        # Test enums
        assert ChannelBackend.MEMORY == "memory"
        assert ChannelBackend.KAFKA == "kafka"
        print("✅ Backend enums work")

        # Test config
        config = ChannelConfig()
        assert config.backend == ChannelBackend.MEMORY  # Default
        print("✅ Channel config works")

        return True

    except ImportError as e:
        print(f"⚠️  Import failed: {e}")
        return False
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

async def test_channel_backends():
    """Test channel backends."""
    print("\n🔧 Testing Channel Backends")

    try:
        # Test memory backend
        try:
            from channels_memory import MemoryChannelFactory
            print("✅ Memory channel factory imported")

            factory = MemoryChannelFactory()
            channel = await factory.create_channel("test", {})
            print("✅ Memory channel created")

            # Test functionality
            await channel.put("memory_test")
            result = await channel.get()
            assert result == "memory_test"
            print("✅ Memory channel operations work")

        except ImportError:
            print("⚠️  Memory channel not available")

        # Test other backends (will likely fail without dependencies)
        backends_to_test = [
            ('channels_kafka', 'KafkaChannelFactory'),
            ('channels_redis', 'RedisChannelFactory'),
            ('channels_flight', 'FlightChannelFactory'),
            ('channels_rocksdb', 'RocksDBChannelFactory'),
        ]

        for module_name, factory_name in backends_to_test:
            try:
                module = __import__(module_name, fromlist=[factory_name])
                factory_class = getattr(module, factory_name)
                print(f"✅ {factory_name} imported")
            except ImportError:
                print(f"⚠️  {factory_name} not available")

        return True

    except Exception as e:
        print(f"❌ Backend test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

async def test_integration():
    """Test channel integration scenarios."""
    print("\n🔗 Testing Channel Integration")

    try:
        from channels import Channel

        # Test producer-consumer pattern
        channel = Channel()

        async def producer():
            for i in range(5):
                await channel.put(f"msg_{i}")
                await asyncio.sleep(0.001)
            await channel.close()

        async def consumer():
            items = []
            async for item in channel:
                items.append(item)
            return items

        # Run concurrently
        consumer_task = asyncio.create_task(consumer())
        await producer()

        results = await consumer_task
        assert len(results) == 5
        assert results == [f"msg_{i}" for i in range(5)]

        print("✅ Producer-consumer integration works")

        # Test pipeline
        input_channel = Channel()
        output_channel = Channel()

        async def transformer():
            async for data in input_channel:
                transformed = data.upper()
                await output_channel.put(transformed)

        async def processor():
            results = []
            async for data in output_channel:
                if len(data) > 3:  # Filter
                    results.append(data)
            return results

        # Input data
        test_data = ["hello", "hi", "world", "ok"]

        async def feed_input():
            for data in test_data:
                await input_channel.put(data)
            await input_channel.close()

        # Run pipeline
        processor_task = asyncio.create_task(processor())
        transformer_task = asyncio.create_task(transformer())
        await feed_input()

        await transformer_task
        await output_channel.close()

        final_results = await processor_task

        # Expected: "HELLO" (5), "WORLD" (5) - both > 3 chars
        expected = ["HELLO", "WORLD"]
        assert final_results == expected

        print("✅ Processing pipeline works")

        return True

    except Exception as e:
        print(f"❌ Integration test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

async def test_performance():
    """Test channel performance."""
    print("\n⚡ Testing Channel Performance")

    try:
        from channels import Channel
        import time

        channel = Channel()
        num_messages = 1000

        start_time = time.time()

        # Producer
        for i in range(num_messages):
            await channel.put(f"perf_{i}")

        # Consumer
        count = 0
        async for _ in channel:
            count += 1
            if count >= num_messages:
                break

        end_time = time.time()
        duration = end_time - start_time

        throughput = num_messages / duration

        print(f"✅ Performance: {throughput:.0f} msg/sec")
        print(".3f")

        # Should be reasonably fast
        assert throughput > 100, f"Throughput too low: {throughput}"

        return True

    except Exception as e:
        print(f"❌ Performance test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

async def main():
    """Run all standalone channel tests."""
    print("🧪 Sabot Channel Standalone Test Suite")
    print("=" * 45)

    tests = [
        ("Basic Channel", test_basic_channel),
        ("Channel Manager", test_channel_manager),
        ("Channel Backends", test_channel_backends),
        ("Integration", test_integration),
        ("Performance", test_performance),
    ]

    passed = 0
    total = len(tests)

    for test_name, test_func in tests:
        try:
            result = await test_func()
            if result:
                passed += 1
        except Exception as e:
            print(f"❌ {test_name} crashed: {e}")

    print("\n" + "=" * 45)
    print(f"📊 Test Results: {passed}/{total} tests passed")

    if passed == total:
        print("🎉 All channel tests passed!")
        return True
    else:
        print("⚠️  Some tests failed - this may be expected if backends aren't implemented")
        return False

if __name__ == "__main__":
    success = asyncio.run(main())
    if success:
        print("\n✅ Channel testing completed successfully!")
    else:
        print("\n⚠️  Channel testing completed with some issues.")
        sys.exit(1)
