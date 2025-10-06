#!/usr/bin/env python3
"""Simple test runner for channel functionality without pytest."""

import asyncio
import sys
import os

# Add sabot to path for testing
sys.path.insert(0, os.path.dirname(__file__))

# Mock pytest
class MockPytest:
    class mark:
        @staticmethod
        def skipif(condition, reason):
            def decorator(func):
                if condition:
                    return lambda *args, **kwargs: None
                return func
            return decorator

sys.modules['pytest'] = MockPytest()

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

async def run_basic_channel_tests():
    """Run basic channel functionality tests."""
    print("🧪 Running Basic Channel Tests")
    print("-" * 30)

    try:
        from sabot.channels import Channel
        print("✅ Channel import successful")

        # Test 1: Basic channel operations
        channel = Channel()
        await channel.put("test_data")
        result = await channel.get()
        assert result == "test_data"
        print("✅ Basic put/get operations work")

        # Test 2: Async iteration
        await channel.put("item1")
        await channel.put("item2")
        await channel.close()

        items = []
        async for item in channel:
            items.append(item)

        assert items == ["item1", "item2"]
        print("✅ Async iteration works")

        return True

    except Exception as e:
        print(f"❌ Channel test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

async def run_channel_manager_tests():
    """Run channel manager tests."""
    print("\n🎛️  Running Channel Manager Tests")
    print("-" * 35)

    try:
        from sabot.channel_manager import ChannelManager, ChannelBackend, ChannelConfig
        print("✅ Channel manager import successful")

        # Test manager creation
        manager = ChannelManager()
        assert manager is not None
        print("✅ Channel manager creation works")

        # Test factory registration (if available)
        try:
            from sabot.channels_memory import MemoryChannelFactory
            factory = MemoryChannelFactory()
            manager.register_factory(ChannelBackend.MEMORY, factory)
            print("✅ Factory registration works")

            # Test channel creation
            config = ChannelConfig(backend=ChannelBackend.MEMORY)
            channel = await manager.create_channel("test", config)
            assert channel is not None
            print("✅ Channel creation via manager works")

        except ImportError:
            print("⚠️  Memory channel factory not available, skipping factory tests")

        return True

    except Exception as e:
        print(f"❌ Channel manager test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

async def run_integration_tests():
    """Run integration tests."""
    print("\n🔗 Running Integration Tests")
    print("-" * 28)

    try:
        from sabot.channels import Channel

        # Test producer-consumer pattern
        channel = Channel()

        async def producer():
            for i in range(10):
                await channel.put(f"msg_{i}")
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
        assert len(results) == 10
        assert results[0] == "msg_0"
        assert results[9] == "msg_9"

        print("✅ Producer-consumer integration works")
        return True

    except Exception as e:
        print(f"❌ Integration test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

async def run_performance_tests():
    """Run basic performance tests."""
    print("\n⚡ Running Performance Tests")
    print("-" * 28)

    try:
        from sabot.channels import Channel
        import time

        channel = Channel()
        num_messages = 1000

        # Test throughput
        start_time = time.time()

        # Producer
        for i in range(num_messages):
            await channel.put(f"perf_msg_{i}")

        # Consumer
        count = 0
        async for _ in channel:
            count += 1
            if count >= num_messages:
                break

        end_time = time.time()
        duration = end_time - start_time

        throughput = num_messages / duration
        print(f"✅ Performance test: {throughput:.0f} msg/sec")
        print(".3f")

        # Should be reasonably fast
        assert throughput > 100  # At least 100 msg/sec

        return True

    except Exception as e:
        print(f"❌ Performance test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

async def main():
    """Run all channel tests."""
    print("🧪 Sabot Channel Test Suite")
    print("=" * 40)

    tests = [
        ("Basic Channel Tests", run_basic_channel_tests),
        ("Channel Manager Tests", run_channel_manager_tests),
        ("Integration Tests", run_integration_tests),
        ("Performance Tests", run_performance_tests),
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

    print("\n" + "=" * 40)
    print(f"📊 Test Results: {passed}/{total} tests passed")

    if passed == total:
        print("🎉 All channel tests passed!")
        return 0
    else:
        print("⚠️  Some tests failed")
        return 1

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
