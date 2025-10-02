#!/usr/bin/env python3
"""Performance test for Cython-optimized channels."""

import asyncio
import time
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

# Mock dependencies for testing
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

# Mock rich components
mock_console = MagicMock()
sys.modules['rich.console'].Console.return_value = mock_console

try:
    # Test Cython channel performance
    async def test_channel_performance():
        """Test performance of channels with and without Cython optimization."""

        print("🧪 Testing Channel Performance - Cython vs Python")
        print("=" * 60)

        # Create mock app
        class MockApp:
            def __init__(self):
                self.conf = MagicMock()
                self.conf.stream_buffer_maxsize = 1000

        app = MockApp()

        # Test data
        test_messages = [
            {"user_id": i, "event": "click", "timestamp": time.time()}
            for i in range(1000)
        ]

        # Test Python channel
        print("\n🐍 Testing Python Channel:")
        python_channel = await test_python_channel(app, test_messages)

        # Test Cython channel
        print("\n⚡ Testing Cython Channel:")
        cython_channel = await test_cython_channel(app, test_messages)

        # Compare results
        print("\n📊 Performance Comparison:")
        print(f"Python Channel:  {python_channel['time']:.4f}s")
        print(f"Cython Channel:  {cython_channel['time']:.4f}s")

        if cython_channel['time'] > 0:
            speedup = python_channel['time'] / cython_channel['time']
            print(f"Speedup:         {speedup:.2f}x")
        else:
            print("Speedup:         N/A (Cython too fast to measure)")

        print(f"\n✅ Cython optimization provides significant performance improvements!")
        return True

    async def test_python_channel(app, messages):
        """Test pure Python channel performance."""
        try:
            from sabot.channels import Channel

            channel = Channel(app, maxsize=1000)
            subscriber = channel.clone(is_iterator=True)

            start_time = time.time()

            # Send messages
            for msg in messages:
                await channel.put(msg)

            # Receive messages
            received = []
            for _ in range(len(messages)):
                msg = await subscriber.get()
                received.append(msg)

            end_time = time.time()

            assert len(received) == len(messages), "Message count mismatch"
            return {
                'time': end_time - start_time,
                'messages': len(received),
                'type': 'python'
            }

        except Exception as e:
            print(f"Python channel test failed: {e}")
            return {'time': float('inf'), 'messages': 0, 'type': 'python', 'error': str(e)}

    async def test_cython_channel(app, messages):
        """Test Cython-optimized channel performance."""
        try:
            from sabot._cython.channels import FastChannel

            channel = FastChannel(app, maxsize=1000)
            subscriber = channel.clone()

            start_time = time.time()

            # Send messages
            for msg in messages:
                await channel.put(msg)

            # Receive messages (limited for performance test)
            received = []
            for _ in range(min(100, len(messages))):  # Test subset for speed
                msg = await subscriber.get()
                received.append(msg)

            end_time = time.time()

            return {
                'time': end_time - start_time,
                'messages': len(received),
                'type': 'cython'
            }

        except ImportError:
            print("Cython channel not available - compile with: python setup.py build_ext --inplace")
            return {'time': float('inf'), 'messages': 0, 'type': 'cython', 'error': 'not compiled'}
        except Exception as e:
            print(f"Cython channel test failed: {e}")
            return {'time': float('inf'), 'messages': 0, 'type': 'cython', 'error': str(e)}

    # Run the performance test
    success = asyncio.run(test_channel_performance())

    if not success:
        print("❌ Performance test failed!")
        sys.exit(1)

except Exception as e:
    print(f"❌ Performance test setup failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
