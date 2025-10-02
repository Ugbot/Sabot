#!/usr/bin/env python3
"""Simple test script for Arrow-focused windowing system."""

import asyncio
import sys
import os

async def test_windowing_system():
    """Test the windowing system."""
    print("Testing Windowing System...")

    from sabot.windows import WindowedStream, WindowSpec, WindowType

    # Create a mock app
    class MockApp:
        pass

    app = MockApp()
    windowed = WindowedStream(app)

    # Test creating different window types
    spec = WindowSpec(
        name="test_tumbling",
        window_type=WindowType.TUMBLING,
        size_seconds=10.0,
        key_field="user_id",
        aggregations={"count": "count"}
    )

    # Test window processor creation
    from sabot.windows import WindowProcessor
    processor = WindowProcessor(spec)

    # Test processing some mock data
    test_records = [
        {"user_id": "user1", "value": 1, "timestamp": 1000.0},
        {"user_id": "user1", "value": 2, "timestamp": 1001.0},
        {"user_id": "user2", "value": 3, "timestamp": 1002.0},
    ]

    await processor.process_record(test_records)
    print("  ✅ Processed test records")

    # Test stats
    stats = await processor.get_stats()
    print(f"  ✅ Got stats: {stats}")

    # Test tumbling window creation
    class MockStream:
        def __init__(self, records):
            self.records = records

        def __aiter__(self):
            return self._async_iter()

        async def _async_iter(self):
            for record in self.records:
                yield record

    mock_stream = MockStream(test_records)
    tumbling_window = windowed.tumbling(
        stream=mock_stream,
        size_seconds=10.0,
        key_field="user_id",
        aggregations={"value": "sum"}
    )

    print("  ✅ Created tumbling window")

    # Test windowed stream iteration
    window_count = 0
    async for window in tumbling_window:
        window_count += 1
        print(f"  ✅ Got window {window_count}: {window}")

    print("✅ Windowing system tests passed!")


async def test_cython_availability():
    """Test Cython availability."""
    print("Testing Cython Availability...")

    try:
        from sabot.windows import CYTHON_AVAILABLE
        print(f"  Cython Available: {CYTHON_AVAILABLE}")

        if CYTHON_AVAILABLE:
            from sabot._cython.windows import get_window_manager
            manager = get_window_manager()
            print("  ✅ Cython window manager available")

            # Test creating a window
            from sabot._cython.windows import create_window_processor
            processor = create_window_processor("tumbling", 10.0)
            print("  ✅ Cython window processor created")

        else:
            print("  ⚠️  Cython not available, using Python fallback")

    except ImportError as e:
        print(f"  ⚠️  Cython import failed: {e}")


async def main():
    """Run all tests."""
    print("🪟 Testing Sabot Windowing System")
    print("=" * 40)

    try:
        await test_cython_availability()
        await test_windowing_system()

        print("\n🎉 All windowing tests passed!")

    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)