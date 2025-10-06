#!/usr/bin/env python3
"""
Test Cython Timer Service

Tests the high-performance timer service implementation for event-time and processing-time timers.
"""

import asyncio
import tempfile
import os
import sys

# Add sabot to path
sys.path.insert(0, os.path.dirname(__file__))

from sabot.stores.rocksdb_fallback import RocksDBBackend
from sabot.stores.base import StoreBackendConfig


async def test_timer_service():
    """Test basic timer service functionality."""
    print("🧪 Testing Timer Service...")

    with tempfile.TemporaryDirectory() as temp_dir:
        config = StoreBackendConfig(path=temp_dir)
        backend = RocksDBBackend(config)

        try:
            await backend.start()

            # Create timer service (without watermark tracker for basic test)
            from sabot._cython.time.timers import TimerService
            timer_service = TimerService(backend)

            # Test processing-time timers
            fired_callbacks = []

            def callback1():
                fired_callbacks.append("callback1")

            def callback2():
                fired_callbacks.append("callback2")

            # Register timers
            timer_service.register_processing_timer(1000, callback1)  # 1 second from epoch
            timer_service.register_processing_timer(2000, callback2)  # 2 seconds from epoch

            # Update processing time to 1500 (should fire first timer)
            timer_service.update_processing_time(1500)

            # Get expired timers
            expired = timer_service.get_expired_timers()
            assert len(expired) == 1, f"Expected 1 expired timer, got {len(expired)}"
            assert expired[0][0] == 1000, f"Expected timestamp 1000, got {expired[0][0]}"

            # Fire expired timers
            timer_service.fire_expired_timers()
            assert len(fired_callbacks) == 1, f"Expected 1 callback fired, got {len(fired_callbacks)}"
            assert fired_callbacks[0] == "callback1"

            # Update time to 2500 (should fire second timer)
            timer_service.update_processing_time(2500)
            timer_service.fire_expired_timers()
            assert len(fired_callbacks) == 2, f"Expected 2 callbacks fired, got {len(fired_callbacks)}"

            print("✅ Timer Service basic tests passed!")

        finally:
            await backend.stop()


async def test_watermark_tracker():
    """Test watermark tracker functionality."""
    print("🧪 Testing Watermark Tracker...")

    with tempfile.TemporaryDirectory() as temp_dir:
        config = StoreBackendConfig(path=temp_dir)
        backend = RocksDBBackend(config)

        try:
            await backend.start()

            # Create watermark tracker
            from sabot._cython.time.watermark_tracker import WatermarkTracker
            tracker = WatermarkTracker(3, backend)  # 3 partitions

            # Test initial state
            assert tracker.get_current_watermark() == -9223372036854775808  # LLONG_MIN

            # Update partition 0
            watermark = tracker.update_watermark(0, 100)
            assert watermark == 100, f"Expected watermark 100, got {watermark}"

            # Update partition 1 (lower watermark)
            watermark = tracker.update_watermark(1, 50)
            assert watermark == 50, f"Expected watermark 50 (min), got {watermark}"

            # Update partition 2 (higher watermark)
            watermark = tracker.update_watermark(2, 200)
            assert watermark == 50, f"Expected watermark 50 (still min), got {watermark}"

            # Update partition 1 to higher value
            watermark = tracker.update_watermark(1, 150)
            assert watermark == 100, f"Expected watermark 100 (min of 100,150,200), got {watermark}"

            # Test late event detection
            assert tracker.is_late_event(50), "Event at 50 should be late (watermark is 100)"
            assert not tracker.is_late_event(150), "Event at 150 should not be late"

            # Test partition watermarks
            partition_marks = tracker.get_partition_watermarks()
            assert partition_marks[0] == 100
            assert partition_marks[1] == 150
            assert partition_marks[2] == 200

            print("✅ Watermark Tracker tests passed!")

        finally:
            await backend.stop()


async def test_time_service():
    """Test unified time service."""
    print("🧪 Testing Time Service...")

    with tempfile.TemporaryDirectory() as temp_dir:
        config = StoreBackendConfig(path=temp_dir)
        backend = RocksDBBackend(config)

        try:
            await backend.start()

            # Create time service
            from sabot._cython.time.time_service import TimeService
            time_service = TimeService(2, backend, "event-time")

            # Test initial state
            assert time_service.get_time_characteristic() == "event-time"
            assert time_service.get_current_watermark() == -9223372036854775808  # LLONG_MIN

            # Test watermark updates
            watermark = time_service.update_watermark(0, 1000)
            assert watermark == 1000

            watermark = time_service.update_watermark(1, 800)
            assert watermark == 800  # Min of 1000, 800

            # Test timer registration
            fired_callbacks = []

            def timer_callback():
                fired_callbacks.append("timer_fired")

            time_service.register_event_timer(500, timer_callback)  # Should fire immediately

            # Process timers (watermark is 800, so timer at 500 should fire)
            expired = time_service.process_timers()
            assert len(expired) == 1, f"Expected 1 expired timer, got {len(expired)}"
            assert len(fired_callbacks) == 1, f"Expected 1 callback, got {len(fired_callbacks)}"

            # Test processing-time mode
            time_service.set_time_characteristic("processing-time")
            assert time_service.get_time_characteristic() == "processing-time"

            # Register processing-time timer
            def proc_timer_callback():
                fired_callbacks.append("proc_timer_fired")

            time_service.register_processing_timer(100, proc_timer_callback)
            time_service.update_processing_time(200)  # Advance processing time

            # Fire timers
            time_service.process_timers()
            assert len(fired_callbacks) == 2, f"Expected 2 callbacks, got {len(fired_callbacks)}"

            print("✅ Time Service tests passed!")

        finally:
            await backend.stop()


async def test_event_time_service():
    """Test event-time service."""
    print("🧪 Testing Event-Time Service...")

    with tempfile.TemporaryDirectory() as temp_dir:
        config = StoreBackendConfig(path=temp_dir)
        backend = RocksDBBackend(config)

        try:
            await backend.start()

            # Create event-time service
            from sabot._cython.time.event_time import EventTimeService
            event_service = EventTimeService(2, backend, watermark_interval_ms=100)

            # Test initial state
            assert event_service.get_current_watermark() == -9223372036854775808

            # Test event processing
            late_events = []

            def late_handler(partition, timestamp, data):
                late_events.append((partition, timestamp, data))

            event_service.set_late_event_handler(late_handler)

            # Process normal event
            event_service.process_event(0, 1000, "event_data")
            assert event_service.get_current_watermark() == 1000

            # Process late event (before current watermark)
            event_service.process_event(0, 500, "late_data")
            assert len(late_events) == 1, f"Expected 1 late event, got {len(late_events)}"
            assert late_events[0] == (0, 500, "late_data")

            # Test idleness detection
            # Partition 1 hasn't received events, so it should be idle
            idle_partitions = event_service.get_idle_partitions()
            assert 1 in idle_partitions, "Partition 1 should be idle"

            # Advance watermark for idle partitions
            event_service.advance_watermark_for_idle_partitions()
            # This should set partition 1's watermark to current global watermark (1000)

            print("✅ Event-Time Service tests passed!")

        finally:
            await backend.stop()


async def test_timer_stats():
    """Test timer statistics and monitoring."""
    print("🧪 Testing Timer Statistics...")

    with tempfile.TemporaryDirectory() as temp_dir:
        config = StoreBackendConfig(path=temp_dir)
        backend = RocksDBBackend(config)

        try:
            await backend.start()

            from sabot._cython.time.time_service import TimeService
            time_service = TimeService(1, backend, "event-time")

            # Register various timers
            time_service.register_event_timer(1000, lambda: None)
            time_service.register_event_timer(2000, lambda: None)
            time_service.register_processing_timer(3000, lambda: None)

            # Update watermark to trigger some timers
            time_service.update_watermark(0, 1500)

            # Get stats
            stats = time_service.get_stats()

            assert stats['timers']['active_timers'] >= 2, f"Expected at least 2 active timers, got {stats['timers']['active_timers']}"
            assert stats['timers']['event_timers'] >= 2, f"Expected at least 2 event timers, got {stats['timers']['event_timers']}"
            assert stats['timers']['processing_timers'] >= 1, f"Expected at least 1 processing timer, got {stats['timers']['processing_timers']}"

            print("✅ Timer Statistics tests passed!")

        finally:
            await backend.stop()


async def main():
    """Run all timer service tests."""
    print("🚀 Cython Timer Service Tests")
    print("=" * 50)

    try:
        await test_timer_service()
        await test_watermark_tracker()
        await test_time_service()
        await test_event_time_service()
        await test_timer_stats()

        print("\n🎉 All timer service tests passed!")
        print("✅ Flink-compatible timer service is working")

    except Exception as e:
        print(f"\n❌ Tests failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
