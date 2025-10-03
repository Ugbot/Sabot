"""Unit tests for Cython time modules."""

import pytest
import asyncio
import time as time_module

from sabot.time import (
    WatermarkTracker,
    Timers,
    EventTime,
    TimeService,
)


class TestWatermarkTracker:
    """Test WatermarkTracker for event-time processing."""

    def test_tracker_creation(self):
        """Test creating a watermark tracker."""
        tracker = WatermarkTracker(num_partitions=3)

        assert tracker.num_partitions == 3
        assert tracker.get_watermark() == 0

    def test_update_watermark(self):
        """Test updating watermark for a partition."""
        tracker = WatermarkTracker(num_partitions=2)

        tracker.update_watermark(partition_id=0, watermark=1000)

        # Global watermark should still be 0 (min of all partitions)
        assert tracker.get_watermark() == 0

        tracker.update_watermark(partition_id=1, watermark=500)

        # Global watermark should now be 500 (min of 1000 and 500)
        assert tracker.get_watermark() == 500

    def test_watermark_advancement(self):
        """Test watermark advances monotonically."""
        tracker = WatermarkTracker(num_partitions=2)

        tracker.update_watermark(partition_id=0, watermark=1000)
        tracker.update_watermark(partition_id=1, watermark=1000)

        assert tracker.get_watermark() == 1000

        # Advance both partitions
        tracker.update_watermark(partition_id=0, watermark=2000)
        tracker.update_watermark(partition_id=1, watermark=2000)

        assert tracker.get_watermark() == 2000

    def test_watermark_skew(self):
        """Test watermark with partition skew."""
        tracker = WatermarkTracker(num_partitions=3)

        # Partition 0 is ahead, partitions 1 and 2 lag
        tracker.update_watermark(partition_id=0, watermark=10000)
        tracker.update_watermark(partition_id=1, watermark=1000)
        tracker.update_watermark(partition_id=2, watermark=500)

        # Global watermark is min (500)
        assert tracker.get_watermark() == 500

    def test_get_partition_watermark(self):
        """Test getting watermark for specific partition."""
        tracker = WatermarkTracker(num_partitions=2)

        tracker.update_watermark(partition_id=0, watermark=1000)
        tracker.update_watermark(partition_id=1, watermark=2000)

        assert tracker.get_partition_watermark(partition_id=0) == 1000
        assert tracker.get_partition_watermark(partition_id=1) == 2000

    def test_watermark_update_performance(self):
        """Test watermark update latency (<5μs claimed)."""
        tracker = WatermarkTracker(num_partitions=100)

        # Measure 10000 updates
        start = time_module.perf_counter()
        for i in range(10000):
            tracker.update_watermark(partition_id=i % 100, watermark=i)
        end = time_module.perf_counter()

        avg_latency_us = ((end - start) / 10000) * 1_000_000

        # Should be <5μs per update
        assert avg_latency_us < 5, f"Watermark update latency {avg_latency_us:.2f}μs exceeds 5μs"


class TestTimers:
    """Test Timer service."""

    @pytest.fixture
    def timer_service(self):
        """Create timer service."""
        return Timers()

    def test_timer_creation(self, timer_service):
        """Test creating timer service."""
        assert timer_service is not None

    def test_register_timer(self, timer_service):
        """Test registering a timer."""
        fired = []

        def callback():
            fired.append(True)

        timer_id = timer_service.register_timer(
            timestamp=1000,
            callback=callback
        )

        assert timer_id is not None

    @pytest.mark.asyncio
    async def test_timer_firing(self, timer_service):
        """Test that timers fire at correct time."""
        fired = []

        def callback():
            fired.append(time_module.time())

        # Register timer for 100ms from now
        fire_time = int((time_module.time() + 0.1) * 1000)
        timer_service.register_timer(
            timestamp=fire_time,
            callback=callback
        )

        # Start timer service
        asyncio.create_task(timer_service.run())

        # Wait for timer to fire
        await asyncio.sleep(0.2)

        assert len(fired) == 1

    def test_cancel_timer(self, timer_service):
        """Test canceling a timer."""
        fired = []

        def callback():
            fired.append(True)

        timer_id = timer_service.register_timer(
            timestamp=1000,
            callback=callback
        )

        timer_service.cancel_timer(timer_id)

        # Timer should not fire
        timer_service.advance_to(timestamp=2000)

        assert len(fired) == 0

    def test_multiple_timers(self, timer_service):
        """Test multiple timers fire in order."""
        fired_order = []

        def callback(timer_id):
            fired_order.append(timer_id)

        # Register timers in non-sequential order
        timer_service.register_timer(timestamp=3000, callback=lambda: callback(3))
        timer_service.register_timer(timestamp=1000, callback=lambda: callback(1))
        timer_service.register_timer(timestamp=2000, callback=lambda: callback(2))

        # Advance time
        timer_service.advance_to(timestamp=4000)

        # Timers should fire in timestamp order
        assert fired_order == [1, 2, 3]


class TestEventTime:
    """Test EventTime utilities."""

    def test_extract_event_time(self):
        """Test extracting event time from message."""
        message = {
            "timestamp": 1609459200000,  # 2021-01-01 00:00:00 UTC
            "data": "test"
        }

        event_time = EventTime.extract(message, timestamp_field="timestamp")

        assert event_time == 1609459200000

    def test_extract_event_time_missing_field(self):
        """Test extracting event time when field is missing."""
        message = {"data": "test"}

        # Should fall back to processing time or raise error
        with pytest.raises(KeyError):
            EventTime.extract(message, timestamp_field="timestamp")

    def test_assign_event_time(self):
        """Test assigning event time to message."""
        message = {"data": "test"}

        EventTime.assign(message, timestamp=1609459200000, timestamp_field="timestamp")

        assert message["timestamp"] == 1609459200000


class TestTimeService:
    """Test TimeService coordination."""

    def test_time_service_creation(self):
        """Test creating time service."""
        service = TimeService()

        assert service is not None

    def test_processing_time(self):
        """Test getting current processing time."""
        service = TimeService()

        proc_time = service.processing_time()

        # Should be close to current time
        current_time = int(time_module.time() * 1000)
        assert abs(proc_time - current_time) < 100  # Within 100ms

    def test_event_time_mode(self):
        """Test setting event-time mode."""
        service = TimeService(time_characteristic="event_time")

        assert service.time_characteristic == "event_time"

    def test_ingestion_time_mode(self):
        """Test setting ingestion-time mode."""
        service = TimeService(time_characteristic="ingestion_time")

        assert service.time_characteristic == "ingestion_time"


@pytest.mark.benchmark
class TestTimePerformance:
    """Performance benchmarks for time operations."""

    def test_watermark_update_benchmark(self, benchmark):
        """Benchmark watermark updates."""
        tracker = WatermarkTracker(num_partitions=10)

        def update_watermark():
            tracker.update_watermark(partition_id=0, watermark=1000)

        benchmark(update_watermark)

    def test_watermark_get_benchmark(self, benchmark):
        """Benchmark getting global watermark."""
        tracker = WatermarkTracker(num_partitions=10)

        # Set up some watermarks
        for i in range(10):
            tracker.update_watermark(partition_id=i, watermark=1000)

        def get_watermark():
            return tracker.get_watermark()

        result = benchmark(get_watermark)
        assert result == 1000
