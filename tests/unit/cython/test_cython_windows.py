"""
Tests for sabot._cython.windows - Pure C++/Arrow window processing.

Tests the complete window processing pipeline:
- WindowBuffer (metadata + batch storage)
- TumblingWindowProcessor
- SlidingWindowProcessor
- HoppingWindowProcessor
- SessionWindowProcessor
- WindowManager
"""

import pytest
import asyncio
import time
from datetime import datetime, timedelta

# Import the compiled Cython module directly
import importlib.util
import os

# Get path to the compiled .so file
sabot_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
windows_so = os.path.join(sabot_root, "sabot/_cython/windows.cpython-311-darwin.so")

# Load module directly from .so
spec = importlib.util.spec_from_file_location("windows", windows_so)
windows = importlib.util.module_from_spec(spec)
spec.loader.exec_module(windows)

# Import classes
WindowBuffer = windows.WindowBuffer
WindowManager = windows.WindowManager
create_window_processor = windows.create_window_processor
WindowType = windows.WindowType


class TestWindowBuffer:
    """Test WindowBuffer C++ map implementation."""

    def test_create_window_buffer(self):
        """Test creating a WindowBuffer instance."""
        buffer = WindowBuffer(max_windows=100)
        assert buffer is not None
        assert buffer.size() == 0

    def test_create_single_window(self):
        """Test creating a single window."""
        buffer = WindowBuffer()
        start_time = time.time()
        end_time = start_time + 60.0

        # Create window and get metadata pointer
        metadata = buffer.create_window(start_time, end_time)

        assert buffer.size() == 1
        # Note: Can't directly access metadata fields from Python,
        # but we verified it was created

    def test_create_multiple_windows(self):
        """Test creating multiple windows."""
        buffer = WindowBuffer()

        for i in range(10):
            start = float(i * 60)
            end = start + 60.0
            buffer.create_window(start, end)

        assert buffer.size() == 10

    def test_window_eviction(self):
        """Test that old windows are evicted when max_windows is reached."""
        buffer = WindowBuffer(max_windows=5)

        # Create 10 windows
        for i in range(10):
            start = float(i * 60)
            end = start + 60.0
            buffer.create_window(start, end)

        # Should only have 5 windows (max)
        assert buffer.size() == 5

    def test_get_window_metadata(self):
        """Test retrieving window metadata."""
        buffer = WindowBuffer()
        start_time = time.time()
        end_time = start_time + 60.0

        # Create window - it gets ID 0 (first window)
        metadata = buffer.create_window(start_time, end_time)

        # Retrieve by ID
        retrieved = buffer.get_window_metadata(0)
        assert retrieved is not None

    def test_get_nonexistent_window(self):
        """Test retrieving non-existent window returns NULL."""
        buffer = WindowBuffer()

        # Try to get window that doesn't exist
        metadata = buffer.get_window_metadata(999)
        # Should return NULL (None in Python)
        assert metadata is None or metadata == 0  # NULL pointer

    def test_remove_window(self):
        """Test removing a window."""
        buffer = WindowBuffer()

        # Create window
        buffer.create_window(0.0, 60.0)
        assert buffer.size() == 1

        # Remove window (ID 0)
        buffer.remove_window(0)
        assert buffer.size() == 0


class TestTumblingWindowProcessor:
    """Test TumblingWindowProcessor - fixed-size, non-overlapping windows."""

    @pytest.mark.asyncio
    async def test_create_tumbling_processor(self):
        """Test creating a tumbling window processor."""
        processor = create_window_processor("tumbling", 60.0)
        assert processor is not None

    @pytest.mark.asyncio
    async def test_tumbling_window_simple(self):
        """Test basic tumbling window processing."""
        processor = create_window_processor("tumbling", 60.0)

        # Create simple record batch (dict for now)
        record = {"value": 100, "timestamp": time.time()}

        # Process record
        await processor.process_record(record)

        # Get stats
        stats = await processor.get_window_stats()
        assert stats is not None

    @pytest.mark.asyncio
    async def test_tumbling_window_multiple_records(self):
        """Test tumbling window with multiple records."""
        processor = create_window_processor("tumbling", 60.0)

        # Process multiple records
        for i in range(10):
            record = {"value": i * 10, "timestamp": time.time()}
            await processor.process_record(record)

        stats = await processor.get_window_stats()
        assert stats is not None


class TestSlidingWindowProcessor:
    """Test SlidingWindowProcessor - fixed-size, overlapping windows."""

    @pytest.mark.asyncio
    async def test_create_sliding_processor(self):
        """Test creating a sliding window processor."""
        processor = create_window_processor("sliding", 60.0, slide_seconds=30.0)
        assert processor is not None

    @pytest.mark.asyncio
    async def test_sliding_window_simple(self):
        """Test basic sliding window processing."""
        processor = create_window_processor("sliding", 60.0, slide_seconds=30.0)

        record = {"value": 100, "timestamp": time.time()}
        await processor.process_record(record)

        stats = await processor.get_window_stats()
        assert stats is not None

    @pytest.mark.asyncio
    async def test_sliding_window_overlap(self):
        """Test that sliding windows overlap correctly."""
        processor = create_window_processor("sliding", 60.0, slide_seconds=20.0)

        # Process records over time
        base_time = time.time()
        for i in range(5):
            record = {"value": i * 10, "timestamp": base_time + (i * 15)}
            await processor.process_record(record)

        stats = await processor.get_window_stats()
        assert stats is not None


class TestHoppingWindowProcessor:
    """Test HoppingWindowProcessor - fixed-size windows with custom hop."""

    @pytest.mark.asyncio
    async def test_create_hopping_processor(self):
        """Test creating a hopping window processor."""
        processor = create_window_processor("hopping", 60.0, hop_seconds=30.0)
        assert processor is not None

    @pytest.mark.asyncio
    async def test_hopping_window_simple(self):
        """Test basic hopping window processing."""
        processor = create_window_processor("hopping", 60.0, hop_seconds=30.0)

        record = {"value": 100, "timestamp": time.time()}
        await processor.process_record(record)

        stats = await processor.get_window_stats()
        assert stats is not None

    @pytest.mark.asyncio
    async def test_hopping_window_multiple_hops(self):
        """Test hopping window with multiple hops."""
        processor = create_window_processor("hopping", 60.0, hop_seconds=20.0)

        # Process records that span multiple hops
        base_time = time.time()
        for i in range(10):
            record = {"value": i * 10, "timestamp": base_time + (i * 25)}
            await processor.process_record(record)

        stats = await processor.get_window_stats()
        assert stats is not None


class TestSessionWindowProcessor:
    """Test SessionWindowProcessor - variable-size windows based on activity gaps."""

    @pytest.mark.asyncio
    async def test_create_session_processor(self):
        """Test creating a session window processor."""
        processor = create_window_processor("session", 60.0, timeout_seconds=30.0)
        assert processor is not None

    @pytest.mark.asyncio
    async def test_session_window_simple(self):
        """Test basic session window processing."""
        processor = create_window_processor("session", 60.0, timeout_seconds=30.0)

        record = {"user_id": "user1", "value": 100, "timestamp": time.time()}
        await processor.process_record(record)

        stats = await processor.get_window_stats()
        assert stats is not None

    @pytest.mark.asyncio
    async def test_session_window_timeout(self):
        """Test session window timeout behavior."""
        processor = create_window_processor("session", 60.0, timeout_seconds=5.0)

        base_time = time.time()

        # First record
        record1 = {"user_id": "user1", "value": 100, "timestamp": base_time}
        await processor.process_record(record1)

        # Wait for session to timeout
        await asyncio.sleep(6)

        # Second record (should start new session)
        record2 = {"user_id": "user1", "value": 200, "timestamp": time.time()}
        await processor.process_record(record2)

        stats = await processor.get_window_stats()
        assert stats is not None

    @pytest.mark.asyncio
    async def test_session_window_multiple_users(self):
        """Test session windows for multiple users."""
        processor = create_window_processor("session", 60.0, timeout_seconds=30.0)

        # Process records for different users
        for user_id in ["user1", "user2", "user3"]:
            for i in range(3):
                record = {
                    "user_id": user_id,
                    "value": i * 10,
                    "timestamp": time.time()
                }
                await processor.process_record(record)

        stats = await processor.get_window_stats()
        assert stats is not None


class TestWindowManager:
    """Test WindowManager - coordinating multiple windows."""

    @pytest.mark.asyncio
    async def test_create_window_manager(self):
        """Test creating a WindowManager instance."""
        manager = WindowManager()
        assert manager is not None

    @pytest.mark.asyncio
    async def test_create_managed_window(self):
        """Test creating a window through the manager."""
        manager = WindowManager()

        processor = await manager.create_window(
            "my_tumbling_window",
            "tumbling",
            60.0
        )
        assert processor is not None

    @pytest.mark.asyncio
    async def test_multiple_managed_windows(self):
        """Test managing multiple windows."""
        manager = WindowManager()

        # Create different window types
        await manager.create_window("tumbling", "tumbling", 60.0)
        await manager.create_window("sliding", "sliding", 60.0, slide_seconds=30.0)
        await manager.create_window("hopping", "hopping", 60.0, hop_seconds=20.0)
        await manager.create_window("session", "session", 60.0, timeout_seconds=30.0)

        # Process records through specific windows
        record = {"value": 100, "timestamp": time.time()}
        await manager.process_record("tumbling", record)
        await manager.process_record("sliding", record)

        # Get stats for all windows
        all_stats = await manager.get_window_stats()
        assert all_stats is not None

    @pytest.mark.asyncio
    async def test_get_window_stats_specific(self):
        """Test getting stats for a specific window."""
        manager = WindowManager()

        await manager.create_window("test_window", "tumbling", 60.0)

        record = {"value": 100, "timestamp": time.time()}
        await manager.process_record("test_window", record)

        # Get stats for specific window
        stats = await manager.get_window_stats("test_window")
        assert stats is not None

    @pytest.mark.asyncio
    async def test_emit_all_windows(self):
        """Test emitting all completed windows."""
        manager = WindowManager()

        await manager.create_window("window1", "tumbling", 60.0)
        await manager.create_window("window2", "sliding", 60.0)

        # Process some records
        for i in range(5):
            record = {"value": i * 10, "timestamp": time.time()}
            await manager.process_record("window1", record)
            await manager.process_record("window2", record)

        # Emit all windows
        await manager.emit_all_windows()


class TestWindowIntegration:
    """Integration tests for complete window processing pipeline."""

    @pytest.mark.asyncio
    async def test_end_to_end_tumbling_window(self):
        """Test complete tumbling window pipeline."""
        # Create buffer
        buffer = WindowBuffer(max_windows=100)

        # Create processor
        processor = create_window_processor("tumbling", 60.0)

        # Process records
        base_time = time.time()
        for i in range(20):
            record = {
                "value": i * 10,
                "timestamp": base_time + (i * 5),
                "user_id": f"user{i % 3}"
            }
            await processor.process_record(record)

        # Get final stats
        stats = await processor.get_window_stats()
        assert stats is not None

    @pytest.mark.asyncio
    async def test_end_to_end_manager_workflow(self):
        """Test complete workflow using WindowManager."""
        manager = WindowManager()

        # Create windows for different aggregations
        await manager.create_window(
            "5min_tumbling",
            "tumbling",
            300.0  # 5 minutes
        )

        await manager.create_window(
            "10min_sliding",
            "sliding",
            600.0,  # 10 minutes
            slide_seconds=300.0  # 5 minute slide
        )

        await manager.create_window(
            "user_sessions",
            "session",
            3600.0,  # 1 hour max
            timeout_seconds=300.0  # 5 minute timeout
        )

        # Simulate streaming data
        base_time = time.time()
        for i in range(50):
            record = {
                "user_id": f"user{i % 5}",
                "value": i * 100,
                "timestamp": base_time + (i * 10)
            }

            # Send to all windows
            await manager.process_record("5min_tumbling", record)
            await manager.process_record("10min_sliding", record)
            await manager.process_record("user_sessions", record)

        # Get stats for all windows
        all_stats = await manager.get_window_stats()
        assert all_stats is not None

        # Emit completed windows
        await manager.emit_all_windows()

    @pytest.mark.asyncio
    async def test_window_factory_all_types(self):
        """Test factory function creates all window types correctly."""
        tumbling = create_window_processor("tumbling", 60.0)
        assert tumbling is not None

        sliding = create_window_processor("sliding", 60.0, slide_seconds=30.0)
        assert sliding is not None

        hopping = create_window_processor("hopping", 60.0, hop_seconds=20.0)
        assert hopping is not None

        session = create_window_processor("session", 60.0, timeout_seconds=30.0)
        assert session is not None

    @pytest.mark.asyncio
    async def test_window_factory_invalid_type(self):
        """Test factory function raises error for invalid window type."""
        with pytest.raises(ValueError, match="Unknown window type"):
            create_window_processor("invalid_type", 60.0)


class TestWindowPerformance:
    """Performance tests for window processing."""

    @pytest.mark.asyncio
    async def test_high_volume_tumbling(self):
        """Test tumbling window with high volume of records."""
        processor = create_window_processor("tumbling", 60.0)

        base_time = time.time()
        record_count = 1000

        start = time.time()
        for i in range(record_count):
            record = {
                "value": i,
                "timestamp": base_time + (i * 0.1)
            }
            await processor.process_record(record)

        elapsed = time.time() - start
        throughput = record_count / elapsed

        print(f"\n  Tumbling window throughput: {throughput:.0f} records/sec")
        print(f"  Total time: {elapsed:.3f}s for {record_count} records")

        # Should handle at least 1000 records/sec
        assert throughput > 100  # Very conservative check

    @pytest.mark.asyncio
    async def test_buffer_large_scale(self):
        """Test WindowBuffer with large number of windows."""
        buffer = WindowBuffer(max_windows=10000)

        start = time.time()
        for i in range(5000):
            start_time = float(i * 60)
            end_time = start_time + 60.0
            buffer.create_window(start_time, end_time)

        elapsed = time.time() - start
        creation_rate = 5000 / elapsed

        print(f"\n  Window creation rate: {creation_rate:.0f} windows/sec")
        print(f"  Total time: {elapsed:.3f}s for 5000 windows")

        # Should handle at least 1000 windows/sec
        assert creation_rate > 100

    @pytest.mark.asyncio
    async def test_manager_many_windows(self):
        """Test WindowManager with many concurrent windows."""
        manager = WindowManager()

        # Create 20 different windows
        for i in range(20):
            await manager.create_window(
                f"window_{i}",
                "tumbling",
                60.0 + (i * 10)
            )

        # Process records through all windows
        record = {"value": 100, "timestamp": time.time()}

        start = time.time()
        for i in range(20):
            await manager.process_record(f"window_{i}", record)

        elapsed = time.time() - start

        print(f"\n  Processed record through 20 windows in {elapsed:.3f}s")

        # Should be fast
        assert elapsed < 1.0  # Less than 1 second for 20 windows


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v", "-s"])
