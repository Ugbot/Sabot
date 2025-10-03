"""Test Arrow zero-copy integration."""

import pytest
import numpy as np
import pyarrow as pa

from sabot import cyarrow
from sabot.cyarrow import (
    compute_window_ids,
    hash_join_batches,
    sort_and_take,
    ArrowComputeEngine,
    USING_ZERO_COPY,
)


class TestArrowIntegration:
    """Test zero-copy Arrow integration."""

    def test_zero_copy_enabled(self):
        """Verify zero-copy mode is enabled."""
        assert USING_ZERO_COPY, "Zero-copy Arrow should be enabled"
        assert arrow.USING_INTERNAL, "Should use internal implementation"
        assert not arrow.USING_EXTERNAL, "Should not use external-only mode"

    def test_window_computation(self):
        """Test zero-copy window computation."""
        # Create test batch
        data = {
            'timestamp': [1000, 2500, 3100, 4200, 5800],
            'value': [10, 20, 30, 40, 50]
        }
        batch = pa.RecordBatch.from_pydict(data)

        # Compute windows (1000ms windows)
        windowed = compute_window_ids(batch, 'timestamp', 1000)

        assert windowed.num_rows == 5
        assert 'window_id' in windowed.column_names

        # Verify window assignments
        window_ids = windowed.column('window_id').to_pylist()
        expected = [1000, 2000, 3000, 4000, 5000]
        assert window_ids == expected

    def test_filter_operation(self):
        """Test zero-copy filter."""
        data = {'value': [10, 20, 30, 40, 50]}
        batch = pa.RecordBatch.from_pydict(data)

        engine = ArrowComputeEngine()
        filtered = engine.filter_batch(batch, 'value > 25')

        assert filtered.num_rows == 3
        values = filtered.column('value').to_pylist()
        assert values == [30, 40, 50]

    def test_hash_join(self):
        """Test zero-copy hash join."""
        left_data = {
            'key': [1, 2, 3, 4],
            'left_value': ['a', 'b', 'c', 'd']
        }
        right_data = {
            'key': [2, 3, 4, 5],
            'right_value': ['B', 'C', 'D', 'E']
        }

        left = pa.RecordBatch.from_pydict(left_data)
        right = pa.RecordBatch.from_pydict(right_data)

        joined = hash_join_batches(left, right, 'key', 'key', 'inner')

        # Inner join should match on keys 2, 3, 4
        assert joined.num_rows == 3
        assert 'left_value' in joined.column_names
        assert 'right_value' in joined.column_names

    def test_sort_and_take(self):
        """Test zero-copy sorting and slicing."""
        data = {
            'value': [50, 10, 40, 20, 30],
            'id': [5, 1, 4, 2, 3]
        }
        batch = pa.RecordBatch.from_pydict(data)

        # Sort ascending and take top 3
        sorted_batch = sort_and_take(batch, [('value', 'ascending')], limit=3)

        assert sorted_batch.num_rows == 3
        values = sorted_batch.column('value').to_pylist()
        assert values == [10, 20, 30]

    def test_performance_window_computation(self):
        """Test window computation performance."""
        import time

        # 100K rows
        n = 100_000
        data = {
            'timestamp': np.arange(n, dtype=np.int64) * 1000,
            'value': np.random.rand(n)
        }
        batch = pa.RecordBatch.from_pydict(data)

        start = time.perf_counter()
        windowed = compute_window_ids(batch, 'timestamp', 10000)
        end = time.perf_counter()

        elapsed_ms = (end - start) * 1000
        throughput = n / (end - start) / 1e6  # M rows/sec

        assert windowed.num_rows == n
        assert throughput > 10, f"Throughput {throughput:.1f}M rows/sec below 10M"
        print(f"\nWindow computation: {throughput:.1f}M rows/sec ({elapsed_ms:.2f}ms for {n:,} rows)")

    def test_performance_filter(self):
        """Test filter performance."""
        import time

        # 100K rows
        n = 100_000
        data = {'value': np.random.rand(n)}
        batch = pa.RecordBatch.from_pydict(data)

        engine = ArrowComputeEngine()

        start = time.perf_counter()
        filtered = engine.filter_batch(batch, 'value > 0.5')
        end = time.perf_counter()

        elapsed_ms = (end - start) * 1000
        throughput = n / (end - start) / 1e6

        assert throughput > 10, f"Throughput {throughput:.1f}M rows/sec below 10M"
        print(f"\nFilter operation: {throughput:.1f}M rows/sec ({elapsed_ms:.2f}ms for {n:,} rows)")

    def test_pyarrow_compatibility(self):
        """Test compatibility with PyArrow operations."""
        # Create batch using standard PyArrow
        data = {'x': [1, 2, 3], 'y': [4, 5, 6]}
        batch = pa.RecordBatch.from_pydict(data)

        # Use with our zero-copy functions
        windowed = compute_window_ids(batch, 'x', 1)

        assert windowed.num_rows == 3
        assert isinstance(windowed, pa.RecordBatch)

    def test_large_batch_processing(self):
        """Test processing large batches."""
        # 1M rows
        n = 1_000_000
        data = {
            'timestamp': np.arange(n, dtype=np.int64),
            'value': np.random.rand(n)
        }
        batch = pa.RecordBatch.from_pydict(data)

        # Should handle without error
        windowed = compute_window_ids(batch, 'timestamp', 1000)
        assert windowed.num_rows == n

        engine = ArrowComputeEngine()
        filtered = engine.filter_batch(windowed, 'value > 0.5')
        assert 450_000 < filtered.num_rows < 550_000  # ~50% filtered


class TestArrowComputeEngine:
    """Test ArrowComputeEngine operations."""

    @pytest.fixture
    def engine(self):
        return ArrowComputeEngine()

    def test_engine_creation(self, engine):
        """Test engine can be created."""
        assert engine is not None

    def test_greater_than_filter(self, engine):
        """Test greater than filter."""
        batch = pa.RecordBatch.from_pydict({'x': [1, 2, 3, 4, 5]})
        result = engine.filter_batch(batch, 'x > 3')
        assert result.num_rows == 2

    def test_less_than_filter(self, engine):
        """Test less than filter."""
        batch = pa.RecordBatch.from_pydict({'x': [1, 2, 3, 4, 5]})
        result = engine.filter_batch(batch, 'x < 3')
        assert result.num_rows == 2

    def test_hash_join_inner(self, engine):
        """Test inner join."""
        left = pa.RecordBatch.from_pydict({'key': [1, 2, 3], 'left_val': [10, 20, 30]})
        right = pa.RecordBatch.from_pydict({'key': [2, 3, 4], 'right_val': [200, 300, 400]})

        result = engine.hash_join(left, right, 'key', 'key', 'inner')

        # Should match on keys 2 and 3
        assert result.num_rows == 2
