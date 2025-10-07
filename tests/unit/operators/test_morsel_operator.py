"""
Test MorselDrivenOperator - Wrapper for morsel-driven parallel execution.

Verifies:
- Should-use-morsel heuristics (small batch bypass)
- Parallel processing of large batches
- Statistics collection
- Integration with wrapped operators
"""

import pytest
import pyarrow as pa
import pyarrow.compute as pc
from sabot._cython.operators.transform import CythonMapOperator, CythonFilterOperator
from sabot._cython.operators.morsel_operator import MorselDrivenOperator


class TestMorselHeuristics:
    """Test should_use_morsel_execution() heuristics."""

    def test_small_batch_bypasses_morsel_execution(self):
        """Batches < 10K rows bypass morsel execution."""
        def double_x(batch):
            return batch.set_column(0, 'x', pc.multiply(batch.column('x'), 2))

        map_op = CythonMapOperator(source=None, map_func=double_x)
        morsel_op = MorselDrivenOperator(map_op, num_workers=4)

        # Small batch (< 10K rows)
        small_batch = pa.RecordBatch.from_pydict({'x': list(range(100))})

        assert not morsel_op.should_use_morsel_execution(small_batch)

    def test_large_batch_uses_morsel_execution(self):
        """Batches >= 10K rows use morsel execution."""
        def double_x(batch):
            return batch.set_column(0, 'x', pc.multiply(batch.column('x'), 2))

        map_op = CythonMapOperator(source=None, map_func=double_x)
        morsel_op = MorselDrivenOperator(map_op, num_workers=4)

        # Large batch (>= 10K rows)
        large_batch = pa.RecordBatch.from_pydict({'x': list(range(10000))})

        assert morsel_op.should_use_morsel_execution(large_batch)

    def test_disabled_morsel_execution(self):
        """Disabled morsel execution bypasses all batches."""
        def double_x(batch):
            return batch.set_column(0, 'x', pc.multiply(batch.column('x'), 2))

        map_op = CythonMapOperator(source=None, map_func=double_x)
        morsel_op = MorselDrivenOperator(map_op, num_workers=4, enabled=False)

        large_batch = pa.RecordBatch.from_pydict({'x': list(range(10000))})

        assert not morsel_op.should_use_morsel_execution(large_batch)

    def test_single_worker_bypasses_morsel_execution(self):
        """Single worker bypasses morsel execution."""
        def double_x(batch):
            return batch.set_column(0, 'x', pc.multiply(batch.column('x'), 2))

        map_op = CythonMapOperator(source=None, map_func=double_x)
        morsel_op = MorselDrivenOperator(map_op, num_workers=1)

        large_batch = pa.RecordBatch.from_pydict({'x': list(range(10000))})

        assert not morsel_op.should_use_morsel_execution(large_batch)

    def test_none_batch_bypasses_morsel_execution(self):
        """None batch bypasses morsel execution."""
        def double_x(batch):
            return batch.set_column(0, 'x', pc.multiply(batch.column('x'), 2))

        map_op = CythonMapOperator(source=None, map_func=double_x)
        morsel_op = MorselDrivenOperator(map_op, num_workers=4)

        assert not morsel_op.should_use_morsel_execution(None)


class TestSmallBatchProcessing:
    """Test processing of small batches (direct passthrough)."""

    def test_small_batch_direct_processing(self):
        """Small batches are processed directly without morsel overhead."""
        def double_x(batch):
            return batch.set_column(0, 'x', pc.multiply(batch.column('x'), 2))

        map_op = CythonMapOperator(source=None, map_func=double_x)
        morsel_op = MorselDrivenOperator(map_op, num_workers=4)

        small_batch = pa.RecordBatch.from_pydict({'x': list(range(100))})
        result = morsel_op.process_batch(small_batch)

        assert result.num_rows == 100
        assert result.column('x').to_pylist() == [i * 2 for i in range(100)]

    def test_empty_batch_processing(self):
        """Empty batches are handled gracefully."""
        def double_x(batch):
            return batch.set_column(0, 'x', pc.multiply(batch.column('x'), 2))

        map_op = CythonMapOperator(source=None, map_func=double_x)
        morsel_op = MorselDrivenOperator(map_op, num_workers=4)

        empty_batch = pa.RecordBatch.from_pydict({'x': []})
        result = morsel_op.process_batch(empty_batch)

        assert result.num_rows == 0


class TestLargeBatchProcessing:
    """Test processing of large batches (morsel parallelism)."""

    @pytest.mark.asyncio
    async def test_large_batch_morsel_processing(self):
        """Large batches are processed with morsel parallelism."""
        def double_x(batch):
            return batch.set_column(0, 'x', pc.multiply(batch.column('x'), 2))

        map_op = CythonMapOperator(source=None, map_func=double_x)
        morsel_op = MorselDrivenOperator(map_op, num_workers=4, morsel_size_kb=64)

        # Large batch (50K rows)
        large_batch = pa.RecordBatch.from_pydict({'x': list(range(50000))})

        result = await morsel_op._async_process_with_morsels(large_batch)

        assert result.num_rows == 50000
        assert result.column('x').to_pylist() == [i * 2 for i in range(50000)]

    @pytest.mark.asyncio
    async def test_large_batch_with_filter(self):
        """Large batches work with filter operations."""
        def filter_even(batch):
            return pc.equal(pc.modulo(batch.column('x'), 2), 0)

        filter_op = CythonFilterOperator(source=None, predicate=filter_even)
        morsel_op = MorselDrivenOperator(filter_op, num_workers=4)

        large_batch = pa.RecordBatch.from_pydict({'x': list(range(10000))})

        result = await morsel_op._async_process_with_morsels(large_batch)

        assert result.num_rows == 5000  # Half are even
        assert all(x % 2 == 0 for x in result.column('x').to_pylist())

    @pytest.mark.asyncio
    async def test_large_batch_with_complex_transform(self):
        """Large batches work with complex transformations."""
        def complex_transform(batch):
            return batch.append_column(
                'y', pc.add(pc.multiply(batch.column('x'), 2), 100)
            ).append_column(
                'z', pc.power(batch.column('x'), 2)
            )

        map_op = CythonMapOperator(source=None, map_func=complex_transform)
        morsel_op = MorselDrivenOperator(map_op, num_workers=4)

        large_batch = pa.RecordBatch.from_pydict({'x': list(range(20000))})

        result = await morsel_op._async_process_with_morsels(large_batch)

        assert result.num_rows == 20000
        assert 'y' in result.schema.names
        assert 'z' in result.schema.names

        # Check a few values
        assert result.column('y').to_pylist()[0] == 100  # 0 * 2 + 100
        assert result.column('y').to_pylist()[10] == 120  # 10 * 2 + 100
        assert result.column('z').to_pylist()[10] == 100  # 10^2


class TestStatisticsCollection:
    """Test statistics collection from morsel processing."""

    def test_stats_before_initialization(self):
        """Stats before initialization show not_initialized."""
        def double_x(batch):
            return batch.set_column(0, 'x', pc.multiply(batch.column('x'), 2))

        map_op = CythonMapOperator(source=None, map_func=double_x)
        morsel_op = MorselDrivenOperator(map_op, num_workers=4)

        stats = morsel_op.get_stats()

        assert stats['morsel_execution'] == 'not_initialized'

    @pytest.mark.asyncio
    async def test_stats_after_processing(self):
        """Stats after processing show morsel metrics."""
        def double_x(batch):
            return batch.set_column(0, 'x', pc.multiply(batch.column('x'), 2))

        map_op = CythonMapOperator(source=None, map_func=double_x)
        morsel_op = MorselDrivenOperator(map_op, num_workers=4)

        large_batch = pa.RecordBatch.from_pydict({'x': list(range(50000))})
        result = await morsel_op._async_process_with_morsels(large_batch)

        stats = morsel_op.get_stats()

        assert 'num_workers' in stats
        assert stats['num_workers'] == 4
        assert 'total_morsels_created' in stats
        assert stats['total_morsels_created'] > 0


class TestWrappedOperatorIntegration:
    """Test integration with wrapped operators."""

    def test_wrapped_operator_metadata_preserved(self):
        """Wrapped operator metadata is preserved."""
        schema = pa.schema([('x', pa.int64()), ('y', pa.int64())])

        def identity(batch):
            return batch

        map_op = CythonMapOperator(source=None, map_func=identity)
        map_op._schema = schema
        map_op._stateful = True
        map_op._key_columns = ['x']

        morsel_op = MorselDrivenOperator(map_op, num_workers=4)

        assert morsel_op.get_schema() == schema
        assert morsel_op.is_stateful()
        assert morsel_op.get_partition_keys() == ['x']

    def test_wrapped_operator_source_preserved(self):
        """Wrapped operator source is preserved."""
        source_batches = [
            pa.RecordBatch.from_pydict({'x': [1, 2, 3]}),
            pa.RecordBatch.from_pydict({'x': [4, 5, 6]}),
        ]

        def identity(batch):
            return batch

        map_op = CythonMapOperator(source=iter(source_batches), map_func=identity)
        morsel_op = MorselDrivenOperator(map_op, num_workers=4)

        # Source should be accessible
        assert morsel_op._source is not None


class TestConfigurationOptions:
    """Test configuration options."""

    def test_custom_morsel_size(self):
        """Custom morsel size is applied."""
        def double_x(batch):
            return batch.set_column(0, 'x', pc.multiply(batch.column('x'), 2))

        map_op = CythonMapOperator(source=None, map_func=double_x)
        morsel_op = MorselDrivenOperator(
            map_op,
            num_workers=4,
            morsel_size_kb=128  # Custom size
        )

        assert morsel_op._morsel_size_kb == 128

    def test_auto_detect_workers(self):
        """Worker count of 0 triggers auto-detection."""
        def double_x(batch):
            return batch.set_column(0, 'x', pc.multiply(batch.column('x'), 2))

        map_op = CythonMapOperator(source=None, map_func=double_x)
        morsel_op = MorselDrivenOperator(
            map_op,
            num_workers=0  # Auto-detect
        )

        # Should bypass morsel execution (num_workers effectively 0 or 1)
        large_batch = pa.RecordBatch.from_pydict({'x': list(range(10000))})
        assert not morsel_op.should_use_morsel_execution(large_batch)

    def test_parallelism_hint_from_workers(self):
        """Parallelism hint reflects worker count."""
        def double_x(batch):
            return batch.set_column(0, 'x', pc.multiply(batch.column('x'), 2))

        map_op = CythonMapOperator(source=None, map_func=double_x)
        morsel_op = MorselDrivenOperator(map_op, num_workers=8)

        assert morsel_op.get_parallelism_hint() == 8


class TestCorrectnessVerification:
    """Verify parallel results match sequential results."""

    @pytest.mark.asyncio
    async def test_parallel_matches_sequential_map(self):
        """Parallel map results match sequential map."""
        def complex_func(batch):
            return batch.set_column(
                0, 'x',
                pc.add(pc.multiply(batch.column('x'), 3), 7)
            )

        map_op = CythonMapOperator(source=None, map_func=complex_func)
        morsel_op = MorselDrivenOperator(map_op, num_workers=4)

        batch = pa.RecordBatch.from_pydict({'x': list(range(30000))})

        # Sequential
        sequential_result = map_op.process_batch(batch)

        # Parallel
        parallel_result = await morsel_op._async_process_with_morsels(batch)

        # Compare
        assert sequential_result.num_rows == parallel_result.num_rows
        assert sequential_result.column('x').to_pylist() == parallel_result.column('x').to_pylist()

    @pytest.mark.asyncio
    async def test_parallel_matches_sequential_filter(self):
        """Parallel filter results match sequential filter."""
        def filter_divisible_by_7(batch):
            return pc.equal(pc.modulo(batch.column('x'), 7), 0)

        filter_op = CythonFilterOperator(source=None, predicate=filter_divisible_by_7)
        morsel_op = MorselDrivenOperator(filter_op, num_workers=4)

        batch = pa.RecordBatch.from_pydict({'x': list(range(20000))})

        # Sequential
        sequential_result = filter_op.process_batch(batch)

        # Parallel
        parallel_result = await morsel_op._async_process_with_morsels(batch)

        # Compare
        assert sequential_result.num_rows == parallel_result.num_rows
        assert sequential_result.column('x').to_pylist() == parallel_result.column('x').to_pylist()


class TestErrorHandling:
    """Test error handling in morsel processing."""

    def test_process_batch_with_none(self):
        """process_batch handles None gracefully."""
        def double_x(batch):
            return batch.set_column(0, 'x', pc.multiply(batch.column('x'), 2))

        map_op = CythonMapOperator(source=None, map_func=double_x)
        morsel_op = MorselDrivenOperator(map_op, num_workers=4)

        result = morsel_op.process_batch(None)

        assert result is None


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-s'])
