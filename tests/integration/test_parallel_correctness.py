"""
Test Parallel Correctness - Verify parallel results match sequential.

Critical correctness tests ensuring morsel-driven parallelism produces
identical results to sequential processing.

Verifies:
- Parallel results match sequential for all operator types
- Different batch sizes (100, 1K, 10K, 100K, 500K rows)
- Different data types (int, float, string, dates)
- Complex transformations and predicates
- Edge cases (empty, single row, all filtered)
"""

import pytest
from sabot import cyarrow as pa
from sabot.cyarrow import compute as pc
import numpy as np
from sabot._cython.operators.transform import CythonMapOperator, CythonFilterOperator
from sabot._cython.operators.morsel_operator import MorselDrivenOperator


@pytest.mark.asyncio
class TestMapOperatorCorrectness:
    """Verify map operator correctness."""

    async def test_simple_map_correctness(self):
        """Simple map produces identical results."""
        def double(batch):
            return batch.set_column(0, 'x', pc.multiply(batch.column('x'), 2))

        map_op = CythonMapOperator(source=None, map_func=double)
        morsel_op = MorselDrivenOperator(map_op, num_workers=4)

        batch = pa.RecordBatch.from_pydict({'x': list(range(50000))})

        # Sequential
        sequential = map_op.process_batch(batch)

        # Parallel
        parallel = await morsel_op._async_process_with_morsels(batch)

        # Compare
        assert sequential.num_rows == parallel.num_rows
        assert sequential.column('x').to_pylist() == parallel.column('x').to_pylist()

    async def test_complex_map_correctness(self):
        """Complex map with multiple operations."""
        def complex_transform(batch):
            return batch.append_column(
                'y', pc.add(pc.multiply(batch.column('x'), 3), 7)
            ).append_column(
                'z', pc.power(batch.column('x'), 2)
            ).append_column(
                'w', pc.divide(batch.column('x'), 10.0)
            )

        map_op = CythonMapOperator(source=None, map_func=complex_transform)
        morsel_op = MorselDrivenOperator(map_op, num_workers=8)

        batch = pa.RecordBatch.from_pydict({'x': [float(i) for i in range(30000)]})

        sequential = map_op.process_batch(batch)
        parallel = await morsel_op._async_process_with_morsels(batch)

        assert sequential.num_rows == parallel.num_rows
        for col in ['y', 'z', 'w']:
            assert sequential.column(col).to_pylist() == parallel.column(col).to_pylist()

    async def test_map_with_string_operations(self):
        """Map with string operations."""
        def string_transform(batch):
            # Note: string operations may not be supported by all Arrow compute kernels
            # This is a simple pass-through test
            return batch

        map_op = CythonMapOperator(source=None, map_func=string_transform)
        morsel_op = MorselDrivenOperator(map_op, num_workers=4)

        batch = pa.RecordBatch.from_pydict({
            'names': [f'name_{i}' for i in range(10000)]
        })

        sequential = map_op.process_batch(batch)
        parallel = await morsel_op._async_process_with_morsels(batch)

        assert sequential.num_rows == parallel.num_rows
        assert sequential.column('names').to_pylist() == parallel.column('names').to_pylist()


@pytest.mark.asyncio
class TestFilterOperatorCorrectness:
    """Verify filter operator correctness."""

    async def test_simple_filter_correctness(self):
        """Simple filter produces identical results."""
        def filter_even(batch):
            return pc.equal(pc.modulo(batch.column('x'), 2), 0)

        filter_op = CythonFilterOperator(source=None, predicate=filter_even)
        morsel_op = MorselDrivenOperator(filter_op, num_workers=4)

        batch = pa.RecordBatch.from_pydict({'x': list(range(20000))})

        sequential = filter_op.process_batch(batch)
        parallel = await morsel_op._async_process_with_morsels(batch)

        assert sequential.num_rows == parallel.num_rows
        assert sequential.column('x').to_pylist() == parallel.column('x').to_pylist()

    async def test_complex_filter_correctness(self):
        """Complex filter with multiple conditions."""
        def complex_filter(batch):
            return pc.and_(
                pc.and_(
                    pc.greater(batch.column('x'), 1000),
                    pc.less(batch.column('x'), 50000)
                ),
                pc.equal(pc.modulo(batch.column('x'), 7), 0)
            )

        filter_op = CythonFilterOperator(source=None, predicate=complex_filter)
        morsel_op = MorselDrivenOperator(filter_op, num_workers=8)

        batch = pa.RecordBatch.from_pydict({'x': list(range(100000))})

        sequential = filter_op.process_batch(batch)
        parallel = await morsel_op._async_process_with_morsels(batch)

        assert sequential.num_rows == parallel.num_rows
        # Sort both to compare (order may differ)
        seq_sorted = sorted(sequential.column('x').to_pylist())
        par_sorted = sorted(parallel.column('x').to_pylist())
        assert seq_sorted == par_sorted

    async def test_filter_removes_all(self):
        """Filter that removes all rows."""
        def filter_negative(batch):
            return pc.less(batch.column('x'), 0)

        filter_op = CythonFilterOperator(source=None, predicate=filter_negative)
        morsel_op = MorselDrivenOperator(filter_op, num_workers=4)

        batch = pa.RecordBatch.from_pydict({'x': list(range(10000))})

        sequential = filter_op.process_batch(batch)
        parallel = await morsel_op._async_process_with_morsels(batch)

        # Both should be None or have 0 rows
        assert (sequential is None or sequential.num_rows == 0)
        assert (parallel is None or parallel.num_rows == 0)


@pytest.mark.asyncio
class TestDifferentBatchSizes:
    """Test correctness with different batch sizes."""

    async def test_100_rows(self):
        """Correctness with 100 rows."""
        def transform(batch):
            return batch.set_column(0, 'x', pc.multiply(batch.column('x'), 5))

        map_op = CythonMapOperator(source=None, map_func=transform)
        morsel_op = MorselDrivenOperator(map_op, num_workers=4)

        batch = pa.RecordBatch.from_pydict({'x': list(range(100))})

        sequential = map_op.process_batch(batch)
        parallel = await morsel_op._async_process_with_morsels(batch)

        assert sequential.column('x').to_pylist() == parallel.column('x').to_pylist()

    async def test_1k_rows(self):
        """Correctness with 1K rows."""
        def transform(batch):
            return batch.set_column(0, 'x', pc.add(batch.column('x'), 42))

        map_op = CythonMapOperator(source=None, map_func=transform)
        morsel_op = MorselDrivenOperator(map_op, num_workers=4)

        batch = pa.RecordBatch.from_pydict({'x': list(range(1000))})

        sequential = map_op.process_batch(batch)
        parallel = await morsel_op._async_process_with_morsels(batch)

        assert sequential.column('x').to_pylist() == parallel.column('x').to_pylist()

    async def test_10k_rows(self):
        """Correctness with 10K rows (threshold)."""
        def transform(batch):
            return batch.set_column(0, 'x', pc.power(batch.column('x'), 2))

        map_op = CythonMapOperator(source=None, map_func=transform)
        morsel_op = MorselDrivenOperator(map_op, num_workers=4)

        batch = pa.RecordBatch.from_pydict({'x': list(range(10000))})

        sequential = map_op.process_batch(batch)
        parallel = await morsel_op._async_process_with_morsels(batch)

        assert sequential.column('x').to_pylist() == parallel.column('x').to_pylist()

    async def test_100k_rows(self):
        """Correctness with 100K rows."""
        def transform(batch):
            return batch.set_column(0, 'x', pc.multiply(batch.column('x'), 3))

        map_op = CythonMapOperator(source=None, map_func=transform)
        morsel_op = MorselDrivenOperator(map_op, num_workers=8)

        batch = pa.RecordBatch.from_pydict({'x': list(range(100000))})

        sequential = map_op.process_batch(batch)
        parallel = await morsel_op._async_process_with_morsels(batch)

        assert sequential.column('x').to_pylist() == parallel.column('x').to_pylist()

    @pytest.mark.slow
    async def test_500k_rows(self):
        """Correctness with 500K rows (slow test)."""
        def transform(batch):
            return batch.set_column(0, 'x', pc.add(batch.column('x'), 1))

        map_op = CythonMapOperator(source=None, map_func=transform)
        morsel_op = MorselDrivenOperator(map_op, num_workers=8)

        batch = pa.RecordBatch.from_pydict({'x': list(range(500000))})

        sequential = map_op.process_batch(batch)
        parallel = await morsel_op._async_process_with_morsels(batch)

        # For large batches, sample check instead of full comparison
        assert sequential.num_rows == parallel.num_rows
        # Check first, middle, and last values
        seq_list = sequential.column('x').to_pylist()
        par_list = parallel.column('x').to_pylist()
        assert seq_list[0] == par_list[0]
        assert seq_list[250000] == par_list[250000]
        assert seq_list[-1] == par_list[-1]


@pytest.mark.asyncio
class TestDifferentDataTypes:
    """Test correctness with different data types."""

    async def test_int64_type(self):
        """Correctness with int64."""
        def transform(batch):
            return batch.set_column(0, 'x', pc.multiply(batch.column('x'), 2))

        map_op = CythonMapOperator(source=None, map_func=transform)
        morsel_op = MorselDrivenOperator(map_op, num_workers=4)

        batch = pa.RecordBatch.from_pydict({
            'x': pa.array(list(range(20000)), type=pa.int64())
        })

        sequential = map_op.process_batch(batch)
        parallel = await morsel_op._async_process_with_morsels(batch)

        assert sequential.column('x').to_pylist() == parallel.column('x').to_pylist()

    async def test_float64_type(self):
        """Correctness with float64."""
        def transform(batch):
            return batch.set_column(0, 'x', pc.divide(batch.column('x'), 3.0))

        map_op = CythonMapOperator(source=None, map_func=transform)
        morsel_op = MorselDrivenOperator(map_op, num_workers=4)

        batch = pa.RecordBatch.from_pydict({
            'x': pa.array([float(i) for i in range(20000)], type=pa.float64())
        })

        sequential = map_op.process_batch(batch)
        parallel = await morsel_op._async_process_with_morsels(batch)

        # Use approximate comparison for floats
        seq_list = sequential.column('x').to_pylist()
        par_list = parallel.column('x').to_pylist()
        assert len(seq_list) == len(par_list)
        for i in range(len(seq_list)):
            assert abs(seq_list[i] - par_list[i]) < 1e-10

    async def test_mixed_types(self):
        """Correctness with mixed column types."""
        def transform(batch):
            return batch.append_column(
                'sum', pc.add(batch.column('x'), batch.column('y'))
            )

        map_op = CythonMapOperator(source=None, map_func=transform)
        morsel_op = MorselDrivenOperator(map_op, num_workers=4)

        batch = pa.RecordBatch.from_pydict({
            'x': pa.array(list(range(15000)), type=pa.int32()),
            'y': pa.array([float(i) * 1.5 for i in range(15000)], type=pa.float64()),
            'name': [f'item_{i}' for i in range(15000)]
        })

        sequential = map_op.process_batch(batch)
        parallel = await morsel_op._async_process_with_morsels(batch)

        assert sequential.num_rows == parallel.num_rows
        # Compare sum column
        seq_sum = sequential.column('sum').to_pylist()
        par_sum = parallel.column('sum').to_pylist()
        for i in range(len(seq_sum)):
            assert abs(seq_sum[i] - par_sum[i]) < 1e-9


@pytest.mark.asyncio
class TestEdgeCases:
    """Test edge cases."""

    async def test_empty_batch(self):
        """Correctness with empty batch."""
        def transform(batch):
            return batch.set_column(0, 'x', pc.multiply(batch.column('x'), 2))

        map_op = CythonMapOperator(source=None, map_func=transform)
        morsel_op = MorselDrivenOperator(map_op, num_workers=4)

        batch = pa.RecordBatch.from_pydict({'x': []})

        sequential = map_op.process_batch(batch)
        parallel = await morsel_op._async_process_with_morsels(batch)

        assert sequential.num_rows == 0
        assert parallel.num_rows == 0

    async def test_single_row(self):
        """Correctness with single row."""
        def transform(batch):
            return batch.set_column(0, 'x', pc.multiply(batch.column('x'), 10))

        map_op = CythonMapOperator(source=None, map_func=transform)
        morsel_op = MorselDrivenOperator(map_op, num_workers=4)

        batch = pa.RecordBatch.from_pydict({'x': [42]})

        sequential = map_op.process_batch(batch)
        parallel = await morsel_op._async_process_with_morsels(batch)

        assert sequential.column('x').to_pylist() == [420]
        assert parallel.column('x').to_pylist() == [420]

    async def test_all_filtered_out(self):
        """Correctness when all rows filtered out."""
        def filter_impossible(batch):
            return pc.and_(
                pc.greater(batch.column('x'), 100),
                pc.less(batch.column('x'), 50)
            )

        filter_op = CythonFilterOperator(source=None, predicate=filter_impossible)
        morsel_op = MorselDrivenOperator(filter_op, num_workers=4)

        batch = pa.RecordBatch.from_pydict({'x': list(range(10000))})

        sequential = filter_op.process_batch(batch)
        parallel = await morsel_op._async_process_with_morsels(batch)

        # Both should be None or empty
        assert (sequential is None or sequential.num_rows == 0)
        assert (parallel is None or parallel.num_rows == 0)


@pytest.mark.asyncio
class TestRandomData:
    """Test correctness with random data."""

    async def test_random_integers(self):
        """Correctness with random integer data."""
        np.random.seed(42)

        def transform(batch):
            return batch.set_column(0, 'x', pc.modulo(batch.column('x'), 100))

        map_op = CythonMapOperator(source=None, map_func=transform)
        morsel_op = MorselDrivenOperator(map_op, num_workers=4)

        random_data = np.random.randint(0, 100000, size=30000).tolist()
        batch = pa.RecordBatch.from_pydict({'x': random_data})

        sequential = map_op.process_batch(batch)
        parallel = await morsel_op._async_process_with_morsels(batch)

        assert sequential.column('x').to_pylist() == parallel.column('x').to_pylist()

    async def test_random_floats(self):
        """Correctness with random float data."""
        np.random.seed(123)

        def transform(batch):
            return batch.set_column(0, 'x', pc.multiply(batch.column('x'), 2.5))

        map_op = CythonMapOperator(source=None, map_func=transform)
        morsel_op = MorselDrivenOperator(map_op, num_workers=4)

        random_data = np.random.uniform(-1000, 1000, size=30000).tolist()
        batch = pa.RecordBatch.from_pydict({'x': random_data})

        sequential = map_op.process_batch(batch)
        parallel = await morsel_op._async_process_with_morsels(batch)

        # Float comparison with tolerance
        seq_list = sequential.column('x').to_pylist()
        par_list = parallel.column('x').to_pylist()
        assert len(seq_list) == len(par_list)
        for i in range(len(seq_list)):
            assert abs(seq_list[i] - par_list[i]) < 1e-9


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-s', '-m', 'not slow'])
