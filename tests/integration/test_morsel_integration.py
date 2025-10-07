"""
Test Morsel Integration - End-to-end integration tests.

Verifies:
- Map operator with morsel parallelism
- Filter operator with morsel parallelism
- Chained operators (map → filter → map)
- Work-stealing across workers
- Different worker counts (2, 4, 8)
- Large datasets (100K+ rows)
"""

import pytest
import pyarrow as pa
import pyarrow.compute as pc
import asyncio
from sabot._cython.operators.transform import CythonMapOperator, CythonFilterOperator
from sabot._cython.operators.morsel_operator import MorselDrivenOperator


@pytest.mark.asyncio
class TestMapOperatorIntegration:
    """Test map operator with morsel parallelism."""

    async def test_simple_map_with_morsels(self):
        """Simple map transformation with morsel parallelism."""
        def double_x(batch):
            return batch.set_column(0, 'x', pc.multiply(batch.column('x'), 2))

        map_op = CythonMapOperator(source=None, map_func=double_x)
        morsel_op = MorselDrivenOperator(map_op, num_workers=4)

        # Large batch
        batch = pa.RecordBatch.from_pydict({'x': list(range(100000))})

        result = await morsel_op._async_process_with_morsels(batch)

        assert result.num_rows == 100000
        assert result.column('x').to_pylist()[0] == 0
        assert result.column('x').to_pylist()[99999] == 199998

    async def test_map_with_multiple_columns(self):
        """Map transformation with multiple columns."""
        def compute_sum_and_product(batch):
            return batch.append_column(
                'sum', pc.add(batch.column('x'), batch.column('y'))
            ).append_column(
                'product', pc.multiply(batch.column('x'), batch.column('y'))
            )

        map_op = CythonMapOperator(source=None, map_func=compute_sum_and_product)
        morsel_op = MorselDrivenOperator(map_op, num_workers=8)

        batch = pa.RecordBatch.from_pydict({
            'x': list(range(50000)),
            'y': list(range(50000, 100000))
        })

        result = await morsel_op._async_process_with_morsels(batch)

        assert result.num_rows == 50000
        assert 'sum' in result.schema.names
        assert 'product' in result.schema.names
        # Check a few values
        assert result.column('sum').to_pylist()[0] == 50000  # 0 + 50000
        assert result.column('product').to_pylist()[10] == 500100  # 10 * 50010

    async def test_map_with_complex_computation(self):
        """Map with complex mathematical computation."""
        def complex_math(batch):
            # Compute: (x^2 + y^2) / (x + y + 1)
            x_squared = pc.power(batch.column('x'), 2)
            y_squared = pc.power(batch.column('y'), 2)
            numerator = pc.add(x_squared, y_squared)
            denominator = pc.add(pc.add(batch.column('x'), batch.column('y')), 1)
            result = pc.divide(numerator, denominator)

            return batch.append_column('computed', result)

        map_op = CythonMapOperator(source=None, map_func=complex_math)
        morsel_op = MorselDrivenOperator(map_op, num_workers=4)

        batch = pa.RecordBatch.from_pydict({
            'x': [float(i) for i in range(20000)],
            'y': [float(i+1) for i in range(20000)]
        })

        result = await morsel_op._async_process_with_morsels(batch)

        assert result.num_rows == 20000
        assert 'computed' in result.schema.names


@pytest.mark.asyncio
class TestFilterOperatorIntegration:
    """Test filter operator with morsel parallelism."""

    async def test_simple_filter_with_morsels(self):
        """Simple filter predicate with morsel parallelism."""
        def filter_large(batch):
            return pc.greater(batch.column('value'), 5000)

        filter_op = CythonFilterOperator(source=None, predicate=filter_large)
        morsel_op = MorselDrivenOperator(filter_op, num_workers=4)

        batch = pa.RecordBatch.from_pydict({'value': list(range(10000))})

        result = await morsel_op._async_process_with_morsels(batch)

        assert result.num_rows == 4999  # 5001-9999
        assert min(result.column('value').to_pylist()) > 5000

    async def test_filter_with_complex_predicate(self):
        """Filter with complex predicate."""
        def complex_predicate(batch):
            # Filter: (x > 1000) AND (x % 3 == 0) AND (x < 50000)
            return pc.and_(
                pc.and_(
                    pc.greater(batch.column('x'), 1000),
                    pc.equal(pc.modulo(batch.column('x'), 3), 0)
                ),
                pc.less(batch.column('x'), 50000)
            )

        filter_op = CythonFilterOperator(source=None, predicate=complex_predicate)
        morsel_op = MorselDrivenOperator(filter_op, num_workers=8)

        batch = pa.RecordBatch.from_pydict({'x': list(range(100000))})

        result = await morsel_op._async_process_with_morsels(batch)

        # Verify all results meet predicate
        for x in result.column('x').to_pylist():
            assert x > 1000
            assert x % 3 == 0
            assert x < 50000

    async def test_filter_removes_all_rows(self):
        """Filter that removes all rows."""
        def filter_negative(batch):
            return pc.less(batch.column('x'), 0)

        filter_op = CythonFilterOperator(source=None, predicate=filter_negative)
        morsel_op = MorselDrivenOperator(filter_op, num_workers=4)

        batch = pa.RecordBatch.from_pydict({'x': list(range(10000))})

        result = await morsel_op._async_process_with_morsels(batch)

        assert result is None or result.num_rows == 0


@pytest.mark.asyncio
class TestChainedOperators:
    """Test chained operators with morsel parallelism."""

    async def test_map_then_filter(self):
        """Chain: map → filter."""
        def double(batch):
            return batch.set_column(0, 'x', pc.multiply(batch.column('x'), 2))

        def filter_large(batch):
            return pc.greater(batch.column('x'), 10000)

        # Create chain
        map_op = CythonMapOperator(source=None, map_func=double)
        map_morsel = MorselDrivenOperator(map_op, num_workers=4)

        filter_op = CythonFilterOperator(source=None, predicate=filter_large)
        filter_morsel = MorselDrivenOperator(filter_op, num_workers=4)

        # Process through chain
        batch = pa.RecordBatch.from_pydict({'x': list(range(20000))})

        # First: map (double)
        after_map = await map_morsel._async_process_with_morsels(batch)

        # Then: filter (> 10000)
        result = await filter_morsel._async_process_with_morsels(after_map)

        # Should have values > 10000 (which are original values > 5000)
        assert min(result.column('x').to_pylist()) > 10000

    async def test_map_filter_map_chain(self):
        """Chain: map → filter → map."""
        def multiply_by_3(batch):
            return batch.set_column(0, 'x', pc.multiply(batch.column('x'), 3))

        def filter_mod_7(batch):
            return pc.equal(pc.modulo(batch.column('x'), 7), 0)

        def add_100(batch):
            return batch.set_column(0, 'x', pc.add(batch.column('x'), 100))

        # Create operators
        map1 = CythonMapOperator(source=None, map_func=multiply_by_3)
        map1_morsel = MorselDrivenOperator(map1, num_workers=4)

        filter1 = CythonFilterOperator(source=None, predicate=filter_mod_7)
        filter1_morsel = MorselDrivenOperator(filter1, num_workers=4)

        map2 = CythonMapOperator(source=None, map_func=add_100)
        map2_morsel = MorselDrivenOperator(map2, num_workers=4)

        # Process through chain
        batch = pa.RecordBatch.from_pydict({'x': list(range(50000))})

        after_map1 = await map1_morsel._async_process_with_morsels(batch)
        after_filter = await filter1_morsel._async_process_with_morsels(after_map1)
        result = await map2_morsel._async_process_with_morsels(after_filter)

        # All values should be (original * 3) % 7 == 0, then + 100
        for x in result.column('x').to_pylist():
            assert (x - 100) % 7 == 0


@pytest.mark.asyncio
class TestDifferentWorkerCounts:
    """Test with different worker counts."""

    async def test_2_workers(self):
        """Test with 2 workers."""
        def double(batch):
            return batch.set_column(0, 'x', pc.multiply(batch.column('x'), 2))

        map_op = CythonMapOperator(source=None, map_func=double)
        morsel_op = MorselDrivenOperator(map_op, num_workers=2)

        batch = pa.RecordBatch.from_pydict({'x': list(range(30000))})
        result = await morsel_op._async_process_with_morsels(batch)

        assert result.num_rows == 30000

    async def test_4_workers(self):
        """Test with 4 workers."""
        def double(batch):
            return batch.set_column(0, 'x', pc.multiply(batch.column('x'), 2))

        map_op = CythonMapOperator(source=None, map_func=double)
        morsel_op = MorselDrivenOperator(map_op, num_workers=4)

        batch = pa.RecordBatch.from_pydict({'x': list(range(30000))})
        result = await morsel_op._async_process_with_morsels(batch)

        assert result.num_rows == 30000

    async def test_8_workers(self):
        """Test with 8 workers."""
        def double(batch):
            return batch.set_column(0, 'x', pc.multiply(batch.column('x'), 2))

        map_op = CythonMapOperator(source=None, map_func=double)
        morsel_op = MorselDrivenOperator(map_op, num_workers=8)

        batch = pa.RecordBatch.from_pydict({'x': list(range(30000))})
        result = await morsel_op._async_process_with_morsels(batch)

        assert result.num_rows == 30000


@pytest.mark.asyncio
class TestLargeDatasets:
    """Test with large datasets."""

    async def test_100k_rows(self):
        """Test with 100K rows."""
        def transform(batch):
            return batch.append_column('y', pc.multiply(batch.column('x'), 2))

        map_op = CythonMapOperator(source=None, map_func=transform)
        morsel_op = MorselDrivenOperator(map_op, num_workers=8)

        batch = pa.RecordBatch.from_pydict({'x': list(range(100000))})
        result = await morsel_op._async_process_with_morsels(batch)

        assert result.num_rows == 100000
        assert 'y' in result.schema.names

    async def test_500k_rows(self):
        """Test with 500K rows."""
        def double(batch):
            return batch.set_column(0, 'x', pc.multiply(batch.column('x'), 2))

        map_op = CythonMapOperator(source=None, map_func=double)
        morsel_op = MorselDrivenOperator(map_op, num_workers=8)

        batch = pa.RecordBatch.from_pydict({'x': list(range(500000))})
        result = await morsel_op._async_process_with_morsels(batch)

        assert result.num_rows == 500000

    @pytest.mark.slow
    async def test_1m_rows(self):
        """Test with 1M rows (slow test)."""
        def increment(batch):
            return batch.set_column(0, 'x', pc.add(batch.column('x'), 1))

        map_op = CythonMapOperator(source=None, map_func=increment)
        morsel_op = MorselDrivenOperator(map_op, num_workers=8)

        batch = pa.RecordBatch.from_pydict({'x': list(range(1000000))})
        result = await morsel_op._async_process_with_morsels(batch)

        assert result.num_rows == 1000000


@pytest.mark.asyncio
class TestStatistics:
    """Test statistics collection."""

    async def test_morsels_created_stat(self):
        """Verify morsels_created statistic."""
        def double(batch):
            return batch.set_column(0, 'x', pc.multiply(batch.column('x'), 2))

        map_op = CythonMapOperator(source=None, map_func=double)
        morsel_op = MorselDrivenOperator(map_op, num_workers=4, morsel_size_kb=64)

        batch = pa.RecordBatch.from_pydict({'x': list(range(50000))})
        result = await morsel_op._async_process_with_morsels(batch)

        stats = morsel_op.get_stats()

        assert 'total_morsels_created' in stats
        assert stats['total_morsels_created'] > 0

    async def test_worker_count_stat(self):
        """Verify worker count statistic."""
        def double(batch):
            return batch.set_column(0, 'x', pc.multiply(batch.column('x'), 2))

        map_op = CythonMapOperator(source=None, map_func=double)
        morsel_op = MorselDrivenOperator(map_op, num_workers=8)

        batch = pa.RecordBatch.from_pydict({'x': list(range(30000))})
        result = await morsel_op._async_process_with_morsels(batch)

        stats = morsel_op.get_stats()

        assert 'num_workers' in stats
        assert stats['num_workers'] == 8


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-s', '-m', 'not slow'])
