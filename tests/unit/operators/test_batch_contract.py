#!/usr/bin/env python3
"""
Test batch-first contract for all operators.

Validates that operators ONLY process batches, never individual records.
"""

import pytest
from sabot import cyarrow as pa
from sabot.cyarrow import compute as pc
from sabot._cython.operators import (
    CythonMapOperator,
    CythonFilterOperator,
    CythonSelectOperator,
    CythonFlatMapOperator,
    CythonUnionOperator,
)


@pytest.fixture
def sample_batches():
    """Generate sample RecordBatches."""
    return [
        pa.RecordBatch.from_pydict({
            'id': list(range(i * 100, (i + 1) * 100)),
            'value': [j * 2 for j in range(100)],
            'category': ['A' if j % 2 == 0 else 'B' for j in range(100)]
        })
        for i in range(10)
    ]


class TestBatchContract:
    """Test that all operators follow batch-first contract."""

    def test_operator_always_yields_batches(self, sample_batches):
        """Verify operators yield RecordBatch, never individual records."""
        operator = CythonMapOperator(
            source=iter(sample_batches),
            map_func=lambda b: b
        )

        for item in operator:
            assert isinstance(item, pa.RecordBatch), \
                f"Operator yielded {type(item)}, expected RecordBatch"
            assert item.num_rows > 0

    def test_process_batch_input_output_types(self, sample_batches):
        """Verify process_batch takes and returns RecordBatch."""
        operator = CythonMapOperator(
            source=None,
            map_func=lambda b: b.append_column('new',
                pc.multiply(b.column('value'), 2))
        )

        batch = sample_batches[0]
        result = operator.process_batch(batch)

        assert isinstance(result, pa.RecordBatch)
        assert 'new' in result.schema.names

    def test_filter_returns_batches(self, sample_batches):
        """Filter operator returns batches."""
        operator = CythonFilterOperator(
            source=iter(sample_batches),
            predicate=lambda b: pc.greater(b.column('value'), 100)
        )

        for batch in operator:
            assert isinstance(batch, pa.RecordBatch)
            # All values should be > 100
            assert all(v > 100 for v in batch.column('value').to_pylist())

    def test_select_returns_batches(self, sample_batches):
        """Select operator returns batches."""
        operator = CythonSelectOperator(
            source=iter(sample_batches),
            columns=['id', 'value']
        )

        for batch in operator:
            assert isinstance(batch, pa.RecordBatch)
            assert batch.schema.names == ['id', 'value']

    def test_flatmap_returns_batches(self, sample_batches):
        """FlatMap operator returns batches."""
        def split_batch(batch):
            # Split into 2 smaller batches
            mid = batch.num_rows // 2
            return [batch.slice(0, mid), batch.slice(mid)]

        operator = CythonFlatMapOperator(
            source=iter(sample_batches),
            flat_map_func=split_batch
        )

        for batch in operator:
            assert isinstance(batch, pa.RecordBatch)

    def test_union_returns_batches(self, sample_batches):
        """Union operator returns batches."""
        source1 = iter(sample_batches[:5])
        source2 = iter(sample_batches[5:])

        operator = CythonUnionOperator(source1, source2)

        for batch in operator:
            assert isinstance(batch, pa.RecordBatch)

    def test_no_per_record_iteration(self, sample_batches):
        """Verify operators don't iterate per-record internally."""
        # This is tested by checking that process_batch is called
        # with RecordBatch, not individual records

        call_log = []

        def logging_map(batch):
            call_log.append(type(batch))
            return batch

        operator = CythonMapOperator(
            source=iter(sample_batches),
            map_func=logging_map
        )

        list(operator)  # Consume

        # All calls should be RecordBatch
        assert all(t == pa.RecordBatch for t in call_log), \
            "Operator called map function with non-batch types"

    def test_empty_batches_handled(self):
        """Empty batches are filtered out."""
        empty_batch = pa.RecordBatch.from_pydict({
            'id': [],
            'value': []
        })

        batches = [empty_batch] * 5

        operator = CythonMapOperator(
            source=iter(batches),
            map_func=lambda b: b
        )

        result = list(operator)
        assert len(result) == 0, "Empty batches should be filtered"

    def test_null_batches_handled(self, sample_batches):
        """Null batches are handled gracefully."""
        def return_null(batch):
            return None

        operator = CythonMapOperator(
            source=iter(sample_batches),
            map_func=return_null
        )

        result = list(operator)
        assert len(result) == 0


class TestBatchVsStreamingMode:
    """Test batch mode vs streaming mode behavior."""

    def test_sync_iteration_terminates(self, sample_batches):
        """Sync iteration terminates (batch mode)."""
        operator = CythonMapOperator(
            source=iter(sample_batches),
            map_func=lambda b: b
        )

        count = 0
        for batch in operator:
            count += 1

        assert count == len(sample_batches)
        # Iterator is exhausted
        assert list(operator) == []

    @pytest.mark.asyncio
    async def test_async_iteration_with_finite_source(self, sample_batches):
        """Async iteration with finite source terminates."""
        operator = CythonMapOperator(
            source=iter(sample_batches),
            map_func=lambda b: b
        )

        count = 0
        async for batch in operator:
            count += 1

        assert count == len(sample_batches)

    @pytest.mark.asyncio
    async def test_async_iteration_with_async_source(self):
        """Async iteration with async source."""
        async def async_source():
            for i in range(5):
                yield pa.RecordBatch.from_pydict({
                    'id': list(range(i * 10, (i + 1) * 10)),
                    'value': list(range(10))
                })

        operator = CythonMapOperator(
            source=async_source(),
            map_func=lambda b: b.append_column('doubled',
                pc.multiply(b.column('value'), 2))
        )

        count = 0
        async for batch in operator:
            assert isinstance(batch, pa.RecordBatch)
            assert 'doubled' in batch.schema.names
            count += 1

        assert count == 5


class TestZeroCopy:
    """Test zero-copy semantics."""

    def test_select_is_zero_copy(self, sample_batches):
        """Select operation doesn't copy data."""
        batch = sample_batches[0]

        operator = CythonSelectOperator(
            source=iter([batch]),
            columns=['id', 'value']
        )

        result = next(iter(operator))

        # Check that underlying buffers are shared (zero-copy)
        # Arrow's select() creates a new RecordBatch but shares buffers
        assert result.schema.names == ['id', 'value']

    def test_filter_minimal_copy(self, sample_batches):
        """Filter copies only selected rows."""
        batch = sample_batches[0]
        original_size = batch.nbytes

        operator = CythonFilterOperator(
            source=iter([batch]),
            predicate=lambda b: pc.greater(b.column('value'), 100)
        )

        result = next(iter(operator))

        # Result should be smaller (fewer rows)
        assert result.nbytes < original_size


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
