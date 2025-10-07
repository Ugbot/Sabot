"""
Test BaseOperator - Base class for all streaming operators.

Verifies:
- Interface contract (process_batch, process_morsel, etc.)
- Default implementations
- Metadata methods
- Iteration interfaces
- Shuffle interface
"""

import pytest
import pyarrow as pa
import pyarrow.compute as pc
from sabot._cython.operators.base_operator import BaseOperator


class TestBaseOperatorInterface:
    """Test BaseOperator interface contract."""

    def test_instantiation(self):
        """BaseOperator can be instantiated directly."""
        op = BaseOperator(source=None)
        assert op is not None
        assert isinstance(op, BaseOperator)

    def test_instantiation_with_kwargs(self):
        """BaseOperator accepts kwargs for initialization."""
        schema = pa.schema([('x', pa.int64()), ('y', pa.int64())])
        op = BaseOperator(source=None, schema=schema)

        assert op.get_schema() == schema
        assert not op.is_stateful()

    def test_operator_name(self):
        """get_operator_name returns class name."""
        op = BaseOperator(source=None)
        assert op.get_operator_name() == "BaseOperator"


class TestProcessBatch:
    """Test process_batch default implementation."""

    def test_process_batch_passthrough(self):
        """Default process_batch passes through batch unchanged."""
        op = BaseOperator(source=None)

        batch = pa.RecordBatch.from_pydict({'x': [1, 2, 3]})
        result = op.process_batch(batch)

        assert result is batch  # Same object

    def test_process_batch_with_none(self):
        """process_batch handles None gracefully."""
        op = BaseOperator(source=None)
        result = op.process_batch(None)
        assert result is None

    def test_process_batch_with_empty_batch(self):
        """process_batch handles empty batches."""
        op = BaseOperator(source=None)

        empty_batch = pa.RecordBatch.from_pydict({'x': []})
        result = op.process_batch(empty_batch)

        assert result == empty_batch
        assert result.num_rows == 0


class TestProcessMorsel:
    """Test process_morsel default implementation."""

    def test_process_morsel_with_morsel_object(self):
        """process_morsel extracts batch, processes it, updates morsel."""
        op = BaseOperator(source=None)

        # Create mock morsel
        class MockMorsel:
            def __init__(self, data):
                self.data = data
                self.processed = False

            def mark_processed(self):
                self.processed = True

        batch = pa.RecordBatch.from_pydict({'x': [1, 2, 3]})
        morsel = MockMorsel(batch)

        result = op.process_morsel(morsel)

        assert result is morsel
        assert result.data is batch
        assert result.processed

    def test_process_morsel_with_batch_directly(self):
        """process_morsel handles batch passed directly (no morsel wrapper)."""
        op = BaseOperator(source=None)

        batch = pa.RecordBatch.from_pydict({'x': [1, 2, 3]})
        result = op.process_morsel(batch)

        # Should call process_batch directly
        assert result is batch

    def test_process_morsel_with_none_result(self):
        """process_morsel returns None if processing returns None."""
        # Create custom operator that filters out all data
        class FilterAllOperator(BaseOperator):
            def process_batch(self, batch):
                return None

        op = FilterAllOperator(source=None)

        class MockMorsel:
            def __init__(self, data):
                self.data = data

        batch = pa.RecordBatch.from_pydict({'x': [1, 2, 3]})
        morsel = MockMorsel(batch)

        result = op.process_morsel(morsel)
        assert result is None


class TestShuffleInterface:
    """Test shuffle interface methods."""

    def test_requires_shuffle_default_false(self):
        """Non-stateful operators don't require shuffle."""
        op = BaseOperator(source=None)
        assert not op.requires_shuffle()

    @pytest.mark.skip(reason="Stateful behavior tested through ShuffledOperator subclass")
    def test_requires_shuffle_when_stateful(self):
        """Stateful operators require shuffle."""
        # Note: This is tested through ShuffledOperator which properly sets _stateful
        pass

    def test_get_partition_keys_none_by_default(self):
        """Non-stateful operators have no partition keys."""
        op = BaseOperator(source=None)
        assert op.get_partition_keys() is None

    @pytest.mark.skip(reason="Stateful behavior tested through ShuffledOperator subclass")
    def test_get_partition_keys_when_stateful(self):
        """Stateful operators return partition keys."""
        # Note: This is tested through ShuffledOperator which properly sets _stateful and _key_columns
        pass

    def test_get_parallelism_hint_default(self):
        """Default parallelism hint is 1."""
        op = BaseOperator(source=None)
        assert op.get_parallelism_hint() == 1


class TestMetadataMethods:
    """Test metadata methods."""

    def test_get_schema_none_by_default(self):
        """Schema is None by default."""
        op = BaseOperator(source=None)
        assert op.get_schema() is None

    def test_get_schema_when_provided(self):
        """Schema is returned when provided."""
        schema = pa.schema([('a', pa.int64()), ('b', pa.float64())])
        op = BaseOperator(source=None, schema=schema)
        assert op.get_schema() == schema

    def test_is_stateful_false_by_default(self):
        """Operators are stateless by default."""
        op = BaseOperator(source=None)
        assert not op.is_stateful()

    @pytest.mark.skip(reason="Stateful behavior tested through ShuffledOperator subclass")
    def test_is_stateful_when_set(self):
        """is_stateful returns True when _stateful is set."""
        # Note: This is tested through ShuffledOperator which properly sets _stateful
        pass


class TestIterationInterface:
    """Test iteration interfaces (__iter__, __aiter__)."""

    def test_iter_with_no_source(self):
        """Iterator with no source terminates immediately."""
        op = BaseOperator(source=None)
        result = list(op)
        assert result == []

    def test_iter_with_source(self):
        """Iterator processes batches from source."""
        # Create source data
        batches = [
            pa.RecordBatch.from_pydict({'x': [1, 2, 3]}),
            pa.RecordBatch.from_pydict({'x': [4, 5, 6]}),
        ]

        op = BaseOperator(source=iter(batches))
        result = list(op)

        assert len(result) == 2
        assert result[0].column('x').to_pylist() == [1, 2, 3]
        assert result[1].column('x').to_pylist() == [4, 5, 6]

    def test_iter_filters_empty_batches(self):
        """Iterator filters out empty batches."""
        # Create custom operator that returns empty batches
        class EmptyOperator(BaseOperator):
            def process_batch(self, batch):
                return pa.RecordBatch.from_pydict({'x': []})

        batches = [pa.RecordBatch.from_pydict({'x': [1, 2, 3]})]
        op = EmptyOperator(source=iter(batches))

        result = list(op)
        assert result == []

    @pytest.mark.asyncio
    async def test_aiter_with_no_source(self):
        """Async iterator with no source terminates immediately."""
        op = BaseOperator(source=None)
        result = []
        async for batch in op:
            result.append(batch)
        assert result == []

    @pytest.mark.asyncio
    async def test_aiter_with_sync_source(self):
        """Async iterator wraps sync source."""
        batches = [
            pa.RecordBatch.from_pydict({'x': [1, 2, 3]}),
            pa.RecordBatch.from_pydict({'x': [4, 5, 6]}),
        ]

        op = BaseOperator(source=iter(batches))
        result = []
        async for batch in op:
            result.append(batch)

        assert len(result) == 2
        assert result[0].column('x').to_pylist() == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_aiter_with_async_source(self):
        """Async iterator handles async sources."""
        async def async_source():
            for i in range(3):
                yield pa.RecordBatch.from_pydict({'x': [i]})

        op = BaseOperator(source=async_source())
        result = []
        async for batch in op:
            result.append(batch)

        assert len(result) == 3


class TestCustomOperator:
    """Test creating custom operators by subclassing."""

    def test_custom_operator_override_process_batch(self):
        """Custom operators can override process_batch."""
        class DoubleOperator(BaseOperator):
            def process_batch(self, batch):
                return batch.set_column(
                    0, 'x',
                    pc.multiply(batch.column('x'), 2)
                )

        op = DoubleOperator(source=None)
        batch = pa.RecordBatch.from_pydict({'x': [1, 2, 3]})
        result = op.process_batch(batch)

        assert result.column('x').to_pylist() == [2, 4, 6]

    def test_custom_operator_override_process_morsel(self):
        """Custom operators can override process_morsel for optimizations."""
        class CustomMorselOperator(BaseOperator):
            def process_morsel(self, morsel):
                # Custom morsel processing
                if not hasattr(morsel, 'data'):
                    return None

                # Process twice for testing
                result = self.process_batch(morsel.data)
                result = self.process_batch(result)

                morsel.data = result
                return morsel

            def process_batch(self, batch):
                return batch.set_column(
                    0, 'x',
                    pc.add(batch.column('x'), 1)
                )

        op = CustomMorselOperator(source=None)

        class MockMorsel:
            def __init__(self, data):
                self.data = data

        batch = pa.RecordBatch.from_pydict({'x': [1, 2, 3]})
        morsel = MockMorsel(batch)

        result = op.process_morsel(morsel)

        # Should be processed twice (added 1 twice)
        assert result.data.column('x').to_pylist() == [3, 4, 5]


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-s'])
