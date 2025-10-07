"""
Test Morsel Processing - Default process_morsel() implementation.

Verifies:
- Default morsel processing works with all operators
- Morsel metadata handling (timestamps, processed flag)
- Edge cases (None, empty batches, invalid morsels)
- Integration with actual operators (map, filter, select)
"""

import pytest
import pyarrow as pa
import pyarrow.compute as pc
from sabot._cython.operators.base_operator import BaseOperator


class MockMorsel:
    """Mock morsel for testing."""

    def __init__(self, data, morsel_id=0):
        self.data = data
        self.morsel_id = morsel_id
        self.processed = False
        self.timestamp = None

    def mark_processed(self):
        self.processed = True
        import time
        self.timestamp = time.time()


class TestDefaultProcessMorsel:
    """Test default process_morsel() implementation."""

    def test_process_morsel_extracts_batch(self):
        """process_morsel extracts batch from morsel."""
        op = BaseOperator(source=None)

        batch = pa.RecordBatch.from_pydict({'x': [1, 2, 3]})
        morsel = MockMorsel(batch)

        result = op.process_morsel(morsel)

        assert result is not None
        assert result is morsel
        assert result.data is batch

    def test_process_morsel_calls_process_batch(self):
        """process_morsel calls process_batch on extracted data."""
        # Create custom operator that transforms data
        class DoubleOperator(BaseOperator):
            def process_batch(self, batch):
                return batch.set_column(
                    0, 'x',
                    pc.multiply(batch.column('x'), 2)
                )

        op = DoubleOperator(source=None)
        batch = pa.RecordBatch.from_pydict({'x': [1, 2, 3]})
        morsel = MockMorsel(batch)

        result = op.process_morsel(morsel)

        # Verify transformation was applied (proves process_batch was called)
        assert result.data.column('x').to_pylist() == [2, 4, 6]

    def test_process_morsel_updates_morsel_data(self):
        """process_morsel updates morsel.data with result."""
        class TransformOperator(BaseOperator):
            def process_batch(self, batch):
                return batch.append_column(
                    'y', pc.multiply(batch.column('x'), 10)
                )

        op = TransformOperator(source=None)
        batch = pa.RecordBatch.from_pydict({'x': [1, 2, 3]})
        morsel = MockMorsel(batch)

        result = op.process_morsel(morsel)

        assert 'y' in result.data.schema.names
        assert result.data.column('y').to_pylist() == [10, 20, 30]

    def test_process_morsel_marks_processed(self):
        """process_morsel marks morsel as processed."""
        op = BaseOperator(source=None)

        batch = pa.RecordBatch.from_pydict({'x': [1, 2, 3]})
        morsel = MockMorsel(batch)

        assert not morsel.processed

        result = op.process_morsel(morsel)

        assert result.processed

    def test_process_morsel_sets_timestamp(self):
        """process_morsel sets processing timestamp."""
        op = BaseOperator(source=None)

        batch = pa.RecordBatch.from_pydict({'x': [1, 2, 3]})
        morsel = MockMorsel(batch)

        result = op.process_morsel(morsel)

        assert result.timestamp is not None


class TestMorselWithMapOperator:
    """Test morsel processing with map operators."""

    def test_map_operator_process_morsel(self):
        """Map operator can process morsels using default implementation."""
        class DoubleOperator(BaseOperator):
            def process_batch(self, batch):
                return batch.set_column(
                    0, 'x',
                    pc.multiply(batch.column('x'), 2)
                )

        op = DoubleOperator(source=None)

        batch = pa.RecordBatch.from_pydict({'x': [5, 10, 15]})
        morsel = MockMorsel(batch)

        result = op.process_morsel(morsel)

        assert result is not None
        assert result.data.column('x').to_pylist() == [10, 20, 30]
        assert result.processed

    def test_map_operator_with_complex_transform(self):
        """Map operator handles complex transformations in morsels."""
        class ComplexOperator(BaseOperator):
            def process_batch(self, batch):
                return batch.append_column(
                    'sum',
                    pc.add(
                        batch.column('x'),
                        batch.column('y')
                    )
                ).append_column(
                    'product',
                    pc.multiply(
                        batch.column('x'),
                        batch.column('y')
                    )
                )

        op = ComplexOperator(source=None)

        batch = pa.RecordBatch.from_pydict({
            'x': [1, 2, 3],
            'y': [10, 20, 30]
        })
        morsel = MockMorsel(batch)

        result = op.process_morsel(morsel)

        assert 'sum' in result.data.schema.names
        assert 'product' in result.data.schema.names
        assert result.data.column('sum').to_pylist() == [11, 22, 33]
        assert result.data.column('product').to_pylist() == [10, 40, 90]


class TestMorselWithFilterOperator:
    """Test morsel processing with filter operators."""

    def test_filter_operator_process_morsel(self):
        """Filter operator can process morsels."""
        class FilterLargeOperator(BaseOperator):
            def process_batch(self, batch):
                mask = pc.greater(batch.column('x'), 5)
                return batch.filter(mask)

        op = FilterLargeOperator(source=None)

        batch = pa.RecordBatch.from_pydict({'x': [1, 5, 10, 15, 3]})
        morsel = MockMorsel(batch)

        result = op.process_morsel(morsel)

        assert result is not None
        assert result.data.num_rows == 2
        assert result.data.column('x').to_pylist() == [10, 15]

    def test_filter_operator_filters_all_rows(self):
        """Filter operator handles case where all rows are filtered out."""
        class FilterNegativeOperator(BaseOperator):
            def process_batch(self, batch):
                mask = pc.less(batch.column('x'), 0)
                result = batch.filter(mask)
                return result if result.num_rows > 0 else None

        op = FilterNegativeOperator(source=None)

        batch = pa.RecordBatch.from_pydict({'x': [1, 2, 3, 4, 5]})
        morsel = MockMorsel(batch)

        result = op.process_morsel(morsel)

        # Should return None when all filtered out
        assert result is None


class TestEdgeCases:
    """Test edge cases for morsel processing."""

    def test_process_morsel_with_empty_batch(self):
        """process_morsel handles empty batches gracefully."""
        op = BaseOperator(source=None)

        empty_batch = pa.RecordBatch.from_pydict({'x': []})
        morsel = MockMorsel(empty_batch)

        result = op.process_morsel(morsel)

        assert result is not None
        assert result.data.num_rows == 0

    def test_process_morsel_with_none_data(self):
        """process_morsel handles None data."""
        op = BaseOperator(source=None)

        morsel = MockMorsel(None)

        result = op.process_morsel(morsel)

        # Should return None when processing None
        assert result is None

    def test_process_morsel_returns_none_when_filtered(self):
        """process_morsel returns None when result is None (filtered)."""
        class FilterAllOperator(BaseOperator):
            def process_batch(self, batch):
                return None

        op = FilterAllOperator(source=None)

        batch = pa.RecordBatch.from_pydict({'x': [1, 2, 3]})
        morsel = MockMorsel(batch)

        result = op.process_morsel(morsel)

        assert result is None

    def test_process_morsel_without_mark_processed_method(self):
        """process_morsel handles morsels without mark_processed method."""
        op = BaseOperator(source=None)

        # Morsel without mark_processed method
        class SimpleMorsel:
            def __init__(self, data):
                self.data = data

        batch = pa.RecordBatch.from_pydict({'x': [1, 2, 3]})
        morsel = SimpleMorsel(batch)

        result = op.process_morsel(morsel)

        # Should still work, just not mark as processed
        assert result is not None
        assert result.data is batch


class TestMultipleMorsels:
    """Test processing multiple morsels sequentially."""

    def test_process_multiple_morsels(self):
        """Operator can process multiple morsels sequentially."""
        class IncrementOperator(BaseOperator):
            def process_batch(self, batch):
                return batch.set_column(
                    0, 'x',
                    pc.add(batch.column('x'), 1)
                )

        op = IncrementOperator(source=None)

        # Create multiple morsels
        morsels = [
            MockMorsel(pa.RecordBatch.from_pydict({'x': [1, 2, 3]}), morsel_id=0),
            MockMorsel(pa.RecordBatch.from_pydict({'x': [4, 5, 6]}), morsel_id=1),
            MockMorsel(pa.RecordBatch.from_pydict({'x': [7, 8, 9]}), morsel_id=2),
        ]

        results = [op.process_morsel(m) for m in morsels]

        assert len(results) == 3
        assert results[0].data.column('x').to_pylist() == [2, 3, 4]
        assert results[1].data.column('x').to_pylist() == [5, 6, 7]
        assert results[2].data.column('x').to_pylist() == [8, 9, 10]

        # All should be marked as processed
        assert all(r.processed for r in results)

    def test_process_morsels_with_different_sizes(self):
        """Operator handles morsels with different sizes."""
        op = BaseOperator(source=None)

        morsels = [
            MockMorsel(pa.RecordBatch.from_pydict({'x': [1]})),              # 1 row
            MockMorsel(pa.RecordBatch.from_pydict({'x': [2, 3, 4]})),        # 3 rows
            MockMorsel(pa.RecordBatch.from_pydict({'x': [5, 6, 7, 8, 9]})),  # 5 rows
        ]

        results = [op.process_morsel(m) for m in morsels]

        assert results[0].data.num_rows == 1
        assert results[1].data.num_rows == 3
        assert results[2].data.num_rows == 5


class TestMorselPreservesMetadata:
    """Test that morsel metadata is preserved during processing."""

    def test_preserves_morsel_id(self):
        """Morsel ID is preserved after processing."""
        op = BaseOperator(source=None)

        batch = pa.RecordBatch.from_pydict({'x': [1, 2, 3]})
        morsel = MockMorsel(batch, morsel_id=42)

        result = op.process_morsel(morsel)

        assert result.morsel_id == 42

    def test_preserves_custom_attributes(self):
        """Custom morsel attributes are preserved."""
        op = BaseOperator(source=None)

        class CustomMorsel(MockMorsel):
            def __init__(self, data, partition_id=0):
                super().__init__(data)
                self.partition_id = partition_id
                self.worker_id = None

        batch = pa.RecordBatch.from_pydict({'x': [1, 2, 3]})
        morsel = CustomMorsel(batch, partition_id=5)
        morsel.worker_id = 3

        result = op.process_morsel(morsel)

        assert result.partition_id == 5
        assert result.worker_id == 3


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-s'])
