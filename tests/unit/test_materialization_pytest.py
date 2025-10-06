#!/usr/bin/env python
"""
Pytest-compatible tests for Materialization Engine

Integrates with pytest test discovery and reporting.
"""
import pytest
import pyarrow as pa
import pyarrow.feather as feather
import tempfile
from pathlib import Path
import shutil

from sabot._c.materialization_engine import (
    MaterializationManager,
    Materialization,
    MaterializationBackend,
    PopulationStrategy,
)
from sabot.materializations import (
    MaterializationManager as PyMaterializationManager,
    DimensionTableView,
    AnalyticalViewAPI,
)
import sabot as sb


@pytest.fixture
def sample_data():
    """Create sample Arrow data for testing."""
    return {
        'security_id': ['SEC001', 'SEC002', 'SEC003', 'SEC004', 'SEC005'],
        'name': ['Apple Inc.', 'Microsoft Corp.', 'Google LLC', 'Amazon.com Inc.', 'Tesla Inc.'],
        'ticker': ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA'],
        'price': [150.25, 380.50, 145.75, 175.00, 250.00],
        'sector': ['Technology', 'Technology', 'Technology', 'Consumer', 'Automotive'],
    }


@pytest.fixture
def arrow_file(sample_data):
    """Create temporary Arrow IPC file."""
    table = pa.Table.from_pydict(sample_data)
    temp_dir = tempfile.mkdtemp()
    arrow_file_path = Path(temp_dir) / "test_securities.arrow"
    feather.write_feather(table, arrow_file_path, compression='zstd')

    yield arrow_file_path

    # Cleanup
    shutil.rmtree(temp_dir)


@pytest.fixture
def cython_mgr():
    """Create Cython MaterializationManager."""
    return MaterializationManager(default_backend='memory')


@pytest.fixture
def py_mgr():
    """Create Python MaterializationManager."""
    app = sb.App('test-app', broker='kafka://localhost:19092')
    return app.materializations(default_backend='memory')


class TestCythonEngine:
    """Tests for Cython materialization engine."""

    def test_import(self):
        """Test Cython engine imports."""
        assert MaterializationManager is not None
        assert Materialization is not None
        assert MaterializationBackend is not None
        assert PopulationStrategy is not None

    def test_manager_creation(self, cython_mgr):
        """Test MaterializationManager creation."""
        assert cython_mgr is not None

    def test_dimension_table_creation(self, cython_mgr):
        """Test dimension table creation."""
        dim_table = cython_mgr.create_dimension_table(
            name='test_securities',
            key_column='security_id',
            backend='memory'
        )
        assert dim_table is not None

    def test_populate_from_file(self, cython_mgr, arrow_file):
        """Test populating from Arrow file."""
        dim_table = cython_mgr.create_dimension_table(
            name='test_securities',
            key_column='security_id',
            backend='memory'
        )
        dim_table.populate_from_arrow_file(str(arrow_file))
        assert dim_table.num_rows() == 5

    def test_build_index(self, cython_mgr, arrow_file):
        """Test hash index build."""
        dim_table = cython_mgr.create_dimension_table(
            name='test_securities',
            key_column='security_id',
            backend='memory'
        )
        dim_table.populate_from_arrow_file(str(arrow_file))
        dim_table.build_index()
        assert dim_table.num_rows() == 5

    def test_lookup(self, cython_mgr, arrow_file):
        """Test lookup operations."""
        dim_table = cython_mgr.create_dimension_table(
            name='test_securities',
            key_column='security_id',
            backend='memory'
        )
        dim_table.populate_from_arrow_file(str(arrow_file))
        dim_table.build_index()

        result = dim_table.lookup('SEC001')
        assert result is not None
        assert result['name'][0] == 'Apple Inc.'

    def test_contains(self, cython_mgr, arrow_file):
        """Test contains check."""
        dim_table = cython_mgr.create_dimension_table(
            name='test_securities',
            key_column='security_id',
            backend='memory'
        )
        dim_table.populate_from_arrow_file(str(arrow_file))
        dim_table.build_index()

        assert dim_table.contains('SEC001') == True
        assert dim_table.contains('SEC999') == False

    def test_scan(self, cython_mgr, arrow_file):
        """Test scan operations."""
        dim_table = cython_mgr.create_dimension_table(
            name='test_securities',
            key_column='security_id',
            backend='memory'
        )
        dim_table.populate_from_arrow_file(str(arrow_file))

        # Scan with limit
        batch = dim_table.scan(limit=3)
        assert batch.num_rows == 3

        # Full scan
        full_batch = dim_table.scan()
        assert full_batch.num_rows == 5


class TestPythonAPI:
    """Tests for Python API wrapper."""

    def test_import(self):
        """Test Python API imports."""
        assert PyMaterializationManager is not None
        assert DimensionTableView is not None
        assert AnalyticalViewAPI is not None

    def test_manager_creation(self, py_mgr):
        """Test Python MaterializationManager creation."""
        assert py_mgr is not None

    def test_dimension_table_view(self, py_mgr, arrow_file):
        """Test DimensionTableView creation."""
        dim_table = py_mgr.dimension_table(
            name='test_securities',
            source=str(arrow_file),
            key='security_id',
            backend='memory'
        )
        assert dim_table is not None
        assert dim_table.name == 'test_securities'

    def test_operator_getitem(self, py_mgr, arrow_file):
        """Test __getitem__ operator."""
        dim_table = py_mgr.dimension_table(
            name='test_securities',
            source=str(arrow_file),
            key='security_id',
            backend='memory'
        )
        result = dim_table['SEC001']
        assert result['name'] == 'Apple Inc.'

    def test_operator_contains(self, py_mgr, arrow_file):
        """Test __contains__ operator."""
        dim_table = py_mgr.dimension_table(
            name='test_securities',
            source=str(arrow_file),
            key='security_id',
            backend='memory'
        )
        assert 'SEC001' in dim_table
        assert 'SEC999' not in dim_table

    def test_operator_len(self, py_mgr, arrow_file):
        """Test __len__ operator."""
        dim_table = py_mgr.dimension_table(
            name='test_securities',
            source=str(arrow_file),
            key='security_id',
            backend='memory'
        )
        assert len(dim_table) == 5

    def test_safe_get(self, py_mgr, arrow_file):
        """Test safe get() method."""
        dim_table = py_mgr.dimension_table(
            name='test_securities',
            source=str(arrow_file),
            key='security_id',
            backend='memory'
        )
        result = dim_table.get('SEC999', {'name': 'Not Found'})
        assert result['name'] == 'Not Found'

    def test_stream_enrichment(self, py_mgr, arrow_file):
        """Test stream enrichment (join)."""
        dim_table = py_mgr.dimension_table(
            name='test_securities',
            source=str(arrow_file),
            key='security_id',
            backend='memory'
        )

        # Create quotes batch
        quotes_data = {
            'quote_id': [1, 2, 3],
            'security_id': ['SEC001', 'SEC002', 'SEC003'],
            'quantity': [100, 50, 200],
        }
        quotes_batch = pa.RecordBatch.from_pydict(quotes_data)

        # Enrich
        enriched = dim_table._cython_mat.enrich_batch(quotes_batch, 'security_id')

        assert enriched.num_rows == 3
        enriched_dict = enriched.to_pydict()
        assert 'name' in enriched_dict
        assert enriched_dict['name'][0] == 'Apple Inc.'


class TestBackendTypes:
    """Tests for backend types."""

    def test_backend_enum(self):
        """Test MaterializationBackend enum."""
        assert MaterializationBackend.MEMORY.value == 2
        assert MaterializationBackend.ROCKSDB.value == 0
        assert MaterializationBackend.TONBO.value == 1

    def test_population_strategy_enum(self):
        """Test PopulationStrategy enum."""
        assert PopulationStrategy.STREAM.value == 0
        assert PopulationStrategy.BATCH.value == 1
        assert PopulationStrategy.OPERATOR.value == 2
        assert PopulationStrategy.CDC.value == 3


class TestErrorHandling:
    """Tests for error handling."""

    def test_missing_key_error(self, py_mgr, arrow_file):
        """Test KeyError for missing key."""
        dim_table = py_mgr.dimension_table(
            name='test_securities',
            source=str(arrow_file),
            key='security_id',
            backend='memory'
        )

        with pytest.raises(KeyError):
            _ = dim_table['SEC999']

    def test_missing_file_error(self, cython_mgr):
        """Test FileNotFoundError for missing file."""
        dim_table = cython_mgr.create_dimension_table(
            name='test_securities',
            key_column='security_id',
            backend='memory'
        )

        with pytest.raises(FileNotFoundError):
            dim_table.populate_from_arrow_file('/nonexistent/path/file.arrow')


class TestSchema:
    """Tests for schema operations."""

    def test_schema_retrieval(self, py_mgr, arrow_file):
        """Test schema retrieval."""
        dim_table = py_mgr.dimension_table(
            name='test_securities',
            source=str(arrow_file),
            key='security_id',
            backend='memory'
        )

        schema = dim_table.schema()
        assert schema is not None
        assert len(schema) == 5

        field_names = [f.name for f in schema]
        assert 'security_id' in field_names
        assert 'name' in field_names


class TestVersioning:
    """Tests for version tracking."""

    def test_version(self, cython_mgr, arrow_file):
        """Test version tracking."""
        dim_table = cython_mgr.create_dimension_table(
            name='test_securities',
            key_column='security_id',
            backend='memory'
        )
        dim_table.populate_from_arrow_file(str(arrow_file))

        version = dim_table.version()
        assert version >= 0


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
