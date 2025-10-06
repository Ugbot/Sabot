# -*- coding: utf-8 -*-
"""
Materialization Management (Python API)

Unified API for dimension tables and analytical views.
Thin Python wrapper around Cython engine with operator overloading.

Usage:
    # Create manager
    mat_mgr = app.materializations()

    # Dimension table
    securities = mat_mgr.dimension_table(
        'securities',
        source='data/securities.arrow',
        key='security_id'
    )

    # Use with operators
    if 'SEC123' in securities:
        data = securities['SEC123']

    # Enrich stream
    enriched = securities @ stream  # Operator overloading!
"""

from typing import Any, Dict, List, Optional, Union
from pathlib import Path

# Import Cython engine
try:
    from ._c.materialization_engine import (
        MaterializationManager as CMaterializationManager,
        Materialization as CMaterialization,
        MaterializationBackend,
    )
    CYTHON_AVAILABLE = True
except ImportError:
    CYTHON_AVAILABLE = False
    CMaterializationManager = None
    CMaterialization = None
    MaterializationBackend = None


class MaterializationManager:
    """
    Manager for all materializations.

    Creates both dimension tables and analytical views.
    """

    def __init__(self, app, default_backend: str = 'memory'):
        """
        Initialize materialization manager.

        Args:
            app: Sabot App instance
            default_backend: Default storage backend ('memory', 'rocksdb', 'tonbo')
        """
        self.app = app
        self.default_backend = default_backend

        if not CYTHON_AVAILABLE:
            raise RuntimeError(
                "Cython materialization engine not available. "
                "Build Cython extensions: python setup.py build_ext --inplace"
            )

        # Create Cython manager
        self._cython_mgr = CMaterializationManager(default_backend)

        # Track Python wrappers
        self._dim_tables: Dict[str, DimensionTableView] = {}
        self._analytical_views: Dict[str, AnalyticalViewAPI] = {}

    def dimension_table(
        self,
        name: str,
        source: Union[str, Path, object],
        key: str,
        backend: str = None,
        refresh_interval: Optional[int] = None
    ) -> 'DimensionTableView':
        """
        Create dimension table (lookup-optimized).

        A dimension table is a materialization optimized for lookups.
        It can be populated from:
        - File: Arrow IPC for 52x faster loading
        - Stream: Continuous updates from stream
        - Operator: Output of another operator
        - Agent: Data from another agent

        Args:
            name: Table name
            source: Data source (file path, stream, operator output)
            key: Key column for lookups
            backend: Storage backend ('memory', 'rocksdb', 'tonbo')
            refresh_interval: Seconds between refreshes (None = no refresh)

        Returns:
            DimensionTableView with operator overloading

        Examples:
            # From file (batch load)
            securities = mgr.dimension_table(
                'securities',
                source='data/securities.arrow',
                key='security_id'
            )

            # From stream (continuous updates)
            users = mgr.dimension_table(
                'users',
                source=user_stream,
                key='user_id'
            )

            # Use with operators
            if 'SEC123' in securities:
                print(securities['SEC123'])

            # Enrich stream
            enriched = securities @ quotes_stream
        """
        backend = backend or self.default_backend

        # Create Cython materialization
        cython_mat = self._cython_mgr.create_dimension_table(
            name=name,
            key_column=key,
            backend=backend
        )

        # Populate based on source type
        if isinstance(source, (str, Path)):
            # File source - load Arrow IPC
            source_path = str(source)

            # Auto-detect Arrow IPC vs CSV
            if source_path.endswith('.arrow') or source_path.endswith('.feather'):
                cython_mat.populate_from_arrow_file(source_path)
            elif source_path.endswith('.csv'):
                # Convert CSV to Arrow batch and populate
                import pyarrow.csv as pa_csv
                table = pa_csv.read_csv(source_path)
                if table.num_rows > 0:
                    batch = table.to_batches()[0]
                    cython_mat.populate_from_arrow_batch(batch)
            else:
                # Try as Arrow IPC
                cython_mat.populate_from_arrow_file(source_path)

        elif hasattr(source, '__iter__'):
            # Stream source - will be populated by stream processor
            # TODO: Setup stream consumer
            pass

        elif hasattr(source, 'output'):
            # Operator output
            # TODO: Setup operator output handler
            pass

        # TODO: Setup refresh if specified
        # if refresh_interval:
        #     self._schedule_refresh(cython_mat, refresh_interval)

        # Wrap in Python API
        view = DimensionTableView(cython_mat, name)
        self._dim_tables[name] = view

        return view

    def analytical_view(
        self,
        name: str,
        source: object,
        group_by: Optional[List[str]] = None,
        aggregations: Optional[Dict[str, str]] = None,
        backend: str = 'tonbo'
    ) -> 'AnalyticalViewAPI':
        """
        Create analytical view (scan-optimized).

        An analytical view is a materialization optimized for scans.
        Uses columnar storage (Tonbo) for efficient analytical queries.

        Args:
            name: View name
            source: Stream or operator output
            group_by: Group-by column names
            aggregations: Aggregation specs (col -> agg_func)
            backend: Storage backend ('tonbo' recommended, 'memory' also works)

        Returns:
            AnalyticalViewAPI for querying

        Example:
            # Aggregation view
            user_stats = mgr.analytical_view(
                'user_stats',
                source=events_stream,
                group_by=['user_id'],
                aggregations={
                    'event_count': 'count',
                    'total_spent': 'sum'
                }
            )

            # Query
            for row in user_stats:
                print(row)
        """
        # Create Cython materialization
        cython_mat = self._cython_mgr.create_analytical_view(
            name=name,
            group_by_keys=group_by,
            aggregations=aggregations,
            backend=backend
        )

        # TODO: Setup stream consumer for population

        # Wrap in Python API
        view = AnalyticalViewAPI(cython_mat, name)
        self._analytical_views[name] = view

        return view

    def get(self, name: str) -> Union['DimensionTableView', 'AnalyticalViewAPI', None]:
        """Get materialization by name."""
        if name in self._dim_tables:
            return self._dim_tables[name]
        elif name in self._analytical_views:
            return self._analytical_views[name]
        return None

    def list_names(self) -> List[str]:
        """List all materialization names."""
        names_bytes = self._cython_mgr.list_names()
        return [name.decode('utf-8') for name in names_bytes]


class DimensionTableView:
    """
    Python view of dimension table with operator overloading.

    Provides ergonomic API over Cython engine.
    """

    def __init__(self, cython_mat: CMaterialization, name: str):
        """
        Create dimension table view.

        Args:
            cython_mat: Cython Materialization instance
            name: Table name
        """
        self._cython_mat = cython_mat
        self.name = name

    # === Operator Overloading ===

    def __getitem__(self, key):
        """
        Lookup by key: dim['key']

        Returns:
            Dict with row data, or None if not found
        """
        result = self._cython_mat.lookup(key)
        if result is None:
            raise KeyError(f"Key '{key}' not found in dimension table '{self.name}'")

        # Convert Arrow dict to regular dict (single row)
        if isinstance(result, dict):
            # Extract values from lists (Arrow batch to_pydict returns lists)
            return {k: v[0] if isinstance(v, list) and len(v) > 0 else v
                    for k, v in result.items()}
        return result

    def __contains__(self, key):
        """
        Check existence: key in dim

        Returns:
            True if key exists, False otherwise
        """
        return self._cython_mat.contains(key)

    def __len__(self):
        """
        Row count: len(dim)

        Returns:
            Number of rows
        """
        return self._cython_mat.num_rows()

    def __matmul__(self, stream):
        """
        Enrich stream: dim @ stream

        This is the dimension table enrichment operator.

        Args:
            stream: Stream to enrich

        Returns:
            EnrichedStream that yields enriched records

        Example:
            enriched = securities @ quotes_stream
            async for quote in enriched:
                print(quote['security']['name'])
        """
        return EnrichedStream(self._cython_mat, stream, self.name)

    # === Methods ===

    def lookup(self, key):
        """
        Lookup by key (same as __getitem__).

        Returns:
            Dict with row data
        """
        return self[key]

    def get(self, key, default=None):
        """
        Safe lookup (returns default if not found).

        Returns:
            Row data or default value
        """
        try:
            return self[key]
        except KeyError:
            return default

    def refresh(self):
        """Trigger refresh from source."""
        self._cython_mat.refresh()

    def num_rows(self) -> int:
        """Get row count."""
        return self._cython_mat.num_rows()

    def schema(self):
        """Get Arrow schema."""
        return self._cython_mat.schema()

    def scan(self, limit: int = None):
        """
        Scan all rows (for debugging/inspection).

        Args:
            limit: Max rows to return

        Returns:
            Arrow RecordBatch
        """
        return self._cython_mat.scan(limit=limit if limit else -1)


class AnalyticalViewAPI:
    """
    Python API for analytical view.

    Provides scan and query operations.
    """

    def __init__(self, cython_mat: CMaterialization, name: str):
        """
        Create analytical view API.

        Args:
            cython_mat: Cython Materialization instance
            name: View name
        """
        self._cython_mat = cython_mat
        self.name = name

    def __iter__(self):
        """
        Iterate over rows: for row in view

        Yields:
            Row dicts
        """
        batch = self._cython_mat.scan()
        if batch is not None:
            # Convert to dicts
            pydict = batch.to_pydict()
            num_rows = batch.num_rows

            # Yield row by row
            for i in range(num_rows):
                row = {k: v[i] for k, v in pydict.items()}
                yield row

    def __len__(self):
        """Row count."""
        return self._cython_mat.num_rows()

    def __getitem__(self, key_or_slice):
        """
        Query: view['key'] or view[0:100]

        Args:
            key_or_slice: Key or slice

        Returns:
            Row data or batch slice
        """
        if isinstance(key_or_slice, slice):
            # Slice: return batch
            limit = key_or_slice.stop if key_or_slice.stop else -1
            return self._cython_mat.scan(limit=limit)
        else:
            # Key: lookup
            return self._cython_mat.lookup(key_or_slice)

    def scan(self, limit: int = None):
        """
        Scan rows.

        Args:
            limit: Max rows to return

        Returns:
            Arrow RecordBatch
        """
        return self._cython_mat.scan(limit=limit if limit else -1)

    def query(self, filter_expr: str):
        """
        Query with filter expression.

        Args:
            filter_expr: Arrow compute filter expression

        Returns:
            Filtered RecordBatch
        """
        return self._cython_mat.scan(filter_expr=filter_expr)

    def to_pandas(self):
        """Convert to pandas DataFrame."""
        batch = self._cython_mat.scan()
        return batch.to_pandas() if batch else None

    def to_arrow(self):
        """Get as Arrow Table."""
        import pyarrow as pa
        batch = self._cython_mat.scan()
        return pa.Table.from_batches([batch]) if batch else None


class EnrichedStream:
    """
    Stream enriched with dimension table data.

    Wraps a stream and enriches each batch with dimension table lookups.
    """

    def __init__(self, dim_mat: CMaterialization, stream, dim_name: str):
        """
        Create enriched stream.

        Args:
            dim_mat: Dimension table materialization
            stream: Source stream
            dim_name: Dimension table name (for field prefixing)
        """
        self._dim_mat = dim_mat
        self._stream = stream
        self._dim_name = dim_name

    def __aiter__(self):
        """Make async iterable."""
        return self

    async def __anext__(self):
        """
        Get next enriched record.

        For now, this is a simplified implementation.
        Real implementation should:
        1. Batch stream records
        2. Use enrich_batch() for zero-copy join
        3. Yield enriched records
        """
        # Get next from stream
        record = await self._stream.__anext__()

        # TODO: Implement batching and zero-copy enrichment
        # For now, just return original record
        return record


# Convenience function
def get_materialization_manager(app, default_backend: str = 'memory') -> MaterializationManager:
    """
    Get or create materialization manager for app.

    Args:
        app: Sabot App instance
        default_backend: Default storage backend

    Returns:
        MaterializationManager instance
    """
    if not hasattr(app, '_materialization_manager'):
        app._materialization_manager = MaterializationManager(app, default_backend)

    return app._materialization_manager
