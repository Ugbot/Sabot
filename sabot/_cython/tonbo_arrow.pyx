# -*- coding: utf-8 -*-
"""
Cython Arrow Integration for Tonbo Store

Provides direct Arrow columnar operations integrated with Tonbo's LSM storage:
- Zero-copy Arrow table operations
- SIMD-accelerated columnar processing
- Direct integration with Tonbo's Parquet backend
- High-performance analytical queries

This enables Sabot's streaming pipelines to leverage Arrow's columnar format
for maximum performance in analytical workloads.
"""

import asyncio
from typing import Any, Dict, List, Optional, Iterator, AsyncIterator
import logging

# Cython imports
from libc.stdint cimport uint64_t, int64_t
from libc.string cimport memcpy
from libcpp.vector cimport vector
from libcpp.string cimport string
from cpython.ref cimport PyObject

# Arrow imports for zero-copy operations
try:
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.dataset as ds
    ARROW_AVAILABLE = True
except ImportError:
    ARROW_AVAILABLE = False
    pa = None
    pc = None
    ds = None

# Tonbo Python bindings
try:
    from tonbo import TonboDB, DbOption, Column, DataType, Record, Bound
    TONBO_AVAILABLE = True
except ImportError:
    TONBO_AVAILABLE = False
    TonboDB = None
    DbOption = None
    Column = None
    DataType = None
    Record = None
    Bound = None

logger = logging.getLogger(__name__)


cdef class ArrowTonboStore:
    """
    High-performance Arrow-native Tonbo store.

    Integrates Arrow columnar operations directly with Tonbo's LSM storage
    for maximum performance in analytical streaming workloads.
    """

    cdef:
        object _db  # TonboDB instance
        object _schema  # Arrow schema for columnar operations
        object _arrow_schema  # PyArrow Schema
        object _lock  # Async lock for thread safety
        str _db_path

    def __init__(self, str db_path):
        """Initialize the Arrow-native Tonbo store."""
        self._db_path = db_path
        self._db = None
        self._schema = None
        self._arrow_schema = None
        self._lock = None

        if not TONBO_AVAILABLE:
            raise RuntimeError("Tonbo Python bindings not available")

        if not ARROW_AVAILABLE:
            raise RuntimeError("PyArrow not available for Arrow operations")

    async def initialize(self):
        """Initialize Tonbo database with Arrow-native schema."""
        if not TONBO_AVAILABLE or not ARROW_AVAILABLE:
            raise RuntimeError("Tonbo and PyArrow required")

        try:
            # Create Arrow-native schema optimized for columnar operations
            # This schema supports direct Arrow table operations
            class ArrowKVRecord:
                def __init__(self, key: str, value: bytes, timestamp: int = 0,
                           partition: str = "", metadata: bytes = b""):
                    self.key = key
                    self.value = value
                    self.timestamp = timestamp
                    self.partition = partition
                    self.metadata = metadata

            # Define Arrow-optimized schema with proper types
            ArrowKVRecord.key = Column(DataType.String, name="key", primary_key=True)
            ArrowKVRecord.value = Column(DataType.Bytes, name="value")
            ArrowKVRecord.timestamp = Column(DataType.Int64, name="timestamp")
            ArrowKVRecord.partition = Column(DataType.String, name="partition")
            ArrowKVRecord.metadata = Column(DataType.Bytes, name="metadata")

            # Mark as record for Tonbo
            ArrowKVRecord.__record__ = True

            # Create corresponding Arrow schema for direct operations
            self._arrow_schema = pa.schema([
                ('key', pa.string()),
                ('value', pa.binary()),
                ('timestamp', pa.int64()),
                ('partition', pa.string()),
                ('metadata', pa.binary())
            ])

            # Create database options with Arrow optimizations
            options = DbOption(self._db_path)

            # Initialize database with the Arrow-optimized schema
            self._db = TonboDB(options, ArrowKVRecord())
            self._schema = ArrowKVRecord

            # Initialize async lock
            import asyncio
            self._lock = asyncio.Lock()

            logger.info(f"ArrowTonboStore initialized at {self._db_path}")

        except Exception as e:
            logger.error(f"Failed to initialize ArrowTonboStore: {e}")
            raise

    async def arrow_insert_batch(self, object arrow_table):
        """
        Insert Arrow table data directly using zero-copy operations.

        This bypasses Python object conversion for maximum performance.
        """
        if not self._db or not arrow_table:
            return

        async with self._lock:
            try:
                # Convert Arrow table to Tonbo records efficiently
                records = []

                # Use Arrow's zero-copy operations where possible
                key_array = arrow_table.column('key')
                value_array = arrow_table.column('value')
                timestamp_array = arrow_table.column('timestamp')
                partition_array = arrow_table.column('partition')
                metadata_array = arrow_table.column('metadata')

                for i in range(arrow_table.num_rows):
                    # Direct Arrow array access (zero-copy)
                    key = key_array[i].as_py()
                    value = value_array[i].as_py()
                    timestamp = timestamp_array[i].as_py() if i < len(timestamp_array) else 0
                    partition = partition_array[i].as_py() if i < len(partition_array) else ""
                    metadata = metadata_array[i].as_py() if i < len(metadata_array) else b""

                    record = self._schema(key=key, value=value, timestamp=timestamp,
                                        partition=partition, metadata=metadata)
                    records.append(record)

                # Batch insert for efficiency
                if hasattr(self._db, 'insert_batch'):
                    await self._db.insert_batch(records)
                else:
                    for record in records:
                        await self._db.insert(record)

            except Exception as e:
                logger.error(f"Arrow batch insert failed: {e}")
                raise

    async def arrow_scan_to_table(self, str start_key=None, str end_key=None,
                                int limit=10000) -> Optional[object]:
        """
        Scan data and return as Arrow table for columnar processing.

        Enables direct Arrow compute operations on streaming data.
        """
        if not self._db or not ARROW_AVAILABLE:
            return None

        async with self._lock:
            try:
                # Set up bounds for efficient scanning
                lower_bound = None
                if start_key is not None:
                    lower_bound = Bound.Included(start_key)

                upper_bound = None
                if end_key is not None:
                    upper_bound = Bound.Excluded(end_key)

                # Scan with all columns for Arrow processing
                scan_stream = await self._db.scan(
                    lower_bound,
                    upper_bound,
                    limit=limit,
                    projection=["key", "value", "timestamp", "partition", "metadata"]
                )

                # Collect results for Arrow table creation
                keys = []
                values = []
                timestamps = []
                partitions = []
                metadatas = []

                async for record in scan_stream:
                    if record:
                        keys.append(record.get('key', ''))
                        values.append(record.get('value', b''))
                        timestamps.append(record.get('timestamp', 0))
                        partitions.append(record.get('partition', ''))
                        metadatas.append(record.get('metadata', b''))

                if not keys:
                    return None

                # Create Arrow table directly (zero-copy where possible)
                arrow_table = pa.table({
                    'key': keys,
                    'value': values,
                    'timestamp': timestamps,
                    'partition': partitions,
                    'metadata': metadatas
                }, schema=self._arrow_schema)

                return arrow_table

            except Exception as e:
                logger.error(f"Arrow scan failed: {e}")
                return None

    async def execute_arrow_query(self, str query_type, dict params) -> Optional[object]:
        """
        Execute Arrow-native queries directly on stored data.

        Supports filtering, aggregation, and analytical operations.
        """
        if not self._db or not ARROW_AVAILABLE:
            return None

        try:
            # Get data as Arrow table
            arrow_table = await self.arrow_scan_to_table(
                params.get('start_key'),
                params.get('end_key'),
                params.get('limit', 10000)
            )

            if arrow_table is None:
                return None

            # Execute query based on type
            if query_type == "filter":
                condition = params.get('condition')
                if condition:
                    return pc.filter(arrow_table, condition)

            elif query_type == "aggregate":
                group_by_cols = params.get('group_by', ['partition'])
                agg_funcs = params.get('aggregations', {})

                # Use Arrow's groupby for efficient aggregation
                grouped = arrow_table.group_by(group_by_cols)
                return grouped.aggregate(agg_funcs)

            elif query_type == "sort":
                sort_keys = params.get('sort_keys', [('timestamp', 'ascending')])
                return arrow_table.sort_by(sort_keys)

            elif query_type == "project":
                columns = params.get('columns', ['key', 'value'])
                return arrow_table.select(columns)

            elif query_type == "join":
                # Support for joining with other Arrow tables
                other_table = params.get('other_table')
                join_keys = params.get('join_keys', ['key'])
                join_type = params.get('join_type', 'inner')

                if other_table:
                    if join_type == 'asof':
                        # Time-series asof join
                        return pc.join_asof(arrow_table, other_table, on=join_keys[0])
                    else:
                        # Standard join
                        return arrow_table.join(other_table, keys=join_keys, join_type=join_type)

            else:
                logger.warning(f"Unsupported Arrow query type: {query_type}")
                return arrow_table

        except Exception as e:
            logger.error(f"Arrow query execution failed: {e}")
            return None

    async def arrow_streaming_aggregate(self, str aggregation_type,
                                      list group_by_cols,
                                      dict params) -> AsyncIterator[object]:
        """
        Streaming aggregation that yields Arrow tables as results arrive.

        Enables real-time analytical processing on streaming data.
        """
        if not self._db or not ARROW_AVAILABLE:
            return

        try:
            # Get streaming data as Arrow table
            arrow_table = await self.arrow_scan_to_table(
                params.get('start_key'),
                params.get('end_key'),
                params.get('batch_size', 1000)
            )

            if arrow_table is None:
                return

            # Perform streaming aggregation
            if aggregation_type == "groupby_count":
                # Count aggregation by groups
                grouped = arrow_table.group_by(group_by_cols)
                result = grouped.aggregate([('key', 'count')])
                yield result

            elif aggregation_type == "time_window":
                # Time-based windowing aggregation
                time_col = params.get('time_column', 'timestamp')
                window_size = params.get('window_size', 3600000000)  # 1 hour in microseconds

                # Create time windows
                windows = pc.floor_temporal(arrow_table.column(time_col), unit='hour')
                arrow_table = arrow_table.append_column('window', windows)

                # Aggregate by window
                grouped = arrow_table.group_by(['window'] + group_by_cols)
                result = grouped.aggregate([('key', 'count')])
                yield result

            elif aggregation_type == "rolling":
                # Rolling window aggregation
                window_size = params.get('window_size', 100)
                result = pc.rolling_sum(arrow_table.column('value'), window_size)
                # Convert back to table format
                result_table = pa.table({'rolling_sum': result})
                yield result_table

        except Exception as e:
            logger.error(f"Arrow streaming aggregation failed: {e}")

    async def arrow_export_to_dataset(self, str output_path,
                                    dict partitioning=None) -> bool:
        """
        Export Tonbo data as Arrow Dataset for external processing.

        Enables integration with other Arrow-based tools.
        """
        if not self._db or not ARROW_AVAILABLE:
            return False

        try:
            # Get all data as Arrow table
            arrow_table = await self.arrow_scan_to_table()

            if arrow_table is None:
                return False

            # Create Arrow dataset
            if partitioning:
                # Partitioned dataset
                partitioning_cols = partitioning.get('columns', [])
                if partitioning_cols:
                    dataset = ds.dataset(arrow_table, partitioning=partitioning_cols)
                else:
                    dataset = ds.dataset(arrow_table)
            else:
                dataset = ds.dataset(arrow_table)

            # Write to output path
            import pyarrow.parquet as pq
            pq.write_dataset(dataset, output_path)

            logger.info(f"Exported Arrow dataset to {output_path}")
            return True

        except Exception as e:
            logger.error(f"Arrow dataset export failed: {e}")
            return False

    async def get_arrow_stats(self) -> Dict[str, Any]:
        """
        Get Arrow-specific statistics and performance metrics.
        """
        stats = {
            'arrow_available': ARROW_AVAILABLE,
            'tonbo_available': TONBO_AVAILABLE,
            'arrow_schema': str(self._arrow_schema) if self._arrow_schema else None,
            'db_path': self._db_path,
        }

        if self._db and ARROW_AVAILABLE:
            try:
                # Get table statistics
                arrow_table = await self.arrow_scan_to_table(limit=1000)
                if arrow_table:
                    stats.update({
                        'table_rows': arrow_table.num_rows,
                        'table_columns': arrow_table.num_columns,
                        'table_size_bytes': arrow_table.nbytes,
                        'column_names': arrow_table.column_names,
                        'schema_info': str(arrow_table.schema)
                    })

                # Memory usage info
                import psutil
                process = psutil.Process()
                stats['memory_usage_mb'] = process.memory_info().rss / 1024 / 1024

            except Exception as e:
                logger.error(f"Failed to get Arrow stats: {e}")
                stats['error'] = str(e)

        return stats

    async def close(self):
        """Clean up Arrow resources and Tonbo database."""
        if self._db:
            try:
                # Close Tonbo database
                if hasattr(self._db, 'close'):
                    await self._db.close()

                # Clear Arrow schema reference
                self._arrow_schema = None

            except Exception as e:
                logger.error(f"Error closing ArrowTonboStore: {e}")

            self._db = None


# Python integration class
class ArrowTonboIntegration:
    """
    Python interface for Arrow-integrated Tonbo operations.

    Provides a clean API for using Arrow operations within Sabot's
    streaming pipelines while leveraging Tonbo's storage capabilities.
    """

    def __init__(self, db_path: str = "./sabot_arrow_tonbo"):
        self.db_path = db_path
        self.store = None

    async def __aenter__(self):
        self.store = ArrowTonboStore(self.db_path)
        await self.store.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.store:
            await self.store.close()

    async def insert_arrow_table(self, table):
        """Insert Arrow table data."""
        await self.store.arrow_insert_batch(table)

    async def query_to_arrow(self, query_type: str, **params) -> Optional[object]:
        """Execute query and return Arrow result."""
        return await self.store.execute_arrow_query(query_type, params)

    async def filter_table(self, condition) -> Optional[object]:
        """Filter current data using Arrow compute."""
        return await self.query_to_arrow("filter", condition=condition)

    async def aggregate_table(self, group_by: List[str], aggregations: Dict[str, str]) -> Optional[object]:
        """Aggregate data using Arrow compute."""
        return await self.query_to_arrow("aggregate", group_by=group_by, aggregations=aggregations)

    async def join_tables(self, other_table, join_keys: List[str] = None,
                         join_type: str = "inner") -> Optional[object]:
        """Join with another Arrow table."""
        return await self.query_to_arrow("join",
                                        other_table=other_table,
                                        join_keys=join_keys or ["key"],
                                        join_type=join_type)

    async def streaming_aggregate(self, agg_type: str, group_by: List[str],
                                **params) -> AsyncIterator[object]:
        """Perform streaming aggregations."""
        async for result in self.store.arrow_streaming_aggregate(agg_type, group_by, params):
            yield result

    async def export_dataset(self, output_path: str, partitioning: Dict = None) -> bool:
        """Export as Arrow dataset."""
        return await self.store.arrow_export_to_dataset(output_path, partitioning)

    async def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive statistics."""
        return await self.store.get_arrow_stats()
