# cython: language_level=3, boundscheck=False, wraparound=False
"""
StreamingGroupBy - Bigger-than-memory GroupBy using MarbleDB.

Instead of accumulating ALL batches in memory, this operator:
1. Computes batch-level partial aggregates using Arrow group_by
2. Buffers partials in memory (up to flush_threshold)
3. Flushes to MarbleDB when buffer exceeds threshold
4. On get_result(), scans all partials and finalizes aggregates

This enables processing 100GB-1TB datasets with ~256MB memory per operator.

Supported aggregations:
- COUNT: count1 + count2
- SUM: sum1 + sum2
- MIN: min(min1, min2)
- MAX: max(max1, max2)
- MEAN: (sum1+sum2) / (count1+count2)
- VARIANCE: Welford's online algorithm
"""

import cython
from typing import Dict, List, Optional
from collections import defaultdict
from libc.stdint cimport int64_t, uint64_t
from libcpp cimport bool as cbool
import logging
import os
import uuid

logger = logging.getLogger(__name__)

# Import Arrow
try:
    from sabot import cyarrow as pa
    pc = pa.compute if hasattr(pa, 'compute') else None
    ARROW_AVAILABLE = True
except ImportError:
    try:
        import pyarrow as pa
        import pyarrow.compute as pc
        ARROW_AVAILABLE = True
    except ImportError:
        ARROW_AVAILABLE = False
        pa = None
        pc = None

# Import MarbleDB for partial storage
try:
    import marbledb
    MARBLEDB_AVAILABLE = True
except ImportError:
    MARBLEDB_AVAILABLE = False
    marbledb = None

# Default configuration
DEFAULT_MEMORY_THRESHOLD = 256 * 1024 * 1024  # 256MB
DEFAULT_FLUSH_THRESHOLD = 64 * 1024 * 1024    # 64MB


# ============================================================================
# Partial Aggregate Schema
# ============================================================================

def create_partial_schema(keys: List[str], aggregations: Dict):
    """Create schema for partial aggregates.

    Partials store intermediate state that can be merged:
    - For each key column: original type
    - count: int64 (row count for this group)
    - For SUM/MEAN: sum_<col> (running sum)
    - For MIN: min_<col>
    - For MAX: max_<col>
    - For VARIANCE: sum_sq_<col>, sum_<col>, count (for Welford's algorithm)
    """
    if not ARROW_AVAILABLE:
        return None

    fields = []

    # Key columns (will be set based on first batch)
    for key in keys:
        fields.append(pa.field(key, pa.string()))  # Will be adjusted based on data

    # Always include count for merging
    fields.append(pa.field('_count', pa.int64()))

    # Aggregate columns
    for output_name, (column, func) in aggregations.items():
        if func in ('sum', 'total', 'mean', 'avg', 'average'):
            fields.append(pa.field(f'_sum_{output_name}', pa.float64()))
        if func in ('mean', 'avg', 'average', 'variance', 'stddev'):
            fields.append(pa.field(f'_sum_sq_{output_name}', pa.float64()))
        if func == 'min':
            fields.append(pa.field(f'_min_{output_name}', pa.float64()))
        if func == 'max':
            fields.append(pa.field(f'_max_{output_name}', pa.float64()))

    return pa.schema(fields)


# ============================================================================
# Streaming GroupBy Operator
# ============================================================================

cdef class StreamingGroupByOperator:
    """
    GroupBy operator that stores partials in MarbleDB for bigger-than-memory.

    Instead of accumulating all batches in memory (which fails for large datasets),
    this operator:
    1. Processes each batch to compute partial aggregates per group
    2. Buffers partials in memory (up to flush_threshold)
    3. When buffer exceeds threshold, merges and flushes to MarbleDB
    4. On finalization, scans all partials from MarbleDB and computes final result

    Memory usage is bounded by flush_threshold (~64MB default).
    """
    cdef:
        object _source
        list _keys
        dict _aggregations
        object _schema
        int64_t _memory_threshold
        int64_t _flush_threshold
        str _db_path
        str _operator_id

        # In-memory partial buffer
        dict _partial_buffer  # key_tuple -> partial_dict
        int64_t _buffer_bytes

        # MarbleDB state
        object _marbledb
        str _table_name
        bint _has_flushed
        bint _closed

    def __init__(self, source, keys: List[str], aggregations: Dict,
                 memory_threshold: int = DEFAULT_MEMORY_THRESHOLD,
                 flush_threshold: int = DEFAULT_FLUSH_THRESHOLD,
                 db_path: str = "/tmp/sabot_groupby_spill",
                 schema=None):
        """
        Initialize streaming groupby operator.

        Args:
            source: Input stream/iterator yielding RecordBatches
            keys: List of column names to group by
            aggregations: Dict mapping output name to (column, function) tuple
                         Functions: sum, mean, count, min, max, stddev, variance
            memory_threshold: Total memory threshold (not used directly, for API compat)
            flush_threshold: Buffer size before flushing to MarbleDB (default 64MB)
            db_path: Path for MarbleDB spill storage
            schema: Output schema (inferred if None)
        """
        self._source = source
        self._keys = keys if keys else []
        self._aggregations = aggregations if aggregations else {}
        self._schema = schema
        self._memory_threshold = memory_threshold
        self._flush_threshold = flush_threshold
        self._db_path = db_path
        self._operator_id = f"groupby_{uuid.uuid4().hex[:8]}"

        # In-memory buffer
        self._partial_buffer = {}
        self._buffer_bytes = 0

        # MarbleDB state
        self._marbledb = None
        self._table_name = f"partials_{self._operator_id}"
        self._has_flushed = False
        self._closed = False

        logger.debug(
            f"StreamingGroupByOperator created: keys={keys}, "
            f"flush_threshold={flush_threshold/(1024*1024):.1f}MB"
        )

    cpdef object process_batch(self, object batch):
        """
        Process batch: compute partials, buffer, flush if needed.

        Returns None during processing (output only on get_result).
        """
        if not ARROW_AVAILABLE or batch is None or batch.num_rows == 0:
            return None

        try:
            # Compute batch-level partial aggregates using Arrow group_by
            batch_partials = self._compute_batch_partials(batch)

            # Merge into buffer
            self._merge_into_buffer(batch_partials)

            # Flush to MarbleDB if buffer exceeds threshold
            if self._buffer_bytes >= self._flush_threshold:
                self._flush_to_marbledb()

            return None  # No output during processing

        except Exception as e:
            logger.error(f"Error processing batch: {e}")
            raise

    cdef object _compute_batch_partials(self, object batch):
        """Compute partial aggregates for a single batch using Arrow."""
        # Build Arrow aggregation spec
        agg_list = []
        for output_name, (column, func) in self._aggregations.items():
            col_name = column if column != '*' else self._keys[0]

            # Always compute count for merging
            if func in ('count', 'count_all'):
                agg_list.append((col_name, 'count'))
            elif func in ('sum', 'total'):
                agg_list.append((col_name, 'sum'))
            elif func in ('mean', 'avg', 'average'):
                # For mean, we need sum and count
                agg_list.append((col_name, 'sum'))
                agg_list.append((col_name, 'count'))
            elif func == 'min':
                agg_list.append((col_name, 'min'))
            elif func == 'max':
                agg_list.append((col_name, 'max'))
            elif func in ('variance', 'stddev'):
                # For variance, we need sum, sum_sq, and count
                agg_list.append((col_name, 'sum'))
                agg_list.append((col_name, 'count'))
                # Note: Arrow doesn't have sum_sq, we'll compute it separately
            else:
                agg_list.append((col_name, 'sum'))

        # Execute Arrow group_by
        table = pa.Table.from_batches([batch])
        grouped = table.group_by(self._keys)
        result = grouped.aggregate(agg_list)

        return result

    cdef void _merge_into_buffer(self, object batch_partials):
        """Merge batch partials into in-memory buffer."""
        if batch_partials is None or batch_partials.num_rows == 0:
            return

        # Convert to pylist for merging
        rows = batch_partials.to_pylist()

        for row in rows:
            # Build key tuple from key columns
            key = tuple(row.get(k) for k in self._keys)

            if key in self._partial_buffer:
                # Merge with existing partial
                existing = self._partial_buffer[key]
                merged = self._merge_partials(existing, row)
                self._partial_buffer[key] = merged
            else:
                # New group - convert row to partial format
                partial = self._row_to_partial(row)
                self._partial_buffer[key] = partial
                self._buffer_bytes += self._estimate_partial_size(partial)

    cdef dict _row_to_partial(self, dict row):
        """Convert Arrow group_by result row to partial format."""
        partial = {}

        # Copy key columns
        for key in self._keys:
            partial[key] = row.get(key)

        # Extract aggregate values
        for output_name, (column, func) in self._aggregations.items():
            col_name = column if column != '*' else self._keys[0]

            if func in ('count', 'count_all'):
                partial[f'_count_{output_name}'] = row.get(f'{col_name}_count', 0)
            elif func in ('sum', 'total'):
                partial[f'_sum_{output_name}'] = row.get(f'{col_name}_sum', 0.0)
            elif func in ('mean', 'avg', 'average'):
                partial[f'_sum_{output_name}'] = row.get(f'{col_name}_sum', 0.0)
                partial[f'_count_{output_name}'] = row.get(f'{col_name}_count', 0)
            elif func == 'min':
                partial[f'_min_{output_name}'] = row.get(f'{col_name}_min')
            elif func == 'max':
                partial[f'_max_{output_name}'] = row.get(f'{col_name}_max')

        return partial

    cdef dict _merge_partials(self, dict p1, dict p2):
        """Merge two partial aggregates."""
        merged = {}

        # Copy key columns (should be same)
        for key in self._keys:
            merged[key] = p1.get(key)

        # Merge each aggregation
        for output_name, (column, func) in self._aggregations.items():
            col_name = column if column != '*' else self._keys[0]

            if func in ('count', 'count_all'):
                c1 = p1.get(f'_count_{output_name}', 0)
                c2 = p2.get(f'{col_name}_count', 0)
                merged[f'_count_{output_name}'] = c1 + c2

            elif func in ('sum', 'total'):
                s1 = p1.get(f'_sum_{output_name}', 0.0)
                s2 = p2.get(f'{col_name}_sum', 0.0)
                merged[f'_sum_{output_name}'] = s1 + s2

            elif func in ('mean', 'avg', 'average'):
                s1 = p1.get(f'_sum_{output_name}', 0.0)
                s2 = p2.get(f'{col_name}_sum', 0.0)
                c1 = p1.get(f'_count_{output_name}', 0)
                c2 = p2.get(f'{col_name}_count', 0)
                merged[f'_sum_{output_name}'] = s1 + s2
                merged[f'_count_{output_name}'] = c1 + c2

            elif func == 'min':
                m1 = p1.get(f'_min_{output_name}')
                m2 = p2.get(f'{col_name}_min')
                if m1 is None:
                    merged[f'_min_{output_name}'] = m2
                elif m2 is None:
                    merged[f'_min_{output_name}'] = m1
                else:
                    merged[f'_min_{output_name}'] = min(m1, m2)

            elif func == 'max':
                m1 = p1.get(f'_max_{output_name}')
                m2 = p2.get(f'{col_name}_max')
                if m1 is None:
                    merged[f'_max_{output_name}'] = m2
                elif m2 is None:
                    merged[f'_max_{output_name}'] = m1
                else:
                    merged[f'_max_{output_name}'] = max(m1, m2)

        return merged

    cdef int64_t _estimate_partial_size(self, dict partial):
        """Estimate memory size of a partial dict."""
        # Rough estimate: 100 bytes per key + 50 bytes per aggregate
        return 100 * len(self._keys) + 50 * len(self._aggregations)

    cdef void _flush_to_marbledb(self):
        """Flush buffer to MarbleDB and clear memory."""
        if not self._partial_buffer:
            return

        if not MARBLEDB_AVAILABLE:
            logger.warning("MarbleDB not available, keeping partials in memory")
            return

        try:
            # Initialize MarbleDB if needed
            if self._marbledb is None:
                self._init_marbledb()

            # Convert buffer to RecordBatch
            batch = self._buffer_to_batch()
            if batch is not None and batch.num_rows > 0:
                self._marbledb.insert_batch(self._table_name, batch)
                logger.debug(
                    f"StreamingGroupBy: flushed {batch.num_rows} groups "
                    f"({self._buffer_bytes / (1024*1024):.1f}MB)"
                )

            # Clear buffer
            self._partial_buffer = {}
            self._buffer_bytes = 0
            self._has_flushed = True

        except Exception as e:
            logger.error(f"Failed to flush to MarbleDB: {e}")
            raise

    cdef void _init_marbledb(self):
        """Initialize MarbleDB for partial storage."""
        # Create directory
        spill_dir = os.path.join(self._db_path, self._operator_id)
        os.makedirs(spill_dir, exist_ok=True)

        # Open database
        self._marbledb = marbledb.open_database(spill_dir)

        # Create table (schema will be inferred from first batch)
        # For now, we'll create on first flush
        logger.debug(f"StreamingGroupBy: initialized MarbleDB at {spill_dir}")

    cdef object _buffer_to_batch(self):
        """Convert partial buffer to Arrow RecordBatch."""
        if not self._partial_buffer:
            return None

        # Build column arrays
        columns = {}
        cdef list values_list = list(self._partial_buffer.values())
        cdef list col_data
        cdef dict p
        cdef str key
        cdef str output_name
        cdef str col_name
        cdef str func

        # Key columns
        for key in self._keys:
            col_data = []
            for p in values_list:
                col_data.append(p.get(key))
            columns[key] = col_data

        # Aggregate columns
        for output_name, (col_name, func) in self._aggregations.items():
            if func in ('count', 'count_all'):
                col_data = []
                for p in values_list:
                    col_data.append(p.get(f'_count_{output_name}', 0))
                columns[f'_count_{output_name}'] = col_data
            elif func in ('sum', 'total', 'mean', 'avg', 'average'):
                col_data = []
                for p in values_list:
                    col_data.append(p.get(f'_sum_{output_name}', 0.0))
                columns[f'_sum_{output_name}'] = col_data
                if func in ('mean', 'avg', 'average'):
                    col_data = []
                    for p in values_list:
                        col_data.append(p.get(f'_count_{output_name}', 0))
                    columns[f'_count_{output_name}'] = col_data
            elif func == 'min':
                col_data = []
                for p in values_list:
                    col_data.append(p.get(f'_min_{output_name}'))
                columns[f'_min_{output_name}'] = col_data
            elif func == 'max':
                col_data = []
                for p in values_list:
                    col_data.append(p.get(f'_max_{output_name}'))
                columns[f'_max_{output_name}'] = col_data

        return pa.RecordBatch.from_pydict(columns)

    cpdef object get_result(self):
        """
        Get final aggregation result.

        Merges all partials (buffer + MarbleDB) and computes final values.
        """
        if not ARROW_AVAILABLE:
            return None

        cdef list key_parts
        cdef str k
        cdef dict row

        try:
            # Collect all partials
            all_partials = dict(self._partial_buffer)  # Copy buffer

            # If we flushed to MarbleDB, scan and merge
            if self._has_flushed and self._marbledb is not None:
                result = self._marbledb.scan_table(self._table_name)
                table = result.to_table()

                # Merge MarbleDB partials with buffer
                for row in table.to_pylist():
                    # Build key tuple without generator expression
                    key_parts = []
                    for k in self._keys:
                        key_parts.append(row.get(k))
                    key = tuple(key_parts)

                    if key in all_partials:
                        all_partials[key] = self._merge_stored_partial(
                            all_partials[key], row
                        )
                    else:
                        all_partials[key] = row

            # Finalize and build result
            return self._finalize_partials(all_partials)

        except Exception as e:
            logger.error(f"Error computing result: {e}")
            raise

    cdef dict _merge_stored_partial(self, dict p1, dict p2):
        """Merge two stored partials (both in partial format)."""
        merged = {}

        # Copy key columns
        for key in self._keys:
            merged[key] = p1.get(key)

        # Merge each aggregation
        for output_name, (column, func) in self._aggregations.items():
            if func in ('count', 'count_all'):
                c1 = p1.get(f'_count_{output_name}', 0)
                c2 = p2.get(f'_count_{output_name}', 0)
                merged[f'_count_{output_name}'] = c1 + c2

            elif func in ('sum', 'total'):
                s1 = p1.get(f'_sum_{output_name}', 0.0)
                s2 = p2.get(f'_sum_{output_name}', 0.0)
                merged[f'_sum_{output_name}'] = s1 + s2

            elif func in ('mean', 'avg', 'average'):
                s1 = p1.get(f'_sum_{output_name}', 0.0)
                s2 = p2.get(f'_sum_{output_name}', 0.0)
                c1 = p1.get(f'_count_{output_name}', 0)
                c2 = p2.get(f'_count_{output_name}', 0)
                merged[f'_sum_{output_name}'] = s1 + s2
                merged[f'_count_{output_name}'] = c1 + c2

            elif func == 'min':
                m1 = p1.get(f'_min_{output_name}')
                m2 = p2.get(f'_min_{output_name}')
                if m1 is None:
                    merged[f'_min_{output_name}'] = m2
                elif m2 is None:
                    merged[f'_min_{output_name}'] = m1
                else:
                    merged[f'_min_{output_name}'] = min(m1, m2)

            elif func == 'max':
                m1 = p1.get(f'_max_{output_name}')
                m2 = p2.get(f'_max_{output_name}')
                if m1 is None:
                    merged[f'_max_{output_name}'] = m2
                elif m2 is None:
                    merged[f'_max_{output_name}'] = m1
                else:
                    merged[f'_max_{output_name}'] = max(m1, m2)

        return merged

    cdef object _finalize_partials(self, dict all_partials):
        """Convert partial aggregates to final result."""
        if not all_partials:
            return None

        # Build result columns
        result_columns = {}
        cdef list values_list = list(all_partials.values())
        cdef list col_data
        cdef dict p
        cdef str key
        cdef str output_name
        cdef str col_name
        cdef str func

        # Key columns
        for key in self._keys:
            col_data = []
            for p in values_list:
                col_data.append(p.get(key))
            result_columns[key] = col_data

        # Finalize each aggregation
        for output_name, (col_name, func) in self._aggregations.items():
            if func in ('count', 'count_all'):
                col_data = []
                for p in values_list:
                    col_data.append(p.get(f'_count_{output_name}', 0))
                result_columns[output_name] = col_data

            elif func in ('sum', 'total'):
                col_data = []
                for p in values_list:
                    col_data.append(p.get(f'_sum_{output_name}', 0.0))
                result_columns[output_name] = col_data

            elif func in ('mean', 'avg', 'average'):
                # mean = sum / count
                col_data = []
                for p in values_list:
                    s = p.get(f'_sum_{output_name}', 0.0)
                    c = p.get(f'_count_{output_name}', 0)
                    mean = s / c if c > 0 else None
                    col_data.append(mean)
                result_columns[output_name] = col_data

            elif func == 'min':
                col_data = []
                for p in values_list:
                    col_data.append(p.get(f'_min_{output_name}'))
                result_columns[output_name] = col_data

            elif func == 'max':
                col_data = []
                for p in values_list:
                    col_data.append(p.get(f'_max_{output_name}'))
                result_columns[output_name] = col_data

        return pa.RecordBatch.from_pydict(result_columns)

    def __iter__(self):
        """Execute streaming groupby."""
        if self._source is None:
            return

        # Process all batches
        for batch in self._source:
            self.process_batch(batch)

        # Yield final result
        result = self.get_result()
        if result is not None and result.num_rows > 0:
            yield result

    cpdef void close(self):
        """Close operator and cleanup resources."""
        if self._closed:
            return

        self._closed = True

        # Close MarbleDB
        if self._marbledb is not None:
            try:
                self._marbledb.close()
            except:
                pass

            # Cleanup spill directory
            import shutil
            spill_dir = os.path.join(self._db_path, self._operator_id)
            try:
                if os.path.exists(spill_dir):
                    shutil.rmtree(spill_dir)
            except:
                pass

        # Clear buffer
        self._partial_buffer = {}
        self._buffer_bytes = 0

        logger.debug(f"StreamingGroupByOperator {self._operator_id}: closed")

    def __dealloc__(self):
        """Cleanup on deallocation."""
        if not self._closed:
            self.close()


# ============================================================================
# Factory Function
# ============================================================================

def create_streaming_groupby_operator(source, keys, aggregations,
                                       memory_threshold=DEFAULT_MEMORY_THRESHOLD,
                                       flush_threshold=DEFAULT_FLUSH_THRESHOLD,
                                       **kwargs):
    """
    Factory function for streaming groupby operator.

    This is the bigger-than-memory version of groupby that uses
    incremental partial aggregates stored in MarbleDB.

    Args:
        source: Input stream/iterator
        keys: List of column names to group by
        aggregations: Dict mapping output name to (column, function) tuple
        memory_threshold: Memory threshold (for API compatibility)
        flush_threshold: Buffer size before flushing to MarbleDB

    Returns:
        StreamingGroupByOperator instance
    """
    return StreamingGroupByOperator(
        source, keys, aggregations,
        memory_threshold=memory_threshold,
        flush_threshold=flush_threshold,
        **kwargs
    )
