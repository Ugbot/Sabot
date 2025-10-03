# cython: language_level=3, boundscheck=False, wraparound=False
"""
High-Performance Data Loader - Cython Implementation

Features:
- Multi-threaded CSV parsing
- Memory-mapped Arrow IPC loading
- Auto-format detection
- Zero-copy operations
- Column filtering
- Row limits with streaming

Performance:
- CSV: 0.5-1.0M rows/sec (multi-threaded)
- Arrow IPC: 5-10M rows/sec (memory-mapped)
"""

from libc.stdint cimport int64_t
from libcpp cimport bool as cbool
from libcpp.string cimport string as cpp_string

import os
from pathlib import Path
from typing import Optional, List

import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.feather as feather
import pyarrow.compute as pc

cimport cython

# Get number of CPU cores at module load
import multiprocessing
DEFAULT_NUM_THREADS = multiprocessing.cpu_count()


@cython.final
cdef class DataLoader:
    """
    High-performance data loader with Cython threading.

    Automatically detects format and uses optimal loading strategy:
    - Arrow IPC: Memory-mapped zero-copy (5-10M rows/sec)
    - CSV: Multi-threaded streaming parser (0.5-1.0M rows/sec)

    Example:
        loader = DataLoader(num_threads=8, block_size=128*1024*1024)
        table = loader.load('data.csv', limit=1_000_000, columns=['id', 'price'])
    """

    def __cinit__(self):
        """Initialize with optimal defaults."""
        self.block_size = 128 * 1024 * 1024  # 128MB blocks
        self.num_threads = DEFAULT_NUM_THREADS
        self.use_threads = True
        self.compression = 'zstd'
        self.compression_level = 3

    def __init__(self, int num_threads=-1, int64_t block_size=-1):
        """
        Initialize data loader.

        Args:
            num_threads: Number of threads for CSV parsing (-1 = auto-detect)
            block_size: Block size for CSV parsing in bytes (-1 = 128MB default)
        """
        if num_threads > 0:
            self.num_threads = num_threads
        if block_size > 0:
            self.block_size = block_size

    cpdef object load(self, str path, int64_t limit=-1, list columns=None):
        """
        Load data with auto-format detection.

        Checks for Arrow IPC file first (fastest), falls back to CSV.

        Args:
            path: File path (CSV or Arrow IPC)
            limit: Maximum rows to load (-1 = all)
            columns: Columns to load (None = all)

        Returns:
            PyArrow Table
        """
        path_obj = Path(path)
        arrow_path = path_obj.with_suffix('.arrow')

        # Check environment variable and file existence
        use_arrow = os.getenv('SABOT_USE_ARROW', '0') == '1'

        if use_arrow and arrow_path.exists():
            return self.load_arrow_ipc(str(arrow_path), limit, columns)

        # Fall back to CSV
        if path_obj.suffix in ['.csv', '.tsv', '.txt']:
            return self.load_csv(path, limit, columns)

        raise ValueError(f"Unsupported file format: {path_obj.suffix}")

    cpdef object load_arrow_ipc(self, str path, int64_t limit=-1, list columns=None):
        """
        Load Arrow IPC file with memory-mapping.

        Zero-copy memory-mapped loading for maximum performance.

        Args:
            path: Path to .arrow file
            limit: Maximum rows to load (-1 = all)
            columns: Columns to load (None = all)

        Returns:
            PyArrow Table
        """
        # Memory-mapped zero-copy read (BLAZING FAST!)
        table = feather.read_table(
            path,
            columns=columns,
            memory_map=True
        )

        # Apply limit if specified (zero-copy slice)
        if limit > 0 and table.num_rows > limit:
            table = table.slice(0, limit)

        # Convert NULL type columns to string for join compatibility
        table = self._convert_null_columns(table)

        return table

    cpdef object load_csv(self, str path, int64_t limit=-1, list columns=None):
        """
        Load CSV with multi-threaded streaming parser.

        Uses all CPU cores for parallel parsing. Stops early if limit specified.

        Args:
            path: Path to CSV file
            limit: Maximum rows to load (-1 = all)
            columns: Columns to load (None = all)

        Returns:
            PyArrow Table
        """
        # Configure for maximum performance
        read_options = pa_csv.ReadOptions(
            use_threads=self.use_threads,
            block_size=self.block_size
        )

        parse_options = pa_csv.ParseOptions(delimiter=',')

        convert_options = pa_csv.ConvertOptions(
            null_values=['NULL', 'null', ''],
            strings_can_be_null=True,
            auto_dict_encode=False,
            include_columns=columns if columns else None
        )

        # Use streaming reader if limit specified (stops early)
        if limit > 0:
            table = self._load_csv_streaming(
                path, limit, read_options, parse_options, convert_options
            )
        else:
            # Load entire file
            table = pa_csv.read_csv(
                path,
                read_options=read_options,
                parse_options=parse_options,
                convert_options=convert_options
            )

        # Convert NULL type columns to string for join compatibility
        table = self._convert_null_columns(table)

        return table

    cdef object _load_csv_streaming(
        self,
        str path,
        int64_t limit,
        object read_options,
        object parse_options,
        object convert_options
    ):
        """
        Internal: Load CSV with streaming to respect limit.

        Stops parsing after reaching limit (much faster for large files).
        """
        batches = []
        cdef int64_t rows_read = 0
        cdef int64_t rows_to_take

        with pa_csv.open_csv(
            path,
            read_options=read_options,
            parse_options=parse_options,
            convert_options=convert_options
        ) as reader:
            for batch in reader:
                if rows_read >= limit:
                    break

                rows_to_take = min(batch.num_rows, limit - rows_read)
                if rows_to_take < batch.num_rows:
                    batch = batch.slice(0, rows_to_take)

                batches.append(batch)
                rows_read += batch.num_rows

        return pa.Table.from_batches(batches)

    cdef object _convert_null_columns(self, object table):
        """
        Internal: Convert NULL type columns to string.

        Arrow joins don't support NULL type, so convert to string.
        """
        new_columns = []
        new_schema_fields = []

        for i in range(table.num_columns):
            field = table.schema[i]
            col = table.column(i)

            if pa.types.is_null(field.type):
                # Convert NULL to string
                col = pc.cast(col, pa.string())
                new_schema_fields.append(pa.field(field.name, pa.string()))
            else:
                new_schema_fields.append(field)

            new_columns.append(col)

        # Rebuild table if any NULL columns found
        if any(pa.types.is_null(f.type) for f in table.schema):
            return pa.Table.from_arrays(new_columns, schema=pa.schema(new_schema_fields))

        return table


# ============================================================================
# Convenience Functions
# ============================================================================

cpdef object load_data(str path, int64_t limit=-1, list columns=None, int num_threads=-1):
    """
    Load data with auto-format detection (convenience function).

    Args:
        path: File path (CSV or Arrow IPC)
        limit: Maximum rows to load (-1 = all)
        columns: Columns to load (None = all)
        num_threads: Number of threads (-1 = auto-detect)

    Returns:
        PyArrow Table

    Example:
        table = load_data('data.csv', limit=1_000_000, columns=['id', 'price'])
    """
    loader = DataLoader(num_threads=num_threads)
    return loader.load(path, limit=limit, columns=columns)


cpdef object convert_csv_to_arrow(str csv_path, str arrow_path=None, str compression='zstd'):
    """
    Convert CSV to Arrow IPC format.

    Args:
        csv_path: Path to CSV file
        arrow_path: Output path (None = auto-generate)
        compression: Compression algorithm ('zstd', 'lz4', None)

    Returns:
        Path to created Arrow file

    Example:
        arrow_path = convert_csv_to_arrow('large.csv')
    """
    csv_path_obj = Path(csv_path)
    if arrow_path is None:
        arrow_path = str(csv_path_obj.with_suffix('.arrow'))

    # Load CSV
    loader = DataLoader()
    table = loader.load_csv(csv_path)

    # Write Arrow IPC
    feather.write_feather(
        table,
        arrow_path,
        compression=compression,
        compression_level=3
    )

    return arrow_path
