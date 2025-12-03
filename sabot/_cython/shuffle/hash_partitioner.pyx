# cython: language_level=3, boundscheck=False, wraparound=False
"""
Hash Partitioner for Network Shuffle

Vectorized hash-based partitioning for distributed query execution.
Uses efficient hash computation and Arrow filtering.
"""

from libc.stdint cimport int32_t, int64_t, uint64_t
from libcpp.vector cimport vector
from libcpp.string cimport string

cimport cython
import pyarrow as pa
import pyarrow.compute as pc


# FNV-1a hash constants for 64-bit
cdef uint64_t FNV_OFFSET = 14695981039346656037ULL
cdef uint64_t FNV_PRIME = 1099511628211ULL


@cython.cfunc
cdef inline uint64_t fnv1a_hash_int64(int64_t value) noexcept nogil:
    """FNV-1a hash for int64 values."""
    cdef uint64_t h = FNV_OFFSET
    cdef int i
    cdef uint64_t uval = <uint64_t>value
    for i in range(8):
        h ^= (uval >> (i * 8)) & 0xFF
        h *= FNV_PRIME
    return h


cdef class ArrowHashPartitioner:
    """
    Hash partitioner for Arrow RecordBatches.

    Partitions data by hash(key_columns) % num_partitions.
    Uses FNV-1a hashing for good distribution.

    Example:
        partitioner = ArrowHashPartitioner(['customer_id'], num_partitions=4)
        partitions = partitioner.partition_batch(batch)
    """

    cdef:
        list _key_columns
        int32_t _num_partitions

    def __init__(self, key_columns: list, num_partitions: int):
        """
        Initialize hash partitioner.

        Args:
            key_columns: Column names to hash on
            num_partitions: Number of partitions to create
        """
        self._key_columns = key_columns
        self._num_partitions = num_partitions

    cpdef list partition_batch(self, batch):
        """
        Partition batch into N partitions by hash(keys) % N.

        Args:
            batch: pyarrow.RecordBatch

        Returns:
            List of RecordBatches (one per partition, may be None if empty)
        """
        cdef int32_t num_rows = batch.num_rows
        cdef int32_t i, partition_id
        cdef list partition_ids_list = []
        cdef uint64_t hash_val
        cdef list key_cols = []

        # Get key columns
        for key_col in self._key_columns:
            key_cols.append(batch.column(key_col))

        # Compute partition ID for each row
        for i in range(num_rows):
            hash_val = FNV_OFFSET
            for col in key_cols:
                val = col[i].as_py()
                if val is not None:
                    # Use Python hash and mix with FNV
                    hash_val ^= <uint64_t>(hash(val) & 0xFFFFFFFFFFFFFFFF)
                    hash_val *= FNV_PRIME
            partition_ids_list.append(<int32_t>(hash_val % self._num_partitions))

        # Create partition ID array
        partition_ids = pa.array(partition_ids_list, type=pa.int32())

        # Split into partitions using Arrow filtering
        partitions = []
        for partition_id in range(self._num_partitions):
            mask = pc.equal(partition_ids, partition_id)
            partition_batch = batch.filter(mask)

            if partition_batch.num_rows > 0:
                partitions.append(partition_batch)
            else:
                partitions.append(None)

        return partitions

    cpdef list partition_table(self, table):
        """
        Partition a Table into N partitions.

        Args:
            table: pyarrow.Table

        Returns:
            List of Tables (one per partition)
        """
        cdef int32_t num_rows = table.num_rows
        cdef int32_t i, partition_id
        cdef list partition_ids_list = []
        cdef uint64_t hash_val
        cdef list key_cols = []

        # Get key columns
        for key_col in self._key_columns:
            key_cols.append(table.column(key_col))

        # Compute partition ID for each row
        for i in range(num_rows):
            hash_val = FNV_OFFSET
            for col in key_cols:
                val = col[i].as_py()
                if val is not None:
                    hash_val ^= <uint64_t>(hash(val) & 0xFFFFFFFFFFFFFFFF)
                    hash_val *= FNV_PRIME
            partition_ids_list.append(<int32_t>(hash_val % self._num_partitions))

        # Create partition ID array
        partition_ids = pa.array(partition_ids_list, type=pa.int32())

        # Split into partitions
        partitions = []
        for partition_id in range(self._num_partitions):
            mask = pc.equal(partition_ids, partition_id)
            partition_table = table.filter(mask)
            partitions.append(partition_table)

        return partitions

    cpdef int32_t get_partition_for_row(self, batch, int64_t row_index):
        """
        Get partition ID for a single row.

        Args:
            batch: pyarrow.RecordBatch
            row_index: Row index within batch

        Returns:
            Partition ID (0 to num_partitions-1)
        """
        cdef uint64_t hash_val = FNV_OFFSET

        for key_col in self._key_columns:
            col = batch.column(key_col)
            val = col[row_index].as_py()
            if val is not None:
                hash_val ^= <uint64_t>(hash(val) & 0xFFFFFFFFFFFFFFFF)
                hash_val *= FNV_PRIME

        return <int32_t>(hash_val % self._num_partitions)


cdef class RangePartitioner:
    """
    Range-based partitioner for sorted data.

    Partitions data based on key ranges rather than hash.
    Useful for time-series data or sorted keys.

    Example:
        partitioner = RangePartitioner(['timestamp'], ranges=[...])
        partitions = partitioner.partition_batch(batch)
    """

    cdef:
        list _key_columns
        list _ranges
        int32_t _num_partitions

    def __init__(self, key_columns: list, ranges: list):
        """
        Initialize range partitioner.

        Args:
            key_columns: Columns to partition by
            ranges: List of (min, max) tuples defining ranges
        """
        self._key_columns = key_columns
        self._ranges = ranges
        self._num_partitions = len(ranges)

    cpdef list partition_batch(self, batch):
        """
        Partition batch by key ranges.

        Args:
            batch: pyarrow.RecordBatch

        Returns:
            List of RecordBatches (one per range)
        """
        partitions = []

        # Get key column
        key_col = batch.column(self._key_columns[0])

        for i, (min_val, max_val) in enumerate(self._ranges):
            # Create range mask
            mask_min = pc.greater_equal(key_col, min_val)
            mask_max = pc.less(key_col, max_val)
            mask = pc.and_(mask_min, mask_max)

            # Filter batch
            partition_batch = batch.filter(mask)

            if partition_batch.num_rows > 0:
                partitions.append(partition_batch)
            else:
                partitions.append(None)

        return partitions


# Python wrapper for ease of use
def hash_partition(batch, key_columns: list, num_partitions: int) -> list:
    """
    Partition batch by hash(keys) % num_partitions.

    Args:
        batch: pyarrow.RecordBatch
        key_columns: Columns to hash
        num_partitions: Number of partitions

    Returns:
        List of RecordBatches
    """
    partitioner = ArrowHashPartitioner(key_columns, num_partitions)
    return partitioner.partition_batch(batch)


def range_partition(batch, key_columns: list, ranges: list) -> list:
    """
    Partition batch by key ranges.

    Args:
        batch: pyarrow.RecordBatch
        key_columns: Columns to partition by
        ranges: List of (min, max) tuples

    Returns:
        List of RecordBatches
    """
    partitioner = RangePartitioner(key_columns, ranges)
    return partitioner.partition_batch(batch)
