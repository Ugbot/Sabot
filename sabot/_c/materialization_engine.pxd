# cython: language_level=3
"""
Materialization Engine Header

Unified materialization system for dimension tables and analytical views.
All heavy lifting in Cython/C++ with direct Arrow buffer access.
"""

from libc.stdint cimport int64_t, int32_t, uint64_t
from libcpp.string cimport string as cpp_string
from libcpp.vector cimport vector
from libcpp.unordered_map cimport unordered_map
from libcpp.memory cimport shared_ptr
from libcpp cimport bool as cbool

# Import Arrow C++ types
cimport pyarrow.lib as ca
from pyarrow.includes.libarrow cimport (
    CRecordBatch as PCRecordBatch,
    CSchema as PCSchema,
    CArray as PCArray,
)

# Forward declarations
cdef class Materialization
cdef class MaterializationManager
cdef class StreamingAggregator


# Enums (defined in .pyx)
# class MaterializationBackend(Enum):
#     ROCKSDB = "rocksdb"
#     TONBO = "tonbo"
#
# class PopulationStrategy(Enum):
#     STREAM = "stream"
#     BATCH = "batch"
#     OPERATOR = "operator"
#     CDC = "cdc"


# Aggregation state struct (C++ struct for zero-copy)
cdef struct AggregationState:
    int64_t count
    double sum_val
    double sum_sq
    double min_val
    double max_val
    int64_t first_timestamp
    int64_t last_timestamp


cdef class StreamingAggregator:
    """
    Streaming aggregation engine in pure C++.

    Updates aggregation state incrementally with each batch.
    No Python object creation in hot path.
    """
    cdef:
        unordered_map[cpp_string, AggregationState] _state
        vector[cpp_string] _group_by_keys
        unordered_map[cpp_string, cpp_string] _aggregation_specs  # col -> agg_func
        cbool _initialized

    # Methods
    cpdef void initialize(self, list group_by_keys, dict aggregations)
    cpdef void update(self, ca.RecordBatch batch)
    cpdef ca.RecordBatch finalize(self)
    cdef void _reset(self)


cdef class Materialization:
    """
    Unified materialization (dimension table or analytical view).

    Attributes:
        _name: Materialization name
        _backend_type: Storage backend (RocksDB or Tonbo)
        _strategy_type: Population strategy
        _storage_handle: Polymorphic backend handle
        _hash_index: Hash index for RocksDB lookups (key_hash -> row_idx)
        _key_column: Key column name for lookups
        _schema: Arrow schema
        _aggregator: Streaming aggregator (NULL if not aggregating)
        _version: Version number for updates
        _is_kv_backend: True if RocksDB, False if Tonbo
        _indexed: Whether hash index is built
    """
    cdef:
        cpp_string _name
        int _backend_type  # enum MaterializationBackend
        int _strategy_type  # enum PopulationStrategy
        void* _storage_handle  # RocksDB* or TonboStore*

        # For lookup access (dimension table use case)
        unordered_map[int64_t, int64_t] _hash_index  # key_hash -> row_idx
        cpp_string _key_column

        # For streaming updates
        StreamingAggregator _aggregator
        cbool _has_aggregator

        # Cached Arrow data (for in-memory materializations)
        shared_ptr[PCRecordBatch] _cached_batch
        cbool _has_cached_batch

        # Metadata
        object _schema  # Python ca.Schema object
        int64_t _version
        cbool _is_kv_backend
        cbool _indexed
        int64_t _num_rows

    # Population methods
    cpdef void populate_from_arrow_batch(self, ca.RecordBatch batch)
    cpdef void populate_from_arrow_file(self, str path)
    cpdef void build_index(self)

    # Access methods
    cpdef object lookup(self, object key)
    cpdef ca.RecordBatch scan(self, str filter_expr=*, int64_t limit=*)
    cpdef ca.RecordBatch enrich_batch(self, ca.RecordBatch stream_batch, str join_key)
    cpdef cbool contains(self, object key)

    # Update methods
    cpdef void update_batch(self, ca.RecordBatch batch)
    cpdef void refresh(self)

    # Metadata
    cpdef int64_t num_rows(self)
    cpdef int64_t version(self)
    cpdef ca.Schema schema(self)

    # Internal helpers
    cdef int64_t _lookup_row(self, int64_t key_hash) nogil
    cdef ca.RecordBatch _to_arrow_batch(self)
    cdef void _upsert_batch(self, ca.RecordBatch batch)
    cdef int64_t _hash_key(self, object key)


cdef class MaterializationManager:
    """
    Manager for all materializations.

    Creates and tracks dimension tables and analytical views.
    """
    cdef:
        dict _materializations  # Python dict: str -> Materialization
        cpp_string _default_backend

    # Creation methods
    cpdef Materialization create_dimension_table(
        self,
        str name,
        str key_column,
        str backend=*
    )

    cpdef Materialization create_analytical_view(
        self,
        str name,
        list group_by_keys=*,
        dict aggregations=*,
        str backend=*
    )

    # Lookup
    cpdef Materialization get(self, str name)
    cpdef cbool has(self, str name)
    cpdef vector[cpp_string] list_names(self)
