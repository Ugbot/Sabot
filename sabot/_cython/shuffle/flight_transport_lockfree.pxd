# cython: language_level=3
"""
Lock-Free Arrow Flight Transport - Type Definitions

Pure C++ Flight implementation with zero-copy, lock-free partition storage.
Uses LMAX Disruptor-style atomics, no Python imports, no mutexes.

Performance targets:
- Partition registration: <100ns
- Partition fetch (local): <50ns
- Network latency: <1ms + bandwidth
- Throughput: 10M+ ops/sec
"""

from libc.stdint cimport int32_t, int64_t
from libcpp cimport bool as cbool
from libcpp.string cimport string
from libcpp.atomic cimport atomic
from libcpp.memory cimport shared_ptr, unique_ptr
from pyarrow.includes.libarrow cimport CRecordBatch as PCRecordBatch
from pyarrow.includes.libarrow_flight cimport (
    CFlightClient,
    PyFlightServer,
    PyFlightServerVtable,
    CLocation,
    CTicket,
    CFlightCallOptions,
    CFlightServerOptions,
    CFlightDataStream,
    CServerCallContext,
)

# Import our lock-free primitives
from .atomic_partition_store cimport AtomicPartitionStore
from .lock_free_queue cimport SPSCRingBuffer


# ============================================================================
# Connection Slot (Lock-Free Pool)
# ============================================================================

# Lock-free connection pool slot
cdef struct ConnectionSlot:
    atomic[cbool] in_use               # Slot occupied flag
    char host[256]                     # Host string
    int32_t port                       # Port number
    shared_ptr[CFlightClient] client   # C++ Flight client


# ============================================================================
# Lock-Free Flight Client
# ============================================================================

cdef class LockFreeFlightClient:
    """
    Lock-free Arrow Flight client with atomic connection pooling.

    Design:
    - Fixed-size connection pool (no dynamic allocation)
    - Atomic flags for slot occupancy
    - Lock-free connection lookup via linear scan
    - All operations nogil

    NO Python imports, NO mutexes, pure atomics.
    """
    cdef:
        ConnectionSlot* connections    # Fixed connection pool
        int32_t max_connections        # Pool size
        atomic[int32_t] connection_count  # Active connections

        int32_t max_retries
        double timeout_seconds

        char _padding[64]              # Cache line padding

    cdef shared_ptr[CFlightClient] get_connection(
        self,
        const char* host,
        int32_t port
    ) nogil

    cdef shared_ptr[PCRecordBatch] fetch_partition(
        self,
        const char* host,
        int32_t port,
        int64_t shuffle_id_hash,
        int32_t partition_id
    ) nogil

    cdef void close_all(self) nogil


# ============================================================================
# Lock-Free Flight Server
# ============================================================================

cdef class LockFreeFlightServer:
    """
    Lock-free Arrow Flight server using atomic partition store.

    Design:
    - AtomicPartitionStore for lock-free partition storage
    - Atomic running flag (no mutex)
    - C++ Flight server (no Python FlightServerBase)
    - All operations nogil

    NO Python imports, NO mutexes, pure C++/atomics.
    """
    cdef:
        string host
        int32_t port

        cbool running                          # Server state (simple bool - single threaded init/stop)
        AtomicPartitionStore _store_instance   # Python-managed store instance

        # C++ Flight server (will be implemented via C++ header)
        void* server_cpp               # PyFlightServer* (opaque for now)

        char _padding[64]

    cdef void start(self) nogil except *
    cdef void stop(self) nogil

    # Lock-free partition registration
    cdef cbool register_partition(
        self,
        int64_t shuffle_id_hash,
        int32_t partition_id,
        shared_ptr[PCRecordBatch] batch
    ) nogil

    # Lock-free partition retrieval
    cdef shared_ptr[PCRecordBatch] get_partition(
        self,
        int64_t shuffle_id_hash,
        int32_t partition_id
    ) nogil


# ============================================================================
# Helper Functions
# ============================================================================

cdef inline int64_t hash_shuffle_id(const char* shuffle_id, int64_t len) nogil:
    """Hash shuffle ID string (FNV-1a)."""
    cdef int64_t hash_val = <int64_t>14695981039346656037ULL
    cdef int64_t i

    for i in range(len):
        hash_val ^= <int64_t>shuffle_id[i]
        hash_val *= <int64_t>1099511628211ULL

    return hash_val
