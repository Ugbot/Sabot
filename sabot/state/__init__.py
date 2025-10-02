"""State management backends for stateful stream processing."""

from sabot._cython.stores_base import BackendConfig, StoreBackend, StoreTransaction
from sabot._cython.stores_memory import OptimizedMemoryBackend, MemoryTransaction
from sabot._cython.state.value_state import ValueState
from sabot._cython.state.map_state import MapState
from sabot._cython.state.list_state import ListState
from sabot._cython.state.reducing_state import ReducingState
from sabot._cython.state.aggregating_state import AggregatingState
from sabot._cython.state.rocksdb_state import RocksDBStateBackend

# Export the primary implementations
MemoryBackend = OptimizedMemoryBackend
RocksDBBackend = RocksDBStateBackend

# Backwards compatibility
UltraFastMemoryBackend = OptimizedMemoryBackend
FastStoreConfig = BackendConfig

__all__ = [
    # Configuration
    "BackendConfig",

    # Backends
    "MemoryBackend",
    "OptimizedMemoryBackend",
    "RocksDBBackend",
    "RocksDBStateBackend",
    "StoreBackend",

    # State types
    "ValueState",
    "MapState",
    "ListState",
    "ReducingState",
    "AggregatingState",

    # Transactions
    "StoreTransaction",
    "MemoryTransaction",

    # Backwards compatibility
    "UltraFastMemoryBackend",
    "FastStoreConfig",
]
