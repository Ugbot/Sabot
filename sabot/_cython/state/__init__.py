# -*- coding: utf-8 -*-
"""
Cython State Management Primitives

Provides Flink-compatible state interfaces with C-level performance.
Implements ValueState, ListState, MapState, ReducingState, AggregatingState.
"""

__all__ = [
    'StateBackend',
    'ValueState',
    'ListState',
    'MapState',
    'ReducingState',
    'AggregatingState',
    'RocksDBStateBackend',
    'TonboStateBackend',
]

# Import Cython extensions (will be available after compilation)
try:
    from .state_backend import StateBackend
    from .value_state import ValueState
    from .list_state import ListState
    from .map_state import MapState
    from .reducing_state import ReducingState
    from .aggregating_state import AggregatingState
    from .rocksdb_state import RocksDBStateBackend
    from .tonbo_state import TonboStateBackend

    CYTHON_STATE_AVAILABLE = True

except ImportError:
    CYTHON_STATE_AVAILABLE = False

    # Provide fallback interfaces for when Cython extensions aren't compiled
    class StateBackend:
        """Fallback state backend interface."""
        pass

    class ValueState:
        """Fallback ValueState interface."""
        pass

    class ListState:
        """Fallback ListState interface."""
        pass

    class MapState:
        """Fallback MapState interface."""
        pass

    class ReducingState:
        """Fallback ReducingState interface."""
        pass

    class AggregatingState:
        """Fallback AggregatingState interface."""
        pass

    class RocksDBStateBackend(StateBackend):
        """Fallback RocksDB state backend."""
        pass

    class TonboStateBackend(StateBackend):
        """Fallback Tonbo state backend."""
        pass
