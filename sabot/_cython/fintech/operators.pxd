# cython: language_level=3
"""
Fintech Kernel Operators - Distributed execution wrappers.

Wraps fintech kernels as proper Sabot operators with:
- Morsel parallelism support
- Network shuffle for stateful kernels
- Symbol-based partitioning for multi-node execution
"""

from sabot._cython.operators.base_operator cimport BaseOperator
from sabot._cython.operators.shuffled_operator cimport ShuffledOperator
from libcpp.unordered_map cimport unordered_map
from libcpp.string cimport string as cpp_string


cdef class FintechKernelOperator(ShuffledOperator):
    """
    Base operator for fintech kernels with symbol-based partitioning.
    
    Fintech kernels are STATEFUL (maintain running state per symbol),
    but each symbol's state is INDEPENDENT. This means:
    
    - Local execution: Each symbol processes in order
    - Distributed execution: Partition by symbol, each node handles subset of symbols
    
    Example:
        Node 1: AAPL, GOOGL, TSLA (with their EWMA states)
        Node 2: MSFT, AMZN, META (with their EWMA states)
        Node 3: NVDA, AMD, INTC (with their EWMA states)
    """
    cdef:
        object _kernel_func          # The fintech kernel function
        str _symbol_column           # Column name for symbol (partitioning)
        unordered_map[cpp_string, object] _symbol_states  # Per-symbol kernel instances
        bint _symbol_keyed          # Whether to partition by symbol
    
    cpdef object process_batch(self, object batch)
    cdef object _get_or_create_kernel(self, str symbol)


cdef class OnlineStatsOperator(FintechKernelOperator):
    """Operator wrapper for online statistics kernels (EWMA, Welford, etc.)"""
    cpdef object process_batch(self, object batch)


cdef class MicrostructureOperator(FintechKernelOperator):
    """Operator wrapper for microstructure kernels (OFI, microprice, etc.)"""
    cpdef object process_batch(self, object batch)


cdef class VolatilityOperator(FintechKernelOperator):
    """Operator wrapper for volatility kernels (RV, BPV, etc.)"""
    cpdef object process_batch(self, object batch)

