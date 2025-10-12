# cython: language_level=3
"""
Distributed Fintech Kernel Operators.

Proper BaseOperator implementations that work with:
- Stream API
- Morsel parallelism
- Network shuffle
- Symbol-based partitioning
"""

from sabot._cython.operators.base_operator cimport BaseOperator
from libcpp.unordered_map cimport unordered_map
from libcpp.string cimport string as cpp_string


cdef class SymbolKeyedOperator(BaseOperator):
    """
    Base operator for symbol-keyed fintech kernels.
    
    Handles per-symbol state management and partitioning.
    Automatically works with both local morsels and network shuffle.
    """
    cdef:
        object _kernel_class              # Kernel class to instantiate
        dict _kernel_kwargs               # Arguments for kernel
        str _symbol_column                # Column to partition by
        unordered_map[cpp_string, object] _symbol_kernels  # Per-symbol instances
        bint _auto_partition             # Enable symbol partitioning
    
    cdef object _get_or_create_symbol_kernel(self, str symbol)
    cdef object _process_symbol_group(self, object symbol_batch, str symbol)
    cpdef object process_batch(self, object batch)


cdef class StatelessKernelOperator(BaseOperator):
    """
    Operator for stateless fintech kernels.
    
    Uses local morsels only (no network shuffle needed).
    """
    cdef:
        object _kernel_func
        dict _kernel_kwargs
    
    cpdef object process_batch(self, object batch)

