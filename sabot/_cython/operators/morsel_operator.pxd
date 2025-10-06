# cython: language_level=3

from sabot._cython.operators.base_operator cimport BaseOperator

cdef class MorselDrivenOperator(BaseOperator):
    """Wrapper for morsel-driven parallel execution."""

    cdef:
        object _wrapped_operator     # Underlying operator
        object _parallel_processor   # ParallelProcessor instance
        int _num_workers            # Number of parallel workers
        long long _morsel_size_kb   # Morsel size
        bint _enabled               # Is morsel execution enabled?

    cpdef object process_batch(self, object batch)
    cpdef bint should_use_morsel_execution(self, object batch)
    cpdef object _process_batch_with_morsels(self, object batch)
