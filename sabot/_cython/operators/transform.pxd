# cython: language_level=3
"""
Cython header for stateless transform operators.

Defines operator classes for zero-copy Arrow-based transformations.
"""

cdef class BaseOperator:
    """Base class for all Cython operators."""
    cdef object _source
    cdef object _schema

    cpdef object process_batch(self, object batch)


cdef class CythonMapOperator(BaseOperator):
    """Map operator using Arrow compute kernels with automatic Numba compilation."""
    cdef object _map_func            # Original user function
    cdef object _compiled_func       # Numba-compiled version (may be same as _map_func)
    cdef bint _is_compiled           # Was function successfully compiled?
    cdef bint _vectorized            # Function operates on arrays directly

    cpdef object process_batch(self, object batch)


cdef class CythonFilterOperator(BaseOperator):
    """Filter operator using Arrow SIMD kernels."""
    cdef object _predicate

    cpdef object process_batch(self, object batch)


cdef class CythonSelectOperator(BaseOperator):
    """Select (project) operator using Arrow zero-copy slicing."""
    cdef list _columns

    cpdef object process_batch(self, object batch)


cdef class CythonFlatMapOperator(BaseOperator):
    """FlatMap operator for 1-to-N expansion."""
    cdef object _flat_map_func

    cpdef object process_batch(self, object batch)


cdef class CythonUnionOperator(BaseOperator):
    """Union operator for merging multiple streams."""
    cdef list _sources

    cpdef object process_batches(self, list batches)
