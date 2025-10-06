# cython: language_level=3

"""
Cython header file for Numba compiler.

Allows other Cython modules to import compiled declarations.
"""

cdef enum CompilationStrategy:
    SKIP
    NJIT
    VECTORIZE
    AUTO

cdef class FunctionPattern:
    cdef:
        public bint has_loops
        public bint has_numpy
        public bint has_pandas
        public bint has_arrow
        public bint has_dict_access
        public bint has_list_ops
        public int loop_count
        public int numpy_call_count

cdef class NumbaCompiler:
    cdef:
        object _numba_module
        bint _numba_available
        dict _compilation_cache
        int _max_cache_size

    cpdef object compile_udf(self, object func)
    cdef str _get_cache_key(self, object func)
    cdef FunctionPattern _analyze_function(self, object func)
    cdef CompilationStrategy _choose_strategy(self, FunctionPattern pattern)
    cdef object _compile_njit(self, object func)
    cdef object _compile_vectorize(self, object func)
    cdef object _try_compile_best_effort(self, object func)
    cdef tuple _generate_test_args(self, object func)
    cdef void _add_to_cache(self, str key, object func)

# Public API
cpdef object auto_compile(object func)
