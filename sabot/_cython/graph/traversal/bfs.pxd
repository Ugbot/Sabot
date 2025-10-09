# cython: language_level=3
"""
BFS Cython Declarations

C++ BFS algorithm declarations for Cython bindings.
"""

from libc.stdint cimport int64_t
from libcpp.memory cimport shared_ptr

cimport pyarrow.lib as ca


# C++ BFS declarations
cdef extern from "bfs.h" namespace "sabot::graph::traversal":
    # BFS result structure
    cdef struct BFSResult:
        shared_ptr[ca.CInt64Array] vertices
        shared_ptr[ca.CInt64Array] distances
        shared_ptr[ca.CInt64Array] predecessors
        int64_t num_visited

    # BFS functions
    ca.CResult[BFSResult] BFS(
        shared_ptr[ca.CInt64Array] indptr,
        shared_ptr[ca.CInt64Array] indices,
        int64_t num_vertices,
        int64_t source,
        int64_t max_depth) except +

    ca.CResult[shared_ptr[ca.CInt64Array]] KHopNeighbors(
        shared_ptr[ca.CInt64Array] indptr,
        shared_ptr[ca.CInt64Array] indices,
        int64_t num_vertices,
        int64_t source,
        int64_t k) except +

    ca.CResult[BFSResult] MultiBFS(
        shared_ptr[ca.CInt64Array] indptr,
        shared_ptr[ca.CInt64Array] indices,
        int64_t num_vertices,
        shared_ptr[ca.CInt64Array] sources,
        int64_t max_depth) except +

    ca.CResult[shared_ptr[ca.CInt64Array]] ComponentBFS(
        shared_ptr[ca.CInt64Array] indptr,
        shared_ptr[ca.CInt64Array] indices,
        int64_t num_vertices,
        int64_t source) except +


# Python wrapper classes
cdef class PyBFSResult:
    cdef ca.Int64Array _vertices
    cdef ca.Int64Array _distances
    cdef ca.Int64Array _predecessors
    cdef int64_t _num_visited

    cpdef ca.Int64Array vertices(self)
    cpdef ca.Int64Array distances(self)
    cpdef ca.Int64Array predecessors(self)
    cpdef int64_t num_visited(self)
