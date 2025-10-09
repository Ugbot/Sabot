# cython: language_level=3
"""
Shortest Paths Cython Declarations

C++ shortest paths algorithm declarations for Cython bindings.
"""

from libc.stdint cimport int64_t
from libcpp.memory cimport shared_ptr

cimport pyarrow.lib as ca


# C++ ShortestPaths declarations
cdef extern from "shortest_paths.h" namespace "sabot::graph::traversal":
    # Shortest paths result structure
    cdef struct ShortestPathsResult:
        shared_ptr[ca.CDoubleArray] distances
        shared_ptr[ca.CInt64Array] predecessors
        int64_t source
        int64_t num_reached

    # Shortest paths functions
    ca.CResult[ShortestPathsResult] Dijkstra(
        shared_ptr[ca.CInt64Array] indptr,
        shared_ptr[ca.CInt64Array] indices,
        shared_ptr[ca.CDoubleArray] weights,
        int64_t num_vertices,
        int64_t source) except +

    ca.CResult[ShortestPathsResult] UnweightedShortestPaths(
        shared_ptr[ca.CInt64Array] indptr,
        shared_ptr[ca.CInt64Array] indices,
        int64_t num_vertices,
        int64_t source) except +

    ca.CResult[shared_ptr[ca.CInt64Array]] ReconstructPath(
        shared_ptr[ca.CInt64Array] predecessors,
        int64_t source,
        int64_t target) except +

    ca.CResult[shared_ptr[ca.CRecordBatch]] FloydWarshall(
        shared_ptr[ca.CInt64Array] indptr,
        shared_ptr[ca.CInt64Array] indices,
        shared_ptr[ca.CDoubleArray] weights,
        int64_t num_vertices) except +

    ca.CResult[ShortestPathsResult] AStar(
        shared_ptr[ca.CInt64Array] indptr,
        shared_ptr[ca.CInt64Array] indices,
        shared_ptr[ca.CDoubleArray] weights,
        int64_t num_vertices,
        int64_t source,
        int64_t target,
        shared_ptr[ca.CDoubleArray] heuristic) except +


# Python wrapper class
cdef class PyShortestPathsResult:
    cdef ca.DoubleArray _distances
    cdef ca.Int64Array _predecessors
    cdef int64_t _source
    cdef int64_t _num_reached

    cpdef ca.DoubleArray distances(self)
    cpdef ca.Int64Array predecessors(self)
    cpdef int64_t source(self)
    cpdef int64_t num_reached(self)
