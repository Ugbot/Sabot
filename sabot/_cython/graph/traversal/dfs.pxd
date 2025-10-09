# cython: language_level=3
"""
DFS Cython Declarations

C++ DFS algorithm declarations for Cython bindings.
"""

from libc.stdint cimport int64_t
from libcpp cimport bool as cbool
from libcpp.memory cimport shared_ptr

cimport pyarrow.lib as ca


# C++ DFS declarations
cdef extern from "dfs.h" namespace "sabot::graph::traversal":
    # DFS result structure
    cdef struct DFSResult:
        shared_ptr[ca.CInt64Array] vertices
        shared_ptr[ca.CInt64Array] discovery_time
        shared_ptr[ca.CInt64Array] finish_time
        shared_ptr[ca.CInt64Array] predecessors
        int64_t num_visited

    # DFS functions
    ca.CResult[DFSResult] DFS(
        shared_ptr[ca.CInt64Array] indptr,
        shared_ptr[ca.CInt64Array] indices,
        int64_t num_vertices,
        int64_t source) except +

    ca.CResult[DFSResult] FullDFS(
        shared_ptr[ca.CInt64Array] indptr,
        shared_ptr[ca.CInt64Array] indices,
        int64_t num_vertices) except +

    ca.CResult[shared_ptr[ca.CInt64Array]] TopologicalSort(
        shared_ptr[ca.CInt64Array] indptr,
        shared_ptr[ca.CInt64Array] indices,
        int64_t num_vertices) except +

    ca.CResult[cbool] HasCycle(
        shared_ptr[ca.CInt64Array] indptr,
        shared_ptr[ca.CInt64Array] indices,
        int64_t num_vertices) except +

    ca.CResult[shared_ptr[ca.CRecordBatch]] FindBackEdges(
        shared_ptr[ca.CInt64Array] indptr,
        shared_ptr[ca.CInt64Array] indices,
        int64_t num_vertices) except +


# Python wrapper classes
cdef class PyDFSResult:
    cdef ca.Int64Array _vertices
    cdef ca.Int64Array _discovery_time
    cdef ca.Int64Array _finish_time
    cdef ca.Int64Array _predecessors
    cdef int64_t _num_visited

    cpdef ca.Int64Array vertices(self)
    cpdef ca.Int64Array discovery_time(self)
    cpdef ca.Int64Array finish_time(self)
    cpdef ca.Int64Array predecessors(self)
    cpdef int64_t num_visited(self)
