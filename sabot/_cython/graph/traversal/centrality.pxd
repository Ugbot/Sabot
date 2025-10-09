# cython: language_level=3
"""
Centrality Cython Declarations

C++ type declarations for centrality algorithms.
"""

from libc.stdint cimport int64_t
from libcpp.memory cimport shared_ptr
from libcpp cimport bool as cbool

cimport pyarrow.lib as ca
from pyarrow.includes.libarrow cimport CArray, CInt64Array, CDoubleArray, CRecordBatch


# Betweenness centrality result structure
cdef extern from "centrality.h" namespace "sabot::graph::traversal":
    cdef struct BetweennessCentralityResult:
        shared_ptr[CDoubleArray] centrality
        int64_t num_vertices
        cbool normalized


# Centrality algorithm declarations
cdef extern from "centrality.h" namespace "sabot::graph::traversal":
    ca.CResult[BetweennessCentralityResult] BetweennessCentrality(
        const shared_ptr[CInt64Array]& indptr,
        const shared_ptr[CInt64Array]& indices,
        int64_t num_vertices,
        cbool normalized
    ) nogil

    ca.CResult[BetweennessCentralityResult] WeightedBetweennessCentrality(
        const shared_ptr[CInt64Array]& indptr,
        const shared_ptr[CInt64Array]& indices,
        const shared_ptr[CDoubleArray]& weights,
        int64_t num_vertices,
        cbool normalized
    ) nogil

    ca.CResult[shared_ptr[CRecordBatch]] EdgeBetweennessCentrality(
        const shared_ptr[CInt64Array]& indptr,
        const shared_ptr[CInt64Array]& indices,
        int64_t num_vertices
    ) nogil

    ca.CResult[shared_ptr[CRecordBatch]] TopKBetweenness(
        const shared_ptr[CDoubleArray]& centrality,
        int64_t k
    ) nogil
