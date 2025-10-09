# cython: language_level=3
"""
PageRank Cython Declarations

C++ type declarations for PageRank algorithms.
"""

from libc.stdint cimport int64_t
from libcpp.memory cimport shared_ptr
from libcpp cimport bool as cbool

cimport pyarrow.lib as ca
from pyarrow.includes.libarrow cimport CArray, CInt64Array, CDoubleArray, CRecordBatch


# PageRank result structure
cdef extern from "pagerank.h" namespace "sabot::graph::traversal":
    cdef struct PageRankResult:
        shared_ptr[CDoubleArray] ranks
        int64_t num_vertices
        int64_t num_iterations
        double final_delta
        cbool converged


# PageRank algorithm declarations
cdef extern from "pagerank.h" namespace "sabot::graph::traversal":
    ca.CResult[PageRankResult] PageRank(
        const shared_ptr[CInt64Array]& indptr,
        const shared_ptr[CInt64Array]& indices,
        int64_t num_vertices,
        double damping,
        int64_t max_iterations,
        double tolerance
    ) nogil

    ca.CResult[PageRankResult] PersonalizedPageRank(
        const shared_ptr[CInt64Array]& indptr,
        const shared_ptr[CInt64Array]& indices,
        int64_t num_vertices,
        const shared_ptr[CInt64Array]& source_vertices,
        double damping,
        int64_t max_iterations,
        double tolerance
    ) nogil

    ca.CResult[PageRankResult] WeightedPageRank(
        const shared_ptr[CInt64Array]& indptr,
        const shared_ptr[CInt64Array]& indices,
        const shared_ptr[CDoubleArray]& weights,
        int64_t num_vertices,
        double damping,
        int64_t max_iterations,
        double tolerance
    ) nogil

    ca.CResult[shared_ptr[CRecordBatch]] TopKPageRank(
        const shared_ptr[CDoubleArray]& ranks,
        int64_t k
    ) nogil
