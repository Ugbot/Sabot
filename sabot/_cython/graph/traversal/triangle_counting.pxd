# cython: language_level=3

from libcpp.memory cimport shared_ptr
from libcpp cimport bool as cbool
from libc.stdint cimport int64_t

from pyarrow.lib cimport (
    CInt64Array,
    CDoubleArray,
    CRecordBatch,
    CResult,
    GetResultValue
)
cimport pyarrow.lib as ca

cdef extern from "triangle_counting.h" namespace "sabot::graph::traversal":
    cdef struct CTriangleCountResult "sabot::graph::traversal::TriangleCountResult":
        shared_ptr[CInt64Array] per_vertex_triangles
        shared_ptr[CDoubleArray] clustering_coefficient
        int64_t total_triangles
        double global_clustering_coefficient
        int64_t num_vertices

    CResult[CTriangleCountResult] CountTriangles(
        const shared_ptr[CInt64Array]& indptr,
        const shared_ptr[CInt64Array]& indices,
        int64_t num_vertices
    ) nogil

    CResult[CTriangleCountResult] CountTrianglesWeighted(
        const shared_ptr[CInt64Array]& indptr,
        const shared_ptr[CInt64Array]& indices,
        const shared_ptr[CDoubleArray]& weights,
        int64_t num_vertices
    ) nogil

    CResult[shared_ptr[CRecordBatch]] TopKTriangleCounts(
        const shared_ptr[CInt64Array]& per_vertex_triangles,
        const shared_ptr[CDoubleArray]& clustering_coefficient,
        int64_t k
    ) nogil

    CResult[double] ComputeTransitivity(
        const shared_ptr[CInt64Array]& indptr,
        const shared_ptr[CInt64Array]& indices,
        int64_t num_vertices
    ) nogil
