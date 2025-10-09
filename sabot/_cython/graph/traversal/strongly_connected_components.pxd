# cython: language_level=3

from libcpp.memory cimport shared_ptr
from libcpp cimport bool as cbool
from libc.stdint cimport int64_t

from pyarrow.lib cimport (
    CInt64Array,
    CRecordBatch,
    CResult,
    GetResultValue
)
cimport pyarrow.lib as ca

cdef extern from "strongly_connected_components.h" namespace "sabot::graph::traversal":
    cdef struct CStronglyConnectedComponentsResult "sabot::graph::traversal::StronglyConnectedComponentsResult":
        shared_ptr[CInt64Array] component_ids
        int64_t num_components
        shared_ptr[CInt64Array] component_sizes
        int64_t largest_component_size
        int64_t num_vertices

    CResult[CStronglyConnectedComponentsResult] FindStronglyConnectedComponents(
        const shared_ptr[CInt64Array]& indptr,
        const shared_ptr[CInt64Array]& indices,
        int64_t num_vertices
    ) nogil

    CResult[shared_ptr[CRecordBatch]] GetSCCMembership(
        const shared_ptr[CInt64Array]& indptr,
        const shared_ptr[CInt64Array]& indices,
        int64_t num_vertices
    ) nogil

    CResult[shared_ptr[CInt64Array]] GetLargestSCC(
        const shared_ptr[CInt64Array]& component_ids,
        const shared_ptr[CInt64Array]& component_sizes
    ) nogil

    CResult[cbool] IsStronglyConnected(
        const shared_ptr[CInt64Array]& indptr,
        const shared_ptr[CInt64Array]& indices,
        int64_t num_vertices
    ) nogil

    CResult[shared_ptr[CInt64Array]] FindSourceSCCs(
        const shared_ptr[CInt64Array]& component_ids,
        const shared_ptr[CInt64Array]& indptr,
        const shared_ptr[CInt64Array]& indices,
        int64_t num_vertices
    ) nogil

    CResult[shared_ptr[CInt64Array]] FindSinkSCCs(
        const shared_ptr[CInt64Array]& component_ids,
        const shared_ptr[CInt64Array]& indptr,
        const shared_ptr[CInt64Array]& indices,
        int64_t num_vertices
    ) nogil
