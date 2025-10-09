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

cdef extern from "connected_components.h" namespace "sabot::graph::traversal":
    cdef struct CConnectedComponentsResult "sabot::graph::traversal::ConnectedComponentsResult":
        shared_ptr[CInt64Array] component_ids
        int64_t num_components
        shared_ptr[CInt64Array] component_sizes
        int64_t largest_component_size
        int64_t num_vertices

    CResult[CConnectedComponentsResult] FindConnectedComponents(
        const shared_ptr[CInt64Array]& indptr,
        const shared_ptr[CInt64Array]& indices,
        int64_t num_vertices
    ) nogil

    CResult[CConnectedComponentsResult] FindConnectedComponentsBFS(
        const shared_ptr[CInt64Array]& indptr,
        const shared_ptr[CInt64Array]& indices,
        int64_t num_vertices
    ) nogil

    CResult[shared_ptr[CInt64Array]] GetLargestComponent(
        const shared_ptr[CInt64Array]& component_ids,
        const shared_ptr[CInt64Array]& component_sizes
    ) nogil

    CResult[shared_ptr[CRecordBatch]] ComponentStatistics(
        const shared_ptr[CInt64Array]& component_ids,
        int64_t num_components
    ) nogil

    CResult[int64_t] CountIsolatedVertices(
        const shared_ptr[CInt64Array]& component_sizes
    ) nogil

    CResult[CConnectedComponentsResult] WeaklyConnectedComponents(
        const shared_ptr[CInt64Array]& indptr,
        const shared_ptr[CInt64Array]& indices,
        int64_t num_vertices
    ) nogil

    # Note: For SymmetrizeGraph, we can't directly expose std::pair in Cython,
    # so we'll handle it manually in the .pyx file
