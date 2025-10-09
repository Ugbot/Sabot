# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
"""
Triangle Counting and Clustering Coefficient

Implements triangle counting using node-iterator method for community detection
and graph cohesion analysis.
"""

from libc.stdint cimport int64_t
from libcpp.memory cimport shared_ptr, static_pointer_cast

cimport cython
cimport pyarrow.lib as ca
from pyarrow.lib cimport pyarrow_unwrap_array, pyarrow_wrap_array, pyarrow_wrap_batch, GetResultValue
from pyarrow.includes.libarrow cimport CArray, CInt64Array, CDoubleArray, CRecordBatch

# Import Arrow Python API
import pyarrow as pa

from .triangle_counting cimport (
    CTriangleCountResult,
    CountTriangles as CCountTriangles,
    CountTrianglesWeighted as CCountTrianglesWeighted,
    TopKTriangleCounts as CTopKTriangleCounts,
    ComputeTransitivity as CComputeTransitivity
)


cdef class PyTriangleCountResult:
    """
    Result of triangle counting operation.

    Attributes:
        per_vertex_triangles: Number of triangles each vertex participates in
        clustering_coefficient: Local clustering coefficient per vertex (0.0 to 1.0)
        total_triangles: Total number of triangles in the graph
        global_clustering_coefficient: Average local clustering coefficient
        num_vertices: Number of vertices in graph
    """
    cdef ca.Int64Array _per_vertex_triangles
    cdef ca.DoubleArray _clustering_coefficient
    cdef int64_t _total_triangles
    cdef double _global_clustering_coefficient
    cdef int64_t _num_vertices

    def __cinit__(self):
        self._total_triangles = 0
        self._global_clustering_coefficient = 0.0
        self._num_vertices = 0

    @staticmethod
    cdef PyTriangleCountResult from_cpp(CTriangleCountResult result):
        cdef PyTriangleCountResult py_result = PyTriangleCountResult()
        py_result._per_vertex_triangles = pyarrow_wrap_array(<shared_ptr[CArray]>result.per_vertex_triangles)
        py_result._clustering_coefficient = pyarrow_wrap_array(<shared_ptr[CArray]>result.clustering_coefficient)
        py_result._total_triangles = result.total_triangles
        py_result._global_clustering_coefficient = result.global_clustering_coefficient
        py_result._num_vertices = result.num_vertices
        return py_result

    def per_vertex_triangles(self):
        """Get per-vertex triangle counts as PyArrow array."""
        return self._per_vertex_triangles

    def clustering_coefficient(self):
        """Get local clustering coefficients as PyArrow array."""
        return self._clustering_coefficient

    def total_triangles(self):
        """Get total number of triangles in the graph."""
        return self._total_triangles

    def global_clustering_coefficient(self):
        """Get global clustering coefficient (average of local coefficients)."""
        return self._global_clustering_coefficient

    def num_vertices(self):
        """Get number of vertices."""
        return self._num_vertices

    def __repr__(self):
        return (
            f"TriangleCountResult(total_triangles={self._total_triangles}, "
            f"global_clustering={self._global_clustering_coefficient:.4f}, "
            f"num_vertices={self._num_vertices})"
        )


def count_triangles(
    ca.Int64Array indptr,
    ca.Int64Array indices,
    int64_t num_vertices
) -> PyTriangleCountResult:
    """
    Count triangles in an undirected graph using node-iterator method.

    For each vertex v, counts triangles by checking if pairs of v's neighbors
    are also connected. This is O(d^2 * V) where d is average degree.

    The graph is assumed to be undirected (edges in both directions).

    Args:
        indptr: CSR indptr array (length num_vertices + 1)
        indices: CSR indices array (neighbor lists)
        num_vertices: Number of vertices in graph

    Returns:
        PyTriangleCountResult with per-vertex and global counts

    Example:
        >>> # Triangle graph: 0-1, 1-2, 2-0
        >>> indptr = pa.array([0, 2, 4, 6], type=pa.int64())
        >>> indices = pa.array([1, 2, 0, 2, 0, 1], type=pa.int64())
        >>> result = count_triangles(indptr, indices, 3)
        >>> result.total_triangles()
        1
        >>> result.clustering_coefficient().to_pylist()
        [1.0, 1.0, 1.0]  # Perfect clustering
    """
    # Unwrap Arrow arrays to C++
    cdef shared_ptr[CArray] arr_base
    cdef shared_ptr[CInt64Array] c_indptr
    cdef shared_ptr[CInt64Array] c_indices

    arr_base = pyarrow_unwrap_array(indptr)
    c_indptr = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    arr_base = pyarrow_unwrap_array(indices)
    c_indices = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    # Call C++ CountTriangles
    cdef ca.CResult[CTriangleCountResult] result = CCountTriangles(
        c_indptr, c_indices, num_vertices)

    # Extract result value (raises exception if failed)
    cdef CTriangleCountResult c_result = GetResultValue(result)

    return PyTriangleCountResult.from_cpp(c_result)


def count_triangles_weighted(
    ca.Int64Array indptr,
    ca.Int64Array indices,
    ca.DoubleArray weights,
    int64_t num_vertices
) -> PyTriangleCountResult:
    """
    Count triangles in a weighted graph (weights ignored, topology only).

    Triangle counting is purely topological, so weights are ignored.
    This function is provided for API consistency with other graph algorithms.

    Args:
        indptr: CSR indptr array
        indices: CSR indices array
        weights: Edge weights (ignored)
        num_vertices: Number of vertices

    Returns:
        PyTriangleCountResult
    """
    # Unwrap arrays
    cdef shared_ptr[CArray] arr_base
    cdef shared_ptr[CInt64Array] c_indptr
    cdef shared_ptr[CInt64Array] c_indices
    cdef shared_ptr[CDoubleArray] c_weights

    arr_base = pyarrow_unwrap_array(indptr)
    c_indptr = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    arr_base = pyarrow_unwrap_array(indices)
    c_indices = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    arr_base = pyarrow_unwrap_array(weights)
    c_weights = <shared_ptr[CDoubleArray]>static_pointer_cast[CDoubleArray, CArray](arr_base)

    # Call C++ CountTrianglesWeighted
    cdef ca.CResult[CTriangleCountResult] result = CCountTrianglesWeighted(
        c_indptr, c_indices, c_weights, num_vertices)

    # Extract result
    cdef CTriangleCountResult c_result = GetResultValue(result)

    return PyTriangleCountResult.from_cpp(c_result)


def top_k_triangle_counts(
    ca.Int64Array per_vertex_triangles,
    ca.DoubleArray clustering_coefficient,
    int64_t k = 10
) -> pa.RecordBatch:
    """
    Get vertices with highest triangle counts.

    Args:
        per_vertex_triangles: Triangle counts per vertex
        clustering_coefficient: Clustering coefficient per vertex
        k: Number of top vertices to return (default: 10)

    Returns:
        RecordBatch with columns:
        - vertex_id: Vertex ID
        - triangle_count: Number of triangles
        - clustering_coefficient: Local clustering coefficient

    Example:
        >>> result = count_triangles(indptr, indices, num_vertices)
        >>> top_vertices = top_k_triangle_counts(
        ...     result.per_vertex_triangles(),
        ...     result.clustering_coefficient(),
        ...     k=5
        ... )
        >>> print(top_vertices.to_pandas())
           vertex_id  triangle_count  clustering_coefficient
        0          5              12                    0.75
        1          3               8                    0.67
        ...
    """
    # Unwrap arrays
    cdef shared_ptr[CArray] arr_base
    cdef shared_ptr[CInt64Array] c_triangles
    cdef shared_ptr[CDoubleArray] c_clustering

    arr_base = pyarrow_unwrap_array(per_vertex_triangles)
    c_triangles = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    arr_base = pyarrow_unwrap_array(clustering_coefficient)
    c_clustering = <shared_ptr[CDoubleArray]>static_pointer_cast[CDoubleArray, CArray](arr_base)

    # Call C++ TopKTriangleCounts
    cdef ca.CResult[shared_ptr[CRecordBatch]] result = CTopKTriangleCounts(
        c_triangles, c_clustering, k)

    # Extract result and wrap to Python
    cdef shared_ptr[CRecordBatch] result_batch = GetResultValue(result)
    return pyarrow_wrap_batch(result_batch)


def compute_transitivity(
    ca.Int64Array indptr,
    ca.Int64Array indices,
    int64_t num_vertices
) -> float:
    """
    Compute transitivity (global clustering coefficient) of the graph.

    Transitivity measures the probability that the adjacent vertices of a vertex
    are also connected. It's the ratio of triangles to connected triples.

    Transitivity = 3 * (number of triangles) / (number of connected triples)

    Args:
        indptr: CSR indptr array
        indices: CSR indices array
        num_vertices: Number of vertices

    Returns:
        Transitivity score (0.0 to 1.0)

    Example:
        >>> # Complete graph K4
        >>> transitivity = compute_transitivity(indptr, indices, 4)
        >>> print(f"Transitivity: {transitivity:.3f}")
        Transitivity: 1.000  # All triples close into triangles
    """
    # Unwrap arrays
    cdef shared_ptr[CArray] arr_base
    cdef shared_ptr[CInt64Array] c_indptr
    cdef shared_ptr[CInt64Array] c_indices

    arr_base = pyarrow_unwrap_array(indptr)
    c_indptr = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    arr_base = pyarrow_unwrap_array(indices)
    c_indices = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    # Call C++ ComputeTransitivity
    cdef ca.CResult[double] result = CComputeTransitivity(
        c_indptr, c_indices, num_vertices)

    # Extract result
    cdef double transitivity = GetResultValue(result)
    return transitivity


# Convenience aliases
triangle_count = count_triangles
clustering = count_triangles
transitivity = compute_transitivity
