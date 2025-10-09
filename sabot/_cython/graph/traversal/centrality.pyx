# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
"""
Centrality Cython Bindings

Python-accessible wrappers for C++ centrality algorithms.
"""

from libc.stdint cimport int64_t
from libcpp.memory cimport shared_ptr, static_pointer_cast

cimport cython
cimport pyarrow.lib as ca
from pyarrow.lib cimport pyarrow_unwrap_array, pyarrow_wrap_array, pyarrow_wrap_batch, GetResultValue
from pyarrow.includes.libarrow cimport CArray, CInt64Array, CDoubleArray, CRecordBatch

# Import Arrow Python API
import pyarrow as pa


# ============================================================================
# Betweenness Centrality Result Wrapper
# ============================================================================

cdef class PyBetweennessCentralityResult:
    """
    Betweenness centrality result.

    Attributes:
        centrality: Betweenness scores for each vertex
        num_vertices: Total number of vertices
        normalized: True if scores are normalized
    """
    cdef ca.DoubleArray _centrality
    cdef int64_t _num_vertices
    cdef bint _normalized

    def __cinit__(self, ca.DoubleArray centrality, int64_t num_vertices, bint normalized):
        self._centrality = centrality
        self._num_vertices = num_vertices
        self._normalized = normalized

    cpdef ca.DoubleArray centrality(self):
        """Get betweenness centrality scores."""
        return self._centrality

    cpdef int64_t num_vertices(self):
        """Get number of vertices."""
        return self._num_vertices

    cpdef bint normalized(self):
        """Check if scores are normalized."""
        return self._normalized

    def __repr__(self):
        norm_str = "normalized" if self._normalized else "unnormalized"
        return (f"PyBetweennessCentralityResult(vertices={self._num_vertices}, {norm_str})")


# ============================================================================
# Betweenness Centrality Functions
# ============================================================================

def betweenness_centrality(ca.Int64Array indptr, ca.Int64Array indices,
                           int64_t num_vertices, bint normalized=True):
    """
    Compute betweenness centrality using Brandes' algorithm.

    Betweenness centrality measures how often a vertex lies on shortest paths
    between other vertices. High betweenness vertices are important connectors
    and potential bottlenecks.

    Formula:
        BC(v) = sum_{s≠v≠t} (σ_st(v) / σ_st)
    where:
        σ_st = number of shortest paths from s to t
        σ_st(v) = number of those paths passing through v

    Args:
        indptr: CSR indptr array (offsets for each vertex's neighbors)
        indices: CSR indices array (neighbor vertex IDs)
        num_vertices: Total number of vertices in graph
        normalized: If True, normalize by (n-1)(n-2)/2 (default True)

    Returns:
        PyBetweennessCentralityResult with centrality scores

    Performance: O(VE) time, O(V+E) space

    Example:
        >>> csr = graph.csr()
        >>> result = betweenness_centrality(csr.indptr, csr.indices, graph.num_vertices())
        >>> print(f"Max betweenness: {max(result.centrality().to_pylist())}")
    """
    # Unwrap Arrow arrays to C++
    cdef shared_ptr[CArray] arr_base
    cdef shared_ptr[CInt64Array] c_indptr
    cdef shared_ptr[CInt64Array] c_indices

    arr_base = pyarrow_unwrap_array(indptr)
    c_indptr = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    arr_base = pyarrow_unwrap_array(indices)
    c_indices = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    # Call C++ BetweennessCentrality
    cdef ca.CResult[BetweennessCentralityResult] result = BetweennessCentrality(
        c_indptr, c_indices, num_vertices, normalized)

    # Extract result value (raises exception if failed)
    cdef BetweennessCentralityResult bc_res = GetResultValue(result)

    # Wrap C++ arrays back to Python
    centrality_py = pyarrow_wrap_array(<shared_ptr[CArray]>bc_res.centrality)

    return PyBetweennessCentralityResult(centrality_py, bc_res.num_vertices, bc_res.normalized)


def weighted_betweenness_centrality(ca.Int64Array indptr, ca.Int64Array indices,
                                    ca.DoubleArray weights, int64_t num_vertices,
                                    bint normalized=True):
    """
    Compute weighted betweenness centrality (Dijkstra-based).

    Like betweenness centrality, but uses edge weights for shortest path
    calculations. Useful for weighted networks where edge importance varies.

    Args:
        indptr: CSR indptr array
        indices: CSR indices array
        weights: Edge weights (parallel to indices, must be >= 0)
        num_vertices: Total number of vertices
        normalized: If True, normalize scores (default True)

    Returns:
        PyBetweennessCentralityResult with weighted centrality scores

    Raises:
        RuntimeError: If weights contain negative values

    Performance: O(VE + V² log V) time, O(V+E) space

    Example:
        >>> csr = graph.csr()
        >>> weights = pa.array([1.0, 2.0, 0.5], type=pa.float64())
        >>> result = weighted_betweenness_centrality(
        ...     csr.indptr, csr.indices, weights, graph.num_vertices())
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

    # Call C++ WeightedBetweennessCentrality
    cdef ca.CResult[BetweennessCentralityResult] result = WeightedBetweennessCentrality(
        c_indptr, c_indices, c_weights, num_vertices, normalized)

    # Extract result
    cdef BetweennessCentralityResult bc_res = GetResultValue(result)

    # Wrap results
    centrality_py = pyarrow_wrap_array(<shared_ptr[CArray]>bc_res.centrality)

    return PyBetweennessCentralityResult(centrality_py, bc_res.num_vertices, bc_res.normalized)


def edge_betweenness_centrality(ca.Int64Array indptr, ca.Int64Array indices,
                                int64_t num_vertices):
    """
    Compute edge betweenness centrality.

    Measures how often each edge lies on shortest paths. High edge betweenness
    indicates edges that connect different communities or regions of the graph.
    Useful for community detection via edge removal.

    Args:
        indptr: CSR indptr array
        indices: CSR indices array
        num_vertices: Total number of vertices

    Returns:
        RecordBatch with columns:
        - src: int64 (source vertex)
        - dst: int64 (destination vertex)
        - betweenness: double (edge betweenness score)

    Performance: O(VE) time, O(E) space

    Example:
        >>> csr = graph.csr()
        >>> result = edge_betweenness_centrality(csr.indptr, csr.indices, graph.num_vertices())
        >>> # Find edges with highest betweenness (community boundaries)
        >>> for i in range(min(5, len(result))):
        ...     src = result['src'][i].as_py()
        ...     dst = result['dst'][i].as_py()
        ...     score = result['betweenness'][i].as_py()
        ...     print(f"Edge {src}->{dst}: {score:.4f}")
    """
    # Unwrap arrays
    cdef shared_ptr[CArray] arr_base
    cdef shared_ptr[CInt64Array] c_indptr
    cdef shared_ptr[CInt64Array] c_indices

    arr_base = pyarrow_unwrap_array(indptr)
    c_indptr = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    arr_base = pyarrow_unwrap_array(indices)
    c_indices = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    # Call C++ EdgeBetweennessCentrality
    cdef ca.CResult[shared_ptr[CRecordBatch]] result = EdgeBetweennessCentrality(
        c_indptr, c_indices, num_vertices)

    # Extract result and wrap to Python
    cdef shared_ptr[CRecordBatch] result_batch = GetResultValue(result)
    return pyarrow_wrap_batch(result_batch)


def top_k_betweenness(ca.DoubleArray centrality, int64_t k):
    """
    Get top-K vertices by betweenness centrality.

    Returns the K vertices with highest betweenness scores in descending order.

    Args:
        centrality: Betweenness scores from betweenness_centrality()
        k: Number of top vertices to return

    Returns:
        RecordBatch with columns:
        - vertex_id: int64 (vertex ID)
        - betweenness: double (betweenness score)

    Performance: O(V log K) time, O(K) space

    Example:
        >>> result = betweenness_centrality(csr.indptr, csr.indices, num_vertices)
        >>> top_5 = top_k_betweenness(result.centrality(), k=5)
        >>> for i in range(len(top_5)):
        ...     vertex_id = top_5['vertex_id'][i].as_py()
        ...     score = top_5['betweenness'][i].as_py()
        ...     print(f"Vertex {vertex_id}: betweenness = {score:.6f}")
    """
    # Unwrap array
    cdef shared_ptr[CArray] arr_base
    cdef shared_ptr[CDoubleArray] c_centrality

    arr_base = pyarrow_unwrap_array(centrality)
    c_centrality = <shared_ptr[CDoubleArray]>static_pointer_cast[CDoubleArray, CArray](arr_base)

    # Call C++ TopKBetweenness
    cdef ca.CResult[shared_ptr[CRecordBatch]] result = TopKBetweenness(c_centrality, k)

    # Extract result and wrap to Python
    cdef shared_ptr[CRecordBatch] result_batch = GetResultValue(result)
    return pyarrow_wrap_batch(result_batch)
