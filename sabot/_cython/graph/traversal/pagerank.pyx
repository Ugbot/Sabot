# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
"""
PageRank Cython Bindings

Python-accessible wrappers for C++ PageRank algorithms.
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
# PageRank Result Wrapper
# ============================================================================

cdef class PyPageRankResult:
    """
    PageRank result.

    Attributes:
        ranks: PageRank scores for each vertex (sum to 1.0)
        num_vertices: Total number of vertices
        num_iterations: Number of iterations until convergence
        final_delta: Final convergence metric (max rank change)
        converged: True if converged before max iterations
    """
    cdef ca.DoubleArray _ranks
    cdef int64_t _num_vertices
    cdef int64_t _num_iterations
    cdef double _final_delta
    cdef bint _converged

    def __cinit__(self, ca.DoubleArray ranks, int64_t num_vertices,
                  int64_t num_iterations, double final_delta, bint converged):
        self._ranks = ranks
        self._num_vertices = num_vertices
        self._num_iterations = num_iterations
        self._final_delta = final_delta
        self._converged = converged

    cpdef ca.DoubleArray ranks(self):
        """Get PageRank scores."""
        return self._ranks

    cpdef int64_t num_vertices(self):
        """Get number of vertices."""
        return self._num_vertices

    cpdef int64_t num_iterations(self):
        """Get number of iterations performed."""
        return self._num_iterations

    cpdef double final_delta(self):
        """Get final convergence delta."""
        return self._final_delta

    cpdef bint converged(self):
        """Check if algorithm converged."""
        return self._converged

    def __repr__(self):
        status = "converged" if self._converged else "max iterations"
        return (f"PyPageRankResult(vertices={self._num_vertices}, "
                f"iterations={self._num_iterations}, {status})")


# ============================================================================
# PageRank Functions
# ============================================================================

def pagerank(ca.Int64Array indptr, ca.Int64Array indices, int64_t num_vertices,
             double damping=0.85, int64_t max_iterations=100, double tolerance=1e-6):
    """
    Compute PageRank for all vertices in the graph.

    PageRank measures vertex importance based on the link structure:
    - Important vertices have many incoming links
    - Links from important vertices count more

    Args:
        indptr: CSR indptr array (offsets for each vertex's neighbors)
        indices: CSR indices array (neighbor vertex IDs)
        num_vertices: Total number of vertices in graph
        damping: Damping factor (typically 0.85), controls teleportation probability
        max_iterations: Maximum number of iterations (default 100)
        tolerance: Convergence tolerance (default 1e-6)

    Returns:
        PyPageRankResult with ranks and convergence info

    Performance: O(E * iterations) time, O(V) space

    Example:
        >>> csr = graph.csr()
        >>> result = pagerank(csr.indptr, csr.indices, graph.num_vertices())
        >>> print(f"Top vertex rank: {max(result.ranks().to_pylist())}")
        >>> print(f"Converged in {result.num_iterations()} iterations")
    """
    # Unwrap Arrow arrays to C++
    cdef shared_ptr[CArray] arr_base
    cdef shared_ptr[CInt64Array] c_indptr
    cdef shared_ptr[CInt64Array] c_indices

    arr_base = pyarrow_unwrap_array(indptr)
    c_indptr = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    arr_base = pyarrow_unwrap_array(indices)
    c_indices = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    # Call C++ PageRank
    cdef ca.CResult[PageRankResult] result = PageRank(
        c_indptr, c_indices, num_vertices, damping, max_iterations, tolerance)

    # Extract result value (raises exception if failed)
    cdef PageRankResult pr_res = GetResultValue(result)

    # Wrap C++ arrays back to Python
    ranks_py = pyarrow_wrap_array(<shared_ptr[CArray]>pr_res.ranks)

    return PyPageRankResult(ranks_py, pr_res.num_vertices, pr_res.num_iterations,
                            pr_res.final_delta, pr_res.converged)


def personalized_pagerank(ca.Int64Array indptr, ca.Int64Array indices,
                          int64_t num_vertices, ca.Int64Array source_vertices,
                          double damping=0.85, int64_t max_iterations=100,
                          double tolerance=1e-6):
    """
    Compute Personalized PageRank from a set of source vertices.

    Like standard PageRank, but random teleportation goes only to source vertices.
    Useful for:
    - Topic-specific importance ranking
    - Personalized recommendations
    - Local graph importance

    Args:
        indptr: CSR indptr array
        indices: CSR indices array
        num_vertices: Total number of vertices
        source_vertices: Source vertex IDs for teleportation (Int64Array)
        damping: Damping factor (default 0.85)
        max_iterations: Maximum iterations (default 100)
        tolerance: Convergence tolerance (default 1e-6)

    Returns:
        PyPageRankResult with personalized ranks

    Performance: O(E * iterations) time, O(V) space

    Example:
        >>> # Find pages most related to "Python" and "Machine Learning"
        >>> sources = pa.array([python_id, ml_id], type=pa.int64())
        >>> result = personalized_pagerank(csr.indptr, csr.indices, num_vertices, sources)
        >>> top_related = top_k_pagerank(result.ranks(), k=10)
    """
    # Unwrap arrays
    cdef shared_ptr[CArray] arr_base
    cdef shared_ptr[CInt64Array] c_indptr
    cdef shared_ptr[CInt64Array] c_indices
    cdef shared_ptr[CInt64Array] c_sources

    arr_base = pyarrow_unwrap_array(indptr)
    c_indptr = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    arr_base = pyarrow_unwrap_array(indices)
    c_indices = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    arr_base = pyarrow_unwrap_array(source_vertices)
    c_sources = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    # Call C++ PersonalizedPageRank
    cdef ca.CResult[PageRankResult] result = PersonalizedPageRank(
        c_indptr, c_indices, num_vertices, c_sources, damping, max_iterations, tolerance)

    # Extract result
    cdef PageRankResult pr_res = GetResultValue(result)

    # Wrap results
    ranks_py = pyarrow_wrap_array(<shared_ptr[CArray]>pr_res.ranks)

    return PyPageRankResult(ranks_py, pr_res.num_vertices, pr_res.num_iterations,
                            pr_res.final_delta, pr_res.converged)


def weighted_pagerank(ca.Int64Array indptr, ca.Int64Array indices,
                      ca.DoubleArray weights, int64_t num_vertices,
                      double damping=0.85, int64_t max_iterations=100,
                      double tolerance=1e-6):
    """
    Compute weighted PageRank with edge weights.

    Edge weights influence how much importance flows along each edge.
    Useful for graphs where edges have varying importance (e.g., citation counts,
    social network interactions, link relevance).

    Args:
        indptr: CSR indptr array
        indices: CSR indices array
        weights: Edge weights (parallel to indices, must be non-negative)
        num_vertices: Total number of vertices
        damping: Damping factor (default 0.85)
        max_iterations: Maximum iterations (default 100)
        tolerance: Convergence tolerance (default 1e-6)

    Returns:
        PyPageRankResult with weighted ranks

    Raises:
        RuntimeError: If weights contain negative values

    Performance: O(E * iterations) time, O(V) space

    Example:
        >>> # Compute PageRank with citation counts as weights
        >>> weights = pa.array([1.0, 5.0, 2.0, 10.0], type=pa.float64())
        >>> result = weighted_pagerank(csr.indptr, csr.indices, weights, num_vertices)
        >>> print(f"Most cited paper rank: {max(result.ranks().to_pylist())}")
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

    # Call C++ WeightedPageRank
    cdef ca.CResult[PageRankResult] result = WeightedPageRank(
        c_indptr, c_indices, c_weights, num_vertices, damping, max_iterations, tolerance)

    # Extract result
    cdef PageRankResult pr_res = GetResultValue(result)

    # Wrap results
    ranks_py = pyarrow_wrap_array(<shared_ptr[CArray]>pr_res.ranks)

    return PyPageRankResult(ranks_py, pr_res.num_vertices, pr_res.num_iterations,
                            pr_res.final_delta, pr_res.converged)


def top_k_pagerank(ca.DoubleArray ranks, int64_t k):
    """
    Get top-K vertices by PageRank score.

    Returns the K highest-ranked vertices in descending order.

    Args:
        ranks: PageRank scores from pagerank()
        k: Number of top vertices to return

    Returns:
        RecordBatch with columns:
        - vertex_id: int64 (vertex ID)
        - rank: double (PageRank score)

    Performance: O(V log K) time, O(K) space

    Example:
        >>> result = pagerank(csr.indptr, csr.indices, num_vertices)
        >>> top_10 = top_k_pagerank(result.ranks(), k=10)
        >>> for i in range(len(top_10)):
        ...     vertex_id = top_10['vertex_id'][i].as_py()
        ...     rank = top_10['rank'][i].as_py()
        ...     print(f"Vertex {vertex_id}: {rank:.6f}")
    """
    # Unwrap array
    cdef shared_ptr[CArray] arr_base
    cdef shared_ptr[CDoubleArray] c_ranks

    arr_base = pyarrow_unwrap_array(ranks)
    c_ranks = <shared_ptr[CDoubleArray]>static_pointer_cast[CDoubleArray, CArray](arr_base)

    # Call C++ TopKPageRank
    cdef ca.CResult[shared_ptr[CRecordBatch]] result = TopKPageRank(c_ranks, k)

    # Extract result and wrap to Python
    cdef shared_ptr[CRecordBatch] result_batch = GetResultValue(result)
    return pyarrow_wrap_batch(result_batch)
