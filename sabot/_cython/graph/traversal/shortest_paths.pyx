# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
"""
Shortest Paths Cython Bindings

Python-accessible wrappers for C++ shortest paths algorithms.
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
# Shortest Paths Result Wrapper
# ============================================================================

cdef class PyShortestPathsResult:
    """
    Shortest paths result.

    Attributes:
        distances: Distance from source to each vertex (inf if unreachable)
        predecessors: Parent vertex in shortest path tree (-1 if no parent)
        source: Source vertex ID
        num_reached: Number of reachable vertices from source
    """

    def __cinit__(self, ca.DoubleArray distances, ca.Int64Array predecessors,
                  int64_t source, int64_t num_reached):
        self._distances = distances
        self._predecessors = predecessors
        self._source = source
        self._num_reached = num_reached

    cpdef ca.DoubleArray distances(self):
        """Get distances from source."""
        return self._distances

    cpdef ca.Int64Array predecessors(self):
        """Get predecessor vertices in shortest path tree."""
        return self._predecessors

    cpdef int64_t source(self):
        """Get source vertex."""
        return self._source

    cpdef int64_t num_reached(self):
        """Get number of reachable vertices."""
        return self._num_reached

    def __repr__(self):
        return (f"PyShortestPathsResult(source={self._source}, "
                f"num_reached={self._num_reached}/{len(self._distances)})")


# ============================================================================
# Shortest Paths Functions
# ============================================================================

def dijkstra(ca.Int64Array indptr, ca.Int64Array indices, ca.DoubleArray weights,
             int64_t num_vertices, int64_t source):
    """
    Dijkstra's single-source shortest paths for weighted graphs.

    Args:
        indptr: CSR indptr array (offsets for each vertex's neighbors)
        indices: CSR indices array (neighbor vertex IDs)
        weights: Edge weights (parallel to indices, must be >= 0)
        num_vertices: Total number of vertices in graph
        source: Source vertex ID to start from

    Returns:
        PyShortestPathsResult with distances and predecessors

    Raises:
        RuntimeError: If weights contain negative values

    Performance: O((V + E) log V) time, O(V) space

    Example:
        >>> csr = graph.csr()
        >>> weights = pa.array([1.0, 2.5, 3.0, 1.5], type=pa.float64())
        >>> result = dijkstra(csr.indptr, csr.indices, weights, graph.num_vertices(), source=0)
        >>> print(f"Distance to vertex 3: {result.distances()[3]}")
    """
    # Unwrap Arrow arrays to C++
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

    # Call C++ Dijkstra
    cdef ca.CResult[ShortestPathsResult] result = Dijkstra(
        c_indptr, c_indices, c_weights, num_vertices, source)

    # Extract result value (raises exception if failed)
    cdef ShortestPathsResult sp_res = GetResultValue(result)

    # Wrap C++ arrays back to Python
    distances_py = pyarrow_wrap_array(<shared_ptr[CArray]>sp_res.distances)
    predecessors_py = pyarrow_wrap_array(<shared_ptr[CArray]>sp_res.predecessors)

    return PyShortestPathsResult(distances_py, predecessors_py, sp_res.source, sp_res.num_reached)


def unweighted_shortest_paths(ca.Int64Array indptr, ca.Int64Array indices,
                               int64_t num_vertices, int64_t source):
    """
    Unweighted shortest paths (BFS-based, all edges have weight 1).

    Faster than Dijkstra for unweighted graphs.

    Args:
        indptr: CSR indptr array
        indices: CSR indices array
        num_vertices: Total number of vertices
        source: Source vertex ID

    Returns:
        PyShortestPathsResult with distances and predecessors

    Performance: O(V + E) time, O(V) space

    Example:
        >>> csr = graph.csr()
        >>> result = unweighted_shortest_paths(csr.indptr, csr.indices, graph.num_vertices(), source=0)
        >>> print(f"Shortest path length to vertex 3: {int(result.distances()[3])}")
    """
    # Unwrap arrays
    cdef shared_ptr[CArray] arr_base
    cdef shared_ptr[CInt64Array] c_indptr
    cdef shared_ptr[CInt64Array] c_indices

    arr_base = pyarrow_unwrap_array(indptr)
    c_indptr = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    arr_base = pyarrow_unwrap_array(indices)
    c_indices = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    # Call C++ UnweightedShortestPaths
    cdef ca.CResult[ShortestPathsResult] result = UnweightedShortestPaths(
        c_indptr, c_indices, num_vertices, source)

    # Extract result value (raises exception if failed)
    cdef ShortestPathsResult sp_res = GetResultValue(result)

    # Wrap results
    distances_py = pyarrow_wrap_array(<shared_ptr[CArray]>sp_res.distances)
    predecessors_py = pyarrow_wrap_array(<shared_ptr[CArray]>sp_res.predecessors)

    return PyShortestPathsResult(distances_py, predecessors_py, sp_res.source, sp_res.num_reached)


def reconstruct_path(ca.Int64Array predecessors, int64_t source, int64_t target):
    """
    Reconstruct shortest path from source to target.

    Uses predecessors from shortest paths result to extract the actual path.

    Args:
        predecessors: Predecessor array from shortest paths result
        source: Source vertex ID
        target: Target vertex ID

    Returns:
        Int64Array with path vertices [source, ..., target]
        Empty array if target is unreachable

    Performance: O(path_length) time, O(path_length) space

    Example:
        >>> result = dijkstra(csr.indptr, csr.indices, weights, num_vertices, source=0)
        >>> path = reconstruct_path(result.predecessors(), source=0, target=3)
        >>> print(f"Path from 0 to 3: {path.to_pylist()}")
    """
    # Unwrap array
    cdef shared_ptr[CArray] arr_base
    cdef shared_ptr[CInt64Array] c_predecessors

    arr_base = pyarrow_unwrap_array(predecessors)
    c_predecessors = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    # Call C++ ReconstructPath
    cdef ca.CResult[shared_ptr[CInt64Array]] result = ReconstructPath(
        c_predecessors, source, target)

    # Extract result and wrap to Python
    cdef shared_ptr[CInt64Array] result_array = GetResultValue(result)
    return pyarrow_wrap_array(<shared_ptr[CArray]>result_array)


def floyd_warshall(ca.Int64Array indptr, ca.Int64Array indices, ca.DoubleArray weights,
                   int64_t num_vertices):
    """
    All-pairs shortest paths (Floyd-Warshall algorithm).

    Computes shortest paths between all pairs of vertices.
    WARNING: O(V^3) time complexity - only suitable for small graphs (<500 vertices).

    Args:
        indptr: CSR indptr array
        indices: CSR indices array
        weights: Edge weights
        num_vertices: Total number of vertices

    Returns:
        RecordBatch with schema:
        - src: int64 (source vertex)
        - dst: int64 (destination vertex)
        - distance: double (shortest distance)
        - via: int64 (intermediate vertex for path reconstruction, -1 for direct)

    Raises:
        RuntimeError: If num_vertices > 500 (too large)

    Performance: O(V^3) time, O(V^2) space
    Memory usage: ~16 bytes per vertex pair (V^2)

    Example:
        >>> csr = graph.csr()
        >>> weights = pa.array([1.0, 2.5, 3.0], type=pa.float64())
        >>> all_pairs = floyd_warshall(csr.indptr, csr.indices, weights, graph.num_vertices())
        >>> print(f"All-pairs shortest paths: {len(all_pairs)} pairs")
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

    # Call C++ FloydWarshall
    cdef ca.CResult[shared_ptr[CRecordBatch]] result = FloydWarshall(
        c_indptr, c_indices, c_weights, num_vertices)

    # Extract result and wrap to Python
    cdef shared_ptr[CRecordBatch] result_batch = GetResultValue(result)
    return pyarrow_wrap_batch(result_batch)


def astar(ca.Int64Array indptr, ca.Int64Array indices, ca.DoubleArray weights,
          int64_t num_vertices, int64_t source, int64_t target,
          ca.DoubleArray heuristic):
    """
    A* shortest path from source to target (heuristic-based).

    Uses heuristic function to guide search towards target.
    More efficient than Dijkstra when target is known and heuristic is good.

    Args:
        indptr: CSR indptr array
        indices: CSR indices array
        weights: Edge weights
        num_vertices: Total number of vertices
        source: Source vertex ID
        target: Target vertex ID
        heuristic: Heuristic estimates (h[v] = estimated distance from v to target)

    Returns:
        PyShortestPathsResult with path from source to target

    Requirements:
    - heuristic must be admissible (never overestimate true distance)
    - heuristic.length() == num_vertices

    Performance: O((V + E) log V) worst case, often much better with good heuristic

    Example:
        >>> csr = graph.csr()
        >>> weights = pa.array([1.0, 2.5, 3.0], type=pa.float64())
        >>> # Euclidean distance heuristic for grid graph
        >>> heuristic = pa.array([3.0, 2.2, 1.4, 0.0], type=pa.float64())
        >>> result = astar(csr.indptr, csr.indices, weights, num_vertices, source=0, target=3, heuristic=heuristic)
        >>> print(f"Shortest path distance: {result.distances()[3]}")
    """
    # Unwrap arrays
    cdef shared_ptr[CArray] arr_base
    cdef shared_ptr[CInt64Array] c_indptr
    cdef shared_ptr[CInt64Array] c_indices
    cdef shared_ptr[CDoubleArray] c_weights
    cdef shared_ptr[CDoubleArray] c_heuristic

    arr_base = pyarrow_unwrap_array(indptr)
    c_indptr = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    arr_base = pyarrow_unwrap_array(indices)
    c_indices = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    arr_base = pyarrow_unwrap_array(weights)
    c_weights = <shared_ptr[CDoubleArray]>static_pointer_cast[CDoubleArray, CArray](arr_base)

    arr_base = pyarrow_unwrap_array(heuristic)
    c_heuristic = <shared_ptr[CDoubleArray]>static_pointer_cast[CDoubleArray, CArray](arr_base)

    # Call C++ AStar
    cdef ca.CResult[ShortestPathsResult] result = AStar(
        c_indptr, c_indices, c_weights, num_vertices, source, target, c_heuristic)

    # Extract result value (raises exception if failed)
    cdef ShortestPathsResult sp_res = GetResultValue(result)

    # Wrap results
    distances_py = pyarrow_wrap_array(<shared_ptr[CArray]>sp_res.distances)
    predecessors_py = pyarrow_wrap_array(<shared_ptr[CArray]>sp_res.predecessors)

    return PyShortestPathsResult(distances_py, predecessors_py, sp_res.source, sp_res.num_reached)
