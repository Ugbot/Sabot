# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
"""
BFS Cython Bindings

Python-accessible wrappers for C++ BFS algorithms.
"""

from libc.stdint cimport int64_t
from libcpp.memory cimport shared_ptr, static_pointer_cast

cimport cython
cimport pyarrow.lib as ca
from pyarrow.lib cimport pyarrow_unwrap_array, pyarrow_wrap_array, GetResultValue
from pyarrow.includes.libarrow cimport CArray, CInt64Array

# Import Arrow Python API
import pyarrow as pa


# ============================================================================
# BFS Result Wrapper
# ============================================================================

cdef class PyBFSResult:
    """
    BFS traversal result.

    Attributes:
        vertices: Visited vertex IDs in BFS order
        distances: Distance from source for each vertex
        predecessors: Parent vertex in BFS tree (-1 for source)
        num_visited: Total number of vertices visited
    """

    def __cinit__(self, ca.Int64Array vertices, ca.Int64Array distances,
                  ca.Int64Array predecessors, int64_t num_visited):
        self._vertices = vertices
        self._distances = distances
        self._predecessors = predecessors
        self._num_visited = num_visited

    cpdef ca.Int64Array vertices(self):
        """Get visited vertices in BFS order."""
        return self._vertices

    cpdef ca.Int64Array distances(self):
        """Get distances from source."""
        return self._distances

    cpdef ca.Int64Array predecessors(self):
        """Get predecessor vertices in BFS tree."""
        return self._predecessors

    cpdef int64_t num_visited(self):
        """Get number of vertices visited."""
        return self._num_visited

    def __repr__(self):
        return (f"PyBFSResult(num_visited={self._num_visited}, "
                f"vertices={len(self._vertices)}, "
                f"avg_distance={self._distances.to_pylist() and sum(self._distances.to_pylist()) / len(self._distances.to_pylist()) or 0:.2f})")


# ============================================================================
# BFS Functions
# ============================================================================

def bfs(ca.Int64Array indptr, ca.Int64Array indices, int64_t num_vertices,
        int64_t source, int64_t max_depth=-1):
    """
    Breadth-first search from a source vertex.

    Args:
        indptr: CSR indptr array (offsets for each vertex's neighbors)
        indices: CSR indices array (neighbor vertex IDs)
        num_vertices: Total number of vertices in graph
        source: Source vertex ID to start traversal
        max_depth: Maximum depth to traverse (-1 = unlimited)

    Returns:
        PyBFSResult with vertices, distances, and predecessors

    Performance: O(V + E) time, O(V) space

    Example:
        >>> csr = graph.csr()
        >>> result = bfs(csr.indptr(), csr.indices(), graph.num_vertices(), source=0)
        >>> print(f"Visited {result.num_visited()} vertices")
        >>> print(f"Distances: {result.distances().to_pylist()}")
    """
    # Unwrap Arrow arrays to C++
    cdef shared_ptr[CArray] arr_base
    cdef shared_ptr[CInt64Array] c_indptr
    cdef shared_ptr[CInt64Array] c_indices

    arr_base = pyarrow_unwrap_array(indptr)
    c_indptr = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    arr_base = pyarrow_unwrap_array(indices)
    c_indices = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    # Call C++ BFS
    cdef ca.CResult[BFSResult] result = BFS(c_indptr, c_indices, num_vertices, source, max_depth)

    # Extract result value (raises exception if failed)
    cdef BFSResult bfs_res = GetResultValue(result)

    # Wrap C++ arrays back to Python
    vertices_py = pyarrow_wrap_array(<shared_ptr[CArray]>bfs_res.vertices)
    distances_py = pyarrow_wrap_array(<shared_ptr[CArray]>bfs_res.distances)
    predecessors_py = pyarrow_wrap_array(<shared_ptr[CArray]>bfs_res.predecessors)

    return PyBFSResult(vertices_py, distances_py, predecessors_py, bfs_res.num_visited)


def k_hop_neighbors(ca.Int64Array indptr, ca.Int64Array indices, int64_t num_vertices,
                    int64_t source, int64_t k):
    """
    Get all vertices exactly k hops away from source.

    Args:
        indptr: CSR indptr array
        indices: CSR indices array
        num_vertices: Total number of vertices
        source: Source vertex ID
        k: Number of hops (must be >= 0)

    Returns:
        Int64Array of vertices exactly k hops from source

    Performance: O(V + E) worst case, typically much faster

    Example:
        >>> # Get all vertices 2 hops from vertex 0
        >>> neighbors = k_hop_neighbors(csr.indptr(), csr.indices(), num_vertices, 0, 2)
        >>> print(f"2-hop neighbors: {neighbors.to_pylist()}")
    """
    # Unwrap Arrow arrays
    cdef shared_ptr[CArray] arr_base
    cdef shared_ptr[CInt64Array] c_indptr
    cdef shared_ptr[CInt64Array] c_indices

    arr_base = pyarrow_unwrap_array(indptr)
    c_indptr = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    arr_base = pyarrow_unwrap_array(indices)
    c_indices = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    # Call C++ KHopNeighbors
    cdef ca.CResult[shared_ptr[CInt64Array]] result = KHopNeighbors(
        c_indptr, c_indices, num_vertices, source, k)

    # Extract result and wrap to Python
    cdef shared_ptr[CInt64Array] result_array = GetResultValue(result)
    return pyarrow_wrap_array(<shared_ptr[CArray]>result_array)


def multi_bfs(ca.Int64Array indptr, ca.Int64Array indices, int64_t num_vertices,
              ca.Int64Array sources, int64_t max_depth=-1):
    """
    Multi-source BFS (breadth-first from multiple sources simultaneously).

    Args:
        indptr: CSR indptr array
        indices: CSR indices array
        num_vertices: Total number of vertices
        sources: Array of source vertex IDs
        max_depth: Maximum depth to traverse (-1 = unlimited)

    Returns:
        PyBFSResult with vertices and distances (predecessors = closest source)

    Performance: O(V + E) time, O(V) space

    Example:
        >>> # BFS from multiple sources
        >>> sources = pa.array([0, 10, 20], type=pa.int64())
        >>> result = multi_bfs(csr.indptr(), csr.indices(), num_vertices, sources)
        >>> print(f"Reachable from sources: {result.num_visited()} vertices")
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

    arr_base = pyarrow_unwrap_array(sources)
    c_sources = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    # Call C++ MultiBFS
    cdef ca.CResult[BFSResult] result = MultiBFS(c_indptr, c_indices, num_vertices, c_sources, max_depth)

    # Extract result value (raises exception if failed)
    cdef BFSResult bfs_res = GetResultValue(result)

    # Wrap results
    vertices_py = pyarrow_wrap_array(<shared_ptr[CArray]>bfs_res.vertices)
    distances_py = pyarrow_wrap_array(<shared_ptr[CArray]>bfs_res.distances)
    predecessors_py = pyarrow_wrap_array(<shared_ptr[CArray]>bfs_res.predecessors)

    return PyBFSResult(vertices_py, distances_py, predecessors_py, bfs_res.num_visited)


def component_bfs(ca.Int64Array indptr, ca.Int64Array indices, int64_t num_vertices,
                  int64_t source):
    """
    Get the connected component containing a source vertex (using BFS).

    Args:
        indptr: CSR indptr array
        indices: CSR indices array
        num_vertices: Total number of vertices
        source: Source vertex ID

    Returns:
        Int64Array of all vertices in the same connected component

    Performance: O(V + E) for the component

    Example:
        >>> # Find all vertices connected to vertex 0
        >>> component = component_bfs(csr.indptr(), csr.indices(), num_vertices, 0)
        >>> print(f"Component size: {len(component)}")
    """
    # Unwrap arrays
    cdef shared_ptr[CArray] arr_base
    cdef shared_ptr[CInt64Array] c_indptr
    cdef shared_ptr[CInt64Array] c_indices

    arr_base = pyarrow_unwrap_array(indptr)
    c_indptr = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    arr_base = pyarrow_unwrap_array(indices)
    c_indices = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    # Call C++ ComponentBFS
    cdef ca.CResult[shared_ptr[CInt64Array]] result = ComponentBFS(
        c_indptr, c_indices, num_vertices, source)

    # Extract result and wrap to Python
    cdef shared_ptr[CInt64Array] result_array = GetResultValue(result)
    return pyarrow_wrap_array(<shared_ptr[CArray]>result_array)
