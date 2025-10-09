# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
"""
DFS Cython Bindings

Python-accessible wrappers for C++ DFS algorithms.
"""

from libc.stdint cimport int64_t
from libcpp cimport bool as cbool
from libcpp.memory cimport shared_ptr, static_pointer_cast

cimport cython
cimport pyarrow.lib as ca
from pyarrow.lib cimport pyarrow_unwrap_array, pyarrow_wrap_array, pyarrow_wrap_batch, GetResultValue
from pyarrow.includes.libarrow cimport CArray, CInt64Array, CRecordBatch

# Import Arrow Python API
import pyarrow as pa


# ============================================================================
# DFS Result Wrapper
# ============================================================================

cdef class PyDFSResult:
    """
    DFS traversal result.

    Attributes:
        vertices: Visited vertex IDs in DFS order
        discovery_time: Discovery time for each vertex (pre-order)
        finish_time: Finish time for each vertex (post-order)
        predecessors: Parent vertex in DFS tree (-1 for roots)
        num_visited: Total number of vertices visited
    """

    def __cinit__(self, ca.Int64Array vertices, ca.Int64Array discovery_time,
                  ca.Int64Array finish_time, ca.Int64Array predecessors,
                  int64_t num_visited):
        self._vertices = vertices
        self._discovery_time = discovery_time
        self._finish_time = finish_time
        self._predecessors = predecessors
        self._num_visited = num_visited

    cpdef ca.Int64Array vertices(self):
        """Get visited vertices in DFS order."""
        return self._vertices

    cpdef ca.Int64Array discovery_time(self):
        """Get discovery times (pre-order)."""
        return self._discovery_time

    cpdef ca.Int64Array finish_time(self):
        """Get finish times (post-order)."""
        return self._finish_time

    cpdef ca.Int64Array predecessors(self):
        """Get predecessor vertices in DFS tree."""
        return self._predecessors

    cpdef int64_t num_visited(self):
        """Get number of vertices visited."""
        return self._num_visited

    def __repr__(self):
        return (f"PyDFSResult(num_visited={self._num_visited}, "
                f"vertices={len(self._vertices)})")


# ============================================================================
# DFS Functions
# ============================================================================

def dfs(ca.Int64Array indptr, ca.Int64Array indices, int64_t num_vertices,
        int64_t source):
    """
    Depth-first search from a source vertex.

    Args:
        indptr: CSR indptr array (offsets for each vertex's neighbors)
        indices: CSR indices array (neighbor vertex IDs)
        num_vertices: Total number of vertices in graph
        source: Source vertex ID to start traversal

    Returns:
        PyDFSResult with vertices, discovery/finish times, and predecessors

    Performance: O(V + E) time, O(V) space (stack depth + visited array)

    Example:
        >>> csr = graph.csr()
        >>> result = dfs(csr.indptr, csr.indices, graph.num_vertices(), source=0)
        >>> print(f"DFS order: {result.vertices().to_pylist()}")
        >>> print(f"Discovery times: {result.discovery_time().to_pylist()}")
    """
    # Unwrap Arrow arrays to C++
    cdef shared_ptr[CArray] arr_base
    cdef shared_ptr[CInt64Array] c_indptr
    cdef shared_ptr[CInt64Array] c_indices

    arr_base = pyarrow_unwrap_array(indptr)
    c_indptr = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    arr_base = pyarrow_unwrap_array(indices)
    c_indices = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    # Call C++ DFS
    cdef ca.CResult[DFSResult] result = DFS(c_indptr, c_indices, num_vertices, source)

    # Extract result value (raises exception if failed)
    cdef DFSResult dfs_res = GetResultValue(result)

    # Wrap C++ arrays back to Python
    vertices_py = pyarrow_wrap_array(<shared_ptr[CArray]>dfs_res.vertices)
    discovery_py = pyarrow_wrap_array(<shared_ptr[CArray]>dfs_res.discovery_time)
    finish_py = pyarrow_wrap_array(<shared_ptr[CArray]>dfs_res.finish_time)
    predecessors_py = pyarrow_wrap_array(<shared_ptr[CArray]>dfs_res.predecessors)

    return PyDFSResult(vertices_py, discovery_py, finish_py, predecessors_py, dfs_res.num_visited)


def full_dfs(ca.Int64Array indptr, ca.Int64Array indices, int64_t num_vertices):
    """
    Full DFS traversal (all connected components).

    Performs DFS from all unvisited vertices, handling disconnected graphs.

    Args:
        indptr: CSR indptr array
        indices: CSR indices array
        num_vertices: Total number of vertices

    Returns:
        PyDFSResult covering all vertices in the graph

    Performance: O(V + E) time, O(V) space

    Example:
        >>> csr = graph.csr()
        >>> result = full_dfs(csr.indptr, csr.indices, graph.num_vertices())
        >>> print(f"Visited all {result.num_visited()} vertices")
    """
    # Unwrap arrays
    cdef shared_ptr[CArray] arr_base
    cdef shared_ptr[CInt64Array] c_indptr
    cdef shared_ptr[CInt64Array] c_indices

    arr_base = pyarrow_unwrap_array(indptr)
    c_indptr = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    arr_base = pyarrow_unwrap_array(indices)
    c_indices = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    # Call C++ FullDFS
    cdef ca.CResult[DFSResult] result = FullDFS(c_indptr, c_indices, num_vertices)

    # Extract result value (raises exception if failed)
    cdef DFSResult dfs_res = GetResultValue(result)

    # Wrap results
    vertices_py = pyarrow_wrap_array(<shared_ptr[CArray]>dfs_res.vertices)
    discovery_py = pyarrow_wrap_array(<shared_ptr[CArray]>dfs_res.discovery_time)
    finish_py = pyarrow_wrap_array(<shared_ptr[CArray]>dfs_res.finish_time)
    predecessors_py = pyarrow_wrap_array(<shared_ptr[CArray]>dfs_res.predecessors)

    return PyDFSResult(vertices_py, discovery_py, finish_py, predecessors_py, dfs_res.num_visited)


def topological_sort(ca.Int64Array indptr, ca.Int64Array indices, int64_t num_vertices):
    """
    Topological sort using DFS (for DAGs only).

    Returns vertices in topological order (reverse post-order).
    Raises error if graph contains cycles.

    Args:
        indptr: CSR indptr array
        indices: CSR indices array
        num_vertices: Total number of vertices

    Returns:
        Int64Array of vertices in topological order

    Raises:
        RuntimeError: If graph contains a cycle

    Performance: O(V + E) time, O(V) space

    Example:
        >>> # For a DAG (dependency graph)
        >>> csr = graph.csr()
        >>> topo_order = topological_sort(csr.indptr, csr.indices, graph.num_vertices())
        >>> print(f"Build order: {topo_order.to_pylist()}")
    """
    # Unwrap arrays
    cdef shared_ptr[CArray] arr_base
    cdef shared_ptr[CInt64Array] c_indptr
    cdef shared_ptr[CInt64Array] c_indices

    arr_base = pyarrow_unwrap_array(indptr)
    c_indptr = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    arr_base = pyarrow_unwrap_array(indices)
    c_indices = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    # Call C++ TopologicalSort
    cdef ca.CResult[shared_ptr[CInt64Array]] result = TopologicalSort(
        c_indptr, c_indices, num_vertices)

    # Extract result and wrap to Python
    cdef shared_ptr[CInt64Array] result_array = GetResultValue(result)
    return pyarrow_wrap_array(<shared_ptr[CArray]>result_array)


def has_cycle(ca.Int64Array indptr, ca.Int64Array indices, int64_t num_vertices):
    """
    Detect if graph contains a cycle using DFS.

    Args:
        indptr: CSR indptr array
        indices: CSR indices array
        num_vertices: Total number of vertices

    Returns:
        bool: True if graph contains at least one cycle, False otherwise

    Performance: O(V + E) time, O(V) space

    Example:
        >>> csr = graph.csr()
        >>> if has_cycle(csr.indptr, csr.indices, graph.num_vertices()):
        ...     print("Graph contains a cycle")
        ... else:
        ...     print("Graph is acyclic (DAG)")
    """
    # Unwrap arrays
    cdef shared_ptr[CArray] arr_base
    cdef shared_ptr[CInt64Array] c_indptr
    cdef shared_ptr[CInt64Array] c_indices

    arr_base = pyarrow_unwrap_array(indptr)
    c_indptr = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    arr_base = pyarrow_unwrap_array(indices)
    c_indices = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    # Call C++ HasCycle
    cdef ca.CResult[cbool] result = HasCycle(c_indptr, c_indices, num_vertices)

    # Extract result
    cdef cbool has_cycle_result = GetResultValue(result)
    return has_cycle_result


def find_back_edges(ca.Int64Array indptr, ca.Int64Array indices, int64_t num_vertices):
    """
    Find all back edges (edges creating cycles) using DFS.

    Back edges are edges (u -> v) where v is an ancestor of u in the DFS tree.

    Args:
        indptr: CSR indptr array
        indices: CSR indices array
        num_vertices: Total number of vertices

    Returns:
        RecordBatch with columns 'src' and 'dst' for back edges

    Performance: O(V + E) time, O(V) space

    Example:
        >>> csr = graph.csr()
        >>> back_edges = find_back_edges(csr.indptr, csr.indices, graph.num_vertices())
        >>> print(f"Found {len(back_edges)} back edges")
        >>> print(f"Back edges: {back_edges.to_pydict()}")
    """
    # Unwrap arrays
    cdef shared_ptr[CArray] arr_base
    cdef shared_ptr[CInt64Array] c_indptr
    cdef shared_ptr[CInt64Array] c_indices

    arr_base = pyarrow_unwrap_array(indptr)
    c_indptr = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    arr_base = pyarrow_unwrap_array(indices)
    c_indices = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    # Call C++ FindBackEdges
    cdef ca.CResult[shared_ptr[CRecordBatch]] result = FindBackEdges(
        c_indptr, c_indices, num_vertices)

    # Extract result and wrap to Python
    cdef shared_ptr[CRecordBatch] result_batch = GetResultValue(result)
    return pyarrow_wrap_batch(result_batch)
