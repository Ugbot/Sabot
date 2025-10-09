# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
"""
Connected Components

Implements connected component finding using union-find and BFS algorithms.
"""

from libc.stdint cimport int64_t
from libcpp.memory cimport shared_ptr, static_pointer_cast

cimport cython
cimport pyarrow.lib as ca
from pyarrow.lib cimport pyarrow_unwrap_array, pyarrow_wrap_array, pyarrow_wrap_batch, GetResultValue
from pyarrow.includes.libarrow cimport CArray, CInt64Array, CRecordBatch

# Import Arrow Python API
import pyarrow as pa

from .connected_components cimport (
    CConnectedComponentsResult,
    FindConnectedComponents as CFindConnectedComponents,
    FindConnectedComponentsBFS as CFindConnectedComponentsBFS,
    GetLargestComponent as CGetLargestComponent,
    ComponentStatistics as CComponentStatistics,
    CountIsolatedVertices as CCountIsolatedVertices,
    WeaklyConnectedComponents as CWeaklyConnectedComponents
)


cdef class PyConnectedComponentsResult:
    """
    Result of connected components computation.

    Attributes:
        component_ids: Component ID per vertex (0-based)
        num_components: Total number of components
        component_sizes: Size of each component
        largest_component_size: Size of largest component
        num_vertices: Number of vertices in graph
    """
    cdef ca.Int64Array _component_ids
    cdef int64_t _num_components
    cdef ca.Int64Array _component_sizes
    cdef int64_t _largest_component_size
    cdef int64_t _num_vertices

    def __cinit__(self):
        self._num_components = 0
        self._largest_component_size = 0
        self._num_vertices = 0

    @staticmethod
    cdef PyConnectedComponentsResult from_cpp(CConnectedComponentsResult result):
        cdef PyConnectedComponentsResult py_result = PyConnectedComponentsResult()
        py_result._component_ids = pyarrow_wrap_array(<shared_ptr[CArray]>result.component_ids)
        py_result._num_components = result.num_components
        py_result._component_sizes = pyarrow_wrap_array(<shared_ptr[CArray]>result.component_sizes)
        py_result._largest_component_size = result.largest_component_size
        py_result._num_vertices = result.num_vertices
        return py_result

    def component_ids(self):
        """Get component IDs per vertex as PyArrow array."""
        return self._component_ids

    def num_components(self):
        """Get total number of components."""
        return self._num_components

    def component_sizes(self):
        """Get component sizes as PyArrow array."""
        return self._component_sizes

    def largest_component_size(self):
        """Get size of largest component."""
        return self._largest_component_size

    def num_vertices(self):
        """Get number of vertices."""
        return self._num_vertices

    def __repr__(self):
        return (
            f"ConnectedComponentsResult(num_components={self._num_components}, "
            f"largest_component_size={self._largest_component_size}, "
            f"num_vertices={self._num_vertices})"
        )


def connected_components(
    ca.Int64Array indptr,
    ca.Int64Array indices,
    int64_t num_vertices
) -> PyConnectedComponentsResult:
    """
    Find connected components using union-find algorithm.

    Uses path compression and union by rank for O(α(n)) amortized time,
    where α is the inverse Ackermann function (effectively constant).

    Args:
        indptr: CSR indptr array (length num_vertices + 1)
        indices: CSR indices array (neighbor lists)
        num_vertices: Number of vertices in graph

    Returns:
        PyConnectedComponentsResult with component assignments

    Example:
        >>> # Graph with 2 components: {0,1,2} and {3,4}
        >>> indptr = pa.array([0, 2, 4, 5, 6, 7], type=pa.int64())
        >>> indices = pa.array([1, 2, 0, 2, 0, 4, 3], type=pa.int64())
        >>> result = connected_components(indptr, indices, 5)
        >>> result.num_components()
        2
        >>> result.component_ids().to_pylist()
        [0, 0, 0, 1, 1]  # Vertices 0,1,2 in component 0; 3,4 in component 1
    """
    # Unwrap Arrow arrays to C++
    cdef shared_ptr[CArray] arr_base
    cdef shared_ptr[CInt64Array] c_indptr
    cdef shared_ptr[CInt64Array] c_indices

    arr_base = pyarrow_unwrap_array(indptr)
    c_indptr = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    arr_base = pyarrow_unwrap_array(indices)
    c_indices = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    # Call C++ FindConnectedComponents
    cdef ca.CResult[CConnectedComponentsResult] result = CFindConnectedComponents(
        c_indptr, c_indices, num_vertices)

    # Extract result value (raises exception if failed)
    cdef CConnectedComponentsResult c_result = GetResultValue(result)

    return PyConnectedComponentsResult.from_cpp(c_result)


def connected_components_bfs(
    ca.Int64Array indptr,
    ca.Int64Array indices,
    int64_t num_vertices
) -> PyConnectedComponentsResult:
    """
    Find connected components using BFS algorithm.

    Alternative to union-find, useful for directed graphs treated as undirected.

    Args:
        indptr: CSR indptr array
        indices: CSR indices array
        num_vertices: Number of vertices

    Returns:
        PyConnectedComponentsResult
    """
    # Unwrap arrays
    cdef shared_ptr[CArray] arr_base
    cdef shared_ptr[CInt64Array] c_indptr
    cdef shared_ptr[CInt64Array] c_indices

    arr_base = pyarrow_unwrap_array(indptr)
    c_indptr = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    arr_base = pyarrow_unwrap_array(indices)
    c_indices = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    # Call C++ FindConnectedComponentsBFS
    cdef ca.CResult[CConnectedComponentsResult] result = CFindConnectedComponentsBFS(
        c_indptr, c_indices, num_vertices)

    # Extract result
    cdef CConnectedComponentsResult c_result = GetResultValue(result)

    return PyConnectedComponentsResult.from_cpp(c_result)


def largest_component(
    ca.Int64Array component_ids,
    ca.Int64Array component_sizes
) -> pa.Int64Array:
    """
    Get vertices in the largest connected component.

    Args:
        component_ids: Component ID per vertex
        component_sizes: Size of each component

    Returns:
        Array of vertex IDs in largest component

    Example:
        >>> result = connected_components(indptr, indices, num_vertices)
        >>> largest = largest_component(
        ...     result.component_ids(),
        ...     result.component_sizes()
        ... )
        >>> print(f"Largest component has {len(largest)} vertices")
        >>> print(f"Vertices: {largest.to_pylist()}")
    """
    # Unwrap arrays
    cdef shared_ptr[CArray] arr_base
    cdef shared_ptr[CInt64Array] c_component_ids
    cdef shared_ptr[CInt64Array] c_component_sizes

    arr_base = pyarrow_unwrap_array(component_ids)
    c_component_ids = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    arr_base = pyarrow_unwrap_array(component_sizes)
    c_component_sizes = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    # Call C++ GetLargestComponent
    cdef ca.CResult[shared_ptr[CInt64Array]] result = CGetLargestComponent(
        c_component_ids, c_component_sizes)

    # Extract result and wrap to Python
    cdef shared_ptr[CInt64Array] c_largest = GetResultValue(result)
    return pyarrow_wrap_array(<shared_ptr[CArray]>c_largest)


def component_statistics(
    ca.Int64Array component_ids,
    int64_t num_components
) -> pa.RecordBatch:
    """
    Get statistics about connected components.

    Args:
        component_ids: Component ID per vertex
        num_components: Number of components

    Returns:
        RecordBatch with columns:
        - component_id: Component ID (0-based)
        - size: Number of vertices in component
        - isolated: Boolean indicating if component is isolated (size 1)

    Example:
        >>> result = connected_components(indptr, indices, num_vertices)
        >>> stats = component_statistics(
        ...     result.component_ids(),
        ...     result.num_components()
        ... )
        >>> print(stats.to_pandas())
           component_id  size  isolated
        0             0     3     False
        1             1     2     False
        2             2     1      True
    """
    # Unwrap array
    cdef shared_ptr[CArray] arr_base
    cdef shared_ptr[CInt64Array] c_component_ids

    arr_base = pyarrow_unwrap_array(component_ids)
    c_component_ids = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    # Call C++ ComponentStatistics
    cdef ca.CResult[shared_ptr[CRecordBatch]] result = CComponentStatistics(
        c_component_ids, num_components)

    # Extract result and wrap to Python
    cdef shared_ptr[CRecordBatch] result_batch = GetResultValue(result)
    return pyarrow_wrap_batch(result_batch)


def count_isolated_vertices(
    ca.Int64Array component_sizes
) -> int:
    """
    Count number of isolated vertices (components of size 1).

    Args:
        component_sizes: Size of each component

    Returns:
        Number of isolated vertices

    Example:
        >>> result = connected_components(indptr, indices, num_vertices)
        >>> isolated = count_isolated_vertices(result.component_sizes())
        >>> print(f"Found {isolated} isolated vertices")
    """
    # Unwrap array
    cdef shared_ptr[CArray] arr_base
    cdef shared_ptr[CInt64Array] c_component_sizes

    arr_base = pyarrow_unwrap_array(component_sizes)
    c_component_sizes = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    # Call C++ CountIsolatedVertices
    cdef ca.CResult[int64_t] result = CCountIsolatedVertices(c_component_sizes)

    # Extract result
    cdef int64_t isolated_count = GetResultValue(result)
    return isolated_count


def weakly_connected_components(
    ca.Int64Array indptr,
    ca.Int64Array indices,
    int64_t num_vertices
) -> PyConnectedComponentsResult:
    """
    Find weakly connected components in a directed graph.

    Weakly connected components ignore edge direction - treat the graph
    as undirected. This is done by symmetrizing the edge list.

    Args:
        indptr: CSR indptr array (directed edges)
        indices: CSR indices array
        num_vertices: Number of vertices

    Returns:
        PyConnectedComponentsResult with component assignments

    Example:
        >>> # Directed graph: 0 -> 1 -> 2, 3 -> 4
        >>> # Weakly connected: {0,1,2} and {3,4}
        >>> indptr = pa.array([0, 1, 2, 3, 4, 5], type=pa.int64())
        >>> indices = pa.array([1, 2, 2, 4, 4], type=pa.int64())
        >>> result = weakly_connected_components(indptr, indices, 5)
        >>> result.num_components()
        2
        >>> result.component_ids().to_pylist()
        [0, 0, 0, 1, 1]  # Vertices 0,1,2 in component 0; 3,4 in component 1
    """
    # Unwrap Arrow arrays to C++
    cdef shared_ptr[CArray] arr_base
    cdef shared_ptr[CInt64Array] c_indptr
    cdef shared_ptr[CInt64Array] c_indices

    arr_base = pyarrow_unwrap_array(indptr)
    c_indptr = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    arr_base = pyarrow_unwrap_array(indices)
    c_indices = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    # Call C++ WeaklyConnectedComponents
    cdef ca.CResult[CConnectedComponentsResult] result = CWeaklyConnectedComponents(
        c_indptr, c_indices, num_vertices)

    # Extract result value (raises exception if failed)
    cdef CConnectedComponentsResult c_result = GetResultValue(result)

    return PyConnectedComponentsResult.from_cpp(c_result)


# Convenience aliases
find_components = connected_components
components = connected_components
weakly_components = weakly_connected_components
