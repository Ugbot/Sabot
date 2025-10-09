# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
"""
Strongly Connected Components

Implements Tarjan's algorithm for finding strongly connected components (SCCs)
in directed graphs. An SCC is a maximal set of vertices where every vertex
is reachable from every other vertex.
"""

from libc.stdint cimport int64_t
from libcpp.memory cimport shared_ptr, static_pointer_cast
from libcpp cimport bool as cbool

cimport cython
cimport pyarrow.lib as ca
from pyarrow.lib cimport pyarrow_unwrap_array, pyarrow_wrap_array, pyarrow_wrap_batch, GetResultValue
from pyarrow.includes.libarrow cimport CArray, CInt64Array, CRecordBatch

# Import Arrow Python API
import pyarrow as pa

from .strongly_connected_components cimport (
    CStronglyConnectedComponentsResult,
    FindStronglyConnectedComponents as CFindStronglyConnectedComponents,
    GetSCCMembership as CGetSCCMembership,
    GetLargestSCC as CGetLargestSCC,
    IsStronglyConnected as CIsStronglyConnected,
    FindSourceSCCs as CFindSourceSCCs,
    FindSinkSCCs as CFindSinkSCCs
)


cdef class PyStronglyConnectedComponentsResult:
    """
    Result of strongly connected components computation.

    Attributes:
        component_ids: Component ID per vertex (0-based)
        num_components: Total number of SCCs
        component_sizes: Size of each SCC
        largest_component_size: Size of largest SCC
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
    cdef PyStronglyConnectedComponentsResult from_cpp(CStronglyConnectedComponentsResult result):
        cdef PyStronglyConnectedComponentsResult py_result = PyStronglyConnectedComponentsResult()
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
        """Get total number of SCCs."""
        return self._num_components

    def component_sizes(self):
        """Get SCC sizes as PyArrow array."""
        return self._component_sizes

    def largest_component_size(self):
        """Get size of largest SCC."""
        return self._largest_component_size

    def num_vertices(self):
        """Get number of vertices."""
        return self._num_vertices

    def __repr__(self):
        return (
            f"StronglyConnectedComponentsResult(num_components={self._num_components}, "
            f"largest_component_size={self._largest_component_size}, "
            f"num_vertices={self._num_vertices})"
        )


def strongly_connected_components(
    ca.Int64Array indptr,
    ca.Int64Array indices,
    int64_t num_vertices
) -> PyStronglyConnectedComponentsResult:
    """
    Find strongly connected components using Tarjan's algorithm.

    A strongly connected component (SCC) is a maximal set of vertices where
    every vertex is reachable from every other vertex in the set.

    Tarjan's algorithm uses DFS with discovery time and low-link values.
    Time complexity: O(V + E)
    Space complexity: O(V)

    Args:
        indptr: CSR indptr array (length num_vertices + 1)
        indices: CSR indices array (neighbor lists)
        num_vertices: Number of vertices in graph

    Returns:
        PyStronglyConnectedComponentsResult with SCC assignments

    Example:
        >>> # Directed graph: 0 -> 1 -> 2 -> 0 (cycle), 3 -> 4
        >>> indptr = pa.array([0, 1, 2, 3, 4, 5], type=pa.int64())
        >>> indices = pa.array([1, 2, 0, 4, 4], type=pa.int64())
        >>> result = strongly_connected_components(indptr, indices, 5)
        >>> result.num_components()
        2
        >>> result.component_ids().to_pylist()
        [0, 0, 0, 1, 2]  # {0,1,2} is one SCC, {3} and {4} are trivial SCCs
    """
    # Unwrap Arrow arrays to C++
    cdef shared_ptr[CArray] arr_base
    cdef shared_ptr[CInt64Array] c_indptr
    cdef shared_ptr[CInt64Array] c_indices

    arr_base = pyarrow_unwrap_array(indptr)
    c_indptr = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    arr_base = pyarrow_unwrap_array(indices)
    c_indices = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    # Call C++ FindStronglyConnectedComponents
    cdef ca.CResult[CStronglyConnectedComponentsResult] result = CFindStronglyConnectedComponents(
        c_indptr, c_indices, num_vertices)

    # Extract result value (raises exception if failed)
    cdef CStronglyConnectedComponentsResult c_result = GetResultValue(result)

    return PyStronglyConnectedComponentsResult.from_cpp(c_result)


def scc_membership(
    ca.Int64Array indptr,
    ca.Int64Array indices,
    int64_t num_vertices
) -> pa.RecordBatch:
    """
    Get SCC membership as a RecordBatch.

    Args:
        indptr: CSR indptr array
        indices: CSR indices array
        num_vertices: Number of vertices

    Returns:
        RecordBatch with columns: component_id, vertex_id

    Example:
        >>> result = scc_membership(indptr, indices, 5)
        >>> print(result.to_pandas())
           component_id  vertex_id
        0             0          0
        1             0          1
        2             0          2
        3             1          3
        4             2          4
    """
    # Unwrap arrays
    cdef shared_ptr[CArray] arr_base
    cdef shared_ptr[CInt64Array] c_indptr
    cdef shared_ptr[CInt64Array] c_indices

    arr_base = pyarrow_unwrap_array(indptr)
    c_indptr = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    arr_base = pyarrow_unwrap_array(indices)
    c_indices = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    # Call C++ GetSCCMembership
    cdef ca.CResult[shared_ptr[CRecordBatch]] result = CGetSCCMembership(
        c_indptr, c_indices, num_vertices)

    # Extract result and wrap to Python
    cdef shared_ptr[CRecordBatch] result_batch = GetResultValue(result)
    return pyarrow_wrap_batch(result_batch)


def largest_scc(
    ca.Int64Array component_ids,
    ca.Int64Array component_sizes
) -> pa.Int64Array:
    """
    Get vertices in the largest strongly connected component.

    Args:
        component_ids: Component ID per vertex
        component_sizes: Size of each component

    Returns:
        Array of vertex IDs in largest SCC

    Example:
        >>> result = strongly_connected_components(indptr, indices, num_vertices)
        >>> largest = largest_scc(
        ...     result.component_ids(),
        ...     result.component_sizes()
        ... )
        >>> print(f"Largest SCC has {len(largest)} vertices")
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

    # Call C++ GetLargestSCC
    cdef ca.CResult[shared_ptr[CInt64Array]] result = CGetLargestSCC(
        c_component_ids, c_component_sizes)

    # Extract result and wrap to Python
    cdef shared_ptr[CInt64Array] c_largest = GetResultValue(result)
    return pyarrow_wrap_array(<shared_ptr[CArray]>c_largest)


def is_strongly_connected(
    ca.Int64Array indptr,
    ca.Int64Array indices,
    int64_t num_vertices
) -> bool:
    """
    Check if graph is strongly connected (single SCC).

    Args:
        indptr: CSR indptr array
        indices: CSR indices array
        num_vertices: Number of vertices

    Returns:
        True if graph is strongly connected, False otherwise

    Example:
        >>> # Cycle graph 0 -> 1 -> 2 -> 0
        >>> indptr = pa.array([0, 1, 2, 3], type=pa.int64())
        >>> indices = pa.array([1, 2, 0], type=pa.int64())
        >>> is_strongly_connected(indptr, indices, 3)
        True
    """
    # Unwrap arrays
    cdef shared_ptr[CArray] arr_base
    cdef shared_ptr[CInt64Array] c_indptr
    cdef shared_ptr[CInt64Array] c_indices

    arr_base = pyarrow_unwrap_array(indptr)
    c_indptr = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    arr_base = pyarrow_unwrap_array(indices)
    c_indices = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    # Call C++ IsStronglyConnected
    cdef ca.CResult[cbool] result = CIsStronglyConnected(
        c_indptr, c_indices, num_vertices)

    # Extract result
    cdef cbool is_connected = GetResultValue(result)
    return is_connected


def find_source_sccs(
    ca.Int64Array component_ids,
    ca.Int64Array indptr,
    ca.Int64Array indices,
    int64_t num_vertices
) -> pa.Int64Array:
    """
    Find source SCCs (no incoming edges from other SCCs).

    Args:
        component_ids: Component ID per vertex
        indptr: CSR indptr array
        indices: CSR indices array
        num_vertices: Number of vertices

    Returns:
        Array of SCC IDs that are sources

    Example:
        >>> result = strongly_connected_components(indptr, indices, num_vertices)
        >>> sources = find_source_sccs(
        ...     result.component_ids(), indptr, indices, num_vertices
        ... )
        >>> print(f"Source SCCs: {sources.to_pylist()}")
    """
    # Unwrap arrays
    cdef shared_ptr[CArray] arr_base
    cdef shared_ptr[CInt64Array] c_component_ids
    cdef shared_ptr[CInt64Array] c_indptr
    cdef shared_ptr[CInt64Array] c_indices

    arr_base = pyarrow_unwrap_array(component_ids)
    c_component_ids = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    arr_base = pyarrow_unwrap_array(indptr)
    c_indptr = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    arr_base = pyarrow_unwrap_array(indices)
    c_indices = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    # Call C++ FindSourceSCCs
    cdef ca.CResult[shared_ptr[CInt64Array]] result = CFindSourceSCCs(
        c_component_ids, c_indptr, c_indices, num_vertices)

    # Extract result and wrap to Python
    cdef shared_ptr[CInt64Array] c_sources = GetResultValue(result)
    return pyarrow_wrap_array(<shared_ptr[CArray]>c_sources)


def find_sink_sccs(
    ca.Int64Array component_ids,
    ca.Int64Array indptr,
    ca.Int64Array indices,
    int64_t num_vertices
) -> pa.Int64Array:
    """
    Find sink SCCs (no outgoing edges to other SCCs).

    Args:
        component_ids: Component ID per vertex
        indptr: CSR indptr array
        indices: CSR indices array
        num_vertices: Number of vertices

    Returns:
        Array of SCC IDs that are sinks

    Example:
        >>> result = strongly_connected_components(indptr, indices, num_vertices)
        >>> sinks = find_sink_sccs(
        ...     result.component_ids(), indptr, indices, num_vertices
        ... )
        >>> print(f"Sink SCCs: {sinks.to_pylist()}")
    """
    # Unwrap arrays
    cdef shared_ptr[CArray] arr_base
    cdef shared_ptr[CInt64Array] c_component_ids
    cdef shared_ptr[CInt64Array] c_indptr
    cdef shared_ptr[CInt64Array] c_indices

    arr_base = pyarrow_unwrap_array(component_ids)
    c_component_ids = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    arr_base = pyarrow_unwrap_array(indptr)
    c_indptr = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    arr_base = pyarrow_unwrap_array(indices)
    c_indices = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

    # Call C++ FindSinkSCCs
    cdef ca.CResult[shared_ptr[CInt64Array]] result = CFindSinkSCCs(
        c_component_ids, c_indptr, c_indices, num_vertices)

    # Extract result and wrap to Python
    cdef shared_ptr[CInt64Array] c_sinks = GetResultValue(result)
    return pyarrow_wrap_array(<shared_ptr[CArray]>c_sinks)


# Convenience aliases
tarjan = strongly_connected_components
find_sccs = strongly_connected_components
