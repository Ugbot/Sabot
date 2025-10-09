# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
"""
Pattern Matching for Graph Queries

Implements efficient pattern matching using hash joins on Arrow tables.
"""

from libcpp.string cimport string
from libcpp.memory cimport shared_ptr
from libcpp.vector cimport vector

cimport cython
cimport pyarrow.lib as ca
from pyarrow.lib cimport pyarrow_unwrap_table, pyarrow_wrap_table, GetResultValue
from pyarrow.includes.libarrow cimport CTable

# Import Arrow Python API
import pyarrow as pa

from .pattern_match cimport (
    CPatternMatchResult,
    Match2Hop as CMatch2Hop,
    Match3Hop as CMatch3Hop,
    MatchVariableLengthPath as CMatchVariableLengthPath,
    OptimizeJoinOrder as COptimizeJoinOrder
)


cdef class PyPatternMatchResult:
    """
    Result of a pattern match query.

    Attributes:
        result_table: Arrow Table with matched patterns
        num_matches: Total number of matches
        binding_names: Names of bound vertices
    """
    cdef ca.Table _result_table
    cdef int64_t _num_matches
    cdef list _binding_names

    def __cinit__(self):
        self._num_matches = 0
        self._binding_names = []

    @staticmethod
    cdef PyPatternMatchResult from_cpp(CPatternMatchResult result):
        cdef PyPatternMatchResult py_result = PyPatternMatchResult()
        py_result._result_table = pyarrow_wrap_table(result.result_table)
        py_result._num_matches = result.num_matches

        # Convert vector<string> to Python list
        py_result._binding_names = []
        for i in range(result.binding_names.size()):
            py_result._binding_names.append(result.binding_names[i].decode('utf-8'))

        return py_result

    def result_table(self):
        """Get result table as PyArrow Table."""
        return self._result_table

    def num_matches(self):
        """Get total number of matches."""
        return self._num_matches

    def binding_names(self):
        """Get list of vertex binding names."""
        return self._binding_names

    def __repr__(self):
        return (
            f"PatternMatchResult(num_matches={self._num_matches}, "
            f"bindings={self._binding_names})"
        )


def match_2hop(
    edges1: pa.Table,
    edges2: pa.Table,
    source_name: str = "a",
    intermediate_name: str = "b",
    target_name: str = "c"
) -> PyPatternMatchResult:
    """
    Match 2-hop pattern: (A)-[R1]->(B)-[R2]->(C)

    Finds all paths A->B->C where:
    - R1 is an edge from A to B (from edges1)
    - R2 is an edge from B to C (from edges2)

    Uses hash join for O(E1 + E2) complexity.

    Args:
        edges1: First edge table (must have 'source' and 'target' columns)
        edges2: Second edge table (must have 'source' and 'target' columns)
        source_name: Name for source vertex binding (default: "a")
        intermediate_name: Name for intermediate vertex binding (default: "b")
        target_name: Name for target vertex binding (default: "c")

    Returns:
        PyPatternMatchResult with matched patterns

    Example:
        >>> # Create edge tables
        >>> edges = pa.table({
        ...     'source': [0, 1, 2, 3],
        ...     'target': [1, 2, 3, 4]
        ... })
        >>> result = match_2hop(edges, edges)
        >>> print(f"Found {result.num_matches()} 2-hop paths")
        >>> print(result.result_table())
    """
    # Unwrap Arrow tables to C++
    cdef shared_ptr[CTable] c_edges1 = pyarrow_unwrap_table(edges1)
    cdef shared_ptr[CTable] c_edges2 = pyarrow_unwrap_table(edges2)

    # Convert Python strings to C++ strings
    cdef string c_source_name = source_name.encode('utf-8')
    cdef string c_intermediate_name = intermediate_name.encode('utf-8')
    cdef string c_target_name = target_name.encode('utf-8')

    # Call C++ Match2Hop
    cdef ca.CResult[CPatternMatchResult] result = CMatch2Hop(
        c_edges1, c_edges2,
        c_source_name, c_intermediate_name, c_target_name
    )

    # Extract result value (raises exception if failed)
    cdef CPatternMatchResult c_result = GetResultValue(result)

    return PyPatternMatchResult.from_cpp(c_result)


def match_3hop(
    edges1: pa.Table,
    edges2: pa.Table,
    edges3: pa.Table
) -> PyPatternMatchResult:
    """
    Match 3-hop pattern: (A)-[R1]->(B)-[R2]->(C)-[R3]->(D)

    Finds all paths A->B->C->D.

    Implementation:
    1. Hash join edges1 and edges2 on target_id = source_id
    2. Hash join result with edges3

    Args:
        edges1: First edge table
        edges2: Second edge table
        edges3: Third edge table

    Returns:
        PyPatternMatchResult with matched 3-hop patterns

    Example:
        >>> edges = pa.table({
        ...     'source': [0, 1, 2, 3],
        ...     'target': [1, 2, 3, 4]
        ... })
        >>> result = match_3hop(edges, edges, edges)
        >>> print(f"Found {result.num_matches()} 3-hop paths")
    """
    # Unwrap Arrow tables to C++
    cdef shared_ptr[CTable] c_edges1 = pyarrow_unwrap_table(edges1)
    cdef shared_ptr[CTable] c_edges2 = pyarrow_unwrap_table(edges2)
    cdef shared_ptr[CTable] c_edges3 = pyarrow_unwrap_table(edges3)

    # Call C++ Match3Hop
    cdef ca.CResult[CPatternMatchResult] result = CMatch3Hop(
        c_edges1, c_edges2, c_edges3
    )

    # Extract result value
    cdef CPatternMatchResult c_result = GetResultValue(result)

    return PyPatternMatchResult.from_cpp(c_result)


def match_variable_length_path(
    ca.Table edges,
    int64_t source_vertex,
    int64_t target_vertex = -1,
    int64_t min_hops = 1,
    int64_t max_hops = 3
) -> PyPatternMatchResult:
    """
    Match variable-length paths: (A)-[*min..max]->(B)

    Finds all paths from source to target (or any vertex if target = -1)
    with length between min_hops and max_hops.

    Implementation:
    1. Use BFS to explore paths level by level
    2. Track full path (vertices and edges)
    3. Collect paths within the specified hop range

    Args:
        edges: Edge table (must have 'source' and 'target' columns)
        source_vertex: Source vertex ID
        target_vertex: Target vertex ID (-1 for all reachable vertices)
        min_hops: Minimum path length (inclusive, default: 1)
        max_hops: Maximum path length (inclusive, default: 3, max: 10)

    Returns:
        PyPatternMatchResult with columns:
            - path_id: Unique path identifier
            - hop_count: Number of hops in path
            - vertices: List of vertex IDs in path
            - edges: List of edge indices traversed

    Example:
        >>> # Find all paths of length 2-4 from vertex 0 to vertex 5
        >>> edges = pa.table({
        ...     'source': [0, 1, 2, 3, 4],
        ...     'target': [1, 2, 3, 4, 5]
        ... })
        >>> result = match_variable_length_path(
        ...     edges, source_vertex=0, target_vertex=5,
        ...     min_hops=2, max_hops=4
        ... )
        >>> print(f"Found {result.num_matches()} paths")
        >>> print(result.result_table())
    """
    # Unwrap Arrow table to C++
    cdef shared_ptr[CTable] c_edges = pyarrow_unwrap_table(edges)

    # Handle optional target vertex
    cdef const int64_t* c_target_vertex = NULL
    cdef int64_t target_value
    if target_vertex >= 0:
        target_value = target_vertex
        c_target_vertex = &target_value

    # Call C++ MatchVariableLengthPath
    cdef ca.CResult[CPatternMatchResult] result = CMatchVariableLengthPath(
        c_edges, source_vertex, c_target_vertex, min_hops, max_hops
    )

    # Extract result value (raises exception if failed)
    cdef CPatternMatchResult c_result = GetResultValue(result)

    return PyPatternMatchResult.from_cpp(c_result)


def OptimizeJoinOrder(edge_tables: list) -> list:
    """
    Optimize pattern join order for multi-hop queries.

    Uses cost-based greedy algorithm to reorder joins and minimize
    intermediate result sizes. Starts with smallest table, then greedily
    selects next table that produces smallest estimated join result.

    Args:
        edge_tables: List of Arrow Tables (edge tables to join)

    Returns:
        List of integers representing optimized join order (indices into edge_tables)

    Example:
        >>> # Three edge tables of different sizes
        >>> e1 = pa.table({'source': [0], 'target': [1]})
        >>> e2 = pa.table({'source': [0, 1], 'target': [1, 2]})
        >>> e3 = pa.table({'source': [0, 1, 2], 'target': [1, 2, 3]})
        >>>
        >>> # Optimizer will choose: [e1, e2, e3] (smallest first)
        >>> order = OptimizeJoinOrder([e3, e2, e1])
        >>> print(order)  # [2, 1, 0]
    """
    # Convert Python list of tables to C++ vector
    cdef vector[shared_ptr[CTable]] c_edges
    cdef shared_ptr[CTable] c_table

    for table in edge_tables:
        c_table = pyarrow_unwrap_table(table)
        c_edges.push_back(c_table)

    # Call C++ OptimizeJoinOrder
    cdef vector[int] c_order = COptimizeJoinOrder(c_edges)

    # Convert C++ vector<int> to Python list
    cdef list py_order = []
    for i in range(c_order.size()):
        py_order.append(c_order[i])

    return py_order


# Convenience aliases
two_hop = match_2hop
three_hop = match_3hop
var_length = match_variable_length_path
