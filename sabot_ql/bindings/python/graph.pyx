# distutils: language = c++
# cython: language_level=3

"""
Python bindings for CSR graph representation.

Exposes Arrow-native graph structures for property path queries.
"""

from pyarrow.lib cimport *
from libcpp.string cimport string
from libcpp.memory cimport shared_ptr, make_shared
from libcpp.vector cimport vector
from libc.stdint cimport int64_t

# Import C++ declarations
cdef extern from "sabot_ql/graph/csr_graph.h" namespace "sabot::graph":
    cdef cppclass CSRGraph:
        shared_ptr[CInt64Array] offsets
        shared_ptr[CInt64Array] targets
        shared_ptr[CInt64Array] edge_ids
        int64_t num_nodes
        int64_t num_edges

        CResult[shared_ptr[CInt64Array]] GetSuccessors(int64_t node) except +
        CResult[shared_ptr[CInt64Array]] GetEdgeIds(int64_t node) except +
        bint empty() except +

    CResult[CSRGraph] BuildCSRGraph(
        const shared_ptr[CRecordBatch]& edges,
        const string& source_col,
        const string& target_col,
        CMemoryPool* pool
    ) except +

    CResult[CSRGraph] BuildReverseCSRGraph(
        const shared_ptr[CRecordBatch]& edges,
        const string& source_col,
        const string& target_col,
        CMemoryPool* pool
    ) except +

cdef extern from "sabot_ql/graph/transitive_closure.h" namespace "sabot::graph":
    cdef cppclass TransitiveClosureResult:
        shared_ptr[CRecordBatch] result
        int64_t num_pairs() except +

    CResult[TransitiveClosureResult] TransitiveClosure(
        const CSRGraph& graph,
        const shared_ptr[CInt64Array]& start_nodes,
        int64_t min_dist,
        int64_t max_dist,
        CMemoryPool* pool
    ) except +

    CResult[TransitiveClosureResult] TransitiveClosureSingle(
        const CSRGraph& graph,
        int64_t start_node,
        int64_t min_dist,
        int64_t max_dist,
        CMemoryPool* pool
    ) except +

cdef extern from "sabot_ql/graph/property_paths.h" namespace "sabot::graph":
    CResult[shared_ptr[CRecordBatch]] SequencePath(
        const shared_ptr[CRecordBatch]& path_p,
        const shared_ptr[CRecordBatch]& path_q,
        CMemoryPool* pool
    ) except +

    CResult[shared_ptr[CRecordBatch]] AlternativePath(
        const shared_ptr[CRecordBatch]& path_p,
        const shared_ptr[CRecordBatch]& path_q,
        CMemoryPool* pool
    ) except +

    CResult[shared_ptr[CRecordBatch]] FilterByPredicate(
        const shared_ptr[CRecordBatch]& edges,
        const vector[int64_t]& excluded_predicates,
        const string& predicate_col,
        CMemoryPool* pool
    ) except +


cdef class PyCSRGraph:
    """
    Python wrapper for CSR (Compressed Sparse Row) graph.

    Attributes:
        offsets (Int64Array): Node boundary indices [num_nodes + 1]
        targets (Int64Array): Target node IDs [num_edges]
        edge_ids (Int64Array): Original edge indices [num_edges]
        num_nodes (int): Number of nodes in graph
        num_edges (int): Number of edges in graph
    """
    cdef CSRGraph csr

    def __cinit__(self):
        pass

    @property
    def offsets(self):
        """Get offsets array."""
        return pyarrow_wrap_array(self.csr.offsets)

    @property
    def targets(self):
        """Get targets array."""
        return pyarrow_wrap_array(self.csr.targets)

    @property
    def edge_ids(self):
        """Get edge IDs array."""
        return pyarrow_wrap_array(self.csr.edge_ids)

    @property
    def num_nodes(self):
        """Get number of nodes."""
        return self.csr.num_nodes

    @property
    def num_edges(self):
        """Get number of edges."""
        return self.csr.num_edges

    def get_successors(self, int64_t node):
        """
        Get successor nodes of a given node.

        Args:
            node (int): Node ID

        Returns:
            Int64Array: Successor node IDs (empty if no edges)

        Examples:
            >>> csr = build_csr_graph(edges, 'subject', 'object')
            >>> successors = csr.get_successors(0)
            >>> print(successors.to_pylist())
            [1, 2]
        """
        cdef CResult[shared_ptr[CInt64Array]] result = self.csr.GetSuccessors(node)
        if not result.ok():
            raise RuntimeError(f"Failed to get successors: {result.status().ToString().decode()}")

        return pyarrow_wrap_array(result.ValueOrDie())

    def get_edge_ids(self, int64_t node):
        """
        Get edge IDs for a node's outgoing edges.

        Args:
            node (int): Node ID

        Returns:
            Int64Array: Original edge indices
        """
        cdef CResult[shared_ptr[CInt64Array]] result = self.csr.GetEdgeIds(node)
        if not result.ok():
            raise RuntimeError(f"Failed to get edge IDs: {result.status().ToString().decode()}")

        return pyarrow_wrap_array(result.ValueOrDie())

    def is_empty(self):
        """Check if graph is empty."""
        return self.csr.empty()


def build_csr_graph(object edges, str source_col='subject', str target_col='object'):
    """
    Build CSR graph from Arrow RecordBatch of edges.

    Args:
        edges (RecordBatch): Edge list with source and target columns
        source_col (str): Name of source node column (default: 'subject')
        target_col (str): Name of target node column (default: 'object')

    Returns:
        PyCSRGraph: CSR graph structure

    Time Complexity: O(E + V) where E is edges, V is nodes

    Examples:
        >>> import pyarrow as pa
        >>> edges = pa.RecordBatch.from_pydict({
        ...     'subject': [0, 0, 1, 2],
        ...     'object': [1, 2, 2, 3]
        ... })
        >>> csr = build_csr_graph(edges, 'subject', 'object')
        >>> print(f"Nodes: {csr.num_nodes}, Edges: {csr.num_edges}")
        Nodes: 4, Edges: 4
    """
    cdef shared_ptr[CRecordBatch] c_edges = pyarrow_unwrap_batch(edges)
    cdef string c_source_col = source_col.encode('utf-8')
    cdef string c_target_col = target_col.encode('utf-8')

    cdef CResult[CSRGraph] result = BuildCSRGraph(
        c_edges,
        c_source_col,
        c_target_col,
        c_default_memory_pool()
    )

    if not result.ok():
        raise RuntimeError(f"Failed to build CSR graph: {result.status().ToString().decode()}")

    cdef PyCSRGraph py_csr = PyCSRGraph()
    py_csr.csr = result.ValueOrDie()
    return py_csr


def build_reverse_csr_graph(object edges, str source_col='subject', str target_col='object'):
    """
    Build reverse CSR graph (swap source/target).

    Used for inverse property paths (^p).

    Args:
        edges (RecordBatch): Edge list
        source_col (str): Name of source column
        target_col (str): Name of target column

    Returns:
        PyCSRGraph: Reverse CSR graph
    """
    cdef shared_ptr[CRecordBatch] c_edges = pyarrow_unwrap_batch(edges)
    cdef string c_source_col = source_col.encode('utf-8')
    cdef string c_target_col = target_col.encode('utf-8')

    cdef CResult[CSRGraph] result = BuildReverseCSRGraph(
        c_edges,
        c_source_col,
        c_target_col,
        c_default_memory_pool()
    )

    if not result.ok():
        raise RuntimeError(f"Failed to build reverse CSR graph: {result.status().ToString().decode()}")

    cdef PyCSRGraph py_csr = PyCSRGraph()
    py_csr.csr = result.ValueOrDie()
    return py_csr


def transitive_closure(PyCSRGraph graph, object start_nodes, int64_t min_dist=1, int64_t max_dist=9223372036854775807):
    """
    Compute transitive closure using DFS.

    Implements SPARQL property path operators:
    - p+ (one or more): min_dist=1, max_dist=unbounded
    - p* (zero or more): min_dist=0, max_dist=unbounded
    - p{m,n} (bounded): min_dist=m, max_dist=n

    Args:
        graph (PyCSRGraph): CSR graph to traverse
        start_nodes (Int64Array or list): Starting node IDs
        min_dist (int): Minimum distance (default: 1 for p+)
        max_dist (int): Maximum distance (default: unbounded)

    Returns:
        RecordBatch: Result with columns (start, end, dist)

    Time Complexity: O(V + E) per start node
    Space Complexity: O(V) for visited set

    Examples:
        >>> # Graph: 0 → 1 → 2 → 3
        >>> result = transitive_closure(csr, pa.array([0]), min_dist=1)
        >>> print(result.to_pydict())
        {'start': [0, 0, 0], 'end': [1, 2, 3], 'dist': [1, 2, 3]}

        >>> # p* includes start node
        >>> result = transitive_closure(csr, pa.array([0]), min_dist=0)
        >>> # Returns: [(0, 0, 0), (0, 1, 1), (0, 2, 2), (0, 3, 3)]
    """
    # Convert start_nodes to Int64Array if needed
    import pyarrow as pa
    if not isinstance(start_nodes, pa.Int64Array):
        start_nodes = pa.array(start_nodes, type=pa.int64())

    cdef shared_ptr[CInt64Array] c_start_nodes = pyarrow_unwrap_array(start_nodes)

    cdef CResult[TransitiveClosureResult] result = TransitiveClosure(
        graph.csr,
        c_start_nodes,
        min_dist,
        max_dist,
        c_default_memory_pool()
    )

    if not result.ok():
        raise RuntimeError(f"Transitive closure failed: {result.status().ToString().decode()}")

    cdef TransitiveClosureResult tc_result = result.ValueOrDie()
    return pyarrow_wrap_batch(tc_result.result)


def sequence_path(object path_p, object path_q):
    """
    Sequence path operator (p/q): Follow path p then path q.

    Implements SPARQL path sequence via hash join on intermediate nodes.

    Args:
        path_p (RecordBatch): First path with columns (start, end)
        path_q (RecordBatch): Second path with columns (start, end)

    Returns:
        RecordBatch: Combined paths (start, end) where:
            - start is from path_p
            - end is from path_q
            - Connected via intermediate node (path_p.end = path_q.start)

    Time Complexity: O(|p| + |q|) with hash join

    Examples:
        >>> # Graph: 0 --p--> 1 --q--> 2
        >>> path_p = pa.RecordBatch.from_pydict({'start': [0], 'end': [1]})
        >>> path_q = pa.RecordBatch.from_pydict({'start': [1], 'end': [2]})
        >>> result = sequence_path(path_p, path_q)
        >>> print(result.to_pydict())
        {'start': [0], 'end': [2]}  # 0 reaches 2 via 1
    """
    cdef shared_ptr[CRecordBatch] c_path_p = pyarrow_unwrap_batch(path_p)
    cdef shared_ptr[CRecordBatch] c_path_q = pyarrow_unwrap_batch(path_q)

    cdef CResult[shared_ptr[CRecordBatch]] result = SequencePath(
        c_path_p,
        c_path_q,
        c_default_memory_pool()
    )

    check_status(result.status())
    return pyarrow_wrap_batch(result.ValueUnsafe())


def alternative_path(object path_p, object path_q):
    """
    Alternative path operator (p|q): Union of two paths.

    Implements SPARQL path alternative with deduplication.

    Args:
        path_p (RecordBatch): First path with columns (start, end)
        path_q (RecordBatch): Second path with columns (start, end)

    Returns:
        RecordBatch: Union of paths (start, end), deduplicated

    Time Complexity: O(|p| + |q|) with hash-based deduplication

    Examples:
        >>> # Graph: 0 --p--> 1, 0 --q--> 2
        >>> path_p = pa.RecordBatch.from_pydict({'start': [0], 'end': [1]})
        >>> path_q = pa.RecordBatch.from_pydict({'start': [0], 'end': [2]})
        >>> result = alternative_path(path_p, path_q)
        >>> print(result.to_pydict())
        {'start': [0, 0], 'end': [1, 2]}  # 0 reaches 1 OR 2
    """
    cdef shared_ptr[CRecordBatch] c_path_p = pyarrow_unwrap_batch(path_p)
    cdef shared_ptr[CRecordBatch] c_path_q = pyarrow_unwrap_batch(path_q)

    cdef CResult[shared_ptr[CRecordBatch]] result = AlternativePath(
        c_path_p,
        c_path_q,
        c_default_memory_pool()
    )

    check_status(result.status())
    return pyarrow_wrap_batch(result.ValueUnsafe())


def filter_by_predicate(object edges, list excluded_predicates, str predicate_col='predicate'):
    """
    Negated property set operator (!p): Filter excluding predicates.

    Implements SPARQL negated property set by filtering edges.

    Args:
        edges (RecordBatch): Edge list with predicate column
        excluded_predicates (list[int]): Predicate IDs to exclude
        predicate_col (str): Name of predicate column (default: 'predicate')

    Returns:
        RecordBatch: Filtered edges where predicate NOT IN excluded_predicates

    Time Complexity: O(E) single-pass filter

    Examples:
        >>> # Graph: 0 --p1--> 1, 0 --p2--> 2, 0 --p3--> 3
        >>> edges = pa.RecordBatch.from_pydict({
        ...     'start': [0, 0, 0],
        ...     'predicate': [10, 20, 30],
        ...     'end': [1, 2, 3]
        ... })
        >>> # Query: ?x !p2 ?y  (any predicate EXCEPT 20)
        >>> result = filter_by_predicate(edges, [20])
        >>> print(result.to_pydict())
        {'start': [0, 0], 'predicate': [10, 30], 'end': [1, 3]}
    """
    cdef shared_ptr[CRecordBatch] c_edges = pyarrow_unwrap_batch(edges)
    cdef vector[int64_t] c_excluded
    for pred in excluded_predicates:
        c_excluded.push_back(pred)

    cdef string c_predicate_col = predicate_col.encode('utf-8')

    cdef CResult[shared_ptr[CRecordBatch]] result = FilterByPredicate(
        c_edges,
        c_excluded,
        c_predicate_col,
        c_default_memory_pool()
    )

    check_status(result.status())
    return pyarrow_wrap_batch(result.ValueUnsafe())
