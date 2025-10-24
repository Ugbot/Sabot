"""
Sabot Graph Module

High-level API for graph operations, integrating:
- Cypher and SPARQL query engines
- MarbleDB-backed persistent RDF triple store
- Property graph operations
- Graph algorithms and traversals

Example:
    >>> from sabot import graph
    >>>
    >>> # Create persistent triple store
    >>> store = graph.create_triple_store("./my_graph_db")
    >>>
    >>> # Or use in-memory property graph
    >>> g = graph.PropertyGraph()
    >>> g.add_vertex(0, label="Person", name="Alice")
    >>> g.add_edge(0, 1, label="KNOWS")
    >>>
    >>> # Query with Cypher
    >>> result = g.query("MATCH (a:Person)-[:KNOWS]->(b) RETURN a.name, b.name")
    >>> print(result.to_pandas())
"""

from typing import Optional, Dict, Any, List, Callable, Union
import warnings

from sabot import cyarrow as pa

# Import Cython-based components
try:
    from sabot._cython.graph import (
        PropertyGraph as _PropertyGraph,
        VertexTable,
        EdgeTable,
        CSRAdjacency,
    )
    _GRAPH_STORAGE_AVAILABLE = True
except ImportError as e:
    warnings.warn(f"Graph storage module not available: {e}")
    _GRAPH_STORAGE_AVAILABLE = False

try:
    from sabot._cython.graph.engine.query_engine import GraphQueryEngine
    from sabot._cython.graph.engine.query_engine import QueryMode, QueryConfig
    _QUERY_ENGINE_AVAILABLE = True
except ImportError as e:
    warnings.warn(f"Graph query engine not available: {e}")
    _QUERY_ENGINE_AVAILABLE = False

# Import MarbleDB-backed triple store
_MARBLEDB_STORE_AVAILABLE = False
_sabot_ql_native = None

try:
    import sys
    import os
    # Add sabot_ql/bindings/python to path - try multiple locations
    potential_paths = [
        # From sabot/ directory
        os.path.join(os.path.dirname(os.path.dirname(__file__)), "sabot_ql", "bindings", "python"),
        # From Sabot/ root
        os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "sabot_ql", "bindings", "python"),
    ]

    for bindings_path in potential_paths:
        if os.path.exists(bindings_path):
            if bindings_path not in sys.path:
                sys.path.insert(0, bindings_path)
            break

    import sabot_ql_native as _sabot_ql_native
    _MARBLEDB_STORE_AVAILABLE = True
except ImportError as e:
    warnings.warn(f"MarbleDB triple store not available: {e}")
    _MARBLEDB_STORE_AVAILABLE = False


__all__ = [
    'PropertyGraph',
    'TripleStore',
    'GraphQueryEngine',
    'QueryMode',
    'QueryConfig',
    'create_triple_store',
    'create_property_graph',
]


class PropertyGraph:
    """
    High-level property graph wrapper.

    Provides a convenient API for property graph operations with automatic
    query engine integration.

    Example:
        >>> g = PropertyGraph()
        >>> g.add_vertex(0, label="Person", name="Alice", age=30)
        >>> g.add_vertex(1, label="Person", name="Bob", age=25)
        >>> g.add_edge(0, 1, label="KNOWS", since=2020)
        >>>
        >>> # Query with Cypher
        >>> result = g.query("MATCH (a:Person)-[:KNOWS]->(b) RETURN a.name, b.name")
        >>> print(result.to_pandas())
    """

    def __init__(
        self,
        state_store: Optional[Any] = None,
        num_vertices_hint: int = 10000,
        num_edges_hint: int = 100000,
    ):
        """
        Create a new property graph.

        Args:
            state_store: Optional persistent state backend (TonboBackend/RocksDBBackend)
            num_vertices_hint: Hint for initial vertex capacity (currently unused)
            num_edges_hint: Hint for initial edge capacity (currently unused)
        """
        if not _GRAPH_STORAGE_AVAILABLE:
            raise RuntimeError(
                "Graph storage not available. Build sabot._cython.graph.storage first."
            )

        if not _QUERY_ENGINE_AVAILABLE:
            raise RuntimeError(
                "Graph query engine not available. Install required dependencies."
            )

        # Create query engine (will manage graph internally)
        self._engine = GraphQueryEngine(
            state_store=state_store,
            num_vertices_hint=num_vertices_hint,
            num_edges_hint=num_edges_hint,
        )

        self._state_store = state_store
        self._graph = None  # Will be created lazily by engine

    def add_vertex(self, vertex_id: int, label: Optional[str] = None, **properties):
        """
        Add a vertex with properties.

        Args:
            vertex_id: Unique vertex identifier
            label: Optional vertex label (e.g., "Person", "Account")
            **properties: Arbitrary vertex properties
        """
        # Prepare vertex data
        data = {'id': [vertex_id]}
        if label:
            data['label'] = [label]
        data.update({k: [v] for k, v in properties.items()})

        # Create Arrow table
        vertex_table = pa.table(data)

        # Load into graph
        self._engine.load_vertices(vertex_table)

    def add_edge(
        self,
        source: int,
        target: int,
        label: Optional[str] = None,
        **properties
    ):
        """
        Add an edge with properties.

        Args:
            source: Source vertex ID
            target: Target vertex ID
            label: Optional edge label (e.g., "KNOWS", "OWNS")
            **properties: Arbitrary edge properties
        """
        # Prepare edge data
        data = {
            'source': [source],
            'target': [target],
        }
        if label:
            data['label'] = [label]
        data.update({k: [v] for k, v in properties.items()})

        # Create Arrow table
        edge_table = pa.table(data)

        # Load into graph
        self._engine.load_edges(edge_table)

    def load_vertices(self, vertices: pa.Table):
        """
        Load vertices from Arrow table.

        Args:
            vertices: Arrow table with 'id' column and optional properties
        """
        self._engine.load_vertices(vertices)

    def load_edges(self, edges: pa.Table):
        """
        Load edges from Arrow table.

        Args:
            edges: Arrow table with 'source' and 'target' columns
        """
        self._engine.load_edges(edges)

    def query(self, query_str: str, language: str = "cypher") -> Any:
        """
        Execute a graph query.

        Args:
            query_str: Query string (Cypher or SPARQL)
            language: Query language ("cypher" or "sparql")

        Returns:
            QueryResult with Arrow table
        """
        if language.lower() == "cypher":
            return self._engine.query_cypher(query_str)
        elif language.lower() == "sparql":
            return self._engine.query_sparql(query_str)
        else:
            raise ValueError(f"Unknown query language: {language}")

    def register_continuous_query(
        self,
        query: str,
        callback: Callable[[Any], None],
        language: str = "cypher"
    ) -> str:
        """
        Register a continuous query that executes on graph updates.

        Args:
            query: Query string
            callback: Function to call with query results
            language: Query language ("cypher" or "sparql")

        Returns:
            Query ID for later management
        """
        return self._engine.register_continuous_query(
            query=query,
            callback=callback,
            language=language
        )

    def close(self):
        """Close the graph and flush state to disk."""
        if self._engine:
            self._engine.close()


class TripleStore:
    """
    High-level RDF triple store wrapper.

    Wraps MarbleDB-backed persistent triple store with convenient API.

    Example:
        >>> store = TripleStore("./my_rdf_db")
        >>> # Future: store.insert_triple("http://ex.org/Alice", ...)
        >>> store.close()
    """

    def __init__(self, db_path: str):
        """
        Open or create persistent triple store.

        Args:
            db_path: Path to MarbleDB database directory
        """
        if not _MARBLEDB_STORE_AVAILABLE or _sabot_ql_native is None:
            raise RuntimeError(
                "MarbleDB triple store not available. Build sabot_ql_native first:\n"
                "  cd sabot_ql/bindings/python\n"
                "  python setup_sabot_ql.py build_ext --inplace"
            )

        self._store = _sabot_ql_native.create_triple_store(db_path)
        self._db_path = db_path

    def insert_triple(self, subject: str, predicate: str, object_val: str):
        """
        Insert a single RDF triple.

        Note: Currently raises NotImplementedError. Use batch loading instead.

        Args:
            subject: Subject IRI
            predicate: Predicate IRI
            object_val: Object IRI or literal
        """
        return self._store.insert_triple(subject, predicate, object_val)

    def size(self) -> int:
        """
        Get approximate number of triples in store.

        Note: Currently raises NotImplementedError.
        """
        return self._store.size()

    def close(self):
        """Close the triple store and flush to disk."""
        self._store.close()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


def create_triple_store(db_path: str) -> TripleStore:
    """
    Create or open a persistent RDF triple store.

    Args:
        db_path: Path to MarbleDB database directory

    Returns:
        TripleStore instance

    Example:
        >>> store = create_triple_store("./my_graph_db")
        >>> # Use store...
        >>> store.close()
    """
    return TripleStore(db_path)


def create_property_graph(
    state_store: Optional[Any] = None,
    num_vertices_hint: int = 10000,
    num_edges_hint: int = 100000,
) -> PropertyGraph:
    """
    Create a property graph with optional persistence.

    Args:
        state_store: Optional state backend for persistence
        num_vertices_hint: Hint for initial vertex capacity
        num_edges_hint: Hint for initial edge capacity

    Returns:
        PropertyGraph instance

    Example:
        >>> from sabot._cython.state import TonboBackend
        >>>
        >>> # In-memory graph
        >>> g = create_property_graph()
        >>>
        >>> # Persistent graph
        >>> backend = TonboBackend(path="./graph_state")
        >>> g = create_property_graph(state_store=backend)
    """
    return PropertyGraph(
        state_store=state_store,
        num_vertices_hint=num_vertices_hint,
        num_edges_hint=num_edges_hint,
    )
