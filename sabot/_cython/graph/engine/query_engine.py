"""
Graph Query Engine

Main entry point for executing SPARQL and Cypher queries on graphs stored in Sabot.
Integrates with state stores (Tonbo/RocksDB) for persistence and supports both
batch and continuous query modes.

Architecture:
- Query Engine: Coordinates query parsing, optimization, and execution
- State Manager: Manages graph persistence in Tonbo/RocksDB
- Query Compilers: Parse and translate SPARQL/Cypher to logical plans (Phase 4.2-4.3)
- Query Executor: Execute logical plans using pattern matching (Phase 4.5)
- Continuous Query Manager: Track and execute continuous queries (Phase 4.6)

Performance Goals:
- Batch queries: 1-10M rows/sec (leveraging existing 3-37M matches/sec pattern matching)
- Continuous queries: <10ms latency for incremental updates
- State persistence: <100ms for graphs with 1M vertices
"""

from sabot import cyarrow as pa
from typing import Optional, Callable, Dict, Any, List
from dataclasses import dataclass
from enum import Enum

from .state_manager import GraphStateManager
from .result_stream import QueryResult, ResultStream
from ..storage.graph_storage import PyPropertyGraph, PyVertexTable, PyEdgeTable


class QueryMode(Enum):
    """Query execution mode."""
    BATCH = "batch"           # One-off query, return all results
    CONTINUOUS = "continuous"  # Continuous query, stream results on updates
    INCREMENTAL = "incremental"  # Return only new results since last query


@dataclass
class QueryConfig:
    """Configuration for query execution."""
    mode: QueryMode = QueryMode.BATCH
    timeout_ms: Optional[int] = None
    max_results: Optional[int] = None
    optimize: bool = True
    explain: bool = False


class GraphQueryEngine:
    """
    High-level graph query engine for SPARQL and Cypher queries.

    Features:
    - Load graphs from Arrow tables
    - Execute SPARQL and Cypher queries
    - Batch and continuous query modes
    - State persistence with Tonbo/RocksDB
    - Integration with existing pattern matching (3-37M matches/sec)

    Example:
        >>> from sabot._cython.state import TonboBackend
        >>> engine = GraphQueryEngine(
        ...     state_store=TonboBackend(path="./graph_state"),
        ...     num_vertices_hint=1000000
        ... )
        >>>
        >>> # Load data
        >>> vertices = pa.table({'id': [0, 1, 2], 'label': ['Person', 'Person', 'Account']})
        >>> edges = pa.table({'source': [0, 1], 'target': [1, 2], 'label': ['KNOWS', 'OWNS']})
        >>> engine.load_vertices(vertices)
        >>> engine.load_edges(edges)
        >>>
        >>> # Batch query
        >>> result = engine.query_cypher("MATCH (a:Person)-[:KNOWS]->(b) RETURN a.id, b.id")
        >>> print(result.to_pandas())
        >>>
        >>> # Continuous query
        >>> query_id = engine.register_continuous_query(
        ...     query="MATCH (a)-[:TRANSFER {amount: > 10000}]->(b) RETURN a, b",
        ...     callback=lambda result: print(f"Fraud: {result}")
        ... )
    """

    def __init__(
        self,
        state_store: Optional[Any] = None,
        num_vertices_hint: int = 10000,
        num_edges_hint: int = 100000,
        enable_continuous: bool = True
    ):
        """
        Initialize graph query engine.

        Args:
            state_store: State backend (TonboBackend or RocksDBBackend)
                        If None, uses in-memory MemoryBackend
            num_vertices_hint: Hint for initial vertex capacity
            num_edges_hint: Hint for initial edge capacity
            enable_continuous: Enable continuous query support
        """
        self.state_store = state_store
        self.num_vertices_hint = num_vertices_hint
        self.num_edges_hint = num_edges_hint
        self.enable_continuous = enable_continuous

        # State manager handles graph persistence
        self.state_manager = GraphStateManager(
            state_store=state_store,
            num_vertices_hint=num_vertices_hint
        )

        # Property graph (in-memory representation)
        # Will be PyPropertyGraph once fully initialized with vertices and edges
        self.graph: Optional[Any] = None

        # Track loaded vertices/edges separately until graph is built
        self._vertices_table: Optional[pa.Table] = None
        self._edges_table: Optional[pa.Table] = None

        # Continuous query tracking
        self.continuous_queries: Dict[str, Dict[str, Any]] = {}
        self.next_query_id: int = 0

        # Query compiler instances (will be initialized in Phase 4.2-4.3)
        self._sparql_compiler = None
        self._cypher_compiler = None

    def load_vertices(self, vertices: pa.Table, persist: bool = True) -> None:
        """
        Load vertices into the graph.

        Args:
            vertices: Arrow table with vertex data
                     Required columns: 'id' (int64), 'label' (string)
                     Optional: Any additional property columns
            persist: Whether to persist to state store

        Example:
            >>> vertices = pa.table({
            ...     'id': [0, 1, 2],
            ...     'label': ['Person', 'Person', 'Account'],
            ...     'name': ['Alice', 'Bob', 'Checking']
            ... })
            >>> engine.load_vertices(vertices)
        """
        # Validate schema
        if 'id' not in vertices.column_names:
            raise ValueError("Vertices table must have 'id' column")
        if 'label' not in vertices.column_names:
            raise ValueError("Vertices table must have 'label' column")

        # Store vertices table
        self._vertices_table = vertices

        # Persist to state store if requested
        if persist:
            self.state_manager.save_vertices(vertices)

        # Build PropertyGraph if we have both vertices and edges
        if self._vertices_table is not None and self._edges_table is not None:
            self._build_property_graph()

    def load_edges(self, edges: pa.Table, persist: bool = True) -> None:
        """
        Load edges into the graph.

        Args:
            edges: Arrow table with edge data
                  Required columns: 'source' (int64), 'target' (int64), 'label' (string)
                  Optional: Any additional property columns
            persist: Whether to persist to state store

        Example:
            >>> edges = pa.table({
            ...     'source': [0, 1],
            ...     'target': [1, 2],
            ...     'label': ['KNOWS', 'OWNS'],
            ...     'since': [2020, 2021]
            ... })
            >>> engine.load_edges(edges)
        """
        # Validate schema
        if 'source' not in edges.column_names:
            raise ValueError("Edges table must have 'source' column")
        if 'target' not in edges.column_names:
            raise ValueError("Edges table must have 'target' column")
        if 'label' not in edges.column_names:
            raise ValueError("Edges table must have 'label' column")

        # Store edges table
        self._edges_table = edges

        # Persist to state store if requested
        if persist:
            self.state_manager.save_edges(edges)

        # Build PropertyGraph if we have both vertices and edges
        if self._vertices_table is not None and self._edges_table is not None:
            self._build_property_graph()

    def query_cypher(
        self,
        query: str,
        config: Optional[QueryConfig] = None
    ) -> QueryResult:
        """
        Execute Cypher query on the graph.

        Args:
            query: Cypher query string
            config: Query configuration (mode, timeout, etc.)

        Returns:
            QueryResult with Arrow table of results

        Example:
            >>> result = engine.query_cypher('''
            ...     MATCH (a:Person)-[:KNOWS]->(b:Person)
            ...     WHERE a.age > 18
            ...     RETURN a.name, b.name, a.age
            ... ''')
            >>> print(result.to_pandas())

        Supported Cypher features (Phase 4.3):
        - MATCH patterns: (a)-[r]->(b), (a)-[r1]->(b)-[r2]->(c)
        - Variable-length paths: -[r:TYPE*1..3]->
        - WHERE filters: Basic comparisons
        - RETURN projections with LIMIT/SKIP
        - Node labels: (a:Person)
        - Edge types: -[r:KNOWS]->

        Not yet supported:
        - Multiple MATCH clauses
        - Complex WHERE expressions
        - Aggregations (COUNT, SUM, etc.)
        - ORDER BY
        - CREATE, UPDATE, DELETE
        """
        config = config or QueryConfig()

        if config.explain:
            return self._explain_cypher(query)

        # Import compiler modules
        from ..compiler import CypherParser, CypherTranslator

        # Parse Cypher query to AST
        parser = CypherParser()
        try:
            ast = parser.parse(query)
        except Exception as e:
            raise ValueError(f"Failed to parse Cypher query: {e}\nQuery: {query}")

        # Translate AST to Sabot pattern matching
        translator = CypherTranslator(self, enable_optimization=config.optimize)
        try:
            result = translator.translate(ast)
        except Exception as e:
            raise RuntimeError(f"Failed to execute Cypher query: {e}\nQuery: {query}")

        return result

    def query_sparql(
        self,
        query: str,
        config: Optional[QueryConfig] = None
    ) -> QueryResult:
        """
        Execute SPARQL query on the graph.

        Args:
            query: SPARQL query string
            config: Query configuration (mode, timeout, etc.)

        Returns:
            QueryResult with Arrow table of results

        Example:
            >>> result = engine.query_sparql('''
            ...     SELECT ?person ?friend
            ...     WHERE {
            ...         ?person rdf:type :Person .
            ...         ?person :knows ?friend .
            ...         FILTER(?person != ?friend)
            ...     }
            ... ''')
            >>> print(result.to_pandas())

        Supported SPARQL features (Phase 4.2):
        - SELECT with projection (SELECT * or SELECT ?var1 ?var2)
        - WHERE clause with Basic Graph Patterns (BGP)
        - Triple patterns with variables
        - FILTER expressions (comparison operators)
        - LIMIT and OFFSET
        - DISTINCT modifier

        Not yet supported:
        - CONSTRUCT, ASK, DESCRIBE
        - Complex filter expressions (regex, functions)
        - OPTIONAL, UNION
        - Property paths
        - Aggregations (COUNT, SUM, etc.)
        - ORDER BY
        - Named graphs
        """
        config = config or QueryConfig()

        if config.explain:
            return self._explain_sparql(query)

        # Import compiler modules
        from ..compiler import SPARQLParser, SPARQLTranslator

        # Parse SPARQL query to AST
        parser = SPARQLParser()
        try:
            ast = parser.parse(query)
        except Exception as e:
            raise ValueError(f"Failed to parse SPARQL query: {e}\nQuery: {query}")

        # Get or build RDF triple store
        # SPARQL queries execute against RDF triple stores, not property graphs
        triple_store = self._get_rdf_triple_store()

        # Translate AST and execute against triple store
        translator = SPARQLTranslator(triple_store)
        try:
            result_table = translator.execute(ast)
        except Exception as e:
            raise RuntimeError(f"Failed to execute SPARQL query: {e}\nQuery: {query}")

        # Wrap result in QueryResult
        return QueryResult(result_table)

    def register_continuous_query(
        self,
        query: str,
        callback: Callable[[QueryResult], None],
        language: str = "cypher",
        mode: str = "incremental"
    ) -> str:
        """
        Register a continuous query that executes on graph updates.

        Args:
            query: Query string (Cypher or SPARQL)
            callback: Function to call with query results
            language: 'cypher' or 'sparql'
            mode: 'continuous' (all results) or 'incremental' (only new results)

        Returns:
            Query ID for later reference

        Example:
            >>> def fraud_alert(result):
            ...     print(f"Fraud detected: {result.to_pandas()}")
            >>>
            >>> query_id = engine.register_continuous_query(
            ...     query="MATCH (a)-[:TRANSFER {amount: > 10000}]->(b) RETURN a, b",
            ...     callback=fraud_alert,
            ...     mode='incremental'
            ... )

        Note:
            Phase 4.6 will implement continuous query support.
            For now, this is a stub that will raise NotImplementedError.
        """
        if not self.enable_continuous:
            raise ValueError("Continuous queries not enabled. Set enable_continuous=True")

        # Phase 4.6: Implement continuous query manager
        raise NotImplementedError(
            "Continuous query support not yet implemented. "
            "This will be added in Phase 4.6."
        )

    def unregister_continuous_query(self, query_id: str) -> None:
        """
        Unregister a continuous query.

        Args:
            query_id: ID returned by register_continuous_query
        """
        if query_id not in self.continuous_queries:
            raise ValueError(f"Query ID {query_id} not found")

        del self.continuous_queries[query_id]

    def _build_property_graph(self) -> None:
        """
        Build PyPropertyGraph from loaded vertices and edges tables.

        Called automatically when both vertices and edges are loaded.
        """
        if self._vertices_table is None or self._edges_table is None:
            return

        # Convert vertices table to PyVertexTable format (id, label, properties)
        # PyPropertyGraph expects schema with 'id' and 'label' columns
        vertices_table = self._vertices_table

        # Convert edges table to PyEdgeTable format (src, dst, type, properties)
        # Need to rename source->src, target->dst, label->type
        edges_table = self._edges_table.rename_columns([
            'src' if col == 'source' else 'dst' if col == 'target' else 'type' if col == 'label' else col
            for col in self._edges_table.column_names
        ])

        # Create PyVertexTable and PyEdgeTable
        vertex_table = PyVertexTable(vertices_table)
        edge_table = PyEdgeTable(edges_table)

        # Create PyPropertyGraph
        self.graph = PyPropertyGraph(vertex_table, edge_table)

    def _get_rdf_triple_store(self):
        """
        Get or create RDF triple store from property graph.

        SPARQL queries execute against RDF triple stores with (subject, predicate, object) triples.
        This method converts the property graph to RDF triples.

        Returns:
            PyRDFTripleStore instance

        Implementation:
        - Convert vertex properties to RDF triples (vertex_id, property_name, property_value)
        - Convert edges to RDF triples (source_id, edge_label, target_id)
        - Use term dictionary encoding for efficient storage
        """
        from ..storage.graph_storage import PyRDFTripleStore

        # Create or reuse triple store
        if not hasattr(self, '_rdf_triple_store'):
            self._rdf_triple_store = PyRDFTripleStore()

            # Convert property graph to RDF triples if we have data loaded
            if self._vertices_table is not None or self._edges_table is not None:
                self._populate_rdf_triple_store()

        return self._rdf_triple_store

    def _populate_rdf_triple_store(self):
        """
        Populate RDF triple store from vertices and edges tables.

        Converts:
        - Vertex: (vertex_id) → Triple: (vertex_id, rdf:type, label)
        - Vertex property: (vertex_id, property_name, property_value)
        - Edge: (source, label, target)
        """
        if not hasattr(self, '_rdf_triple_store'):
            return

        # Convert vertices to triples
        if self._vertices_table is not None:
            # Add type triples: (vertex_id, rdf:type, label)
            for batch in self._vertices_table.to_batches():
                ids = batch.column('id').to_pylist()
                labels = batch.column('label').to_pylist()

                for vid, label in zip(ids, labels):
                    # Triple: (vertex_id, rdf:type, label)
                    self._rdf_triple_store.add_triple(
                        subject=str(vid),
                        predicate='rdf:type',
                        object=label,
                        graph=None
                    )

                # Add property triples
                for col_name in batch.schema.names:
                    if col_name not in ['id', 'label']:
                        values = batch.column(col_name).to_pylist()
                        for vid, value in zip(ids, values):
                            if value is not None:
                                # Triple: (vertex_id, property_name, property_value)
                                self._rdf_triple_store.add_triple(
                                    subject=str(vid),
                                    predicate=col_name,
                                    object=str(value),
                                    graph=None
                                )

        # Convert edges to triples
        if self._edges_table is not None:
            # Add edge triples: (source, edge_label, target)
            for batch in self._edges_table.to_batches():
                sources = batch.column('source').to_pylist()
                targets = batch.column('target').to_pylist()
                labels = batch.column('label').to_pylist()

                for src, tgt, label in zip(sources, targets, labels):
                    # Triple: (source, edge_label, target)
                    self._rdf_triple_store.add_triple(
                        subject=str(src),
                        predicate=label,
                        object=str(tgt),
                        graph=None
                    )

                # Add edge property triples
                for col_name in batch.schema.names:
                    if col_name not in ['source', 'target', 'label']:
                        values = batch.column(col_name).to_pylist()
                        for src, tgt, label, value in zip(sources, targets, labels, values):
                            if value is not None:
                                # Triple: (edge_id, property_name, property_value)
                                # Use edge as subject (encoded as "source_label_target")
                                edge_id = f"{src}_{label}_{tgt}"
                                self._rdf_triple_store.add_triple(
                                    subject=edge_id,
                                    predicate=col_name,
                                    object=str(value),
                                    graph=None
                                )

    def get_graph_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the loaded graph.

        Returns:
            Dictionary with graph statistics
        """
        if self._vertices_table is None and self._edges_table is None:
            return {
                'num_vertices': 0,
                'num_edges': 0,
                'loaded': False
            }

        num_vertices = len(self._vertices_table) if self._vertices_table is not None else 0
        num_edges = len(self._edges_table) if self._edges_table is not None else 0

        # Get unique labels
        vertex_labels = []
        edge_labels = []

        if self._vertices_table is not None and 'label' in self._vertices_table.column_names:
            vertex_labels = self._vertices_table.column('label').unique().to_pylist()

        if self._edges_table is not None and 'label' in self._edges_table.column_names:
            edge_labels = self._edges_table.column('label').unique().to_pylist()

        return {
            'num_vertices': num_vertices,
            'num_edges': num_edges,
            'loaded': True,
            'vertex_labels': vertex_labels,
            'edge_labels': edge_labels,
        }

    def _explain_cypher(self, query: str) -> QueryResult:
        """
        Generate execution plan for Cypher query.

        Args:
            query: Cypher query string

        Returns:
            QueryResult with explanation as formatted text
        """
        # Import compiler modules
        from ..compiler import CypherParser, CypherTranslator

        # Parse Cypher query to AST
        parser = CypherParser()
        try:
            ast = parser.parse(query)
        except Exception as e:
            raise ValueError(f"Failed to parse Cypher query: {e}\nQuery: {query}")

        # Create translator with optimization enabled
        translator = CypherTranslator(self, enable_optimization=True)

        # Generate explanation
        try:
            explanation = translator.explain(ast)
        except Exception as e:
            raise RuntimeError(f"Failed to generate explanation: {e}\nQuery: {query}")

        # Convert explanation to Arrow table
        # Create single-column table with explanation text
        explanation_text = explanation.to_string()
        result_table = pa.table({
            'QUERY PLAN': [explanation_text]
        })

        return QueryResult(result_table)

    def _explain_sparql(self, query: str) -> QueryResult:
        """
        Generate execution plan for SPARQL query.

        Args:
            query: SPARQL query string

        Returns:
            QueryResult with explanation as formatted text
        """
        # Import compiler modules
        from ..compiler import SPARQLParser, SPARQLTranslator

        # Parse SPARQL query to AST
        parser = SPARQLParser()
        try:
            ast = parser.parse(query)
        except Exception as e:
            raise ValueError(f"Failed to parse SPARQL query: {e}\nQuery: {query}")

        # Get or build RDF triple store
        triple_store = self._get_rdf_triple_store()

        # Create translator with optimization enabled
        translator = SPARQLTranslator(triple_store, enable_optimization=True)

        # Generate explanation
        try:
            explanation = translator.explain(ast)
        except Exception as e:
            raise RuntimeError(f"Failed to generate explanation: {e}\nQuery: {query}")

        # Convert explanation to Arrow table
        explanation_text = explanation.to_string()
        result_table = pa.table({
            'QUERY PLAN': [explanation_text]
        })

        return QueryResult(result_table)

    def persist(self) -> None:
        """Persist current graph state to storage."""
        if self._vertices_table is None and self._edges_table is None:
            raise ValueError("No graph loaded")

        # Persist vertices and edges tables
        if self._vertices_table is not None:
            self.state_manager.save_vertices(self._vertices_table)
        if self._edges_table is not None:
            self.state_manager.save_edges(self._edges_table)

    def load(self) -> None:
        """Load graph state from storage."""
        # Load vertices and edges tables from state store
        self._vertices_table = self.state_manager.load_vertices()
        self._edges_table = self.state_manager.load_edges()

        # Build property graph if both are loaded
        if self._vertices_table is not None and self._edges_table is not None:
            self._build_property_graph()

    def close(self) -> None:
        """Close engine and release resources."""
        # Stop continuous queries
        self.continuous_queries.clear()

        # Close state manager
        self.state_manager.close()

        # Clear graph
        self.graph = None
