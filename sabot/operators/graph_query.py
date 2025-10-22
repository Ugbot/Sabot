"""
Graph Query Operators for Sabot Streams

Cypher and SPARQL query operators as native Sabot stream operators.
Enables graph queries in normal Sabot flows alongside filter, map, join, etc.
"""

from sabot import cyarrow as ca


class CypherOperator:
    """
    Cypher query operator for Sabot streams.

    Usage in Sabot flow:
        stream.cypher("MATCH (a)-[:KNOWS]->(b) RETURN b.id", graph_store)

    NOTE: Not yet implemented for streaming. Use GraphQueryEngine directly:
        from sabot._cython.graph.engine.query_engine import GraphQueryEngine
        engine = GraphQueryEngine()
        result = engine.query_cypher(query)
    """

    def __init__(self, query, graph_store=None):
        """
        Create Cypher operator.

        Args:
            query: Cypher query string
            graph_store: GraphQueryEngine instance (optional)
        """
        self.query = query
        self.graph_store = graph_store

        if not graph_store:
            # Graph streaming operators not yet implemented
            # Use GraphQueryEngine directly
            raise NotImplementedError(
                "Graph streaming operators not yet implemented.\n"
                "Use GraphQueryEngine directly:\n"
                "  from sabot._cython.graph.engine.query_engine import GraphQueryEngine\n"
                "  engine = GraphQueryEngine()\n"
                "  result = engine.query_cypher(query)"
            )
    
    def __iter__(self):
        """Sync iteration (batch mode)."""
        for batch in self.source:
            yield self._process_batch(batch)
    
    async def __aiter__(self):
        """Async iteration (streaming mode)."""
        if hasattr(self.source, '__aiter__'):
            async for batch in self.source:
                yield self._process_batch(batch)
        else:
            for batch in self.source:
                yield self._process_batch(batch)
    
    def _process_batch(self, batch):
        """Execute Cypher query on batch."""
        if batch is None or batch.num_rows == 0:
            return batch
        
        # Execute Cypher query
        result = self.graph_store.execute_cypher_on_batch(self.query, batch)
        
        return result


class SPARQLOperator:
    """
    SPARQL query operator for Sabot streams.

    Usage in Sabot flow:
        stream.sparql("SELECT ?s ?o WHERE { ?s <knows> ?o }", graph_store)

    NOTE: Not yet implemented for streaming. SPARQL support is partial.
    """

    def __init__(self, query, graph_store=None):
        """
        Create SPARQL operator.

        Args:
            query: SPARQL query string
            graph_store: GraphQueryEngine instance (optional)
        """
        self.query = query
        self.graph_store = graph_store

        if not graph_store:
            # Graph streaming operators not yet implemented
            raise NotImplementedError(
                "Graph streaming operators not yet implemented.\n"
                "SPARQL support is partial. Use Cypher instead:\n"
                "  from sabot._cython.graph.engine.query_engine import GraphQueryEngine\n"
                "  engine = GraphQueryEngine()\n"
                "  result = engine.query_cypher(query)"
            )
    
    def __iter__(self):
        """Sync iteration (batch mode)."""
        for batch in self.source:
            yield self._process_batch(batch)
    
    async def __aiter__(self):
        """Async iteration (streaming mode)."""
        if hasattr(self.source, '__aiter__'):
            async for batch in self.source:
                yield self._process_batch(batch)
        else:
            for batch in self.source:
                yield self._process_batch(batch)
    
    def _process_batch(self, batch):
        """Execute SPARQL query on batch."""
        if batch is None or batch.num_rows == 0:
            return batch
        
        # Execute SPARQL query
        result = self.graph_store.execute_sparql_on_batch(self.query, batch)
        
        return result


class GraphEnrichOperator:
    """
    Graph enrichment operator (like SQL dimension table join).
    
    Enriches stream with graph lookups.
    
    Usage in Sabot flow:
        stream.graph_enrich('user_id', graph_store)
    """
    
    def __init__(self, vertex_column, graph_store, enrich_query=None):
        """
        Create graph enrichment operator.
        
        Args:
            vertex_column: Column containing vertex IDs
            graph_store: SabotGraphBridge instance
            enrich_query: Optional Cypher query for enrichment
        """
        self.vertex_column = vertex_column
        self.graph_store = graph_store
        self.enrich_query = enrich_query or f"MATCH (v) WHERE v.id = ${vertex_column} RETURN v.*"
    
    def __iter__(self):
        """Sync iteration (batch mode)."""
        for batch in self.source:
            yield self._enrich_batch(batch)
    
    async def __aiter__(self):
        """Async iteration (streaming mode)."""
        if hasattr(self.source, '__aiter__'):
            async for batch in self.source:
                yield self._enrich_batch(batch)
        else:
            for batch in self.source:
                yield self._enrich_batch(batch)
    
    def _enrich_batch(self, batch):
        """Enrich batch with graph lookups."""
        raise NotImplementedError(
            "_enrich_batch() not yet implemented. Requires graph lookup implementation.\n"
            "Implementation needed: Batch vertex lookup and join with stream data."
        )

