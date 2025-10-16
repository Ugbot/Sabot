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
    """
    
    def __init__(self, query, graph_store=None):
        """
        Create Cypher operator.
        
        Args:
            query: Cypher query string
            graph_store: SabotGraphBridge instance (optional)
        """
        self.query = query
        self.graph_store = graph_store
        
        if not graph_store:
            # Create default graph store
            from sabot_graph import create_sabot_graph_bridge
            self.graph_store = create_sabot_graph_bridge()
    
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
    """
    
    def __init__(self, query, graph_store=None):
        """
        Create SPARQL operator.
        
        Args:
            query: SPARQL query string
            graph_store: SabotGraphBridge instance (optional)
        """
        self.query = query
        self.graph_store = graph_store
        
        if not graph_store:
            # Create default graph store
            from sabot_graph import create_sabot_graph_bridge
            self.graph_store = create_sabot_graph_bridge()
    
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
        if batch is None or batch.num_rows == 0:
            return batch
        
        # Extract vertex IDs
        vertex_ids = batch.column(self.vertex_column)
        
        # Batch graph lookups
        # TODO: Optimize with batch vertex lookup
        
        print(f"Enriching {batch.num_rows} rows with graph data")
        
        return batch  # For now, pass through

