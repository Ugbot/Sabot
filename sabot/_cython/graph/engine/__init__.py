"""
Sabot Graph Query Engine

Provides high-level query API for SPARQL and Cypher queries on graphs stored in Sabot.
Supports both batch queries (one-off) and continuous queries (streaming results).

Architecture:
1. Query Engine - Main entry point for graph queries
2. State Manager - Integrates with Tonbo/RocksDB for graph persistence
3. Result Stream - Formats query results as Arrow tables

Example:
    from sabot._cython.graph.engine import GraphQueryEngine

    # Create engine with Tonbo backend
    engine = GraphQueryEngine(
        state_store=TonboBackend(path="./graph_state"),
        num_vertices_hint=1000000
    )

    # Load data
    vertices = pa.table({'id': [0, 1, 2], 'label': ['Person', 'Person', 'Account']})
    edges = pa.table({'source': [0, 1], 'target': [1, 2], 'label': ['KNOWS', 'OWNS']})
    engine.load_vertices(vertices)
    engine.load_edges(edges)

    # Batch query
    result = engine.query_cypher(
        "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.id, b.id"
    )
    print(result.to_pandas())

    # Continuous query
    query_id = engine.register_continuous_query(
        query="MATCH (a:Account)-[:TRANSFER {amount: > 10000}]->(b) RETURN a, b",
        callback=lambda result: print(f"Fraud alert: {result.to_pandas()}"),
        mode='incremental'
    )
"""

from .query_engine import GraphQueryEngine
from .state_manager import GraphStateManager
from .result_stream import QueryResult, ResultStream

__all__ = [
    'GraphQueryEngine',
    'GraphStateManager',
    'QueryResult',
    'ResultStream',
]
