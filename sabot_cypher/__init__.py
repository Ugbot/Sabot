"""
SabotCypher: High-Performance Cypher Query Engine

Hard fork of Kuzu's parser/binder/optimizer with Sabot's Arrow-based execution.

Examples
--------
>>> import sabot_cypher
>>> import pyarrow as pa
>>>
>>> # Create bridge
>>> bridge = sabot_cypher.SabotCypherBridge.create()
>>>
>>> # Register graph data
>>> vertices = pa.table({
...     'id': [1, 2, 3],
...     'label': ['Person', 'Person', 'City'],
...     'name': ['Alice', 'Bob', 'NYC']
... })
>>> edges = pa.table({
...     'source': [1, 2],
...     'target': [2, 3],
...     'type': ['KNOWS', 'LIVES_IN']
... })
>>> bridge.register_graph(vertices, edges)
>>>
>>> # Execute Cypher query
>>> result = bridge.execute("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name")
>>> print(result.table)
>>>
>>> # Or use convenience function
>>> result = sabot_cypher.execute(
...     "MATCH (a)-[r]->(b) RETURN count(*)",
...     vertices,
...     edges
... )
"""

__version__ = "0.1.0"

# Try to import C++ extension
try:
    from .sabot_cypher_native import (
        SabotCypherBridge,
        CypherResult,
        execute,
    )
    _native_available = True
except ImportError as e:
    _native_available = False
    _import_error = str(e)
    
    # Placeholder until pybind11 bindings are built
    class SabotCypherBridge:
        """
        Placeholder for SabotCypherBridge.
        
        The C++ native module is not yet built. To build:
        
        1. cd sabot_cypher
        2. cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
        3. cmake --build build
        4. Install Python module
        """
        
        @staticmethod
        def create():
            raise NotImplementedError(
                f"SabotCypherBridge not yet built.\n"
                f"Import error: {_import_error}\n"
                f"Please build the C++ extension first."
            )
        
        def execute(self, query, params=None):
            raise NotImplementedError("execute() requires native extension")
        
        def explain(self, query):
            raise NotImplementedError("explain() requires native extension")
        
        def register_graph(self, vertices, edges):
            raise NotImplementedError("register_graph() requires native extension")
    
    class CypherResult:
        """Placeholder for CypherResult"""
        pass
    
    def execute(query, vertices, edges, params=None):
        """Placeholder for execute()"""
        raise NotImplementedError("execute() requires native extension")


__all__ = [
    "__version__",
    "SabotCypherBridge",
    "CypherResult",
    "execute",
]


def is_native_available():
    """Check if C++ native extension is available."""
    return _native_available


def get_import_error():
    """Get the import error message if native extension failed to load."""
    return _import_error if not _native_available else None

