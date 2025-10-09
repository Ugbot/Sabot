"""
Sabot Graph Processing Module

Arrow-native graph storage and query engine with C++ kernels and Cython bindings.

Modules:
- storage: Graph storage (vertices, edges, CSR/CSC adjacency)
- traversal: Graph traversal kernels (BFS, DFS, PageRank)
- operators: Graph operators (pattern matching, joins)
- query: Query compilers (Cypher, SPARQL)
"""

__all__ = []

# Storage will be imported when built
try:
    from .storage.graph_storage import (
        PyCSRAdjacency as CSRAdjacency,
        PyVertexTable as VertexTable,
        PyEdgeTable as EdgeTable,
        PyPropertyGraph as PropertyGraph,
        PyRDFTripleStore as RDFTripleStore,
    )
    __all__.extend([
        'CSRAdjacency',
        'VertexTable',
        'EdgeTable',
        'PropertyGraph',
        'RDFTripleStore',
    ])
except ImportError as e:
    import warnings
    warnings.warn(f"Graph storage module not built: {e}")

# Query pattern matching
try:
    from .query.pattern_match import (
        match_2hop,
        match_3hop,
        match_variable_length_path,
        PyPatternMatchResult,
    )
    __all__.extend([
        'match_2hop',
        'match_3hop',
        'match_variable_length_path',
        'PyPatternMatchResult',
    ])
except ImportError as e:
    import warnings
    warnings.warn(f"Graph query module not built: {e}")
