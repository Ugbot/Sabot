"""
Graph Traversal Algorithms

High-performance graph traversal algorithms implemented in C++/Cython.

Available algorithms:
- BFS (Breadth-First Search) ✅
- DFS (Depth-First Search) ✅
- Dijkstra (Shortest Paths) ✅
- PageRank ✅
- Betweenness Centrality ✅
- Triangle Counting ✅
- Connected Components ✅
"""

# BFS algorithms
try:
    from .bfs import bfs, k_hop_neighbors, multi_bfs, component_bfs, PyBFSResult
    _bfs_exports = ['bfs', 'k_hop_neighbors', 'multi_bfs', 'component_bfs', 'PyBFSResult']
except ImportError:
    _bfs_exports = []

# DFS algorithms
try:
    from .dfs import dfs, full_dfs, topological_sort, has_cycle, find_back_edges, PyDFSResult
    _dfs_exports = ['dfs', 'full_dfs', 'topological_sort', 'has_cycle', 'find_back_edges', 'PyDFSResult']
except ImportError:
    _dfs_exports = []

# Shortest paths algorithms
try:
    from .shortest_paths import (
        dijkstra, unweighted_shortest_paths, reconstruct_path,
        floyd_warshall, astar, PyShortestPathsResult
    )
    _sp_exports = [
        'dijkstra', 'unweighted_shortest_paths', 'reconstruct_path',
        'floyd_warshall', 'astar', 'PyShortestPathsResult'
    ]
except ImportError:
    _sp_exports = []

# PageRank algorithms
try:
    from .pagerank import (
        pagerank, personalized_pagerank, weighted_pagerank,
        top_k_pagerank, PyPageRankResult
    )
    _pr_exports = [
        'pagerank', 'personalized_pagerank', 'weighted_pagerank',
        'top_k_pagerank', 'PyPageRankResult'
    ]
except ImportError:
    _pr_exports = []

# Centrality algorithms
try:
    from .centrality import (
        betweenness_centrality, weighted_betweenness_centrality,
        edge_betweenness_centrality, top_k_betweenness,
        PyBetweennessCentralityResult
    )
    _centrality_exports = [
        'betweenness_centrality', 'weighted_betweenness_centrality',
        'edge_betweenness_centrality', 'top_k_betweenness',
        'PyBetweennessCentralityResult'
    ]
except ImportError:
    _centrality_exports = []

# Triangle counting algorithms
try:
    from .triangle_counting import (
        count_triangles, count_triangles_weighted, top_k_triangle_counts,
        compute_transitivity, PyTriangleCountResult
    )
    _triangle_exports = [
        'count_triangles', 'count_triangles_weighted', 'top_k_triangle_counts',
        'compute_transitivity', 'PyTriangleCountResult'
    ]
except ImportError:
    _triangle_exports = []

# Connected components algorithms
try:
    from .connected_components import (
        connected_components, connected_components_bfs, largest_component,
        component_statistics, count_isolated_vertices, weakly_connected_components,
        PyConnectedComponentsResult
    )
    _components_exports = [
        'connected_components', 'connected_components_bfs', 'largest_component',
        'component_statistics', 'count_isolated_vertices', 'weakly_connected_components',
        'PyConnectedComponentsResult'
    ]
except ImportError:
    _components_exports = []

# Strongly connected components algorithms
try:
    from .strongly_connected_components import (
        strongly_connected_components, scc_membership, largest_scc,
        is_strongly_connected, find_source_sccs, find_sink_sccs,
        PyStronglyConnectedComponentsResult
    )
    _scc_exports = [
        'strongly_connected_components', 'scc_membership', 'largest_scc',
        'is_strongly_connected', 'find_source_sccs', 'find_sink_sccs',
        'PyStronglyConnectedComponentsResult'
    ]
except ImportError:
    _scc_exports = []

__all__ = _bfs_exports + _dfs_exports + _sp_exports + _pr_exports + _centrality_exports + _triangle_exports + _components_exports + _scc_exports
