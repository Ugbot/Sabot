"""
Graph Statistics Collector

Collects and maintains statistics about graphs and RDF triple stores
for cost-based query optimization.

Statistics collected:
- Vertex/edge counts per label/type
- Property value distributions
- Average vertex degree
- Join selectivity estimates

Used by query optimizer for cardinality estimation and cost modeling.
"""

from typing import Dict, Optional, Any, Set
from dataclasses import dataclass, field
import pyarrow as pa
from sabot.cyarrow import compute as pc


@dataclass
class LabelStatistics:
    """Statistics for a specific vertex label or edge type."""
    count: int = 0
    property_stats: Dict[str, 'PropertyStatistics'] = field(default_factory=dict)


@dataclass
class PropertyStatistics:
    """Statistics for a specific property."""
    distinct_values: int = 0
    null_count: int = 0
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None


@dataclass
class GraphStatistics:
    """
    Comprehensive graph statistics.

    Collected from property graphs or RDF triple stores.
    Used for query optimization and cost estimation.
    """
    # Basic counts
    total_vertices: int = 0
    total_edges: int = 0
    total_triples: int = 0

    # Per-label/type counts
    vertex_label_counts: Dict[str, int] = field(default_factory=dict)
    edge_type_counts: Dict[str, int] = field(default_factory=dict)
    predicate_counts: Dict[str, int] = field(default_factory=dict)  # For RDF

    # Detailed statistics
    label_stats: Dict[str, LabelStatistics] = field(default_factory=dict)

    # Graph topology
    avg_degree: float = 0.0
    max_degree: int = 0

    # Join selectivity estimates (cached)
    join_selectivity_cache: Dict[str, float] = field(default_factory=dict)

    def get_vertex_count(self, label: Optional[str] = None) -> int:
        """
        Get vertex count for a specific label or all vertices.

        Args:
            label: Vertex label (None = all vertices)

        Returns:
            Number of vertices
        """
        if label is None:
            return self.total_vertices
        return self.vertex_label_counts.get(label, 0)

    def get_edge_count(self, edge_type: Optional[str] = None) -> int:
        """
        Get edge count for a specific type or all edges.

        Args:
            edge_type: Edge type (None = all edges)

        Returns:
            Number of edges
        """
        if edge_type is None:
            return self.total_edges
        return self.edge_type_counts.get(edge_type, 0)

    def get_triple_count(self, predicate: Optional[str] = None) -> int:
        """
        Get triple count for a specific predicate or all triples.

        Args:
            predicate: RDF predicate (None = all triples)

        Returns:
            Number of triples
        """
        if predicate is None:
            return self.total_triples
        return self.predicate_counts.get(predicate, 0)

    def estimate_join_selectivity(
        self,
        left_table: str,
        right_table: str,
        join_keys: tuple
    ) -> float:
        """
        Estimate join selectivity.

        Args:
            left_table: Left table identifier
            right_table: Right table identifier
            join_keys: Tuple of (left_key, right_key)

        Returns:
            Selectivity estimate (0.0-1.0)

        Algorithm:
            Selectivity ≈ 1 / max(distinct_left, distinct_right)
            This is a simple heuristic - more sophisticated methods could use:
            - Histograms
            - Sampling
            - Join cardinality history
        """
        cache_key = f"{left_table}_{right_table}_{join_keys}"

        if cache_key in self.join_selectivity_cache:
            return self.join_selectivity_cache[cache_key]

        # For graph joins (edge.target = edge.source), use degree-based estimate
        if 'edge' in left_table.lower() and 'edge' in right_table.lower():
            # Average out-degree is total_edges / total_vertices
            if self.total_vertices > 0:
                selectivity = min(1.0, self.avg_degree / self.total_vertices)
            else:
                selectivity = 0.5
        else:
            # Default heuristic for other joins
            selectivity = 0.1  # Assume 10% selectivity

        # Cache result
        self.join_selectivity_cache[cache_key] = selectivity
        return selectivity

    def estimate_filter_selectivity(
        self,
        column: str,
        operator: str,
        value: Any
    ) -> float:
        """
        Estimate filter selectivity.

        Args:
            column: Column name
            operator: Comparison operator (=, !=, <, >, etc.)
            value: Filter value

        Returns:
            Selectivity estimate (0.0-1.0)

        Default heuristics:
            - Equality (=): 1 / distinct_values
            - Inequality (!=): 1 - (1 / distinct_values)
            - Range (<, >, <=, >=): 0.33 (1/3 of values)
        """
        # Map of operator to default selectivity
        default_selectivities = {
            '=': 0.01,   # 1% (highly selective)
            '!=': 0.99,  # 99% (not selective)
            '<': 0.33,   # 33% (moderately selective)
            '>': 0.33,
            '<=': 0.33,
            '>=': 0.33,
        }

        return default_selectivities.get(operator, 0.5)

    def __repr__(self) -> str:
        return (
            f"GraphStatistics("
            f"vertices={self.total_vertices:,}, "
            f"edges={self.total_edges:,}, "
            f"avg_degree={self.avg_degree:.2f})"
        )


class StatisticsCollector:
    """
    Collects statistics from graphs and RDF triple stores.

    Usage:
        collector = StatisticsCollector()
        stats = collector.collect_from_property_graph(vertices, edges)
        # or
        stats = collector.collect_from_triple_store(triples)
    """

    def __init__(self):
        """Initialize statistics collector."""
        pass

    def collect_from_property_graph(
        self,
        vertices: pa.Table,
        edges: pa.Table
    ) -> GraphStatistics:
        """
        Collect statistics from property graph.

        Args:
            vertices: Vertex table with 'id' and 'label' columns
            edges: Edge table with 'source', 'target', 'label' columns

        Returns:
            GraphStatistics object
        """
        stats = GraphStatistics()

        # Basic counts
        stats.total_vertices = len(vertices)
        stats.total_edges = len(edges)

        # Vertex label counts
        if 'label' in vertices.column_names:
            label_counts = pc.value_counts(vertices.column('label'))
            for i in range(len(label_counts)):
                label = label_counts.column('values')[i].as_py()
                count = label_counts.column('counts')[i].as_py()
                stats.vertex_label_counts[label] = count

        # Edge type counts
        if 'label' in edges.column_names:
            type_counts = pc.value_counts(edges.column('label'))
            for i in range(len(type_counts)):
                edge_type = type_counts.column('values')[i].as_py()
                count = type_counts.column('counts')[i].as_py()
                stats.edge_type_counts[edge_type] = count

        # Compute average degree
        if stats.total_vertices > 0:
            stats.avg_degree = stats.total_edges / stats.total_vertices
        else:
            stats.avg_degree = 0.0

        # Compute max degree (expensive - skip for large graphs)
        if stats.total_vertices < 10000:
            stats.max_degree = self._compute_max_degree(edges, stats.total_vertices)
        else:
            # Estimate max degree from average
            stats.max_degree = int(stats.avg_degree * 10)

        return stats

    def collect_from_triple_store(
        self,
        triples: pa.Table
    ) -> GraphStatistics:
        """
        Collect statistics from RDF triple store.

        Args:
            triples: Triple table with 'subject', 'predicate', 'object' columns

        Returns:
            GraphStatistics object
        """
        stats = GraphStatistics()

        # Basic counts
        stats.total_triples = len(triples)

        # Count unique subjects (vertices)
        if 'subject' in triples.column_names:
            unique_subjects = pc.count_distinct(triples.column('subject'))
            stats.total_vertices = unique_subjects.as_py()

        # Predicate counts
        if 'predicate' in triples.column_names:
            predicate_counts = pc.value_counts(triples.column('predicate'))
            for i in range(len(predicate_counts)):
                predicate = predicate_counts.column('values')[i].as_py()
                count = predicate_counts.column('counts')[i].as_py()
                stats.predicate_counts[predicate] = count

        # Estimate average degree (triples per subject)
        if stats.total_vertices > 0:
            stats.avg_degree = stats.total_triples / stats.total_vertices
        else:
            stats.avg_degree = 0.0

        return stats

    def update_statistics(
        self,
        stats: GraphStatistics,
        vertices_delta: Optional[pa.Table] = None,
        edges_delta: Optional[pa.Table] = None
    ) -> GraphStatistics:
        """
        Incrementally update statistics with new data.

        Args:
            stats: Existing statistics
            vertices_delta: New vertices to add
            edges_delta: New edges to add

        Returns:
            Updated GraphStatistics
        """
        # Update vertex counts
        if vertices_delta is not None and len(vertices_delta) > 0:
            stats.total_vertices += len(vertices_delta)

            # Update label counts
            if 'label' in vertices_delta.column_names:
                label_counts = pc.value_counts(vertices_delta.column('label'))
                for i in range(len(label_counts)):
                    label = label_counts.column('values')[i].as_py()
                    count = label_counts.column('counts')[i].as_py()
                    stats.vertex_label_counts[label] = (
                        stats.vertex_label_counts.get(label, 0) + count
                    )

        # Update edge counts
        if edges_delta is not None and len(edges_delta) > 0:
            stats.total_edges += len(edges_delta)

            # Update type counts
            if 'label' in edges_delta.column_names:
                type_counts = pc.value_counts(edges_delta.column('label'))
                for i in range(len(type_counts)):
                    edge_type = type_counts.column('values')[i].as_py()
                    count = type_counts.column('counts')[i].as_py()
                    stats.edge_type_counts[edge_type] = (
                        stats.edge_type_counts.get(edge_type, 0) + count
                    )

        # Recompute average degree
        if stats.total_vertices > 0:
            stats.avg_degree = stats.total_edges / stats.total_vertices

        # Clear join selectivity cache (statistics changed)
        stats.join_selectivity_cache.clear()

        return stats

    def _compute_max_degree(self, edges: pa.Table, num_vertices: int) -> int:
        """
        Compute maximum vertex degree.

        Args:
            edges: Edge table
            num_vertices: Number of vertices

        Returns:
            Maximum degree
        """
        if len(edges) == 0:
            return 0

        # Count out-degree for each vertex
        source_counts = pc.value_counts(edges.column('source'))

        # Get max count
        if len(source_counts) > 0:
            max_degree = pc.max(source_counts.column('counts')).as_py()
            return max_degree
        else:
            return 0


# ==============================================================================
# Statistics Cache
# ==============================================================================

class StatisticsCache:
    """
    Cache for graph statistics to avoid recomputation.

    Thread-safe cache with TTL support (future extension).
    """

    def __init__(self):
        """Initialize statistics cache."""
        self._cache: Dict[str, GraphStatistics] = {}

    def get(self, key: str) -> Optional[GraphStatistics]:
        """Get statistics from cache."""
        return self._cache.get(key)

    def put(self, key: str, stats: GraphStatistics) -> None:
        """Put statistics into cache."""
        self._cache[key] = stats

    def clear(self) -> None:
        """Clear all cached statistics."""
        self._cache.clear()

    def invalidate(self, key: str) -> None:
        """Invalidate specific cached statistics."""
        if key in self._cache:
            del self._cache[key]
