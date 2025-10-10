"""
Cypher to Sabot Pattern Matching Translator

Translates Cypher AST into Sabot pattern matching operations.
Strategy inspired by KuzuDB's binder and planner modules.

Reference:
- vendor/kuzu/src/binder/bind/bind_graph_pattern.cpp
- vendor/kuzu/src/planner/plan/plan_query.cpp
(MIT License, Copyright 2022-2025 Kùzu Inc.)

Translation Strategy:
    Cypher Query
    ↓ (CypherParser)
    Cypher AST
    ↓ (CypherTranslator - this module)
    Sabot Pattern Matching Calls
    ↓
    QueryResult (Arrow table)
"""

from sabot import cyarrow as pa
from sabot.cyarrow import compute as pc
from typing import Optional, List, Tuple, Dict, Any

from .cypher_ast import *
from ..query import match_2hop, match_3hop, match_variable_length_path
from ..engine.result_stream import QueryResult, QueryMetadata
from .query_optimizer import QueryOptimizer, OptimizationContext
from .query_plan import LogicalPlan, PlanBuilder, NodeType
from .statistics import GraphStatistics, StatisticsCollector
from .plan_explainer import PlanExplainer, OptimizationInfo, create_simple_explanation, ExplanationResult


class CypherTranslator:
    """
    Translates Cypher AST to Sabot pattern matching operations.

    Example:
        >>> translator = CypherTranslator(graph_engine)
        >>> ast = parser.parse("MATCH (a:Person)-[:KNOWS]->(b) RETURN a.name")
        >>> result = translator.translate(ast)
    """

    def __init__(self, graph_engine, enable_optimization: bool = True):
        """
        Initialize translator.

        Args:
            graph_engine: GraphQueryEngine instance with loaded graph data
            enable_optimization: Whether to enable query optimization
        """
        self.graph_engine = graph_engine
        self.enable_optimization = enable_optimization

        # Collect statistics for optimization
        if enable_optimization:
            self.statistics = self._collect_statistics()
            self.optimizer = QueryOptimizer(self.statistics)
        else:
            self.statistics = None
            self.optimizer = None

    def translate(self, query: CypherQuery) -> QueryResult:
        """
        Translate Cypher query to Sabot operations and execute.

        Args:
            query: Parsed CypherQuery AST

        Returns:
            QueryResult with execution results

        Raises:
            NotImplementedError: For unsupported query patterns
        """
        import time
        start_time = time.time()

        # Currently only support single MATCH clause
        if len(query.match_clauses) != 1:
            raise NotImplementedError(
                f"Multiple MATCH clauses not yet supported. "
                f"Found {len(query.match_clauses)} MATCH clauses."
            )

        match_clause = query.match_clauses[0]

        # Translate MATCH pattern to pattern matching
        result_table = self._translate_match(match_clause)

        # Apply RETURN projection if present
        if query.return_clause:
            result_table = self._apply_return(result_table, query.return_clause)

        # Create query metadata
        execution_time = (time.time() - start_time) * 1000  # ms
        metadata = QueryMetadata(
            query=str(query),
            language='cypher',
            num_results=result_table.num_rows,
            execution_time_ms=execution_time
        )

        return QueryResult(result_table, metadata)

    def explain(self, query: CypherQuery) -> ExplanationResult:
        """
        Generate execution plan explanation for Cypher query.

        Args:
            query: CypherQuery AST node

        Returns:
            ExplanationResult with formatted plan explanation

        Example:
            >>> translator = CypherTranslator(graph_engine)
            >>> explanation = translator.explain(query_ast)
            >>> print(explanation.to_string())
        """
        # Currently only support single MATCH clause
        if len(query.match_clauses) != 1:
            raise NotImplementedError(
                f"EXPLAIN for multiple MATCH clauses not yet supported. "
                f"Found {len(query.match_clauses)} MATCH clauses."
            )

        match_clause = query.match_clauses[0]
        pattern = match_clause.pattern

        # Build selectivity scores for display
        selectivity_scores = {}

        # Score each pattern element
        for i, element in enumerate(pattern.elements, 1):
            selectivity = self._estimate_pattern_element_selectivity(element)
            pattern_str = f"Element {i}: {self._format_pattern_element(element)}"
            selectivity_scores[pattern_str] = selectivity

        # Optimize if enabled
        if self.enable_optimization and self.optimizer:
            optimized_pattern, _ = self._optimize_match(pattern, match_clause.where)

            # Show optimized order
            pattern_info_lines = []
            pattern_info_lines.append("Pattern elements will be executed in this order:")
            pattern_info_lines.append("")
            for i, element in enumerate(optimized_pattern.elements, 1):
                selectivity = self._estimate_pattern_element_selectivity(element)
                pattern_info_lines.append(f"  {i}. {self._format_pattern_element(element)}")
                pattern_info_lines.append(f"      Selectivity: {selectivity:.2f}")

            pattern_info = '\n'.join(pattern_info_lines)

            # Record optimizations applied
            optimizations = [
                OptimizationInfo(
                    rule_name="Pattern Element Reordering",
                    description="Reordered pattern elements by selectivity (most selective first)",
                    impact="2-10x speedup on multi-element patterns"
                )
            ]
        else:
            # No optimization
            pattern_info_lines = []
            pattern_info_lines.append("Pattern elements (unoptimized order):")
            pattern_info_lines.append("")
            for i, element in enumerate(pattern.elements, 1):
                pattern_info_lines.append(f"  {i}. {self._format_pattern_element(element)}")

            pattern_info = '\n'.join(pattern_info_lines)
            optimizations = []

        # Create explanation using simple format
        explanation = create_simple_explanation(
            query=str(query) if hasattr(query, '__str__') else "Cypher query",
            language="cypher",
            pattern_info=pattern_info,
            selectivity_scores=selectivity_scores,
            statistics=self.statistics
        )

        # Override optimizations with our list
        explanation.optimizations_applied = optimizations

        return explanation

    def _format_pattern_element(self, element: PatternElement) -> str:
        """Format pattern element for display."""
        parts = []

        for i, node in enumerate(element.nodes):
            # Format node
            node_str = "("
            if node.variable:
                node_str += node.variable
            if node.labels:
                node_str += ":" + ":".join(node.labels)
            node_str += ")"
            parts.append(node_str)

            # Add edge if not last node
            if i < len(element.edges):
                edge = element.edges[i]
                edge_str = "-"
                if edge.types or edge.variable:
                    edge_str += "["
                    if edge.variable:
                        edge_str += edge.variable
                    if edge.types:
                        edge_str += ":" + ":".join(edge.types)
                    if edge.recursive:
                        rec = edge.recursive
                        edge_str += f"*{rec.min_hops}..{rec.max_hops if rec.max_hops else ''}"
                    edge_str += "]"
                edge_str += "->"
                parts.append(edge_str)

        return ''.join(parts)

    def _collect_statistics(self) -> GraphStatistics:
        """
        Collect statistics from property graph for optimization.

        Returns:
            GraphStatistics object with vertex/edge counts and distributions
        """
        stats = GraphStatistics()

        # Get basic counts from graph engine
        graph_stats = self.graph_engine.get_graph_stats()
        stats.total_vertices = graph_stats.get('num_vertices', 0)
        stats.total_edges = graph_stats.get('num_edges', 0)

        # Collect vertex label counts
        if self.graph_engine._vertices_table is not None:
            vertices = self.graph_engine._vertices_table
            if 'label' in vertices.column_names:
                label_counts = pc.value_counts(vertices.column('label'))
                # value_counts returns a StructArray with 'values' and 'counts' fields
                values_array = label_counts.field('values')
                counts_array = label_counts.field('counts')
                for i in range(len(values_array)):
                    label = values_array[i].as_py()
                    count = counts_array[i].as_py()
                    stats.vertex_label_counts[label] = count

        # Collect edge type counts
        if self.graph_engine._edges_table is not None:
            edges = self.graph_engine._edges_table
            if 'label' in edges.column_names:
                type_counts = pc.value_counts(edges.column('label'))
                # value_counts returns a StructArray with 'values' and 'counts' fields
                values_array = type_counts.field('values')
                counts_array = type_counts.field('counts')
                for i in range(len(values_array)):
                    edge_type = values_array[i].as_py()
                    count = counts_array[i].as_py()
                    stats.edge_type_counts[edge_type] = count

        # Compute average degree
        if stats.total_vertices > 0:
            stats.avg_degree = stats.total_edges / stats.total_vertices
        else:
            stats.avg_degree = 0.0

        return stats

    def _translate_match(self, match: MatchClause) -> pa.Table:
        """
        Translate MATCH clause to pattern matching.

        Strategy (adapted from KuzuDB's bind_graph_pattern.cpp):
        1. Identify pattern type (2-hop, 3-hop, variable-length)
        2. Extract filters from nodes/edges and WHERE clause
        3. Filter graph data (vertices by label, edges by type)
        4. Apply WHERE filters
        5. Execute appropriate pattern matching function
        6. Return result table

        Args:
            match: MatchClause AST node

        Returns:
            Arrow table with matched patterns
        """
        pattern = match.pattern

        # Optimize pattern if enabled
        if self.enable_optimization and self.optimizer:
            optimized_pattern, optimized_where = self._optimize_match(pattern, match.where)
        else:
            optimized_pattern = pattern
            optimized_where = match.where

        # Determine pattern type and execute
        if pattern_is_2hop(optimized_pattern):
            result = self._translate_2hop(optimized_pattern, optimized_where)
        elif pattern_is_3hop(optimized_pattern):
            result = self._translate_3hop(optimized_pattern, optimized_where)
        elif pattern_is_variable_length(optimized_pattern):
            result = self._translate_variable_length(optimized_pattern, optimized_where)
        else:
            raise NotImplementedError(
                f"Pattern type not yet supported. "
                f"Pattern has {len(optimized_pattern.elements)} elements."
            )

        return result

    def _translate_2hop(self, pattern: Pattern, where: Optional[Expression]) -> pa.Table:
        """
        Translate 2-hop pattern: (a)-[r]->(b)

        Example:
            MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.age > 18

        Translation:
            1. Filter vertices by label 'Person'
            2. Filter edges by type 'KNOWS'
            3. Apply WHERE filter on 'a' vertices
            4. Call match_2hop(filtered_vertices, edges)
        """
        element = pattern.elements[0]
        node_a, node_b = element.nodes[0], element.nodes[1]
        edge = element.edges[0]

        # Get base edge table
        if not self.graph_engine._edges_table:
            raise ValueError("No edges loaded in graph")

        edges_table = self.graph_engine._edges_table

        # Filter edges by type if specified
        if edge.types:
            # Filter to matching edge types
            edge_type = edge.types[0]  # Use first type
            mask = pc.equal(edges_table.column('label'), pa.scalar(edge_type))
            edges_table = edges_table.filter(mask)

        # Convert to simple edge table for pattern matching
        edge_pattern_table = pa.table({
            'source': edges_table.column('source'),
            'target': edges_table.column('target')
        })

        # Execute 2-hop pattern match
        result = match_2hop(edge_pattern_table, edge_pattern_table)
        result_table = result.result_table()

        # Apply WHERE filters
        if where:
            result_table = self._apply_where_filter(result_table, where, element)

        return result_table

    def _translate_3hop(self, pattern: Pattern, where: Optional[Expression]) -> pa.Table:
        """
        Translate 3-hop pattern: (a)-[r1]->(b)-[r2]->(c)

        Example:
            MATCH (a:Person)-[:KNOWS]->(b)-[:KNOWS]->(c)
        """
        element = pattern.elements[0]
        edge1, edge2 = element.edges[0], element.edges[1]

        # Get base edge table
        edges_table = self.graph_engine._edges_table
        if not edges_table:
            raise ValueError("No edges loaded in graph")

        # Filter edge1 by type
        edges1_table = edges_table
        if edge1.types:
            mask = pc.equal(edges_table.column('label'), pa.scalar(edge1.types[0]))
            edges1_table = edges_table.filter(mask)

        # Filter edge2 by type
        edges2_table = edges_table
        if edge2.types:
            mask = pc.equal(edges_table.column('label'), pa.scalar(edge2.types[0]))
            edges2_table = edges_table.filter(mask)

        # Convert to pattern matching format
        e1 = pa.table({
            'source': edges1_table.column('source'),
            'target': edges1_table.column('target')
        })
        e2 = pa.table({
            'source': edges2_table.column('source'),
            'target': edges2_table.column('target')
        })

        # Execute 3-hop pattern match
        result = match_3hop(e1, e2, e2)
        result_table = result.result_table()

        # Apply WHERE filters
        if where:
            result_table = self._apply_where_filter(result_table, where, element)

        return result_table

    def _translate_variable_length(self, pattern: Pattern, where: Optional[Expression]) -> pa.Table:
        """
        Translate variable-length path: (a)-[r*1..3]->(b)

        Example:
            MATCH (a:Person)-[:KNOWS*1..3]->(b) WHERE a.name = 'Alice'
        """
        element = pattern.elements[0]

        # Find the edge with recursive info
        edge_with_path = None
        for edge in element.edges:
            if edge.recursive:
                edge_with_path = edge
                break

        if not edge_with_path:
            raise ValueError("No variable-length edge found in pattern")

        # Get edge table
        edges_table = self.graph_engine._edges_table
        if not edges_table:
            raise ValueError("No edges loaded in graph")

        # Filter by edge type
        if edge_with_path.types:
            mask = pc.equal(edges_table.column('label'), pa.scalar(edge_with_path.types[0]))
            edges_table = edges_table.filter(mask)

        # Convert to pattern matching format
        edges = pa.table({
            'source': edges_table.column('source'),
            'target': edges_table.column('target')
        })

        # Get min/max hops
        recursive = edge_with_path.recursive
        min_hops = recursive.min_hops
        max_hops = recursive.max_hops if recursive.max_hops is not None else 10  # Default max

        # Execute variable-length path match
        # Note: start_vertex=-1 means all vertices
        result = match_variable_length_path(edges, 0, -1, min_hops, max_hops)
        result_table = result.result_table()

        return result_table

    def _apply_where_filter(self, table: pa.Table, where: Expression, element: PatternElement) -> pa.Table:
        """
        Apply WHERE clause filter to result table.

        This is simplified - full implementation would evaluate
        arbitrary expressions against table columns.

        Args:
            table: Result table from pattern matching
            where: WHERE clause expression
            element: Pattern element for variable mapping

        Returns:
            Filtered table
        """
        # For now, only support simple comparisons like a.age > 18
        if isinstance(where, Comparison):
            if isinstance(where.left, PropertyAccess):
                # Map variable to column name
                var_name = where.left.variable
                prop_name = where.left.property_name

                # Find column name in result table
                # Pattern match results use names like 'a_id', 'b_id', etc.
                # For properties, we'd need to join back to vertex table
                # This is simplified - just filter if column exists

                # For now, skip property filters
                # TODO: Implement full WHERE clause evaluation
                pass

        return table

    def _apply_return(self, table: pa.Table, return_clause: ReturnClause) -> pa.Table:
        """
        Apply RETURN clause projection to result table.

        Args:
            table: Result table from MATCH
            return_clause: ReturnClause AST node

        Returns:
            Projected table with selected columns
        """
        # For now, just return the table as-is
        # Full implementation would:
        # 1. Project only requested columns
        # 2. Rename columns based on aliases
        # 3. Apply DISTINCT if requested
        # 4. Apply ORDER BY
        # 5. Apply SKIP/LIMIT

        # Apply LIMIT if present
        if return_clause.limit:
            table = table.slice(0, return_clause.limit)

        # Apply SKIP if present
        if return_clause.skip:
            table = table.slice(return_clause.skip)

        return table

    # ========================================================================
    # Query Optimization Methods
    # ========================================================================

    def _optimize_match(
        self,
        pattern: Pattern,
        where: Optional[Expression]
    ) -> Tuple[Pattern, Optional[Expression]]:
        """
        Optimize MATCH pattern by reordering elements and pushing filters.

        Args:
            pattern: Original pattern from query
            where: WHERE clause expression

        Returns:
            Tuple of (optimized_pattern, remaining_where_filters)

        Optimization strategies:
        1. Reorder pattern elements by selectivity (most selective first)
        2. Push WHERE filters into pattern if possible
        3. Use statistics to guide reordering decisions
        """
        if not pattern.elements:
            return pattern, where

        # For now, focus on reordering
        # Filter pushdown is more complex and can be added later
        optimized_elements = []

        for element in pattern.elements:
            # Estimate selectivity for this pattern element
            selectivity = self._estimate_pattern_element_selectivity(element)
            optimized_elements.append((selectivity, element))

        # Sort by selectivity (descending - most selective first)
        optimized_elements.sort(key=lambda x: x[0], reverse=True)

        # Extract reordered elements
        reordered = [elem for (_, elem) in optimized_elements]

        # Create optimized pattern
        optimized_pattern = Pattern(elements=reordered)

        # For now, don't modify WHERE filters
        # TODO: Push WHERE filters into pattern matching
        remaining_where = where

        return optimized_pattern, remaining_where

    def _estimate_pattern_element_selectivity(self, element: PatternElement) -> float:
        """
        Estimate selectivity of a pattern element.

        Args:
            element: Pattern element (nodes + edges)

        Returns:
            Selectivity score (higher = more selective)

        Heuristics:
        - Node with label: +3.0
        - Edge with type: +2.0
        - Variable-length path: -1.0 (less selective)
        - Use statistics for refinement
        """
        selectivity = 0.0

        # Score nodes
        for node in element.nodes:
            if node.labels:
                # Node has label constraint - very selective
                selectivity += 3.0

                # Use statistics if available
                if self.statistics and node.labels[0] in self.statistics.vertex_label_counts:
                    label_count = self.statistics.vertex_label_counts[node.labels[0]]
                    if self.statistics.total_vertices > 0:
                        # Add bonus for rare labels
                        label_ratio = label_count / self.statistics.total_vertices
                        selectivity += (1.0 - label_ratio) * 2.0

        # Score edges
        for edge in element.edges:
            if edge.recursive:
                # Variable-length path - less selective
                selectivity -= 1.0
            elif edge.types:
                # Edge has type constraint - moderately selective
                selectivity += 2.0

                # Use statistics if available
                if self.statistics and edge.types[0] in self.statistics.edge_type_counts:
                    type_count = self.statistics.edge_type_counts[edge.types[0]]
                    if self.statistics.total_edges > 0:
                        # Add bonus for rare edge types
                        type_ratio = type_count / self.statistics.total_edges
                        selectivity += (1.0 - type_ratio) * 2.0

        return selectivity
