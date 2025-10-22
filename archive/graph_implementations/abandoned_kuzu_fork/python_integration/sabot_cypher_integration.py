#!/usr/bin/env python3
"""
SabotCypher Integration

Complete integration of Cypher parser with SabotCypher execution engine.
"""

import sys
import os
import pyarrow as pa
from pathlib import Path

# Add current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from minimal_cypher_parser import MinimalCypherParser


class SabotCypherIntegration:
    """
    Complete SabotCypher integration with Cypher parser.
    
    Provides end-to-end Cypher query execution:
    1. Parse Cypher query to ArrowPlan
    2. Execute ArrowPlan using SabotCypher engine
    3. Return results as PyArrow table
    """
    
    def __init__(self):
        """Initialize SabotCypher integration."""
        self.parser = MinimalCypherParser()
        
        # Try to import SabotCypher module
        try:
            import sabot_cypher_working
            self.engine = sabot_cypher_working.create_engine()
            self.cypher_available = True
        except ImportError:
            print("Warning: SabotCypher engine not available")
            self.engine = None
            self.cypher_available = False
    
    def execute_cypher(self, query_string: str, vertices=None, edges=None):
        """
        Execute Cypher query end-to-end.
        
        Args:
            query_string: Cypher query string
            vertices: Optional PyArrow table with vertices
            edges: Optional PyArrow table with edges
            
        Returns:
            Dict with results and metadata
        """
        # Parse Cypher query to ArrowPlan
        plan = self.parser.parse_to_arrow_plan(query_string)
        
        if 'error' in plan:
            return {
                'success': False,
                'error': plan['error'],
                'query': query_string
            }
        
        if not self.cypher_available:
            return {
                'success': False,
                'error': 'SabotCypher engine not available',
                'plan': plan,
                'query': query_string
            }
        
        # Register graph if provided
        if vertices is not None and edges is not None:
            self.engine.register_graph(vertices, edges)
        
        # Execute ArrowPlan
        try:
            result = self.engine.execute_plan(plan)
            
            return {
                'success': True,
                'table': result['table'],
                'execution_time_ms': result['execution_time_ms'],
                'num_rows': result['num_rows'],
                'plan': plan,
                'query': query_string
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'plan': plan,
                'query': query_string
            }
    
    def explain_plan(self, query_string: str):
        """
        Explain query execution plan.
        
        Args:
            query_string: Cypher query string
            
        Returns:
            Dict with plan explanation
        """
        plan = self.parser.parse_to_arrow_plan(query_string)
        
        if 'error' in plan:
            return {
                'success': False,
                'error': plan['error'],
                'query': query_string
            }
        
        explanation = {
            'success': True,
            'query': query_string,
            'operators': [],
            'summary': {
                'total_operators': len(plan['operators']),
                'has_joins': plan['has_joins'],
                'has_aggregates': plan['has_aggregates'],
                'has_filters': plan['has_filters']
            }
        }
        
        for i, op in enumerate(plan['operators'], 1):
            explanation['operators'].append({
                'step': i,
                'type': op['type'],
                'params': op.get('params', {}),
                'description': self._describe_operator(op)
            })
        
        return explanation
    
    def _describe_operator(self, op: dict) -> str:
        """Generate human-readable description of operator."""
        op_type = op['type']
        params = op.get('params', {})
        
        descriptions = {
            'Scan': f"Scan {params.get('table', 'table')}",
            'Match2Hop': f"Match 2-hop pattern: {params.get('source_name', 'a')} -> {params.get('intermediate_name', 'b')} -> {params.get('target_name', 'c')}",
            'Match3Hop': "Match 3-hop pattern",
            'MatchVariableLength': f"Match variable-length path (min: {params.get('min_hops', 1)}, max: {params.get('max_hops', 3)})",
            'MatchTriangle': "Match triangle pattern",
            'Project': f"Project columns: {params.get('columns', 'all')}",
            'Filter': f"Filter with predicate: {params.get('predicate', 'unknown')}",
            'Aggregate': f"Aggregate {params.get('function', 'unknown')}({params.get('column', '*')})",
            'OrderBy': f"Order by: {params.get('columns', 'unknown')}",
            'Limit': f"Limit to {params.get('limit', 'unknown')} rows",
            'PropertyAccess': f"Access property {params.get('property', 'unknown')} of {params.get('variable', 'unknown')}"
        }
        
        return descriptions.get(op_type, f"Execute {op_type} operation")


def test_integration():
    """Test the complete SabotCypher integration."""
    print("Testing SabotCypher Integration")
    print("=" * 50)
    
    integration = SabotCypherIntegration()
    
    # Create test graph
    vertices = pa.table({
        'id': pa.array([1, 2, 3, 4, 5], type=pa.int64()),
        'name': pa.array(['Alice', 'Bob', 'Charlie', 'David', 'Eve']),
        'age': pa.array([25, 30, 35, 28, 32], type=pa.int32()),
        'city': pa.array(['NYC', 'LA', 'Chicago', 'NYC', 'LA'])
    })
    
    edges = pa.table({
        'source': pa.array([1, 1, 2, 3, 4], type=pa.int64()),
        'target': pa.array([2, 3, 3, 4, 5], type=pa.int64()),
        'type': pa.array(['KNOWS', 'KNOWS', 'KNOWS', 'KNOWS', 'KNOWS'])
    })
    
    print(f"Test graph: {vertices.num_rows} vertices, {edges.num_rows} edges")
    
    # Test queries
    test_queries = [
        "MATCH (a:Person) RETURN a.name",
        "MATCH (a)-[:KNOWS]->(b) RETURN a.name, b.age",
        "MATCH (a)-[:KNOWS]->(b)-[:KNOWS]->(c) RETURN count(*)",
        "MATCH (a) RETURN a ORDER BY a.name LIMIT 3"
    ]
    
    for i, query in enumerate(test_queries, 1):
        print(f"\n{i}. Query: {query}")
        
        # Explain plan
        explanation = integration.explain_plan(query)
        if explanation['success']:
            print(f"   Plan: {explanation['summary']['total_operators']} operators")
            for op in explanation['operators']:
                print(f"      {op['step']}. {op['description']}")
        
        # Execute query
        result = integration.execute_cypher(query, vertices, edges)
        
        if result['success']:
            print(f"   ✅ Executed successfully")
            print(f"      Rows: {result['num_rows']}")
            print(f"      Time: {result['execution_time_ms']:.2f}ms")
            if result['table'] is not None:
                print(f"      Schema: {result['table'].schema}")
        else:
            print(f"   ❌ Execution failed: {result['error']}")
    
    print("\n" + "=" * 50)
    print("Integration test complete!")


if __name__ == "__main__":
    test_integration()
