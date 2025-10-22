#!/usr/bin/env python3
"""
Simple Cypher Parser Integration

Minimal Lark parser integration for SabotCypher without Sabot dependencies.
"""

import sys
import os
from pathlib import Path

# Add grammar path
grammar_path = Path(__file__).parent.parent.parent / "grammar" / "cypher.lark"
if not grammar_path.exists():
    print(f"Grammar file not found: {grammar_path}")
    sys.exit(1)

try:
    from lark import Lark
except ImportError:
    print("Lark parser not available. Install with: pip install lark")
    sys.exit(1)


class SimpleCypherParser:
    """
    Simple Cypher parser using Lark grammar.
    
    Parses Cypher queries and converts them to ArrowPlan format.
    """
    
    def __init__(self):
        """Initialize parser with grammar."""
        with open(grammar_path, 'r') as f:
            grammar = f.read()
        
        self.parser = Lark(grammar, parser='earley', start='cypher')
    
    def parse_to_arrow_plan(self, query_string: str) -> dict:
        """
        Parse Cypher query to ArrowPlan.
        
        Args:
            query_string: Cypher query string
            
        Returns:
            Dict representing ArrowPlan
        """
        try:
            # Parse query
            tree = self.parser.parse(query_string)
            
            # Convert to ArrowPlan (simplified)
            return self._tree_to_arrow_plan(tree, query_string)
            
        except Exception as e:
            return {
                'operators': [],
                'error': f"Parse error: {e}",
                'query': query_string
            }
    
    def _tree_to_arrow_plan(self, tree, query_string: str) -> dict:
        """Convert parse tree to ArrowPlan (simplified)."""
        operators = []
        
        # Simple pattern detection based on query string
        query_lower = query_string.lower()
        
        # Detect MATCH clause
        if 'match' in query_lower:
            if '->' in query_string:
                # Pattern matching
                if query_string.count('->') == 1:
                    operators.append({
                        'type': 'Match2Hop',
                        'params': {'source_name': 'a', 'intermediate_name': 'b', 'target_name': 'c'}
                    })
                elif query_string.count('->') == 2:
                    operators.append({
                        'type': 'Match3Hop',
                        'params': {}
                    })
                else:
                    operators.append({
                        'type': 'Scan',
                        'params': {'table': 'vertices'}
                    })
            else:
                # Simple node scan
                operators.append({
                    'type': 'Scan',
                    'params': {'table': 'vertices'}
                })
        
        # Detect RETURN clause
        if 'return' in query_lower:
            # Extract return items
            return_part = query_string[query_string.lower().find('return') + 6:].strip()
            
            # Check for aggregations
            if 'count(' in return_part.lower():
                operators.append({
                    'type': 'Aggregate',
                    'params': {'function': 'COUNT', 'column': '*'}
                })
            elif 'avg(' in return_part.lower():
                operators.append({
                    'type': 'Aggregate',
                    'params': {'function': 'AVG', 'column': return_part}
                })
            elif 'sum(' in return_part.lower():
                operators.append({
                    'type': 'Aggregate',
                    'params': {'function': 'SUM', 'column': return_part}
                })
            else:
                # Project columns
                operators.append({
                    'type': 'Project',
                    'params': {'columns': return_part}
                })
        
        # Detect ORDER BY
        if 'order by' in query_lower:
            operators.append({
                'type': 'OrderBy',
                'params': {'columns': 'id'}  # Simplified
            })
        
        # Detect LIMIT
        if 'limit' in query_lower:
            limit_start = query_lower.find('limit') + 5
            limit_part = query_string[limit_start:].strip()
            operators.append({
                'type': 'Limit',
                'params': {'limit': limit_part}
            })
        
        return {
            'operators': operators,
            'has_joins': any(op['type'] in ['Match2Hop', 'Match3Hop'] for op in operators),
            'has_aggregates': any(op['type'] == 'Aggregate' for op in operators),
            'has_filters': any(op['type'] == 'Filter' for op in operators),
            'query': query_string
        }


def test_simple_parser():
    """Test the simple Cypher parser."""
    print("Testing Simple Cypher Parser")
    print("=" * 40)
    
    parser = SimpleCypherParser()
    
    # Test queries
    test_queries = [
        "MATCH (a:Person) RETURN a.name",
        "MATCH (a)-[:KNOWS]->(b) RETURN a.name, b.age",
        "MATCH (a)-[:KNOWS]->(b)-[:KNOWS]->(c) RETURN count(*)",
        "MATCH (a) RETURN a ORDER BY a.name LIMIT 10",
        "MATCH (a:Person) WHERE a.age > 30 RETURN a.name"
    ]
    
    for i, query in enumerate(test_queries, 1):
        print(f"\n{i}. Query: {query}")
        plan = parser.parse_to_arrow_plan(query)
        
        if 'error' in plan:
            print(f"   ❌ {plan['error']}")
        else:
            print(f"   ✅ Translated to {len(plan['operators'])} operators:")
            for j, op in enumerate(plan['operators'], 1):
                print(f"      {j}. {op['type']}")
    
    print("\n" + "=" * 40)
    print("Simple parser test complete!")


if __name__ == "__main__":
    test_simple_parser()
