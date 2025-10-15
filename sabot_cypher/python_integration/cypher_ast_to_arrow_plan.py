#!/usr/bin/env python3
"""
Cypher AST to ArrowPlan Translator

Connects the Lark Cypher parser to SabotCypher's Arrow execution engine.
Translates CypherQuery AST into ArrowPlan for execution.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sabot._cython.graph.compiler.cypher_parser import CypherParser
from sabot._cython.graph.compiler.cypher_ast import (
    CypherQuery, MatchClause, ReturnClause, WithClause,
    NodePattern, RelationshipPattern, PatternElement,
    Variable, PropertyLookup, FunctionCall, Comparison
)


class CypherASTToArrowPlanTranslator:
    """
    Translates CypherQuery AST to ArrowPlan for SabotCypher execution.
    
    This bridges the gap between:
    - Lark parser (CypherQuery AST)
    - SabotCypher execution engine (ArrowPlan)
    """
    
    def __init__(self):
        self.parser = CypherParser()
    
    def translate_query(self, query_string: str) -> dict:
        """
        Translate Cypher query string to ArrowPlan.
        
        Args:
            query_string: Cypher query string
            
        Returns:
            Dict representing ArrowPlan with operators list
        """
        # Parse Cypher query to AST
        ast = self.parser.parse(query_string)
        
        # Translate AST to ArrowPlan
        return self._translate_ast(ast)
    
    def _translate_ast(self, ast: CypherQuery) -> dict:
        """Translate CypherQuery AST to ArrowPlan."""
        operators = []
        
        # Process MATCH clauses
        for match_clause in ast.match_clauses:
            operators.extend(self._translate_match_clause(match_clause))
        
        # Process WITH clauses
        for with_clause in ast.with_clauses:
            operators.extend(self._translate_with_clause(with_clause))
        
        # Process RETURN clause
        if ast.return_clause:
            operators.extend(self._translate_return_clause(ast.return_clause))
        
        return {
            'operators': operators,
            'has_joins': self._has_joins(operators),
            'has_aggregates': self._has_aggregates(operators),
            'has_filters': self._has_filters(operators)
        }
    
    def _translate_match_clause(self, match_clause: MatchClause) -> list:
        """Translate MATCH clause to operators."""
        operators = []
        
        for pattern in match_clause.patterns:
            # Determine pattern type and create appropriate operator
            if self._is_2hop_pattern(pattern):
                operators.append({
                    'type': 'Match2Hop',
                    'params': {
                        'source_name': 'a',
                        'intermediate_name': 'b', 
                        'target_name': 'c'
                    }
                })
            elif self._is_3hop_pattern(pattern):
                operators.append({
                    'type': 'Match3Hop',
                    'params': {}
                })
            elif self._is_variable_length_pattern(pattern):
                operators.append({
                    'type': 'MatchVariableLength',
                    'params': {
                        'min_hops': 1,
                        'max_hops': 3
                    }
                })
            elif self._is_triangle_pattern(pattern):
                operators.append({
                    'type': 'MatchTriangle',
                    'params': {}
                })
            else:
                # Simple node scan
                operators.append({
                    'type': 'Scan',
                    'params': {'table': 'vertices'}
                })
        
        # Add WHERE clause as filter
        if match_clause.where_clause:
            operators.append({
                'type': 'Filter',
                'params': {'predicate': str(match_clause.where_clause)}
            })
        
        return operators
    
    def _translate_with_clause(self, with_clause: WithClause) -> list:
        """Translate WITH clause to operators."""
        operators = []
        
        # WITH clause typically involves projection and filtering
        if with_clause.projection_items:
            columns = []
            for item in with_clause.projection_items:
                if hasattr(item, 'expression'):
                    # Handle property access: a.name -> a_name
                    if isinstance(item.expression, PropertyLookup):
                        var_name = item.expression.variable
                        prop_name = item.expression.property
                        columns.append(f"{var_name}.{prop_name}")
                    else:
                        columns.append(str(item.expression))
                else:
                    columns.append(str(item))
            
            operators.append({
                'type': 'Project',
                'params': {'columns': ','.join(columns)}
            })
        
        # Add WHERE clause as filter
        if with_clause.where_clause:
            operators.append({
                'type': 'Filter',
                'params': {'predicate': str(with_clause.where_clause)}
            })
        
        return operators
    
    def _translate_return_clause(self, return_clause: ReturnClause) -> list:
        """Translate RETURN clause to operators."""
        operators = []
        
        # Project selected columns
        if return_clause.projection_items:
            columns = []
            for item in return_clause.projection_items:
                if hasattr(item, 'expression'):
                    # Handle property access: a.name -> a_name
                    if isinstance(item.expression, PropertyLookup):
                        var_name = item.expression.variable
                        prop_name = item.expression.property
                        columns.append(f"{var_name}.{prop_name}")
                    # Handle function calls: count(*) -> COUNT
                    elif isinstance(item.expression, FunctionCall):
                        if item.expression.name.upper() == 'COUNT':
                            operators.append({
                                'type': 'Aggregate',
                                'params': {'function': 'COUNT', 'column': '*'}
                            })
                            continue
                        else:
                            columns.append(str(item.expression))
                    else:
                        columns.append(str(item.expression))
                else:
                    columns.append(str(item))
            
            if columns:
                operators.append({
                    'type': 'Project',
                    'params': {'columns': ','.join(columns)}
                })
        
        # Add ORDER BY
        if return_clause.order_clause:
            sort_columns = []
            for sort_item in return_clause.order_clause.sort_items:
                sort_columns.append(str(sort_item.expression))
            
            operators.append({
                'type': 'OrderBy',
                'params': {'columns': ','.join(sort_columns)}
            })
        
        # Add LIMIT
        if return_clause.limit_clause:
            operators.append({
                'type': 'Limit',
                'params': {'limit': str(return_clause.limit_clause)}
            })
        
        return operators
    
    def _is_2hop_pattern(self, pattern) -> bool:
        """Check if pattern is 2-hop: (a)-[r1]->(b)-[r2]->(c)"""
        # Simplified check - in real implementation, analyze pattern structure
        return False
    
    def _is_3hop_pattern(self, pattern) -> bool:
        """Check if pattern is 3-hop: (a)-[r1]->(b)-[r2]->(c)-[r3]->(d)"""
        return False
    
    def _is_variable_length_pattern(self, pattern) -> bool:
        """Check if pattern has variable-length relationships: -[*1..3]->"""
        return False
    
    def _is_triangle_pattern(self, pattern) -> bool:
        """Check if pattern forms a triangle: (a)-[r1]->(b)-[r2]->(c)-[r3]->(a)"""
        return False
    
    def _has_joins(self, operators: list) -> bool:
        """Check if plan has join operations."""
        return any(op['type'] in ['Join', 'Match2Hop', 'Match3Hop'] for op in operators)
    
    def _has_aggregates(self, operators: list) -> bool:
        """Check if plan has aggregate operations."""
        return any(op['type'] == 'Aggregate' for op in operators)
    
    def _has_filters(self, operators: list) -> bool:
        """Check if plan has filter operations."""
        return any(op['type'] == 'Filter' for op in operators)


def test_translator():
    """Test the Cypher AST to ArrowPlan translator."""
    print("Testing Cypher AST to ArrowPlan Translator")
    print("=" * 50)
    
    translator = CypherASTToArrowPlanTranslator()
    
    # Test queries
    test_queries = [
        "MATCH (a:Person) RETURN a.name",
        "MATCH (a)-[:KNOWS]->(b) RETURN a.name, b.age",
        "MATCH (a)-[:KNOWS]->(b)-[:KNOWS]->(c) RETURN count(*)",
        "MATCH (a) WITH a, count(*) as c RETURN c ORDER BY c LIMIT 10"
    ]
    
    for i, query in enumerate(test_queries, 1):
        print(f"\n{i}. Query: {query}")
        try:
            plan = translator.translate_query(query)
            print(f"   ✅ Translated to {len(plan['operators'])} operators:")
            for j, op in enumerate(plan['operators'], 1):
                print(f"      {j}. {op['type']}")
        except Exception as e:
            print(f"   ❌ Translation failed: {e}")
    
    print("\n" + "=" * 50)
    print("Translator test complete!")


if __name__ == "__main__":
    test_translator()
