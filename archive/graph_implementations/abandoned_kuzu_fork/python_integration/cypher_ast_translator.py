#!/usr/bin/env python3
"""
Cypher AST to ArrowPlan Translator

Translates Cypher AST (from Lark parser) to ArrowPlan for execution.
This bypasses the need to build Kuzu frontend by using our existing parser.
"""

import sys
sys.path.insert(0, '/Users/bengamble/Sabot')

from sabot._cython.graph.compiler.cypher_parser import CypherParser
from sabot._cython.graph.compiler.cypher_ast import (
    CypherQuery, MatchClause, WhereClause, ReturnClause, WithClause,
    PatternElement, NodePattern, RelationshipPattern,
    PropertyAccess, Variable, Literal, FunctionCall, Comparison, BinaryOp
)


class CypherASTTranslator:
    """
    Translates Cypher AST to ArrowPlan.
    
    This allows us to use the existing Lark-based parser
    instead of building the complex Kuzu frontend.
    """
    
    def __init__(self):
        self.var_counter = 0
    
    def translate(self, query: CypherQuery) -> dict:
        """
        Translate complete Cypher query to ArrowPlan.
        
        Returns:
            dict: ArrowPlan structure with operators list
        """
        operators = []
        
        # Process MATCH clauses
        for match_clause in query.match_clauses:
            match_ops = self.translate_match(match_clause)
            operators.extend(match_ops)
        
        # Process WHERE clause
        if query.where_clause:
            where_ops = self.translate_where(query.where_clause)
            operators.extend(where_ops)
        
        # Process WITH clause
        if query.with_clauses:
            for with_clause in query.with_clauses:
                with_ops = self.translate_with(with_clause)
                operators.extend(with_ops)
        
        # Process RETURN clause
        if query.return_clause:
            return_ops = self.translate_return(query.return_clause)
            operators.extend(return_ops)
        
        return {
            'operators': operators,
            'has_joins': any(op['type'] == 'Extend' for op in operators),
            'has_aggregates': any(op['type'] == 'Aggregate' for op in operators),
            'has_filters': any(op['type'] == 'Filter' for op in operators),
        }
    
    def translate_match(self, match: MatchClause) -> list:
        """Translate MATCH clause to Scan + Extend operators."""
        operators = []
        
        pattern = match.pattern
        
        # For each node in pattern, add Scan
        for i, element in enumerate(pattern.elements):
            for node in element.nodes:
                if node.label:
                    operators.append({
                        'type': 'Scan',
                        'params': {
                            'table': 'vertices',
                            'label': node.label,
                            'variable': node.variable or f'_node_{self.var_counter}',
                        }
                    })
                    self.var_counter += 1
            
            # For each relationship, add Extend
            for rel in element.relationships:
                operators.append({
                    'type': 'Extend',
                    'params': {
                        'edge_type': rel.type if rel.type else '',
                        'direction': rel.direction or 'outgoing',
                        'min_hops': str(rel.min_hops) if hasattr(rel, 'min_hops') else '1',
                        'max_hops': str(rel.max_hops) if hasattr(rel, 'max_hops') else '1',
                    }
                })
        
        return operators
    
    def translate_where(self, where: WhereClause) -> list:
        """Translate WHERE clause to Filter operator."""
        # Convert WHERE expression to predicate string
        predicate = self.expression_to_string(where.expression)
        
        return [{
            'type': 'Filter',
            'params': {
                'predicate': predicate,
            }
        }]
    
    def translate_return(self, return_clause: ReturnClause) -> list:
        """Translate RETURN clause to Project + Aggregate + OrderBy + Limit."""
        operators = []
        
        # Check if we have aggregations
        has_agg = any(
            isinstance(item.expression, FunctionCall) and 
            item.expression.name.upper() in ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX']
            for item in return_clause.items
        )
        
        if has_agg:
            # Aggregation query
            for item in return_clause.items:
                if isinstance(item.expression, FunctionCall):
                    func_name = item.expression.name.upper()
                    
                    if func_name == 'COUNT':
                        operators.append({
                            'type': 'Aggregate',
                            'params': {
                                'functions': 'COUNT',
                            }
                        })
                    else:
                        # SUM, AVG, MIN, MAX
                        column = self.expression_to_string(item.expression.arguments[0])
                        operators.append({
                            'type': 'Aggregate',
                            'params': {
                                'functions': func_name,
                                'column': column,
                            }
                        })
        else:
            # Regular projection
            columns = []
            for item in return_clause.items:
                col_expr = self.expression_to_string(item.expression)
                columns.append(col_expr)
            
            if columns:
                operators.append({
                    'type': 'Project',
                    'params': {
                        'columns': ','.join(columns),
                    }
                })
        
        # ORDER BY
        if return_clause.order_by:
            sort_keys = []
            directions = []
            
            for order in return_clause.order_by:
                sort_keys.append(self.expression_to_string(order.expression))
                directions.append('DESC' if order.descending else 'ASC')
            
            operators.append({
                'type': 'OrderBy',
                'params': {
                    'sort_keys': ','.join(sort_keys),
                    'directions': directions[0] if len(directions) == 1 else 'ASC',
                }
            })
        
        # LIMIT
        if return_clause.limit:
            operators.append({
                'type': 'Limit',
                'params': {
                    'limit': str(return_clause.limit),
                    'offset': str(return_clause.skip) if return_clause.skip else '0',
                }
            })
        
        return operators
    
    def translate_with(self, with_clause: WithClause) -> list:
        """Translate WITH clause (similar to RETURN)."""
        # WITH is like RETURN but for intermediate results
        # Use same logic as translate_return
        return self.translate_return(with_clause)
    
    def expression_to_string(self, expr) -> str:
        """Convert expression to string representation."""
        if isinstance(expr, Variable):
            return expr.name
        elif isinstance(expr, Literal):
            return str(expr.value)
        elif isinstance(expr, PropertyAccess):
            return f"{expr.variable}.{expr.property_name}"
        elif isinstance(expr, FunctionCall):
            args = [self.expression_to_string(arg) for arg in expr.arguments]
            return f"{expr.name}({','.join(args)})"
        elif isinstance(expr, Comparison):
            left = self.expression_to_string(expr.left)
            right = self.expression_to_string(expr.right)
            return f"{left} {expr.operator} {right}"
        elif isinstance(expr, BinaryOp):
            left = self.expression_to_string(expr.left)
            right = self.expression_to_string(expr.right)
            return f"{left} {expr.operator} {right}"
        else:
            return str(expr)


# Integration function
def parse_and_translate_cypher(query_text: str) -> dict:
    """
    Parse Cypher query and translate to ArrowPlan.
    
    Args:
        query_text: Cypher query string
    
    Returns:
        ArrowPlan dict ready for execution
    """
    # Parse with existing Lark parser
    parser = CypherParser()
    ast = parser.parse(query_text)
    
    # Translate to ArrowPlan
    translator = CypherASTTranslator()
    arrow_plan = translator.translate(ast)
    
    return arrow_plan


# Test
if __name__ == "__main__":
    # Test with Q1
    query = """
        MATCH (follower:Person)-[:Follows]->(person:Person)
        RETURN person.id AS personID, person.name AS name, count(follower.id) AS numFollowers
        ORDER BY numFollowers DESC LIMIT 3
    """
    
    try:
        plan = parse_and_translate_cypher(query)
        print("✅ Q1 translated successfully!")
        print(f"Operators: {len(plan['operators'])}")
        for i, op in enumerate(plan['operators']):
            print(f"  {i+1}. {op['type']}: {op['params']}")
    except Exception as e:
        print(f"❌ Translation failed: {e}")
        import traceback
        traceback.print_exc()

