#!/usr/bin/env python3
"""
Minimal Cypher Parser

Simple regex-based Cypher parser for SabotCypher without external dependencies.
"""

import re
from typing import Dict, List, Any


class MinimalCypherParser:
    """
    Minimal Cypher parser using regex patterns.
    
    Converts Cypher queries to ArrowPlan format without external dependencies.
    """
    
    def __init__(self):
        """Initialize parser with regex patterns."""
        # Compile regex patterns for Cypher syntax
        self.match_pattern = re.compile(r'MATCH\s+(.+?)(?:\s+WHERE|\s+RETURN|$)', re.IGNORECASE)
        self.return_pattern = re.compile(r'RETURN\s+(.+?)(?:\s+ORDER|\s+LIMIT|$)', re.IGNORECASE)
        self.where_pattern = re.compile(r'WHERE\s+(.+?)(?:\s+RETURN|$)', re.IGNORECASE)
        self.order_pattern = re.compile(r'ORDER\s+BY\s+(.+?)(?:\s+LIMIT|$)', re.IGNORECASE)
        self.limit_pattern = re.compile(r'LIMIT\s+(\d+)', re.IGNORECASE)
        self.with_pattern = re.compile(r'WITH\s+(.+?)(?:\s+WHERE|\s+RETURN|$)', re.IGNORECASE)
        
        # Pattern detection
        self.arrow_pattern = re.compile(r'->')
        self.count_pattern = re.compile(r'count\s*\(\s*\*\s*\)', re.IGNORECASE)
        self.aggregate_patterns = {
            'count': re.compile(r'count\s*\(\s*(.+?)\s*\)', re.IGNORECASE),
            'avg': re.compile(r'avg\s*\(\s*(.+?)\s*\)', re.IGNORECASE),
            'sum': re.compile(r'sum\s*\(\s*(.+?)\s*\)', re.IGNORECASE),
            'min': re.compile(r'min\s*\(\s*(.+?)\s*\)', re.IGNORECASE),
            'max': re.compile(r'max\s*\(\s*(.+?)\s*\)', re.IGNORECASE)
        }
    
    def parse_to_arrow_plan(self, query_string: str) -> Dict[str, Any]:
        """
        Parse Cypher query to ArrowPlan.
        
        Args:
            query_string: Cypher query string
            
        Returns:
            Dict representing ArrowPlan
        """
        try:
            query_string = query_string.strip()
            operators = []
            
            # Parse MATCH clause
            match_ops = self._parse_match_clause(query_string)
            operators.extend(match_ops)
            
            # Parse WITH clause
            with_ops = self._parse_with_clause(query_string)
            operators.extend(with_ops)
            
            # Parse WHERE clause
            where_ops = self._parse_where_clause(query_string)
            operators.extend(where_ops)
            
            # Parse RETURN clause
            return_ops = self._parse_return_clause(query_string)
            operators.extend(return_ops)
            
            # Parse ORDER BY clause
            order_ops = self._parse_order_clause(query_string)
            operators.extend(order_ops)
            
            # Parse LIMIT clause
            limit_ops = self._parse_limit_clause(query_string)
            operators.extend(limit_ops)
            
            return {
                'operators': operators,
                'has_joins': self._has_joins(operators),
                'has_aggregates': self._has_aggregates(operators),
                'has_filters': self._has_filters(operators),
                'query': query_string
            }
            
        except Exception as e:
            return {
                'operators': [],
                'error': f"Parse error: {e}",
                'query': query_string
            }
    
    def _parse_match_clause(self, query: str) -> List[Dict[str, Any]]:
        """Parse MATCH clause to operators."""
        operators = []
        match = self.match_pattern.search(query)
        
        if match:
            pattern = match.group(1).strip()
            
            # Count arrows to determine pattern type
            arrow_count = len(self.arrow_pattern.findall(pattern))
            
            if arrow_count == 0:
                # Simple node scan
                operators.append({
                    'type': 'Scan',
                    'params': {'table': 'vertices'}
                })
            elif arrow_count == 1:
                # 2-hop pattern
                operators.append({
                    'type': 'Match2Hop',
                    'params': {
                        'source_name': 'a',
                        'intermediate_name': 'b',
                        'target_name': 'c'
                    }
                })
            elif arrow_count == 2:
                # 3-hop pattern
                operators.append({
                    'type': 'Match3Hop',
                    'params': {}
                })
            else:
                # Variable-length pattern
                operators.append({
                    'type': 'MatchVariableLength',
                    'params': {
                        'min_hops': 1,
                        'max_hops': arrow_count
                    }
                })
        
        return operators
    
    def _parse_with_clause(self, query: str) -> List[Dict[str, Any]]:
        """Parse WITH clause to operators."""
        operators = []
        match = self.with_pattern.search(query)
        
        if match:
            projection = match.group(1).strip()
            
            # Check for aggregations
            agg_op = self._detect_aggregation(projection)
            if agg_op:
                operators.append(agg_op)
            else:
                # Project columns
                operators.append({
                    'type': 'Project',
                    'params': {'columns': projection}
                })
        
        return operators
    
    def _parse_where_clause(self, query: str) -> List[Dict[str, Any]]:
        """Parse WHERE clause to operators."""
        operators = []
        match = self.where_pattern.search(query)
        
        if match:
            predicate = match.group(1).strip()
            operators.append({
                'type': 'Filter',
                'params': {'predicate': predicate}
            })
        
        return operators
    
    def _parse_return_clause(self, query: str) -> List[Dict[str, Any]]:
        """Parse RETURN clause to operators."""
        operators = []
        match = self.return_pattern.search(query)
        
        if match:
            projection = match.group(1).strip()
            
            # Check for aggregations
            agg_op = self._detect_aggregation(projection)
            if agg_op:
                operators.append(agg_op)
            else:
                # Project columns
                operators.append({
                    'type': 'Project',
                    'params': {'columns': projection}
                })
        
        return operators
    
    def _parse_order_clause(self, query: str) -> List[Dict[str, Any]]:
        """Parse ORDER BY clause to operators."""
        operators = []
        match = self.order_pattern.search(query)
        
        if match:
            columns = match.group(1).strip()
            operators.append({
                'type': 'OrderBy',
                'params': {'columns': columns}
            })
        
        return operators
    
    def _parse_limit_clause(self, query: str) -> List[Dict[str, Any]]:
        """Parse LIMIT clause to operators."""
        operators = []
        match = self.limit_pattern.search(query)
        
        if match:
            limit = match.group(1).strip()
            operators.append({
                'type': 'Limit',
                'params': {'limit': limit}
            })
        
        return operators
    
    def _detect_aggregation(self, projection: str) -> Dict[str, Any]:
        """Detect aggregation functions in projection."""
        for func_name, pattern in self.aggregate_patterns.items():
            match = pattern.search(projection)
            if match:
                column = match.group(1).strip() if match.groups() else '*'
                return {
                    'type': 'Aggregate',
                    'params': {
                        'function': func_name.upper(),
                        'column': column
                    }
                }
        return None
    
    def _has_joins(self, operators: List[Dict[str, Any]]) -> bool:
        """Check if plan has join operations."""
        return any(op['type'] in ['Match2Hop', 'Match3Hop', 'Join'] for op in operators)
    
    def _has_aggregates(self, operators: List[Dict[str, Any]]) -> bool:
        """Check if plan has aggregate operations."""
        return any(op['type'] == 'Aggregate' for op in operators)
    
    def _has_filters(self, operators: List[Dict[str, Any]]) -> bool:
        """Check if plan has filter operations."""
        return any(op['type'] == 'Filter' for op in operators)


def test_minimal_parser():
    """Test the minimal Cypher parser."""
    print("Testing Minimal Cypher Parser")
    print("=" * 40)
    
    parser = MinimalCypherParser()
    
    # Test queries
    test_queries = [
        "MATCH (a:Person) RETURN a.name",
        "MATCH (a)-[:KNOWS]->(b) RETURN a.name, b.age",
        "MATCH (a)-[:KNOWS]->(b)-[:KNOWS]->(c) RETURN count(*)",
        "MATCH (a) RETURN a ORDER BY a.name LIMIT 10",
        "MATCH (a:Person) WHERE a.age > 30 RETURN a.name",
        "MATCH (a) WITH a, count(*) as c RETURN c ORDER BY c LIMIT 5"
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
                if 'params' in op:
                    for key, value in op['params'].items():
                        print(f"         {key}: {value}")
    
    print("\n" + "=" * 40)
    print("Minimal parser test complete!")


if __name__ == "__main__":
    test_minimal_parser()
