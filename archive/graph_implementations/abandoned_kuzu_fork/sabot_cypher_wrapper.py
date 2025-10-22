#!/usr/bin/env python3
"""
SabotCypher Python Wrapper

High-level Python API that connects:
- Lark Cypher parser (existing)
- AST → ArrowPlan translator
- C++ execution engine

This provides end-to-end Cypher query execution without building Kuzu frontend.
"""

import sys
import os

# Add paths
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, '/Users/bengamble/Sabot')

import pyarrow as pa


class SabotCypherEngine:
    """
    Complete Cypher query engine using:
    - Existing Lark parser
    - AST → ArrowPlan translation
    - sabot_cypher C++ execution
    """
    
    def __init__(self):
        """Initialize engine with stub components."""
        self.vertices = None
        self.edges = None
        self.use_native = False
        
        # Try to import native module
        try:
            import sabot_cypher
            if hasattr(sabot_cypher, 'SabotCypherBridge'):
                self.bridge = sabot_cypher.SabotCypherBridge.create()
                self.use_native = True
                print("✅ Using native sabot_cypher C++ module")
        except Exception as e:
            print(f"⚠️  Native module not available: {e}")
            print("   Using Python-only mode for demonstration")
            self.use_native = False
    
    def register_graph(self, vertices: pa.Table, edges: pa.Table):
        """Register graph data."""
        self.vertices = vertices
        self.edges = edges
        
        if self.use_native:
            self.bridge.register_graph(vertices, edges)
        
        print(f"✅ Graph registered: {vertices.num_rows:,} vertices, {edges.num_rows:,} edges")
    
    def execute(self, query: str, params: dict = None):
        """
        Execute Cypher query.
        
        Steps:
        1. Parse query with Lark parser
        2. Translate AST to ArrowPlan
        3. Execute with C++ engine
        
        Args:
            query: Cypher query string
            params: Query parameters (optional)
        
        Returns:
            Result object with table and metadata
        """
        if params is None:
            params = {}
        
        print(f"\nExecuting query:")
        print(f"  {query.strip()[:60]}...")
        
        # Step 1: Parse (would use Lark parser)
        # For now, use simple pattern matching for demo
        plan = self._query_to_plan(query)
        
        # Step 2: Execute plan
        if self.use_native:
            result = self.bridge.execute_plan(plan)
            print(f"  ✅ Executed in {result.execution_time_ms:.2f}ms")
            print(f"  Result: {result.num_rows} rows")
            return result
        else:
            print("  ⚠️  Native execution not available")
            print(f"  Plan generated: {len(plan['operators'])} operators")
            for i, op in enumerate(plan['operators'], 1):
                print(f"    {i}. {op['type']}")
            return None
    
    def _query_to_plan(self, query: str) -> dict:
        """
        Convert query to ArrowPlan.
        
        For demonstration, handles simple patterns.
        In production, would use full AST translator.
        """
        query_lower = query.lower()
        plan = {'operators': []}
        
        # Detect MATCH
        if 'match' in query_lower:
            # Simple scan
            plan['operators'].append({
                'type': 'Scan',
                'params': {'table': 'vertices'}
            })
        
        # Detect aggregation
        if 'count(' in query_lower:
            plan['operators'].append({
                'type': 'Aggregate',
                'params': {'functions': 'COUNT'}
            })
        
        # Detect LIMIT
        if 'limit' in query_lower:
            # Extract limit value
            import re
            match = re.search(r'limit\s+(\d+)', query_lower)
            if match:
                limit = match.group(1)
                plan['operators'].append({
                    'type': 'Limit',
                    'params': {'limit': limit, 'offset': '0'}
                })
        
        # Detect ORDER BY
        if 'order by' in query_lower:
            plan['operators'].append({
                'type': 'OrderBy',
                'params': {'sort_keys': 'result', 'directions': 'ASC'}
            })
        
        return plan


def demo():
    """Demonstrate end-to-end execution."""
    print("=" * 70)
    print("SabotCypher Python Wrapper Demo")
    print("=" * 70)
    print()
    
    # Create engine
    engine = SabotCypherEngine()
    print()
    
    # Create sample graph
    vertices = pa.table({
        'id': pa.array([1, 2, 3, 4, 5], type=pa.int64()),
        'label': pa.array(['Person', 'Person', 'Person', 'City', 'Interest']),
        'name': pa.array(['Alice', 'Bob', 'Charlie', 'NYC', 'Tennis']),
        'age': pa.array([25, 30, 35, None, None], type=pa.int32()),
    })
    
    edges = pa.table({
        'source': pa.array([1, 1, 2, 3, 1], type=pa.int64()),
        'target': pa.array([2, 3, 3, 4, 5], type=pa.int64()),
        'type': pa.array(['KNOWS', 'KNOWS', 'KNOWS', 'LIVES_IN', 'INTERESTED_IN']),
    })
    
    engine.register_graph(vertices, edges)
    print()
    
    # Test queries
    queries = [
        "MATCH (a) RETURN a LIMIT 3",
        "MATCH (a) RETURN count(a)",
        "MATCH (a:Person) RETURN a ORDER BY a.age LIMIT 5",
    ]
    
    for query in queries:
        result = engine.execute(query)
        print()
    
    print("=" * 70)
    print("Demo Complete!")
    print("=" * 70)
    print()
    print("Next steps:")
    print("  1. Build native module: cmake --build build")
    print("  2. Full AST translator: Connect to Lark parser")
    print("  3. Pattern matching: Integrate match kernels")
    print("  4. Validation: Run Q1-Q9 benchmarks")
    print()


if __name__ == "__main__":
    demo()

