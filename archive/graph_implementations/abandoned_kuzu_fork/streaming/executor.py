"""
Continuous Query Executor

Executes Cypher queries continuously on streaming data.
"""

import pyarrow as pa
from typing import Callable, List, Tuple, Any
from datetime import datetime


class ContinuousQueryExecutor:
    """
    Executes Cypher queries continuously on streaming data.
    
    Features:
    - Multiple concurrent queries
    - Asynchronous execution
    - Result streaming
    - Error handling
    """
    
    def __init__(self, processor):
        """
        Initialize continuous query executor.
        
        Args:
            processor: StreamingGraphProcessor instance
        """
        self.processor = processor
        self.queries = []
        self.query_stats = {}
    
    def register(self, query_id: str, query: str, output_fn: Callable[[pa.Table], None]):
        """
        Register a continuous query.
        
        Args:
            query_id: Unique identifier for the query
            query: Cypher query string
            output_fn: Function to call with query results
        """
        self.queries.append({
            'id': query_id,
            'query': query,
            'output_fn': output_fn,
            'execution_count': 0,
            'total_time_ms': 0.0,
            'error_count': 0
        })
        
        self.query_stats[query_id] = {
            'execution_count': 0,
            'avg_time_ms': 0.0,
            'error_count': 0,
            'last_execution': None
        }
    
    def execute_on_batch(self, vertices: pa.Table, edges: pa.Table):
        """
        Execute all registered queries on new batch.
        
        Args:
            vertices: Arrow table of vertices
            edges: Arrow table of edges
        """
        for query_info in self.queries:
            query_id = query_info['id']
            query = query_info['query']
            output_fn = query_info['output_fn']
            
            try:
                start_time = datetime.now()
                
                # Execute query
                result = self.processor.cypher_engine.execute_cypher(query, vertices, edges)
                
                end_time = datetime.now()
                execution_time_ms = (end_time - start_time).total_seconds() * 1000
                
                # Update stats
                query_info['execution_count'] += 1
                query_info['total_time_ms'] += execution_time_ms
                
                if result['success'] and result['result_table'] is not None:
                    # Call output function with results
                    output_fn(result['result_table'])
                    
                    # Update stats
                    self.query_stats[query_id]['execution_count'] += 1
                    self.query_stats[query_id]['avg_time_ms'] = query_info['total_time_ms'] / query_info['execution_count']
                    self.query_stats[query_id]['last_execution'] = end_time
                else:
                    query_info['error_count'] += 1
                    self.query_stats[query_id]['error_count'] += 1
                
            except Exception as e:
                query_info['error_count'] += 1
                self.query_stats[query_id]['error_count'] += 1
                print(f"Error executing query {query_id}: {e}")
    
    def get_stats(self) -> dict:
        """Get executor statistics."""
        return {
            'total_queries': len(self.queries),
            'query_stats': self.query_stats
        }

