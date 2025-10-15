"""
Streaming Graph Processor

Processes streaming graph data with temporal windows.
"""

import pyarrow as pa
import sys
from pathlib import Path
from datetime import timedelta, datetime
from typing import Callable, List, Tuple, Optional

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent / "python_integration"))

try:
    from sabot_cypher_integration import SabotCypherIntegration
    CYPHER_AVAILABLE = True
except ImportError:
    CYPHER_AVAILABLE = False

from .temporal_store import TemporalGraphStore
from .window import TimeWindowManager


class StreamingGraphProcessor:
    """
    Processes streaming graph data with temporal windows.
    
    Features:
    - Batch ingestion
    - Time-windowed queries
    - Continuous query execution
    - Zero-copy Arrow operations
    """
    
    def __init__(self, 
                 window_size: timedelta = timedelta(minutes=5),
                 slide_interval: timedelta = timedelta(seconds=30),
                 ttl: timedelta = timedelta(hours=1)):
        """
        Initialize streaming graph processor.
        
        Args:
            window_size: Size of query time window
            slide_interval: How often to slide the window
            ttl: Time-to-live for graph data
        """
        self.window_manager = TimeWindowManager(window_size, slide_interval)
        self.temporal_store = TemporalGraphStore(ttl=ttl)
        self.cypher_engine = SabotCypherIntegration() if CYPHER_AVAILABLE else None
        self.continuous_queries = []
        self.running = False
    
    def ingest_batch(self, vertices: Optional[pa.Table] = None, edges: Optional[pa.Table] = None):
        """
        Ingest a batch of vertices and edges.
        
        Args:
            vertices: Arrow table of vertices
            edges: Arrow table of edges
        """
        if vertices is not None:
            self.temporal_store.insert_vertices(vertices)
        
        if edges is not None:
            self.temporal_store.insert_edges(edges)
        
        # Check if window should slide
        if self.window_manager.should_slide():
            self._on_window_slide()
    
    def register_continuous_query(self, query: str, callback: Callable[[pa.Table], None]):
        """
        Register a query to run continuously on streaming data.
        
        Args:
            query: Cypher query string
            callback: Function to call with query results
        """
        self.continuous_queries.append({
            'query': query,
            'callback': callback
        })
    
    def _on_window_slide(self):
        """Execute continuous queries when window slides."""
        # Get current window data
        time_range = self.window_manager.get_current_window()
        vertices, edges = self.temporal_store.query(time_range)
        
        # Execute all registered queries
        for query_info in self.continuous_queries:
            query = query_info['query']
            callback = query_info['callback']
            
            try:
                # Execute query on windowed data
                if self.cypher_engine:
                    result = self.cypher_engine.execute_cypher(query, vertices, edges)
                    if result['success'] and result['result_table'] is not None:
                        callback(result['result_table'])
                else:
                    # Fallback: just pass through the data
                    callback(vertices)
            except Exception as e:
                print(f"Error executing continuous query: {e}")
        
        # Slide window
        self.window_manager.slide()
    
    def query_current_window(self, query: str) -> pa.Table:
        """
        Execute a query on the current window.
        
        Args:
            query: Cypher query string
        
        Returns:
            Arrow table with query results
        """
        time_range = self.window_manager.get_current_window()
        vertices, edges = self.temporal_store.query(time_range)
        
        if self.cypher_engine:
            result = self.cypher_engine.execute_cypher(query, vertices, edges)
            if result['success'] and result['result_table'] is not None:
                return result['result_table']
        
        # Fallback: return vertices
        return vertices
    
    def get_current_graph(self) -> Tuple[pa.Table, pa.Table]:
        """
        Get current windowed graph.
        
        Returns:
            Tuple of (vertices, edges) in current window
        """
        time_range = self.window_manager.get_current_window()
        return self.temporal_store.query(time_range)
    
    def get_stats(self) -> dict:
        """Get processor statistics."""
        store_stats = self.temporal_store.get_stats()
        window_stats = self.window_manager.get_stats()
        
        return {
            'store': store_stats,
            'window': window_stats,
            'continuous_queries': len(self.continuous_queries),
            'cypher_available': CYPHER_AVAILABLE
        }

