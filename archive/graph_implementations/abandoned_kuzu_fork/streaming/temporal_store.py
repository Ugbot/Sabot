"""
Temporal Graph Store

In-memory temporal graph store with time-based indexing and expiration.
"""

import pyarrow as pa
import pyarrow.compute as pc
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from collections import defaultdict


class TemporalGraphStore:
    """
    In-memory temporal graph store with time-based indexing.
    
    Features:
    - Time-bucketed storage
    - TTL-based expiration
    - Efficient time-range queries
    - Zero-copy Arrow operations
    """
    
    def __init__(self, ttl: timedelta = timedelta(hours=1), bucket_size: timedelta = timedelta(minutes=5)):
        """
        Initialize temporal graph store.
        
        Args:
            ttl: Time-to-live for graph data
            bucket_size: Size of time buckets for partitioning
        """
        self.ttl = ttl
        self.bucket_size = bucket_size
        self.vertices: Dict[int, pa.Table] = {}  # bucket_id -> Table
        self.edges: Dict[int, pa.Table] = {}     # bucket_id -> Table
        self.vertex_index: Dict[int, List[int]] = defaultdict(list)  # vertex_id -> [bucket_ids]
    
    def _get_bucket_id(self, timestamp: datetime) -> int:
        """Get bucket ID for a timestamp."""
        epoch = datetime(1970, 1, 1)
        seconds = (timestamp - epoch).total_seconds()
        bucket_seconds = self.bucket_size.total_seconds()
        return int(seconds // bucket_seconds)
    
    def _get_bucket_range(self, start_time: datetime, end_time: datetime) -> List[int]:
        """Get list of bucket IDs in time range."""
        start_bucket = self._get_bucket_id(start_time)
        end_bucket = self._get_bucket_id(end_time)
        return list(range(start_bucket, end_bucket + 1))
    
    def insert_vertices(self, vertices: pa.Table):
        """
        Insert vertices with timestamps.
        
        Args:
            vertices: Arrow table with columns including 'id' and 'timestamp'
        """
        if 'timestamp' not in vertices.column_names:
            # Add current timestamp if not present
            current_time = datetime.now()
            timestamps = pa.array([current_time] * vertices.num_rows)
            vertices = vertices.append_column('timestamp', timestamps)
        
        # Partition by time bucket
        timestamp_col = vertices['timestamp']
        
        for i in range(vertices.num_rows):
            timestamp = timestamp_col[i].as_py()
            bucket_id = self._get_bucket_id(timestamp)
            
            # Get single row as table
            row_table = vertices.slice(i, 1)
            
            # Add to bucket
            if bucket_id in self.vertices:
                # Concatenate with existing data
                self.vertices[bucket_id] = pa.concat_tables([self.vertices[bucket_id], row_table])
            else:
                self.vertices[bucket_id] = row_table
            
            # Update index
            if 'id' in vertices.column_names:
                vertex_id = vertices['id'][i].as_py()
                self.vertex_index[vertex_id].append(bucket_id)
        
        # Expire old buckets
        self._expire_old_buckets()
    
    def insert_edges(self, edges: pa.Table):
        """
        Insert edges with timestamps.
        
        Args:
            edges: Arrow table with columns including 'source', 'target', and 'timestamp'
        """
        if 'timestamp' not in edges.column_names:
            # Add current timestamp if not present
            current_time = datetime.now()
            timestamps = pa.array([current_time] * edges.num_rows)
            edges = edges.append_column('timestamp', timestamps)
        
        # Partition by time bucket
        timestamp_col = edges['timestamp']
        
        for i in range(edges.num_rows):
            timestamp = timestamp_col[i].as_py()
            bucket_id = self._get_bucket_id(timestamp)
            
            # Get single row as table
            row_table = edges.slice(i, 1)
            
            # Add to bucket
            if bucket_id in self.edges:
                # Concatenate with existing data
                self.edges[bucket_id] = pa.concat_tables([self.edges[bucket_id], row_table])
            else:
                self.edges[bucket_id] = row_table
        
        # Expire old buckets
        self._expire_old_buckets()
    
    def query(self, time_range: Optional[Tuple[datetime, datetime]] = None) -> Tuple[pa.Table, pa.Table]:
        """
        Query graph within time range.
        
        Args:
            time_range: Optional (start_time, end_time) tuple. If None, returns all data.
        
        Returns:
            Tuple of (vertices, edges) Arrow tables
        """
        if time_range is None:
            # Return all data
            time_range = (datetime.now() - self.ttl, datetime.now())
        
        start_time, end_time = time_range
        bucket_ids = self._get_bucket_range(start_time, end_time)
        
        # Gather vertices from relevant buckets
        vertex_tables = []
        for bucket_id in bucket_ids:
            if bucket_id in self.vertices:
                vertex_tables.append(self.vertices[bucket_id])
        
        # Gather edges from relevant buckets
        edge_tables = []
        for bucket_id in bucket_ids:
            if bucket_id in self.edges:
                edge_tables.append(self.edges[bucket_id])
        
        # Concatenate tables
        if vertex_tables:
            vertices = pa.concat_tables(vertex_tables)
            # Filter by exact time range
            if 'timestamp' in vertices.column_names:
                mask = pc.and_(
                    pc.greater_equal(vertices['timestamp'], pa.scalar(start_time)),
                    pc.less_equal(vertices['timestamp'], pa.scalar(end_time))
                )
                vertices = vertices.filter(mask)
        else:
            # Return empty table with proper schema
            vertices = pa.table({
                'id': pa.array([], type=pa.int64()),
                'timestamp': pa.array([], type=pa.timestamp('us'))
            })
        
        if edge_tables:
            edges = pa.concat_tables(edge_tables)
            # Filter by exact time range
            if 'timestamp' in edges.column_names:
                mask = pc.and_(
                    pc.greater_equal(edges['timestamp'], pa.scalar(start_time)),
                    pc.less_equal(edges['timestamp'], pa.scalar(end_time))
                )
                edges = edges.filter(mask)
        else:
            # Return empty table with proper schema
            edges = pa.table({
                'source': pa.array([], type=pa.int64()),
                'target': pa.array([], type=pa.int64()),
                'timestamp': pa.array([], type=pa.timestamp('us'))
            })
        
        return vertices, edges
    
    def _expire_old_buckets(self):
        """Remove buckets older than TTL."""
        current_time = datetime.now()
        expiry_time = current_time - self.ttl
        expiry_bucket = self._get_bucket_id(expiry_time)
        
        # Remove old vertex buckets
        expired_vertex_buckets = [b for b in self.vertices.keys() if b < expiry_bucket]
        for bucket_id in expired_vertex_buckets:
            del self.vertices[bucket_id]
        
        # Remove old edge buckets
        expired_edge_buckets = [b for b in self.edges.keys() if b < expiry_bucket]
        for bucket_id in expired_edge_buckets:
            del self.edges[bucket_id]
        
        # Clean up vertex index
        for vertex_id in list(self.vertex_index.keys()):
            self.vertex_index[vertex_id] = [b for b in self.vertex_index[vertex_id] if b >= expiry_bucket]
            if not self.vertex_index[vertex_id]:
                del self.vertex_index[vertex_id]
    
    def get_stats(self) -> Dict:
        """Get storage statistics."""
        total_vertices = sum(table.num_rows for table in self.vertices.values())
        total_edges = sum(table.num_rows for table in self.edges.values())
        
        return {
            'num_vertex_buckets': len(self.vertices),
            'num_edge_buckets': len(self.edges),
            'total_vertices': total_vertices,
            'total_edges': total_edges,
            'indexed_vertices': len(self.vertex_index)
        }

