# cython: language_level=3, boundscheck=False, wraparound=False
"""
SabotCypher Streaming Python Bindings - Cython Interface

High-performance streaming graph queries with Cython/C++.
"""

from libcpp.string cimport string as cpp_string
from libcpp.memory cimport shared_ptr
from libcpp.vector cimport vector
from libcpp.pair cimport pair
from libcpp cimport bool as cpp_bool
from libcpp.functional cimport function
from libc.stdint cimport uint64_t, int64_t

# Arrow C++ bindings
from pyarrow.lib cimport *
import pyarrow as pa
from datetime import datetime, timedelta

# Forward declarations for C++ types
cdef extern from "sabot_cypher/streaming/temporal_graph_store.h" namespace "sabot_cypher::streaming":
    # Time types (using milliseconds)
    ctypedef int64_t BucketID
    
    # Stats structure
    cdef struct Stats "sabot_cypher::streaming::TemporalGraphStore::Stats":
        int64_t num_vertex_buckets
        int64_t num_edge_buckets
        int64_t total_vertices
        int64_t total_edges
        int64_t indexed_vertices
    
    # TemporalGraphStore class
    cdef cppclass TemporalGraphStore:
        TemporalGraphStore(int64_t ttl_ms, int64_t bucket_size_ms) except +
        CStatus InsertVertices(shared_ptr[CTable] vertices) except +
        CStatus InsertEdges(shared_ptr[CTable] edges) except +
        CResult[pair[shared_ptr[CTable], shared_ptr[CTable]]] Query(int64_t start_time_ms, int64_t end_time_ms) except +
        int64_t ExpireOldBuckets() except +
        Stats GetStats() except +

cdef extern from "sabot_cypher/streaming/streaming_processor.h" namespace "sabot_cypher::streaming":
    # Processor stats
    cdef struct ProcessorStats "sabot_cypher::streaming::StreamingGraphProcessor::Stats":
        Stats store_stats
        int64_t window_count
        int64_t continuous_queries
        int64_t window_size_ms
        int64_t slide_interval_ms
    
    # StreamingGraphProcessor class
    cdef cppclass StreamingGraphProcessor:
        StreamingGraphProcessor(int64_t window_size_ms, int64_t slide_interval_ms, int64_t ttl_ms) except +
        CStatus IngestBatch(shared_ptr[CTable] vertices, shared_ptr[CTable] edges) except +
        CResult[shared_ptr[CTable]] QueryCurrentWindow(const cpp_string& query) except +
        CResult[pair[shared_ptr[CTable], shared_ptr[CTable]]] GetCurrentGraph() except +
        ProcessorStats GetStats() except +

# Python wrapper for TemporalGraphStore
cdef class PyTemporalGraphStore:
    cdef shared_ptr[TemporalGraphStore] _store
    
    def __cinit__(self, ttl_hours=1, bucket_size_minutes=5):
        """
        Create temporal graph store.
        
        Args:
            ttl_hours: Time-to-live in hours
            bucket_size_minutes: Bucket size in minutes
        """
        cdef int64_t ttl_ms = ttl_hours * 3600 * 1000
        cdef int64_t bucket_size_ms = bucket_size_minutes * 60 * 1000
        self._store = shared_ptr[TemporalGraphStore](new TemporalGraphStore(ttl_ms, bucket_size_ms))
    
    def insert_vertices(self, vertices: pa.Table):
        """Insert vertices with timestamps."""
        cdef shared_ptr[CTable] c_vertices = pyarrow_unwrap_table(vertices)
        cdef CStatus status = self._store.get().InsertVertices(c_vertices)
        if not status.ok():
            raise RuntimeError(status.ToString().decode('utf-8'))
    
    def insert_edges(self, edges: pa.Table):
        """Insert edges with timestamps."""
        cdef shared_ptr[CTable] c_edges = pyarrow_unwrap_table(edges)
        cdef CStatus status = self._store.get().InsertEdges(c_edges)
        if not status.ok():
            raise RuntimeError(status.ToString().decode('utf-8'))
    
    def query(self, start_time: datetime, end_time: datetime):
        """Query graph within time range."""
        # Convert datetime to milliseconds since epoch
        cdef int64_t start_ms = int(start_time.timestamp() * 1000)
        cdef int64_t end_ms = int(end_time.timestamp() * 1000)
        
        cdef CResult[pair[shared_ptr[CTable], shared_ptr[CTable]]] result = self._store.get().Query(start_ms, end_ms)
        if not result.ok():
            raise RuntimeError(result.status().ToString().decode('utf-8'))
        
        cdef pair[shared_ptr[CTable], shared_ptr[CTable]] graph = result.ValueOrDie()
        vertices = pyarrow_wrap_table(graph.first)
        edges = pyarrow_wrap_table(graph.second)
        
        return vertices, edges
    
    def expire_old_buckets(self):
        """Expire old buckets beyond TTL."""
        return self._store.get().ExpireOldBuckets()
    
    def get_stats(self):
        """Get storage statistics."""
        cdef Stats stats = self._store.get().GetStats()
        return {
            'num_vertex_buckets': stats.num_vertex_buckets,
            'num_edge_buckets': stats.num_edge_buckets,
            'total_vertices': stats.total_vertices,
            'total_edges': stats.total_edges,
            'indexed_vertices': stats.indexed_vertices
        }

# Python wrapper for StreamingGraphProcessor
cdef class PyStreamingGraphProcessor:
    cdef shared_ptr[StreamingGraphProcessor] _processor
    cdef object _callbacks  # Store Python callbacks
    
    def __cinit__(self, window_size_minutes=5, slide_interval_seconds=30, ttl_hours=1):
        """
        Create streaming graph processor.
        
        Args:
            window_size_minutes: Window size in minutes
            slide_interval_seconds: Slide interval in seconds
            ttl_hours: Time-to-live in hours
        """
        cdef int64_t window_size_ms = window_size_minutes * 60 * 1000
        cdef int64_t slide_interval_ms = slide_interval_seconds * 1000
        cdef int64_t ttl_ms = ttl_hours * 3600 * 1000
        
        self._processor = shared_ptr[StreamingGraphProcessor](
            new StreamingGraphProcessor(window_size_ms, slide_interval_ms, ttl_ms)
        )
        self._callbacks = {}
    
    def ingest_batch(self, vertices: pa.Table = None, edges: pa.Table = None):
        """
        Ingest a batch of vertices and edges.
        
        Args:
            vertices: Arrow table of vertices (optional)
            edges: Arrow table of edges (optional)
        """
        cdef shared_ptr[CTable] c_vertices
        cdef shared_ptr[CTable] c_edges
        
        if vertices is not None:
            c_vertices = pyarrow_unwrap_table(vertices)
        else:
            c_vertices = shared_ptr[CTable]()
        
        if edges is not None:
            c_edges = pyarrow_unwrap_table(edges)
        else:
            c_edges = shared_ptr[CTable]()
        
        cdef CStatus status = self._processor.get().IngestBatch(c_vertices, c_edges)
        if not status.ok():
            raise RuntimeError(status.ToString().decode('utf-8'))
    
    def register_continuous_query(self, query_id: str, query: str, callback):
        """
        Register a continuous query.
        
        Args:
            query_id: Unique identifier
            query: Cypher query string
            callback: Python function to call with results
        """
        # Store callback in Python dict
        self._callbacks[query_id] = callback
        
        # Note: Full callback integration requires C++ lambda to Python callback bridge
        # For now, store in Python and call manually
    
    def query_current_window(self, query: str) -> pa.Table:
        """Execute query on current window."""
        cdef cpp_string c_query = query.encode('utf-8')
        cdef CResult[shared_ptr[CTable]] result = self._processor.get().QueryCurrentWindow(c_query)
        
        if not result.ok():
            raise RuntimeError(result.status().ToString().decode('utf-8'))
        
        return pyarrow_wrap_table(result.ValueOrDie())
    
    def get_current_graph(self):
        """Get current windowed graph."""
        cdef CResult[pair[shared_ptr[CTable], shared_ptr[CTable]]] result = self._processor.get().GetCurrentGraph()
        
        if not result.ok():
            raise RuntimeError(result.status().ToString().decode('utf-8'))
        
        cdef pair[shared_ptr[CTable], shared_ptr[CTable]] graph = result.ValueOrDie()
        vertices = pyarrow_wrap_table(graph.first)
        edges = pyarrow_wrap_table(graph.second)
        
        return vertices, edges
    
    def get_stats(self):
        """Get processor statistics."""
        cdef ProcessorStats stats = self._processor.get().GetStats()
        
        return {
            'store': {
                'num_vertex_buckets': stats.store_stats.num_vertex_buckets,
                'num_edge_buckets': stats.store_stats.num_edge_buckets,
                'total_vertices': stats.store_stats.total_vertices,
                'total_edges': stats.store_stats.total_edges,
                'indexed_vertices': stats.store_stats.indexed_vertices
            },
            'window': {
                'window_count': stats.window_count,
                'window_size_ms': stats.window_size_ms,
                'slide_interval_ms': stats.slide_interval_ms
            },
            'continuous_queries': stats.continuous_queries
        }

# Convenience function
def create_streaming_processor(window_size_minutes=5, slide_interval_seconds=30, ttl_hours=1):
    """
    Create a streaming graph processor.
    
    Args:
        window_size_minutes: Window size in minutes
        slide_interval_seconds: Slide interval in seconds
        ttl_hours: Time-to-live in hours
    
    Returns:
        PyStreamingGraphProcessor instance
    """
    return PyStreamingGraphProcessor(window_size_minutes, slide_interval_seconds, ttl_hours)

# Version and status
__version__ = "0.1.0"
__status__ = "Streaming graph queries - C++/Cython implementation"

