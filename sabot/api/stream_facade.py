#!/usr/bin/env python3
"""
Stream API Facade

Unified Stream API that wraps existing Stream implementation.
Provides integration with unified Sabot engine.
"""

import logging
from typing import Optional, Any, Callable, Iterable

logger = logging.getLogger(__name__)


class StreamAPI:
    """
    Stream processing API facade.
    
    Wraps existing Stream implementation and integrates with unified engine.
    Provides access to stream sources and operators through engine context.
    
    Example:
        engine = Sabot()
        stream = engine.stream.from_kafka('topic').filter(lambda b: b.column('x') > 0)
    """
    
    def __init__(self, engine):
        """
        Initialize Stream API.
        
        Args:
            engine: Parent Sabot engine instance
        """
        self.engine = engine
        self._stream_class = None
    
    def _get_stream_class(self):
        """Lazy-load Stream class."""
        if self._stream_class is None:
            from sabot.api.stream import Stream
            self._stream_class = Stream
        return self._stream_class
    
    # Stream sources
    
    def from_kafka(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        **kwargs
    ):
        """
        Create stream from Kafka topic.
        
        Args:
            bootstrap_servers: Kafka brokers
            topic: Kafka topic name
            group_id: Consumer group ID
            **kwargs: Additional Kafka consumer options
            
        Returns:
            Stream: Kafka stream
            
        Example:
            stream = engine.stream.from_kafka('localhost:9092', 'events', 'my-group')
        """
        Stream = self._get_stream_class()
        return Stream.from_kafka(bootstrap_servers, topic, group_id, **kwargs)
    
    def from_parquet(self, path: str, **kwargs):
        """
        Create stream from Parquet file(s).
        
        Args:
            path: File path or glob pattern
            **kwargs: Parquet read options
            
        Returns:
            Stream: Parquet stream
        """
        Stream = self._get_stream_class()
        return Stream.from_parquet(path, **kwargs)
    
    def from_csv(self, path: str, **kwargs):
        """
        Create stream from CSV file(s).
        
        Args:
            path: File path or glob pattern
            **kwargs: CSV read options
            
        Returns:
            Stream: CSV stream
        """
        Stream = self._get_stream_class()
        return Stream.from_csv(path, **kwargs)
    
    def from_table(self, table, batch_size: int = 100000):
        """
        Create stream from Arrow table.
        
        Args:
            table: Arrow table or compatible object
            batch_size: Batch size for streaming
            
        Returns:
            Stream: Table stream
        """
        Stream = self._get_stream_class()
        return Stream.from_table(table, batch_size=batch_size)
    
    def from_batches(self, batches: Iterable):
        """
        Create stream from iterable of batches.
        
        Args:
            batches: Iterable of Arrow RecordBatch objects
            
        Returns:
            Stream: Batch stream
        """
        Stream = self._get_stream_class()
        return Stream.from_batches(batches)
    
    def from_uri(self, uri: str, **kwargs):
        """
        Create stream from URI (auto-detects format).
        
        Args:
            uri: Data URI (file://, s3://, postgres://, etc.)
            **kwargs: Format-specific options
            
        Returns:
            Stream: URI stream
            
        Example:
            engine.stream.from_uri('s3://bucket/data/*.parquet')
            engine.stream.from_uri('postgres://user:pass@host/db?table=events')
        """
        from sabot.connectors.uri import from_uri
        
        source = from_uri(uri, **kwargs)
        Stream = self._get_stream_class()
        return Stream.from_batches(source)
    
    def range(self, start: int, end: int, step: int = 1):
        """
        Create stream of integers in range.
        
        Args:
            start: Range start
            end: Range end (exclusive)
            step: Step size
            
        Returns:
            Stream: Range stream
            
        Example:
            numbers = engine.stream.range(0, 1000000)
        """
        Stream = self._get_stream_class()
        return Stream.range(start, end, step)
    
    def parallelize(self, data: Iterable, num_partitions: Optional[int] = None):
        """
        Create stream from Python iterable (parallel if distributed).
        
        Args:
            data: Python iterable
            num_partitions: Number of partitions (auto-detect if None)
            
        Returns:
            Stream: Parallelized stream
        """
        Stream = self._get_stream_class()
        
        # Convert to Arrow batches
        import pyarrow as pa
        
        # Chunk data into batches
        batch_size = 10000
        batches = []
        
        chunk = []
        for item in data:
            chunk.append(item)
            if len(chunk) >= batch_size:
                # Convert chunk to RecordBatch
                # Assume items are dicts
                if chunk and isinstance(chunk[0], dict):
                    batch = pa.RecordBatch.from_pylist(chunk)
                    batches.append(batch)
                chunk = []
        
        if chunk:
            if chunk and isinstance(chunk[0], dict):
                batch = pa.RecordBatch.from_pylist(chunk)
                batches.append(batch)
        
        return Stream.from_batches(batches)
    
    def get_stats(self) -> dict:
        """
        Get stream processing statistics.
        
        Returns:
            Dict with stream stats
        """
        return {
            'api': 'stream',
            'operators_available': len(self.engine._operator_registry.list_operators()) if self.engine._operator_registry else 0
        }

