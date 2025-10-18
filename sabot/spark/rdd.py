#!/usr/bin/env python3
"""
Spark RDD Compatibility

RDD wrapper over Sabot Stream for legacy Spark code.
"""

import logging

logger = logging.getLogger(__name__)


class RDD:
    """
    RDD API (Spark compatibility).
    
    Wraps Sabot Stream with row-based operations.
    
    Note: RDD is row-based, Stream is batch-based.
    This wrapper converts between the two (adds overhead).
    
    Recommendation: Use DataFrame API instead for better performance.
    
    Example:
        rdd = sc.parallelize([1, 2, 3, 4, 5])
        result = rdd.map(lambda x: x * 2).filter(lambda x: x > 5).collect()
    """
    
    def __init__(self, stream, context):
        """
        Initialize RDD.
        
        Args:
            stream: Sabot Stream
            context: SparkContext
        """
        self._stream = stream
        self._context = context
    
    # Transformations
    
    def map(self, func):
        """Map function over elements."""
        # Convert row function to batch function
        def batch_func(batch):
            # Apply func to each row
            rows = batch.to_pylist()
            results = [func(row) for row in rows]
            
            # Convert back to batch
            import pyarrow as pa
            return pa.RecordBatch.from_pylist(results) if results else batch
        
        new_stream = self._stream.map(batch_func)
        return RDD(new_stream, self._context)
    
    def flatMap(self, func):
        """FlatMap function over elements."""
        def batch_func(batch):
            rows = batch.to_pylist()
            all_results = []
            for row in rows:
                results = func(row)
                all_results.extend(results)
            
            import pyarrow as pa
            return pa.RecordBatch.from_pylist(all_results) if all_results else batch
        
        new_stream = self._stream.flat_map(batch_func)
        return RDD(new_stream, self._context)
    
    def filter(self, func):
        """Filter elements."""
        def batch_func(batch):
            rows = batch.to_pylist()
            filtered = [row for row in rows if func(row)]
            
            import pyarrow as pa
            return pa.RecordBatch.from_pylist(filtered) if filtered else batch
        
        new_stream = self._stream.filter(batch_func)
        return RDD(new_stream, self._context)
    
    def reduceByKey(self, func):
        """Reduce by key."""
        # Assume RDD contains (key, value) tuples
        # Convert to grouped aggregation
        logger.warning("reduceByKey not yet fully implemented")
        return self
    
    def groupByKey(self):
        """Group by key."""
        logger.warning("groupByKey not yet fully implemented")
        return self
    
    def join(self, other, numPartitions=None):
        """Join with another RDD."""
        # Use Stream join
        new_stream = self._stream.join(
            other._stream,
            left_keys=['_1'],  # Assume key-value pairs
            right_keys=['_1']
        )
        return RDD(new_stream, self._context)
    
    # Actions
    
    def collect(self):
        """Collect all elements."""
        results = []
        for batch in self._stream:
            results.extend(batch.to_pylist())
        return results
    
    def count(self):
        """Count elements."""
        total = 0
        for batch in self._stream:
            total += batch.num_rows
        return total
    
    def take(self, num: int):
        """Take first num elements."""
        results = []
        count = 0
        
        for batch in self._stream:
            batch_list = batch.to_pylist()
            for row in batch_list:
                results.append(row)
                count += 1
                if count >= num:
                    break
            if count >= num:
                break
        
        return results[:num]
    
    def first(self):
        """Get first element."""
        results = self.take(1)
        return results[0] if results else None
    
    def foreach(self, func):
        """Apply function to each element (side effects)."""
        for batch in self._stream:
            for row in batch.to_pylist():
                func(row)

