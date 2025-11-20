#!/usr/bin/env python3
"""
Spark DataFrame Compatibility

DataFrame and Column wrappers that map to Sabot Stream API.

Design: Thin wrapper over Stream with Spark method names.
Performance: Delegates to Sabot's Cython operators.
"""

import logging
from typing import Optional, Any, List, Union

logger = logging.getLogger(__name__)


class Column:
    """
    Spark Column API.
    
    Wraps column expressions for Spark compatibility.
    Delegates to PyArrow compute functions.
    
    Example:
        col("amount") > 1000
        col("name").isNotNull()
    """
    
    def __init__(self, expr):
        """
        Initialize column.
        
        Args:
            expr: Column expression (string name or compute expression)
        """
        self._expr = expr
    
    # Comparison operators
    
    def __gt__(self, other):
        """Greater than."""
        from sabot import cyarrow as ca
        return Column(lambda b: ca.compute.greater(self._get_array(b), other))
    
    def __lt__(self, other):
        """Less than."""
        from sabot import cyarrow as ca
        return Column(lambda b: ca.compute.less(self._get_array(b), other))
    
    def __eq__(self, other):
        """Equal."""
        from sabot import cyarrow as ca
        return Column(lambda b: ca.compute.equal(self._get_array(b), other))
    
    def __ge__(self, other):
        """Greater than or equal."""
        from sabot import cyarrow as ca
        return Column(lambda b: ca.compute.greater_equal(self._get_array(b), other))
    
    def __le__(self, other):
        """Less than or equal."""
        from sabot import cyarrow as ca
        return Column(lambda b: ca.compute.less_equal(self._get_array(b), other))
    
    # Arithmetic operators
    
    def __add__(self, other):
        """Addition."""
        from sabot import cyarrow as ca
        return Column(lambda b: ca.compute.add(self._get_array(b), other))
    
    def __sub__(self, other):
        """Subtraction."""
        from sabot import cyarrow as ca
        return Column(lambda b: ca.compute.subtract(self._get_array(b), other))
    
    def __mul__(self, other):
        """Multiplication."""
        from sabot import cyarrow as ca
        return Column(lambda b: ca.compute.multiply(self._get_array(b), other))
    
    def __truediv__(self, other):
        """Division."""
        from sabot import cyarrow as ca
        return Column(lambda b: ca.compute.divide(self._get_array(b), other))
    
    # Spark DataFrame methods
    
    def alias(self, name: str):
        """Alias column."""
        # TODO: Implement aliasing
        return Column(self._expr)
    
    def cast(self, dataType):
        """Cast to different type."""
        from sabot import cyarrow as ca
        return Column(lambda b: ca.compute.cast(self._get_array(b), dataType))
    
    def isNull(self):
        """Check if null."""
        from sabot import cyarrow as ca
        return Column(lambda b: ca.compute.is_null(self._get_array(b)))
    
    def isNotNull(self):
        """Check if not null."""
        from sabot import cyarrow as ca
        return Column(lambda b: ca.compute.invert(ca.compute.is_null(self._get_array(b))))
    
    def _get_array(self, batch):
        """Get array from batch for this column."""
        if isinstance(self._expr, str):
            # Column name
            return batch.column(self._expr)
        elif callable(self._expr):
            # Computed expression
            return self._expr(batch)
        else:
            return self._expr


class DataFrame:
    """
    Spark DataFrame API.
    
    Wraps Sabot Stream with Spark-compatible methods.
    
    Example:
        df = spark.read.parquet("data.parquet")
        result = df.filter(df.amount > 1000).groupBy("customer_id").count()
    """
    
    def __init__(self, stream_or_table, session):
        """
        Initialize DataFrame.
        
        Args:
            stream_or_table: Sabot Stream or Arrow table
            session: Parent SparkSession
        """
        self._session = session
        
        # Convert to Stream if needed
        from sabot import cyarrow as pa  # Use Sabot's vendored Arrow
        from sabot.api.stream import Stream
        
        if isinstance(stream_or_table, Stream):
            # Already a Sabot Stream
            self._stream = stream_or_table
        elif isinstance(stream_or_table, pa.Table):
            # Arrow Table - convert to Stream
            self._stream = Stream.from_table(stream_or_table)
        elif hasattr(stream_or_table, 'to_batches'):
            # Has to_batches (Arrow table-like)
            self._stream = Stream(stream_or_table.to_batches())
        else:
            # Assume iterable of batches
            self._stream = Stream(stream_or_table)
    
    # Transformations (lazy)
    
    def select(self, *cols):
        """
        Select columns.
        
        Args:
            *cols: Column names or Column objects
            
        Returns:
            DataFrame
        """
        # Extract column names
        col_names = []
        for col in cols:
            if isinstance(col, str):
                col_names.append(col)
            elif isinstance(col, Column):
                # TODO: Handle column expressions
                col_names.append(str(col._expr))
            else:
                col_names.append(str(col))
        
        # Use Sabot's select
        new_stream = self._stream.select(*col_names)
        return DataFrame(new_stream, self._session)
    
    def filter(self, condition):
        """
        Filter rows.
        
        Args:
            condition: Filter condition (Column or string)
            
        Returns:
            DataFrame
        """
        if isinstance(condition, Column):
            # Column expression - evaluate it
            filter_func = lambda b: condition._get_array(b)
        elif isinstance(condition, str):
            # SQL-like string condition
            filter_func = lambda b: b.column(condition)
        else:
            # Callable
            filter_func = condition
        
        # Use Sabot's filter
        new_stream = self._stream.filter(filter_func)
        return DataFrame(new_stream, self._session)
    
    def where(self, condition):
        """Alias for filter."""
        return self.filter(condition)
    
    def groupBy(self, *cols):
        """
        Group by columns.
        
        Args:
            *cols: Column names to group by
            
        Returns:
            GroupedData
        """
        from .grouped import GroupedData
        
        # Use Sabot's group_by
        grouped_stream = self._stream.group_by(*cols)
        
        return GroupedData(grouped_stream, self._session, list(cols))
    
    def withColumn(self, colName: str, col):
        """
        Add or replace a column.
        
        Args:
            colName: Column name
            col: Column expression
            
        Returns:
            DataFrame
        """
        # Use select to add/replace column
        from .dataframe import Column
        from sabot import cyarrow as ca; pc = ca.compute  # Sabot vendored Arrow
        
        def with_column_stream():
            for batch in self._stream:
                # Evaluate the column expression
                if isinstance(col, Column):
                    new_array = col._get_array(batch)
                else:
                    new_array = col
                
                # Check if column exists
                from sabot import cyarrow as pa  # Sabot vendored Arrow
                if colName in batch.schema.names:
                    # Replace existing column
                    idx = batch.schema.names.index(colName)
                    arrays = [batch.column(i) if i != idx else new_array 
                             for i in range(batch.num_columns)]
                    names = batch.schema.names
                else:
                    # Add new column
                    arrays = [batch.column(i) for i in range(batch.num_columns)]
                    arrays.append(new_array)
                    names = list(batch.schema.names) + [colName]
                
                # Create new batch
                new_batch = pa.record_batch(arrays, names=names)
                yield new_batch
        
        from sabot.api.stream import Stream
        return DataFrame(Stream(with_column_stream()), self._session)
    
    def withColumnRenamed(self, existing: str, new: str):
        """
        Rename a column.
        
        Args:
            existing: Existing column name
            new: New column name
            
        Returns:
            DataFrame
        """
        def renamed_stream():
            for batch in self._stream:
                from sabot import cyarrow as pa  # Sabot vendored Arrow
                # Rename in schema
                names = list(batch.schema.names)
                if existing in names:
                    idx = names.index(existing)
                    names[idx] = new
                
                # Create new batch with renamed schema
                new_batch = pa.record_batch(
                    [batch.column(i) for i in range(batch.num_columns)],
                    names=names
                )
                yield new_batch
        
        from sabot.api.stream import Stream
        return DataFrame(Stream(renamed_stream()), self._session)
    
    def drop(self, *cols):
        """
        Drop columns.
        
        Args:
            *cols: Column names to drop
            
        Returns:
            DataFrame
        """
        cols_to_drop = set(cols)
        
        def drop_stream():
            for batch in self._stream:
                from sabot import cyarrow as pa  # Sabot vendored Arrow
                # Keep columns not in drop list
                keep_indices = [i for i, name in enumerate(batch.schema.names) 
                               if name not in cols_to_drop]
                keep_names = [batch.schema.names[i] for i in keep_indices]
                keep_arrays = [batch.column(i) for i in keep_indices]
                
                new_batch = pa.record_batch(keep_arrays, names=keep_names)
                yield new_batch
        
        from sabot.api.stream import Stream
        return DataFrame(Stream(drop_stream()), self._session)
    
    def join(self, other, on, how: str = 'inner'):
        """
        Join with another DataFrame.
        
        Args:
            other: Right DataFrame
            on: Join keys (string or list)
            how: Join type ('inner', 'left', 'right', 'outer')
            
        Returns:
            DataFrame
        """
        # Convert on to lists
        if isinstance(on, str):
            left_keys = [on]
            right_keys = [on]
        elif isinstance(on, list):
            left_keys = on
            right_keys = on
        else:
            # TODO: Handle column expressions
            left_keys = [str(on)]
            right_keys = [str(on)]
        
        # Use Sabot's join
        new_stream = self._stream.join(
            other._stream,
            left_keys=left_keys,
            right_keys=right_keys,
            how=how
        )
        
        return DataFrame(new_stream, self._session)
    
    def orderBy(self, *cols, ascending=True):
        """
        Sort DataFrame by columns.
        
        Args:
            *cols: Columns to sort by
            ascending: Sort order (bool or list of bool)
            
        Returns:
            DataFrame
        """
        # Extract column names
        col_names = []
        for c in cols:
            if isinstance(c, str):
                col_names.append(c)
            elif isinstance(c, Column):
                col_names.append(str(c._expr))
            else:
                col_names.append(str(c))
        
        # Use Arrow sort
        def sorted_stream():
            from sabot import cyarrow as pa  # Sabot vendored Arrow
            from sabot import cyarrow as ca; pc = ca.compute  # Sabot vendored Arrow
            
            batches = list(self._stream)
            if not batches:
                return
            
            # Combine to table
            table = pa.Table.from_batches(batches)
            
            # Prepare sort keys
            if isinstance(ascending, bool):
                sort_keys = [(col, "ascending" if ascending else "descending") for col in col_names]
            else:
                sort_keys = [(col, "ascending" if asc else "descending") 
                            for col, asc in zip(col_names, ascending)]
            
            # Sort
            sorted_table = table.sort_by(sort_keys)
            
            # Yield batches
            for batch in sorted_table.to_batches():
                yield batch
        
        from sabot.api.stream import Stream
        return DataFrame(Stream(sorted_stream()), self._session)
    
    def limit(self, num: int):
        """
        Limit number of rows.
        
        Args:
            num: Number of rows
            
        Returns:
            DataFrame
        """
        def limited_stream():
            count = 0
            for batch in self._stream:
                if count >= num:
                    break
                
                rows_needed = num - count
                if batch.num_rows <= rows_needed:
                    # Take whole batch
                    yield batch
                    count += batch.num_rows
                else:
                    # Take partial batch
                    yield batch.slice(0, rows_needed)
                    count += rows_needed
                    break
        
        from sabot.api.stream import Stream
        return DataFrame(Stream(limited_stream()), self._session)
    
    def unionByName(self, other, allowMissingColumns=False):
        """
        Union with another DataFrame by column names.
        
        Args:
            other: DataFrame to union with
            allowMissingColumns: Allow schema differences
            
        Returns:
            DataFrame
        """
        def union_stream():
            from sabot import cyarrow as pa  # Sabot vendored Arrow
            
            # Get schemas
            self_batches = list(self._stream)
            other_batches = list(other._stream)
            
            if not self_batches or not other_batches:
                # Yield all batches from both
                for b in self_batches:
                    yield b
                for b in other_batches:
                    yield b
                return
            
            # Get schemas
            self_schema = self_batches[0].schema
            other_schema = other_batches[0].schema
            
            # Union schemas (take union of column names)
            all_columns = set(self_schema.names) | set(other_schema.names)
            
            # Yield self batches (add null columns if needed)
            for batch in self_batches:
                if allowMissingColumns and set(batch.schema.names) != all_columns:
                    # Add missing columns as nulls
                    arrays = [batch.column(name) if name in batch.schema.names 
                             else pa.nulls(batch.num_rows) 
                             for name in sorted(all_columns)]
                    new_batch = pa.record_batch(arrays, names=sorted(all_columns))
                    yield new_batch
                else:
                    yield batch
            
            # Yield other batches (add null columns if needed)
            for batch in other_batches:
                if allowMissingColumns and set(batch.schema.names) != all_columns:
                    arrays = [batch.column(name) if name in batch.schema.names 
                             else pa.nulls(batch.num_rows) 
                             for name in sorted(all_columns)]
                    new_batch = pa.record_batch(arrays, names=sorted(all_columns))
                    yield new_batch
                else:
                    yield batch
        
        from sabot.api.stream import Stream
        return DataFrame(Stream(union_stream()), self._session)
    
    def sample(self, withReplacement=False, fraction=None, seed=None):
        """
        Sample rows.
        
        Args:
            withReplacement: Sample with replacement
            fraction: Fraction to sample (0.0-1.0)
            seed: Random seed
            
        Returns:
            DataFrame
        """
        import random
        if seed is not None:
            random.seed(seed)
        
        def sample_stream():
            for batch in self._stream:
                if fraction is None or fraction >= 1.0:
                    yield batch
                else:
                    # Sample rows
                    import numpy as np
                    if seed is not None:
                        np.random.seed(seed)
                    
                    num_samples = int(batch.num_rows * fraction)
                    if withReplacement:
                        indices = np.random.choice(batch.num_rows, num_samples, replace=True)
                    else:
                        indices = np.random.choice(batch.num_rows, num_samples, replace=False)
                    
                    # Take sampled rows
                    sampled = batch.take(sorted(indices))
                    if sampled.num_rows > 0:
                        yield sampled
        
        from sabot.api.stream import Stream
        return DataFrame(Stream(sample_stream()), self._session)
    
    def distinct(self):
        """Get distinct rows."""
        new_stream = self._stream.distinct()
        return DataFrame(new_stream, self._session)
    
    def repartition(self, numPartitions: int):
        """
        Repartition DataFrame to specified number of partitions.
        
        Args:
            numPartitions: Target number of partitions
            
        Returns:
            DataFrame
        """
        def repartitioned_stream():
            from sabot import cyarrow as pa  # Sabot vendored Arrow
            
            # Collect all batches
            batches = list(self._stream)
            if not batches:
                return
            
            # Combine to table
            table = pa.Table.from_batches(batches)
            
            # Calculate rows per partition
            rows_per_partition = max(1, table.num_rows // numPartitions)
            
            # Yield batches of target size
            for i in range(0, table.num_rows, rows_per_partition):
                end = min(i + rows_per_partition, table.num_rows)
                partition_table = table.slice(i, end - i)
                for batch in partition_table.to_batches():
                    yield batch
        
        from sabot.api.stream import Stream
        return DataFrame(Stream(repartitioned_stream()), self._session)
    
    def coalesce(self, numPartitions: int):
        """
        Reduce number of partitions (optimization of repartition).
        
        Args:
            numPartitions: Target number of partitions
            
        Returns:
            DataFrame
        """
        # For now, same as repartition (optimization would avoid shuffle)
        return self.repartition(numPartitions)
    
    def cache(self):
        """
        Cache DataFrame in memory.
        
        Returns:
            DataFrame (self)
        """
        # Materialize the stream into memory
        self._cached_batches = list(self._stream)
        
        # Create new stream from cached batches
        def cached_stream():
            for batch in self._cached_batches:
                yield batch
        
        from sabot.api.stream import Stream
        self._stream = Stream(cached_stream())
        return self
    
    def persist(self, storageLevel=None):
        """
        Persist DataFrame (alias for cache).
        
        Args:
            storageLevel: Storage level (ignored - always memory)
            
        Returns:
            DataFrame (self)
        """
        return self.cache()
    
    def unpersist(self):
        """
        Unpersist DataFrame.
        
        Returns:
            DataFrame (self)
        """
        if hasattr(self, '_cached_batches'):
            delattr(self, '_cached_batches')
        return self
    
    # Actions (trigger execution)
    
    def collect(self):
        """
        Collect all rows to driver.
        
        Returns:
            List of rows
        """
        # Materialize stream
        results = []
        for batch in self._stream:
            # Convert to list of dicts
            results.extend(batch.to_pylist())
        return results
    
    def count(self):
        """
        Count rows.
        
        Returns:
            Row count
        """
        total = 0
        for batch in self._stream:
            total += batch.num_rows
        return total
    
    def show(self, n: int = 20, truncate: bool = True):
        """
        Display first n rows.
        
        Args:
            n: Number of rows to show
            truncate: Truncate long strings
        """
        rows = []
        count = 0
        
        for batch in self._stream:
            batch_list = batch.to_pylist()
            for row in batch_list:
                rows.append(row)
                count += 1
                if count >= n:
                    break
            if count >= n:
                break
        
        # Print table
        if rows:
            # Get column names
            cols = list(rows[0].keys())
            
            # Print header
            header = " | ".join(cols)
            print(header)
            print("-" * len(header))
            
            # Print rows
            for row in rows[:n]:
                values = [str(row.get(col, '')) for col in cols]
                if truncate:
                    values = [v[:20] if len(v) > 20 else v for v in values]
                print(" | ".join(values))
        
        print(f"\nShowing {min(len(rows), n)} rows")
    
    def take(self, num: int):
        """Take first num rows."""
        rows = []
        count = 0
        
        for batch in self._stream:
            batch_list = batch.to_pylist()
            for row in batch_list:
                rows.append(row)
                count += 1
                if count >= num:
                    break
            if count >= num:
                break
        
        return rows[:num]
    
    def first(self):
        """Get first row."""
        results = self.take(1)
        return results[0] if results else None
    
    # Persistence
    
    def cache(self):
        """Cache DataFrame in memory."""
        # TODO: Use Sabot's checkpointing/caching
        logger.warning("cache() not yet fully implemented, returning self")
        return self
    
    def persist(self, storageLevel=None):
        """Persist with storage level."""
        # TODO: Implement with Sabot's checkpointing
        logger.warning("persist() not yet fully implemented, returning self")
        return self
    
    def unpersist(self):
        """Remove from cache."""
        return self
    
    # Write operations
    
    @property
    def write(self):
        """Get DataFrameWriter."""
        from .writer import DataFrameWriter
        return DataFrameWriter(self)
    
    # Column access (Spark syntax)
    
    def __getitem__(self, item):
        """Access column: df["col_name"]."""
        return Column(item)
    
    def __getattr__(self, name):
        """Access column: df.col_name."""
        if name.startswith('_'):
            # Internal attribute
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")
        return Column(name)

