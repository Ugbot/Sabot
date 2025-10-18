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
        if hasattr(stream_or_table, 'filter'):
            # Already a Stream
            self._stream = stream_or_table
        else:
            # Convert table to Stream
            self._stream = session._engine.stream.from_table(stream_or_table)
    
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
            # Column expression
            filter_func = condition._expr if callable(condition._expr) else lambda b: condition._expr
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
        Sort DataFrame.
        
        Args:
            *cols: Columns to sort by
            ascending: Sort order
            
        Returns:
            DataFrame
        """
        # TODO: Implement orderBy via SQL or custom operator
        logger.warning("orderBy not yet fully implemented, returning self")
        return self
    
    def limit(self, num: int):
        """
        Limit number of rows.
        
        Args:
            num: Number of rows
            
        Returns:
            DataFrame
        """
        # TODO: Implement limit operator
        logger.warning("limit not yet fully implemented, returning self")
        return self
    
    def distinct(self):
        """Get distinct rows."""
        new_stream = self._stream.distinct()
        return DataFrame(new_stream, self._session)
    
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

