#!/usr/bin/env python3
"""
Spark GroupedData Compatibility

GroupedData wrapper for Spark API compatibility.
"""

import logging

logger = logging.getLogger(__name__)


class GroupedData:
    """
    Grouped data for aggregations (Spark API).
    
    Wraps Sabot's grouped stream.
    
    Example:
        grouped = df.groupBy("customer_id")
        result = grouped.agg({"amount": "sum", "quantity": "avg"})
    """
    
    def __init__(self, grouped_stream, session, group_keys):
        """
        Initialize grouped data.
        
        Args:
            grouped_stream: Sabot grouped stream
            session: Parent SparkSession
            group_keys: Grouping keys
        """
        self._grouped_stream = grouped_stream
        self._session = session
        self._group_keys = group_keys
    
    def agg(self, *exprs, **kwargs):
        """
        Compute aggregations.
        
        Args:
            *exprs: Aggregation expressions
            **kwargs: Named aggregations (column=function)
            
        Returns:
            DataFrame with aggregated results
        """
        from .dataframe import DataFrame
        
        # Build aggregations dict
        aggregations = {}
        
        # From kwargs
        for col, func in kwargs.items():
            aggregations[col] = (col, func)
        
        # TODO: Handle exprs (Column objects)
        
        # Use Sabot's aggregate
        result_stream = self._grouped_stream.aggregate(aggregations)
        
        return DataFrame(result_stream, self._session)
    
    def count(self):
        """Count rows in each group."""
        from .dataframe import DataFrame
        
        # Count aggregation
        aggregations = {'count': ('*', 'count')}
        result_stream = self._grouped_stream.aggregate(aggregations)
        
        return DataFrame(result_stream, self._session)
    
    def sum(self, *cols):
        """Sum columns."""
        aggregations = {col: (col, 'sum') for col in cols}
        return self.agg(**aggregations)
    
    def avg(self, *cols):
        """Average columns."""
        aggregations = {col: (col, 'avg') for col in cols}
        return self.agg(**aggregations)
    
    def min(self, *cols):
        """Minimum of columns."""
        aggregations = {col: (col, 'min') for col in cols}
        return self.agg(**aggregations)
    
    def max(self, *cols):
        """Maximum of columns."""
        aggregations = {col: (col, 'max') for col in cols}
        return self.agg(**aggregations)

