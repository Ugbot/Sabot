#!/usr/bin/env python3
"""
Spark Functions Compatibility

Spark SQL functions that map to PyArrow compute.
Uses Cython implementation when available for maximum performance.
"""

import logging

logger = logging.getLogger(__name__)

# Use Python implementation with cyarrow acceleration
CYTHON_AVAILABLE = False
logger.info("Using Python implementation with cyarrow acceleration")


def col(column_name: str):
    """
    Reference a column by name.
    
    Args:
        column_name: Column name
        
    Returns:
        Column reference
    """
    from .dataframe import Column
    return Column(column_name)


def lit(value):
    """
    Create literal value column.
    
    Args:
        value: Literal value
        
    Returns:
        Column with literal
    """
    from .dataframe import Column
    return Column(value)


# Aggregation functions

def sum(col):
    """Sum aggregation."""
    from .dataframe import Column
    if isinstance(col, str):
        col = Column(col)
    
    # Create aggregation expression
    def sum_expr(batch):
        from sabot import cyarrow as ca
        return ca.compute.sum(col._get_array(batch))
    
    return Column(sum_expr)


def avg(col):
    """Average aggregation."""
    from .dataframe import Column
    if isinstance(col, str):
        col = Column(col)
    
    # Create aggregation expression
    def avg_expr(batch):
        from sabot import cyarrow as ca
        return ca.compute.mean(col._get_array(batch))
    
    return Column(avg_expr)


def count(col):
    """Count aggregation."""
    from .dataframe import Column
    if isinstance(col, str):
        col = Column(col)
    
    # Create aggregation expression
    def count_expr(batch):
        from sabot import cyarrow as ca
        if col._expr == "*":
            return ca.scalar(batch.num_rows)
        else:
            return ca.compute.sum(ca.compute.if_else(ca.compute.is_null(col._get_array(batch)), ca.scalar(0), ca.scalar(1)))
    
    return Column(count_expr)


def min(col):
    """Minimum aggregation."""
    from .dataframe import Column
    if isinstance(col, str):
        col = Column(col)
    
    # Create aggregation expression
    def min_expr(batch):
        from sabot import cyarrow as ca
        return ca.compute.min(col._get_array(batch))
    
    return Column(min_expr)


def max(col):
    """Maximum aggregation."""
    from .dataframe import Column
    if isinstance(col, str):
        col = Column(col)
    
    # Create aggregation expression
    def max_expr(batch):
        from sabot import cyarrow as ca
        return ca.compute.max(col._get_array(batch))
    
    return Column(max_expr)


# Conditional functions

def when(condition, value):
    """Conditional expression."""
    from .dataframe import Column
    from sabot import cyarrow as ca
    
    # Create conditional expression
    def when_expr(batch):
        if isinstance(condition, Column):
            cond_array = condition._get_array(batch)
        else:
            cond_array = condition
        
        if isinstance(value, Column):
            value_array = value._get_array(batch)
        else:
            value_array = ca.scalar(value)
        
        return ca.compute.if_else(cond_array, value_array, ca.scalar(None))
    
    return Column(when_expr)


def coalesce(*cols):
    """Return first non-null value."""
    from .dataframe import Column
    from sabot import cyarrow as ca
    
    if not cols:
        return Column(None)
    
    # Convert strings to Columns
    columns = []
    for col in cols:
        if isinstance(col, str):
            columns.append(Column(col))
        else:
            columns.append(col)
    
    # Create coalesce expression
    def coalesce_expr(batch):
        result = None
        for col in columns:
            if isinstance(col, Column):
                col_array = col._get_array(batch)
            else:
                col_array = col
            
            if result is None:
                result = col_array
            else:
                result = ca.compute.if_else(ca.compute.is_null(result), col_array, result)
        
        return result
    
    return Column(coalesce_expr)


# Date functions

def year(col):
    """Extract year from date column."""
    from .dataframe import Column
    if isinstance(col, str):
        col = Column(col)
    
    # Create year extraction expression
    def year_expr(batch):
        from sabot import cyarrow as ca
        return ca.compute.year(col._get_array(batch))
    
    return Column(year_expr)


def month(col):
    """Extract month from date column."""
    from .dataframe import Column
    if isinstance(col, str):
        col = Column(col)
    
    # Create month extraction expression
    def month_expr(batch):
        from sabot import cyarrow as ca
        return ca.compute.month(col._get_array(batch))
    
    return Column(month_expr)


def quarter(col):
    """Extract quarter from date column."""
    from .dataframe import Column
    if isinstance(col, str):
        col = Column(col)
    
    # Create quarter extraction expression
    def quarter_expr(batch):
        from sabot import cyarrow as ca
        months = ca.compute.month(col._get_array(batch))
        return ca.compute.divide(months, ca.scalar(3))
    
    return Column(quarter_expr)


def dayofweek(col):
    """Extract day of week from date column."""
    from .dataframe import Column
    if isinstance(col, str):
        col = Column(col)
    
    # Create day of week extraction expression
    def dayofweek_expr(batch):
        from sabot import cyarrow as ca
        return ca.compute.day_of_week(col._get_array(batch))
    
    return Column(dayofweek_expr)


def countDistinct(col):
    """Count distinct values in column."""
    from .dataframe import Column
    if isinstance(col, str):
        col = Column(col)
    
    # Create count distinct expression
    def count_distinct_expr(batch):
        from sabot import cyarrow as ca
        return ca.scalar(len(ca.compute.unique(col._get_array(batch))))
    
    return Column(count_distinct_expr)


# String functions

def lower(col):
    """Convert to lowercase."""
    from .dataframe import Column
    from sabot import cyarrow as ca
    if isinstance(col, str):
        col = Column(col)
    return Column(lambda b: ca.compute.utf8_lower(col._get_array(b)))


def upper(col):
    """Convert to uppercase."""
    from .dataframe import Column
    from sabot import cyarrow as ca
    if isinstance(col, str):
        col = Column(col)
    return Column(lambda b: ca.compute.utf8_upper(col._get_array(b)))


def substring(col, pos: int, len: int):
    """Extract substring."""
    from .dataframe import Column
    from sabot import cyarrow as ca
    if isinstance(col, str):
        col = Column(col)
    return Column(lambda b: ca.compute.utf8_slice_codeunits(col._get_array(b), pos, pos + len))


# Date/time functions

def year(col):
    """Extract year."""
    from .dataframe import Column
    # TODO: Implement with Arrow compute
    return col


def month(col):
    """Extract month."""
    from .dataframe import Column
    # TODO: Implement with Arrow compute
    return col


def day(col):
    """Extract day."""
    from .dataframe import Column
    # TODO: Implement with Arrow compute
    return col


# Window functions

def row_number():
    """Row number window function."""
    from .dataframe import Column
    # TODO: Integrate with sabot_sql window functions
    logger.warning("row_number() not yet implemented")
    return Column("row_number")


def rank():
    """Rank window function."""
    from .dataframe import Column
    logger.warning("rank() not yet implemented")
    return Column("rank")


def lag(col, offset=1):
    """Lag window function."""
    from .dataframe import Column
    logger.warning("lag() not yet implemented")
    return col if isinstance(col, Column) else Column(col)


def lead(col, offset=1):
    """Lead window function."""
    from .dataframe import Column
    logger.warning("lead() not yet implemented")
    return col if isinstance(col, Column) else Column(col)

