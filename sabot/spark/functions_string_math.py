#!/usr/bin/env python3
"""
Complete Spark Functions Implementation

Implements PySpark functions using Sabot's cyarrow and Arrow compute.
Follows zero-copy principles and integrates with Sabot's Cython operators.

Architecture:
  PySpark function → Column expression → Arrow compute kernel
  All operations are zero-copy, SIMD-accelerated via Arrow
"""

import logging
from typing import Any, List, Union

logger = logging.getLogger(__name__)

# Import Sabot's vendored Arrow (cyarrow)
# This uses the Arrow built specifically for Sabot in vendor/arrow/
try:
    from sabot import cyarrow as ca
    # Use cyarrow's compute module which wraps vendored Arrow
    # This ensures we use Sabot's optimized Arrow, not system pyarrow
    pc = ca.compute  # Vendored Arrow compute via cyarrow
    ARROW_AVAILABLE = True
    logger.info("Using Sabot vendored Arrow (cyarrow.compute)")
except ImportError:
    # Fallback to system pyarrow only if vendored not available
    try:
        import pyarrow as ca
        import pyarrow.compute as pc
        ARROW_AVAILABLE = True
        logger.warning("Using system pyarrow (vendored Arrow not built)")
    except ImportError:
        ARROW_AVAILABLE = False
        logger.error("No Arrow available - functions will fail")


def _get_column(col_or_name):
    """Helper to convert string or Column to Column."""
    from .dataframe import Column
    if isinstance(col_or_name, str):
        return Column(col_or_name)
    return col_or_name


# ============================================================================
# Basic Functions
# ============================================================================

def col(name: str):
    """Reference a column by name."""
    from .dataframe import Column
    return Column(name)


def lit(value: Any):
    """Create a literal value column."""
    from .dataframe import Column
    return Column(lambda b: pc.scalar(value))


# ============================================================================
# String Functions (Zero-Copy via Arrow)
# ============================================================================

def concat(*cols):
    """
    Concatenate string columns (zero-copy).
    
    Uses Arrow's binary_join_element_wise for SIMD acceleration.
    """
    from .dataframe import Column
    
    def concat_expr(batch):
        # Get arrays for each column
        arrays = []
        for c in cols:
            col_obj = _get_column(c)
            arrays.append(col_obj._get_array(batch))
        
        # Use Arrow's string concatenation (zero-copy)
        if len(arrays) == 1:
            return arrays[0]
        
        result = arrays[0]
        for arr in arrays[1:]:
            result = pc.binary_join_element_wise(result, arr, '')
        return result
    
    return Column(concat_expr)


def substring(col, pos: int, length: int):
    """
    Extract substring (zero-copy slice).
    
    Uses Arrow's utf8_slice_codeunits for efficient slicing.
    """
    col_obj = _get_column(col)
    from .dataframe import Column
    
    return Column(lambda b: pc.utf8_slice_codeunits(
        col_obj._get_array(b), pos, stop=pos + length
    ))


def upper(col):
    """Convert to uppercase (SIMD-accelerated)."""
    col_obj = _get_column(col)
    from .dataframe import Column
    return Column(lambda b: pc.utf8_upper(col_obj._get_array(b)))


def lower(col):
    """Convert to lowercase (SIMD-accelerated)."""
    col_obj = _get_column(col)
    from .dataframe import Column
    return Column(lambda b: pc.utf8_lower(col_obj._get_array(b)))


def length(col):
    """String length (SIMD-accelerated)."""
    col_obj = _get_column(col)
    from .dataframe import Column
    return Column(lambda b: pc.utf8_length(col_obj._get_array(b)))


def trim(col):
    """Trim whitespace."""
    col_obj = _get_column(col)
    from .dataframe import Column
    return Column(lambda b: pc.utf8_trim_whitespace(col_obj._get_array(b)))


def ltrim(col):
    """Left trim whitespace."""
    col_obj = _get_column(col)
    from .dataframe import Column
    return Column(lambda b: pc.utf8_ltrim_whitespace(col_obj._get_array(b)))


def rtrim(col):
    """Right trim whitespace."""
    col_obj = _get_column(col)
    from .dataframe import Column
    return Column(lambda b: pc.utf8_rtrim_whitespace(col_obj._get_array(b)))


def regexp_extract(col, pattern: str, idx: int = 0):
    """Extract string matching regex pattern."""
    col_obj = _get_column(col)
    from .dataframe import Column
    return Column(lambda b: pc.extract_regex(col_obj._get_array(b), pattern))


def regexp_replace(col, pattern: str, replacement: str):
    """Replace string matching regex pattern."""
    col_obj = _get_column(col)
    from .dataframe import Column
    return Column(lambda b: pc.replace_substring_regex(
        col_obj._get_array(b), pattern, replacement
    ))


# ============================================================================
# Math Functions (SIMD via Arrow)
# ============================================================================

def abs(col):
    """Absolute value (SIMD)."""
    col_obj = _get_column(col)
    from .dataframe import Column
    return Column(lambda b: pc.abs(col_obj._get_array(b)))


def sqrt(col):
    """Square root (SIMD)."""
    col_obj = _get_column(col)
    from .dataframe import Column
    return Column(lambda b: pc.sqrt(col_obj._get_array(b)))


def pow(col, exponent):
    """Power (SIMD)."""
    col_obj = _get_column(col)
    from .dataframe import Column
    return Column(lambda b: pc.power(col_obj._get_array(b), exponent))


def round(col, scale: int = 0):
    """Round to decimal places (SIMD)."""
    col_obj = _get_column(col)
    from .dataframe import Column
    return Column(lambda b: pc.round(col_obj._get_array(b), scale))


def floor(col):
    """Floor function (SIMD)."""
    col_obj = _get_column(col)
    from .dataframe import Column
    return Column(lambda b: pc.floor(col_obj._get_array(b)))


def ceil(col):
    """Ceiling function (SIMD)."""
    col_obj = _get_column(col)
    from .dataframe import Column
    return Column(lambda b: pc.ceil(col_obj._get_array(b)))


def log(col, base=None):
    """Logarithm (SIMD)."""
    col_obj = _get_column(col)
    from .dataframe import Column
    
    if base is None or base == 'e':
        return Column(lambda b: pc.ln(col_obj._get_array(b)))
    elif base == 10:
        return Column(lambda b: pc.log10(col_obj._get_array(b)))
    elif base == 2:
        return Column(lambda b: pc.log2(col_obj._get_array(b)))
    else:
        # Custom base: log_base(x) = ln(x) / ln(base)
        return Column(lambda b: pc.divide(
            pc.ln(col_obj._get_array(b)),
            pc.scalar(import_math.log(base))
        ))


def exp(col):
    """Exponential (SIMD)."""
    col_obj = _get_column(col)
    from .dataframe import Column
    return Column(lambda b: pc.exp(col_obj._get_array(b)))


def sin(col):
    """Sine (SIMD)."""
    col_obj = _get_column(col)
    from .dataframe import Column
    return Column(lambda b: pc.sin(col_obj._get_array(b)))


def cos(col):
    """Cosine (SIMD)."""
    col_obj = _get_column(col)
    from .dataframe import Column
    return Column(lambda b: pc.cos(col_obj._get_array(b)))


def tan(col):
    """Tangent (SIMD)."""
    col_obj = _get_column(col)
    from .dataframe import Column
    return Column(lambda b: pc.tan(col_obj._get_array(b)))


# ============================================================================
# Array/List Functions (Zero-Copy)
# ============================================================================

def array(*cols):
    """Create array from columns."""
    from .dataframe import Column
    
    def array_expr(batch):
        import pyarrow as pa
        arrays = [_get_column(c)._get_array(batch) for c in cols]
        # Create struct array, then convert to list
        return pa.array([list(row) for row in zip(*[arr.to_pylist() for arr in arrays])])
    
    return Column(array_expr)


def array_contains(col, value):
    """Check if array contains value."""
    col_obj = _get_column(col)
    from .dataframe import Column
    
    def contains_expr(batch):
        arr = col_obj._get_array(batch)
        # Use Arrow's is_in for efficient membership test
        import pyarrow as pa
        value_array = pa.array([value] * arr.length())
        return pc.is_in(arr, value_array)
    
    return Column(contains_expr)


def explode(col):
    """
    Explode array column to rows.
    
    Note: This creates new rows, breaking batch structure.
    Use carefully - may not be zero-copy.
    """
    col_obj = _get_column(col)
    from .dataframe import Column
    
    def explode_expr(batch):
        import pyarrow as pa
        arr = col_obj._get_array(batch)
        
        # Flatten list array
        if pa.types.is_list(arr.type):
            return arr.flatten()
        return arr
    
    return Column(explode_expr)


def size(col):
    """Array/map size."""
    col_obj = _get_column(col)
    from .dataframe import Column
    return Column(lambda b: pc.list_value_length(col_obj._get_array(b)))


# ============================================================================
# Date/Time Functions (Arrow Temporal)
# ============================================================================

def current_date():
    """Current date (scalar)."""
    from .dataframe import Column
    import datetime
    today = datetime.date.today()
    return Column(lambda b: pc.scalar(today))


def current_timestamp():
    """Current timestamp (scalar)."""
    from .dataframe import Column
    import datetime
    now = datetime.datetime.now()
    return Column(lambda b: pc.scalar(now))


def year(col):
    """Extract year from date (SIMD)."""
    col_obj = _get_column(col)
    from .dataframe import Column
    return Column(lambda b: pc.year(col_obj._get_array(b)))


def month(col):
    """Extract month from date (SIMD)."""
    col_obj = _get_column(col)
    from .dataframe import Column
    return Column(lambda b: pc.month(col_obj._get_array(b)))


def day(col):
    """Extract day from date (SIMD)."""
    col_obj = _get_column(col)
    from .dataframe import Column
    return Column(lambda b: pc.day(col_obj._get_array(b)))


def hour(col):
    """Extract hour from timestamp (SIMD)."""
    col_obj = _get_column(col)
    from .dataframe import Column
    return Column(lambda b: pc.hour(col_obj._get_array(b)))


def minute(col):
    """Extract minute from timestamp (SIMD)."""
    col_obj = _get_column(col)
    from .dataframe import Column
    return Column(lambda b: pc.minute(col_obj._get_array(b)))


def second(col):
    """Extract second from timestamp (SIMD)."""
    col_obj = _get_column(col)
    from .dataframe import Column
    return Column(lambda b: pc.second(col_obj._get_array(b)))


def dayofweek(col):
    """Day of week (0=Monday, 6=Sunday) (SIMD)."""
    col_obj = _get_column(col)
    from .dataframe import Column
    return Column(lambda b: pc.day_of_week(col_obj._get_array(b)))


def dayofyear(col):
    """Day of year (1-366) (SIMD)."""
    col_obj = _get_column(col)
    from .dataframe import Column
    return Column(lambda b: pc.day_of_year(col_obj._get_array(b)))


def quarter(col):
    """Quarter (1-4) (SIMD)."""
    col_obj = _get_column(col)
    from .dataframe import Column
    return Column(lambda b: pc.quarter(col_obj._get_array(b)))


def weekofyear(col):
    """Week of year (SIMD)."""
    col_obj = _get_column(col)
    from .dataframe import Column
    return Column(lambda b: pc.week(col_obj._get_array(b)))


def date_add(col, days: int):
    """Add days to date."""
    col_obj = _get_column(col)
    from .dataframe import Column
    import pyarrow as pa
    return Column(lambda b: pc.add(
        col_obj._get_array(b),
        pc.scalar(days, type=pa.int32())
    ))


def date_sub(col, days: int):
    """Subtract days from date."""
    col_obj = _get_column(col)
    from .dataframe import Column
    import pyarrow as pa
    return Column(lambda b: pc.subtract(
        col_obj._get_array(b),
        pc.scalar(days, type=pa.int32())
    ))


# ============================================================================
# Aggregation Functions
# ============================================================================

def sum(col):
    """Sum aggregation (SIMD)."""
    col_obj = _get_column(col)
    from .dataframe import Column
    return Column(lambda b: pc.sum(col_obj._get_array(b)))


def avg(col):
    """Average aggregation (SIMD)."""
    col_obj = _get_column(col)
    from .dataframe import Column
    return Column(lambda b: pc.mean(col_obj._get_array(b)))


def mean(col):
    """Mean aggregation (alias for avg)."""
    return avg(col)


def count(col):
    """Count aggregation."""
    if col == "*":
        from .dataframe import Column
        return Column(lambda b: pc.scalar(b.num_rows))
    
    col_obj = _get_column(col)
    from .dataframe import Column
    return Column(lambda b: pc.count(col_obj._get_array(b)))


def min(col):
    """Minimum aggregation (SIMD)."""
    col_obj = _get_column(col)
    from .dataframe import Column
    return Column(lambda b: pc.min(col_obj._get_array(b)))


def max(col):
    """Maximum aggregation (SIMD)."""
    col_obj = _get_column(col)
    from .dataframe import Column
    return Column(lambda b: pc.max(col_obj._get_array(b)))


def stddev(col):
    """Standard deviation (SIMD)."""
    col_obj = _get_column(col)
    from .dataframe import Column
    return Column(lambda b: pc.stddev(col_obj._get_array(b)))


def variance(col):
    """Variance (SIMD)."""
    col_obj = _get_column(col)
    from .dataframe import Column
    return Column(lambda b: pc.variance(col_obj._get_array(b)))


def countDistinct(col):
    """Count distinct values."""
    col_obj = _get_column(col)
    from .dataframe import Column
    return Column(lambda b: pc.scalar(
        len(pc.unique(col_obj._get_array(b)))
    ))


def approx_count_distinct(col, rsd=0.05):
    """Approximate count distinct (faster for large datasets)."""
    col_obj = _get_column(col)
    from .dataframe import Column
    return Column(lambda b: pc.approximate_count_distinct(col_obj._get_array(b)))


# ============================================================================
# Conditional Functions
# ============================================================================

def when(condition, value):
    """
    Conditional expression (zero-copy).
    
    Uses Arrow's if_else for branch-free SIMD execution.
    """
    from .dataframe import Column
    
    def when_expr(batch):
        # Get condition array
        if isinstance(condition, Column):
            cond_array = condition._get_array(batch)
        else:
            cond_array = condition
        
        # Get value
        if isinstance(value, Column):
            value_array = value._get_array(batch)
        else:
            value_array = pc.scalar(value)
        
        # Use Arrow's if_else (SIMD, branch-free)
        return pc.if_else(cond_array, value_array, pc.scalar(None))
    
    return Column(when_expr)


def coalesce(*cols):
    """
    Return first non-null value (zero-copy).
    
    Uses Arrow's coalesce for efficient null handling.
    """
    from .dataframe import Column
    
    def coalesce_expr(batch):
        arrays = [_get_column(c)._get_array(batch) for c in cols]
        
        # Use Arrow's coalesce (efficient null handling)
        result = arrays[0]
        for arr in arrays[1:]:
            result = pc.coalesce(result, arr)
        return result
    
    return Column(coalesce_expr)


def isnull(col):
    """Check if null (SIMD)."""
    col_obj = _get_column(col)
    from .dataframe import Column
    return Column(lambda b: pc.is_null(col_obj._get_array(b)))


def isnan(col):
    """Check if NaN (SIMD)."""
    col_obj = _get_column(col)
    from .dataframe import Column
    return Column(lambda b: pc.is_nan(col_obj._get_array(b)))


# ============================================================================
# Type Conversion Functions
# ============================================================================

def cast(col, dataType):
    """
    Cast to different type (zero-copy when possible).
    
    Arrow's cast is zero-copy for compatible types.
    """
    col_obj = _get_column(col)
    from .dataframe import Column
    import pyarrow as pa
    
    # Map Spark types to Arrow types
    type_map = {
        'string': pa.utf8(),
        'int': pa.int32(),
        'long': pa.int64(),
        'float': pa.float32(),
        'double': pa.float64(),
        'boolean': pa.bool_(),
        'timestamp': pa.timestamp('us'),
        'date': pa.date32(),
    }
    
    arrow_type = type_map.get(dataType, dataType)
    
    return Column(lambda b: pc.cast(col_obj._get_array(b), arrow_type))


# ============================================================================
# Collection Functions
# ============================================================================

def collect_list(col):
    """Aggregate column into list."""
    col_obj = _get_column(col)
    from .dataframe import Column
    
    def collect_expr(batch):
        arr = col_obj._get_array(batch)
        # Collect into list array
        import pyarrow as pa
        return pa.array([arr.to_pylist()])
    
    return Column(collect_expr)


def collect_set(col):
    """Aggregate column into set (distinct values)."""
    col_obj = _get_column(col)
    from .dataframe import Column
    
    def collect_set_expr(batch):
        arr = col_obj._get_array(batch)
        unique_arr = pc.unique(arr)
        import pyarrow as pa
        return pa.array([unique_arr.to_pylist()])
    
    return Column(collect_set_expr)


# ============================================================================
# Statistical Functions
# ============================================================================

def corr(col1, col2):
    """Correlation coefficient."""
    col1_obj = _get_column(col1)
    col2_obj = _get_column(col2)
    from .dataframe import Column
    
    def corr_expr(batch):
        arr1 = col1_obj._get_array(batch)
        arr2 = col2_obj._get_array(batch)
        
        # Use numpy for correlation (Arrow doesn't have this)
        import numpy as np
        return pc.scalar(np.corrcoef(arr1.to_numpy(), arr2.to_numpy())[0, 1])
    
    return Column(corr_expr)


def covar_pop(col1, col2):
    """Population covariance."""
    col1_obj = _get_column(col1)
    col2_obj = _get_column(col2)
    from .dataframe import Column
    
    def covar_expr(batch):
        arr1 = col1_obj._get_array(batch)
        arr2 = col2_obj._get_array(batch)
        
        import numpy as np
        return pc.scalar(np.cov(arr1.to_numpy(), arr2.to_numpy(), ddof=0)[0, 1])
    
    return Column(covar_expr)


# ============================================================================
# Null Handling
# ============================================================================

def nullif(col1, col2):
    """Return null if col1 == col2, else col1."""
    col1_obj = _get_column(col1)
    col2_obj = _get_column(col2)
    from .dataframe import Column
    
    def nullif_expr(batch):
        arr1 = col1_obj._get_array(batch)
        arr2 = col2_obj._get_array(batch)
        
        # Use if_else: if arr1 == arr2 then null else arr1
        equals = pc.equal(arr1, arr2)
        return pc.if_else(equals, pc.scalar(None), arr1)
    
    return Column(nullif_expr)


def nvl(col1, col2):
    """Return col2 if col1 is null, else col1 (alias for coalesce)."""
    return coalesce(col1, col2)


def ifnull(col1, col2):
    """Return col2 if col1 is null, else col1 (alias for coalesce)."""
    return coalesce(col1, col2)


# ============================================================================
# Hashing Functions (for shuffling/partitioning)
# ============================================================================

def hash(*cols):
    """
    Hash columns (for partitioning).
    
    Uses Sabot's hash_array for efficient hashing.
    This is used internally for shuffle operations.
    """
    from .dataframe import Column
    
    def hash_expr(batch):
        # Use Sabot's hash if available
        try:
            from sabot.cyarrow.compute import hash_array
            arrays = [_get_column(c)._get_array(batch) for c in cols]
            # Combine hashes
            result = hash_array(arrays[0])
            for arr in arrays[1:]:
                from sabot.cyarrow.compute import hash_combine
                result = hash_combine(result, hash_array(arr))
            return result
        except ImportError:
            # Fallback to Arrow's hash
            arrays = [_get_column(c)._get_array(batch) for c in cols]
            import pyarrow as pa
            # Create struct and hash
            struct_arr = pa.StructArray.from_arrays(arrays, names=[f'col{i}' for i in range(len(arrays))])
            return pc.hash(struct_arr)
    
    return Column(hash_expr)


# ============================================================================
# Misc Functions
# ============================================================================

def greatest(*cols):
    """Return greatest value across columns (SIMD)."""
    from .dataframe import Column
    
    def greatest_expr(batch):
        arrays = [_get_column(c)._get_array(batch) for c in cols]
        result = arrays[0]
        for arr in arrays[1:]:
            result = pc.max_element_wise(result, arr)
        return result
    
    return Column(greatest_expr)


def least(*cols):
    """Return least value across columns (SIMD)."""
    from .dataframe import Column
    
    def least_expr(batch):
        arrays = [_get_column(c)._get_array(batch) for c in cols]
        result = arrays[0]
        for arr in arrays[1:]:
            result = pc.min_element_wise(result, arr)
        return result
    
    return Column(least_expr)


def rand(seed=None):
    """Random number generator."""
    from .dataframe import Column
    import random
    if seed is not None:
        random.seed(seed)
    
    def rand_expr(batch):
        import numpy as np
        import pyarrow as pa
        if seed is not None:
            np.random.seed(seed)
        return pa.array(np.random.rand(batch.num_rows))
    
    return Column(rand_expr)


# ============================================================================
# List All Available Functions
# ============================================================================

__all__ = [
    # Basic
    'col', 'lit',
    # String
    'concat', 'substring', 'upper', 'lower', 'length', 'trim', 'ltrim', 'rtrim',
    'regexp_extract', 'regexp_replace',
    # Math
    'abs', 'sqrt', 'pow', 'round', 'floor', 'ceil',
    'log', 'exp', 'sin', 'cos', 'tan',
    # Aggregations
    'sum', 'avg', 'mean', 'count', 'min', 'max',
    'stddev', 'variance', 'countDistinct', 'approx_count_distinct',
    # Arrays
    'array', 'array_contains', 'explode', 'size',
    'collect_list', 'collect_set',
    # Date/Time
    'current_date', 'current_timestamp',
    'year', 'month', 'day', 'hour', 'minute', 'second',
    'dayofweek', 'dayofyear', 'quarter', 'weekofyear',
    'date_add', 'date_sub',
    # Conditional
    'when', 'coalesce', 'isnull', 'isnan', 'nullif', 'nvl', 'ifnull',
    # Type conversion
    'cast',
    # Collection
    # Statistical
    'corr', 'covar_pop',
    # Hashing
    'hash',
    # Misc
    'greatest', 'least', 'rand',
]

# Print count on import
logger.info(f"Loaded {len(__all__)} Spark-compatible functions")

