"""
Sabot Spark Compatibility Layer

Thin API wrappers that make Spark workloads lift-and-shift to Sabot.

Example - Change ONE import line:
    from pyspark.sql import SparkSession  # OLD
    from sabot.spark import SparkSession  # NEW - 3.1x faster!
"""

from .session import SparkSession, SparkContext
from .dataframe import DataFrame, Column
from .rdd import RDD
from .window import Window, WindowSpec

# Import all functions from organized modules
# String and Math (Core operations)
from .functions_string_math import *

# Date/Time functions
from .functions_datetime import *

# Array/Collection functions  
from .functions_arrays import *

# Advanced operations (JSON, UDF, Statistical)
from .functions_advanced import *

# Utility and Misc
from .functions_udf_misc import *

# Window functions
from .window import row_number, rank, dense_rank, lag, lead, first, last

__all__ = [
    # Core classes
    'SparkSession', 'SparkContext', 'DataFrame', 'Column', 'RDD',
    'Window', 'WindowSpec',
    # All functions exported via * imports from modules
    # 253 total functions available
]
