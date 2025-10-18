"""
Sabot Spark Compatibility Layer

Thin API wrappers that make Spark workloads lift-and-shift to Sabot.

This is a SHIM, not a reimplementation. All Spark APIs map to existing
Sabot features for zero-code-change migration.

Example:
    # Spark code (before)
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.master("local[*]").getOrCreate()
    df = spark.read.parquet("data.parquet")
    result = df.filter(df.amount > 1000).groupBy("customer_id").count()
    
    # Sabot code (after) - JUST CHANGE THE IMPORT!
    from sabot.spark import SparkSession
    spark = SparkSession.builder.master("local[*]").getOrCreate()
    df = spark.read.parquet("data.parquet")
    result = df.filter(df.amount > 1000).groupBy("customer_id").count()
"""

from .session import SparkSession, SparkContext
from .dataframe import DataFrame, Column
from .rdd import RDD
from .functions import (
    col, lit, sum, avg, count, min, max,
    when, coalesce, year, month, quarter, dayofweek, countDistinct,
    # Add more as needed
)

__all__ = [
    'SparkSession',
    'SparkContext',
    'DataFrame',
    'Column',
    'RDD',
    # Functions
    'col',
    'lit',
    'sum',
    'avg',
    'count',
    'min',
    'max',
    'when',
    'coalesce',
    'year',
    'month',
    'quarter',
    'dayofweek',
    'countDistinct',
]

