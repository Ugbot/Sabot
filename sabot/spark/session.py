#!/usr/bin/env python3
"""
Spark Session Compatibility

SparkSession and SparkContext wrappers that map to Sabot engine.

Design: Thin wrappers over existing Sabot functionality.
Performance: Delegates to Sabot's C++/Cython implementations.
"""

import logging
from typing import Optional, Any, Dict, List

logger = logging.getLogger(__name__)


class SparkContext:
    """
    Spark context compatibility wrapper.
    
    Maps to Sabot engine for distributed execution.
    
    Example:
        sc = SparkContext("local[*]")
        data = sc.parallelize([1, 2, 3, 4, 5])
    """
    
    def __init__(self, master: str = "local[*]", appName: Optional[str] = None):
        """
        Initialize Spark context.
        
        Args:
            master: Cluster URL or "local[*]"
            appName: Application name
        """
        self.master = master
        self.appName = appName or "SabotApp"
        
        # Determine mode from master
        if master.startswith("local"):
            self.mode = 'local'
        elif master.startswith("sabot://"):
            self.mode = 'distributed'
            self.coordinator = master.replace("sabot://", "")
        else:
            # Assume distributed
            self.mode = 'distributed'
            self.coordinator = master
        
        # Initialize Sabot engine
        from sabot import Sabot
        
        self._engine = Sabot(
            mode=self.mode,
            coordinator=self.coordinator if self.mode == 'distributed' else None
        )
        
        logger.info(f"Initialized SparkContext: {self.master} ({self.mode} mode)")
    
    def parallelize(self, data: List, numSlices: Optional[int] = None):
        """
        Distribute collection to create RDD.
        
        Args:
            data: Python collection
            numSlices: Number of partitions
            
        Returns:
            RDD wrapping Sabot stream
        """
        from .rdd import RDD
        
        # Use Sabot's stream.parallelize
        stream = self._engine.stream.parallelize(data, num_partitions=numSlices)
        
        return RDD(stream, self)
    
    def textFile(self, path: str, minPartitions: Optional[int] = None):
        """
        Read text file as RDD.
        
        Args:
            path: File path
            minPartitions: Minimum partitions
            
        Returns:
            RDD of lines
        """
        from .rdd import RDD
        
        # Use Sabot's file reading
        stream = self._engine.stream.from_uri(f"file://{path}")
        
        return RDD(stream, self)
    
    def stop(self):
        """Stop Spark context and cleanup."""
        if self._engine:
            self._engine.shutdown()
        logger.info("SparkContext stopped")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
        return False


class SparkSessionBuilder:
    """Builder for SparkSession (Spark API compatibility)."""
    
    def __init__(self):
        self.master_url = "local[*]"
        self.app_name = "SabotApp"
        self.config_options = {}
    
    def master(self, master: str):
        """Set master URL."""
        self.master_url = master
        return self
    
    def appName(self, name: str):
        """Set application name."""
        self.app_name = name
        return self
    
    def config(self, key: str, value: Any):
        """Set configuration option."""
        self.config_options[key] = value
        return self
    
    def getOrCreate(self):
        """Create or get existing SparkSession."""
        return SparkSession(self.master_url, self.app_name, self.config_options)


class SparkSession:
    """
    Spark session compatibility wrapper.
    
    Maps to Sabot unified engine.
    
    Example:
        spark = SparkSession.builder.master("local[*]").getOrCreate()
        df = spark.read.parquet("data.parquet")
        result = df.filter(df.amount > 1000).groupBy("customer_id").count()
    """
    
    # Class-level builder
    builder = SparkSessionBuilder()
    
    def __init__(
        self,
        master: str = "local[*]",
        appName: str = "SabotApp",
        config: Optional[Dict] = None
    ):
        """
        Initialize Spark session.
        
        Args:
            master: Cluster URL or "local[*]"
            appName: Application name
            config: Configuration options
        """
        self._config = config or {}
        
        # Create SparkContext
        self._spark_context = SparkContext(master, appName)
        
        # Get Sabot engine from context
        self._engine = self._spark_context._engine
        
        logger.info(f"Initialized SparkSession: {appName}")
    
    @property
    def sparkContext(self):
        """Get SparkContext."""
        return self._spark_context
    
    @property
    def read(self):
        """Get DataFrameReader."""
        from .reader import DataFrameReader
        return DataFrameReader(self)
    
    def sql(self, sqlQuery: str):
        """
        Execute SQL query and return DataFrame.
        
        Args:
            sqlQuery: SQL query string
            
        Returns:
            DataFrame with results
        """
        from .dataframe import DataFrame
        
        # Use Sabot's SQL engine
        result = self._engine.sql.execute(sqlQuery)
        
        return DataFrame(result, self)
    
    def createDataFrame(self, data, schema=None):
        """
        Create DataFrame from data.
        
        Args:
            data: Python collection or pandas DataFrame
            schema: Schema specification
            
        Returns:
            DataFrame
        """
        from .dataframe import DataFrame
        import pyarrow as pa
        
        # Convert to Arrow table
        if hasattr(data, 'to_arrow'):
            # Pandas DataFrame with to_arrow
            table = data.to_arrow()
        elif isinstance(data, list):
            # Python list
            table = pa.Table.from_pylist(data)
        else:
            # Assume it's already Arrow-compatible
            table = data
        
        # Wrap in Stream, then DataFrame
        stream = self._engine.stream.from_table(table)
        
        return DataFrame(stream, self)
    
    def stop(self):
        """Stop Spark session."""
        if self._spark_context:
            self._spark_context.stop()
        logger.info("SparkSession stopped")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
        return False


# Convenience function
def getOrCreate(master: str = "local[*]", appName: str = "SabotApp"):
    """
    Create or get SparkSession.
    
    Args:
        master: Cluster URL
        appName: Application name
        
    Returns:
        SparkSession
    """
    return SparkSession(master, appName)

