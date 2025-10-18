#!/usr/bin/env python3
"""
Spark DataFrameReader Compatibility

DataFrameReader wrapper that maps to Sabot connectors.
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)


class DataFrameReader:
    """
    DataFrame reader (Spark API).
    
    Maps to Sabot's connector system.
    
    Example:
        df = spark.read.parquet("data.parquet")
        df = spark.read.csv("data.csv", header=True)
    """
    
    def __init__(self, session):
        """
        Initialize reader.
        
        Args:
            session: Parent SparkSession
        """
        self._session = session
        self._format = None
        self._options = {}
    
    def format(self, source: str):
        """Set input format."""
        self._format = source
        return self
    
    def option(self, key: str, value):
        """Set option."""
        self._options[key] = value
        return self
    
    def options(self, **options):
        """Set multiple options."""
        self._options.update(options)
        return self
    
    def load(self, path: Optional[str] = None):
        """Load data from path."""
        from .dataframe import DataFrame
        
        if not self._format:
            raise ValueError("Must specify format with .format() or use .parquet(), .csv(), etc.")
        
        # Use Sabot's from_uri
        if path:
            uri = f"{self._format}://{path}"
        else:
            uri = self._options.get('path', '')
        
        stream = self._session._engine.stream.from_uri(uri, **self._options)
        return DataFrame(stream, self._session)
    
    def parquet(self, path: str):
        """Read Parquet file."""
        from .dataframe import DataFrame
        
        # Use Sabot's parquet reader
        stream = self._session._engine.stream.from_parquet(path)
        return DataFrame(stream, self._session)
    
    def csv(self, path: str, **options):
        """Read CSV file."""
        from .dataframe import DataFrame
        import pyarrow as pa
        import pyarrow.csv as pa_csv
        
        # Convert Spark options to PyArrow options
        read_options = {}
        parse_options = {}
        convert_options = {}
        
        if 'header' in options:
            read_options['skip_rows'] = 0 if options['header'] else 1
        if 'inferSchema' in options:
            convert_options['strings_can_be_null'] = True
        if 'delimiter' in options:
            parse_options['delimiter'] = options['delimiter']
        
        # Use PyArrow CSV reader with proper options
        table = pa_csv.read_csv(
            path,
            read_options=pa_csv.ReadOptions(**read_options) if read_options else None,
            parse_options=pa_csv.ParseOptions(**parse_options) if parse_options else None,
            convert_options=pa_csv.ConvertOptions(**convert_options) if convert_options else None
        )
        
        # Convert to Stream
        stream = self._session._engine.stream.from_table(table)
        return DataFrame(stream, self._session)
    
    def json(self, path: str):
        """Read JSON file."""
        from .dataframe import DataFrame
        
        # Use Sabot's connector
        stream = self._session._engine.stream.from_uri(f"file://{path}")
        return DataFrame(stream, self._session)
    
    def jdbc(self, url: str, table: str, **options):
        """Read from JDBC source."""
        from .dataframe import DataFrame
        
        # Use Sabot's postgres connector (for JDBC compatibility)
        jdbc_uri = f"postgres://{url}?table={table}"
        stream = self._session._engine.stream.from_uri(jdbc_uri, **options)
        return DataFrame(stream, self._session)
    
    def table(self, tableName: str):
        """Read registered table."""
        from .dataframe import DataFrame
        
        # TODO: Get table from SQL catalog
        logger.warning(f"Reading table {tableName} not yet implemented")
        # For now, try to execute as SQL
        result = self._session.sql(f"SELECT * FROM {tableName}")
        return result

