#!/usr/bin/env python3
"""
Spark DataFrameWriter Compatibility

DataFrameWriter wrapper using PyArrow writers.
"""

import logging

logger = logging.getLogger(__name__)


class DataFrameWriter:
    """
    DataFrame writer (Spark API).
    
    Maps to PyArrow writers and Sabot connectors.
    
    Example:
        df.write.parquet("output.parquet")
        df.write.mode("append").csv("output.csv")
    """
    
    def __init__(self, df):
        """
        Initialize writer.
        
        Args:
            df: DataFrame to write
        """
        self._df = df
        self._mode = 'overwrite'
        self._format = None
        self._options = {}
    
    def mode(self, saveMode: str):
        """
        Set save mode.
        
        Args:
            saveMode: 'overwrite', 'append', 'ignore', 'error'
            
        Returns:
            Self for chaining
        """
        self._mode = saveMode
        return self
    
    def format(self, source: str):
        """Set output format."""
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
    
    def save(self, path: Optional[str] = None):
        """Save to path."""
        if not self._format:
            raise ValueError("Must specify format")
        
        # Delegate to format-specific method
        if self._format == 'parquet':
            self.parquet(path)
        elif self._format == 'csv':
            self.csv(path)
        elif self._format == 'json':
            self.json(path)
        else:
            raise ValueError(f"Unknown format: {self._format}")
    
    def parquet(self, path: str):
        """Write as Parquet."""
        import pyarrow.parquet as pq
        import pyarrow as pa
        
        # Collect all batches
        batches = list(self._df._stream)
        
        # Combine to table
        if batches:
            table = pa.Table.from_batches(batches)
            
            # Write
            pq.write_table(table, path)
            logger.info(f"Wrote {table.num_rows} rows to {path}")
    
    def csv(self, path: str):
        """Write as CSV."""
        import pyarrow.csv as csv
        import pyarrow as pa
        
        # Collect all batches
        batches = list(self._df._stream)
        
        # Combine to table
        if batches:
            table = pa.Table.from_batches(batches)
            
            # Write
            with open(path, 'w') as f:
                csv.write_csv(table, f)
            logger.info(f"Wrote {table.num_rows} rows to {path}")
    
    def json(self, path: str):
        """Write as JSON."""
        import json
        
        # Collect rows
        rows = self._df.collect()
        
        # Write
        with open(path, 'w') as f:
            for row in rows:
                f.write(json.dumps(row) + '\n')
        logger.info(f"Wrote {len(rows)} rows to {path}")

