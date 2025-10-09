"""
High-performance data connectors for Sabot.

Leverages DuckDB's connector ecosystem for zero-copy data ingestion and export.
"""

# Base connector interfaces
from .base import ConnectorSource, ConnectorSink

# DuckDB connectors (source and sink)
from .duckdb_source import DuckDBSource
from .duckdb_sink import DuckDBSink

# Plugin management
from .plugin import DuckDBConnectorPlugin, ExtensionRegistry

# Format-specific helpers
from .formats import (
    ParquetSource,
    CSVSource,
    PostgresSource,
    DeltaSource,
    ParquetSink,
    CSVSink,
    DeltaSink,
)

# URI-based smart connector
from .uri import from_uri, to_uri

__all__ = [
    # Base interfaces
    'ConnectorSource',
    'ConnectorSink',

    # DuckDB connectors
    'DuckDBSource',
    'DuckDBSink',

    # Plugin system
    'DuckDBConnectorPlugin',
    'ExtensionRegistry',

    # Format helpers
    'ParquetSource',
    'CSVSource',
    'PostgresSource',
    'DeltaSource',
    'ParquetSink',
    'CSVSink',
    'DeltaSink',

    # Smart connectors
    'from_uri',
    'to_uri',
]
