"""
Sabot Connectors.

High-performance data connectors for Change Data Capture and streaming.
"""

# Re-export MySQL connector for convenience
from .mysql import (
    MySQLCDCConfig,
    MySQLCDCConnector,
    MySQLAutoConfigurator,
    MySQLTableDiscovery,
    MySQLSchemaTracker,
    MySQLStreamRouter,
)

# Try to import DuckDB connector (may not be compiled)
try:
    from .duckdb_core import (
        DuckDBConnection,
        DuckDBArrowResult,
        DuckDBError,
    )
    DUCKDB_AVAILABLE = True
except ImportError:
    DuckDBConnection = None
    DuckDBArrowResult = None
    DuckDBError = None
    DUCKDB_AVAILABLE = False

__all__ = [
    'MySQLCDCConfig',
    'MySQLCDCConnector',
    'MySQLAutoConfigurator',
    'MySQLTableDiscovery',
    'MySQLSchemaTracker',
    'MySQLStreamRouter',
    'DuckDBConnection',
    'DuckDBArrowResult',
    'DuckDBError',
    'DUCKDB_AVAILABLE',
]
