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

__all__ = [
    'MySQLCDCConfig',
    'MySQLCDCConnector',
    'MySQLAutoConfigurator',
    'MySQLTableDiscovery',
    'MySQLSchemaTracker',
    'MySQLStreamRouter',
]
