"""
PostgreSQL Connectors for Sabot.

Provides high-performance CDC (Change Data Capture) functionality using
PostgreSQL logical replication with wal2json output plugin.
"""

from .libpq_conn import PostgreSQLConnection
from .wal2json_parser import Wal2JsonParser, CDCEvent, CDCTransaction, create_parser
from .cdc_connector import PostgreSQLCDCConnector, PostgreSQLCDCConfig, CDCRecord, create_postgresql_cdc_connector

__all__ = [
    # Core connection
    'PostgreSQLConnection',

    # Parsing
    'Wal2JsonParser',
    'CDCEvent',
    'CDCTransaction',
    'create_parser',

    # CDC connector
    'PostgreSQLCDCConnector',
    'PostgreSQLCDCConfig',
    'CDCRecord',
    'create_postgresql_cdc_connector',
]






