"""
MySQL CDC Connector for Sabot.

High-performance Change Data Capture from MySQL binlog replication.
"""

from .mysql_cdc import (
    MySQLCDCConfig,
    MySQLCDCConnector,
    CDCRecord,
    cdc_records_to_arrow_batch,
    create_mysql_cdc_connector
)

from .mysql_auto_config import (
    MySQLAutoConfigurator,
    ConfigurationIssue,
    Severity
)

from .mysql_discovery import (
    MySQLTableDiscovery
)

from .mysql_table_patterns import (
    TablePattern,
    TablePatternMatcher,
    validate_pattern,
    expand_patterns
)

from .mysql_schema_tracker import (
    MySQLSchemaTracker,
    DDLEvent,
    DDLType,
    TableSchema
)

from .mysql_stream_router import (
    MySQLStreamRouter,
    TableRoute,
    create_table_router
)

__all__ = [
    # Core CDC
    'MySQLCDCConfig',
    'MySQLCDCConnector',
    'CDCRecord',
    'cdc_records_to_arrow_batch',
    'create_mysql_cdc_connector',

    # Auto-configuration
    'MySQLAutoConfigurator',
    'ConfigurationIssue',
    'Severity',

    # Discovery
    'MySQLTableDiscovery',

    # Pattern matching
    'TablePattern',
    'TablePatternMatcher',
    'validate_pattern',
    'expand_patterns',

    # Schema tracking
    'MySQLSchemaTracker',
    'DDLEvent',
    'DDLType',
    'TableSchema',

    # Stream routing
    'MySQLStreamRouter',
    'TableRoute',
    'create_table_router',
]
