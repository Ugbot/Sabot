#!/usr/bin/env python3
"""
MySQL CDC Connector for Sabot.

Provides Change Data Capture functionality using MySQL binlog replication
via the python-mysql-replication library. Streams database changes in real-time.

Features:
- Binlog replication with ROW format
- GTID-based resume support
- Automatic binlog position tracking
- Configurable table filtering
- Arrow RecordBatch output
- Error handling and recovery
"""

import asyncio
import logging
import sys
from pathlib import Path
from typing import Optional, Dict, Any, List, AsyncGenerator, Union
from dataclasses import dataclass, field
from contextlib import asynccontextmanager
from datetime import datetime

# Add vendored library to path
VENDOR_PATH = Path(__file__).parent.parent.parent.parent.parent / "vendor" / "python-mysql-replication"
if str(VENDOR_PATH) not in sys.path:
    sys.path.insert(0, str(VENDOR_PATH))

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)
from pymysqlreplication.event import (
    QueryEvent,
    RotateEvent,
    GtidEvent,
)

from sabot import cyarrow as ca
from .mysql_schema_tracker import MySQLSchemaTracker, DDLEvent

logger = logging.getLogger(__name__)


# ============================================================================
# Configuration
# ============================================================================

@dataclass
class MySQLCDCConfig:
    """Configuration for MySQL CDC connector."""

    # Connection settings
    host: str = "localhost"
    port: int = 3306
    user: str = "root"
    password: str = ""
    database: Optional[str] = None  # None = monitor all databases
    connection_timeout: int = 30

    # Replication settings
    server_id: int = 1000001  # Unique ID for this replication client
    blocking: bool = False  # Non-blocking mode (stream continuously)
    resume_stream: bool = True  # Resume from last position

    # Binlog position (for resume)
    log_file: Optional[str] = None
    log_pos: Optional[int] = None

    # GTID settings (preferred over file+pos)
    auto_position: bool = False  # Use GTID auto-positioning
    gtid_set: Optional[str] = None  # GTID set to resume from

    # Filtering
    only_tables: Optional[List[str]] = None  # e.g., ["db.users", "db.orders"]
    ignored_tables: Optional[List[str]] = None
    only_schemas: Optional[List[str]] = None
    ignored_schemas: Optional[List[str]] = None
    only_events: Optional[List] = None  # Filter event types
    ignored_events: Optional[List] = None

    # Event settings
    freeze_schema: bool = False  # Don't update table metadata on DDL
    fail_on_table_metadata_unavailable: bool = False
    slave_uuid: Optional[str] = None
    pymysql_wrapper: Optional[Any] = None

    # Streaming settings
    batch_size: int = 100  # Max events per batch
    max_poll_interval: float = 1.0  # Max time to wait for new data (seconds)
    heartbeat_interval: float = 10.0  # Heartbeat frequency

    # Error handling
    max_retry_attempts: int = 3
    retry_backoff_seconds: float = 1.0

    # DDL tracking
    track_ddl: bool = True  # Track DDL changes in schema tracker
    emit_ddl_events: bool = True  # Emit DDL events to stream

    def to_connection_params(self) -> Dict[str, Any]:
        """Convert config to pymysqlreplication connection parameters."""
        params = {
            "connection_settings": {
                "host": self.host,
                "port": self.port,
                "user": self.user,
                "passwd": self.password,
            },
            "server_id": self.server_id,
            "blocking": self.blocking,
            "resume_stream": self.resume_stream,
        }

        if self.database:
            params["connection_settings"]["database"] = self.database

        # Position/GTID
        if self.auto_position:
            params["auto_position"] = True
            if self.gtid_set:
                params["gtid_set"] = self.gtid_set
        else:
            if self.log_file:
                params["log_file"] = self.log_file
            if self.log_pos:
                params["log_pos"] = self.log_pos

        # Filters
        if self.only_tables:
            params["only_tables"] = self.only_tables
        if self.ignored_tables:
            params["ignored_tables"] = self.ignored_tables
        if self.only_schemas:
            params["only_schemas"] = self.only_schemas
        if self.ignored_schemas:
            params["ignored_schemas"] = self.ignored_schemas
        if self.only_events:
            params["only_events"] = self.only_events
        if self.ignored_events:
            params["ignored_events"] = self.ignored_events

        # Other settings
        params["freeze_schema"] = self.freeze_schema
        params["fail_on_table_metadata_unavailable"] = self.fail_on_table_metadata_unavailable

        if self.slave_uuid:
            params["slave_uuid"] = self.slave_uuid
        if self.pymysql_wrapper:
            params["pymysql_wrapper"] = self.pymysql_wrapper

        return params


# ============================================================================
# CDC Event Types (Exposed to Users)
# ============================================================================

@dataclass
class CDCRecord:
    """High-level CDC record for user consumption."""

    event_type: str  # 'insert', 'update', 'delete', 'ddl', 'gtid'
    schema: str
    table: str
    timestamp: datetime

    # Data fields
    data: Optional[Dict[str, Any]] = None  # New/changed data
    old_data: Optional[Dict[str, Any]] = None  # Old data (for updates)

    # Position tracking
    log_file: Optional[str] = None
    log_pos: Optional[int] = None
    gtid: Optional[str] = None

    # DDL-specific
    query: Optional[str] = None

    @classmethod
    def from_binlog_event(cls, event: Any, log_file: str, log_pos: int) -> List['CDCRecord']:
        """Convert binlog event to user-friendly records."""
        records = []

        if isinstance(event, WriteRowsEvent):
            # INSERT
            for row in event.rows:
                records.append(cls(
                    event_type='insert',
                    schema=event.schema,
                    table=event.table,
                    timestamp=datetime.fromtimestamp(event.timestamp),
                    data=row["values"],
                    log_file=log_file,
                    log_pos=log_pos,
                ))

        elif isinstance(event, UpdateRowsEvent):
            # UPDATE
            for row in event.rows:
                records.append(cls(
                    event_type='update',
                    schema=event.schema,
                    table=event.table,
                    timestamp=datetime.fromtimestamp(event.timestamp),
                    data=row["after_values"],
                    old_data=row["before_values"],
                    log_file=log_file,
                    log_pos=log_pos,
                ))

        elif isinstance(event, DeleteRowsEvent):
            # DELETE
            for row in event.rows:
                records.append(cls(
                    event_type='delete',
                    schema=event.schema,
                    table=event.table,
                    timestamp=datetime.fromtimestamp(event.timestamp),
                    data=row["values"],
                    log_file=log_file,
                    log_pos=log_pos,
                ))

        elif isinstance(event, QueryEvent):
            # DDL (CREATE, ALTER, DROP, etc.)
            records.append(cls(
                event_type='ddl',
                schema=event.schema.decode() if isinstance(event.schema, bytes) else event.schema,
                table='',
                timestamp=datetime.fromtimestamp(event.timestamp),
                query=event.query,
                log_file=log_file,
                log_pos=log_pos,
            ))

        elif isinstance(event, GtidEvent):
            # GTID marker
            records.append(cls(
                event_type='gtid',
                schema='',
                table='',
                timestamp=datetime.fromtimestamp(event.timestamp) if hasattr(event, 'timestamp') else datetime.now(),
                gtid=event.gtid if hasattr(event, 'gtid') else None,
                log_file=log_file,
                log_pos=log_pos,
            ))

        return records


# ============================================================================
# Arrow Conversion Utilities
# ============================================================================

def cdc_records_to_arrow_batch(records: List[CDCRecord]) -> ca.RecordBatch:
    """
    Convert CDC records to Arrow RecordBatch.

    Schema:
        event_type: string
        schema: string
        table: string
        timestamp: timestamp[us]
        data: string (JSON-encoded)
        old_data: string (JSON-encoded, nullable)
        log_file: string
        log_pos: int64
        gtid: string (nullable)
        query: string (nullable)
    """
    import json

    # Build column arrays
    event_types = []
    schemas = []
    tables = []
    timestamps = []
    data_json = []
    old_data_json = []
    log_files = []
    log_positions = []
    gtids = []
    queries = []

    for record in records:
        event_types.append(record.event_type)
        schemas.append(record.schema)
        tables.append(record.table)
        timestamps.append(record.timestamp)
        data_json.append(json.dumps(record.data) if record.data else None)
        old_data_json.append(json.dumps(record.old_data) if record.old_data else None)
        log_files.append(record.log_file or "")
        log_positions.append(record.log_pos or 0)
        gtids.append(record.gtid)
        queries.append(record.query)

    # Create Arrow arrays
    arrays = [
        ca.array(event_types, type=ca.string()),
        ca.array(schemas, type=ca.string()),
        ca.array(tables, type=ca.string()),
        ca.array(timestamps, type=ca.timestamp('us')),
        ca.array(data_json, type=ca.string()),
        ca.array(old_data_json, type=ca.string()),
        ca.array(log_files, type=ca.string()),
        ca.array(log_positions, type=ca.int64()),
        ca.array(gtids, type=ca.string()),
        ca.array(queries, type=ca.string()),
    ]

    # Create schema
    schema = ca.schema([
        ('event_type', ca.string()),
        ('schema', ca.string()),
        ('table', ca.string()),
        ('timestamp', ca.timestamp('us')),
        ('data', ca.string()),
        ('old_data', ca.string()),
        ('log_file', ca.string()),
        ('log_pos', ca.int64()),
        ('gtid', ca.string()),
        ('query', ca.string()),
    ])

    return ca.RecordBatch.from_arrays(arrays, schema=schema)


# ============================================================================
# MySQL CDC Connector
# ============================================================================

class MySQLCDCConnector:
    """
    MySQL CDC Connector using binlog replication.

    Streams database changes in real-time using MySQL's binlog replication
    via python-mysql-replication library.

    Example:
        >>> config = MySQLCDCConfig(host="localhost", user="root", password="sabot")
        >>> connector = MySQLCDCConnector(config)
        >>>
        >>> async with connector:
        >>>     async for batch in connector.stream_batches():
        >>>         print(f"Batch: {batch.num_rows} records")
    """

    def __init__(self, config: MySQLCDCConfig):
        """
        Initialize CDC connector.

        Args:
            config: MySQL CDC configuration
        """
        self.config = config
        self._stream: Optional[BinLogStreamReader] = None
        self._running = False
        self._last_log_file: Optional[str] = None
        self._last_log_pos: Optional[int] = None
        self._error_count = 0

        # Schema tracker for DDL changes
        self._schema_tracker: Optional[MySQLSchemaTracker] = None
        if config.track_ddl:
            self._schema_tracker = MySQLSchemaTracker()

            # Track tables from config
            if config.only_tables:
                for table_spec in config.only_tables:
                    if '.' in table_spec:
                        db, table = table_spec.split('.', 1)
                        self._schema_tracker.track_table(db, table)

    async def __aenter__(self):
        """Context manager entry."""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        await self.stop()

    async def start(self):
        """Start the CDC connector."""
        if self._running:
            return

        try:
            # Create BinLogStreamReader
            params = self.config.to_connection_params()

            # Default to only row events if not specified
            if params.get("only_events") is None and params.get("ignored_events") is None:
                params["only_events"] = [WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent]

            self._stream = BinLogStreamReader(**params)
            self._running = True

            logger.info(
                f"MySQL CDC connector started: "
                f"host={self.config.host}, "
                f"server_id={self.config.server_id}"
            )

        except Exception as e:
            logger.error(f"Failed to start CDC connector: {e}")
            await self.stop()
            raise

    async def stop(self):
        """Stop the CDC connector."""
        if not self._running:
            return

        self._running = False

        if self._stream:
            try:
                self._stream.close()
            except Exception as e:
                logger.warning(f"Error closing stream: {e}")
            self._stream = None

        logger.info("MySQL CDC connector stopped")

    async def stream_changes(self) -> AsyncGenerator[List[CDCRecord], None]:
        """
        Stream CDC changes as records.

        Yields:
            Batches of CDC records
        """
        if not self._running:
            await self.start()

        batch = []
        last_yield_time = asyncio.get_event_loop().time()

        try:
            for binlog_event in self._stream:
                # Track position
                if hasattr(binlog_event, 'packet') and hasattr(binlog_event.packet, 'log_pos'):
                    self._last_log_file = getattr(binlog_event, 'log_file', None) or self._last_log_file
                    self._last_log_pos = binlog_event.packet.log_pos

                # Process DDL events with schema tracker
                if isinstance(binlog_event, QueryEvent) and self._schema_tracker:
                    ddl_event = self._schema_tracker.process_query_event(
                        database=binlog_event.schema.decode() if isinstance(binlog_event.schema, bytes) else binlog_event.schema,
                        query=binlog_event.query,
                        timestamp=datetime.fromtimestamp(binlog_event.timestamp),
                        log_file=self._last_log_file or "",
                        log_pos=self._last_log_pos or 0
                    )

                    if ddl_event:
                        # Handle DDL event
                        self._handle_ddl_event(ddl_event)

                        # Include DDL events in stream if configured
                        if not self.config.emit_ddl_events:
                            continue  # Skip emitting DDL event to stream

                # Convert to CDC records
                records = CDCRecord.from_binlog_event(
                    binlog_event,
                    self._last_log_file or "",
                    self._last_log_pos or 0
                )
                batch.extend(records)

                # Yield batch when it reaches target size
                if len(batch) >= self.config.batch_size:
                    yield batch
                    batch = []
                    last_yield_time = asyncio.get_event_loop().time()

                # Yield batch after timeout
                now = asyncio.get_event_loop().time()
                if batch and (now - last_yield_time) > self.config.max_poll_interval:
                    yield batch
                    batch = []
                    last_yield_time = now

                # Allow other tasks to run
                await asyncio.sleep(0)

            # Yield final batch
            if batch:
                yield batch

        except asyncio.CancelledError:
            logger.info("CDC streaming cancelled")
            raise
        except Exception as e:
            logger.error(f"CDC streaming error: {e}")
            self._error_count += 1
            raise

    async def stream_batches(self) -> AsyncGenerator[ca.RecordBatch, None]:
        """
        Stream CDC changes as Arrow RecordBatches.

        Yields:
            Arrow RecordBatches containing CDC events
        """
        async for records in self.stream_changes():
            if records:
                yield cdc_records_to_arrow_batch(records)

    @classmethod
    def auto_configure(
        cls,
        host: str,
        port: int = 3306,
        user: str = "root",
        password: str = "",
        database: Optional[str] = None,
        table_patterns: Union[str, List[str]] = "*.*",
        validate_config: bool = True,
        create_user: bool = False,
        cdc_user: Optional[str] = None,
        cdc_password: Optional[str] = None,
        case_sensitive: bool = True,
        **kwargs
    ) -> 'MySQLCDCConnector':
        """
        Auto-configure MySQL CDC with validation and table discovery.

        Args:
            host: MySQL host
            port: MySQL port
            user: Admin user for setup
            password: Admin password
            database: Database to monitor (None = all databases)
            table_patterns: Table patterns (e.g., "db.*", ["db1.*", "db2.orders"])
            validate_config: Validate MySQL configuration
            create_user: Create dedicated CDC user (requires admin access)
            cdc_user: CDC user name (if create_user=True)
            cdc_password: CDC user password (if create_user=True)
            case_sensitive: Case-sensitive pattern matching
            **kwargs: Additional MySQLCDCConfig options

        Returns:
            Configured MySQLCDCConnector instance

        Example:
            >>> # Auto-configure with validation
            >>> connector = MySQLCDCConnector.auto_configure(
            ...     host="localhost",
            ...     user="root",
            ...     password="password",
            ...     table_patterns=["ecommerce.*", "analytics.events_*"],
            ...     validate_config=True
            ... )
            >>>
            >>> # With dedicated CDC user
            >>> connector = MySQLCDCConnector.auto_configure(
            ...     host="localhost",
            ...     user="root",
            ...     password="admin_pass",
            ...     table_patterns="ecommerce.*",
            ...     create_user=True,
            ...     cdc_user="cdc_user",
            ...     cdc_password="cdc_pass"
            ... )
        """
        from .mysql_auto_config import MySQLAutoConfigurator, Severity
        from .mysql_discovery import MySQLTableDiscovery

        # Step 1: Validate configuration
        if validate_config:
            configurator = MySQLAutoConfigurator(
                host=host,
                port=port,
                admin_user=user,
                admin_password=password
            )

            issues = configurator.validate_configuration(
                require_gtid=kwargs.get('auto_position', False)
            )

            if issues:
                error_issues = [i for i in issues if i.severity == Severity.ERROR]
                if error_issues:
                    error_msg = "MySQL not configured for CDC:\n\n"
                    for issue in error_issues:
                        error_msg += str(issue) + "\n"

                    logger.error(error_msg)
                    raise RuntimeError(error_msg)

                # Log warnings
                for issue in issues:
                    if issue.severity == Severity.WARNING:
                        logger.warning(f"{issue.message}: {issue.fix_sql}")

        # Step 2: Create CDC user (if requested)
        if create_user:
            if not cdc_user or not cdc_password:
                raise ValueError("cdc_user and cdc_password required when create_user=True")

            configurator = MySQLAutoConfigurator(
                host=host,
                port=port,
                admin_user=user,
                admin_password=password
            )

            configurator.create_replication_user(cdc_user, cdc_password)

            # Use CDC user for connector
            user = cdc_user
            password = cdc_password

        # Step 3: Discover tables
        if isinstance(table_patterns, str):
            table_patterns = [table_patterns]

        discovery = MySQLTableDiscovery(
            host=host,
            port=port,
            user=user,
            password=password
        )

        matched_tables = discovery.expand_patterns(
            table_patterns,
            database=database,
            case_sensitive=case_sensitive
        )

        if not matched_tables:
            raise ValueError(f"No tables matched patterns: {table_patterns}")

        logger.info(f"Auto-configured CDC for {len(matched_tables)} tables: " +
                   (f"{matched_tables}" if len(matched_tables) <= 10 else f"{matched_tables[:10]}..."))

        # Step 4: Create connector with discovered tables
        config = MySQLCDCConfig(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            only_tables=[f"{db}.{tbl}" for db, tbl in matched_tables],
            **kwargs
        )

        return cls(config)

    def _handle_ddl_event(self, ddl_event: DDLEvent):
        """
        Handle DDL event by updating schema tracker.

        Args:
            ddl_event: DDL event to process
        """
        from .mysql_schema_tracker import DDLType

        if ddl_event.ddl_type == DDLType.DROP_TABLE and ddl_event.table:
            self._schema_tracker.remove_table(ddl_event.database, ddl_event.table)
            logger.info(f"Schema tracker: Removed {ddl_event.database}.{ddl_event.table}")

        elif ddl_event.ddl_type == DDLType.RENAME_TABLE:
            if ddl_event.old_table_name and ddl_event.new_table_name:
                old_db, old_table = ddl_event.old_table_name.rsplit('.', 1)
                new_db, new_table = ddl_event.new_table_name.rsplit('.', 1)
                self._schema_tracker.rename_table(old_db, old_table, new_db, new_table)
                logger.info(f"Schema tracker: Renamed {old_db}.{old_table} -> {new_db}.{new_table}")

        elif ddl_event.ddl_type == DDLType.ALTER_TABLE and ddl_event.table:
            self._schema_tracker.invalidate_table(ddl_event.database, ddl_event.table)
            logger.info(f"Schema tracker: Invalidated {ddl_event.database}.{ddl_event.table}")

        elif ddl_event.ddl_type == DDLType.CREATE_TABLE and ddl_event.table:
            # Track newly created table if it matches our patterns
            if self.config.only_tables:
                table_spec = f"{ddl_event.database}.{ddl_event.table}"
                if table_spec in self.config.only_tables:
                    self._schema_tracker.track_table(ddl_event.database, ddl_event.table)
                    logger.info(f"Schema tracker: Now tracking {table_spec}")

    def get_schema_tracker(self) -> Optional[MySQLSchemaTracker]:
        """
        Get schema tracker for DDL tracking.

        Returns:
            MySQLSchemaTracker instance if enabled, None otherwise
        """
        return self._schema_tracker

    def get_status(self) -> Dict[str, Any]:
        """Get connector status."""
        return {
            "running": self._running,
            "connected": self._stream is not None,
            "last_log_file": self._last_log_file,
            "last_log_pos": self._last_log_pos,
            "error_count": self._error_count,
            "server_id": self.config.server_id,
            "host": self.config.host,
            "database": self.config.database,
        }

    def get_position(self) -> Dict[str, Any]:
        """Get current binlog position for checkpointing."""
        return {
            "log_file": self._last_log_file,
            "log_pos": self._last_log_pos,
        }


# ============================================================================
# Convenience Functions
# ============================================================================

def create_mysql_cdc_connector(
    host: str = "localhost",
    port: int = 3306,
    user: str = "root",
    password: str = "",
    database: Optional[str] = None,
    **kwargs
) -> MySQLCDCConnector:
    """
    Create a MySQL CDC connector.

    Args:
        host: MySQL host
        port: MySQL port
        user: Username
        password: Password
        database: Database name (None = all databases)
        **kwargs: Additional MySQLCDCConfig options

    Returns:
        Configured MySQLCDCConnector instance
    """
    config = MySQLCDCConfig(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        **kwargs
    )
    return MySQLCDCConnector(config)
