#!/usr/bin/env python3
"""
PostgreSQL CDC Connector for Sabot.

Provides Change Data Capture functionality using PostgreSQL logical replication
with wal2json output plugin. Streams database changes in real-time.

Features:
- Logical replication with wal2json
- Automatic slot management
- Configurable output format (v1/v2)
- Error handling and recovery
- Backpressure support
"""

import asyncio
import logging
from typing import Optional, Dict, Any, List, AsyncGenerator, Union
from dataclasses import dataclass
from contextlib import asynccontextmanager

from .libpq_conn import PostgreSQLConnection, ReplicationMessage
from .wal2json_parser import Wal2JsonParser, CDCEvent, CDCTransaction

logger = logging.getLogger(__name__)


# ============================================================================
# Configuration
# ============================================================================

@dataclass
class PostgreSQLCDCConfig:
    """Configuration for PostgreSQL CDC connector."""

    # Connection settings
    host: str = "localhost"
    port: int = 5432
    database: str = "postgres"
    user: str = "postgres"
    password: str = ""
    connection_timeout: int = 30

    # Replication settings
    replication_slot: str = "sabot_cdc_slot"
    output_plugin: str = "wal2json"
    format_version: int = 2  # wal2json format version (1 or 2)

    # Wal2json options (passed as plugin_args)
    include_xids: bool = True
    include_timestamps: bool = True
    include_lsn: bool = True
    include_schemas: bool = True
    include_types: bool = True
    include_type_oids: bool = False
    include_typmod: bool = False
    include_domain_data_type: bool = False
    include_column_positions: bool = False
    include_not_null: bool = False
    include_default: bool = False
    include_pk: bool = True
    pretty_print: bool = False
    write_in_chunks: bool = False
    numeric_data_types_as_string: bool = False

    # Filter settings
    filter_tables: Optional[List[str]] = None  # e.g., ["public.users", "public.orders"]
    add_tables: Optional[List[str]] = None     # Alternative: only these tables
    filter_origins: Optional[List[str]] = None # Filter by replication origin

    # Message filtering (v2 only)
    add_msg_prefixes: Optional[List[str]] = None  # e.g., ["wal2json"]
    filter_msg_prefixes: Optional[List[str]] = None

    # Streaming settings
    batch_size: int = 100  # Max events per batch
    max_poll_interval: float = 1.0  # Max time to wait for new data
    heartbeat_interval: float = 10.0  # Send heartbeat every N seconds

    # Error handling
    max_retry_attempts: int = 3
    retry_backoff_seconds: float = 1.0
    dead_letter_topic: Optional[str] = None

    def to_connection_string(self) -> str:
        """Convert config to PostgreSQL connection string."""
        parts = [
            f"host={self.host}",
            f"port={self.port}",
            f"dbname={self.database}",
            f"user={self.user}",
            f"connect_timeout={self.connection_timeout}"
        ]

        if self.password:
            parts.append(f"password={self.password}")

        # Replication connection settings
        parts.extend([
            "replication=database",  # Required for logical replication
            "fallback_application_name=sabot_cdc"
        ])

        return " ".join(parts)

    def to_plugin_args(self) -> List[str]:
        """Convert wal2json options to plugin arguments."""
        args = []

        # Format version
        args.append(f"format-version={self.format_version}")

        # Include options
        if self.include_xids:
            args.append("include-xids=1")
        if self.include_timestamps:
            args.append("include-timestamp=1")
        if self.include_lsn:
            args.append("include-lsn=1")
        if self.include_schemas:
            args.append("include-schemas=1")
        if self.include_types:
            args.append("include-types=1")
        if self.include_type_oids:
            args.append("include-type-oids=1")
        if self.include_typmod:
            args.append("include-typmod=1")
        if self.include_domain_data_type:
            args.append("include-domain-data-type=1")
        if self.include_column_positions:
            args.append("include-column-positions=1")
        if self.include_not_null:
            args.append("include-not-null=1")
        if self.include_default:
            args.append("include-default=1")
        if self.include_pk:
            args.append("include-pk=1")

        # Other options
        if self.pretty_print:
            args.append("pretty-print=1")
        if self.write_in_chunks:
            args.append("write-in-chunks=1")
        if self.numeric_data_types_as_string:
            args.append("numeric-data-types-as-string=1")

        # Filters
        if self.filter_tables:
            for table in self.filter_tables:
                args.append(f"filter-tables={table}")
        if self.add_tables:
            for table in self.add_tables:
                args.append(f"add-tables={table}")
        if self.filter_origins:
            for origin in self.filter_origins:
                args.append(f"filter-origins={origin}")

        # Message filters (v2 only)
        if self.add_msg_prefixes:
            for prefix in self.add_msg_prefixes:
                args.append(f"add-msg-prefixes={prefix}")
        if self.filter_msg_prefixes:
            for prefix in self.filter_msg_prefixes:
                args.append(f"filter-msg-prefixes={prefix}")

        return args


# ============================================================================
# CDC Event Types (Exposed to Users)
# ============================================================================

@dataclass
class CDCRecord:
    """High-level CDC record for user consumption."""

    event_type: str  # 'insert', 'update', 'delete', 'truncate', 'message'
    schema: str
    table: str
    data: Optional[Dict[str, Any]] = None  # New/changed data
    old_data: Optional[Dict[str, Any]] = None  # Old data (for updates)
    key_data: Optional[Dict[str, Any]] = None  # Primary key data
    lsn: Optional[str] = None
    timestamp: Optional[Any] = None  # datetime
    transaction_id: Optional[int] = None
    origin: Optional[str] = None

    # Message-specific fields
    message_prefix: Optional[str] = None
    message_content: Optional[str] = None
    transactional: Optional[bool] = None

    @classmethod
    def from_cdc_event(cls, event: Union[CDCEvent, CDCTransaction]) -> List['CDCRecord']:
        """Convert CDC event/transaction to user-friendly records."""
        records = []

        if isinstance(event, CDCTransaction):
            # Format v1: transaction with multiple changes
            for change in event.changes:
                records.extend(cls.from_cdc_event(change))
            return records

        elif isinstance(event, CDCEvent):
            # Format v2: individual event
            if event.action == 'B':
                # Transaction begin marker
                return [cls(
                    event_type='begin',
                    schema='',
                    table='',
                    transaction_id=event.xid,
                    lsn=event.lsn,
                    timestamp=event.timestamp,
                    origin=event.origin
                )]

            elif event.action == 'C':
                # Transaction commit marker
                return [cls(
                    event_type='commit',
                    schema='',
                    table='',
                    transaction_id=event.xid,
                    lsn=event.lsn,
                    timestamp=event.timestamp,
                    origin=event.origin
                )]

            elif event.action == 'I':
                # INSERT
                data = {col.name: col.value for col in (event.columns or [])}
                return [cls(
                    event_type='insert',
                    schema=event.schema,
                    table=event.table,
                    data=data,
                    lsn=event.lsn,
                    timestamp=event.timestamp,
                    transaction_id=event.xid,
                    origin=event.origin
                )]

            elif event.action == 'U':
                # UPDATE
                data = {col.name: col.value for col in (event.columns or [])}
                key_data = None
                if event.identity:
                    key_data = dict(zip(event.identity.names, event.identity.values))
                return [cls(
                    event_type='update',
                    schema=event.schema,
                    table=event.table,
                    data=data,
                    key_data=key_data,
                    lsn=event.lsn,
                    timestamp=event.timestamp,
                    transaction_id=event.xid,
                    origin=event.origin
                )]

            elif event.action == 'D':
                # DELETE
                key_data = None
                if event.identity:
                    key_data = dict(zip(event.identity.names, event.identity.values))
                return [cls(
                    event_type='delete',
                    schema=event.schema,
                    table=event.table,
                    key_data=key_data,
                    lsn=event.lsn,
                    timestamp=event.timestamp,
                    transaction_id=event.xid,
                    origin=event.origin
                )]

            elif event.action == 'T':
                # TRUNCATE
                return [cls(
                    event_type='truncate',
                    schema=event.schema,
                    table=event.table,
                    lsn=event.lsn,
                    timestamp=event.timestamp,
                    transaction_id=event.xid,
                    origin=event.origin
                )]

            elif event.action == 'M':
                # MESSAGE
                return [cls(
                    event_type='message',
                    schema='',
                    table='',
                    message_prefix=event.prefix,
                    message_content=event.content,
                    transactional=event.transactional,
                    lsn=event.lsn
                )]

            else:
                logger.warning(f"Unknown event action: {event.action}")
                return []

        return []


# ============================================================================
# PostgreSQL CDC Connector
# ============================================================================

class PostgreSQLCDCConnector:
    """
    PostgreSQL CDC Connector using logical replication.

    Streams database changes in real-time using PostgreSQL's logical replication
    with wal2json output plugin.

    Example:
        >>> config = PostgreSQLCDCConfig(host="localhost", database="mydb")
        >>> connector = PostgreSQLCDCConnector(config)
        >>>
        >>> async with connector:
        >>>     async for records in connector.stream_changes():
        >>>         for record in records:
        >>>             print(f"Change: {record.event_type} on {record.schema}.{record.table}")
    """

    def __init__(self, config: PostgreSQLCDCConfig):
        """
        Initialize CDC connector.

        Args:
            config: PostgreSQL CDC configuration
        """
        self.config = config
        self._connection: Optional[PostgreSQLConnection] = None
        self._parser: Optional[Wal2JsonParser] = None
        self._running = False
        self._last_lsn = 0
        self._error_count = 0

        # Create parser
        self._parser = Wal2JsonParser(format_version=config.format_version)

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
            # Create connection
            self._connection = PostgreSQLConnection(self.config.to_connection_string())
            self._connection.connect()

            # Ensure wal2json is available
            await self._ensure_wal2json()

            # Create replication slot if it doesn't exist
            await self._ensure_replication_slot()

            # Start replication
            plugin_args = self.config.to_plugin_args()
            self._connection.start_replication(
                self.config.replication_slot,
                start_lsn=self._last_lsn,
                plugin_args=plugin_args
            )

            self._running = True
            logger.info(f"PostgreSQL CDC connector started: slot={self.config.replication_slot}")

        except Exception as e:
            logger.error(f"Failed to start CDC connector: {e}")
            await self.stop()
            raise

    async def stop(self):
        """Stop the CDC connector."""
        if not self._running:
            return

        self._running = False

        if self._connection:
            try:
                self._connection.close()
            except Exception as e:
                logger.warning(f"Error closing connection: {e}")
            self._connection = None

        logger.info("PostgreSQL CDC connector stopped")

    async def _ensure_wal2json(self):
        """Ensure wal2json extension is installed."""
        try:
            # Check if wal2json extension exists
            result = self._connection.execute("SELECT name FROM pg_available_extensions WHERE name = 'wal2json'")
            if not result:
                raise RuntimeError("wal2json extension not available. Please install it in PostgreSQL.")

            # Try to create extension (will succeed if not already exists)
            self._connection.execute("CREATE EXTENSION IF NOT EXISTS wal2json")

        except Exception as e:
            logger.error(f"wal2json extension check failed: {e}")
            raise RuntimeError("wal2json extension is required for CDC") from e

    async def _ensure_replication_slot(self):
        """Ensure replication slot exists."""
        try:
            # Check if slot exists
            result = self._connection.execute("""
                SELECT slot_name FROM pg_replication_slots
                WHERE slot_name = %s AND plugin = %s
            """, [self.config.replication_slot, self.config.output_plugin])

            if not result:
                # Create slot
                self._connection.create_replication_slot(
                    self.config.replication_slot,
                    self.config.output_plugin
                )
                logger.info(f"Created replication slot: {self.config.replication_slot}")

        except Exception as e:
            logger.error(f"Replication slot setup failed: {e}")
            raise

    async def stream_changes(self) -> AsyncGenerator[List[CDCRecord], None]:
        """
        Stream CDC changes.

        Yields:
            Batches of CDC records
        """
        if not self._running:
            await self.start()

        batch = []
        last_heartbeat = asyncio.get_event_loop().time()

        try:
            async for replication_msg in self._connection.stream_replication():
                # Parse wal2json data
                cdc_events = self._parser.parse_replication_message(replication_msg)

                # Convert to user records
                for cdc_event in cdc_events:
                    records = CDCRecord.from_cdc_event(cdc_event)
                    batch.extend(records)

                    # Yield batch when it reaches target size
                    if len(batch) >= self.config.batch_size:
                        yield batch
                        batch = []

                # Update LSN for recovery
                if replication_msg.wal_end > self._last_lsn:
                    self._last_lsn = replication_msg.wal_end

                # Send periodic heartbeats
                now = asyncio.get_event_loop().time()
                if now - last_heartbeat > self.config.heartbeat_interval:
                    self._connection.send_standby_status(
                        write_lsn=self._last_lsn,
                        flush_lsn=self._last_lsn,
                        apply_lsn=self._last_lsn
                    )
                    last_heartbeat = now

                # Yield remaining batch after timeout
                if batch and now - last_heartbeat > self.config.max_poll_interval:
                    yield batch
                    batch = []
                    last_heartbeat = now

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

    def get_status(self) -> Dict[str, Any]:
        """Get connector status."""
        return {
            "running": self._running,
            "connected": self._connection.is_connected if self._connection else False,
            "replication_active": self._connection.is_replication if self._connection else False,
            "last_lsn": self._last_lsn,
            "error_count": self._error_count,
            "slot_name": self.config.replication_slot,
            "database": self.config.database,
            "host": self.config.host,
            "format_version": self.config.format_version,
        }


# ============================================================================
# Convenience Functions
# ============================================================================

def create_postgresql_cdc_connector(
    host: str = "localhost",
    port: int = 5432,
    database: str = "postgres",
    user: str = "postgres",
    password: str = "",
    replication_slot: str = "sabot_cdc_slot",
    **kwargs
) -> PostgreSQLCDCConnector:
    """
    Create a PostgreSQL CDC connector.

    Args:
        host: PostgreSQL host
        port: PostgreSQL port
        database: Database name
        user: Username
        password: Password
        replication_slot: Replication slot name
        **kwargs: Additional PostgreSQLCDCConfig options

    Returns:
        Configured PostgreSQLCDCConnector instance
    """
    config = PostgreSQLCDCConfig(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
        replication_slot=replication_slot,
        **kwargs
    )
    return PostgreSQLCDCConnector(config)






