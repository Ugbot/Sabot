"""
MySQL schema change tracking.

Handles DDL event detection and schema cache management for MySQL CDC,
providing schema evolution tracking similar to PostgreSQL CDC.
"""

from dataclasses import dataclass, field
from typing import Dict, Optional, List, Set, Any
from enum import Enum
import logging
import re
from datetime import datetime

logger = logging.getLogger(__name__)


class DDLType(Enum):
    """DDL operation types."""
    CREATE_TABLE = "CREATE_TABLE"
    DROP_TABLE = "DROP_TABLE"
    ALTER_TABLE = "ALTER_TABLE"
    RENAME_TABLE = "RENAME_TABLE"
    TRUNCATE_TABLE = "TRUNCATE_TABLE"
    CREATE_DATABASE = "CREATE_DATABASE"
    DROP_DATABASE = "DROP_DATABASE"
    CREATE_INDEX = "CREATE_INDEX"
    DROP_INDEX = "DROP_INDEX"
    UNKNOWN = "UNKNOWN"


@dataclass
class DDLEvent:
    """
    DDL change event.

    Emitted when schema changes are detected in the binlog.
    """
    ddl_type: DDLType
    database: str
    table: Optional[str]
    query: str
    timestamp: datetime
    log_file: str
    log_pos: int

    # Parsed details (when available)
    old_table_name: Optional[str] = None  # For RENAME
    new_table_name: Optional[str] = None  # For RENAME
    affected_columns: List[str] = field(default_factory=list)  # For ALTER

    def __repr__(self):
        if self.table:
            return (f"DDLEvent({self.ddl_type.value}, "
                   f"{self.database}.{self.table}, "
                   f"{self.log_file}:{self.log_pos})")
        return f"DDLEvent({self.ddl_type.value}, {self.database})"


@dataclass
class TableSchema:
    """
    Cached table schema metadata.

    Stores schema version for invalidation on DDL changes.
    """
    database: str
    table: str
    columns: Dict[str, str]  # column_name -> column_type
    primary_key: List[str]
    schema_version: int  # Incremented on ALTER TABLE
    last_modified: datetime

    def invalidate(self):
        """Mark schema as stale (increment version)."""
        self.schema_version += 1
        self.last_modified = datetime.now()


class MySQLSchemaTracker:
    """
    Track MySQL schema changes from binlog QueryEvents.

    Features:
    - DDL event detection from QueryEvent
    - Schema cache with versioning
    - Table/database creation/drop tracking
    - ALTER TABLE column tracking
    - RENAME TABLE tracking

    Example:
        >>> tracker = MySQLSchemaTracker()
        >>>
        >>> # Process QueryEvent from binlog
        >>> ddl_event = tracker.process_query_event(
        ...     database="ecommerce",
        ...     query="ALTER TABLE users ADD COLUMN email VARCHAR(255)",
        ...     timestamp=datetime.now(),
        ...     log_file="mysql-bin.000001",
        ...     log_pos=12345
        ... )
        >>>
        >>> if ddl_event:
        ...     print(f"DDL detected: {ddl_event.ddl_type}")
        ...     # Invalidate cached schema
        ...     tracker.invalidate_table("ecommerce", "users")
    """

    def __init__(self):
        """Initialize schema tracker."""
        self._schema_cache: Dict[tuple, TableSchema] = {}  # (database, table) -> TableSchema
        self._tracked_databases: Set[str] = set()
        self._tracked_tables: Set[tuple] = set()  # (database, table)

        # DDL pattern regexes
        self._ddl_patterns = self._compile_ddl_patterns()

    def _compile_ddl_patterns(self) -> Dict[DDLType, re.Pattern]:
        """Compile regex patterns for DDL detection."""
        return {
            DDLType.CREATE_TABLE: re.compile(
                r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?`?(\w+)`?\.?`?(\w+)?`?',
                re.IGNORECASE
            ),
            DDLType.DROP_TABLE: re.compile(
                r'DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?`?(\w+)`?\.?`?(\w+)?`?',
                re.IGNORECASE
            ),
            DDLType.ALTER_TABLE: re.compile(
                r'ALTER\s+TABLE\s+`?(\w+)`?\.?`?(\w+)?`?\s+(.*)',
                re.IGNORECASE
            ),
            DDLType.RENAME_TABLE: re.compile(
                r'RENAME\s+TABLE\s+`?(\w+)`?\.?`?(\w+)?`?\s+TO\s+`?(\w+)`?\.?`?(\w+)?`?',
                re.IGNORECASE
            ),
            DDLType.TRUNCATE_TABLE: re.compile(
                r'TRUNCATE\s+(?:TABLE\s+)?`?(\w+)`?\.?`?(\w+)?`?',
                re.IGNORECASE
            ),
            DDLType.CREATE_DATABASE: re.compile(
                r'CREATE\s+(?:DATABASE|SCHEMA)\s+(?:IF\s+NOT\s+EXISTS\s+)?`?(\w+)`?',
                re.IGNORECASE
            ),
            DDLType.DROP_DATABASE: re.compile(
                r'DROP\s+(?:DATABASE|SCHEMA)\s+(?:IF\s+EXISTS\s+)?`?(\w+)`?',
                re.IGNORECASE
            ),
            DDLType.CREATE_INDEX: re.compile(
                r'CREATE\s+(?:UNIQUE\s+)?INDEX\s+.*\s+ON\s+`?(\w+)`?\.?`?(\w+)?`?',
                re.IGNORECASE
            ),
            DDLType.DROP_INDEX: re.compile(
                r'DROP\s+INDEX\s+.*\s+ON\s+`?(\w+)`?\.?`?(\w+)?`?',
                re.IGNORECASE
            ),
        }

    def process_query_event(
        self,
        database: str,
        query: str,
        timestamp: datetime,
        log_file: str,
        log_pos: int
    ) -> Optional[DDLEvent]:
        """
        Process QueryEvent from binlog and detect DDL changes.

        Args:
            database: Current database context
            query: SQL query from QueryEvent
            timestamp: Event timestamp
            log_file: Binlog file
            log_pos: Binlog position

        Returns:
            DDLEvent if DDL detected, None otherwise
        """
        query_stripped = query.strip()

        # Try to match each DDL pattern
        for ddl_type, pattern in self._ddl_patterns.items():
            match = pattern.match(query_stripped)
            if match:
                return self._create_ddl_event(
                    ddl_type=ddl_type,
                    match=match,
                    database=database,
                    query=query,
                    timestamp=timestamp,
                    log_file=log_file,
                    log_pos=log_pos
                )

        # Not a DDL statement we track
        return None

    def _create_ddl_event(
        self,
        ddl_type: DDLType,
        match: re.Match,
        database: str,
        query: str,
        timestamp: datetime,
        log_file: str,
        log_pos: int
    ) -> DDLEvent:
        """Create DDLEvent from regex match."""
        groups = match.groups()

        # Handle different DDL types
        if ddl_type == DDLType.RENAME_TABLE:
            # RENAME TABLE old_db.old_table TO new_db.new_table
            old_db, old_table, new_db, new_table = groups[:4]
            table = old_table or old_db  # Handle both `db.table` and `table` formats
            db = database if not old_table else old_db

            return DDLEvent(
                ddl_type=ddl_type,
                database=db,
                table=table,
                query=query,
                timestamp=timestamp,
                log_file=log_file,
                log_pos=log_pos,
                old_table_name=f"{old_db}.{old_table}" if old_table else old_db,
                new_table_name=f"{new_db}.{new_table}" if new_table else new_db
            )

        elif ddl_type == DDLType.ALTER_TABLE:
            # ALTER TABLE db.table ADD/DROP/MODIFY/CHANGE ...
            db_or_table, table_if_db, alter_spec = groups[:3]
            table = table_if_db or db_or_table
            db = database if not table_if_db else db_or_table

            # Parse affected columns from ALTER spec
            affected_columns = self._parse_alter_columns(alter_spec)

            return DDLEvent(
                ddl_type=ddl_type,
                database=db,
                table=table,
                query=query,
                timestamp=timestamp,
                log_file=log_file,
                log_pos=log_pos,
                affected_columns=affected_columns
            )

        elif ddl_type in (DDLType.CREATE_DATABASE, DDLType.DROP_DATABASE):
            # CREATE/DROP DATABASE db
            db = groups[0]
            return DDLEvent(
                ddl_type=ddl_type,
                database=db,
                table=None,
                query=query,
                timestamp=timestamp,
                log_file=log_file,
                log_pos=log_pos
            )

        else:
            # CREATE/DROP/TRUNCATE TABLE, CREATE/DROP INDEX
            db_or_table, table_if_db = groups[:2]
            table = table_if_db or db_or_table
            db = database if not table_if_db else db_or_table

            return DDLEvent(
                ddl_type=ddl_type,
                database=db,
                table=table,
                query=query,
                timestamp=timestamp,
                log_file=log_file,
                log_pos=log_pos
            )

    def _parse_alter_columns(self, alter_spec: str) -> List[str]:
        """
        Parse column names from ALTER TABLE specification.

        Examples:
            "ADD COLUMN email VARCHAR(255)" -> ["email"]
            "DROP COLUMN age" -> ["age"]
            "MODIFY COLUMN name VARCHAR(100)" -> ["name"]
            "CHANGE COLUMN old_name new_name INT" -> ["old_name", "new_name"]
        """
        columns = []

        # ADD COLUMN
        match = re.search(r'ADD\s+(?:COLUMN\s+)?`?(\w+)`?', alter_spec, re.IGNORECASE)
        if match:
            columns.append(match.group(1))

        # DROP COLUMN
        match = re.search(r'DROP\s+(?:COLUMN\s+)?`?(\w+)`?', alter_spec, re.IGNORECASE)
        if match:
            columns.append(match.group(1))

        # MODIFY COLUMN
        match = re.search(r'MODIFY\s+(?:COLUMN\s+)?`?(\w+)`?', alter_spec, re.IGNORECASE)
        if match:
            columns.append(match.group(1))

        # CHANGE COLUMN old_name new_name
        match = re.search(r'CHANGE\s+(?:COLUMN\s+)?`?(\w+)`?\s+`?(\w+)`?', alter_spec, re.IGNORECASE)
        if match:
            columns.extend([match.group(1), match.group(2)])

        return columns

    def invalidate_table(self, database: str, table: str):
        """
        Invalidate cached schema for table.

        Call this when DDL events modify table structure.

        Args:
            database: Database name
            table: Table name
        """
        key = (database, table)
        if key in self._schema_cache:
            self._schema_cache[key].invalidate()
            logger.info(f"Invalidated schema cache for {database}.{table}")

    def remove_table(self, database: str, table: str):
        """
        Remove table from schema cache.

        Call this for DROP TABLE events.

        Args:
            database: Database name
            table: Table name
        """
        key = (database, table)
        if key in self._schema_cache:
            del self._schema_cache[key]
            logger.info(f"Removed schema cache for {database}.{table}")

        if key in self._tracked_tables:
            self._tracked_tables.remove(key)

    def rename_table(
        self,
        old_database: str,
        old_table: str,
        new_database: str,
        new_table: str
    ):
        """
        Update schema cache for renamed table.

        Args:
            old_database: Old database name
            old_table: Old table name
            new_database: New database name
            new_table: New table name
        """
        old_key = (old_database, old_table)
        new_key = (new_database, new_table)

        if old_key in self._schema_cache:
            schema = self._schema_cache.pop(old_key)
            schema.database = new_database
            schema.table = new_table
            schema.invalidate()
            self._schema_cache[new_key] = schema
            logger.info(f"Renamed schema cache: {old_database}.{old_table} -> {new_database}.{new_table}")

        if old_key in self._tracked_tables:
            self._tracked_tables.remove(old_key)
            self._tracked_tables.add(new_key)

    def update_schema(
        self,
        database: str,
        table: str,
        columns: Dict[str, str],
        primary_key: List[str]
    ):
        """
        Update cached schema for table.

        Args:
            database: Database name
            table: Table name
            columns: Column name -> type mapping
            primary_key: List of primary key columns
        """
        key = (database, table)

        if key in self._schema_cache:
            schema = self._schema_cache[key]
            schema.columns = columns
            schema.primary_key = primary_key
            schema.invalidate()
        else:
            self._schema_cache[key] = TableSchema(
                database=database,
                table=table,
                columns=columns,
                primary_key=primary_key,
                schema_version=1,
                last_modified=datetime.now()
            )

        self._tracked_tables.add(key)
        logger.debug(f"Updated schema cache for {database}.{table}")

    def get_schema(
        self,
        database: str,
        table: str
    ) -> Optional[TableSchema]:
        """
        Get cached schema for table.

        Args:
            database: Database name
            table: Table name

        Returns:
            TableSchema if cached, None otherwise
        """
        return self._schema_cache.get((database, table))

    def is_tracked(self, database: str, table: str) -> bool:
        """
        Check if table is being tracked.

        Args:
            database: Database name
            table: Table name

        Returns:
            True if tracked
        """
        return (database, table) in self._tracked_tables

    def track_table(self, database: str, table: str):
        """
        Start tracking table for schema changes.

        Args:
            database: Database name
            table: Table name
        """
        self._tracked_tables.add((database, table))
        self._tracked_databases.add(database)
        logger.info(f"Now tracking schema for {database}.{table}")

    def untrack_table(self, database: str, table: str):
        """
        Stop tracking table.

        Args:
            database: Database name
            table: Table name
        """
        key = (database, table)
        if key in self._tracked_tables:
            self._tracked_tables.remove(key)
            logger.info(f"Stopped tracking schema for {database}.{table}")

    def get_tracked_tables(self) -> List[tuple]:
        """
        Get list of tracked tables.

        Returns:
            List of (database, table) tuples
        """
        return sorted(list(self._tracked_tables))

    def clear_cache(self):
        """Clear all cached schemas."""
        self._schema_cache.clear()
        logger.info("Cleared schema cache")
