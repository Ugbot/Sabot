# MySQL CDC Feature Parity Implementation Guide

**Goal:** Achieve feature parity with PostgreSQL CDC connector

**Effort:** ~10 days (2 weeks)
**Status:** Phase 1 complete (basic CDC), Phase 1B planned (feature parity)

---

## Feature Gap Analysis

| Feature | PostgreSQL CDC | MySQL CDC (Current) | Status |
|---------|---------------|---------------------|---------|
| Basic CDC | ✅ | ✅ | Complete |
| Arrow output | ✅ | ✅ | Complete |
| Wildcard table selection | ✅ `public.*` | ❌ | **TODO** |
| Auto-configuration | ✅ | ❌ | **TODO** |
| DDL change tracking | ✅ | ⚠️ Partial | **TODO** |
| Schema evolution | ✅ | ❌ | **TODO** |
| Per-table routing | ✅ Shuffle-based | ❌ | **TODO** |
| Auto SQL setup | ✅ | ❌ | **TODO** |
| Configuration validation | ✅ | ❌ | **TODO** |

---

## Implementation Phases

### Phase 1A: Wildcard Table Selection (2 days)

**Goal:** Support Flink CDC-style table patterns

#### Files to Create

**1. `sabot/connectors/mysql_table_patterns.py`**

```python
"""
Table pattern matching for MySQL CDC (Flink CDC compatible).

Supports patterns like:
- ecommerce.users          # Single table
- ecommerce.*              # All tables in database
- ecommerce.user_*         # Wildcard match
- *.orders                 # Table in any database
- ecommerce.user_[0-9]+    # Regex pattern
"""

import re
from typing import List, Pattern, Tuple
from dataclasses import dataclass

@dataclass
class TablePattern:
    """
    Table pattern for matching MySQL tables.

    MySQL uses database.table (vs PostgreSQL schema.table).
    """

    database: str
    table_pattern: str
    database_regex: Pattern
    table_regex: Pattern

    @classmethod
    def parse(cls, pattern: str) -> 'TablePattern':
        """
        Parse table pattern string.

        Args:
            pattern: Pattern like 'db.users', 'db.*', 'app_db_*.orders'

        Returns:
            TablePattern instance

        Examples:
            >>> TablePattern.parse('ecommerce.users')
            TablePattern(database='ecommerce', table_pattern='users')

            >>> TablePattern.parse('ecommerce.*')
            TablePattern(database='ecommerce', table_pattern='.*')

            >>> TablePattern.parse('*.user_[0-9]+')
            TablePattern(database='*', table_pattern='user_[0-9]+')
        """
        # Split database.table
        if '.' in pattern:
            database, table = pattern.split('.', 1)
        else:
            # Default to all databases
            database = '*'
            table = pattern

        # Convert SQL wildcards to regex
        # * → .* (match any characters)
        # ? → . (match single character)
        db_regex_str = database.replace('*', '.*').replace('?', '.')
        tbl_regex_str = table.replace('*', '.*').replace('?', '.')

        # MySQL table names are case-sensitive on Linux, case-insensitive on Windows/Mac
        # Default to case-sensitive for compatibility
        return cls(
            database=database,
            table_pattern=table,
            database_regex=re.compile(f'^{db_regex_str}$'),
            table_regex=re.compile(f'^{tbl_regex_str}$')
        )

    def matches(self, database: str, table: str) -> bool:
        """
        Check if database.table matches this pattern.

        Args:
            database: MySQL database name
            table: Table name

        Returns:
            True if matches
        """
        if not self.database_regex.match(database):
            return False
        if not self.table_regex.match(table):
            return False
        return True


class TablePatternMatcher:
    """
    Matches MySQL tables against Flink CDC-style patterns.

    Example:
        >>> matcher = TablePatternMatcher(['ecommerce.*', 'analytics.events_*'])
        >>> tables = [
        ...     ('ecommerce', 'users'),
        ...     ('ecommerce', 'orders'),
        ...     ('analytics', 'events_click'),
        ...     ('other', 'data')
        ... ]
        >>> matched = matcher.match_tables(tables)
        >>> # Returns: [('ecommerce', 'users'), ('ecommerce', 'orders'),
        >>> #           ('analytics', 'events_click')]
    """

    def __init__(self, patterns: List[str]):
        """
        Initialize matcher with patterns.

        Args:
            patterns: List of pattern strings
        """
        self.patterns = [TablePattern.parse(p) for p in patterns]

    def match_tables(
        self,
        available_tables: List[Tuple[str, str]]
    ) -> List[Tuple[str, str]]:
        """
        Match available tables against patterns.

        Args:
            available_tables: List of (database, table) tuples

        Returns:
            List of matched (database, table) tuples (deduplicated)
        """
        matched = set()

        for database, table in available_tables:
            for pattern in self.patterns:
                if pattern.matches(database, table):
                    matched.add((database, table))
                    break

        return sorted(list(matched))

    def matches_table(self, database: str, table: str) -> bool:
        """Check if single table matches any pattern."""
        for pattern in self.patterns:
            if pattern.matches(database, table):
                return True
        return False
```

**2. `sabot/connectors/mysql_discovery.py`**

```python
"""
MySQL table discovery and introspection.
"""

import pymysql
from typing import List, Tuple, Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


class MySQLTableDiscovery:
    """
    Discover tables in MySQL database.

    Example:
        >>> discovery = MySQLTableDiscovery(
        ...     host="localhost",
        ...     user="root",
        ...     password="password"
        ... )
        >>> tables = discovery.discover_tables(database="ecommerce")
        >>> # Returns: [('ecommerce', 'users'), ('ecommerce', 'orders'), ...]
    """

    def __init__(
        self,
        host: str,
        port: int = 3306,
        user: str = "root",
        password: str = "",
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password

    def discover_tables(
        self,
        database: Optional[str] = None
    ) -> List[Tuple[str, str]]:
        """
        Discover all tables.

        Args:
            database: Specific database (None = all databases)

        Returns:
            List of (database, table) tuples
        """
        conn = self._connect()
        cursor = conn.cursor()

        try:
            if database:
                # Tables in specific database
                cursor.execute(f"""
                    SELECT TABLE_SCHEMA, TABLE_NAME
                    FROM information_schema.TABLES
                    WHERE TABLE_SCHEMA = %s
                      AND TABLE_TYPE = 'BASE TABLE'
                    ORDER BY TABLE_SCHEMA, TABLE_NAME
                """, (database,))
            else:
                # Tables in all databases (exclude system databases)
                cursor.execute("""
                    SELECT TABLE_SCHEMA, TABLE_NAME
                    FROM information_schema.TABLES
                    WHERE TABLE_SCHEMA NOT IN ('information_schema', 'mysql',
                                                 'performance_schema', 'sys')
                      AND TABLE_TYPE = 'BASE TABLE'
                    ORDER BY TABLE_SCHEMA, TABLE_NAME
                """)

            tables = [(row[0], row[1]) for row in cursor.fetchall()]
            logger.info(f"Discovered {len(tables)} tables")
            return tables

        finally:
            cursor.close()
            conn.close()

    def get_table_schema(
        self,
        database: str,
        table: str
    ) -> Dict[str, Any]:
        """
        Get table schema metadata.

        Returns:
            {
                'database': str,
                'table': str,
                'columns': [{'name': str, 'type': str, 'nullable': bool}, ...],
                'primary_key': [str, ...]
            }
        """
        conn = self._connect()
        cursor = conn.cursor()

        try:
            # Get columns
            cursor.execute("""
                SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_KEY
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                ORDER BY ORDINAL_POSITION
            """, (database, table))

            columns = []
            primary_key = []

            for row in cursor.fetchall():
                col_name, col_type, is_nullable, col_key = row
                columns.append({
                    'name': col_name,
                    'type': col_type,
                    'nullable': is_nullable == 'YES'
                })
                if col_key == 'PRI':
                    primary_key.append(col_name)

            return {
                'database': database,
                'table': table,
                'columns': columns,
                'primary_key': primary_key
            }

        finally:
            cursor.close()
            conn.close()

    def expand_patterns(
        self,
        patterns: List[str],
        database: Optional[str] = None
    ) -> List[Tuple[str, str]]:
        """
        Expand patterns to concrete table list.

        Args:
            patterns: Table patterns (e.g., ["db.*", "db.user_*"])
            database: Limit to specific database

        Returns:
            List of (database, table) tuples
        """
        from .mysql_table_patterns import TablePatternMatcher

        # Discover all tables
        all_tables = self.discover_tables(database=database)

        # Match against patterns
        matcher = TablePatternMatcher(patterns)
        matched = matcher.match_tables(all_tables)

        logger.info(
            f"Expanded {len(patterns)} patterns to {len(matched)} tables: "
            f"{matched if len(matched) <= 10 else f'{matched[:10]}...'}"
        )

        return matched

    def _connect(self):
        """Create MySQL connection."""
        return pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password
        )
```

#### Testing

**File:** `tests/unit/test_mysql_table_patterns.py`

```python
import pytest
from sabot.connectors.mysql_table_patterns import TablePattern, TablePatternMatcher

def test_single_table():
    """Test single table pattern."""
    pattern = TablePattern.parse('ecommerce.users')
    assert pattern.matches('ecommerce', 'users')
    assert not pattern.matches('ecommerce', 'orders')
    assert not pattern.matches('other', 'users')

def test_wildcard_database():
    """Test wildcard database pattern."""
    pattern = TablePattern.parse('*.users')
    assert pattern.matches('ecommerce', 'users')
    assert pattern.matches('analytics', 'users')
    assert not pattern.matches('ecommerce', 'orders')

def test_wildcard_table():
    """Test wildcard table pattern."""
    pattern = TablePattern.parse('ecommerce.*')
    assert pattern.matches('ecommerce', 'users')
    assert pattern.matches('ecommerce', 'orders')
    assert not pattern.matches('analytics', 'users')

def test_prefix_wildcard():
    """Test prefix wildcard pattern."""
    pattern = TablePattern.parse('ecommerce.user_*')
    assert pattern.matches('ecommerce', 'user_001')
    assert pattern.matches('ecommerce', 'user_profiles')
    assert not pattern.matches('ecommerce', 'orders')

def test_regex_pattern():
    """Test regex pattern."""
    pattern = TablePattern.parse('app_db_[0-9]+.orders')
    assert pattern.matches('app_db_1', 'orders')
    assert pattern.matches('app_db_999', 'orders')
    assert not pattern.matches('app_db_abc', 'orders')
    assert not pattern.matches('app_db_1', 'users')

def test_matcher_multiple_patterns():
    """Test matching multiple patterns."""
    matcher = TablePatternMatcher([
        'ecommerce.*',
        'analytics.events_*'
    ])

    tables = [
        ('ecommerce', 'users'),
        ('ecommerce', 'orders'),
        ('analytics', 'events_click'),
        ('analytics', 'events_page'),
        ('other', 'data')
    ]

    matched = matcher.match_tables(tables)

    assert ('ecommerce', 'users') in matched
    assert ('ecommerce', 'orders') in matched
    assert ('analytics', 'events_click') in matched
    assert ('analytics', 'events_page') in matched
    assert ('other', 'data') not in matched
```

---

### Phase 1B: Auto-Configuration (3 days)

**Goal:** Automatic setup and validation

#### Files to Create

**1. `sabot/connectors/mysql_auto_config.py`**

```python
"""
Auto-configuration for MySQL CDC.

Handles:
- Configuration validation
- Table discovery
- User/permission setup (optional)
- Binlog position management
"""

import pymysql
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class Severity(Enum):
    ERROR = "ERROR"
    WARNING = "WARNING"
    INFO = "INFO"


@dataclass
class ConfigurationIssue:
    """Configuration issue with fix suggestion."""
    severity: Severity
    message: str
    variable: str
    current_value: Any
    required_value: Any
    fix_sql: Optional[str] = None
    requires_restart: bool = False


class MySQLAutoConfigurator:
    """
    Auto-configure MySQL CDC connector.

    Example:
        >>> config = MySQLAutoConfigurator(
        ...     host="localhost",
        ...     admin_user="root",
        ...     admin_password="password"
        ... )
        >>> issues = config.validate_configuration()
        >>> if issues:
        ...     for issue in issues:
        ...         print(f"{issue.severity}: {issue.message}")
        ...         print(f"Fix: {issue.fix_sql}")
    """

    def __init__(
        self,
        host: str,
        port: int = 3306,
        admin_user: str = "root",
        admin_password: str = "",
    ):
        self.host = host
        self.port = port
        self.admin_user = admin_user
        self.admin_password = admin_password

    def validate_configuration(
        self,
        require_gtid: bool = False
    ) -> List[ConfigurationIssue]:
        """
        Validate MySQL configuration for CDC.

        Checks:
        - log_bin = ON
        - binlog_format = ROW
        - binlog_row_image = FULL
        - binlog_row_metadata = FULL (MySQL 8.0.14+)
        - gtid_mode = ON (if require_gtid=True)

        Args:
            require_gtid: Require GTID mode

        Returns:
            List of configuration issues
        """
        issues = []
        conn = self._connect()

        try:
            # Check log_bin
            log_bin = self._get_variable(conn, 'log_bin')
            if log_bin != 'ON':
                issues.append(ConfigurationIssue(
                    severity=Severity.ERROR,
                    message='Binary logging is disabled',
                    variable='log_bin',
                    current_value=log_bin,
                    required_value='ON',
                    fix_sql='# Add to my.cnf:\nlog-bin=mysql-bin',
                    requires_restart=True
                ))

            # Check binlog_format
            binlog_format = self._get_variable(conn, 'binlog_format')
            if binlog_format != 'ROW':
                issues.append(ConfigurationIssue(
                    severity=Severity.ERROR,
                    message='binlog_format must be ROW for CDC',
                    variable='binlog_format',
                    current_value=binlog_format,
                    required_value='ROW',
                    fix_sql='SET GLOBAL binlog_format = ROW;',
                    requires_restart=False
                ))

            # Check binlog_row_image
            binlog_row_image = self._get_variable(conn, 'binlog_row_image')
            if binlog_row_image != 'FULL':
                issues.append(ConfigurationIssue(
                    severity=Severity.WARNING,
                    message='binlog_row_image should be FULL for complete row data',
                    variable='binlog_row_image',
                    current_value=binlog_row_image,
                    required_value='FULL',
                    fix_sql='SET GLOBAL binlog_row_image = FULL;',
                    requires_restart=False
                ))

            # Check binlog_row_metadata (MySQL 8.0.14+)
            version = self._get_variable(conn, 'version')
            if self._version_gte(version, '8.0.14'):
                binlog_row_metadata = self._get_variable(conn, 'binlog_row_metadata')
                if binlog_row_metadata != 'FULL':
                    issues.append(ConfigurationIssue(
                        severity=Severity.WARNING,
                        message='binlog_row_metadata should be FULL for rich metadata',
                        variable='binlog_row_metadata',
                        current_value=binlog_row_metadata,
                        required_value='FULL',
                        fix_sql='SET GLOBAL binlog_row_metadata = FULL;',
                        requires_restart=False
                    ))

            # Check GTID (if required)
            if require_gtid:
                gtid_mode = self._get_variable(conn, 'gtid_mode')
                if gtid_mode != 'ON':
                    issues.append(ConfigurationIssue(
                        severity=Severity.ERROR,
                        message='GTID mode required for auto_position',
                        variable='gtid_mode',
                        current_value=gtid_mode,
                        required_value='ON',
                        fix_sql='# Add to my.cnf:\ngtid-mode=ON\nenforce-gtid-consistency=ON',
                        requires_restart=True
                    ))

        finally:
            conn.close()

        return issues

    def create_replication_user(
        self,
        user: str,
        password: str,
        host: str = "%"
    ):
        """
        Create replication user with proper permissions.

        SQL executed:
            CREATE USER IF NOT EXISTS 'user'@'host' IDENTIFIED BY 'password';
            GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'user'@'host';
            GRANT SELECT ON *.* TO 'user'@'host';
            FLUSH PRIVILEGES;
        """
        conn = self._connect()
        cursor = conn.cursor()

        try:
            # Create user
            cursor.execute(f"""
                CREATE USER IF NOT EXISTS '{user}'@'{host}'
                IDENTIFIED BY '{password}'
            """)

            # Grant replication privileges
            cursor.execute(f"""
                GRANT REPLICATION SLAVE, REPLICATION CLIENT
                ON *.* TO '{user}'@'{host}'
            """)

            # Grant SELECT on all tables
            cursor.execute(f"""
                GRANT SELECT ON *.* TO '{user}'@'{host}'
            """)

            # Flush privileges
            cursor.execute("FLUSH PRIVILEGES")

            conn.commit()
            logger.info(f"Created replication user: {user}@{host}")

        finally:
            cursor.close()
            conn.close()

    def get_current_position(self) -> Dict[str, Any]:
        """
        Get current binlog position.

        Returns:
            {
                'log_file': str,
                'log_pos': int,
                'gtid_executed': str  # if GTID enabled
            }
        """
        conn = self._connect()
        cursor = conn.cursor()

        try:
            # Get binlog position
            cursor.execute("SHOW MASTER STATUS")
            row = cursor.fetchone()

            if not row:
                raise RuntimeError("Could not get master status")

            result = {
                'log_file': row[0],
                'log_pos': int(row[1])
            }

            # Get GTID if enabled
            gtid_mode = self._get_variable(conn, 'gtid_mode')
            if gtid_mode == 'ON':
                cursor.execute("SELECT @@GLOBAL.gtid_executed")
                gtid_row = cursor.fetchone()
                result['gtid_executed'] = gtid_row[0] if gtid_row else ""

            return result

        finally:
            cursor.close()
            conn.close()

    def _connect(self):
        """Create MySQL admin connection."""
        return pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.admin_user,
            password=self.admin_password
        )

    def _get_variable(self, conn, variable: str) -> str:
        """Get MySQL variable value."""
        cursor = conn.cursor()
        try:
            cursor.execute(f"SHOW VARIABLES LIKE '{variable}'")
            row = cursor.fetchone()
            return row[1] if row else None
        finally:
            cursor.close()

    def _version_gte(self, version: str, target: str) -> bool:
        """Compare MySQL versions."""
        def parse_version(v):
            return tuple(map(int, v.split('-')[0].split('.')[:3]))
        try:
            return parse_version(version) >= parse_version(target)
        except:
            return False
```

#### Integration with MySQLCDCConnector

**Modify:** `sabot/connectors/mysql_cdc.py`

```python
class MySQLCDCConnector:
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
        route_by_table: bool = False,
        **kwargs
    ) -> Union['MySQLCDCConnector', Dict[str, 'MySQLCDCConnector']]:
        """
        Auto-configure MySQL CDC.

        Args:
            host: MySQL host
            port: MySQL port
            user: Admin user for setup
            password: Admin password
            database: Database to monitor (None = all databases)
            table_patterns: Table patterns (e.g., "db.*", ["db1.*", "db2.orders"])
            validate_config: Validate MySQL configuration
            create_user: Create dedicated CDC user
            cdc_user: CDC user name (if create_user=True)
            cdc_password: CDC user password (if create_user=True)
            route_by_table: Return per-table connectors
            **kwargs: Additional MySQLCDCConfig options

        Returns:
            Single connector or dict of per-table connectors

        Example:
            >>> # Auto-configure with validation
            >>> connector = MySQLCDCConnector.auto_configure(
            ...     host="localhost",
            ...     user="root",
            ...     password="password",
            ...     table_patterns=["ecommerce.*", "analytics.events_*"],
            ...     validate_config=True
            ... )

            >>> # Per-table routing
            >>> connectors = MySQLCDCConnector.auto_configure(
            ...     host="localhost",
            ...     user="root",
            ...     password="password",
            ...     table_patterns="ecommerce.*",
            ...     route_by_table=True
            ... )
            >>> async for batch in connectors['ecommerce.orders'].stream_batches():
            ...     # Process orders
            ...     pass
        """
        from .mysql_auto_config import MySQLAutoConfigurator
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
                        error_msg += f"❌ {issue.message}\n"
                        error_msg += f"   Current: {issue.variable}={issue.current_value}\n"
                        error_msg += f"   Required: {issue.variable}={issue.required_value}\n"
                        error_msg += f"   Fix: {issue.fix_sql}\n"
                        if issue.requires_restart:
                            error_msg += "   ⚠️ Requires MySQL restart\n"
                        error_msg += "\n"
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

        discovery = MySQLTableDiscovery(host=host, port=port, user=user, password=password)
        matched_tables = discovery.expand_patterns(table_patterns, database=database)

        if not matched_tables:
            raise ValueError(f"No tables matched patterns: {table_patterns}")

        logger.info(f"Auto-configured CDC for {len(matched_tables)} tables: {matched_tables}")

        # Step 4: Create connector(s)
        config = MySQLCDCConfig(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            **kwargs
        )

        if route_by_table:
            # Per-table routing using shuffle
            from .mysql_stream_router import create_routed_connectors
            return create_routed_connectors(config, matched_tables)
        else:
            # Single connector with table filter
            config.only_tables = [f"{db}.{tbl}" for db, tbl in matched_tables]
            return cls(config)
```

---

## Remaining Phases

**Phase 1C:** DDL Change Tracking (2 days)
**Phase 1D:** Per-Table Stream Routing (2 days)
**Phase 1E:** Automatic SQL Setup (1 day)

These phases follow the same pattern as PostgreSQL CDC and will be documented separately.

---

## Summary

**Total Effort:** ~10 days (2 weeks)

**Deliverables:**
- ✅ Wildcard table selection
- ✅ Auto-configuration
- ✅ DDL change tracking
- ✅ Schema evolution
- ✅ Per-table routing
- ✅ Auto SQL setup

**Result:** Full feature parity with PostgreSQL CDC connector
