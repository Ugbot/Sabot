"""
MySQL table discovery and introspection.

Provides functionality to discover tables, inspect schemas, and expand
wildcard patterns to concrete table lists.
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
        """
        Initialize discovery client.

        Args:
            host: MySQL host
            port: MySQL port
            user: MySQL user
            password: MySQL password
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password

    def discover_tables(
        self,
        database: Optional[str] = None,
        exclude_system_databases: bool = True
    ) -> List[Tuple[str, str]]:
        """
        Discover all tables.

        Args:
            database: Specific database (None = all databases)
            exclude_system_databases: Exclude information_schema, mysql, etc.

        Returns:
            List of (database, table) tuples
        """
        conn = self._connect()
        cursor = conn.cursor()

        try:
            if database:
                # Tables in specific database
                cursor.execute("""
                    SELECT TABLE_SCHEMA, TABLE_NAME
                    FROM information_schema.TABLES
                    WHERE TABLE_SCHEMA = %s
                      AND TABLE_TYPE = 'BASE TABLE'
                    ORDER BY TABLE_SCHEMA, TABLE_NAME
                """, (database,))
            else:
                # Tables in all databases
                if exclude_system_databases:
                    cursor.execute("""
                        SELECT TABLE_SCHEMA, TABLE_NAME
                        FROM information_schema.TABLES
                        WHERE TABLE_SCHEMA NOT IN ('information_schema', 'mysql',
                                                     'performance_schema', 'sys')
                          AND TABLE_TYPE = 'BASE TABLE'
                        ORDER BY TABLE_SCHEMA, TABLE_NAME
                    """)
                else:
                    cursor.execute("""
                        SELECT TABLE_SCHEMA, TABLE_NAME
                        FROM information_schema.TABLES
                        WHERE TABLE_TYPE = 'BASE TABLE'
                        ORDER BY TABLE_SCHEMA, TABLE_NAME
                    """)

            tables = [(row[0], row[1]) for row in cursor.fetchall()]
            logger.info(f"Discovered {len(tables)} tables" +
                       (f" in database '{database}'" if database else ""))
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
                'columns': [
                    {
                        'name': str,
                        'type': str,
                        'nullable': bool,
                        'key': str,  # 'PRI', 'UNI', 'MUL', ''
                        'default': Any,
                        'extra': str  # 'auto_increment', etc.
                    },
                    ...
                ],
                'primary_key': [str, ...],
                'engine': str,
                'charset': str
            }
        """
        conn = self._connect()
        cursor = conn.cursor()

        try:
            # Get columns
            cursor.execute("""
                SELECT
                    COLUMN_NAME,
                    COLUMN_TYPE,
                    IS_NULLABLE,
                    COLUMN_KEY,
                    COLUMN_DEFAULT,
                    EXTRA
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                ORDER BY ORDINAL_POSITION
            """, (database, table))

            columns = []
            primary_key = []

            for row in cursor.fetchall():
                col_name, col_type, is_nullable, col_key, col_default, extra = row
                columns.append({
                    'name': col_name,
                    'type': col_type,
                    'nullable': is_nullable == 'YES',
                    'key': col_key,
                    'default': col_default,
                    'extra': extra or ''
                })
                if col_key == 'PRI':
                    primary_key.append(col_name)

            # Get table metadata
            cursor.execute("""
                SELECT ENGINE, TABLE_COLLATION
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            """, (database, table))

            table_row = cursor.fetchone()
            engine = table_row[0] if table_row else 'InnoDB'
            collation = table_row[1] if table_row else 'utf8mb4_general_ci'
            charset = collation.split('_')[0] if collation else 'utf8mb4'

            return {
                'database': database,
                'table': table,
                'columns': columns,
                'primary_key': primary_key,
                'engine': engine,
                'charset': charset
            }

        finally:
            cursor.close()
            conn.close()

    def expand_patterns(
        self,
        patterns: List[str],
        database: Optional[str] = None,
        case_sensitive: bool = True
    ) -> List[Tuple[str, str]]:
        """
        Expand patterns to concrete table list.

        Args:
            patterns: Table patterns (e.g., ["db.*", "db.user_*"])
            database: Limit to specific database
            case_sensitive: Case-sensitive pattern matching

        Returns:
            List of (database, table) tuples
        """
        from .mysql_table_patterns import TablePatternMatcher

        # Discover all tables
        all_tables = self.discover_tables(database=database)

        # Match against patterns
        matcher = TablePatternMatcher(patterns, case_sensitive=case_sensitive)
        matched = matcher.match_tables(all_tables)

        logger.info(
            f"Expanded {len(patterns)} pattern(s) to {len(matched)} table(s): "
            f"{matched if len(matched) <= 10 else f'{matched[:10]}...'}"
        )

        return matched

    def get_databases(self) -> List[str]:
        """
        Get list of all databases (excluding system databases).

        Returns:
            List of database names
        """
        conn = self._connect()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                SELECT SCHEMA_NAME
                FROM information_schema.SCHEMATA
                WHERE SCHEMA_NAME NOT IN ('information_schema', 'mysql',
                                           'performance_schema', 'sys')
                ORDER BY SCHEMA_NAME
            """)

            databases = [row[0] for row in cursor.fetchall()]
            logger.info(f"Discovered {len(databases)} databases")
            return databases

        finally:
            cursor.close()
            conn.close()

    def table_exists(self, database: str, table: str) -> bool:
        """
        Check if table exists.

        Args:
            database: Database name
            table: Table name

        Returns:
            True if table exists
        """
        conn = self._connect()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                SELECT COUNT(*)
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = %s
                  AND TABLE_NAME = %s
                  AND TABLE_TYPE = 'BASE TABLE'
            """, (database, table))

            count = cursor.fetchone()[0]
            return count > 0

        finally:
            cursor.close()
            conn.close()

    def get_table_stats(
        self,
        database: str,
        table: str
    ) -> Dict[str, Any]:
        """
        Get table statistics.

        Returns:
            {
                'row_count': int,
                'data_size': int,  # bytes
                'index_size': int,  # bytes
                'total_size': int,  # bytes
                'auto_increment': Optional[int]
            }
        """
        conn = self._connect()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                SELECT
                    TABLE_ROWS,
                    DATA_LENGTH,
                    INDEX_LENGTH,
                    AUTO_INCREMENT
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            """, (database, table))

            row = cursor.fetchone()
            if not row:
                raise ValueError(f"Table {database}.{table} not found")

            table_rows, data_length, index_length, auto_increment = row

            return {
                'row_count': table_rows or 0,
                'data_size': data_length or 0,
                'index_size': index_length or 0,
                'total_size': (data_length or 0) + (index_length or 0),
                'auto_increment': auto_increment
            }

        finally:
            cursor.close()
            conn.close()

    def _connect(self):
        """Create MySQL connection."""
        return pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            cursorclass=pymysql.cursors.Cursor
        )
