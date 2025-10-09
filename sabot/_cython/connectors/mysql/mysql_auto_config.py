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
    """Issue severity levels."""
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

    def __str__(self):
        """Format issue for display."""
        msg = f"{self.severity.value}: {self.message}\n"
        msg += f"  Current: {self.variable}={self.current_value}\n"
        msg += f"  Required: {self.variable}={self.required_value}\n"
        if self.fix_sql:
            msg += f"  Fix: {self.fix_sql}\n"
        if self.requires_restart:
            msg += "  ⚠️  Requires MySQL restart\n"
        return msg


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
        ...         print(issue)
        ...     config.print_fix_instructions(issues)
    """

    def __init__(
        self,
        host: str,
        port: int = 3306,
        admin_user: str = "root",
        admin_password: str = "",
    ):
        """
        Initialize auto-configurator.

        Args:
            host: MySQL host
            port: MySQL port
            admin_user: Admin user with SUPER privilege
            admin_password: Admin password
        """
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
        - server_id > 0

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
                    fix_sql='# Add to my.cnf:\nlog-bin=mysql-bin\nbinlog-format=ROW',
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
                    fix_sql='SET GLOBAL binlog_format = \'ROW\';',
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
                    fix_sql='SET GLOBAL binlog_row_image = \'FULL\';',
                    requires_restart=False
                ))

            # Check binlog_row_metadata (MySQL 8.0.14+)
            version = self._get_variable(conn, 'version')
            if self._version_gte(version, '8.0.14'):
                binlog_row_metadata = self._get_variable(conn, 'binlog_row_metadata')
                if binlog_row_metadata and binlog_row_metadata != 'FULL':
                    issues.append(ConfigurationIssue(
                        severity=Severity.INFO,
                        message='binlog_row_metadata should be FULL for rich metadata',
                        variable='binlog_row_metadata',
                        current_value=binlog_row_metadata,
                        required_value='FULL',
                        fix_sql='SET GLOBAL binlog_row_metadata = \'FULL\';',
                        requires_restart=False
                    ))

            # Check server_id
            server_id = self._get_variable(conn, 'server_id')
            if not server_id or int(server_id) == 0:
                issues.append(ConfigurationIssue(
                    severity=Severity.ERROR,
                    message='server_id must be non-zero for replication',
                    variable='server_id',
                    current_value=server_id,
                    required_value='>0',
                    fix_sql='# Add to my.cnf:\nserver-id=1',
                    requires_restart=True
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

            # Check binlog retention
            binlog_expire_logs_seconds = self._get_variable(conn, 'binlog_expire_logs_seconds')
            if binlog_expire_logs_seconds:
                seconds = int(binlog_expire_logs_seconds)
                if seconds < 86400:  # Less than 1 day
                    issues.append(ConfigurationIssue(
                        severity=Severity.WARNING,
                        message='binlog_expire_logs_seconds is very short',
                        variable='binlog_expire_logs_seconds',
                        current_value=seconds,
                        required_value='>=86400',
                        fix_sql='SET GLOBAL binlog_expire_logs_seconds = 604800; -- 7 days',
                        requires_restart=False
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

        Args:
            user: Username to create
            password: Password for user
            host: Host pattern (default '%' for all hosts)
        """
        conn = self._connect()
        cursor = conn.cursor()

        try:
            logger.info(f"Creating replication user: {user}@{host}")

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
            logger.info(f"Successfully created replication user: {user}@{host}")

        except Exception as e:
            logger.error(f"Failed to create replication user: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

    def grant_table_permissions(
        self,
        user: str,
        tables: List[tuple],  # [(database, table), ...]
        host: str = "%"
    ):
        """
        Grant SELECT permissions on specific tables.

        Args:
            user: Username
            tables: List of (database, table) tuples
            host: Host pattern
        """
        conn = self._connect()
        cursor = conn.cursor()

        try:
            for database, table in tables:
                cursor.execute(f"""
                    GRANT SELECT ON `{database}`.`{table}` TO '{user}'@'{host}'
                """)
                logger.debug(f"Granted SELECT on {database}.{table} to {user}@{host}")

            cursor.execute("FLUSH PRIVILEGES")
            conn.commit()
            logger.info(f"Granted permissions on {len(tables)} tables to {user}@{host}")

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
                raise RuntimeError(
                    "Could not get master status. "
                    "Ensure binary logging is enabled (log_bin=ON)"
                )

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

    def print_fix_instructions(self, issues: List[ConfigurationIssue]):
        """
        Print formatted fix instructions for configuration issues.

        Args:
            issues: List of configuration issues
        """
        if not issues:
            logger.info("✅ MySQL is properly configured for CDC")
            return

        errors = [i for i in issues if i.severity == Severity.ERROR]
        warnings = [i for i in issues if i.severity == Severity.WARNING]
        infos = [i for i in issues if i.severity == Severity.INFO]

        print("\n" + "="*70)
        print("MySQL CDC Configuration Issues")
        print("="*70 + "\n")

        if errors:
            print("❌ ERRORS (must fix):\n")
            for issue in errors:
                print(issue)

        if warnings:
            print("\n⚠️  WARNINGS (recommended):\n")
            for issue in warnings:
                print(issue)

        if infos:
            print("\nℹ️  INFO (optional):\n")
            for issue in infos:
                print(issue)

        # Summary of fixes
        print("\n" + "="*70)
        print("Quick Fix Summary")
        print("="*70 + "\n")

        restart_needed = any(i.requires_restart for i in errors + warnings)

        if restart_needed:
            print("1. Add to my.cnf (or my.ini on Windows):\n")
            for issue in errors + warnings:
                if issue.requires_restart and issue.fix_sql:
                    if not issue.fix_sql.startswith('#'):
                        continue
                    print("   " + issue.fix_sql.split('\n')[1])

            print("\n2. Restart MySQL\n")

        runtime_fixes = [i for i in errors + warnings if not i.requires_restart and i.fix_sql]
        if runtime_fixes:
            print("3. Execute SQL (no restart needed):\n")
            for issue in runtime_fixes:
                if not issue.fix_sql.startswith('SET'):
                    continue
                print("   " + issue.fix_sql)

        print("\n" + "="*70 + "\n")

    def _connect(self):
        """Create MySQL admin connection."""
        return pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.admin_user,
            password=self.admin_password,
            cursorclass=pymysql.cursors.Cursor
        )

    def _get_variable(self, conn, variable: str) -> Optional[str]:
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
            # Extract numeric version (e.g., "8.0.32-ubuntu" -> (8, 0, 32))
            try:
                return tuple(map(int, v.split('-')[0].split('.')[:3]))
            except:
                return (0, 0, 0)

        try:
            return parse_version(version) >= parse_version(target)
        except:
            return False
