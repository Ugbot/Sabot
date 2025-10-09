"""
DuckDB connector plugin system for Sabot.

Manages DuckDB extensions and provides extension registry for common data sources.
"""

from typing import Dict, List, Optional, Set
from dataclasses import dataclass, field
import logging

logger = logging.getLogger(__name__)


@dataclass
class ExtensionInfo:
    """Information about a DuckDB extension."""

    name: str
    """Extension name (e.g., 'httpfs', 'postgres_scanner')"""

    description: str
    """Human-readable description"""

    functions: List[str] = field(default_factory=list)
    """Functions provided by this extension (e.g., ['read_parquet', 's3_scan'])"""

    auto_install: bool = True
    """Whether to auto-install on first use"""

    required_settings: Dict[str, str] = field(default_factory=dict)
    """Required DuckDB settings (e.g., {'s3_region': 'us-west-2'})"""


class ExtensionRegistry:
    """
    Registry of known DuckDB extensions for common data sources.

    Provides automatic extension loading based on data source type.
    """

    # Built-in extension registry
    _registry: Dict[str, ExtensionInfo] = {
        'httpfs': ExtensionInfo(
            name='httpfs',
            description='HTTP/S3 file system support',
            functions=['read_parquet', 'read_csv', 's3_scan'],
            auto_install=True,
            required_settings={
                # S3 credentials can be set via environment variables:
                # AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION
            }
        ),

        'postgres_scanner': ExtensionInfo(
            name='postgres_scanner',
            description='PostgreSQL scanner for reading Postgres tables',
            functions=['postgres_scan', 'postgres_attach'],
            auto_install=True
        ),

        'delta': ExtensionInfo(
            name='delta',
            description='Delta Lake support',
            functions=['delta_scan'],
            auto_install=True
        ),

        'iceberg': ExtensionInfo(
            name='iceberg',
            description='Apache Iceberg support',
            functions=['iceberg_scan'],
            auto_install=True
        ),

        'json': ExtensionInfo(
            name='json',
            description='JSON reading and writing',
            functions=['read_json', 'read_ndjson'],
            auto_install=True
        ),

        'parquet': ExtensionInfo(
            name='parquet',
            description='Parquet reading and writing (built-in)',
            functions=['read_parquet', 'write_parquet'],
            auto_install=False  # Built-in
        ),

        'excel': ExtensionInfo(
            name='spatial',
            description='Excel file support',
            functions=['read_excel'],
            auto_install=True
        ),
    }

    @classmethod
    def get_extension(cls, name: str) -> Optional[ExtensionInfo]:
        """
        Get extension info by name.

        Args:
            name: Extension name

        Returns:
            ExtensionInfo or None if not found
        """
        return cls._registry.get(name)

    @classmethod
    def register_extension(cls, info: ExtensionInfo):
        """
        Register a custom extension.

        Args:
            info: Extension information
        """
        cls._registry[info.name] = info
        logger.info(f"Registered extension: {info.name}")

    @classmethod
    def get_extensions_for_function(cls, function_name: str) -> List[str]:
        """
        Get required extensions for a function.

        Args:
            function_name: DuckDB function name (e.g., 'postgres_scan')

        Returns:
            List of extension names that provide this function
        """
        extensions = []
        for ext_name, ext_info in cls._registry.items():
            if function_name in ext_info.functions:
                extensions.append(ext_name)
        return extensions

    @classmethod
    def get_extensions_for_path(cls, path: str) -> List[str]:
        """
        Auto-detect required extensions based on file path or URI.

        Args:
            path: File path or URI (e.g., 's3://bucket/file.parquet')

        Returns:
            List of required extension names

        Examples:
            >>> ExtensionRegistry.get_extensions_for_path('s3://bucket/data.parquet')
            ['httpfs']
            >>> ExtensionRegistry.get_extensions_for_path('/tmp/data.json')
            ['json']
        """
        extensions = []

        # S3/HTTP paths
        if path.startswith('s3://') or path.startswith('http://') or path.startswith('https://'):
            extensions.append('httpfs')

        # File extensions
        if path.endswith('.json') or path.endswith('.ndjson'):
            extensions.append('json')
        elif path.endswith('.xlsx') or path.endswith('.xls'):
            extensions.append('spatial')

        return extensions

    @classmethod
    def list_extensions(cls) -> Dict[str, ExtensionInfo]:
        """
        List all registered extensions.

        Returns:
            Dict of extension name to ExtensionInfo
        """
        return cls._registry.copy()


class DuckDBConnectorPlugin:
    """
    Plugin for managing DuckDB extensions in Sabot connectors.

    Automatically installs and loads required extensions based on usage patterns.
    """

    def __init__(self, auto_install: bool = True):
        """
        Initialize plugin.

        Args:
            auto_install: Automatically install extensions on first use
        """
        self.auto_install = auto_install
        self._loaded_extensions: Set[str] = set()
        self._failed_extensions: Set[str] = set()

    def ensure_extension(self, connection, extension_name: str) -> bool:
        """
        Ensure an extension is installed and loaded.

        Args:
            connection: DuckDBConnection instance
            extension_name: Extension name

        Returns:
            True if successful, False otherwise
        """
        # Already loaded
        if extension_name in self._loaded_extensions:
            return True

        # Previously failed
        if extension_name in self._failed_extensions:
            logger.warning(f"Extension {extension_name} previously failed to load")
            return False

        # Get extension info
        ext_info = ExtensionRegistry.get_extension(extension_name)
        if not ext_info:
            logger.warning(f"Unknown extension: {extension_name}")
            self._failed_extensions.add(extension_name)
            return False

        # Install if needed
        if self.auto_install and ext_info.auto_install:
            try:
                logger.info(f"Installing extension: {extension_name}")
                connection.install_extension(extension_name)
            except Exception as e:
                logger.error(f"Failed to install extension {extension_name}: {e}")
                self._failed_extensions.add(extension_name)
                return False

        # Load extension
        try:
            logger.info(f"Loading extension: {extension_name}")
            connection.load_extension(extension_name)
            self._loaded_extensions.add(extension_name)
            return True
        except Exception as e:
            logger.error(f"Failed to load extension {extension_name}: {e}")
            self._failed_extensions.add(extension_name)
            return False

    def ensure_extensions(self, connection, extension_names: List[str]) -> bool:
        """
        Ensure multiple extensions are installed and loaded.

        Args:
            connection: DuckDBConnection instance
            extension_names: List of extension names

        Returns:
            True if all successful, False if any failed
        """
        success = True
        for ext_name in extension_names:
            if not self.ensure_extension(connection, ext_name):
                success = False
        return success

    def auto_detect_extensions(self, sql: str, connection) -> List[str]:
        """
        Auto-detect and load required extensions based on SQL query.

        Args:
            sql: SQL query string
            connection: DuckDBConnection instance

        Returns:
            List of loaded extension names
        """
        loaded = []

        # Scan SQL for function calls
        sql_lower = sql.lower()
        for ext_name, ext_info in ExtensionRegistry.list_extensions().items():
            for func in ext_info.functions:
                if func.lower() in sql_lower:
                    if self.ensure_extension(connection, ext_name):
                        loaded.append(ext_name)
                    break

        return loaded

    def get_loaded_extensions(self) -> Set[str]:
        """
        Get set of successfully loaded extensions.

        Returns:
            Set of extension names
        """
        return self._loaded_extensions.copy()

    def get_failed_extensions(self) -> Set[str]:
        """
        Get set of extensions that failed to load.

        Returns:
            Set of extension names
        """
        return self._failed_extensions.copy()

    def reset(self):
        """Reset plugin state (clears loaded/failed extension tracking)."""
        self._loaded_extensions.clear()
        self._failed_extensions.clear()


# Global plugin instance for convenience
_global_plugin = DuckDBConnectorPlugin()


def ensure_extension(connection, extension_name: str) -> bool:
    """
    Convenience function to ensure an extension is loaded.

    Args:
        connection: DuckDBConnection instance
        extension_name: Extension name

    Returns:
        True if successful, False otherwise
    """
    return _global_plugin.ensure_extension(connection, extension_name)


def auto_detect_extensions(sql: str, connection) -> List[str]:
    """
    Auto-detect and load required extensions based on SQL query.

    Args:
        sql: SQL query string
        connection: DuckDBConnection instance

    Returns:
        List of loaded extension names
    """
    return _global_plugin.auto_detect_extensions(sql, connection)
