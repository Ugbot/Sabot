"""
URI-based smart connectors for Sabot.

Automatically detects the appropriate connector based on URI scheme and format.
"""

from typing import Optional, Dict, Any, List
from urllib.parse import urlparse, parse_qs
import re

from .base import ConnectorSource, ConnectorSink
from .duckdb_source import DuckDBSource
from .duckdb_sink import DuckDBSink
from .formats import (
    ParquetSource, CSVSource, PostgresSource, DeltaSource,
    ParquetSink, CSVSink, DeltaSink
)


def from_uri(
    uri: str,
    filters: Optional[Dict[str, Any]] = None,
    columns: Optional[List[str]] = None,
    **options
) -> ConnectorSource:
    """
    Create a data source from URI (auto-detects format).

    Supports:
    - file:// - Local files (Parquet, CSV, JSON)
    - s3:// - S3 files (requires httpfs extension)
    - http(s):// - HTTP files
    - postgres:// - PostgreSQL tables
    - delta:// - Delta Lake tables
    - duckdb:// - Direct DuckDB SQL

    Args:
        uri: Data source URI
        filters: Column filters for pushdown
        columns: Columns to read (projection pushdown)
        **options: Format-specific options

    Returns:
        ConnectorSource for the URI

    Examples:
        # Parquet file
        source = from_uri('file:///data/transactions.parquet')

        # S3 with filters
        source = from_uri(
            's3://bucket/data/*.parquet',
            filters={'date': ">= '2025-01-01'"},
            columns=['id', 'amount']
        )

        # Postgres
        source = from_uri(
            'postgres://user:pass@localhost/db?table=transactions',
            filters={'amount': '> 1000'}
        )

        # Delta Lake
        source = from_uri('delta://s3://bucket/delta-table')

        # DuckDB SQL
        source = from_uri(
            'duckdb://?sql=SELECT * FROM read_parquet("data.parquet")'
        )
    """
    parsed = urlparse(uri)
    scheme = parsed.scheme.lower()

    # File paths (local, S3, HTTP)
    if scheme in ('file', 's3', 'http', 'https', ''):
        # Extract file path
        if scheme == 'file':
            path = parsed.path
        elif scheme in ('s3', 'http', 'https'):
            path = f"{scheme}://{parsed.netloc}{parsed.path}"
        else:
            # No scheme - assume local file
            path = uri

        # Detect format from extension
        path_lower = path.lower()
        if path_lower.endswith('.parquet'):
            return ParquetSource(path, filters=filters, columns=columns)
        elif path_lower.endswith('.csv'):
            return CSVSource(path, filters=filters, columns=columns, **options)
        elif path_lower.endswith(('.json', '.ndjson')):
            sql = f"SELECT * FROM read_json('{path}')"
            return DuckDBSource(
                sql=sql,
                filters=filters,
                columns=columns,
                extensions=['json']
            )
        else:
            # Default to Parquet
            return ParquetSource(path, filters=filters, columns=columns)

    # PostgreSQL
    elif scheme == 'postgres' or scheme == 'postgresql':
        # Parse connection string
        # Format: postgres://user:pass@host:port/database?table=tablename
        user = parsed.username or 'postgres'
        password = parsed.password or ''
        host = parsed.hostname or 'localhost'
        port = parsed.port or 5432
        database = parsed.path.lstrip('/')

        # Get table from query params
        query_params = parse_qs(parsed.query)
        table = query_params.get('table', [''])[0]
        if not table:
            raise ValueError("PostgreSQL URI must specify table in query params: ?table=tablename")

        # Build connection string
        conn_str = f"host={host} port={port} user={user} dbname={database}"
        if password:
            conn_str += f" password={password}"

        return PostgresSource(conn_str, table, filters=filters, columns=columns)

    # Delta Lake
    elif scheme == 'delta':
        # Format: delta://path/to/table or delta://s3://bucket/table
        if parsed.netloc:
            # delta://s3://bucket/table -> s3://bucket/table
            path = f"{parsed.netloc}{parsed.path}"
        else:
            path = parsed.path

        return DeltaSource(path, filters=filters, columns=columns)

    # DuckDB SQL
    elif scheme == 'duckdb':
        # Format: duckdb://?sql=SELECT * FROM ...
        query_params = parse_qs(parsed.query)
        sql = query_params.get('sql', [''])[0]
        if not sql:
            raise ValueError("DuckDB URI must specify SQL in query params: ?sql=SELECT ...")

        # Extract extensions from query params
        extensions = query_params.get('extensions', [''])[0]
        ext_list = [e.strip() for e in extensions.split(',')] if extensions else []

        return DuckDBSource(
            sql=sql,
            filters=filters,
            columns=columns,
            extensions=ext_list,
            **options
        )

    else:
        raise ValueError(f"Unsupported URI scheme: {scheme}")


def to_uri(
    uri: str,
    mode: str = 'overwrite',
    **options
) -> ConnectorSink:
    """
    Create a data sink from URI (auto-detects format).

    Supports:
    - file:// - Local files (Parquet, CSV)
    - s3:// - S3 files (requires httpfs extension)
    - postgres:// - PostgreSQL tables
    - delta:// - Delta Lake tables
    - duckdb:// - Direct DuckDB SQL

    Args:
        uri: Data sink URI
        mode: Write mode ('overwrite' or 'append')
        **options: Format-specific options

    Returns:
        ConnectorSink for the URI

    Examples:
        # Parquet file
        sink = to_uri('file:///output/data.parquet')

        # S3
        sink = to_uri('s3://bucket/output/data.parquet')

        # Postgres
        sink = to_uri(
            'postgres://user:pass@localhost/db?table=transactions',
            mode='append'
        )

        # Delta Lake
        sink = to_uri('delta://s3://bucket/delta-table', mode='append')
    """
    parsed = urlparse(uri)
    scheme = parsed.scheme.lower()

    # File paths (local, S3)
    if scheme in ('file', 's3', ''):
        # Extract file path
        if scheme == 'file':
            path = parsed.path
        elif scheme == 's3':
            path = f"{scheme}://{parsed.netloc}{parsed.path}"
        else:
            # No scheme - assume local file
            path = uri

        # Detect format from extension
        path_lower = path.lower()
        if path_lower.endswith('.parquet'):
            return ParquetSink(path, mode=mode)
        elif path_lower.endswith('.csv'):
            return CSVSink(path, mode=mode, **options)
        else:
            # Default to Parquet
            return ParquetSink(path, mode=mode)

    # PostgreSQL
    elif scheme == 'postgres' or scheme == 'postgresql':
        # Parse connection string
        user = parsed.username or 'postgres'
        password = parsed.password or ''
        host = parsed.hostname or 'localhost'
        port = parsed.port or 5432
        database = parsed.path.lstrip('/')

        # Get table from query params
        query_params = parse_qs(parsed.query)
        table = query_params.get('table', [''])[0]
        if not table:
            raise ValueError("PostgreSQL URI must specify table in query params: ?table=tablename")

        # Build connection string
        conn_str = f"host={host} port={port} user={user} dbname={database}"
        if password:
            conn_str += f" password={password}"

        # Create sink using postgres_scan
        destination = f"postgres_scan('{conn_str}', '{table}')"
        return DuckDBSink(
            destination=destination,
            mode=mode,
            extensions=['postgres_scanner']
        )

    # Delta Lake
    elif scheme == 'delta':
        # Format: delta://path/to/table or delta://s3://bucket/table
        if parsed.netloc:
            # delta://s3://bucket/table -> s3://bucket/table
            path = f"{parsed.netloc}{parsed.path}"
        else:
            path = parsed.path

        return DeltaSink(path, mode=mode)

    # DuckDB SQL
    elif scheme == 'duckdb':
        # Format: duckdb://?destination=...&format=parquet
        query_params = parse_qs(parsed.query)
        destination = query_params.get('destination', [''])[0]
        if not destination:
            raise ValueError("DuckDB URI must specify destination in query params: ?destination=...")

        # Extract format and extensions
        format = query_params.get('format', ['parquet'])[0]
        extensions = query_params.get('extensions', [''])[0]
        ext_list = [e.strip() for e in extensions.split(',')] if extensions else []

        return DuckDBSink(
            destination=destination,
            format=format,
            mode=mode,
            extensions=ext_list,
            **options
        )

    else:
        raise ValueError(f"Unsupported URI scheme: {scheme}")


# ============================================================================
# Smart Path Detection (without URI scheme)
# ============================================================================

def smart_source(
    path_or_sql: str,
    filters: Optional[Dict[str, Any]] = None,
    columns: Optional[List[str]] = None,
    **options
) -> ConnectorSource:
    """
    Smart source that auto-detects file format or treats as SQL.

    If the input looks like a file path, uses appropriate format reader.
    If it looks like SQL, executes it directly with DuckDB.

    Args:
        path_or_sql: File path, URI, or SQL query
        filters: Column filters for pushdown
        columns: Columns to read
        **options: Format-specific options

    Returns:
        ConnectorSource

    Examples:
        # File path (auto-detects Parquet)
        source = smart_source('data/transactions.parquet')

        # S3 path
        source = smart_source('s3://bucket/data/*.parquet')

        # SQL query
        source = smart_source(
            "SELECT * FROM read_parquet('data/*.parquet') WHERE amount > 1000"
        )
    """
    # Check if it's a URI with scheme
    if '://' in path_or_sql:
        return from_uri(path_or_sql, filters=filters, columns=columns, **options)

    # Check if it's SQL (contains SELECT, FROM, etc.)
    sql_keywords = ['select', 'from', 'where', 'join', 'group by', 'order by']
    is_sql = any(keyword in path_or_sql.lower() for keyword in sql_keywords)

    if is_sql:
        # Treat as SQL query
        return DuckDBSource(
            sql=path_or_sql,
            filters=filters,
            columns=columns,
            **options
        )
    else:
        # Treat as file path
        path_lower = path_or_sql.lower()
        if path_lower.endswith('.parquet'):
            return ParquetSource(path_or_sql, filters=filters, columns=columns)
        elif path_lower.endswith('.csv'):
            return CSVSource(path_or_sql, filters=filters, columns=columns, **options)
        elif path_lower.endswith(('.json', '.ndjson')):
            sql = f"SELECT * FROM read_json('{path_or_sql}')"
            return DuckDBSource(sql=sql, filters=filters, columns=columns, extensions=['json'])
        else:
            # Default to Parquet
            return ParquetSource(path_or_sql, filters=filters, columns=columns)
