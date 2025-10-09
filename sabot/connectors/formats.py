"""
Format-specific connector helpers for Sabot.

Convenience wrappers around DuckDBSource/DuckDBSink for common data formats.
"""

from typing import Optional, Dict, Any, List
from .duckdb_source import DuckDBSource
from .duckdb_sink import DuckDBSink


# ============================================================================
# Source Connectors (Reading)
# ============================================================================

def ParquetSource(
    path: str,
    filters: Optional[Dict[str, Any]] = None,
    columns: Optional[List[str]] = None
) -> DuckDBSource:
    """
    Create a Parquet data source with automatic pushdown.

    Args:
        path: Parquet file path (supports globs: 'data/*.parquet')
        filters: Column filters for pushdown
        columns: Columns to read (projection pushdown)

    Returns:
        DuckDBSource configured for Parquet

    Example:
        >>> source = ParquetSource(
        >>>     'data/transactions_*.parquet',
        >>>     filters={'date': ">= '2025-01-01'"},
        >>>     columns=['id', 'amount', 'timestamp']
        >>> )
        >>>
        >>> async for batch in source.stream_batches():
        >>>     process(batch)
    """
    sql = f"SELECT * FROM read_parquet('{path}')"
    return DuckDBSource(
        sql=sql,
        filters=filters,
        columns=columns
    )


def CSVSource(
    path: str,
    filters: Optional[Dict[str, Any]] = None,
    columns: Optional[List[str]] = None,
    **csv_options
) -> DuckDBSource:
    """
    Create a CSV data source with automatic pushdown.

    Args:
        path: CSV file path (supports globs: 'data/*.csv')
        filters: Column filters for pushdown
        columns: Columns to read (projection pushdown)
        **csv_options: DuckDB CSV options (header, delimiter, etc.)

    Returns:
        DuckDBSource configured for CSV

    Example:
        >>> source = CSVSource(
        >>>     'data/transactions.csv',
        >>>     filters={'amount': '> 1000'},
        >>>     columns=['id', 'amount', 'timestamp'],
        >>>     header=True,
        >>>     delimiter=','
        >>> )
        >>>
        >>> async for batch in source.stream_batches():
        >>>     process(batch)
    """
    # Build CSV read options
    options_str = ""
    if csv_options:
        opts = ', '.join(f"{k}={repr(v)}" for k, v in csv_options.items())
        options_str = f", {opts}"

    sql = f"SELECT * FROM read_csv('{path}'{options_str})"
    return DuckDBSource(
        sql=sql,
        filters=filters,
        columns=columns
    )


def PostgresSource(
    connection_string: str,
    table: str,
    filters: Optional[Dict[str, Any]] = None,
    columns: Optional[List[str]] = None
) -> DuckDBSource:
    """
    Create a PostgreSQL data source with automatic pushdown.

    Args:
        connection_string: Postgres connection (e.g., 'host=localhost user=postgres')
        table: Table name to read from
        filters: Column filters (pushed to Postgres)
        columns: Columns to read (pushed to Postgres)

    Returns:
        DuckDBSource configured for PostgreSQL

    Example:
        >>> source = PostgresSource(
        >>>     'host=localhost user=postgres password=secret',
        >>>     'transactions',
        >>>     filters={'date': ">= '2025-01-01'"},
        >>>     columns=['id', 'amount', 'timestamp']
        >>> )
        >>>
        >>> async for batch in source.stream_batches():
        >>>     process(batch)
    """
    sql = f"SELECT * FROM postgres_scan('{connection_string}', '{table}')"
    return DuckDBSource(
        sql=sql,
        filters=filters,
        columns=columns,
        extensions=['postgres_scanner']
    )


def DeltaSource(
    path: str,
    filters: Optional[Dict[str, Any]] = None,
    columns: Optional[List[str]] = None
) -> DuckDBSource:
    """
    Create a Delta Lake data source with automatic pushdown.

    Args:
        path: Delta table path (local or S3)
        filters: Column filters for pushdown
        columns: Columns to read (projection pushdown)

    Returns:
        DuckDBSource configured for Delta Lake

    Example:
        >>> source = DeltaSource(
        >>>     's3://bucket/delta-table',
        >>>     filters={'date': ">= '2025-01-01'"},
        >>>     columns=['id', 'amount', 'timestamp']
        >>> )
        >>>
        >>> async for batch in source.stream_batches():
        >>>     process(batch)
    """
    sql = f"SELECT * FROM delta_scan('{path}')"
    return DuckDBSource(
        sql=sql,
        filters=filters,
        columns=columns,
        extensions=['delta']
    )


# ============================================================================
# Sink Connectors (Writing)
# ============================================================================

def ParquetSink(
    path: str,
    mode: str = 'overwrite',
    partition_by: Optional[List[str]] = None
) -> DuckDBSink:
    """
    Create a Parquet data sink.

    Args:
        path: Output file path
        mode: Write mode ('overwrite' or 'append')
        partition_by: Columns to partition by (hive-style partitioning)

    Returns:
        DuckDBSink configured for Parquet

    Example:
        >>> sink = ParquetSink('output/data.parquet', mode='overwrite')
        >>>
        >>> await sink.write_batch(batch1)
        >>> await sink.write_batch(batch2)
        >>> await sink.close()

        >>> # Partitioned write
        >>> sink = ParquetSink(
        >>>     'output/data',
        >>>     partition_by=['date', 'region']
        >>> )
    """
    # TODO: Add partitioning support via DuckDB PARTITION BY
    if partition_by:
        raise NotImplementedError("Partitioning not yet supported (coming soon)")

    return DuckDBSink(
        destination=f"'{path}'",
        format='parquet',
        mode=mode
    )


def CSVSink(
    path: str,
    mode: str = 'overwrite',
    **csv_options
) -> DuckDBSink:
    """
    Create a CSV data sink.

    Args:
        path: Output file path
        mode: Write mode ('overwrite' or 'append')
        **csv_options: DuckDB CSV options (header, delimiter, etc.)

    Returns:
        DuckDBSink configured for CSV

    Example:
        >>> sink = CSVSink(
        >>>     'output/data.csv',
        >>>     mode='overwrite',
        >>>     header=True,
        >>>     delimiter=','
        >>> )
        >>>
        >>> await sink.write_batch(batch)
        >>> await sink.close()
    """
    # TODO: Pass csv_options to DuckDB COPY
    if csv_options:
        raise NotImplementedError("CSV options not yet supported (coming soon)")

    return DuckDBSink(
        destination=f"'{path}'",
        format='csv',
        mode=mode
    )


def DeltaSink(
    path: str,
    mode: str = 'append'
) -> DuckDBSink:
    """
    Create a Delta Lake data sink.

    Args:
        path: Delta table path (local or S3)
        mode: Write mode ('append' or 'overwrite')

    Returns:
        DuckDBSink configured for Delta Lake

    Example:
        >>> sink = DeltaSink('s3://bucket/delta-table', mode='append')
        >>>
        >>> await sink.write_batch(batch)
        >>> await sink.close()
    """
    destination = f"delta_scan('{path}')"
    return DuckDBSink(
        destination=destination,
        mode=mode,
        extensions=['delta']
    )


# ============================================================================
# Convenience Aliases
# ============================================================================

# Sources
parquet_source = ParquetSource
csv_source = CSVSource
postgres_source = PostgresSource
delta_source = DeltaSource

# Sinks
parquet_sink = ParquetSink
csv_sink = CSVSink
delta_sink = DeltaSink
