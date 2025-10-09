"""
DuckDB source connector for Sabot.

Zero-copy Arrow streaming with automatic filter and projection pushdown.
"""

import asyncio
from typing import AsyncIterator, Optional, Dict, Any, List
from dataclasses import dataclass, field

from sabot import cyarrow as ca
from sabot._cython.connectors import DuckDBConnection, DuckDBError
from .base import ConnectorSource

import logging
logger = logging.getLogger(__name__)


@dataclass
class DuckDBSource(ConnectorSource):
    """
    DuckDB-backed data source with automatic pushdown optimization.

    Leverages DuckDB's query optimizer to push filters and projections down
    to the data source (Parquet, CSV, S3, Postgres, etc.) for maximum performance.

    Examples:
        >>> # Parquet with pushdown
        >>> source = DuckDBSource(
        >>>     sql="SELECT * FROM read_parquet('s3://data/*.parquet')",
        >>>     filters={'date': ">= '2025-01-01'", 'price': '> 100'},
        >>>     columns=['id', 'symbol', 'price', 'volume']
        >>> )
        >>>
        >>> # Stream batches (zero-copy)
        >>> async for batch in source.stream_batches():
        >>>     print(f"Batch: {batch.num_rows} rows")

    Args:
        sql: Base SQL query (without WHERE/SELECT filtering)
        database: Database path (default: in-memory)
        filters: Column filters for pushdown (dict of column: condition)
        columns: Columns to select (projection pushdown)
        extensions: DuckDB extensions to load (e.g., ['httpfs', 'postgres_scanner'])
        batch_size: Target batch size for streaming (None = use DuckDB default)
    """

    sql: str
    database: str = ':memory:'
    filters: Optional[Dict[str, Any]] = None
    columns: Optional[List[str]] = None
    extensions: List[str] = field(default_factory=list)
    batch_size: Optional[int] = None

    # Internal state
    _conn: Optional[DuckDBConnection] = field(default=None, init=False, repr=False)
    _schema: Optional[ca.Schema] = field(default=None, init=False, repr=False)
    _optimized_sql: Optional[str] = field(default=None, init=False, repr=False)

    def __post_init__(self):
        """Initialize connection and apply optimizations."""
        # Create DuckDB connection
        self._conn = DuckDBConnection(self.database)

        # Load extensions
        for ext in self.extensions:
            try:
                logger.info(f"Installing extension: {ext}")
                self._conn.install_extension(ext)
                self._conn.load_extension(ext)
            except DuckDBError as e:
                logger.warning(f"Failed to load extension {ext}: {e}")

        # Build optimized SQL with pushdown
        self._optimized_sql = self._build_optimized_sql()

        logger.info(f"DuckDBSource initialized with SQL: {self._optimized_sql[:100]}...")

    def _build_optimized_sql(self) -> str:
        """
        Build optimized SQL with filter and projection pushdown.

        DuckDB's optimizer will push these down to the underlying data source.
        """
        sql = self.sql.strip()

        # Apply projection pushdown (SELECT specific columns)
        if self.columns:
            # If SQL is "SELECT * FROM ...", replace * with column list
            if sql.upper().startswith('SELECT *'):
                columns_str = ', '.join(self.columns)
                sql = sql.replace('SELECT *', f'SELECT {columns_str}', 1)
            else:
                # Wrap in subquery and select columns
                columns_str = ', '.join(self.columns)
                sql = f"SELECT {columns_str} FROM ({sql}) AS subquery"

        # Apply filter pushdown (WHERE clauses)
        if self.filters:
            where_clauses = []
            for column, condition in self.filters.items():
                # Handle different condition formats
                if isinstance(condition, str):
                    # Direct SQL condition (e.g., "> 100" or ">= '2025-01-01'")
                    where_clauses.append(f"{column} {condition}")
                elif isinstance(condition, (list, tuple)):
                    # IN clause (e.g., ['A', 'B', 'C'])
                    values_str = ', '.join(f"'{v}'" if isinstance(v, str) else str(v) for v in condition)
                    where_clauses.append(f"{column} IN ({values_str})")
                else:
                    # Equality (e.g., 'A' or 100)
                    value_str = f"'{condition}'" if isinstance(condition, str) else str(condition)
                    where_clauses.append(f"{column} = {value_str}")

            # Add WHERE clause
            where_sql = ' AND '.join(where_clauses)

            if 'WHERE' in sql.upper():
                # Append to existing WHERE
                sql = f"{sql} AND {where_sql}"
            else:
                # Add new WHERE
                sql = f"{sql} WHERE {where_sql}"

        return sql

    def get_schema(self) -> ca.Schema:
        """Get Arrow schema from DuckDB query."""
        if self._schema is None:
            # Execute query and get schema from result
            result = self._conn.execute_arrow(self._optimized_sql)
            self._schema = result.schema

        return self._schema

    async def stream_batches(self) -> AsyncIterator[ca.RecordBatch]:
        """
        Stream Arrow batches from DuckDB query (zero-copy).

        Yields:
            ca.RecordBatch: Zero-copy Arrow batches from vendored Arrow C++
        """
        # Execute query
        result = self._conn.execute_arrow(self._optimized_sql)

        # Iterate over batches (zero-copy)
        # DuckDB returns batches via Arrow C Data Interface
        # which we import directly into vendored Arrow C++
        for batch in result:
            # Yield to event loop occasionally for async cooperation
            await asyncio.sleep(0)
            yield batch

    def apply_filters(self, filters: Dict[str, Any]) -> 'DuckDBSource':
        """
        Apply additional filters (rebuilds optimized SQL).

        Args:
            filters: Additional column filters

        Returns:
            Self for chaining
        """
        if self.filters is None:
            self.filters = {}

        self.filters.update(filters)
        self._optimized_sql = self._build_optimized_sql()

        logger.debug(f"Applied filters: {filters}, new SQL: {self._optimized_sql[:100]}...")

        return self

    def select_columns(self, columns: List[str]) -> 'DuckDBSource':
        """
        Select specific columns (rebuilds optimized SQL).

        Args:
            columns: Column names to select

        Returns:
            Self for chaining
        """
        self.columns = columns
        self._optimized_sql = self._build_optimized_sql()

        logger.debug(f"Selected columns: {columns}, new SQL: {self._optimized_sql[:100]}...")

        return self

    def close(self):
        """Close DuckDB connection."""
        if self._conn:
            self._conn.close()
            self._conn = None

    def __del__(self):
        """Cleanup on deletion."""
        self.close()
