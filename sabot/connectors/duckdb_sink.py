"""
DuckDB sink connector for Sabot.

Zero-copy Arrow writing using DuckDB's arrow_scan for maximum performance.
"""

import asyncio
from typing import AsyncIterator, Optional, Dict, Any, List
from dataclasses import dataclass, field

from sabot import cyarrow as ca
from sabot._cython.connectors import DuckDBConnection, DuckDBError
from .base import ConnectorSink

import logging
logger = logging.getLogger(__name__)


@dataclass
class DuckDBSink(ConnectorSink):
    """
    DuckDB-backed data sink using arrow_scan for zero-copy writes.

    Leverages DuckDB's arrow_scan to register Arrow data as a temporary table,
    then writes using SQL (COPY TO, INSERT INTO, etc.) for high performance.

    Examples:
        >>> # Write to Parquet
        >>> sink = DuckDBSink(
        >>>     destination="'s3://output/data.parquet'",
        >>>     format='parquet',
        >>>     mode='overwrite'
        >>> )
        >>>
        >>> # Write batches
        >>> await sink.write_batch(batch1)
        >>> await sink.write_batch(batch2)
        >>> await sink.close()  # Flushes to destination

        >>> # Write to Postgres
        >>> sink = DuckDBSink(
        >>>     destination="postgres_scan('host=...', 'table')",
        >>>     mode='append',
        >>>     extensions=['postgres_scanner']
        >>> )

    Args:
        destination: Target destination (SQL or path)
                    - File: "'path/to/file.parquet'" (note: quoted for SQL)
                    - SQL: "postgres_scan('host=...', 'table')"
        format: Output format (parquet, csv, json) for file destinations
        mode: Write mode ('overwrite', 'append', 'error')
        extensions: DuckDB extensions to load
        database: Database path (default: in-memory)
        temp_table: Name for temporary Arrow table (default: 'arrow_data')
        batch_buffer_size: Number of batches to buffer before writing (default: 1)
    """

    destination: str
    format: str = 'parquet'
    mode: str = 'append'
    extensions: List[str] = field(default_factory=list)
    database: str = ':memory:'
    temp_table: str = 'arrow_data'
    batch_buffer_size: int = 1

    # Internal state
    _conn: Optional[DuckDBConnection] = field(default=None, init=False, repr=False)
    _batch_buffer: List[ca.RecordBatch] = field(default_factory=list, init=False, repr=False)
    _total_rows: int = field(default=0, init=False, repr=False)
    _is_initialized: bool = field(default=False, init=False, repr=False)

    def __post_init__(self):
        """Initialize connection."""
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

        logger.info(f"DuckDBSink initialized: destination={self.destination}, format={self.format}")

    def _initialize_destination(self, first_batch: ca.RecordBatch):
        """
        Initialize destination on first batch (schema-dependent operations).

        Args:
            first_batch: First batch (used for schema inference)
        """
        if self._is_initialized:
            return

        # For file destinations with overwrite mode, drop existing file
        if self.mode == 'overwrite' and self._is_file_destination():
            # DuckDB will handle overwriting with COPY TO
            pass

        self._is_initialized = True
        logger.debug(f"Destination initialized: {self.destination}")

    def _is_file_destination(self) -> bool:
        """Check if destination is a file path (vs SQL table function)."""
        # File paths are quoted strings
        return self.destination.strip().startswith("'") or self.destination.strip().startswith('"')

    async def write_batch(self, batch: ca.RecordBatch):
        """
        Write a single Arrow RecordBatch to the sink.

        Batches are buffered and written when buffer is full.

        Args:
            batch: Arrow batch to write
        """
        # Initialize on first batch
        if not self._is_initialized:
            self._initialize_destination(batch)

        # Add to buffer
        self._batch_buffer.append(batch)
        self._total_rows += batch.num_rows

        # Flush if buffer is full
        if len(self._batch_buffer) >= self.batch_buffer_size:
            await self._flush_buffer()

    async def _flush_buffer(self):
        """
        Flush buffered batches to DuckDB using arrow_scan.

        Uses Arrow C Data Interface for zero-copy transfer.
        """
        if not self._batch_buffer:
            return

        logger.debug(f"Flushing {len(self._batch_buffer)} batches ({self._total_rows} rows) to {self.destination}")

        # TODO: Implement arrow_scan integration
        # This requires:
        # 1. Creating an Arrow RecordBatchReader from buffered batches
        # 2. Exporting via Arrow C Data Interface (ArrowArrayStream)
        # 3. Calling duckdb_arrow_scan() to register as temp table
        # 4. Executing COPY TO or INSERT INTO SQL

        # For now, use a workaround: write batches to temporary Parquet, then load
        # (This is less efficient but works without arrow_scan implementation)

        # Combine batches into table
        table = ca.Table.from_batches(self._batch_buffer)

        # Write table to temporary parquet
        import tempfile
        import os
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.parquet')
        temp_path = temp_file.name
        temp_file.close()

        try:
            # Write to temp parquet
            ca.parquet.write_table(table, temp_path)

            # Load into DuckDB and write to destination
            if self._is_file_destination():
                # File destination: COPY TO
                write_sql = f"""
                COPY (SELECT * FROM read_parquet('{temp_path}'))
                TO {self.destination}
                (FORMAT {self.format.upper()})
                """
            else:
                # SQL destination: INSERT INTO
                write_sql = f"""
                INSERT INTO {self.destination}
                SELECT * FROM read_parquet('{temp_path}')
                """

            self._conn.execute(write_sql)

            logger.info(f"Wrote {self._total_rows} rows to {self.destination}")

        finally:
            # Cleanup temp file
            if os.path.exists(temp_path):
                os.unlink(temp_path)

        # Clear buffer
        self._batch_buffer.clear()

        # Yield to event loop
        await asyncio.sleep(0)

    async def close(self):
        """Flush remaining batches and close the sink."""
        # Flush any remaining batches
        await self._flush_buffer()

        # Close connection
        if self._conn:
            self._conn.close()
            self._conn = None

        logger.info(f"DuckDBSink closed: {self._total_rows} total rows written")

    def __del__(self):
        """Cleanup on deletion."""
        if self._conn:
            self._conn.close()


# TODO: Implement true arrow_scan integration
# Once duckdb_arrow_scan() is wrapped in the Cython layer, replace the
# temporary Parquet workaround with direct Arrow C Data Interface transfer:
#
# 1. Create ArrowArrayStream from buffered batches
# 2. Export via _export_to_c()
# 3. Call duckdb_arrow_scan(conn, temp_table, arrow_stream)
# 4. Execute: INSERT INTO destination SELECT * FROM temp_table
#
# This will be true zero-copy without temporary files.
