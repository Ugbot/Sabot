#!/usr/bin/env python3
"""
CSV Source Operator with Arrow Flight IPC

Stage 1: Loads CSV files, chunks into morsels, and streams via Arrow Flight.
Uses native PyArrow CSV reader for maximum performance.

Architecture:
    CSV Files → PyArrow Reader → Morsel Chunking → Arrow Flight IPC → Next Stage

Performance:
    - Native C++ CSV parser with multi-threading
    - 64KB morsels (L2 cache-friendly)
    - Zero-copy Arrow Flight transport
    - Work-stealing parallelism
"""

import asyncio
import logging
import time
from pathlib import Path
from typing import Optional, AsyncGenerator

# Use Sabot's Arrow API
from sabot import arrow as pa
import pyarrow.csv as pa_csv  # CSV not yet in sabot.arrow
import pyarrow.flight as flight  # Flight not yet in sabot.arrow

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import (
    MORSEL_SIZE_BYTES,
    MORSEL_ROWS_ESTIMATE,
    NUM_WORKERS,
    CSV_BLOCK_SIZE,
    CSV_USE_THREADS,
    BATCH_SIZE_SECURITIES,
    BATCH_SIZE_QUOTES,
    BATCH_SIZE_TRADES,
    get_flight_location,
)

logger = logging.getLogger(__name__)


class CSVSourceOperator:
    """
    CSV Source Operator - Stage 1 of Pipeline

    Loads CSV files using PyArrow native reader and streams RecordBatch morsels
    via Arrow Flight IPC to downstream operators.

    Features:
        - Native PyArrow C++ CSV parser (5-10x faster than Python csv module)
        - Multi-threaded parsing for parallelism
        - Morsel-based chunking (64KB default)
        - Arrow Flight zero-copy transport
        - Configurable batch limits
    """

    def __init__(
        self,
        csv_path: Path,
        flight_location: str,
        batch_size: Optional[int] = None,
        morsel_rows: int = MORSEL_ROWS_ESTIMATE,
    ):
        """
        Initialize CSV source operator.

        Args:
            csv_path: Path to CSV file
            flight_location: Arrow Flight server location (e.g., grpc://localhost:8815)
            batch_size: Maximum rows to load (None = all)
            morsel_rows: Rows per morsel for downstream processing
        """
        self.csv_path = Path(csv_path)
        self.flight_location = flight_location
        self.batch_size = batch_size
        self.morsel_rows = morsel_rows

        # Validate CSV exists
        if not self.csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {self.csv_path}")

        # Flight client
        self._flight_client = None

        # Metrics
        self._rows_loaded = 0
        self._rows_sent = 0
        self._morsels_sent = 0
        self._load_time = 0.0
        self._send_time = 0.0

        logger.info(f"CSVSourceOperator initialized: {self.csv_path.name} → {flight_location}")

    def _load_csv_native(self) -> pa.Table:
        """
        Load CSV using PyArrow's native C++ parser.

        Returns:
            PyArrow Table with loaded data
        """
        start = time.time()

        # Configure native CSV reader for maximum performance
        read_options = pa_csv.ReadOptions(
            use_threads=CSV_USE_THREADS,
            block_size=CSV_BLOCK_SIZE
        )

        # Parse options
        parse_options = pa_csv.ParseOptions(
            delimiter=','
        )

        # Convert options - disable dict encoding for join compatibility
        convert_options = pa_csv.ConvertOptions(
            null_values=['NULL', 'null', ''],
            strings_can_be_null=True,
            auto_dict_encode=False  # Prevents "unifying differing dictionaries" errors
        )

        # Read CSV natively with Arrow
        table = pa_csv.read_csv(
            self.csv_path,
            read_options=read_options,
            parse_options=parse_options,
            convert_options=convert_options
        )

        # Apply row limit if specified
        if self.batch_size is not None and table.num_rows > self.batch_size:
            table = table.slice(0, self.batch_size)

        # Cast null-type columns to string to avoid join errors
        import pyarrow.types as pa_types  # types module not in sabot.arrow yet
        schema_fields = []
        for field in table.schema:
            if pa_types.is_null(field.type):
                schema_fields.append(pa.field(field.name, pa.string()))
            else:
                schema_fields.append(field)

        if schema_fields != list(table.schema):
            new_schema = pa.schema(schema_fields)
            table = table.cast(new_schema)

        self._load_time = time.time() - start
        self._rows_loaded = table.num_rows

        rows_per_sec = self._rows_loaded / self._load_time if self._load_time > 0 else 0
        logger.info(
            f"CSV loaded: {self._rows_loaded:,} rows in {self._load_time:.2f}s "
            f"({rows_per_sec:,.0f} rows/sec) - {self.csv_path.name}"
        )

        return table

    def _chunk_into_morsels(self, table: pa.Table) -> list[pa.RecordBatch]:
        """
        Chunk table into morsel-sized RecordBatches.

        Args:
            table: PyArrow Table to chunk

        Returns:
            List of RecordBatch morsels
        """
        morsels = []
        total_rows = table.num_rows
        offset = 0

        while offset < total_rows:
            length = min(self.morsel_rows, total_rows - offset)
            batch = table.slice(offset, length).to_batches()[0] if length > 0 else None

            if batch is not None:
                morsels.append(batch)

            offset += length

        logger.info(
            f"Chunked into {len(morsels)} morsels "
            f"(~{self.morsel_rows} rows each, ~{MORSEL_SIZE_BYTES/1024:.0f}KB)"
        )

        return morsels

    async def _send_morsel_via_flight(self, batch: pa.RecordBatch, descriptor: flight.FlightDescriptor):
        """
        Send a single morsel via Arrow Flight.

        Args:
            batch: RecordBatch morsel to send
            descriptor: Flight descriptor for the stream
        """
        # Create Flight data stream
        writer, _ = self._flight_client.do_put(
            descriptor,
            batch.schema
        )

        # Write batch
        writer.write_batch(batch)
        writer.close()

        self._rows_sent += batch.num_rows
        self._morsels_sent += 1

    async def stream_morsels(self) -> AsyncGenerator[pa.RecordBatch, None]:
        """
        Stream morsels as an async generator (for local testing).

        Yields:
            RecordBatch morsels
        """
        # Load CSV
        table = self._load_csv_native()

        # Chunk into morsels
        morsels = self._chunk_into_morsels(table)

        # Yield each morsel
        for batch in morsels:
            yield batch

    async def run(self, descriptor_path: str = "csv_source"):
        """
        Run the CSV source operator - load CSV and send via Arrow Flight.

        Args:
            descriptor_path: Flight descriptor path (e.g., "csv_source")
        """
        start = time.time()

        # Connect to Flight server
        logger.info(f"Connecting to Arrow Flight: {self.flight_location}")
        self._flight_client = flight.FlightClient(self.flight_location)

        try:
            # Load CSV
            table = self._load_csv_native()

            # Chunk into morsels
            morsels = self._chunk_into_morsels(table)

            # Send morsels via Flight
            send_start = time.time()
            descriptor = flight.FlightDescriptor.for_path(descriptor_path)

            for i, batch in enumerate(morsels):
                await self._send_morsel_via_flight(batch, descriptor)

                # Log progress every 100 morsels
                if (i + 1) % 100 == 0:
                    logger.info(f"Sent {i + 1}/{len(morsels)} morsels ({self._rows_sent:,} rows)")

            self._send_time = time.time() - send_start

            # Log final metrics
            total_time = time.time() - start
            throughput = self._rows_sent / total_time if total_time > 0 else 0

            logger.info(
                f"CSV source completed: {self._rows_sent:,} rows in {self._morsels_sent} morsels "
                f"({total_time:.2f}s, {throughput:,.0f} rows/sec)"
            )

        except Exception as e:
            logger.error(f"CSV source operator failed: {e}")
            raise

        finally:
            if self._flight_client:
                self._flight_client.close()

    def get_metrics(self) -> dict:
        """
        Get operator metrics.

        Returns:
            Dict with performance metrics
        """
        return {
            'csv_file': str(self.csv_path),
            'rows_loaded': self._rows_loaded,
            'rows_sent': self._rows_sent,
            'morsels_sent': self._morsels_sent,
            'load_time_seconds': self._load_time,
            'send_time_seconds': self._send_time,
            'load_throughput_rows_per_sec': (
                self._rows_loaded / self._load_time if self._load_time > 0 else 0
            ),
            'send_throughput_rows_per_sec': (
                self._rows_sent / self._send_time if self._send_time > 0 else 0
            ),
        }


# Convenience factory functions
def create_securities_source(flight_location: Optional[str] = None) -> CSVSourceOperator:
    """Create CSV source for securities master data."""
    if flight_location is None:
        flight_location = get_flight_location('csv_source')

    return CSVSourceOperator(
        csv_path=Path('master_security_10m.csv'),
        flight_location=flight_location,
        batch_size=BATCH_SIZE_SECURITIES,
    )


def create_quotes_source(flight_location: Optional[str] = None) -> CSVSourceOperator:
    """Create CSV source for inventory quotes."""
    if flight_location is None:
        flight_location = get_flight_location('csv_source')

    return CSVSourceOperator(
        csv_path=Path('synthetic_inventory.csv'),
        flight_location=flight_location,
        batch_size=BATCH_SIZE_QUOTES,
    )


def create_trades_source(flight_location: Optional[str] = None) -> CSVSourceOperator:
    """Create CSV source for trades data."""
    if flight_location is None:
        flight_location = get_flight_location('csv_source')

    return CSVSourceOperator(
        csv_path=Path('trax_trades_1m.csv'),
        flight_location=flight_location,
        batch_size=BATCH_SIZE_TRADES,
    )


# CLI for testing
if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    async def test_csv_source():
        """Test CSV source operator locally."""
        # Create operator
        source = create_quotes_source()

        # Stream morsels locally
        print(f"\nStreaming morsels from {source.csv_path.name}...")
        morsel_count = 0
        row_count = 0

        async for batch in source.stream_morsels():
            morsel_count += 1
            row_count += batch.num_rows

            if morsel_count <= 3:
                print(f"  Morsel {morsel_count}: {batch.num_rows} rows, {batch.num_columns} columns")

        print(f"\nTotal: {morsel_count} morsels, {row_count:,} rows")
        print("\nMetrics:")
        for k, v in source.get_metrics().items():
            print(f"  {k}: {v}")

    # Run test
    asyncio.run(test_csv_source())
