#!/usr/bin/env python3
"""
Enrichment Operator with Dimension Table Join

Stage 5: Joins streaming quote/trade data with security master (dimension table).
Implements stream-table join to enrich transactional data with reference data.

Architecture:
    Flight Input (Quotes) + Dimension Table (Securities) → Hash Join → Flight Output

Performance:
    - Arrow compute hash join (SIMD-accelerated)
    - Build side: Securities (dimension table, fits in memory)
    - Probe side: Quotes/Trades (streaming data)
    - Multi-threaded join execution
"""

import asyncio
import logging
import time
from pathlib import Path
from typing import Optional, List, AsyncGenerator

# Use Sabot's Arrow API
from sabot import cyarrow as pa
from sabot.cyarrow import compute as pc
from sabot.cyarrow import flight  # Flight from vendored Arrow

# Import Sabot's Cython compute functions
try:
    from sabot._c.arrow_core import hash_join_batches
    CYTHON_AVAILABLE = True
except ImportError:
    logging.warning("Cython hash_join_batches not available, using PyArrow fallback")
    hash_join_batches = None
    CYTHON_AVAILABLE = False

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import (
    JOIN_KEYS_QUOTES_SECURITIES,
    HASH_JOIN_BUFFER_SIZE,
    get_flight_location,
    CSV_BLOCK_SIZE,
    CSV_USE_THREADS,
    BATCH_SIZE_SECURITIES,
)

logger = logging.getLogger(__name__)


class EnrichmentOperator:
    """
    Enrichment Operator - Stage 5 of Pipeline

    Performs stream-table join to enrich quotes/trades with security master data.
    Securities (dimension table) are loaded once into memory, then joined with
    streaming morsels of quotes/trades.

    Features:
        - PyArrow hash join (SIMD-accelerated)
        - Build dimension table once, reuse for all morsels
        - Multi-threaded join execution
        - Arrow Flight IPC for input/output
    """

    def __init__(
        self,
        input_location: str,
        output_location: str,
        securities_csv: Path,
        join_keys: List[str] = None,
        join_type: str = "inner",
    ):
        """
        Initialize enrichment operator.

        Args:
            input_location: Input Flight server location (streaming quotes/trades)
            output_location: Output Flight server location (enriched data)
            securities_csv: Path to securities master CSV (dimension table)
            join_keys: Columns to join on (default: ['instrumentId'])
            join_type: Join type - inner, left, right, full
        """
        self.input_location = input_location
        self.output_location = output_location
        self.securities_csv = Path(securities_csv)
        self.join_keys = join_keys or JOIN_KEYS_QUOTES_SECURITIES
        self.join_type = join_type

        # Validate CSV exists
        if not self.securities_csv.exists():
            raise FileNotFoundError(f"Securities CSV not found: {self.securities_csv}")

        # Flight clients
        self._input_client = None
        self._output_client = None

        # Dimension table (loaded once)
        self._securities_table = None
        self._securities_batch = None  # RecordBatch version for Cython join

        # Metrics
        self._dim_rows_loaded = 0
        self._dim_load_time = 0.0
        self._stream_rows_in = 0
        self._stream_rows_out = 0
        self._morsels_processed = 0
        self._join_time = 0.0

        logger.info(
            f"EnrichmentOperator initialized: join_keys={join_keys}, "
            f"join_type={join_type}"
        )

    def _load_securities_dimension(self) -> pa.Table:
        """
        Load securities dimension table from CSV using native PyArrow reader.

        Returns:
            PyArrow Table with securities data
        """
        start = time.time()

        logger.info(f"Loading dimension table from {self.securities_csv.name}...")

        from sabot.cyarrow import csv as pa_csv  # CSV from vendored Arrow

        # Configure native CSV reader for maximum performance
        read_options = pa_csv.ReadOptions(
            use_threads=CSV_USE_THREADS,
            block_size=CSV_BLOCK_SIZE
        )

        parse_options = pa_csv.ParseOptions(delimiter=',')

        convert_options = pa_csv.ConvertOptions(
            null_values=['NULL', 'null', ''],
            strings_can_be_null=True,
            auto_dict_encode=False  # Prevents join errors
        )

        # Read CSV
        table = pa_csv.read_csv(
            self.securities_csv,
            read_options=read_options,
            parse_options=parse_options,
            convert_options=convert_options
        )

        # Apply batch limit
        if BATCH_SIZE_SECURITIES is not None and table.num_rows > BATCH_SIZE_SECURITIES:
            table = table.slice(0, BATCH_SIZE_SECURITIES)

        # Rename ID column to instrumentId if needed
        if 'ID' in table.column_names:
            column_names = [
                'instrumentId' if c == 'ID' else c
                for c in table.column_names
            ]
            table = table.rename_columns(column_names)

        # Cast null-type columns to string
        from sabot.cyarrow import types as pa_types  # types from vendored Arrow
        schema_fields = []
        for field in table.schema:
            if pa_types.is_null(field.type):
                schema_fields.append(pa.field(field.name, pa.string()))
            else:
                schema_fields.append(field)

        if schema_fields != list(table.schema):
            table = table.cast(pa.schema(schema_fields))

        self._dim_load_time = time.time() - start
        self._dim_rows_loaded = table.num_rows

        rows_per_sec = self._dim_rows_loaded / self._dim_load_time if self._dim_load_time > 0 else 0
        logger.info(
            f"Dimension table loaded: {self._dim_rows_loaded:,} rows in "
            f"{self._dim_load_time:.2f}s ({rows_per_sec:,.0f} rows/sec)"
        )

        return table

    def load_dimension_table(self):
        """Load and cache the dimension table."""
        if self._securities_table is None:
            self._securities_table = self._load_securities_dimension()
            # Also create RecordBatch version for Cython join
            if CYTHON_AVAILABLE:
                self._securities_batch = self._securities_table.to_batches()[0] if self._securities_table.num_rows > 0 else None

    def _process_morsel_cython(self, stream_batch: pa.RecordBatch) -> pa.RecordBatch:
        """
        Process morsel using Cython hash join (zero-copy, SIMD-accelerated).

        Args:
            stream_batch: Input RecordBatch from stream

        Returns:
            Enriched RecordBatch
        """
        # Ensure dimension table is loaded
        if self._securities_batch is None:
            self.load_dimension_table()

        # Use Cython hash join function
        # Note: hash_join_batches takes left_batch, right_batch, left_key, right_key
        # We want to join stream with securities on join_keys
        join_key = self.join_keys[0] if len(self.join_keys) == 1 else self.join_keys[0]
        enriched_batch = hash_join_batches(
            stream_batch,
            self._securities_batch,
            join_key,
            join_key,
            self.join_type
        )

        return enriched_batch

    def _process_morsel_fallback(self, stream_batch: pa.RecordBatch) -> pa.RecordBatch:
        """
        Process morsel using PyArrow Table join (fallback).

        Args:
            stream_batch: Input RecordBatch from stream

        Returns:
            Enriched RecordBatch
        """
        # Ensure dimension table is loaded
        if self._securities_table is None:
            self.load_dimension_table()

        # Convert stream batch to table for join
        stream_table = pa.Table.from_batches([stream_batch])

        # Perform join using PyArrow
        enriched_table = stream_table.join(
            self._securities_table,
            keys=self.join_keys,
            join_type=self.join_type,
            use_threads=True
        )

        # Convert back to RecordBatch
        enriched_batch = enriched_table.to_batches()[0] if enriched_table.num_rows > 0 else stream_batch.slice(0, 0)

        return enriched_batch

    def process_morsel(self, stream_batch: pa.RecordBatch) -> pa.RecordBatch:
        """
        Process a single morsel - join with dimension table.

        Automatically uses Cython or PyArrow fallback based on availability.

        Args:
            stream_batch: Input RecordBatch from stream (quotes/trades)

        Returns:
            Enriched RecordBatch with joined dimension data
        """
        start = time.perf_counter()

        if stream_batch.num_rows == 0:
            return stream_batch

        try:
            # Use Cython-accelerated join if available
            if CYTHON_AVAILABLE and self._securities_batch is not None:
                enriched_batch = self._process_morsel_cython(stream_batch)
            else:
                enriched_batch = self._process_morsel_fallback(stream_batch)

            elapsed = time.perf_counter() - start
            self._join_time += elapsed
            self._stream_rows_in += stream_batch.num_rows
            self._stream_rows_out += enriched_batch.num_rows
            self._morsels_processed += 1

            if self._morsels_processed % 100 == 0:
                rows_per_sec = stream_batch.num_rows / elapsed if elapsed > 0 else 0
                logger.debug(
                    f"Join: {stream_batch.num_rows} → {enriched_batch.num_rows} rows "
                    f"({rows_per_sec:,.0f} rows/sec)"
                )

            return enriched_batch

        except Exception as e:
            logger.error(f"Join failed for morsel: {e}")
            raise

    async def stream_morsels(
        self,
        input_morsels: AsyncGenerator[pa.RecordBatch, None]
    ) -> AsyncGenerator[pa.RecordBatch, None]:
        """
        Stream morsels through enrichment operator (for local testing).

        Args:
            input_morsels: Async generator of input RecordBatches

        Yields:
            Enriched RecordBatches
        """
        # Load dimension table once
        self.load_dimension_table()

        async for batch in input_morsels:
            enriched_batch = self.process_morsel(batch)
            if enriched_batch.num_rows > 0:
                yield enriched_batch

    async def run(self, input_descriptor_path: str, output_descriptor_path: str):
        """
        Run the enrichment operator - receive via Flight, join, send via Flight.

        Args:
            input_descriptor_path: Input Flight descriptor path
            output_descriptor_path: Output Flight descriptor path
        """
        start = time.time()

        # Load dimension table first
        self.load_dimension_table()

        # Connect to Flight servers
        logger.info(f"Connecting to input: {self.input_location}")
        self._input_client = flight.FlightClient(self.input_location)

        logger.info(f"Connecting to output: {self.output_location}")
        self._output_client = flight.FlightClient(self.output_location)

        try:
            # Get input stream
            descriptor = flight.FlightDescriptor.for_path(input_descriptor_path)
            flight_stream = self._input_client.do_get(descriptor)

            # Process morsels
            for i, batch in enumerate(flight_stream):
                # Process morsel
                enriched_batch = self.process_morsel(batch.data)

                if enriched_batch.num_rows > 0:
                    # Send to output via Flight
                    output_descriptor = flight.FlightDescriptor.for_path(output_descriptor_path)
                    writer, _ = self._output_client.do_put(
                        output_descriptor,
                        enriched_batch.schema
                    )
                    writer.write_batch(enriched_batch)
                    writer.close()

                # Log progress
                if (i + 1) % 100 == 0:
                    logger.info(
                        f"Processed {i + 1} morsels: "
                        f"{self._stream_rows_in:,} → {self._stream_rows_out:,} rows"
                    )

            # Log final metrics
            total_time = time.time() - start
            throughput = self._stream_rows_in / self._join_time if self._join_time > 0 else 0

            logger.info(
                f"Enrichment completed: {self._stream_rows_in:,} → {self._stream_rows_out:,} rows "
                f"({total_time:.2f}s total, {self._join_time:.2f}s join, {throughput:,.0f} rows/sec)"
            )
            logger.info(
                f"Dimension table: {self._dim_rows_loaded:,} securities loaded in {self._dim_load_time:.2f}s"
            )

        except Exception as e:
            logger.error(f"Enrichment operator failed: {e}")
            raise

        finally:
            if self._input_client:
                self._input_client.close()
            if self._output_client:
                self._output_client.close()

    def get_metrics(self) -> dict:
        """
        Get operator metrics.

        Returns:
            Dict with performance metrics
        """
        return {
            'dimension_table_rows': self._dim_rows_loaded,
            'dimension_load_time_seconds': self._dim_load_time,
            'stream_rows_in': self._stream_rows_in,
            'stream_rows_out': self._stream_rows_out,
            'morsels_processed': self._morsels_processed,
            'join_time_seconds': self._join_time,
            'join_throughput_rows_per_sec': (
                self._stream_rows_in / self._join_time if self._join_time > 0 else 0
            ),
            'join_type': self.join_type,
            'join_keys': self.join_keys,
        }


# Convenience factory function
def create_enrichment_operator(
    input_location: Optional[str] = None,
    output_location: Optional[str] = None,
    securities_csv: Optional[Path] = None,
) -> EnrichmentOperator:
    """
    Create enrichment operator with default Flight locations.

    Args:
        input_location: Input Flight location (default: from config)
        output_location: Output Flight location (default: from config)
        securities_csv: Path to securities CSV (default: master_security_10m.csv)

    Returns:
        EnrichmentOperator instance
    """
    if input_location is None:
        input_location = get_flight_location('ranking')
    if output_location is None:
        output_location = get_flight_location('enrichment')
    if securities_csv is None:
        securities_csv = Path('master_security_10m.csv')

    return EnrichmentOperator(
        input_location=input_location,
        output_location=output_location,
        securities_csv=securities_csv,
    )


# CLI for testing
if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    async def test_enrichment():
        """Test enrichment operator locally."""
        # Create test quote data
        # Using cyarrow already imported at top of file

        quote_batch = pa.RecordBatch.from_arrays([
            pa.array(['BOND_12345', 'BOND_67890', 'BOND_11111']),
            pa.array([99.50, 102.10, 95.75]),
            pa.array([1000000, 500000, 2000000]),
        ], names=['instrumentId', 'price', 'size'])

        print(f"\nTest quote batch: {quote_batch.num_rows} rows")
        print("Quotes:")
        print(quote_batch.to_pandas())

        # Create operator (will load securities from CSV)
        securities_csv = Path('master_security_10m.csv')
        if not securities_csv.exists():
            print(f"\n❌ {securities_csv} not found!")
            print("Generate it first with: python master_security_synthesiser.py")
            return

        op = EnrichmentOperator(
            input_location="grpc://localhost:8818",
            output_location="grpc://localhost:8819",
            securities_csv=securities_csv,
        )

        # Load dimension table
        print("\nLoading dimension table...")
        op.load_dimension_table()

        # Process locally
        print("\nProcessing enrichment (join with securities)...")
        enriched = op.process_morsel(quote_batch)

        print(f"\nEnriched batch: {enriched.num_rows} rows")
        print(f"Columns: {enriched.num_columns} ({', '.join(enriched.schema.names[:10])}...)")
        print("\nFirst 3 enriched rows (selected columns):")
        cols_to_show = ['instrumentId', 'price', 'size']
        # Add some security columns if they exist
        for col in ['CUSIP', 'ISIN', 'MARKETSEGMENT', 'SECURITYNAME']:
            if col in enriched.schema.names:
                cols_to_show.append(col)
        print(enriched.select(cols_to_show).slice(0, 3).to_pandas())

        print("\nMetrics:")
        for k, v in op.get_metrics().items():
            print(f"  {k}: {v}")

    # Run test
    asyncio.run(test_enrichment())
