#!/usr/bin/env python3
"""
Ranking Operator with Top-N Selection

Stage 4: Computes best N bids and offers per instrument and window using efficient sorting.
Takes the top N records within each partition based on configurable sort order.

Architecture:
    Flight Input → Group By Partition → Sort Within Group → Take Top-N → Flight Output

Performance:
    - Arrow compute sort (SIMD-accelerated)
    - Zero-copy partitioning
    - Morsel-parallel processing
"""

import asyncio
import logging
import time
from typing import Optional, List, Tuple, AsyncGenerator

# Use Sabot's Arrow API
from sabot import arrow as pa
from sabot.arrow import compute as pc
import pyarrow.flight as flight  # Flight not yet in sabot.arrow

# Import Sabot's Cython compute functions
try:
    from sabot._c.arrow_core import sort_and_take
    CYTHON_AVAILABLE = True
except ImportError:
    logging.warning("Cython sort_and_take not available, using Python fallback")
    sort_and_take = None
    CYTHON_AVAILABLE = False

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import (
    TOP_N,
    RANK_PARTITION_BY,
    RANK_ORDER_BID,
    RANK_ORDER_OFFER,
    get_flight_location,
)

logger = logging.getLogger(__name__)


class RankingOperator:
    """
    Ranking Operator - Stage 4 of Pipeline

    Performs Top-N selection within partitions.
    Computes best N bids/offers per instrument and window.

    Features:
        - Arrow compute partitioning and sorting
        - Separate bid/offer ranking logic
        - Configurable Top-N
        - Morsel-parallel processing
    """

    def __init__(
        self,
        input_location: str,
        output_location: str,
        top_n: int = TOP_N,
        partition_by: List[str] = None,
        order_bid: List[Tuple[str, str]] = None,
        order_offer: List[Tuple[str, str]] = None,
    ):
        """
        Initialize ranking operator.

        Args:
            input_location: Input Flight server location
            output_location: Output Flight server location
            top_n: Number of top records to keep per partition
            partition_by: Columns to partition by (default: instrumentId, window_start)
            order_bid: Sort order for bids (column, direction) - higher price = better
            order_offer: Sort order for offers (column, direction) - lower price = better
        """
        self.input_location = input_location
        self.output_location = output_location
        self.top_n = top_n
        self.partition_by = partition_by or RANK_PARTITION_BY
        self.order_bid = order_bid or RANK_ORDER_BID
        self.order_offer = order_offer or RANK_ORDER_OFFER

        # Flight clients
        self._input_client = None
        self._output_client = None

        # Metrics
        self._rows_in = 0
        self._rows_out = 0
        self._morsels_processed = 0
        self._partitions_processed = 0
        self._processing_time = 0.0

        logger.info(
            f"RankingOperator initialized: top_n={top_n}, "
            f"partition_by={partition_by}"
        )

    def _rank_within_partition_cython(
        self,
        partition_batch: pa.RecordBatch,
        order_by: List[Tuple[str, str]]
    ) -> pa.RecordBatch:
        """
        Take Top-N records within a single partition using Cython.

        Uses sabot._c.arrow_core.sort_and_take() for zero-copy sorting and slicing.
        Performance: O(n log n) with SIMD-accelerated comparison.

        Args:
            partition_batch: RecordBatch for a single partition
            order_by: List of (column, direction) tuples for sorting

        Returns:
            Top-N RecordBatch
        """
        if partition_batch.num_rows == 0:
            return partition_batch

        # Normalize sort order (convert DESC/ASC to descending/ascending)
        sort_keys = []
        for col, direction in order_by:
            dir_normalized = 'descending' if direction.upper() == 'DESC' else 'ascending'
            sort_keys.append((col, dir_normalized))

        # Use Cython compute function (SIMD-accelerated sort + zero-copy slice)
        top_n_batch = sort_and_take(partition_batch, sort_keys, self.top_n)

        return top_n_batch

    def _rank_within_partition_fallback(
        self,
        partition_batch: pa.RecordBatch,
        order_by: List[Tuple[str, str]]
    ) -> pa.RecordBatch:
        """
        Take Top-N records within a single partition (Python fallback).

        Used when Cython sort_and_take is not available.

        Args:
            partition_batch: RecordBatch for a single partition
            order_by: List of (column, direction) tuples for sorting

        Returns:
            Top-N RecordBatch
        """
        if partition_batch.num_rows == 0:
            return partition_batch

        # Sort by specified order (convert DESC/ASC to descending/ascending)
        sort_keys = []
        for col, direction in order_by:
            dir_normalized = 'descending' if direction.upper() == 'DESC' else 'ascending'
            sort_keys.append((col, dir_normalized))
        sorted_indices = pc.sort_indices(partition_batch, sort_keys=sort_keys)
        sorted_batch = pc.take(partition_batch, sorted_indices)

        # Take top N
        top_n_batch = sorted_batch.slice(0, min(self.top_n, sorted_batch.num_rows))

        return top_n_batch

    def _rank_within_partition(
        self,
        partition_batch: pa.RecordBatch,
        order_by: List[Tuple[str, str]]
    ) -> pa.RecordBatch:
        """
        Take Top-N records within a single partition.

        Automatically uses Cython or Python fallback based on availability.
        """
        if CYTHON_AVAILABLE:
            return self._rank_within_partition_cython(partition_batch, order_by)
        else:
            return self._rank_within_partition_fallback(partition_batch, order_by)

    def process_morsel(self, batch: pa.RecordBatch, quote_type: str = 'bid') -> pa.RecordBatch:
        """
        Process a single morsel - apply Top-N selection within partitions.

        Args:
            batch: Input RecordBatch morsel
            quote_type: 'bid' or 'offer' - determines sort order

        Returns:
            RecordBatch with top-N per partition
        """
        start = time.perf_counter()

        if batch.num_rows == 0:
            return batch

        # Choose sort order based on quote type
        order_by = self.order_bid if quote_type == 'bid' else self.order_offer

        # Group by partition columns using Arrow compute
        # Strategy: Sort by partition keys, then process each group
        partition_sort_keys = [(col, 'ascending') for col in self.partition_by]

        try:
            # Sort by partition columns
            sorted_indices = pc.sort_indices(batch, sort_keys=partition_sort_keys)
            sorted_batch = pc.take(batch, sorted_indices)

            # Identify partition boundaries
            partitions = []
            current_partition_values = None
            partition_start = 0

            for i in range(sorted_batch.num_rows):
                # Get partition key values for current row
                partition_values = tuple(
                    sorted_batch.column(col)[i].as_py()
                    for col in self.partition_by
                )

                # Check if we've moved to a new partition
                if partition_values != current_partition_values:
                    # Save previous partition
                    if current_partition_values is not None:
                        partitions.append((partition_start, i))

                    current_partition_values = partition_values
                    partition_start = i

            # Don't forget the last partition
            if current_partition_values is not None:
                partitions.append((partition_start, sorted_batch.num_rows))

            # Process each partition
            ranked_batches = []
            for start_idx, end_idx in partitions:
                partition_batch = sorted_batch.slice(start_idx, end_idx - start_idx)
                ranked_batch = self._rank_within_partition(partition_batch, order_by)
                ranked_batches.append(ranked_batch)

            # Concatenate all ranked partitions
            if ranked_batches:
                # Use pyarrow concat_tables (not yet in sabot.arrow)
                import pyarrow as _pa  # Only for concat_tables
                tables = [_pa.Table.from_batches([b]) for b in ranked_batches]
                result_table = _pa.concat_tables(tables)
                result_batch = result_table.to_batches()[0] if result_table.num_rows > 0 else ranked_batches[0].slice(0, 0)
            else:
                result_batch = batch.slice(0, 0)  # Empty batch with schema

            elapsed = time.perf_counter() - start
            self._processing_time += elapsed
            self._rows_in += batch.num_rows
            self._rows_out += result_batch.num_rows
            self._morsels_processed += 1
            self._partitions_processed += len(partitions)

            if self._morsels_processed % 100 == 0:
                logger.debug(
                    f"Ranking: {batch.num_rows} rows → {result_batch.num_rows} rows "
                    f"({len(partitions)} partitions)"
                )

            return result_batch

        except Exception as e:
            logger.error(f"Ranking failed: {e}")
            raise

    async def stream_morsels(
        self,
        input_morsels: AsyncGenerator[pa.RecordBatch, None],
        quote_type: str = 'bid'
    ) -> AsyncGenerator[pa.RecordBatch, None]:
        """
        Stream morsels through ranking operator (for local testing).

        Args:
            input_morsels: Async generator of input RecordBatches
            quote_type: 'bid' or 'offer'

        Yields:
            Ranked RecordBatches
        """
        async for batch in input_morsels:
            ranked_batch = self.process_morsel(batch, quote_type)
            if ranked_batch.num_rows > 0:
                yield ranked_batch

    async def run(
        self,
        input_descriptor_path: str,
        output_descriptor_path: str,
        quote_type: str = 'bid'
    ):
        """
        Run the ranking operator - receive via Flight, process, send via Flight.

        Args:
            input_descriptor_path: Input Flight descriptor path
            output_descriptor_path: Output Flight descriptor path
            quote_type: 'bid' or 'offer' for sort order
        """
        start = time.time()

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
                ranked_batch = self.process_morsel(batch.data, quote_type)

                if ranked_batch.num_rows > 0:
                    # Send to output via Flight
                    output_descriptor = flight.FlightDescriptor.for_path(output_descriptor_path)
                    writer, _ = self._output_client.do_put(
                        output_descriptor,
                        ranked_batch.schema
                    )
                    writer.write_batch(ranked_batch)
                    writer.close()

                # Log progress
                if (i + 1) % 100 == 0:
                    reduction = (1 - self._rows_out / self._rows_in) * 100 if self._rows_in > 0 else 0
                    logger.info(
                        f"Processed {i + 1} morsels: "
                        f"{self._rows_in:,} → {self._rows_out:,} rows "
                        f"({reduction:.1f}% reduction, {self._partitions_processed} partitions)"
                    )

            # Log final metrics
            total_time = time.time() - start
            throughput = self._rows_in / total_time if total_time > 0 else 0
            reduction = (1 - self._rows_out / self._rows_in) * 100 if self._rows_in > 0 else 0

            logger.info(
                f"Ranking completed: {self._rows_in:,} → {self._rows_out:,} rows "
                f"({reduction:.1f}% reduction, {total_time:.2f}s, {throughput:,.0f} rows/sec)"
            )
            logger.info(
                f"Partitions processed: {self._partitions_processed}, "
                f"avg rows/partition: {self._rows_in/self._partitions_processed:.1f}"
            )

        except Exception as e:
            logger.error(f"Ranking operator failed: {e}")
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
        reduction = (1 - self._rows_out / self._rows_in) * 100 if self._rows_in > 0 else 0

        return {
            'rows_in': self._rows_in,
            'rows_out': self._rows_out,
            'reduction_percent': reduction,
            'morsels_processed': self._morsels_processed,
            'partitions_processed': self._partitions_processed,
            'processing_time_seconds': self._processing_time,
            'throughput_rows_per_sec': (
                self._rows_in / self._processing_time if self._processing_time > 0 else 0
            ),
            'avg_rows_per_partition': (
                self._rows_in / self._partitions_processed if self._partitions_processed > 0 else 0
            ),
            'top_n': self.top_n,
        }


# Convenience factory function
def create_ranking_operator(
    input_location: Optional[str] = None,
    output_location: Optional[str] = None,
    top_n: int = TOP_N,
) -> RankingOperator:
    """
    Create ranking operator with default Flight locations.

    Args:
        input_location: Input Flight location (default: from config)
        output_location: Output Flight location (default: from config)
        top_n: Number of top records per partition

    Returns:
        RankingOperator instance
    """
    if input_location is None:
        input_location = get_flight_location('filtering')
    if output_location is None:
        output_location = get_flight_location('ranking')

    return RankingOperator(
        input_location=input_location,
        output_location=output_location,
        top_n=top_n,
    )


# CLI for testing
if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    async def test_ranking():
        """Test ranking operator locally."""
        # Create test data with multiple instruments and windows
        import pyarrow as pa

        instrument_ids = ['BOND_1', 'BOND_1', 'BOND_1', 'BOND_1', 'BOND_1',
                          'BOND_2', 'BOND_2', 'BOND_2']
        window_starts = [100000, 100000, 100000, 100000, 100000,
                         100000, 100000, 100000]
        prices = [99.50, 99.75, 99.60, 99.55, 99.70,
                  102.10, 102.05, 102.15]
        sizes = [1000, 2000, 1500, 1200, 1800,
                 3000, 2500, 2800]

        test_batch = pa.RecordBatch.from_arrays([
            pa.array(instrument_ids),
            pa.array(window_starts),
            pa.array(prices),
            pa.array(sizes),
        ], names=['instrumentId', 'window_start', 'price', 'size'])

        print(f"\nTest batch: {test_batch.num_rows} rows")
        print(f"Unique instruments: {len(set(instrument_ids))}")
        print(f"\nInput data:")
        print(test_batch.to_pandas())

        # Create operator
        op = RankingOperator(
            input_location="grpc://localhost:8817",
            output_location="grpc://localhost:8818",
            top_n=3,  # Top 3 for testing
        )

        # Process locally (bid ranking - highest price first)
        print("\n\nProcessing ranking (BID - highest price first)...")
        ranked = op.process_morsel(test_batch, quote_type='bid')

        print(f"\nRanked batch: {ranked.num_rows} rows (top {op.top_n} per partition)")
        print(f"Columns: {ranked.schema.names}")
        print(f"\nRanked data:")
        print(ranked.to_pandas())

        print("\n\nMetrics:")
        for k, v in op.get_metrics().items():
            print(f"  {k}: {v}")

    # Run test
    asyncio.run(test_ranking())
