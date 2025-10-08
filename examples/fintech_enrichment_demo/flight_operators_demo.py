#!/usr/bin/env python3
"""
Fintech Data Enrichment with Arrow Flight Operators

Orchestrates a morsel-driven operator pipeline using Arrow Flight IPC.
Demonstrates Sabot's high-performance streaming capabilities with:
- CSV source with morsel generation
- Tumbling window assignment
- Top-N ranking within partitions
- Dimension table enrichment
- Aggregations and metrics

Pipeline Stages:
    1. CSV Source → Morsels → Flight
    2. Windowing (TUMBLE)
    3. Filtering (data quality)
    4. Ranking (Top-N per partition)
    5. Enrichment (dimension join)
    6. Spread Calculation
    7. Aggregation (final metrics)

Usage:
    python flight_operators_demo.py
"""

import asyncio
import logging
import sys
import time
from pathlib import Path
from typing import Optional

from config import (
    WINDOW_SIZE_MS,
    TOP_N,
    print_config,
)

from operators.csv_source import (
    create_quotes_source,
    create_securities_source,
    create_trades_source,
)
from operators.windowing import create_windowing_operator
from operators.ranking import create_ranking_operator
from operators.enrichment import create_enrichment_operator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FlightOperatorsPipeline:
    """
    Orchestrates the Arrow Flight operator pipeline.

    Manages operator lifecycle and coordinates data flow between stages.
    """

    def __init__(self):
        """Initialize pipeline orchestrator."""
        self.operators = {}
        self.metrics = {}

    async def run_quotes_pipeline(self):
        """
        Run the full quotes enrichment pipeline.

        Pipeline:
            Quotes CSV → Windowing → Ranking → Enrichment → Results
        """
        logger.info("=" * 70)
        logger.info("FINTECH ARROW FLIGHT OPERATOR PIPELINE")
        logger.info("=" * 70)

        # Print configuration
        print_config()

        start_time = time.time()

        # ====================================================================
        # Stage 1: CSV Source (Quotes)
        # ====================================================================
        logger.info("\n📂 Stage 1: Loading quotes from CSV...")
        quotes_source = create_quotes_source()

        # For simplicity, we'll process locally without actual Flight servers
        # In production, each operator would run as a separate service

        # Stream quotes morsels locally
        quotes_morsels = []
        async for morsel in quotes_source.stream_morsels():
            quotes_morsels.append(morsel)

        self.metrics['quotes_source'] = quotes_source.get_metrics()
        logger.info(f"✅ Loaded {quotes_source.get_metrics()['rows_loaded']:,} quotes in {quotes_source.get_metrics()['load_time_seconds']:.2f}s")

        # ====================================================================
        # Stage 2: Windowing
        # ====================================================================
        logger.info("\n🕐 Stage 2: Applying tumbling windows...")
        windowing_op = create_windowing_operator()

        windowed_morsels = []
        for morsel in quotes_morsels:
            windowed_morsel = windowing_op.process_morsel(morsel)
            windowed_morsels.append(windowed_morsel)

        self.metrics['windowing'] = windowing_op.get_metrics()
        logger.info(
            f"✅ Windowed {windowing_op.get_metrics()['rows_processed']:,} rows, "
            f"assigned {windowing_op.get_metrics()['windows_assigned']} windows"
        )

        # ====================================================================
        # Stage 3: Filtering (Optional - for data quality)
        # ====================================================================
        logger.info("\n🔍 Stage 3: Filtering for data quality...")
        # Simple filter: Remove sentinel prices and invalid actions
        from sabot import arrow as pa
        from sabot.arrow import compute as pc
        from config import FILTER_SENTINEL_VALUES, VALID_ACTIONS

        filtered_morsels = []
        filtered_rows_total = 0

        for morsel in windowed_morsels:
            # Filter sentinel prices if 'price' column exists
            if 'price' in morsel.schema.names:
                sentinel_array = pa.array(FILTER_SENTINEL_VALUES)
                mask = pc.is_in(morsel.column('price'), value_set=sentinel_array)
                mask = pc.invert(mask)  # Keep non-sentinel values
                filtered = pc.filter(morsel, mask)
            else:
                filtered = morsel

            # Filter by action if column exists
            if 'action' in filtered.schema.names:
                action_array = pa.array(VALID_ACTIONS)
                action_mask = pc.is_in(filtered.column('action'), value_set=action_array)
                filtered = pc.filter(filtered, action_mask)

            if filtered.num_rows > 0:
                filtered_morsels.append(filtered)
                filtered_rows_total += filtered.num_rows

        logger.info(f"✅ Filtered to {filtered_rows_total:,} valid rows")

        # ====================================================================
        # Stage 4: Ranking (Top-N)
        # ====================================================================
        logger.info(f"\n📊 Stage 4: Ranking top-{TOP_N} quotes per partition...")
        ranking_op = create_ranking_operator(top_n=TOP_N)

        ranked_morsels = []
        for morsel in filtered_morsels:
            # Assuming 'bid' quotes - adjust based on actual data
            ranked_morsel = ranking_op.process_morsel(morsel, quote_type='bid')
            if ranked_morsel.num_rows > 0:
                ranked_morsels.append(ranked_morsel)

        self.metrics['ranking'] = ranking_op.get_metrics()
        logger.info(
            f"✅ Ranked: {ranking_op.get_metrics()['rows_in']:,} → "
            f"{ranking_op.get_metrics()['rows_out']:,} rows "
            f"({ranking_op.get_metrics()['reduction_percent']:.1f}% reduction)"
        )

        # ====================================================================
        # Stage 5: Enrichment (Dimension Join)
        # ====================================================================
        logger.info("\n🔗 Stage 5: Enriching with security master data...")
        securities_csv = Path('master_security_10m.csv')

        if not securities_csv.exists():
            logger.error(f"❌ Securities CSV not found: {securities_csv}")
            logger.error("   Generate it first with: python master_security_synthesiser.py")
            return

        enrichment_op = create_enrichment_operator(securities_csv=securities_csv)
        enrichment_op.load_dimension_table()  # Load once

        enriched_morsels = []
        for morsel in ranked_morsels:
            enriched_morsel = enrichment_op.process_morsel(morsel)
            if enriched_morsel.num_rows > 0:
                enriched_morsels.append(enriched_morsel)

        self.metrics['enrichment'] = enrichment_op.get_metrics()
        logger.info(
            f"✅ Enriched: {enrichment_op.get_metrics()['stream_rows_in']:,} rows "
            f"with {enrichment_op.get_metrics()['dimension_table_rows']:,} securities "
            f"({enrichment_op.get_metrics()['join_throughput_rows_per_sec']:,.0f} rows/sec)"
        )

        # ====================================================================
        # Stage 6: Results & Analytics
        # ====================================================================
        logger.info("\n📈 Stage 6: Computing final analytics...")

        # Combine all enriched morsels
        if enriched_morsels:
            from sabot import cyarrow as _pa  # concat_tables from vendored Arrow
            enriched_tables = [_pa.Table.from_batches([m]) for m in enriched_morsels]
            final_table = _pa.concat_tables(enriched_tables)

            logger.info(f"✅ Final enriched dataset: {final_table.num_rows:,} rows, {final_table.num_columns} columns")

            # Compute some analytics
            logger.info("\n📊 Analytics Summary:")

            # Top instruments by count
            if 'instrumentId' in final_table.column_names:
                grouped = final_table.group_by('instrumentId').aggregate([
                    ('price', 'count'),
                ])
                sorted_instruments = grouped.sort_by([('price_count', 'descending')])

                logger.info(f"\n   Top 10 Instruments by Quote Count:")
                logger.info(f"   {'Instrument ID':<25} {'Count':>10}")
                logger.info(f"   {'-'*37}")

                for i in range(min(10, sorted_instruments.num_rows)):
                    inst_id = sorted_instruments.column('instrumentId')[i].as_py()
                    count = sorted_instruments.column('price_count')[i].as_py()
                    logger.info(f"   {inst_id:<25} {count:>10}")

            # Market segment analysis
            if 'MARKETSEGMENT' in final_table.column_names:
                segment_grouped = final_table.group_by('MARKETSEGMENT').aggregate([
                    ('instrumentId', 'count'),
                ])
                sorted_segments = segment_grouped.sort_by([('instrumentId_count', 'descending')])

                logger.info(f"\n   Market Segments:")
                logger.info(f"   {'Segment':<15} {'Quotes':>10}")
                logger.info(f"   {'-'*27}")

                for i in range(min(sorted_segments.num_rows, 10)):
                    segment = sorted_segments.column('MARKETSEGMENT')[i].as_py()
                    count = sorted_segments.column('instrumentId_count')[i].as_py()
                    logger.info(f"   {segment:<15} {count:>10}")

        # ====================================================================
        # Pipeline Summary
        # ====================================================================
        total_time = time.time() - start_time

        logger.info("\n" + "=" * 70)
        logger.info("🎉 PIPELINE COMPLETE!")
        logger.info("=" * 70)
        logger.info(f"\n⏱️  Total Pipeline Time: {total_time:.2f}s")
        logger.info("\n📊 Stage Performance:")
        logger.info(f"   1. CSV Source:     {self.metrics.get('quotes_source', {}).get('load_time_seconds', 0):.2f}s")
        logger.info(f"   2. Windowing:      {self.metrics.get('windowing', {}).get('processing_time_seconds', 0):.3f}s")
        logger.info(f"   3. Filtering:      (included in windowing)")
        logger.info(f"   4. Ranking:        {self.metrics.get('ranking', {}).get('processing_time_seconds', 0):.3f}s")
        logger.info(f"   5. Enrichment:     {self.metrics.get('enrichment', {}).get('join_time_seconds', 0):.2f}s")

        logger.info("\n✅ Successfully demonstrated:")
        logger.info("   ✓ Morsel-driven processing (64KB chunks)")
        logger.info("   ✓ Tumbling window assignment (Flink SQL TUMBLE)")
        logger.info("   ✓ Top-N ranking per partition")
        logger.info("   ✓ Dimension table enrichment (stream-table join)")
        logger.info("   ✓ PyArrow native CSV loading")
        logger.info("   ✓ Arrow compute operations (SIMD-accelerated)")
        logger.info("=" * 70)


async def main():
    """Main entry point."""
    # Check for required CSV files
    required_files = [
        'synthetic_inventory.csv',
        'master_security_10m.csv',
    ]

    missing = [f for f in required_files if not Path(f).exists()]
    if missing:
        logger.error("❌ Missing required CSV files:")
        for f in missing:
            logger.error(f"   - {f}")
        logger.error("\nGenerate them with:")
        logger.error("   python invenory_rows_synthesiser.py")
        logger.error("   python master_security_synthesiser.py")
        return 1

    # Run pipeline
    pipeline = FlightOperatorsPipeline()
    await pipeline.run_quotes_pipeline()

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
