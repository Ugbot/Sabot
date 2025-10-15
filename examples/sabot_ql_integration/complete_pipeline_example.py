#!/usr/bin/env python3
"""
Complete SabotQL + Sabot Pipeline Example

Demonstrates full integration: Kafka → Sabot transforms → Triple lookup → Output

This is a production-ready example showing:
- Kafka source with schema
- Multiple pipeline stages
- Triple store enrichment
- State management
- Error handling
- Monitoring
- Output sink
"""

import asyncio
import logging
from datetime import datetime
from typing import AsyncIterator

from sabot.api.stream import Stream
from sabot import cyarrow as pa
import pyarrow.compute as pc

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================================
# Configuration
# ============================================================================

class Config:
    """Pipeline configuration."""
    # Kafka
    KAFKA_BROKER = "localhost:9092"
    INPUT_TOPIC = "raw-transactions"
    OUTPUT_TOPIC = "enriched-transactions"
    CONSUMER_GROUP = "transaction-enricher"
    
    # Triple Store
    KNOWLEDGE_GRAPH_PATH = "./production_kg.db"
    COMPANY_DATA_FILE = "./sample_data/companies.nt"
    
    # Performance
    BATCH_SIZE = 10000
    CACHE_SIZE = 5000
    
    # Monitoring
    STATS_INTERVAL = 100  # Log stats every N batches


# ============================================================================
# 1. Initialize Knowledge Graph (Startup)
# ============================================================================

def initialize_knowledge_graph():
    """
    Initialize and populate knowledge graph.
    
    This runs once at application startup (dimension table pattern).
    """
    logger.info("Initializing knowledge graph...")
    
    try:
        from sabot_ql.bindings.python import create_triple_store, load_ntriples
    except ImportError as e:
        logger.error(f"SabotQL not installed: {e}")
        logger.error("Run: cd sabot_ql/bindings/python && ./build.sh")
        raise
    
    # Create triple store
    kg = create_triple_store(Config.KNOWLEDGE_GRAPH_PATH)
    logger.info(f"✅ Created triple store at {Config.KNOWLEDGE_GRAPH_PATH}")
    
    # Load reference data
    from pathlib import Path
    if Path(Config.COMPANY_DATA_FILE).exists():
        load_ntriples(kg, Config.COMPANY_DATA_FILE)
        logger.info(f"✅ Loaded {kg.total_triples():,} triples from {Config.COMPANY_DATA_FILE}")
    else:
        logger.warning(f"Company data file not found: {Config.COMPANY_DATA_FILE}")
        logger.info("Creating sample data...")
        _create_sample_companies(kg)
    
    return kg


def _create_sample_companies(kg):
    """Create sample company data."""
    companies = [
        ("AAPL", "Apple Inc.", "Technology", "USA", 3000000000000, "AAA"),
        ("GOOGL", "Alphabet Inc.", "Technology", "USA", 1800000000000, "AA+"),
        ("MSFT", "Microsoft Corp.", "Technology", "USA", 2800000000000, "AAA"),
        ("JPM", "JPMorgan Chase", "Finance", "USA", 600000000000, "A+"),
        ("GS", "Goldman Sachs", "Finance", "USA", 150000000000, "A"),
        ("TSLA", "Tesla Inc.", "Automotive", "USA", 800000000000, "BB+"),
        ("BAC", "Bank of America", "Finance", "USA", 300000000000, "A-"),
        ("WMT", "Walmart Inc.", "Retail", "USA", 400000000000, "AA"),
        ("AMZN", "Amazon.com Inc.", "Technology", "USA", 1700000000000, "AA+"),
        ("V", "Visa Inc.", "Finance", "USA", 500000000000, "AA-"),
    ]
    
    for symbol, name, sector, country, market_cap, rating in companies:
        base_iri = f"http://stocks.example.org/{symbol}"
        
        kg.insert_triple(base_iri, "http://schema.org/name", f'"{name}"')
        kg.insert_triple(base_iri, "http://schema.org/sector", f'"{sector}"')
        kg.insert_triple(base_iri, "http://schema.org/country", f'"{country}"')
        kg.insert_triple(base_iri, "http://financial.org/marketCap", f'"{market_cap}"')
        kg.insert_triple(base_iri, "http://financial.org/creditRating", f'"{rating}"')
    
    logger.info(f"✅ Created {len(companies)} sample companies ({kg.total_triples()} triples)")


# ============================================================================
# 2. Build Pipeline
# ============================================================================

class TransactionEnrichmentPipeline:
    """
    Complete transaction enrichment pipeline.
    
    Stages:
    1. Source: Kafka consumer
    2. Validation: Filter invalid records
    3. Normalization: Standardize symbols
    4. Enrichment: Triple store lookup
    5. Calculation: Compute derived fields
    6. Sink: Write to output topic
    """
    
    def __init__(self, knowledge_graph):
        """Initialize pipeline with knowledge graph."""
        self.kg = knowledge_graph
        self.stats = {
            'batches_processed': 0,
            'rows_processed': 0,
            'enrichment_failures': 0,
            'start_time': datetime.now()
        }
    
    async def run(self):
        """Execute the pipeline."""
        logger.info("Starting transaction enrichment pipeline...")
        
        # ====================================================================
        # Stage 1: Source (Kafka)
        # ====================================================================
        
        source = Stream.from_kafka(
            Config.INPUT_TOPIC,
            Config.KAFKA_BROKER,
            Config.CONSUMER_GROUP
        )
        
        logger.info(f"✅ Connected to Kafka: {Config.KAFKA_BROKER}")
        logger.info(f"   Topic: {Config.INPUT_TOPIC}")
        logger.info(f"   Group: {Config.CONSUMER_GROUP}")
        
        # ====================================================================
        # Stage 2: Validation
        # ====================================================================
        
        # Filter out invalid records
        validated = source.filter(self._validate_record)
        
        # ====================================================================
        # Stage 3: Normalization
        # ====================================================================
        
        # Normalize symbol format
        normalized = validated.map(self._normalize_symbols)
        
        # ====================================================================
        # Stage 4: Enrichment (Triple Lookup)
        # ====================================================================
        
        # Enrich with company information from knowledge graph
        enriched = normalized.triple_lookup(
            self.kg,
            lookup_key='symbol',
            pattern='''
                ?symbol <http://schema.org/name> ?company_name .
                ?symbol <http://schema.org/sector> ?sector .
                ?symbol <http://schema.org/country> ?country .
                ?symbol <http://financial.org/creditRating> ?credit_rating .
                OPTIONAL { ?symbol <http://financial.org/marketCap> ?market_cap }
            ''',
            batch_lookups=True,
            cache_size=Config.CACHE_SIZE
        )
        
        # ====================================================================
        # Stage 5: Calculations
        # ====================================================================
        
        # Add derived fields
        calculated = enriched.map(self._add_calculations)
        
        # ====================================================================
        # Stage 6: Monitoring
        # ====================================================================
        
        # Add stats tracking
        monitored = self._add_monitoring(calculated)
        
        # ====================================================================
        # Stage 7: Sink (Output)
        # ====================================================================
        
        # Process enriched data
        async for batch in monitored:
            # Write to output topic, database, etc.
            await self._write_output(batch)
    
    def _validate_record(self, batch: pa.RecordBatch) -> pa.BooleanArray:
        """Validate records (filter invalid)."""
        # Check required fields
        has_symbol = pc.is_valid(batch.column('symbol'))
        has_amount = pc.is_valid(batch.column('amount'))
        amount_positive = pc.greater(batch.column('amount'), 0)
        
        # Combine conditions
        return pc.and_(pc.and_(has_symbol, has_amount), amount_positive)
    
    def _normalize_symbols(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        """Normalize symbol format to match knowledge graph."""
        # Convert 'AAPL' → 'http://stocks.example.org/AAPL'
        symbols = batch.column('symbol')
        
        # Build normalized IRIs
        normalized = []
        for i in range(symbols.length()):
            if symbols.is_null(i):
                normalized.append(None)
            else:
                symbol = symbols[i].as_py()
                normalized.append(f'http://stocks.example.org/{symbol}')
        
        # Replace column
        symbol_array = pa.array(normalized, type=pa.string())
        return batch.set_column(
            batch.schema.get_field_index('symbol'),
            'symbol',
            symbol_array
        )
    
    def _add_calculations(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        """Add calculated fields."""
        # Example: Calculate transaction fee based on credit rating
        fees = []
        
        for row in batch.to_pylist():
            rating = row.get('credit_rating', 'NR')
            amount = row['amount']
            
            # Fee schedule based on credit rating
            if rating in ['AAA', 'AA+', 'AA', 'AA-']:
                fee_pct = 0.001  # 0.1%
            elif rating in ['A+', 'A', 'A-']:
                fee_pct = 0.002  # 0.2%
            elif rating in ['BBB+', 'BBB', 'BBB-']:
                fee_pct = 0.005  # 0.5%
            else:
                fee_pct = 0.01   # 1.0%
            
            fees.append(amount * fee_pct)
        
        # Add fee column
        fee_array = pa.array(fees, type=pa.float64())
        return batch.append_column('fee', fee_array)
    
    def _add_monitoring(self, stream):
        """Add monitoring wrapper."""
        async def monitored_stream():
            async for batch in stream:
                # Update stats
                self.stats['batches_processed'] += 1
                self.stats['rows_processed'] += batch.num_rows
                
                # Log periodically
                if self.stats['batches_processed'] % Config.STATS_INTERVAL == 0:
                    self._log_stats()
                
                yield batch
        
        return monitored_stream()
    
    def _log_stats(self):
        """Log pipeline statistics."""
        elapsed = (datetime.now() - self.stats['start_time']).total_seconds()
        
        batches = self.stats['batches_processed']
        rows = self.stats['rows_processed']
        
        batch_rate = batches / elapsed if elapsed > 0 else 0
        row_rate = rows / elapsed if elapsed > 0 else 0
        
        logger.info("="*70)
        logger.info("Pipeline Statistics:")
        logger.info(f"  Batches: {batches:,} ({batch_rate:,.0f} batches/sec)")
        logger.info(f"  Rows: {rows:,} ({row_rate:,.0f} rows/sec)")
        logger.info(f"  Runtime: {elapsed:.1f} sec")
        logger.info(f"  Enrichment failures: {self.stats['enrichment_failures']}")
        logger.info("="*70)
    
    async def _write_output(self, batch: pa.RecordBatch):
        """Write enriched batch to output."""
        # In production:
        # - Write to Kafka output topic
        # - Write to database
        # - Trigger alerts
        # - Update dashboards
        
        # For demo: just log
        if self.stats['batches_processed'] <= 2:
            logger.info(f"\nEnriched Batch Sample:")
            logger.info(f"  Rows: {batch.num_rows}")
            logger.info(f"  Columns: {batch.schema.names}")
            
            if batch.num_rows > 0:
                sample = batch.to_pylist()[0]
                logger.info(f"\n  Sample Row:")
                for key, value in sample.items():
                    logger.info(f"    {key}: {value}")


# ============================================================================
# 3. Main Application
# ============================================================================

async def main():
    """Main application entry point."""
    print("╔══════════════════════════════════════════════════════════╗")
    print("║  SabotQL + Sabot Complete Pipeline Example              ║")
    print("╚══════════════════════════════════════════════════════════╝")
    print()
    
    # Initialize knowledge graph (dimension table)
    kg = initialize_knowledge_graph()
    
    # Create and run pipeline
    pipeline = TransactionEnrichmentPipeline(kg)
    
    try:
        await pipeline.run()
    except KeyboardInterrupt:
        logger.info("\nPipeline stopped by user")
        pipeline._log_stats()
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        raise


# ============================================================================
# Helper: Generate Test Data
# ============================================================================

def generate_test_data():
    """Generate test transactions for pipeline."""
    import random
    
    symbols = ['AAPL', 'GOOGL', 'MSFT', 'JPM', 'GS', 'TSLA', 'BAC', 'WMT', 'AMZN', 'V']
    
    for i in range(100):
        batch = pa.RecordBatch.from_pydict({
            'transaction_id': [f'TX{i*1000+j}' for j in range(1000)],
            'symbol': [random.choice(symbols) for _ in range(1000)],
            'amount': [random.uniform(1000, 1000000) for _ in range(1000)],
            'timestamp': [datetime.now().isoformat() for _ in range(1000)]
        })
        yield batch


async def run_with_test_data():
    """Run pipeline with generated test data (no Kafka needed)."""
    logger.info("Running with generated test data (no Kafka)...")
    
    # Initialize knowledge graph
    kg = initialize_knowledge_graph()
    
    # Create stream from test data
    source = Stream(generate_test_data())
    
    # Build pipeline
    enriched = source.triple_lookup(
        kg,
        lookup_key='symbol',
        pattern='''
            ?symbol <http://schema.org/name> ?company_name .
            ?symbol <http://schema.org/sector> ?sector .
            ?symbol <http://financial.org/creditRating> ?credit_rating
        ''',
        batch_lookups=True,
        cache_size=Config.CACHE_SIZE
    )
    
    # Add fee calculation
    def add_fee(batch):
        fees = []
        for row in batch.to_pylist():
            rating = row.get('credit_rating', 'NR')
            amount = row['amount']
            fee_pct = 0.001 if 'AA' in rating else 0.002
            fees.append(amount * fee_pct)
        
        return batch.append_column('fee', pa.array(fees))
    
    calculated = enriched.map(add_fee)
    
    # Process
    batch_count = 0
    row_count = 0
    
    async for batch in calculated:
        batch_count += 1
        row_count += batch.num_rows
        
        if batch_count % 10 == 0:
            logger.info(f"Processed {batch_count} batches, {row_count:,} rows")
            
            if batch_count == 10:
                # Show sample
                logger.info("\nSample Enriched Transaction:")
                sample = batch.to_pylist()[0]
                logger.info(f"  TX ID: {sample['transaction_id']}")
                logger.info(f"  Symbol: {sample['symbol']}")
                logger.info(f"  Company: {sample.get('company_name', 'N/A')}")
                logger.info(f"  Sector: {sample.get('sector', 'N/A')}")
                logger.info(f"  Rating: {sample.get('credit_rating', 'N/A')}")
                logger.info(f"  Amount: ${sample['amount']:,.2f}")
                logger.info(f"  Fee: ${sample['fee']:.2f}")
    
    logger.info(f"\n✅ Pipeline complete: {row_count:,} transactions processed")


# ============================================================================
# Entry Points
# ============================================================================

if __name__ == "__main__":
    import sys
    
    if '--test-data' in sys.argv:
        # Run with generated test data (no Kafka required)
        asyncio.run(run_with_test_data())
    else:
        # Run with real Kafka
        print("\nMode: Production (Kafka)")
        print("To run with test data: python complete_pipeline_example.py --test-data")
        print()
        
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            logger.info("Pipeline interrupted")

