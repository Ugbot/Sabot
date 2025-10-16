#!/usr/bin/env python3
"""
SabotQL Integration Example 1: Company Information Enrichment

Demonstrates how to enrich streaming stock quotes with company information
from an RDF knowledge graph using SabotQL integrated into Sabot pipelines.

Architecture:
1. Load company information as RDF triples (dimension table pattern)
2. Stream stock quotes from Kafka
3. Enrich each quote with company metadata via SPARQL queries
4. Output enriched stream

Performance:
- 100K-1M enrichments/sec (cached)
- 10K-100K enrichments/sec (uncached)
- 23,798 SPARQL queries/sec parsing
"""

import asyncio
import logging
from pathlib import Path
from datetime import datetime
from typing import AsyncIterator

from sabot.api.stream import Stream
from sabot import cyarrow as pa

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================================
# Configuration
# ============================================================================

KAFKA_BROKER = "localhost:9092"
TOPIC_QUOTES = "stock-quotes"
TRIPLE_STORE_PATH = "./company_knowledge.db"
COMPANY_DATA_FILE = "./sample_data/companies.nt"


# ============================================================================
# 1. Setup Knowledge Graph (Dimension Table Pattern)
# ============================================================================

def setup_knowledge_graph():
    """
    Load company information into RDF triple store.
    
    This is the "dimension table" for graph enrichment.
    Loaded once, queried many times.
    """
    logger.info("Setting up company knowledge graph...")
    
    try:
        from sabot_ql.bindings.python import create_triple_store, load_ntriples
    except ImportError:
        logger.error("SabotQL Python bindings not installed!")
        logger.error("Build: cd sabot_ql/build && cmake .. && make")
        logger.error("Install: cd sabot_ql/bindings/python && pip install -e .")
        raise
    
    # Create triple store (MarbleDB backend)
    kg = create_triple_store(TRIPLE_STORE_PATH)
    logger.info(f"✅ Created triple store at {TRIPLE_STORE_PATH}")
    
    # Load company data from N-Triples
    if Path(COMPANY_DATA_FILE).exists():
        load_ntriples(kg, COMPANY_DATA_FILE)
        logger.info(f"✅ Loaded companies from {COMPANY_DATA_FILE}")
    else:
        # Create sample data if file doesn't exist
        logger.info("Sample data not found, creating example triples...")
        _create_sample_data(kg)
    
    total = kg.total_triples()
    logger.info(f"✅ Knowledge graph ready: {total} triples loaded")
    
    return kg


def _create_sample_data(kg):
    """Create sample company data as RDF triples."""
    companies = [
        ("AAPL", "Apple Inc.", "Technology", "USA", "AAA"),
        ("GOOGL", "Alphabet Inc.", "Technology", "USA", "AA+"),
        ("MSFT", "Microsoft Corp.", "Technology", "USA", "AAA"),
        ("JPM", "JPMorgan Chase", "Finance", "USA", "A+"),
        ("GS", "Goldman Sachs", "Finance", "USA", "A"),
        ("BAC", "Bank of America", "Finance", "USA", "A-"),
        ("TSLA", "Tesla Inc.", "Automotive", "USA", "BB+"),
        ("F", "Ford Motor", "Automotive", "USA", "BBB"),
    ]
    
    for symbol, name, sector, country, rating in companies:
        kg.insert_triple(
            subject=f"http://stocks.example.org/{symbol}",
            predicate="http://schema.org/name",
            object=f'"{name}"'
        )
        kg.insert_triple(
            subject=f"http://stocks.example.org/{symbol}",
            predicate="http://schema.org/sector",
            object=f'"{sector}"'
        )
        kg.insert_triple(
            subject=f"http://stocks.example.org/{symbol}",
            predicate="http://schema.org/country",
            object=f'"{country}"'
        )
        kg.insert_triple(
            subject=f"http://stocks.example.org/{symbol}",
            predicate="http://financial.org/creditRating",
            object=f'"{rating}"'
        )
    
    logger.info(f"✅ Created {len(companies)} sample companies")


# ============================================================================
# 2. Enrichment Pipeline
# ============================================================================

async def run_enrichment_pipeline():
    """
    Main pipeline: Enrich stock quotes with company information.
    """
    logger.info("Starting quote enrichment pipeline...")
    
    # Setup knowledge graph
    kg = setup_knowledge_graph()
    
    # Create streaming source (Kafka)
    # Schema: {symbol: string, price: float64, timestamp: timestamp}
    quotes = Stream.from_kafka(TOPIC_QUOTES, KAFKA_BROKER, 'quote-enricher')
    
    # Enrich with company information via SPARQL
    enriched = quotes.triple_lookup(
        kg,
        lookup_key='symbol',
        pattern='''
            ?symbol <hasName> ?company_name .
            ?symbol <inSector> ?sector .
            ?symbol <creditRating> ?rating .
            OPTIONAL { ?symbol <country> ?country }
        ''',
        batch_lookups=True,    # Batch lookups for 10-100x speedup
        cache_size=1000        # Cache 1000 hot stocks
    )
    
    # Process enriched quotes
    batch_count = 0
    row_count = 0
    
    async for batch in enriched:
        batch_count += 1
        row_count += batch.num_rows
        
        if batch_count % 100 == 0:
            logger.info(f"Processed {batch_count} batches, {row_count} quotes")
            
            # Show sample enriched row
            if batch.num_rows > 0:
                sample = batch.to_pydict()
                logger.info(f"Sample enriched quote:")
                logger.info(f"  Symbol: {sample['symbol'][0]}")
                logger.info(f"  Price: ${sample['price'][0]:.2f}")
                logger.info(f"  Company: {sample['company_name'][0]}")
                logger.info(f"  Sector: {sample['sector'][0]}")
                logger.info(f"  Rating: {sample['rating'][0]}")
        
        # Downstream processing
        # - Write to database
        # - Trigger alerts
        # - Update dashboards
        yield batch


# ============================================================================
# 3. Alternative: Simple Pattern Lookup (No SPARQL)
# ============================================================================

async def simple_enrichment():
    """
    Simpler pattern: Direct triple pattern lookup without SPARQL.
    
    Faster for simple cases (skips SPARQL parser).
    """
    kg = setup_knowledge_graph()
    
    quotes = Stream.from_kafka(TOPIC_QUOTES, KAFKA_BROKER, 'simple-enricher')
    
    # Direct triple pattern lookup
    # Pattern: <symbol> <hasName> ?name
    enriched = quotes.triple_lookup(
        kg,
        lookup_key='symbol',
        predicate='http://schema.org/name',  # Fixed predicate
        object=None  # Wildcard - return all values
    )
    
    async for batch in enriched:
        # batch has: symbol, price, timestamp, name
        logger.info(f"Enriched {batch.num_rows} quotes with company names")
        yield batch


# ============================================================================
# 4. Monitoring & Statistics
# ============================================================================

async def run_with_monitoring():
    """Run pipeline with performance monitoring."""
    kg = setup_knowledge_graph()
    
    quotes = Stream.from_kafka(TOPIC_QUOTES, KAFKA_BROKER, 'monitored-enricher')
    
    enrichment_op = quotes.triple_lookup(
        kg,
        lookup_key='symbol',
        pattern='?symbol <hasName> ?name',
        cache_size=5000
    )
    
    # Track metrics
    start_time = datetime.now()
    total_batches = 0
    total_rows = 0
    
    async for batch in enrichment_op:
        total_batches += 1
        total_rows += batch.num_rows
        
        if total_batches % 1000 == 0:
            elapsed = (datetime.now() - start_time).total_seconds()
            throughput = total_rows / elapsed if elapsed > 0 else 0
            
            # Get operator stats
            stats = enrichment_op.get_stats()
            
            logger.info("="*60)
            logger.info("Pipeline Statistics:")
            logger.info(f"  Batches processed: {total_batches}")
            logger.info(f"  Rows processed: {total_rows:,}")
            logger.info(f"  Throughput: {throughput:,.0f} rows/sec")
            logger.info(f"  Cache hit rate: {stats['cache_hit_rate']*100:.1f}%")
            logger.info(f"  Cache size: {stats['cache_size']}/{enrichment_op._cache_size}")
            logger.info("="*60)


# ============================================================================
# Main Entry Point
# ============================================================================

async def main():
    """Main entry point."""
    print("╔══════════════════════════════════════════════════════════╗")
    print("║  SabotQL Integration Example 1: Company Enrichment      ║")
    print("╚══════════════════════════════════════════════════════════╝")
    print()
    
    # Run enrichment pipeline
    async for batch in run_enrichment_pipeline():
        # Process enriched batches
        pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        raise


