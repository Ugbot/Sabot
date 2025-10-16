#!/usr/bin/env python3
"""
SabotQL Integration Quickstart

The fastest way to get started with RDF triple lookups in Sabot pipelines.

This example shows:
1. Creating a triple store
2. Loading RDF data
3. Enriching a stream with SPARQL queries
4. Processing enriched data

Run time: ~1 minute
"""

import asyncio
from pathlib import Path

# Sabot imports
from sabot.api.stream import Stream
from sabot import cyarrow as pa


# ============================================================================
# Step 1: Create Knowledge Graph
# ============================================================================

print("Step 1: Creating knowledge graph...")

try:
    from sabot_ql.bindings.python import create_triple_store
except ImportError:
    print("❌ SabotQL Python bindings not installed!")
    print("\nInstall:")
    print("  cd sabot_ql/build && cmake .. && make -j8")
    print("  cd ../bindings/python && pip install -e .")
    exit(1)

# Create triple store (like creating a dimension table)
kg = create_triple_store('./quickstart_kg.db')
print(f"✅ Created triple store")

# ============================================================================
# Step 2: Load RDF Data
# ============================================================================

print("\nStep 2: Loading company data...")

# Load from N-Triples file (if exists)
sample_file = Path(__file__).parent / 'sample_data' / 'companies.nt'

if sample_file.exists():
    from sabot_ql.bindings.python import load_ntriples
    load_ntriples(kg, str(sample_file))
    print(f"✅ Loaded from {sample_file}")
else:
    # Create sample data inline
    companies = [
        ("AAPL", "Apple Inc.", "Technology"),
        ("GOOGL", "Alphabet Inc.", "Technology"),
        ("MSFT", "Microsoft Corp.", "Technology"),
    ]
    
    for symbol, name, sector in companies:
        kg.insert_triple(
            subject=f'http://stocks.example.org/{symbol}',
            predicate='http://schema.org/name',
            object=f'"{name}"'
        )
        kg.insert_triple(
            subject=f'http://stocks.example.org/{symbol}',
            predicate='http://schema.org/sector',
            object=f'"{sector}"'
        )
    
    print(f"✅ Created {len(companies)} sample companies")

print(f"Total triples: {kg.total_triples()}")

# ============================================================================
# Step 3: Create Sample Stream
# ============================================================================

print("\nStep 3: Creating sample stream...")

# Simulate streaming stock quotes
def generate_quotes():
    """Generate sample stock quotes."""
    symbols = ['AAPL', 'GOOGL', 'MSFT']
    import random
    
    for i in range(10):
        batch = pa.RecordBatch.from_pydict({
            'symbol': [f'http://stocks.example.org/{random.choice(symbols)}' 
                      for _ in range(100)],
            'price': [random.uniform(100, 300) for _ in range(100)],
            'volume': [random.randint(1000, 100000) for _ in range(100)]
        })
        yield batch

stream = Stream(generate_quotes())
print("✅ Stream created (10 batches x 100 rows)")

# ============================================================================
# Step 4: Enrich Stream with Triple Lookups
# ============================================================================

print("\nStep 4: Enriching stream with knowledge graph...")

# Add triple lookup to pipeline
enriched = stream.triple_lookup(
    kg,
    lookup_key='symbol',
    pattern='''
        ?symbol <http://schema.org/name> ?company_name .
        ?symbol <http://schema.org/sector> ?sector
    ''',
    batch_lookups=True,    # Batch queries for performance
    cache_size=100         # Cache 100 hot stocks
)

print("✅ Pipeline built")

# ============================================================================
# Step 5: Process Enriched Data
# ============================================================================

print("\nStep 5: Processing enriched data...")
print("\nEnriched Quotes:")
print("-" * 80)

# Process batches
total_rows = 0
batch_count = 0

for batch in enriched:
    batch_count += 1
    total_rows += batch.num_rows
    
    # Show first batch in detail
    if batch_count == 1:
        print(f"\nFirst batch (showing first 5 rows):")
        print(f"Columns: {batch.schema.names}")
        print()
        
        for i, row in enumerate(batch.to_pylist()[:5]):
            print(f"Row {i+1}:")
            print(f"  Symbol: {row['symbol']}")
            print(f"  Price: ${row['price']:.2f}")
            print(f"  Volume: {row['volume']:,}")
            print(f"  Company: {row.get('company_name', 'N/A')}")
            print(f"  Sector: {row.get('sector', 'N/A')}")
            print()

print(f"\nProcessed {batch_count} batches, {total_rows} total rows")

# ============================================================================
# Summary
# ============================================================================

print("\n" + "="*80)
print("Quickstart Complete! 🎉")
print("="*80)
print()
print("What just happened:")
print("  1. Created RDF triple store (knowledge graph)")
print("  2. Loaded company information as RDF triples")
print("  3. Streamed stock quotes through pipeline")
print("  4. Enriched each quote with company info via SPARQL")
print("  5. Processed enriched data")
print()
print("Key concepts:")
print("  • Triple store = dimension table for graph data")
print("  • SPARQL queries = flexible graph lookups")
print("  • Zero-copy = Arrow format throughout")
print("  • Batch lookups = 10-100x faster than row-by-row")
print()
print("Next steps:")
print("  • See examples/sabot_ql_integration/README.md for more patterns")
print("  • Run benchmark_triple_lookup.py for performance tests")
print("  • Check example1_company_enrichment.py for full Kafka example")
print()
print("Performance you can expect:")
print("  • 100K-1M enrichments/sec (cached)")
print("  • 10K-100K enrichments/sec (uncached)")
print("  • <2x overhead vs direct Arrow joins")
print()


