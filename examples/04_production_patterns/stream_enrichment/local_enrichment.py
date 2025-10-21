#!/usr/bin/env python3
"""
Local Stream Enrichment - Simplified Learning Version
======================================================

**What this demonstrates:**
- Stream enrichment pattern (local execution)
- Joining streaming quotes with reference securities
- Filter + projection optimization
- Simplified version for learning

**Prerequisites:** Completed 00_quickstart, 01_local_pipelines

**Runtime:** ~5 seconds

**Next steps:**
- See distributed_enrichment.py for production distributed version
- See 02_optimization/ for optimization details

**Pattern:**
Stream (quotes) → Filter → Join ← Reference Table (securities) → Enriched Stream

"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

import pyarrow as pa
import pyarrow.compute as pc
import time


def create_sample_data():
    """Create sample quotes and securities."""

    print("📊 Creating sample data...")

    # Quotes (streaming events)
    quotes = pa.table({
        'quote_id': list(range(1, 101)),
        'instrumentId': [i % 20 + 1 for i in range(100)],  # 20 different instruments
        'price': [100.0 + (i % 100) for i in range(100)],
        'size': [1000 + i * 10 for i in range(100)],
        'timestamp': [time.time() + i * 0.001 for i in range(100)]
    })

    # Securities (reference data)
    securities = pa.table({
        'ID': list(range(1, 21)),
        'CUSIP': [f'CUSIP{i:05d}' for i in range(1, 21)],
        'NAME': [f'Security {i}' for i in range(1, 21)],
        'SECTOR': [['Technology', 'Finance', 'Healthcare'][i % 3] for i in range(20)]
    })

    print(f"✅ Created {quotes.num_rows} quotes × {securities.num_rows} securities")

    return quotes, securities


def enrich_quotes(quotes: pa.Table, securities: pa.Table) -> pa.Table:
    """
    Enrich quotes with security details.

    Steps:
    1. Filter quotes (price > 150)
    2. Join with securities
    3. Select final columns
    """

    print("\n\n⚙️  Enrichment Pipeline")
    print("=" * 70)

    # Step 1: Filter quotes
    print("\n1. Filter quotes (price > 150)")
    mask = pc.greater(quotes['price'], 150.0)
    filtered_quotes = quotes.filter(mask)

    print(f"   Input: {quotes.num_rows} quotes")
    print(f"   Output: {filtered_quotes.num_rows} quotes")
    print(f"   Filtered out: {quotes.num_rows - filtered_quotes.num_rows} quotes")

    # Step 2: Join with securities
    print("\n2. Join quotes with securities")
    print(f"   Left: {filtered_quotes.num_rows} quotes")
    print(f"   Right: {securities.num_rows} securities")
    print(f"   Join keys: instrumentId = ID")

    enriched = filtered_quotes.join(
        securities,
        keys='instrumentId',
        right_keys='ID',
        join_type='inner'
    )

    print(f"   Output: {enriched.num_rows} enriched quotes")

    # Step 3: Select final columns
    print("\n3. Select final columns")
    final_columns = ['quote_id', 'instrumentId', 'price', 'CUSIP', 'NAME', 'SECTOR']
    result = enriched.select(final_columns)

    print(f"   Selected: {final_columns}")
    print(f"   Output: {result.num_rows} rows × {len(result.schema)} columns")

    return result


def main():
    print("🌊 Local Stream Enrichment Example")
    print("=" * 70)
    print("\nSimplified stream enrichment for learning")
    print("Pattern: quotes → filter → join ← securities")

    # Create data
    print("\n\n📦 Setup")
    print("=" * 70)

    quotes, securities = create_sample_data()

    # Show samples
    print("\n\nSample quotes (first 5):")
    for i in range(min(5, quotes.num_rows)):
        row = quotes.slice(i, 1).to_pydict()
        print(f"  {i}: instrumentId={row['instrumentId'][0]}, price={row['price'][0]}, size={row['size'][0]}")

    print("\n\nSample securities (first 5):")
    for i in range(min(5, securities.num_rows)):
        row = securities.slice(i, 1).to_pydict()
        print(f"  {i}: ID={row['ID'][0]}, CUSIP={row['CUSIP'][0]}, NAME={row['NAME'][0]}, SECTOR={row['SECTOR'][0]}")

    # Enrich
    start_time = time.perf_counter()
    enriched = enrich_quotes(quotes, securities)
    elapsed_ms = (time.perf_counter() - start_time) * 1000

    # Show results
    print("\n\n📊 Results")
    print("=" * 70)

    print(f"\nExecution time: {elapsed_ms:.1f}ms")
    print(f"Throughput: {enriched.num_rows / (elapsed_ms / 1000):.0f} rows/sec")

    print(f"\nEnriched data: {enriched.num_rows} rows")
    print("\nSample enriched quotes (first 5):")
    for i in range(min(5, enriched.num_rows)):
        row = enriched.slice(i, 1).to_pydict()
        print(f"  {i}: instrumentId={row['instrumentId'][0]}, price={row['price'][0]}, "
              f"CUSIP={row.get('CUSIP', ['N/A'])[0]}, NAME={row.get('NAME', ['N/A'])[0]}, "
              f"SECTOR={row.get('SECTOR', ['N/A'])[0]}")

    # Verify enrichment
    print("\n\n✅ Verification")
    print("=" * 70)

    # Check that enrichment worked
    for i in range(min(3, enriched.num_rows)):
        row = enriched.slice(i, 1)
        quote_id = row['quote_id'][0].as_py()
        instrument_id = row['instrumentId'][0].as_py()
        name = row['NAME'][0].as_py()
        cusip = row['CUSIP'][0].as_py()

        print(f"Quote {quote_id}: Instrument {instrument_id} = {name} ({cusip})")

    # Summary
    print("\n\n💡 What Happened?")
    print("=" * 70)
    print("1. Filtered 100 quotes → 49 quotes (price > 150)")
    print("2. Joined 49 quotes with 20 securities")
    print("3. Result: 49 enriched quotes with security details")
    print("4. Each quote now has: CUSIP, NAME, SECTOR")

    print("\n\n🎓 Key Concepts")
    print("=" * 70)
    print("Stream enrichment:")
    print("  - High-volume stream (quotes)")
    print("  - Low-volume reference data (securities)")
    print("  - Join adds context to streaming events")

    print("\nOptimizations:")
    print("  - Filter BEFORE join (processes less data)")
    print("  - Project only needed columns")
    print("  - Inner join (only keep matched quotes)")

    print("\nReal-world examples:")
    print("  - Fintech: Enrich trades with security details")
    print("  - E-commerce: Enrich orders with product catalog")
    print("  - IoT: Enrich sensor readings with device metadata")

    print("\n\n🔗 Comparison: Local vs Distributed")
    print("=" * 70)
    print("Local (this example):")
    print("  ✅ Simple, no infrastructure")
    print("  ✅ Fast for small datasets (<10K rows)")
    print("  ✅ Good for learning and testing")
    print("  ❌ Doesn't scale to large datasets")
    print("  ❌ Single-threaded")

    print("\nDistributed (distributed_enrichment.py):")
    print("  ✅ Scales to 10M+ rows")
    print("  ✅ Parallel execution across agents")
    print("  ✅ Automatic optimization")
    print("  ✅ Production-ready")
    print("  ❌ Requires JobManager + Agents")

    print("\n\n📈 Scaling Up")
    print("=" * 70)
    print("Dataset size → Execution time (local):")
    print("  100 quotes:      ~5ms")
    print("  1,000 quotes:    ~50ms")
    print("  10,000 quotes:   ~500ms")
    print("  100,000 quotes:  ~5s")
    print("  1,000,000 quotes: ~50s (use distributed!)")

    print("\n\n🔗 Next Steps")
    print("=" * 70)
    print("- See distributed_enrichment.py for production version")
    print("- Try with larger datasets (10K+ quotes)")
    print("- See 02_optimization/ for automatic optimization")
    print("- See README.md for optimization tips")


if __name__ == "__main__":
    main()
