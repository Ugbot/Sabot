#!/usr/bin/env python3
"""
Dimension Tables Demo

Demonstrates Sabot's unified materialization system:
- Loading dimension tables from Arrow IPC files
- Using dimension tables for stream enrichment
- Operator overloading for ergonomic API
- Zero-copy joins with CyArrow

Usage:
    # Build Cython extensions first
    python setup.py build_ext --inplace

    # Run demo
    python examples/dimension_tables_demo.py
"""

import os
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import sabot as sb
from sabot import cyarrow as ca

print("=" * 70)
print("Sabot Dimension Tables Demo")
print("=" * 70)


def create_sample_dimension_data():
    """Create sample dimension tables as Arrow IPC files."""
    data_dir = Path(__file__).parent / 'data'
    data_dir.mkdir(exist_ok=True)

    # === Create Securities Dimension ===
    securities_data = {
        'security_id': ['SEC001', 'SEC002', 'SEC003', 'SEC004', 'SEC005'],
        'name': ['Apple Inc.', 'Microsoft Corp.', 'Google LLC', 'Amazon.com Inc.', 'Tesla Inc.'],
        'ticker': ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA'],
        'sector': ['Technology', 'Technology', 'Technology', 'Consumer', 'Automotive'],
        'exchange': ['NASDAQ', 'NASDAQ', 'NASDAQ', 'NASDAQ', 'NASDAQ'],
        'country': ['USA', 'USA', 'USA', 'USA', 'USA']
    }

    securities_table = pa.Table.from_pydict(securities_data)
    securities_path = data_dir / 'securities.arrow'

    import pyarrow.feather as feather
    feather.write_feather(securities_table, securities_path, compression='zstd')
    print(f"\n✅ Created securities dimension: {securities_path}")
    print(f"   Rows: {securities_table.num_rows}, Columns: {securities_table.num_columns}")

    # === Create Users Dimension ===
    users_data = {
        'user_id': ['U001', 'U002', 'U003', 'U004', 'U005'],
        'name': ['Alice Johnson', 'Bob Smith', 'Charlie Brown', 'Diana Prince', 'Eve Adams'],
        'email': ['alice@example.com', 'bob@example.com', 'charlie@example.com',
                  'diana@example.com', 'eve@example.com'],
        'account_type': ['premium', 'basic', 'premium', 'premium', 'basic'],
        'country': ['USA', 'UK', 'Canada', 'USA', 'Australia'],
        'join_date': ['2024-01-15', '2024-02-20', '2024-03-10', '2024-01-05', '2024-04-12']
    }

    users_table = pa.Table.from_pydict(users_data)
    users_path = data_dir / 'users.arrow'

    feather.write_feather(users_table, users_path, compression='zstd')
    print(f"✅ Created users dimension: {users_path}")
    print(f"   Rows: {users_table.num_rows}, Columns: {users_table.num_columns}")

    return securities_path, users_path


def demo_dimension_table_basics():
    """Demonstrate basic dimension table operations."""
    print("\n" + "=" * 70)
    print("Demo 1: Dimension Table Basics")
    print("=" * 70)

    # Create sample data
    securities_path, users_path = create_sample_dimension_data()

    # Create Sabot app
    app = sb.App('dimension-demo', broker='kafka://localhost:19092')

    # Get materialization manager
    mat_mgr = app.materializations(default_backend='memory')

    # === Load Securities Dimension ===
    print("\n📊 Loading securities dimension table...")
    securities = mat_mgr.dimension_table(
        name='securities',
        source=str(securities_path),
        key='security_id',
        backend='memory'  # Use memory backend for fast lookups
    )

    print(f"✅ Loaded {len(securities)} securities")
    print(f"   Schema: {[f.name for f in securities.schema()]}")

    # === Demonstrate Operator Overloading ===

    # 1. Lookup with __getitem__
    print("\n🔍 Lookup Examples:")
    print(f"\n  securities['SEC001']:")
    apple = securities['SEC001']
    print(f"    Name: {apple['name']}")
    print(f"    Ticker: {apple['ticker']}")
    print(f"    Sector: {apple['sector']}")

    # 2. Check existence with __contains__
    print(f"\n  'SEC003' in securities: {('SEC003' in securities)}")
    print(f"  'SEC999' in securities: {('SEC999' in securities)}")

    # 3. Get row count with __len__
    print(f"\n  len(securities): {len(securities)}")

    # 4. Safe get with default
    print(f"\n  securities.get('SEC999', {{}}):")
    result = securities.get('SEC999', {'name': 'Not Found'})
    print(f"    {result}")

    # === Load Users Dimension ===
    print("\n📊 Loading users dimension table...")
    users = mat_mgr.dimension_table(
        name='users',
        source=str(users_path),
        key='user_id',
        backend='memory'
    )

    print(f"✅ Loaded {len(users)} users")

    # Lookup user
    print(f"\n  users['U001']:")
    alice = users['U001']
    print(f"    Name: {alice['name']}")
    print(f"    Email: {alice['email']}")
    print(f"    Account Type: {alice['account_type']}")


def demo_dimension_table_enrichment():
    """Demonstrate stream enrichment with dimension tables."""
    print("\n" + "=" * 70)
    print("Demo 2: Stream Enrichment (Conceptual)")
    print("=" * 70)

    # Create sample data
    securities_path, users_path = create_sample_dimension_data()

    # Create Sabot app
    app = sb.App('enrichment-demo', broker='kafka://localhost:19092')
    mat_mgr = app.materializations()

    # Load dimensions
    securities = mat_mgr.dimension_table('securities', source=str(securities_path), key='security_id')
    users = mat_mgr.dimension_table('users', source=str(users_path), key='user_id')

    print("\n📊 Loaded dimension tables:")
    print(f"  - Securities: {len(securities)} records")
    print(f"  - Users: {len(users)} records")

    # Create sample quote data (as Arrow batch)
    print("\n💹 Sample quote data:")
    quotes_data = {
        'quote_id': [1, 2, 3, 4, 5],
        'security_id': ['SEC001', 'SEC002', 'SEC003', 'SEC001', 'SEC004'],
        'user_id': ['U001', 'U002', 'U003', 'U001', 'U004'],
        'price': [150.25, 380.50, 145.75, 151.00, 725.50],
        'quantity': [100, 50, 200, 150, 25]
    }

    quotes_batch = pa.RecordBatch.from_pydict(quotes_data)
    print(f"  Quotes: {quotes_batch.num_rows} records")

    # Demonstrate enrichment using join
    print("\n🔗 Enriching quotes with security information...")

    enriched = securities._cython_mat.enrich_batch(quotes_batch, 'security_id')

    print(f"✅ Enriched {enriched.num_rows} quotes")
    print(f"\nEnriched Schema:")
    for field in enriched.schema:
        print(f"  - {field.name}: {field.type}")

    # Show first enriched record
    print(f"\nFirst enriched quote:")
    enriched_dict = enriched.to_pydict()
    for i in range(min(1, enriched.num_rows)):
        print(f"  Quote ID: {enriched_dict['quote_id'][i]}")
        print(f"  Security: {enriched_dict.get('name', ['N/A'])[i]} ({enriched_dict.get('ticker', ['N/A'])[i]})")
        print(f"  Sector: {enriched_dict.get('sector', ['N/A'])[i]}")
        print(f"  Price: ${enriched_dict['price'][i]:.2f}")
        print(f"  Quantity: {enriched_dict['quantity'][i]}")


def demo_scan_operations():
    """Demonstrate scanning dimension tables."""
    print("\n" + "=" * 70)
    print("Demo 3: Scan Operations")
    print("=" * 70)

    # Create sample data
    securities_path, _ = create_sample_dimension_data()

    # Load dimension
    app = sb.App('scan-demo', broker='kafka://localhost:19092')
    mat_mgr = app.materializations()

    securities = mat_mgr.dimension_table('securities', source=str(securities_path), key='security_id')

    print(f"\n📊 Scanning securities dimension table...")

    # Scan with limit
    batch = securities.scan(limit=3)
    print(f"\nFirst 3 securities:")
    securities_dict = batch.to_pydict()

    for i in range(batch.num_rows):
        print(f"  {i+1}. {securities_dict['name'][i]} ({securities_dict['ticker'][i]}) - {securities_dict['sector'][i]}")

    # Full scan
    full_batch = securities.scan()
    print(f"\nFull scan: {full_batch.num_rows} securities")


def main():
    """Run all demos."""
    try:
        demo_dimension_table_basics()
        demo_dimension_table_enrichment()
        demo_scan_operations()

        print("\n" + "=" * 70)
        print("✅ All demos completed successfully!")
        print("=" * 70)

        print("\n📚 Key Takeaways:")
        print("  1. Dimension tables = lookup-optimized materializations")
        print("  2. Load from Arrow IPC for 52x speedup vs CSV")
        print("  3. Operator overloading: dim['key'], key in dim, len(dim)")
        print("  4. Zero-copy enrichment with CyArrow (104M rows/sec)")
        print("  5. Can be populated from streams, files, operators, or CDC")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
