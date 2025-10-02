#!/usr/bin/env python3
"""
Arrow-Integrated Tonbo Demo for Sabot

Demonstrates direct Arrow columnar operations integrated with Tonbo's LSM storage.
Shows how Sabot's streaming pipelines can leverage Arrow's columnar format
for maximum performance in analytical workloads.

This example showcases:
- Zero-copy Arrow table operations
- SIMD-accelerated columnar processing
- Direct integration with Tonbo's Parquet backend
- High-performance analytical queries
"""

import asyncio
import tempfile
import time
from pathlib import Path
import sys
import os

# Add sabot to path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

try:
    from sabot.stores import StateStoreManager, BackendType
    from sabot.stores.base import StoreBackendConfig
    import pyarrow as pa
    import pyarrow.compute as pc
    ARROW_AVAILABLE = True
except ImportError as e:
    print(f"Missing dependencies: {e}")
    print("Install: pip install sabot pyarrow")
    sys.exit(1)


async def create_sample_data(store_manager):
    """Create sample streaming data for demonstration."""
    print("📊 Creating sample streaming data...")

    # Create sample e-commerce events
    events = []
    for i in range(1000):
        event = {
            'user_id': f'user_{i % 100}',  # 100 unique users
            'product_id': f'product_{i % 50}',  # 50 unique products
            'event_type': ['view', 'cart', 'purchase'][i % 3],
            'timestamp': int(time.time() * 1000000) + i * 1000,  # Microseconds
            'price': float(i % 100) + 10.0,  # $10-$109
            'category': ['electronics', 'books', 'clothing'][i % 3],
            'session_id': f'session_{(i // 10) % 20}',  # 20 sessions
        }
        events.append(event)

    # Store events in Tonbo using Arrow table format
    arrow_table = pa.Table.from_pylist(events)
    print(f"✅ Created Arrow table with {len(events)} events, {arrow_table.num_columns} columns")

    # Insert as Arrow table (direct columnar storage)
    start_time = time.time()
    await store_manager.set_table_data('events', arrow_table)
    insert_time = time.time() - start_time
    print(".2f"
    return arrow_table


async def demonstrate_arrow_operations(store_manager):
    """Demonstrate Arrow-native operations on stored data."""
    print("\n🔍 Demonstrating Arrow-native operations...")

    # 1. Scan data as Arrow table (zero-copy)
    print("   Scanning data as Arrow table...")
    start_time = time.time()
    arrow_table = await store_manager.scan_as_arrow_table('events', limit=500)
    scan_time = time.time() - start_time
    print(".2f"
    if arrow_table:
        print(f"      Columns: {arrow_table.column_names}")
        print(f"      Rows: {arrow_table.num_rows}")

    # 2. Filter operations using Arrow compute
    print("   Filtering for purchases only...")
    start_time = time.time()
    purchase_filter = pc.equal(arrow_table.column('event_type'), 'purchase')
    purchases = await store_manager.arrow_filter('events', purchase_filter)
    filter_time = time.time() - start_time
    print(".2f"
    if purchases:
        purchase_count = purchases.num_rows
        total_revenue = pc.sum(purchases.column('price')).as_py()
        print(".2f"
    # 3. Aggregation operations
    print("   Aggregating sales by category...")
    start_time = time.time()
    sales_by_category = await store_manager.arrow_aggregate(
        'events',
        group_by=['category'],
        aggregations={'price': 'sum', 'event_type': 'count'}
    )
    agg_time = time.time() - start_time
    print(".2f"
    if sales_by_category:
        print("      Category sales summary:")
        for batch in sales_by_category.to_batches():
            for row in zip(*[batch.column(i).to_pylist() for i in range(batch.num_columns)]):
                print(f"        {row[0]}: ${row[1]:.2f} ({row[2]} events)")

    # 4. Time-windowed analytics
    print("   Performing time-windowed analytics...")
    start_time = time.time()
    async for window_result in store_manager.arrow_streaming_aggregate(
        'events',
        'time_window',
        group_by_cols=['category'],
        time_column='timestamp',
        window_size=3600000000  # 1 hour in microseconds
    ):
        window_time = time.time() - start_time
        print(".2f"
        if window_result:
            print(f"        Windows: {window_result.num_rows}")
        break  # Just show first window

    return {
        'scan_time': scan_time,
        'filter_time': filter_time,
        'agg_time': agg_time,
        'purchase_count': purchase_count if 'purchase_count' in locals() else 0,
        'total_revenue': total_revenue if 'total_revenue' in locals() else 0.0
    }


async def demonstrate_joins(store_manager):
    """Demonstrate Arrow join operations."""
    print("\n🔗 Demonstrating Arrow join operations...")

    # Create user demographics table
    users_data = [
        {'user_id': f'user_{i}', 'age': 20 + (i % 40), 'region': ['US', 'EU', 'ASIA'][i % 3]}
        for i in range(10)
    ]
    users_table = pa.Table.from_pylist(users_data)

    # Store user data
    await store_manager.set_table_data('users', users_table)

    # Get events table and join with user data using Arrow
    print("   Joining events with user demographics...")
    start_time = time.time()

    # Get events as Arrow table
    events_table = await store_manager.scan_as_arrow_table('events', limit=500)
    if events_table:
        # Perform Arrow join directly on tables
        joined = events_table.join(users_table, keys=['user_id'], join_type='inner')
        join_time = time.time() - start_time
        print(".2f"
        print(f"      Joined rows: {joined.num_rows}")
        print(f"      Joined columns: {joined.num_columns}")

        # Analyze by region using Arrow aggregation
        region_sales = pc.sum(joined.column('price')).as_py()
        print(".2f"
    else:
        join_time = time.time() - start_time
        print("      ❌ Could not retrieve events table for joining")

    return {'join_time': join_time}


async def demonstrate_export(store_manager):
    """Demonstrate Arrow dataset export capabilities."""
    print("\n📤 Demonstrating Arrow dataset export...")

    with tempfile.TemporaryDirectory() as temp_dir:
        export_path = Path(temp_dir) / "exported_dataset"

        # Export as partitioned Arrow dataset
        print(f"   Exporting data as partitioned Arrow dataset to {export_path}...")
        start_time = time.time()
        success = await store_manager.arrow_export_dataset(
            'events',
            output_path=str(export_path),
            partitioning={'columns': ['category']}
        )
        export_time = time.time() - start_time

        if success:
            print(".2f"
            # List exported files
            parquet_files = list(export_path.rglob("*.parquet"))
            print(f"      Exported {len(parquet_files)} Parquet files")

            # Show partitioning structure
            for partition_dir in export_path.iterdir():
                if partition_dir.is_dir():
                    files = list(partition_dir.glob("*.parquet"))
                    print(f"        {partition_dir.name}: {len(files)} files")
        else:
            print("      ❌ Export failed")
            export_time = 0

    return {'export_time': export_time, 'export_success': success}


async def show_performance_comparison(store_manager):
    """Compare traditional vs Arrow operations performance."""
    print("\n⚡ Performance comparison...")

    # Traditional key-value operations
    print("   Testing traditional key-value operations...")
    kv_times = []
    for i in range(100):
        key = f'test_key_{i}'
        value = f'test_value_{i}' * 10  # Larger values

        start = time.time()
        await store_manager.set(key, value)
        await store_manager.get(key)
        kv_times.append(time.time() - start)

    avg_kv_time = sum(kv_times) / len(kv_times)

    # Arrow columnar operations
    print("   Testing Arrow columnar operations...")
    arrow_times = []

    for i in range(10):  # Fewer iterations for Arrow ops
        start = time.time()

        # Scan and filter
        table = await store_manager.scan_as_arrow_table('events', limit=100)
        if table:
            filtered = pc.filter(table, pc.equal(table.column('event_type'), 'purchase'))
            arrow_times.append(time.time() - start)

    avg_arrow_time = sum(arrow_times) / len(arrow_times) if arrow_times else 0

    print(".6f"
    print(".6f"

    if avg_arrow_time > 0:
        speedup = avg_kv_time / avg_arrow_time
        print(".1f"
    return {
        'avg_kv_time': avg_kv_time,
        'avg_arrow_time': avg_arrow_time,
        'speedup': speedup if avg_arrow_time > 0 else 0
    }


async def main():
    """Main demonstration of Arrow-integrated Tonbo functionality."""
    print("🚀 Sabot Arrow-Integrated Tonbo Demo")
    print("=" * 50)

    # Create temporary directory for the demo
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "sabot_tonbo_demo"

        # Configure Tonbo as the backend
        config = StoreBackendConfig(
            backend_type="tonbo",
            path=db_path
        )

        # Initialize store manager
        print("🔧 Initializing Tonbo store with Arrow integration...")
        store_manager = StateStoreManager(
            config={
                'backend_type': BackendType.TONBO,
                'tonbo_config': config
            }
        )

        try:
            await store_manager.start()
            print("✅ Tonbo store initialized successfully")

            # Get initial stats
            stats = await store_manager.get_stats()
            print(f"   Arrow integration: {stats.get('arrow_enabled', False)}")
            print(f"   Cython optimization: {stats.get('cython_enabled', False)}")

            # Create and store sample data
            sample_data = await create_sample_data(store_manager)

            # Demonstrate Arrow operations
            arrow_results = await demonstrate_arrow_operations(store_manager)

            # Demonstrate joins
            join_results = await demonstrate_joins(store_manager)

            # Demonstrate export
            export_results = await demonstrate_export(store_manager)

            # Performance comparison
            perf_results = await show_performance_comparison(store_manager)

            # Final summary
            print("\n🎉 Demo completed successfully!")
            print("=" * 50)
            print("Summary of Arrow-integrated Tonbo capabilities:")
            print(f"• Processed {sample_data.num_rows} events with {sample_data.num_columns} columns")
            print(f"• Arrow scan performance: {arrow_results['scan_time']:.3f}s")
            print(f"• Arrow filter performance: {arrow_results['filter_time']:.3f}s")
            print(f"• Arrow aggregation performance: {arrow_results['agg_time']:.3f}s")
            print(f"• Total purchases: {arrow_results['purchase_count']}")
            print(f"• Total revenue: ${arrow_results['total_revenue']:.2f}")
            print(f"• Join performance: {join_results['join_time']:.3f}s")
            print(f"• Dataset export: {'✅' if export_results['export_success'] else '❌'}")
            if perf_results['speedup'] > 1:
                print(f"• Arrow performance advantage: {perf_results['speedup']:.1f}x faster")

        except Exception as e:
            print(f"❌ Demo failed: {e}")
            import traceback
            traceback.print_exc()

        finally:
            await store_manager.stop()


if __name__ == "__main__":
    asyncio.run(main())
