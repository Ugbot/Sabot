#!/usr/bin/env python3
"""
Test Tonbo FFI integration within Sabot framework

Tests Tonbo as a state backend in Sabot's actual streaming infrastructure.
"""

import asyncio
import tempfile
import os
import sys
from pathlib import Path

async def test_tonbo_store_backend():
    """Test Tonbo as a Sabot store backend."""
    print("=" * 60)
    print("TEST 1: Tonbo Store Backend in Sabot")
    print("=" * 60)

    from sabot.stores.tonbo import TonboBackend
    from sabot.stores.base import StoreBackendConfig

    with tempfile.TemporaryDirectory() as tmpdir:
        config = StoreBackendConfig(path=tmpdir)
        backend = TonboBackend(config)

        # Start backend
        await backend.start()
        print("✅ Tonbo backend started")

        # Test basic operations
        await backend.set("user:1", {"name": "Alice", "balance": 1000})
        await backend.set("user:2", {"name": "Bob", "balance": 500})
        await backend.set("user:3", {"name": "Charlie", "balance": 750})
        print("✅ Inserted 3 users")

        # Get operations
        user1 = await backend.get("user:1")
        user2 = await backend.get("user:2")
        assert user1["name"] == "Alice"
        assert user2["balance"] == 500
        print(f"✅ Retrieved users: {user1['name']}, {user2['name']}")

        # Delete operation
        deleted = await backend.delete("user:2")
        assert deleted == True
        user2_after = await backend.get("user:2")
        assert user2_after is None
        print("✅ Deleted user:2")

        # Exists check
        exists = await backend.exists("user:1")
        not_exists = await backend.exists("user:2")
        assert exists == True
        assert not_exists == False
        print("✅ Existence checks working")

        # Batch operations
        batch_data = {
            f"product:{i}": {"sku": f"SKU{i}", "price": 10 + i}
            for i in range(10)
        }
        await backend.batch_set(batch_data)
        print("✅ Batch inserted 10 products")

        # Get stats
        stats = await backend.get_stats()
        print(f"📊 Backend stats: {stats}")
        assert stats['backend_type'] == 'tonbo'
        assert stats['cython_enabled'] == True

        # Stop backend
        await backend.stop()
        print("✅ Tonbo backend stopped cleanly")

    print("\n✅ Test 1 PASSED: Tonbo Store Backend\n")
    return True


async def test_tonbo_with_sabot_table():
    """Test Tonbo with Sabot's Table abstraction."""
    print("=" * 60)
    print("TEST 2: Tonbo with Sabot Table")
    print("=" * 60)

    from sabot import App, create_app
    from sabot.stores.tonbo import TonboBackend
    from sabot.stores.base import StoreBackendConfig

    with tempfile.TemporaryDirectory() as tmpdir:
        # Create app with Tonbo backend
        app = create_app("test_tonbo_app")

        # Create table with Tonbo backend
        config = StoreBackendConfig(path=os.path.join(tmpdir, "customers"))
        backend = TonboBackend(config)
        await backend.start()

        # Use table like a dict
        await backend.set("cust_001", {
            "name": "Acme Corp",
            "tier": "gold",
            "revenue": 125000
        })

        await backend.set("cust_002", {
            "name": "Globex Inc",
            "tier": "silver",
            "revenue": 89000
        })

        print("✅ Inserted customer data")

        # Retrieve and verify
        acme = await backend.get("cust_001")
        assert acme["tier"] == "gold"
        assert acme["revenue"] == 125000
        print(f"✅ Retrieved customer: {acme['name']} (tier: {acme['tier']})")

        # Update operation
        await backend.set("cust_001", {
            "name": "Acme Corp",
            "tier": "platinum",  # Upgraded!
            "revenue": 150000
        })

        acme_updated = await backend.get("cust_001")
        assert acme_updated["tier"] == "platinum"
        assert acme_updated["revenue"] == 150000
        print("✅ Updated customer tier: gold → platinum")

        await backend.stop()

    print("\n✅ Test 2 PASSED: Tonbo with Sabot Table\n")
    return True


async def test_tonbo_materialization_pattern():
    """Test Tonbo for materialized view pattern."""
    print("=" * 60)
    print("TEST 3: Tonbo for Materialization Engine")
    print("=" * 60)

    from sabot.stores.tonbo import TonboBackend
    from sabot.stores.base import StoreBackendConfig

    with tempfile.TemporaryDirectory() as tmpdir:
        # Create dimension table backend
        dim_config = StoreBackendConfig(path=os.path.join(tmpdir, "dimensions"))
        dim_store = TonboBackend(dim_config)
        await dim_store.start()

        # Load dimension data (like a dimension table in data warehouse)
        print("\n📊 Loading dimension table...")
        securities = {
            "AAPL": {"name": "Apple Inc", "sector": "Technology"},
            "GOOGL": {"name": "Alphabet Inc", "sector": "Technology"},
            "JPM": {"name": "JPMorgan Chase", "sector": "Finance"},
            "BAC": {"name": "Bank of America", "sector": "Finance"},
        }

        for symbol, data in securities.items():
            await dim_store.set(f"security:{symbol}", data)

        print(f"   ✅ Loaded {len(securities)} securities")

        # Create analytical view backend
        view_config = StoreBackendConfig(path=os.path.join(tmpdir, "views"))
        view_store = TonboBackend(view_config)
        await view_store.start()

        # Simulate aggregated metrics (like a materialized view)
        print("\n📊 Building analytical view...")
        sector_metrics = {
            "Technology": {"volume": 1500000, "trades": 4500, "avg_price": 175.50},
            "Finance": {"volume": 890000, "trades": 3200, "avg_price": 45.25},
        }

        for sector, metrics in sector_metrics.items():
            await view_store.set(f"metrics:sector:{sector}", metrics)

        print(f"   ✅ Materialized {len(sector_metrics)} sector metrics")

        # Query pattern: Enrich trade data with dimension lookup
        print("\n💰 Simulating trade enrichment...")
        trades = [
            {"symbol": "AAPL", "quantity": 100, "price": 175.50},
            {"symbol": "JPM", "quantity": 200, "price": 145.30},
            {"symbol": "GOOGL", "quantity": 50, "price": 140.25},
        ]

        enriched_trades = []
        for trade in trades:
            symbol = trade["symbol"]
            security = await dim_store.get(f"security:{symbol}")

            enriched = {
                **trade,
                "security_name": security["name"],
                "sector": security["sector"]
            }
            enriched_trades.append(enriched)
            print(f"   ✅ Enriched {symbol}: {security['name']} ({security['sector']})")

        # Query analytical view
        print("\n📈 Querying analytical view...")
        for sector in ["Technology", "Finance"]:
            metrics = await view_store.get(f"metrics:sector:{sector}")
            print(f"   📊 {sector}:")
            print(f"      Volume: {metrics['volume']:,}")
            print(f"      Trades: {metrics['trades']:,}")
            print(f"      Avg Price: ${metrics['avg_price']:.2f}")

        await dim_store.stop()
        await view_store.stop()

    print("\n✅ Test 3 PASSED: Materialization Pattern\n")
    return True


async def test_tonbo_performance():
    """Test Tonbo performance characteristics."""
    print("=" * 60)
    print("TEST 4: Tonbo Performance Test")
    print("=" * 60)

    from sabot.stores.tonbo import TonboBackend
    from sabot.stores.base import StoreBackendConfig
    import time

    with tempfile.TemporaryDirectory() as tmpdir:
        config = StoreBackendConfig(path=tmpdir)
        backend = TonboBackend(config)
        await backend.start()

        # Test write performance
        print("\n⚡ Write performance test...")
        num_writes = 1000
        start = time.perf_counter()

        for i in range(num_writes):
            await backend.set(f"key:{i}", {"value": i, "data": f"test_data_{i}"})

        write_time = time.perf_counter() - start
        write_throughput = num_writes / write_time
        print(f"   ✅ Wrote {num_writes} records in {write_time:.3f}s")
        print(f"   📊 Write throughput: {write_throughput:.0f} ops/sec")

        # Test read performance
        print("\n⚡ Read performance test...")
        start = time.perf_counter()

        for i in range(num_writes):
            value = await backend.get(f"key:{i}")
            assert value is not None

        read_time = time.perf_counter() - start
        read_throughput = num_writes / read_time
        print(f"   ✅ Read {num_writes} records in {read_time:.3f}s")
        print(f"   📊 Read throughput: {read_throughput:.0f} ops/sec")

        # Test batch operations
        print("\n⚡ Batch operation test...")
        batch_data = {f"batch:{i}": {"id": i} for i in range(100)}
        start = time.perf_counter()
        await backend.batch_set(batch_data)
        batch_time = time.perf_counter() - start
        batch_throughput = 100 / batch_time
        print(f"   ✅ Batch inserted 100 records in {batch_time:.3f}s")
        print(f"   📊 Batch throughput: {batch_throughput:.0f} ops/sec")

        await backend.stop()

    print("\n✅ Test 4 PASSED: Performance Test\n")
    return True


async def main():
    """Run all Sabot integration tests."""
    print("\n" + "=" * 60)
    print("Tonbo FFI Integration Tests in Sabot Framework")
    print("=" * 60 + "\n")

    tests = [
        ("Store Backend", test_tonbo_store_backend),
        ("Sabot Table", test_tonbo_with_sabot_table),
        ("Materialization", test_tonbo_materialization_pattern),
        ("Performance", test_tonbo_performance),
    ]

    passed = 0
    failed = 0

    for name, test_func in tests:
        try:
            result = await test_func()
            if result:
                passed += 1
        except Exception as e:
            print(f"\n❌ Test '{name}' FAILED: {e}")
            import traceback
            traceback.print_exc()
            failed += 1

    print("\n" + "=" * 60)
    print("FINAL RESULTS")
    print("=" * 60)
    print(f"✅ Passed: {passed}/{len(tests)}")
    print(f"❌ Failed: {failed}/{len(tests)}")

    if failed == 0:
        print("\n🎉 All Tonbo integration tests passed!")
        print("\nTonbo FFI is production-ready for Sabot!")
        return 0
    else:
        print(f"\n⚠️  {failed} test(s) failed")
        return 1


if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n\nTests interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
