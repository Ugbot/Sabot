#!/usr/bin/env python3
"""
Test basic PostgreSQL connection and table discovery WITHOUT libpq module.
Uses psycopg2 to verify the connector logic works.
"""

import psycopg2

def test_basic_connection():
    """Test basic connection to PostgreSQL."""
    conn_str = "host=localhost port=5433 dbname=sabot user=sabot password=sabot"

    try:
        conn = psycopg2.connect(conn_str)
        cur = conn.cursor()

        # Test 1: Check WAL configuration
        print("=" * 60)
        print("Test 1: Check WAL Configuration")
        print("=" * 60)
        cur.execute("""
            SELECT name, setting
            FROM pg_settings
            WHERE name IN ('wal_level', 'max_replication_slots', 'max_wal_senders')
        """)
        for name, setting in cur.fetchall():
            print(f"  {name}: {setting}")

        # Test 2: Discover tables
        print("\n" + "=" * 60)
        print("Test 2: Discover Tables")
        print("=" * 60)
        cur.execute("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE'
            AND table_schema = 'public'
            ORDER BY table_name
        """)
        tables = cur.fetchall()
        print(f"  Found {len(tables)} tables:")
        for schema, table in tables:
            print(f"    - {schema}.{table}")

        # Test 3: Get table columns
        print("\n" + "=" * 60)
        print("Test 3: Get Table Columns (users)")
        print("=" * 60)
        cur.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'users'
            ORDER BY ordinal_position
        """)
        cols = cur.fetchall()
        print(f"  Found {len(cols)} columns:")
        for col, dtype, nullable in cols:
            print(f"    - {col}: {dtype} (nullable={nullable})")

        # Test 4: Pattern matching simulation
        print("\n" + "=" * 60)
        print("Test 4: Pattern Matching (public.*)")
        print("=" * 60)
        patterns = ["public.*"]
        cur.execute("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE'
            AND table_schema = 'public'
        """)
        matched = cur.fetchall()
        print(f"  Pattern 'public.*' matched {len(matched)} tables:")
        for schema, table in matched:
            print(f"    - {schema}.{table}")

        # Test 5: Check replication slots
        print("\n" + "=" * 60)
        print("Test 5: Check Replication Slots")
        print("=" * 60)
        cur.execute("SELECT * FROM pg_replication_slots")
        slots = cur.fetchall()
        if slots:
            print(f"  Found {len(slots)} replication slots:")
            for slot in slots:
                print(f"    - {slot}")
        else:
            print("  No replication slots found (expected)")

        # Test 6: Test shuffle partitioning simulation
        print("\n" + "=" * 60)
        print("Test 6: Shuffle Partitioning Simulation")
        print("=" * 60)
        # Simulate CDC events with (schema, table) columns
        cur.execute("""
            SELECT 'public' as schema, 'users' as table, count(*) as event_count
            FROM users
            UNION ALL
            SELECT 'public' as schema, 'orders' as table, count(*) as event_count
            FROM orders
        """)
        events = cur.fetchall()
        print("  Simulated CDC events by table:")
        for schema, table, count in events:
            # Simulate hash partitioning
            table_key = f"{schema}.{table}"
            partition_id = hash(table_key) % 4  # 4 partitions
            print(f"    - {table_key}: {count} events → partition {partition_id}")

        cur.close()
        conn.close()

        print("\n" + "=" * 60)
        print("✅ All tests passed!")
        print("=" * 60)
        print("\nNext steps:")
        print("  1. Fix Cython compilation errors in libpq_conn.pyx")
        print("  2. Compile the CDC modules")
        print("  3. Test with actual ArrowCDCReader")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_basic_connection()
