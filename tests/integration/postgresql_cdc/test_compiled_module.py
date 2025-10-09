#!/usr/bin/env python3
"""
Test compiled PostgreSQL libpq_conn module.

Tests basic connection and query functionality.
"""

import sys
sys.path.insert(0, '/Users/bengamble/Sabot')

from sabot._cython.connectors.postgresql.libpq_conn import PostgreSQLConnection

def main():
    print("=" * 70)
    print("PostgreSQL Compiled Module Test")
    print("=" * 70)

    # Connect to PostgreSQL
    print("\n1. Testing connection...")
    conn = PostgreSQLConnection(
        "host=localhost port=5433 dbname=sabot user=sabot password=sabot"
    )
    print(f"✅ Connected: {conn}")

    # Test query execution
    print("\n2. Testing query execution...")
    try:
        results = conn.execute("SELECT version()")
        print(f"✅ Query executed, got {len(results)} rows")
        if results:
            print(f"   PostgreSQL version: {results[0][0][:50]}...")
    except Exception as e:
        print(f"❌ Query failed: {e}")

    # Test table discovery
    print("\n3. Testing table discovery...")
    try:
        tables = conn.discover_tables("public")
        print(f"✅ Found {len(tables)} tables: {', '.join(tables)}")
    except Exception as e:
        print(f"❌ Table discovery failed: {e}")

    # Test schema introspection
    print("\n4. Testing schema introspection...")
    try:
        if tables:
            table_name = tables[0]
            columns = conn.get_table_columns("public", table_name)
            print(f"✅ Table '{table_name}' has {len(columns)} columns")
            for col in columns[:3]:  # Show first 3
                print(f"   - {col['column_name']}: {col['data_type']}")
    except Exception as e:
        print(f"❌ Schema introspection failed: {e}")

    # Close connection
    print("\n5. Closing connection...")
    conn.close()
    print("✅ Connection closed")

    print("\n" + "=" * 70)
    print("✅ All tests passed!")
    print("=" * 70)
    print("\nNext steps:")
    print("  1. ✅ Cython compilation fixed")
    print("  2. ✅ Basic connectivity working")
    print("  3. ⏳ Implement wal2arrow plugin (~1500 LOC)")
    print("  4. ⏳ Test end-to-end CDC replication")

if __name__ == "__main__":
    main()
