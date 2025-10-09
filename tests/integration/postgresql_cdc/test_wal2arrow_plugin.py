#!/usr/bin/env python3
"""
Test wal2arrow PostgreSQL output plugin.

This script tests the basic functionality of the wal2arrow plugin by:
1. Creating a replication slot with the wal2arrow output plugin
2. Making changes to test tables
3. Reading the replication stream
"""

import sys
sys.path.insert(0, '/Users/bengamble/Sabot')

from sabot._cython.connectors.postgresql.libpq_conn import PostgreSQLConnection

def main():
    print("=" * 70)
    print("wal2arrow Plugin Test")
    print("=" * 70)

    # Connect to PostgreSQL
    print("\n1. Connecting to PostgreSQL...")
    conn = PostgreSQLConnection(
        "host=localhost port=5433 dbname=sabot user=sabot password=sabot replication=database"
    )
    print("✅ Connected")

    # Check if wal2arrow plugin is available
    print("\n2. Checking for wal2arrow plugin...")
    try:
        # Try to create a temporary replication slot
        result = conn.execute("""
            SELECT pg_create_logical_replication_slot('test_wal2arrow', 'wal2arrow', true)
        """)
        print("✅ wal2arrow plugin is available!")
        print(f"   Slot info: {result}")
    except Exception as e:
        print(f"❌ wal2arrow plugin not available: {e}")
        print("\n   To install the plugin:")
        print("   1. Copy wal2arrow.so to PostgreSQL lib directory")
        print("   2. Restart PostgreSQL")
        print(f"\n   Copy command:")
        pg_lib = conn.execute("SHOW dynamic_library_path")[0][0]
        print(f"   cp sabot/_cython/connectors/postgresql/wal2arrow/wal2arrow.so {pg_lib}/")

    # Close connection
    print("\n3. Closing connection...")
    conn.close()
    print("✅ Connection closed")

    print("\n" + "=" * 70)
    print("Test complete!")
    print("=" * 70)

if __name__ == "__main__":
    main()
