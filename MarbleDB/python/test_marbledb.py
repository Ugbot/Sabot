#!/usr/bin/env python3
"""
Test script for MarbleDB Python bindings.

Run with:
    python test_marbledb.py
"""

import pyarrow as pa
import marbledb
import tempfile
import shutil
import os


def test_basic_operations():
    """Test basic MarbleDB operations."""
    print("=" * 60)
    print("Test 1: Basic Operations")
    print("=" * 60)

    # Create temporary directory
    test_path = tempfile.mkdtemp(prefix="marbledb_test_")
    print(f"Test database path: {test_path}")

    try:
        # Create database options
        options = marbledb.PyDBOptions()
        options.db_path = test_path
        options.enable_wal = True
        options.enable_sparse_index = True
        print("✓ Created database options")

        # Open database
        db = marbledb.MarbleDB.open(options)
        print("✓ Opened database")

        # Create schema
        schema = pa.schema([
            pa.field('id', pa.int64()),
            pa.field('name', pa.string()),
        ])
        print(f"✓ Created schema: {schema}")

        # Create column family
        cf_options = marbledb.PyColumnFamilyOptions()
        cf_options.set_schema(schema)
        cf_options.enable_bloom_filter = True

        db.create_column_family('test_data', cf_options)
        print("✓ Created column family 'test_data'")

        # Insert data
        ids = pa.array([1, 2, 3, 4, 5], type=pa.int64())
        names = pa.array(['Alice', 'Bob', 'Charlie', 'David', 'Eve'], type=pa.string())
        batch = pa.RecordBatch.from_arrays([ids, names], schema=schema)

        db.insert_batch('test_data', batch)
        print(f"✓ Inserted batch with {len(batch)} rows")

        # Scan table
        result = db.scan_table('test_data')
        table = result.to_table()
        print(f"✓ Scanned table: {table.num_rows} rows")
        print(f"  Data:\n{table.to_pandas()}")

        # Flush
        db.flush()
        print("✓ Flushed to disk")

        # Close
        db.close()
        print("✓ Closed database")

        print("\n✅ Test 1 PASSED\n")

    finally:
        # Cleanup
        if os.path.exists(test_path):
            shutil.rmtree(test_path)
            print(f"Cleaned up test directory: {test_path}")


def test_iterator():
    """Test iterator functionality."""
    print("=" * 60)
    print("Test 2: Iterator")
    print("=" * 60)

    test_path = tempfile.mkdtemp(prefix="marbledb_test_")
    print(f"Test database path: {test_path}")

    try:
        # Setup database
        options = marbledb.PyDBOptions()
        options.db_path = test_path
        options.enable_wal = True

        db = marbledb.MarbleDB.open(options)
        print("✓ Opened database")

        # Create schema
        schema = pa.schema([
            pa.field('id', pa.int64()),
            pa.field('value', pa.int64()),
        ])

        cf_options = marbledb.PyColumnFamilyOptions()
        cf_options.set_schema(schema)
        db.create_column_family('iter_data', cf_options)
        print("✓ Created column family")

        # Insert 100 records
        ids = pa.array(list(range(100)), type=pa.int64())
        values = pa.array([i * 10 for i in range(100)], type=pa.int64())
        batch = pa.RecordBatch.from_arrays([ids, values], schema=schema)
        db.insert_batch('iter_data', batch)
        print(f"✓ Inserted {len(batch)} records")

        # Create iterator
        iterator = db.new_iterator('iter_data')
        print("✓ Created iterator")

        # Seek to start
        start_key = marbledb.PyTripleKey(0, 0, 0)
        iterator.seek(start_key)
        print(f"✓ Seeked to: {start_key}")

        # Iterate through records
        count = 0
        while iterator.valid() and count < 10:
            key = iterator.key()
            value = iterator.value()
            if value:
                print(f"  Record {count}: key={key}, rows={len(value)}")
            count += 1
            iterator.next()

        print(f"✓ Iterated through {count} records")

        db.close()
        print("✓ Closed database")

        print("\n✅ Test 2 PASSED\n")

    finally:
        if os.path.exists(test_path):
            shutil.rmtree(test_path)
            print(f"Cleaned up test directory: {test_path}")


def test_context_manager():
    """Test context manager usage."""
    print("=" * 60)
    print("Test 3: Context Manager")
    print("=" * 60)

    test_path = tempfile.mkdtemp(prefix="marbledb_test_")
    print(f"Test database path: {test_path}")

    try:
        options = marbledb.PyDBOptions()
        options.db_path = test_path
        options.enable_wal = True

        # Use context manager
        with marbledb.MarbleDB.open(options) as db:
            print("✓ Opened database with context manager")

            schema = pa.schema([
                pa.field('id', pa.int64()),
                pa.field('data', pa.string()),
            ])

            cf_options = marbledb.PyColumnFamilyOptions()
            cf_options.set_schema(schema)
            db.create_column_family('ctx_data', cf_options)

            ids = pa.array([1, 2, 3], type=pa.int64())
            data = pa.array(['a', 'b', 'c'], type=pa.string())
            batch = pa.RecordBatch.from_arrays([ids, data], schema=schema)
            db.insert_batch('ctx_data', batch)
            print(f"✓ Inserted {len(batch)} records")

        print("✓ Database closed automatically")
        print("\n✅ Test 3 PASSED\n")

    finally:
        if os.path.exists(test_path):
            shutil.rmtree(test_path)
            print(f"Cleaned up test directory: {test_path}")


def test_triple_keys():
    """Test TripleKey operations."""
    print("=" * 60)
    print("Test 4: TripleKey Operations")
    print("=" * 60)

    # Create triple keys
    key1 = marbledb.PyTripleKey(1, 2, 3)
    key2 = marbledb.PyTripleKey(1, 2, 4)
    key3 = marbledb.PyTripleKey(1, 2, 3)

    print(f"key1: {key1}")
    print(f"key2: {key2}")
    print(f"key3: {key3}")

    # Test properties
    assert key1.subject == 1
    assert key1.predicate == 2
    assert key1.object == 3
    print("✓ Key properties correct")

    # Test comparison
    cmp = key1.compare(key2)
    print(f"key1.compare(key2) = {cmp} (should be < 0)")
    assert cmp < 0

    cmp = key1.compare(key3)
    print(f"key1.compare(key3) = {cmp} (should be 0)")
    assert cmp == 0

    print("\n✅ Test 4 PASSED\n")


def test_large_batch():
    """Test large batch insertion."""
    print("=" * 60)
    print("Test 5: Large Batch (10K records)")
    print("=" * 60)

    test_path = tempfile.mkdtemp(prefix="marbledb_test_")
    print(f"Test database path: {test_path}")

    try:
        options = marbledb.PyDBOptions()
        options.db_path = test_path
        options.enable_wal = True

        db = marbledb.MarbleDB.open(options)
        print("✓ Opened database")

        schema = pa.schema([
            pa.field('id', pa.int64()),
            pa.field('value', pa.int64()),
        ])

        cf_options = marbledb.PyColumnFamilyOptions()
        cf_options.set_schema(schema)
        db.create_column_family('large_data', cf_options)

        # Insert 10K records
        ids = pa.array(list(range(10000)), type=pa.int64())
        values = pa.array([i * 100 for i in range(10000)], type=pa.int64())
        batch = pa.RecordBatch.from_arrays([ids, values], schema=schema)

        import time
        start = time.time()
        db.insert_batch('large_data', batch)
        elapsed = time.time() - start

        print(f"✓ Inserted {len(batch)} records in {elapsed:.3f}s")
        print(f"  Throughput: {len(batch)/elapsed:.0f} records/sec")

        # Scan and verify
        start = time.time()
        result = db.scan_table('large_data')
        table = result.to_table()
        elapsed = time.time() - start

        print(f"✓ Scanned {table.num_rows} rows in {elapsed:.3f}s")
        print(f"  Throughput: {table.num_rows/elapsed:.0f} rows/sec")

        db.close()
        print("\n✅ Test 5 PASSED\n")

    finally:
        if os.path.exists(test_path):
            shutil.rmtree(test_path)
            print(f"Cleaned up test directory: {test_path}")


def main():
    """Run all tests."""
    print("\n" + "=" * 60)
    print("MarbleDB Python Bindings Test Suite")
    print("=" * 60 + "\n")

    tests = [
        test_basic_operations,
        test_iterator,
        test_context_manager,
        test_triple_keys,
        test_large_batch,
    ]

    passed = 0
    failed = 0

    for test_func in tests:
        try:
            test_func()
            passed += 1
        except Exception as e:
            print(f"\n❌ {test_func.__name__} FAILED:")
            print(f"   {e}")
            import traceback
            traceback.print_exc()
            failed += 1
            print()

    print("=" * 60)
    print(f"Test Results: {passed} passed, {failed} failed")
    print("=" * 60)

    return 0 if failed == 0 else 1


if __name__ == '__main__':
    import sys
    sys.exit(main())
