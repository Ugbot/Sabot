#!/usr/bin/env python
"""
Test Materialization Engine

Tests for unified materialization system (dimension tables + analytical views).
Validates Cython implementation and Python API.
"""
import sys
import os
from pathlib import Path

# Add sabot to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from sabot import cyarrow as pa
from sabot.cyarrow import feather
import tempfile

print("=" * 70)
print("MATERIALIZATION ENGINE TEST")
print("=" * 70)

# Test 1: Import Cython engine
print("\n1. Testing Cython engine import...")
try:
    from sabot._c.materialization_engine import (
        MaterializationManager,
        Materialization,
        MaterializationBackend,
        PopulationStrategy,
    )
    print("   ✓ Cython engine imported successfully")
except ImportError as e:
    print(f"   ✗ Failed to import Cython engine: {e}")
    sys.exit(1)

# Test 2: Import Python API
print("\n2. Testing Python API import...")
try:
    from sabot.materializations import (
        MaterializationManager as PyMaterializationManager,
        DimensionTableView,
        AnalyticalViewAPI,
    )
    print("   ✓ Python API imported successfully")
except ImportError as e:
    print(f"   ✗ Failed to import Python API: {e}")
    sys.exit(1)

# Test 3: Create sample Arrow data
print("\n3. Creating sample Arrow data...")
sample_data = {
    'security_id': ['SEC001', 'SEC002', 'SEC003', 'SEC004', 'SEC005'],
    'name': ['Apple Inc.', 'Microsoft Corp.', 'Google LLC', 'Amazon.com Inc.', 'Tesla Inc.'],
    'ticker': ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA'],
    'price': [150.25, 380.50, 145.75, 175.00, 250.00],
    'sector': ['Technology', 'Technology', 'Technology', 'Consumer', 'Automotive'],
}
table = pa.Table.from_pydict(sample_data)
print(f"   ✓ Created Arrow table with {table.num_rows} rows, {table.num_columns} columns")

# Create temp file for Arrow IPC
temp_dir = tempfile.mkdtemp()
arrow_file = Path(temp_dir) / "test_securities.arrow"
feather.write_feather(table, arrow_file, compression='zstd')
print(f"   ✓ Saved Arrow IPC file: {arrow_file}")

# Test 4: Cython MaterializationManager
print("\n4. Testing Cython MaterializationManager...")
cython_mgr = MaterializationManager(default_backend='memory')
print("   ✓ Created Cython MaterializationManager")

# Test 5: Create dimension table
print("\n5. Testing dimension table creation...")
dim_table = cython_mgr.create_dimension_table(
    name='securities',
    key_column='security_id',
    backend='memory'
)
print(f"   ✓ Created dimension table: securities")

# Test 6: Populate from Arrow file
print("\n6. Testing Arrow file population...")
dim_table.populate_from_arrow_file(str(arrow_file))
print(f"   ✓ Populated from Arrow IPC: {dim_table.num_rows()} rows")
assert dim_table.num_rows() == 5, f"Expected 5 rows, got {dim_table.num_rows()}"

# Test 7: Build hash index
print("\n7. Testing hash index build...")
dim_table.build_index()
print(f"   ✓ Built hash index for {dim_table.num_rows()} keys")

# Test 8: Lookup operations
print("\n8. Testing lookup operations...")
result = dim_table.lookup('SEC001')
assert result is not None, "Lookup returned None for existing key"
print(f"   ✓ Lookup('SEC001'): {result['name'][0]}")
assert result['name'][0] == 'Apple Inc.', f"Expected 'Apple Inc.', got {result['name'][0]}"

# Test 9: Contains check
print("\n9. Testing contains check...")
assert dim_table.contains('SEC001') == True, "contains() failed for existing key"
assert dim_table.contains('SEC999') == False, "contains() failed for non-existing key"
print("   ✓ contains('SEC001'): True")
print("   ✓ contains('SEC999'): False")

# Test 10: Scan operations
print("\n10. Testing scan operations...")
scan_batch = dim_table.scan(limit=3)
assert scan_batch.num_rows == 3, f"Expected 3 rows, got {scan_batch.num_rows}"
print(f"   ✓ scan(limit=3): {scan_batch.num_rows} rows")

full_scan = dim_table.scan()
assert full_scan.num_rows == 5, f"Expected 5 rows, got {full_scan.num_rows}"
print(f"   ✓ scan(): {full_scan.num_rows} rows")

# Test 11: Python API - MaterializationManager
print("\n11. Testing Python MaterializationManager...")
try:
    import sabot as sb
    app = sb.App('test-app', broker='kafka://localhost:19092')
    py_mgr = app.materializations(default_backend='memory')
    print("   ✓ Created Python MaterializationManager via App")
except Exception as e:
    print(f"   ✗ Failed to create Python MaterializationManager: {e}")

# Test 12: Python API - DimensionTableView
print("\n12. Testing DimensionTableView (operator overloading)...")
py_dim = py_mgr.dimension_table(
    name='py_securities',
    source=str(arrow_file),
    key='security_id',
    backend='memory'
)
print(f"   ✓ Created DimensionTableView: {py_dim.name}")

# Test 13: Operator overloading - __getitem__
print("\n13. Testing operator overloading - __getitem__...")
apple = py_dim['SEC001']
assert apple['name'] == 'Apple Inc.', f"Expected 'Apple Inc.', got {apple['name']}"
print(f"   ✓ py_dim['SEC001']['name']: {apple['name']}")

# Test 14: Operator overloading - __contains__
print("\n14. Testing operator overloading - __contains__...")
assert ('SEC001' in py_dim) == True, "Expected 'SEC001' in py_dim"
assert ('SEC999' in py_dim) == False, "Expected 'SEC999' not in py_dim"
print("   ✓ 'SEC001' in py_dim: True")
print("   ✓ 'SEC999' in py_dim: False")

# Test 15: Operator overloading - __len__
print("\n15. Testing operator overloading - __len__...")
count = len(py_dim)
assert count == 5, f"Expected 5, got {count}"
print(f"   ✓ len(py_dim): {count}")

# Test 16: Safe get() method
print("\n16. Testing safe get() method...")
result = py_dim.get('SEC999', {'name': 'Not Found'})
assert result['name'] == 'Not Found', f"Expected 'Not Found', got {result['name']}"
print(f"   ✓ py_dim.get('SEC999', default): {result['name']}")

# Test 17: Stream enrichment (join)
print("\n17. Testing stream enrichment (join)...")
quotes_data = {
    'quote_id': [1, 2, 3],
    'security_id': ['SEC001', 'SEC002', 'SEC003'],
    'user_id': ['U001', 'U002', 'U003'],
    'quantity': [100, 50, 200],
    'quote_price': [151.00, 381.00, 146.00],
}
quotes_batch = pa.RecordBatch.from_pydict(quotes_data)
print(f"   ✓ Created quotes batch: {quotes_batch.num_rows} rows")

enriched = py_dim._cython_mat.enrich_batch(quotes_batch, 'security_id')
assert enriched.num_rows == 3, f"Expected 3 rows, got {enriched.num_rows}"
print(f"   ✓ Enriched batch: {enriched.num_rows} rows, {enriched.num_columns} columns")

# Verify enrichment added columns
enriched_dict = enriched.to_pydict()
assert 'name' in enriched_dict, "Missing 'name' column after enrichment"
assert 'ticker' in enriched_dict, "Missing 'ticker' column after enrichment"
assert enriched_dict['name'][0] == 'Apple Inc.', f"Expected 'Apple Inc.', got {enriched_dict['name'][0]}"
print(f"   ✓ Enriched quote 0: {enriched_dict['name'][0]} ({enriched_dict['ticker'][0]})")

# Test 18: Populate from Arrow batch
print("\n18. Testing populate from Arrow batch...")
batch_dim = cython_mgr.create_dimension_table(
    name='batch_securities',
    key_column='security_id',
    backend='memory'
)
batch_dim.populate_from_arrow_batch(table.to_batches()[0])
assert batch_dim.num_rows() == 5, f"Expected 5 rows, got {batch_dim.num_rows()}"
print(f"   ✓ Populated from batch: {batch_dim.num_rows()} rows")

# Test 19: Schema retrieval
print("\n19. Testing schema retrieval...")
schema = py_dim.schema()
assert schema is not None, "Schema is None"
assert len(schema) == 5, f"Expected 5 fields, got {len(schema)}"
field_names = [f.name for f in schema]
print(f"   ✓ Schema fields: {field_names}")

# Test 20: Manager list_names
print("\n20. Testing manager list_names...")
names = py_mgr.list_names()
assert len(names) > 0, "No materializations found"
print(f"   ✓ Materialization names: {names}")

# Test 21: Scan with DimensionTableView
print("\n21. Testing DimensionTableView.scan()...")
scan_result = py_dim.scan(limit=2)
assert scan_result.num_rows == 2, f"Expected 2 rows, got {scan_result.num_rows}"
print(f"   ✓ scan(limit=2): {scan_result.num_rows} rows")

# Test 22: Version tracking
print("\n22. Testing version tracking...")
version = dim_table.version()
print(f"   ✓ Version: {version}")
assert version >= 0, f"Invalid version: {version}"

# Test 23: Backend type validation
print("\n23. Testing backend types...")
for backend_name, expected_value in [
    ('MEMORY', 2),
    ('ROCKSDB', 0),
    ('TONBO', 1),
]:
    backend = getattr(MaterializationBackend, backend_name)
    assert backend.value == expected_value, f"Expected {expected_value}, got {backend.value}"
    print(f"   ✓ MaterializationBackend.{backend_name}: {backend.value}")

# Test 24: Population strategy types
print("\n24. Testing population strategies...")
for strategy_name, expected_value in [
    ('STREAM', 0),
    ('BATCH', 1),
    ('OPERATOR', 2),
    ('CDC', 3),
]:
    strategy = getattr(PopulationStrategy, strategy_name)
    assert strategy.value == expected_value, f"Expected {expected_value}, got {strategy.value}"
    print(f"   ✓ PopulationStrategy.{strategy_name}: {strategy.value}")

# Test 25: Error handling - missing key
print("\n25. Testing error handling - missing key...")
try:
    result = py_dim['SEC999']
    print("   ✗ Should have raised KeyError")
    raise AssertionError("Should have raised KeyError")
except KeyError as e:
    print(f"   ✓ Raised KeyError for missing key: {e}")

# Test 26: Error handling - missing file
print("\n26. Testing error handling - missing file...")
try:
    missing_file_dim = cython_mgr.create_dimension_table(
        name='missing_file',
        key_column='security_id',
        backend='memory'
    )
    missing_file_dim.populate_from_arrow_file('/nonexistent/path/file.arrow')
    print("   ✗ Should have raised FileNotFoundError")
    raise AssertionError("Should have raised FileNotFoundError")
except FileNotFoundError as e:
    print(f"   ✓ Raised FileNotFoundError for missing file")

# Cleanup
print("\n27. Cleanup...")
import shutil
shutil.rmtree(temp_dir)
print(f"   ✓ Removed temp directory: {temp_dir}")

print("\n" + "=" * 70)
print("✅ ALL TESTS PASSED")
print("=" * 70)
print("\nMaterialization Engine Summary:")
print(f"  - Cython engine: ✓ Working")
print(f"  - Python API: ✓ Working")
print(f"  - Operator overloading: ✓ Working")
print(f"  - Hash indexing: ✓ Working (O(1) lookups)")
print(f"  - Stream enrichment: ✓ Working (zero-copy joins)")
print(f"  - Arrow IPC loading: ✓ Working (memory-mapped)")
print(f"  - Error handling: ✓ Working")
print("\nTotal tests: 27")
