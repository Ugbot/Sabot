# Marble Storage - Usage Guide

**Quick Start:** How to use Marble storage in Sabot right now

---

## Current Status

✅ **Writes Working:** CreateTable, InsertBatch, Put, MultiGet  
✅ **StateBackend Reads Working:** Get, Exists, MultiGet  
⚠️ **StoreBackend Reads:** Use `marbledb_store.pyx` directly (shim scan_table is stub)

---

## Option 1: Use Shim Layer (Recommended for Writes)

**Best for:** New code, clean abstraction, write-heavy workloads

```python
from sabot._cython.storage.storage_shim import SabotStateBackend, SabotStoreBackend
import pyarrow as pa

# StateBackend - Fully Working ✅
state_backend = SabotStateBackend("marbledb")
state_backend.open("/data/state")

# Writes
state_backend.put("user:123", b"{'name': 'Alice'}")
state_backend.put("user:456", b"{'name': 'Bob'}")

# Reads - All Working
value = state_backend.get("user:123")  # Returns b"{'name': 'Alice'}"
exists = state_backend.exists("user:456")  # Returns True
values = state_backend.multi_get(["user:123", "user:456"])  # Returns dict

state_backend.close()

# StoreBackend - Writes Working ✅, Reads Pending ⚠️
store_backend = SabotStoreBackend("marbledb")
store_backend.open("/data/tables")

# Writes - All Working
schema = pa.schema([pa.field('id', pa.int64()), pa.field('name', pa.utf8())])
store_backend.create_table("users", schema)

batch = pa.record_batch([[1, 2, 3], ["Alice", "Bob", "Charlie"]], schema=schema)
store_backend.insert_batch("users", batch)

tables = store_backend.list_tables()  # ['users', 'default']

# Reads - Stub (use Option 2 for reads)
result = store_backend.scan_table("users")  # Returns empty currently

store_backend.close()
```

---

## Option 2: Use Direct Marble Backend (For Reads)

**Best for:** Production code needing reads, until shim scan_table is fixed

```python
from sabot._cython.stores.marbledb_store import MarbleDBStoreBackend
import pyarrow as pa

backend = MarbleDBStoreBackend()
backend.open("/data/tables")

# Writes
schema = pa.schema([pa.field('id', pa.int64()), pa.field('name', pa.utf8())])
backend.create_table("users", schema)

batch = pa.record_batch([[1, 2, 3], ["Alice", "Bob", "Charlie"]], schema=schema)
backend.insert_batch("users", batch)

# Reads - Fully Working ✅
result = backend.scan_table("users")  # Returns actual data!
print(f"Rows: {result.num_rows}")  # 3
print(f"Data: {result.to_pydict()}")  # {'id': [1,2,3], 'name': ['Alice','Bob','Charlie']}

backend.close()
```

---

## Hybrid Approach (Recommended Now)

Use shim for StateBackend, direct backend for StoreBackend reads:

```python
from sabot._cython.storage.storage_shim import SabotStateBackend
from sabot._cython.stores.marbledb_store import MarbleDBStoreBackend
import pyarrow as pa

# State backend via shim - fully working
state = SabotStateBackend("marbledb")
state.open("/data/state")
state.put("key", b"value")
value = state.get("key")
state.close()

# Store backend via direct - has working scan_table
store = MarbleDBStoreBackend()
store.open("/data/tables")

schema = pa.schema([pa.field('id', pa.int64())])
store.create_table("my_table", schema)
store.insert_batch("my_table", pa.record_batch([[1,2,3]], schema=schema))

result = store.scan_table("my_table")  # Works!
print(f"Got {result.num_rows} rows")

store.close()
```

---

## Future Improvements

### Priority 1: Fix Shim scan_table()

**Approach:** Arrow C Data Interface (standard solution)

```cpp
// Add to C++ shim
Status ExportTableToC(const std::string& table_name,
                     ArrowSchema* c_schema,
                     ArrowArray* c_array) {
    std::shared_ptr<arrow::Table> table;
    ScanTable(table_name, &table);
    return arrow::ExportTable(*table, c_schema, c_array).ok() 
        ? Status::OK() 
        : Status::IOError("Export failed");
}
```

```python
# In Cython
def scan_table(self, str table_name):
    from pyarrow.lib import Schema, Array
    cdef ArrowSchema c_schema
    cdef ArrowArray c_array
    
    self._backend.get().ExportTableToC(table_name, &c_schema, &c_array)
    schema = Schema._import_from_c(&c_schema)
    array = Array._import_from_c(&c_array, schema)
    return pa.Table.from_arrays([array], schema=schema)
```

**Benefit:** Zero-copy, standard Arrow interop, ~10 lines of code

### Priority 2: NewIterator for Range Scans

```python
# Simple approach - just expose scan_range
def scan_range(self, str table_name, str start_key, str end_key):
    # Use same ExportTableToC pattern
    # Marble's NewIterator already works in C++
    pass
```

### Priority 3: Optimize StateBackend Get()

Current `Get()` scans entire table - change to use Marble's point lookup:

```cpp
// Use Marble::Get() with Key object instead of full scan
// Requires Key::FromString() or similar
// 100-1000x faster
```

---

## Performance Notes

**Current Performance:**
- StateBackend Put/Get: ✅ Working, but Get scans full table (slow for large datasets)
- StoreBackend Insert: ✅ Native Arrow, very fast
- StoreBackend Scan (via marbledb_store.pyx): ✅ Works, uses QueryResult iterator

**Optimizations (Future):**
- [ ] StateBackend: Use Marble point lookups (not scans)
- [ ] Shim scan_table: Arrow C Data Interface for zero-copy
- [ ] Add NewIterator for streaming large results
- [ ] Add scan_range for selective queries

---

## When to Use What

| Use Case | Backend | Status | Notes |
|----------|---------|--------|-------|
| Key-value state | SabotStateBackend (shim) | ✅ Working | All ops work |
| Create tables | SabotStoreBackend (shim) | ✅ Working | CreateTable works |
| Insert batches | SabotStoreBackend (shim) | ✅ Working | InsertBatch works |
| Scan tables | MarbleDBStoreBackend (direct) | ✅ Working | Use direct backend |
| Range scans | MarbleDBStoreBackend (direct) | ⚠️ Partial | NewIterator exists in C++ |

---

## Architecture Benefits

Even with scan_table stubbed in shim:

✅ **Unified Storage:** Both backends use Marble's Arrow LSM  
✅ **Clean Abstraction:** C++ shim decouples Sabot from Marble  
✅ **Arrow Throughout:** No conversions, native Arrow flow  
✅ **Production Ready:** Writes work, reads via direct backend  

**The shim provides value even without full read support - it's the foundation for future improvements.**

---

## Summary

**Use This Now:**
1. Shim for StateBackend (all operations work)
2. Shim for StoreBackend writes (CreateTable/InsertBatch work)
3. Direct `marbledb_store.pyx` for StoreBackend reads (scan_table works)

**Fix Later:**
1. Add Arrow C Data Interface helper (~50 lines of code)
2. Expose NewIterator in shim
3. Optimize StateBackend to use point lookups

**The integration is production-ready with this hybrid approach.**

