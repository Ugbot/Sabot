# Marble Integration Status

**Date:** November 12, 2025  
**Status:** ✅ Core Integration Complete, Read Path Pending

---

## Summary

Sabot now uses Marble's Arrow-native LSM storage for both key-value state and table storage through a clean C++ shim layer.

### ✅ What Works

**StateBackend (Key-Value)**
- ✅ Put(key, value) - Writes Arrow RecordBatch `{key: utf8, value: binary}`
- ✅ Get(key) - Scans table and filters by key
- ✅ MultiGet([keys]) - Batch lookups
- ✅ Delete(key) - Marks as deleted
- ✅ Flush() - Persists to disk
- ✅ Exists(key) - Checks presence
- ✅ DeleteRange(start, end) - Range deletion

**StoreBackend (Tables)**
- ✅ CreateTable(name, schema) - Creates Marble column family
- ✅ InsertBatch(table, batch) - Native Arrow insert
- ✅ ListTables() - Enumerates column families
- ✅ Flush() - Persists to disk

**Architecture**
- ✅ Both backends use `marble::MarbleDB` (not LSMTree directly)
- ✅ Arrow types throughout: Python → Cython → C++ → Marble
- ✅ C++ shim provides stable abstraction
- ✅ LSMTree internal to Marble (correct architecture)

### ⚠️ Pending Implementation

**1. scan_table() - Returns Empty Stub**

**The Issue:**
Cython cannot convert `vector<shared_ptr<RecordBatch>>` from C++ to Python list.
Error: `Cannot convert 'shared_ptr[RecordBatch]' to Python object`

**Why This Happens:**
- `marbledb_store.pyx` works because it builds the list through `QueryResult::Next(&batch_ptr)` iterator calls
- Each `Next()` populates a fresh `batch_ptr`, which Cython can append to a Python list  
- Our `ScanTableBatches()` returns a `vector<>`, and accessing `vector[i]` returns a reference
- Cython cannot convert the reference to a Python object for list storage

**The Real Solution:**

Use **Arrow C Data Interface** - designed for zero-copy C++ ↔ Python conversion:

```cpp
// In C++ (marbledb_backend.cpp) - add helper method
Status MarbleDBStoreBackend::ExportTableToC(const std::string& table_name,
                                            void* c_schema_out,
                                            void* c_array_out) {
    std::shared_ptr<arrow::Table> table;
    auto status = ScanTable(table_name, &table);
    if (!status.ok()) return status;
    
    // Export using Arrow C Data Interface
    auto export_status = arrow::ExportTable(*table, 
                                            (ArrowSchema*)c_schema_out,
                                            (ArrowArray*)c_array_out);
    if (!export_status.ok()) {
        return Status::IOError(export_status.ToString());
    }
    return Status::OK();
}
```

```python
# In Cython (storage_shim.pyx)
def scan_table(self, str table_name):
    import pyarrow as pa
    from pyarrow import Array, Schema
    
    # Allocate C structures
    cdef:
        ArrowSchema c_schema
        ArrowArray c_array
    
    # Export from C++
    status = self._backend.get().ExportTableToC(table_name, &c_schema, &c_array)
    if not status.ok():
        raise RuntimeError(f"Export failed: {status.message}")
    
    # Import to PyArrow (zero-copy)
    schema = Schema._import_from_c(&c_schema)
    array = Array._import_from_c(&c_array, schema)
    
    return pa.Table.from_arrays([array], schema=schema)
```

**Alternative Solution:**

Use `GetBatchAt()` pattern (already implemented in C++):

```python
# Get count
cdef size_t count
self._backend.get().GetBatchCount(table_name, &count)

# Get each batch individually and convert
# The challenge is still RecordBatch.wrap() - needs investigation
```

**2. NewIterator() - Not Exposed**

**Status:** C++ implementation exists, Cython wrapper had type issues

**The Solution:**

Declare Iterator in `.pxd` file instead of inline:

```python
# In storage_shim.pxd - add Iterator declaration
cdef extern from "sabot/storage/interface.h" namespace "sabot::storage::StoreBackend":
    cdef cppclass Iterator:
        cbool Valid()
        void Next()
        Status GetBatch(shared_ptr[RecordBatch]*)

# In storage_shim.pyx - use it
def new_iterator(self, str table_name, str start_key="", str end_key=""):
    cdef unique_ptr[Iterator] cpp_iter
    status = self._backend.get().NewIterator(table_name, start_key, end_key, &cpp_iter)
    
    # Return Python wrapper
    # Challenge: Same type conversion issue for GetBatch()
    pass
```

---

## Test Results

### StateBackend Test ✅

```
✓ Created backend
✓ Opened backend
✓ Put(test_key, b"test_value")
✓ Get(test_key) = b'test_value'
✓ MultiGet(['key1', 'key2', 'key3']) = {... 3 values}
✓ Flush()
✓ Closed backend
```

### StoreBackend Test ✅

```
✓ Created backend
✓ Opened backend
✓ CreateTable(test_table)
✓ InsertBatch(3 rows)
✓ ListTables() = ['test_table', 'default']
✓ Closed backend
```

---

## Architecture

```
┌─────────────────────────────────────────┐
│  Python Application                      │
│  - StateBackend.put(k, v)               │
│  - StoreBackend.insert_batch(t, batch)  │
└───────────────┬─────────────────────────┘
                ↓
┌─────────────────────────────────────────┐
│  Cython Wrapper                          │
│  sabot/_cython/storage/storage_shim.pyx │
│  - SabotStateBackend                     │
│  - SabotStoreBackend                     │
└───────────────┬─────────────────────────┘
                ↓
┌─────────────────────────────────────────┐
│  C++ Shim Layer                          │
│  sabot/storage/marbledb_backend.cpp     │
│  - MarbleDBStateBackend                  │
│    • Uses column family: __sabot_state__|
│    • Schema: {key: utf8, value: binary} │
│  - MarbleDBStoreBackend                  │
│    • User-defined column families       │
│    • User-defined Arrow schemas         │
└───────────────┬─────────────────────────┘
                ↓
┌─────────────────────────────────────────┐
│  Marble::MarbleDB                        │
│  - Column families (= tables)            │
│  - Arrow RecordBatch operations          │
│  - LSM-of-Arrow storage                  │
│    • LSMTree internal (not exposed)      │
│    • Arrow-native throughout             │
└──────────────────────────────────────────┘
```

---

## Built Artifacts

```
sabot/storage/build/libsabot_storage.a (585 KB)
  ├─ interface.h - Abstract backend interface
  ├─ marbledb_backend.cpp - Marble implementation
  └─ factory.cpp - Backend creation

sabot/_cython/storage/storage_shim.cpython-313-darwin.so (2.0 MB)
  ├─ SabotStateBackend - Key-value operations
  └─ SabotStoreBackend - Table operations
```

---

## How to Use

### StateBackend Example

```python
from sabot._cython.storage.storage_shim import SabotStateBackend

backend = SabotStateBackend("marbledb")
backend.open("/data/state")

# Write
backend.put("user:123", b"{'name': 'Alice'}")

# Read
value = backend.get("user:123")

# Batch read
values = backend.multi_get(["user:123", "user:456"])

backend.close()
```

### StoreBackend Example

```python
from sabot._cython.storage.storage_shim import SabotStoreBackend
import pyarrow as pa

backend = SabotStoreBackend("marbledb")
backend.open("/data/tables")

# Create table
schema = pa.schema([
    pa.field("id", pa.int64()),
    pa.field("name", pa.utf8())
])
backend.create_table("users", schema)

# Insert data
batch = pa.record_batch([[1, 2, 3], ["A", "B", "C"]], schema=schema)
backend.insert_batch("users", batch)

# List tables
tables = backend.list_tables()  # ['users', 'default']

backend.close()
```

---

## Next Steps

### Priority 1: Fix scan_table()

**Recommended Approach:** Arrow C Data Interface

1. Add `ExportTableToC()` helper in C++ shim
2. Use `ArrowSchema` and `ArrowArray` structs
3. Import to PyArrow using `_import_from_c()`
4. Zero-copy, standard approach

**Files to modify:**
- `sabot/storage/interface.h` - Add ExportTableToC declaration
- `sabot/storage/marbledb_backend.cpp` - Implement Export helper
- `sabot/_cython/storage/storage_shim.pyx` - Use C Data Interface

### Priority 2: Implement NewIterator

**Approach:** Declare Iterator in `.pxd`, wrap in Cython class

1. Move Iterator declaration to `storage_shim.pxd`
2. Create `SabotStoreIterator` Python class
3. Implement `valid()`, `next()`, `get_batch()` methods
4. Handle batch conversion (same challenge as scan_table)

**Alternative:** Use scan_range() which is simpler

### Priority 3: Optimize StateBackend Operations

Current `Get()` scans entire table - should use Marble's key-based lookup:

1. Use Marble's `Get()` with Key object (not full scan)
2. Requires creating `Key` from string (via KeyFactory or schema)
3. 100-1000x faster than current scan-and-filter

---

## Conclusion

✅ **Core integration complete** - both backends use Marble's Arrow-native API  
✅ **Write operations working** - CreateTable, InsertBatch, Put, Delete  
⚠️ **Read operations need Arrow C Data Interface** - standard solution exists  
✅ **Architecture correct** - LSMTree internal, MarbleDB exposed via shim

**The shim layer successfully abstracts Marble and provides a clean, stable API for Sabot.**

