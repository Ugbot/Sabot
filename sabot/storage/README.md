# Sabot Storage Shim Layer

## Overview

This directory contains a **zero-cost C++ abstraction layer** between Sabot and storage backends (currently MarbleDB). This shim provides:

1. **Decoupling**: Sabot code doesn't depend on MarbleDB's specific API
2. **Stability**: If MarbleDB changes, only the shim needs updating
3. **Flexibility**: Easy to swap MarbleDB for another backend (RocksDB, custom, etc.)
4. **Zero-cost**: All abstractions are inline/header-only where possible
5. **Clean C++**: Part of Sabot's move toward more C++ code

## Architecture

```
┌─────────────────┐
│   Sabot Python  │
│   (sabot/state/ │
│   sabot/stores) │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Cython Wrapper │
│ (storage_shim.pyx)│
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  C++ Shim Layer │  ◄─── YOU ARE HERE
│  (interface.h)  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   MarbleDB      │
│  (or other)     │
└─────────────────┘
```

## Files

### Interface (`interface.h`)
- **StateBackend**: Key-value operations (Get, Put, Delete, Scan, MultiGet, etc.)
- **StoreBackend**: Arrow table operations (CreateTable, InsertBatch, ScanTable, etc.)
- **Status**: Error code system (no exceptions per Sabot rules)
- **StorageConfig**: Configuration struct

### Implementation (`marbledb_backend.h/cpp`)
- **MarbleDBStateBackend**: MarbleDB implementation of StateBackend
- **MarbleDBStoreBackend**: MarbleDB implementation of StoreBackend
- All MarbleDB-specific code is isolated here

### Factory (`factory.cpp`)
- `CreateStateBackend()`: Factory function for state backends
- `CreateStoreBackend()`: Factory function for store backends
- Easy to add new backend types here

### Cython Wrapper (`storage_shim.pyx`)
- Python bindings for the C++ shim
- Clean, backend-agnostic Python API
- Handles Arrow type conversions

## Building

### Build C++ Library
```bash
cd sabot/storage
mkdir -p build && cd build
cmake ..
make
```

This creates `libsabot_storage.a` in `build/`.

### Build Cython Extensions
```bash
cd /path/to/Sabot
python build_storage_shim.py
```

## Usage

### From Python
```python
from sabot._cython.storage.storage_shim import SabotStateBackend, SabotStoreBackend

# State backend
state = SabotStateBackend("marbledb")
state.open("/path/to/state")
state.put("key", b"value")
value = state.get("key")
state.close()

# Store backend
store = SabotStoreBackend("marbledb")
store.open("/path/to/store")
store.create_table("my_table", schema)
store.insert_batch("my_table", batch)
table = store.scan_table("my_table")
store.close()
```

### From C++
```cpp
#include "sabot/storage/interface.h"

using namespace sabot::storage;

// Create backend
auto state = CreateStateBackend("marbledb");

// Configure and open
StorageConfig config;
config.path = "/path/to/state";
config.enable_bloom_filter = true;
state->Open(config);

// Use it
std::string value;
auto status = state->Get("key", &value);
if (status.ok()) {
    // Use value
}
```

## Adding a New Backend

To add a new backend (e.g., RocksDB):

1. **Create implementation files**:
   - `rocksdb_backend.h`
   - `rocksdb_backend.cpp`

2. **Implement interfaces**:
   - `class RocksDBStateBackend : public StateBackend`
   - `class RocksDBStoreBackend : public StoreBackend`

3. **Update factory**:
   ```cpp
   if (backend_type == "rocksdb") {
       return std::make_unique<RocksDBStateBackend>();
   }
   ```

4. **That's it!** Sabot code doesn't need to change.

## Design Principles

1. **Zero-cost abstraction**: Use inline functions, avoid virtual calls where possible
2. **Minimal API**: Only what Sabot needs, nothing more
3. **Clean separation**: No backend-specific types leak through
4. **Future-proof**: Easy to swap implementations
5. **Error codes**: No exceptions (per Sabot rules)

## Benefits

- **Isolation**: MarbleDB changes don't affect Sabot code
- **Testing**: Easy to mock/test with fake backends
- **Performance**: Zero-cost abstractions, direct calls
- **Maintainability**: Clear boundaries, single responsibility
- **Flexibility**: Swap backends without changing Sabot

## Status

- ✅ Interface defined (`interface.h`)
- ✅ MarbleDB implementation (`marbledb_backend.cpp`)
- ✅ Cython wrapper (`storage_shim.pyx`)
- ⏳ Build system integration
- ⏳ Update existing Cython backends to use shim
- ⏳ Tests

