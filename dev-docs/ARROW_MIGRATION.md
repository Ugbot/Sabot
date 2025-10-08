# Arrow Migration: From pip pyarrow to Vendored Arrow C++

**Date:** October 6, 2025
**Status:** ✅ Complete

## Summary

Sabot now uses a **vendored Apache Arrow C++ library** instead of pip's `pyarrow`. This gives us:

✅ **Full version control** - Pin exact Arrow version, no dependency conflicts
✅ **Optimized builds** - Custom compile flags for target hardware
✅ **No pip bloat** - Single Arrow build shared by all Cython modules
✅ **Faster installs** - Arrow built once, cached forever
✅ **Zero-copy Cython** - Direct C++ API access via `cimport pyarrow.lib`

## What Changed

### 1. Removed pip pyarrow Dependency

**Before:**
```toml
# pyproject.toml
dependencies = [
    "pyarrow>=10.0.0",  # From pip
    ...
]
```

**After:**
```toml
# pyproject.toml
dependencies = [
    # NO pyarrow from pip - we use vendored Arrow C++ via Cython bindings
    # See: vendor/arrow/ (built via build.py)
    ...
]
```

### 2. Created Arrow Build Infrastructure

**New files:**
- `build.py` - Python build script (cross-platform: Windows/Linux/macOS)
- `scripts/build_arrow.sh` - Bash build script (Unix-only, manual builds)

**Build process:**
1. User runs `pip install -e .`
2. `setup.py` checks if Arrow C++ is built
3. If not, calls `build.py` to build Arrow C++ from `vendor/arrow/cpp/`
4. Arrow installed to `vendor/arrow/cpp/build/install/`
5. Cython modules built against vendored Arrow headers

### 3. Updated Python Imports

**Before:**
```python
import pyarrow as pa
import pyarrow.compute as pc
```

**After:**
```python
from sabot import cyarrow as ca
# Compute functions via ca.compute
```

**Files updated:**
- `sabot/api/stream.py` - Now uses `ca.` and `cc.compute`
- `sabot/api/state.py` - Now uses `ca.`
- `sabot/_c/data_loader.pyx` - Now uses `ca.`
- `sabot/_cython/shuffle/*.pyx` - Added comments about vendored Arrow

### 4. Cython Bindings (No Changes Needed!)

**The magic:** Cython imports were ALREADY using vendored Arrow!

```cython
# This has ALWAYS used vendor/arrow/python/pyarrow/lib.pxd
cimport pyarrow.lib as ca

# This has ALWAYS used vendor/arrow/python/pyarrow/includes/libarrow.pxd
from pyarrow.includes.libarrow cimport CRecordBatch, CSchema
```

**How it works:**
- `setup.py` adds `vendor/arrow/python/` to Cython include path (line 303-304)
- `cimport pyarrow.lib` resolves to vendored `.pxd` files
- `.pxd` files define Cython bindings to Arrow C++ API
- No code changes needed in Cython files!

### 5. Updated Documentation

**Updated files:**
- `README.md` - Added installation prerequisites and Arrow build instructions
- `PROJECT_MAP.md` - Added vendored Arrow section
- `docs/ARROW_MIGRATION.md` - This file

## Build Instructions

### For Users

```bash
# Option A: Automatic (Arrow built during pip install)
git clone --recursive https://github.com/sabot/sabot.git
cd sabot
pip install -e .  # Builds Arrow automatically (~30-60 mins first time)

# Option B: Manual Arrow build first (recommended for development)
git clone --recursive https://github.com/sabot/sabot.git
cd sabot
python build.py          # One-time Arrow build
pip install -e .         # Fast install

# Verify
python -c "from sabot import cyarrow; print(f'Vendored Arrow: {cyarrow.USING_ZERO_COPY}')"
```

### For CI/Docker

```dockerfile
# Dockerfile
FROM python:3.11

# Install build dependencies
RUN apt-get update && apt-get install -y cmake g++

# Clone Sabot
RUN git clone --recursive https://github.com/sabot/sabot.git /app
WORKDIR /app

# Build Arrow C++ (cached in Docker layer)
RUN python build.py

# Install Sabot (fast, Arrow already built)
RUN pip install -e .
```

## Technical Details

### Arrow Build Configuration

**Enabled features** (see `build.py`):
- `ARROW_COMPUTE=ON` - Compute kernels (SIMD-accelerated)
- `ARROW_CSV=ON` - CSV reader/writer
- `ARROW_DATASET=ON` - Dataset API
- `ARROW_FILESYSTEM=ON` - Filesystem abstraction
- `ARROW_FLIGHT=ON` - gRPC-based data transport
- `ARROW_IPC=ON` - Inter-process communication
- `ARROW_JSON=ON` - JSON reader/writer
- `ARROW_PARQUET=ON` - Parquet reader/writer

**Disabled features:**
- `ARROW_S3=OFF` - AWS S3 integration (not needed)
- `ARROW_CUDA=OFF` - GPU support (separate module)
- `ARROW_GANDIVA=OFF` - LLVM expression compiler (not needed)

### Cython Include Path

`setup.py` line 303-304:
```python
# Add vendored Arrow Python bindings for Flight .pxd files
if system_libs['arrow_python_dir']:
    include_dirs.append(system_libs['arrow_python_dir'])
    # Resolves to: vendor/arrow/python/
```

This makes `cimport pyarrow.lib` resolve to `vendor/arrow/python/pyarrow/lib.pxd`.

### Python Wrapper: `sabot/cyarrow.py`

**Purpose:** Compatibility layer for Python-level code

**Provides:**
- `ca.Table`, `ca.RecordBatch`, `ca.Array`, etc. - Arrow types
- `ca.compute` - Arrow compute functions (delegates to pip pyarrow.compute for now)
- `ca.USING_ZERO_COPY` - Flag indicating vendored Arrow is active

**TODO:** Replace `pyarrow.compute` with our own Cython wrappers

## Migration Checklist

- [x] Create `build.py` (cross-platform Arrow build script)
- [x] Create `scripts/build_arrow.sh` (Unix build script)
- [x] Update `setup.py` to call `build.py` before Cython build
- [x] Remove `pyarrow>=10.0.0` from `pyproject.toml`
- [x] Update Python imports: `import pyarrow` → `from sabot import cyarrow`
- [x] Update `README.md` with build instructions
- [x] Update `PROJECT_MAP.md` with vendored Arrow info
- [x] Create this migration guide

## Testing

### Verify Vendored Arrow is Used

```python
from sabot import cyarrow
print(f"Using vendored Arrow: {cyarrow.USING_ZERO_COPY}")
print(f"Using external pyarrow: {cyarrow.USING_EXTERNAL}")

# Should print:
# Using vendored Arrow: True
# Using external pyarrow: False
```

### Test Zero-Copy Operations

```python
from sabot.cyarrow import compute_window_ids, hash_join_batches
import pyarrow as pa

# Create test batch
batch = pa.RecordBatch.from_pydict({
    'timestamp': [1000, 2000, 3000],
    'value': [10, 20, 30]
})

# Test window computation (uses vendored Arrow C++)
windowed = compute_window_ids(batch, 'timestamp', 1000)
print(windowed.column_names)  # ['timestamp', 'value', 'window_id']
```

## Performance Impact

**Build time:**
- First install: +30-60 minutes (Arrow C++ build)
- Subsequent installs: No change (Arrow cached)

**Runtime performance:**
- No change - same Arrow C++ code
- Potentially faster if vendored build has better optimizations

**Binary size:**
- Arrow C++ libraries: ~150MB (installed to `vendor/arrow/cpp/build/install/lib`)
- No change to Python package size

## Troubleshooting

### Arrow build fails

```bash
# Check CMake version
cmake --version  # Need 3.16+

# Check C++ compiler
c++ --version    # Need GCC 7+ or Clang 5+

# Manual build with verbose output
python build.py
```

### "pyarrow not found" at runtime

```python
# Check what Arrow implementation is being used
from sabot import cyarrow
print(f"USING_ZERO_COPY: {cyarrow.USING_ZERO_COPY}")
print(f"USING_EXTERNAL: {cyarrow.USING_EXTERNAL}")

# If USING_EXTERNAL=True, Arrow C++ wasn't built properly
# Solution: rm -rf vendor/arrow/cpp/build && python build.py
```

### Import errors in Cython modules

```python
# Error: "cannot import name 'CRecordBatch' from 'pyarrow.includes.libarrow'"

# Cause: Cython module can't find vendored Arrow headers
# Solution: Rebuild Cython extensions
pip install -e . --force-reinstall --no-deps
```

## Future Work

- [ ] Create pre-built Arrow wheels for common platforms
- [ ] Add Arrow build caching to CI
- [ ] Implement `cyarrow.compute` in Cython (remove pyarrow.compute dependency)
- [ ] Add `cyarrow.csv`, `cyarrow.parquet` wrappers
- [ ] Document Arrow C++ build options for custom builds
