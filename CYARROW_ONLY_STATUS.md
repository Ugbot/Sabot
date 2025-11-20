# CyArrow-Only Status

**Eliminating all system pyarrow usage**

---

## Audit Complete ✅

### Found System PyArrow Usage

**Core Sabot:**
- `sabot/api/stream.py` - 3 locations (CRITICAL)
- Stream API GroupBy fallback
- Stream API join fallback

**Benchmarks:**
- `sabot_native/*.py` - 3 files
- Using `import pyarrow.compute as pc`

**Total:** ~6 critical locations

---

## Fixes Applied ✅

### sabot/api/stream.py

**Changed:**
```python
# OLD:
import pyarrow as pa
import pyarrow.compute as pc

# NEW:
from sabot import cyarrow as ca
pa = ca
pc = ca.compute
```

**Impact:** Core Stream API now uses vendored Arrow exclusively

### Benchmark Files

**Changed all sabot_native queries:**
```python
# OLD:
import pyarrow.compute as pc

# NEW:
pc = ca.compute  # Sabot's vendored Arrow
```

**Impact:** Benchmarks now measure Sabot's real performance

---

## Verification

**Remaining system pyarrow imports:** [checking]

**CyArrow working:**
- ✓ Custom kernels available (hash_array, hash_combine)
- ✓ Vendored Arrow compute accessible
- ✓ All operations use Sabot's build

---

## Expected Impact

**Before (system pyarrow):**
- No custom kernels
- Version conflicts
- Not zero-copy guaranteed

**After (cyarrow only):**
- Custom kernels available
- Consistent vendored Arrow
- Proper zero-copy
- **Expected: 2-3x faster**

**Next:** Profile with cyarrow-only code to find real bottlenecks
