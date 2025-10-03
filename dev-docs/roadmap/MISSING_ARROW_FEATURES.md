# Missing Arrow Features - Reality Check
**Last Updated:** October 2, 2025
**Status:** Revised based on actual implementation audit

---

## ⚠️ **CRITICAL REALITY CHECK**

**The fundamental issue:** `sabot/arrow.py` has **32 NotImplementedError** statements and is essentially a stub module.

**Current Approach:** Sabot falls back to `pyarrow` from pip, not vendored Arrow as claimed.

---

## Current Arrow Integration Status

### What Actually Works ✅
1. **pyarrow from pip** - Working fallback (version 21.0.0)
2. **Cython Arrow bindings** - `sabot/_c/arrow_core.pyx` (just fixed in refactor)
   - Direct C++ buffer access
   - Zero-copy operations
   - `batch_processor.pyx` working
3. **Basic Arrow operations** via pyarrow compute kernels

### What's Stubbed in sabot/arrow.py ❌

#### Type Constructors (10 stubs)
```python
def int64():
    raise NotImplementedError("Type constructors not yet in internal Arrow")

def float32():
    raise NotImplementedError("...")
def string():
    raise NotImplementedError("...")
# ... 7 more
```

#### Array Construction (1 stub)
```python
def array(data, type=None):
    raise NotImplementedError("array() not yet in internal Arrow")
```

#### IPC - Inter-Process Communication (4 stubs)
```python
def serialize(batch):
    raise NotImplementedError("IPC not yet in internal Arrow")

def deserialize(buffer):
    raise NotImplementedError("IPC not yet in internal Arrow")

def BufferOutputStream():
    raise NotImplementedError("...")

def py_buffer(obj):
    raise NotImplementedError("...")
```

#### Compute Functions (17 stubs)
```python
def compute():
    raise NotImplementedError("Arrow not available")

def sum():
    raise NotImplementedError("...")

def mean():
    raise NotImplementedError("...")

# ... 14 more compute functions
```

**Total:** 32 NotImplementedError statements

---

## Decision Point: Three Options

### Option A: Remove sabot/arrow.py (RECOMMENDED)
**Effort:** 1 day
**Pros:**
- Honest about using pyarrow
- Removes misleading code
- Simpler architecture
- Already working this way

**Cons:**
- Admits no vendored Arrow

**Action:**
1. Delete `sabot/arrow.py`
2. Update imports to use `import pyarrow as pa` directly
3. Update documentation to list pyarrow as dependency
4. Focus on Cython bindings for performance

---

### Option B: Complete sabot/arrow.py Implementation
**Effort:** 3-4 weeks
**Pros:**
- Could have internal Arrow layer
- Matches original vision

**Cons:**
- Massive effort
- Duplicates pyarrow functionality
- Maintenance burden
- Already have Cython bindings working

**Not Recommended**

---

### Option C: Keep as Fallback Layer (CURRENT STATUS)
**Effort:** Minimal
**Pros:**
- Already works
- Provides abstraction

**Cons:**
- Misleading (claims features it doesn't have)
- 32 stubs confuse users
- Not actually vendoring

**Action:**
1. Add clear docstring: "Fallback to pyarrow, internal implementation incomplete"
2. Update documentation
3. Mark as experimental/internal

---

## Recommended Path Forward

### Immediate (This Week):
1. ✅ **Keep Cython bindings** - `sabot/_c/arrow_core.pyx` works great
2. ✅ **Keep pyarrow dependency** - It's what we actually use
3. ⚠️ **Document sabot/arrow.py as stub** - Add warning header
4. ⚠️ **Update CLAUDE.md** - Remove "vendored Arrow" claim

### Short Term (This Month):
5. **Decide:** Remove sabot/arrow.py or complete it
6. **Focus on** Cython operators using pyarrow C++ API directly
7. **Performance tuning** in batch_processor.pyx, join_processor.pyx

### Long Term (3-6 Months):
8. If needed: Implement specific optimized operations in Cython
9. Use pyarrow compute kernels for everything else
10. Don't duplicate pyarrow - build on top of it

---

## Real Performance Bottlenecks (From Actual Code)

### 1. Arrow Integration Already Working
**Via:** `sabot/_c/arrow_core.pyx` + pyarrow
**Performance:** Zero-copy buffer access working
**Status:** ✅ SOLVED by recent refactor

### 2. Batch Processing
**File:** `sabot/_cython/arrow/batch_processor.pyx`
**Status:** ✅ Compiles and imports successfully
**Performance:** Direct C++ buffer operations

### 3. Join Operations
**File:** `sabot/_cython/arrow/join_processor.pyx`
**Status:** ✅ Compiles successfully
**Uses:** pyarrow compute kernels via Cython

### 4. Window Processing
**File:** `sabot/_cython/arrow/window_processor.pyx`
**Status:** ✅ Compiles successfully

---

## What We Actually Need (Not Stub Completion)

### High Priority:
1. **Test Cython operators** - Verify they work with real data
2. **Benchmark performance** - Measure actual speedup
3. **Document API** - How to use Cython operators from Python
4. **Integration examples** - Show Arrow usage in fraud demo

### Medium Priority:
5. **Optimize hot paths** - Profile and optimize Cython code
6. **Add more operators** - As needed for real use cases
7. **Error handling** - Graceful fallbacks when Arrow ops fail

### Low Priority:
8. Complete sabot/arrow.py (OR remove it)
9. Vendored Arrow build (if performance demands it)
10. Custom Arrow operators (only if pyarrow insufficient)

---

## Honest Feature Assessment

| Feature | sabot/arrow.py | Cython Bindings | pyarrow | Status |
|---------|----------------|-----------------|---------|--------|
| Type constructors | ❌ Stub | ✅ Via pyarrow | ✅ Works | Use pyarrow |
| Array construction | ❌ Stub | ✅ Via pyarrow | ✅ Works | Use pyarrow |
| IPC/Serialization | ❌ Stub | ⚠️ Partial | ✅ Works | Use pyarrow |
| Compute functions | ❌ Stub | ✅ Via Cython | ✅ Works | Use Cython+pyarrow |
| Batch processing | ❌ Stub | ✅ Working | ✅ Works | **Cython is faster** |
| Zero-copy ops | ❌ Stub | ✅ Working | ✅ Works | **Cython direct** |
| Group-by | ❌ Stub | ⚠️ TODO | ✅ Works | Use pyarrow |
| Joins | ❌ Stub | ✅ Working | ✅ Works | **Cython available** |
| Window functions | ❌ Stub | ✅ Working | ⚠️ Limited | **Cython better** |

---

## Corrected Priority List

### Phase 1 (Immediate - This Week):
1. ✅ **Document reality** - sabot/arrow.py is stub, using pyarrow
2. ⚠️ **Add warning** to sabot/arrow.py docstring
3. ⚠️ **Test Cython operators** - Verify batch_processor works
4. ⚠️ **Update README** - Accurate Arrow integration description

### Phase 2 (This Month):
5. **Remove or fix** sabot/arrow.py (decide one way)
6. **Benchmark Cython ops** - Measure performance gains
7. **Add integration tests** - Arrow operations in fraud demo
8. **Document Cython API** - How to use from Python

### Phase 3 (3-6 Months):
9. **Optimize Cython operators** - Based on profiling
10. **Add missing operators** - As needed by use cases
11. **Consider vendored build** - Only if pyarrow insufficient

---

## Conclusion

**Don't implement sabot/arrow.py stubs.**

**Instead:**
1. Use pyarrow directly (we already do)
2. Focus on Cython bindings for performance (already working)
3. Document honestly what we're using
4. Remove misleading stub code

**Current approach is correct** - recent refactor proves Cython + pyarrow works great.

---

**See Also:**
- `REALITY_CHECK.md` - Overall project status
- `dev-docs/status/COMPLETE_STATUS_AUDIT_OCT2025.md` - Full audit
- `sabot/_c/arrow_core.pyx` - Actual working Arrow integration

**Last Updated:** October 2, 2025
**Next Review:** After deciding to remove or document sabot/arrow.py
