# Sabot Reality Check - What's Actually Working
**Date:** October 2, 2025
**Status:** HISTORICAL

---

## ⚠️ **HISTORICAL DOCUMENT (October 2, 2025)**

**This is a snapshot of Sabot's status on October 2, 2025.**

**For current status, see:**
- **[../roadmap/CURRENT_ROADMAP_OCT2025.md](../roadmap/CURRENT_ROADMAP_OCT2025.md)** - Current roadmap (October 8)

**What changed since this document:**
- Phases 1-4 completed (were not started on October 2)
- CLI mock removed (was identified as critical issue)
- Arrow integration completed (was 32 NotImplementedError)
- Agent runtime integration progressing (was "not integrated")
- Test coverage improved (5% → 10%, 66+ → 115+ tests)
- Functional: 20-25% → 70%

**This document is preserved for historical reference.**

---

## Quick Status

| Component | Status | Reality |
|-----------|--------|---------|
| **Build System** | ✅ Working | 31 Cython modules compile |
| **CLI** | ⚠️ Mocked | Has mock App implementation |
| **Agent Runtime** | 🟡 Partial | 657 LOC (not 22K) |
| **Checkpoint** | ✅ Complete | Cython, <10μs |
| **State (Memory)** | ✅ Complete | 1M+ ops/sec |
| **State (RocksDB)** | ✅ Complete | With transactions |
| **Watermarks** | ✅ Complete | Cython primitives |
| **Arrow Integration** | ❌ Stubbed | 32 NotImplementedError |
| **Stream API** | 🟡 Partial | Basic ops work |
| **Execution Layer** | 🟡 Designed | Not integrated |
| **Cluster** | 🟡 Designed | Not functional |
| **Test Coverage** | ❌ 5% | Dangerously low |

---

## Critical Issues Found

### 1. CLI Uses Mock App (sabot/cli.py:45-61)
```python
def create_app(id: str = "sabot", broker: str = "memory://", **kwargs):
    """Mock create_app function for CLI testing."""
    class MockApp:
        ...
```
**Impact:** `sabot -A myapp:app worker` doesn't actually run your app

---

### 2. Internal Arrow Module is Stub (sabot/arrow.py)
- **32 NotImplementedError** statements
- Falls back to pip pyarrow
- Contradicts "vendored Arrow" claim

---

### 3. Project Map Line Counts are 34-37x Inflated
- `agents/runtime.py`: Claimed 22K, actually 657 LOC
- `cluster/coordinator.py`: Claimed 39K, actually 1,076 LOC
- Pattern across entire PROJECT_MAP.md

---

### 4. Agent Runtime Not Integrated
- Structure exists (657 lines)
- Not wired to Kafka consumers
- Supervision strategies not implemented
- Health checks not running

---

### 5. Test Coverage ~5%
- Integration tests minimal
- Most modules untested
- Unknown bugs lurking

---

## What Actually Works

### ✅ Solid Foundation:
1. **Checkpoint Coordination** - Chandy-Lamport in Cython
2. **State Backends** - Memory + RocksDB complete
3. **Watermark Tracking** - Cython primitives
4. **Basic Kafka** - JSON/Avro deserialization
5. **Fraud Demo** - Measured 3K-6K txn/s

### 🟡 Partially Working:
1. **Stream API** - Basic transformations work
2. **Agent Runtime** - Designed but not integrated
3. **Execution Layer** - TaskGraph designed
4. **CLI** - Structure exists, uses mocks

### ❌ Not Working:
1. **Arrow Module** - 32 stubs
2. **CLI Worker Execution** - Mock app
3. **Multi-node Coordination** - Not functional
4. **Service Discovery** - 3 NotImplementedError
5. **Production Error Handling** - Minimal

---

## Stub Inventory (Ground Truth)

**Actual Sabot Code (excluding vendor/venv):**
- `NotImplementedError`: **84 occurrences**
- `TODO/FIXME/XXX`: **4,209 comments**
- `Mock/Stub`: **1,080 references**

**Top Offenders:**
1. `sabot/arrow.py` - 32 NotImplementedError
2. `sabot/_cython/state/state_backend.pyx` - 25 (abstract base class, expected)
3. `sabot/api/stream.py` - 7
4. `sabot/cluster/discovery.py` - 3
5. `sabot/stores/base.py` - 3

---

## Critical Path to Fix

### P0 - Blocking Basic Usage (1-2 weeks):
1. ✅ Fix arrow_core build (DONE)
2. Remove CLI mock app → load real App
3. Fix channel async/sync creation
4. Add fraud demo integration test

### P1 - Core Functionality (1-2 months):
5. Complete agent runtime integration
6. Wire execution layer to runtime
7. Increase test coverage to 30%+
8. Remove or document arrow.py stubs

### P2 - Production Hardening (3-6 months):
9. Service discovery implementation
10. Cluster coordination completion
11. Error handling and recovery
12. Test coverage to 60%+

---

## Honest Assessment

**Grade: C+ / Beta Quality**

**Strengths:**
- Excellent Cython acceleration strategy
- Working checkpoint/state/time primitives
- Well-designed architecture
- Recent refactor improved C++ integration

**Weaknesses:**
- CLI mocked
- Agent runtime incomplete
- Arrow integration stubbed (despite claims)
- Test coverage dangerously low (5%)
- Documentation oversells capabilities

**Time to Production:** 6-9 months with proper execution

---

## Recommendations

### For Users:
- ✅ Use for experimentation and learning
- ⚠️ Don't use in production (alpha quality)
- ✅ Fraud detection demo is real and works
- ⚠️ CLI worker command uses mocks

### For Contributors:
1. Start with P0 fixes
2. Add integration tests first
3. Focus on agent runtime completion
4. Update documentation to match reality

### For Documentation:
1. Fix PROJECT_MAP.md line counts
2. Mark arrow.py as stub
3. Document CLI mock limitations
4. Add "What Actually Works" section

---

## Truth in Advertising

**Sabot IS:**
- Experimental Flink-inspired framework
- Cython-accelerated core primitives
- Working state backends (Memory, RocksDB)
- Alpha software (correctly labeled)

**Sabot IS NOT (yet):**
- Production-ready
- Complete Flink replacement
- Using vendored Arrow (uses pip pyarrow)
- Fully orchestrated
- Well-tested (5% coverage)

---

**See Also:**
- `dev-docs/status/COMPLETE_STATUS_AUDIT_OCT2025.md` - Full audit
- `STUB_INVENTORY.md` - Detailed stub catalog
- `PROJECT_MAP.md` - Architecture overview (line counts inflated)

**Last Updated:** October 2, 2025
