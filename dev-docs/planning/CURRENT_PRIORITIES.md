# Current Priorities - Post-Refactor Next Steps
**Date:** October 2, 2025
**Status:** OUTDATED - See updated priorities

---

## ⚠️ **THIS DOCUMENT IS OUTDATED (October 2, 2025)**

**This reflected priorities when Phases 1-4 were just starting.**

**Current Status (October 8, 2025):**
- Phases 1-4 complete (batch operators, Numba, morsels, network shuffle)
- ~70% functional (up from 20-25%)
- Test coverage: ~10% (66+ unit tests created)
- CLI and channel creation: ✅ Working

**See current priorities:**
- **[CURRENT_ROADMAP_OCT2025.md](../roadmap/CURRENT_ROADMAP_OCT2025.md)** - Q4 2025 timeline and priorities

---

## What Changed Since This Document

**Completed (were P0 in October 2):**
- ✅ Arrow core refactor (P0 #1)
- ✅ CLI mock removed (P0 #2)
- ✅ Channel creation fixed (P0 #3)
- ✅ Integration tests created (P0 #4)
- ✅ Phases 1-4 implementation (batch, Numba, morsels, shuffle)

**In Progress (were P1 in October 2):**
- 🚧 Agent runtime integration (P1 #5) - 40% complete
- 🚧 Stream API completion (P1 #6) - Basic ops working
- 🚧 Test coverage increase (P1 #8) - 5% → 10%

**Current Priorities (October 8):**
- Complete Phase 5 (agent workers) - 2 weeks
- Complete Phase 6 (DBOS control plane) - 1 week
- Phase 7 (plan optimization) - Optional

**This document is preserved for historical reference.**

---

## Immediate Priorities (P0) - Next 1-2 Weeks

### 1. ✅ Fix Arrow Core Build
**Status:** COMPLETE
- Recent refactor reduced Python object overhead
- Direct C++ buffer access working
- 31 Cython modules compiling successfully

### 2. ❌ Remove CLI Mock Implementation
**File:** `sabot/cli.py:45-61`
**Issue:** CLI uses mock App class instead of loading real apps
**Impact:** `sabot -A myapp:app worker` doesn't actually execute user code

**Current Code:**
```python
def create_app(id: str = "sabot", broker: str = "memory://", **kwargs):
    """Mock create_app function for CLI testing."""
    class MockApp:
        ...
```

**Required Fix:**
```python
def create_app(app_module: str):
    """Load real App from module spec."""
    module_name, app_name = app_module.split(':')
    module = importlib.import_module(module_name)
    return getattr(module, app_name)
```

**Effort:** 1-2 days
**Blocks:** Real worker execution

---

### 3. ❌ Fix Channel Creation Async Issue
**File:** `sabot/app.py:381, 405`
**Issue:** Cannot create Kafka/Redis channels due to async/sync mismatch

**Current:**
```python
def channel(self, name: Optional[str] = None, ...) -> ChannelT:
    # Line 381
    raise NotImplementedError("Non-memory channels require async creation...")
```

**Options:**
A. Add async method: `async def async_channel(...)`
B. Use sync wrapper with event loop detection
C. Document memory-only limitation

**Effort:** 2-3 days
**Blocks:** Kafka/Redis channel usage

---

### 4. ❌ Add Integration Test for Fraud Demo
**Missing:** Integration test that runs fraud_app.py end-to-end
**Current:** Only unit tests, no E2E validation

**Test Should:**
- Start app with real Kafka
- Send test transactions
- Verify fraud detection
- Check state consistency
- Validate checkpoint coordination

**Effort:** 3-4 days
**Blocks:** Confidence in working system

---

## High Priority (P1) - Next 1-2 Months

### 5. Agent Runtime Integration
**File:** `sabot/agents/runtime.py` (657 lines)
**Status:** Structure exists, execution incomplete

**Missing:**
- Integration with Kafka consumers
- Supervision strategy implementation
- Health check loops
- Resource monitoring active enforcement

**Effort:** 2-3 weeks
**Blocks:** Multi-agent coordination

---

### 6. Stream API Completeness
**File:** `sabot/api/stream.py`
**NotImplementedError:** 7
**Status:** Basic ops work, advanced ops stubbed

**Complete These:**
- Advanced window operations
- Complex joins
- State integration
- Event-time processing

**Effort:** 2-3 weeks
**Blocks:** User-facing feature completeness

---

### 7. Arrow Module Decision
**File:** `sabot/arrow.py`
**NotImplementedError:** 32
**Status:** Internal Arrow module is stub

**Options:**
A. Complete internal implementation (3+ weeks)
B. Remove module, use pyarrow directly (1 day)
C. Document as fallback layer

**Recommendation:** Option B or C
**Effort:** 1 day (remove) or 3 weeks (complete)
**Blocks:** Honest documentation

---

### 8. Increase Test Coverage
**Current:** ~5%
**Target:** 30%+ (short term), 60%+ (long term)

**Focus Areas:**
1. Integration tests for working features
2. Unit tests for Cython modules
3. E2E tests for example apps
4. Checkpoint/state persistence tests

**Effort:** Ongoing, 1 test per module
**Blocks:** Production confidence

---

## Medium Priority (P2) - Next 3-6 Months

### 9. Execution Layer Integration
**Files:** `sabot/execution/*.py` (1,314 lines)
**Status:** Designed but not wired up

**Complete:**
- Wire ExecutionGraph to AgentRuntime
- Implement job submission API
- Task scheduling loop
- Slot allocation enforcement

**Effort:** 3-4 weeks
**Enables:** Distributed task scheduling

---

### 10. Cluster Coordination
**Files:** `sabot/cluster/*.py` (1,951 lines)
**Status:** Classes defined, coordination not working

**Complete:**
- Leader election
- Service discovery (3 NotImplementedError)
- Health monitoring loop
- Node failure detection

**Effort:** 4-6 weeks
**Enables:** Multi-node deployments

---

### 11. Error Handling and Recovery
**Status:** Minimal error handling

**Add:**
- Graceful degradation
- Checkpoint recovery testing
- Kafka consumer error policies
- State backend failover

**Effort:** 2-3 weeks
**Enables:** Production reliability

---

### 12. Documentation Accuracy
**Issues:**
- PROJECT_MAP.md line counts 34-37x inflated
- Arrow vendoring claims incorrect
- Completion claims aspirational

**Fix:**
- Update PROJECT_MAP.md with real LOC
- Document pyarrow dependency
- Add "What Actually Works" section
- Mark experimental features

**Effort:** 1 week
**Enables:** User trust

---

## Low Priority (P3) - Future

### 13. Tonbo Backend Completion
**File:** `sabot/stores/tonbo.py`
**Status:** Experimental, minimal usage

**Decision:** Complete or remove?

---

### 14. Performance Optimization
**Areas:**
- Arrow batch processing
- Join operator optimization
- State backend tuning
- Checkpoint compression

**Timing:** After functionality complete

---

### 15. Advanced Features
- SQL/Table API
- CEP (Complex Event Processing)
- GPU acceleration (RAFT)
- Web UI for monitoring

**Timing:** Post-beta

---

## This Week's Focus

**Must Complete:**
1. ✅ Arrow core refactor validation (DONE)
2. Remove CLI mock implementation
3. Fix channel creation
4. Add fraud demo integration test

**Goal:** Have working CLI that executes real apps by end of week

---

## This Month's Focus

**Must Complete:**
1. P0 items above
2. Start agent runtime integration
3. Increase test coverage to 15%+
4. Update documentation for accuracy

**Goal:** Move from "build working" to "execution working"

---

## Success Metrics

**Week 1 (October 9):**
- [ ] CLI loads real apps
- [ ] Fraud demo runs via CLI
- [ ] Integration test passes

**Month 1 (November 2):**
- [ ] Agent runtime integrated
- [ ] Test coverage 15%+
- [ ] Documentation accurate

**Quarter 1 (January 2):**
- [ ] Stream API complete
- [ ] Test coverage 30%+
- [ ] Execution layer wired up
- [ ] Beta release ready

---

## Not Priorities Right Now

**Don't Focus On:**
- ❌ Tonbo backend (experimental)
- ❌ GPU acceleration (future)
- ❌ Web UI (polish)
- ❌ Cluster coordination (P2)
- ❌ New features

**Focus On:**
- ✅ Making existing features work
- ✅ Testing what we claim works
- ✅ Honest documentation
- ✅ Core execution path

---

## Definitions

**P0 - Critical:** Blocks basic usage, must fix now
**P1 - High:** Core functionality, needed for completeness
**P2 - Medium:** Production features, can wait
**P3 - Low:** Nice-to-have, future work

---

**Last Updated:** October 2, 2025
**Review Cadence:** Weekly
**Next Review:** October 9, 2025
