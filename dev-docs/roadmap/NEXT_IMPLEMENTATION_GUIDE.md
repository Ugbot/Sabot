# Next Implementation Guide - Reality-Based
**Last Updated:** October 2, 2025
**Status:** OUTDATED - See updated roadmap

---

## ⚠️ **THIS DOCUMENT IS OUTDATED (October 2, 2025)**

**This reflected early October status with blockers identified.**

**Current Status (October 8, 2025):**
- Many blockers from this document now resolved
- Phases 1-4 complete (were 0-20% in this doc)
- ~70% functional (was 20-25% in this doc)

**See current implementation guide:**
- **[CURRENT_ROADMAP_OCT2025.md](CURRENT_ROADMAP_OCT2025.md)** - Updated roadmap with Q4 2025 timeline
- **[../implementation/IMPLEMENTATION_INDEX.md](../implementation/IMPLEMENTATION_INDEX.md)** - Phase-by-phase plans

---

## What Changed Since This Document

**Completed (were blockers in October 2):**
- ✅ Batch operator API (Phase 1)
- ✅ Auto-Numba compilation (Phase 2)
- ✅ Morsel-driven parallelism (Phase 3)
- ✅ Network shuffle (Phase 4)
- ✅ State backends integrated (Tonbo + RocksDB)

**Still needed (from October 8 status):**
- 🚧 Agent workers (Phase 5) - 40% complete
- 🚧 DBOS control plane (Phase 6) - 20% complete
- 📋 Window operators (Phase 8)

**This document is preserved for historical reference.**

---

## 📋 **Actual Current State**

### What Works (Verified) ✅
- **Cython Build System:** 31 modules compile successfully
- **Checkpoint Coordination:** Chandy-Lamport in Cython (<10μs)
- **State Backends:** Memory (complete), RocksDB (complete)
- **Watermark Primitives:** WatermarkTracker in Cython
- **Basic Kafka:** JSON/Avro deserialization working
- **Fraud Demo:** 3K-6K txn/s measured

### What's Broken/Missing ❌
- **CLI:** Uses mock App, doesn't load real applications
- **Agent Runtime:** 657 lines of structure, no execution
- **Arrow Module:** 32 NotImplementedError statements
- **Stream API:** 7 NotImplementedError, untested
- **Execution Layer:** Designed but not wired up
- **Cluster Coordination:** Not functional
- **Test Coverage:** ~5%

---

## 🎯 **Realistic 3-Month Plan**

### **MONTH 1: Fix Critical Blockers**

#### Week 1: CLI & Agent Runtime Bootstrap
**Goal:** Make CLI actually work

**Tasks:**
1. **Remove CLI Mock** (`sabot/cli.py:45-61`)
   ```python
   # Replace mock App with real loader
   def load_app_from_spec(app_spec: str) -> App:
       module_name, app_name = app_spec.split(':')
       module = importlib.import_module(module_name)
       return getattr(module, app_name)
   ```
   **Effort:** 1-2 days
   **Files:** `sabot/cli.py`
   **Test:** `sabot -A examples.fraud_app:app worker` should run

2. **Fix Channel Creation** (`sabot/app.py:381`)
   ```python
   # Add sync wrapper for async channel creation
   def channel(self, ...):
       if backend == "memory":
           return MemoryChannel(...)
       else:
           # Use asyncio.run() or event loop detection
           return self._create_channel_sync(...)
   ```
   **Effort:** 2-3 days
   **Files:** `sabot/app.py`
   **Test:** Can create Kafka channels

3. **Wire Agent Runtime to Kafka**
   - Connect `AgentProcess` to `KafkaSource`
   - Implement actual consumer loop
   - Handle message deserialization

   **Effort:** 3-4 days
   **Files:** `sabot/agents/runtime.py`, `sabot/kafka/source.py`
   **Test:** Agent receives Kafka messages

**Milestone:** Fraud demo runs via CLI end-to-end

---

#### Week 2: Integration Testing
**Goal:** Validate what we claim works

**Tasks:**
1. **Fraud Demo Integration Test**
   ```python
   # tests/integration/test_fraud_demo.py
   @pytest.mark.integration
   async def test_fraud_detection_end_to_end():
       # Start app
       # Send 1000 transactions
       # Verify fraud alerts
       # Check state consistency
   ```
   **Effort:** 2-3 days
   **Coverage:** Kafka → Agent → State → Output

2. **Checkpoint Recovery Test**
   ```python
   @pytest.mark.integration
   async def test_checkpoint_recovery():
       # Process 100 events
       # Create checkpoint at 50
       # Simulate failure at 75
       # Recover from checkpoint
       # Verify no duplicates/loss
   ```
   **Effort:** 2-3 days
   **Validates:** Exactly-once claims

3. **State Backend Tests**
   - Memory backend CRUD operations
   - RocksDB backend persistence
   - State recovery after restart

   **Effort:** 2-3 days
   **Files:** `tests/integration/test_state_backends.py`

**Milestone:** Integration tests passing, confidence in working features

---

#### Week 3-4: Documentation Cleanup
**Goal:** Documentation matches reality

**Tasks:**
1. **Update README.md**
   - Fix "60K LOC" claim
   - Document pyarrow dependency (not vendored)
   - Mark experimental features
   - Honest "What Works" section

   **Effort:** 1 day

2. **Fix PROJECT_MAP.md**
   - Correct line counts (currently 34-37x inflated)
   - Update module statuses
   - Document stubs clearly

   **Effort:** 1 day

3. **Add Warning to sabot/arrow.py**
   ```python
   """
   ⚠️ INTERNAL STUB MODULE - DO NOT USE DIRECTLY

   This module is a fallback layer that delegates to pyarrow.
   Contains 32 NotImplementedError statements.
   Use pyarrow directly or Cython bindings in sabot/_c/
   """
   ```
   **Effort:** 1 hour

4. **Update CLAUDE.md**
   - Remove "vendored Arrow" claim
   - Document pyarrow as dependency
   - Fix incorrect assertions

   **Effort:** 1 hour

**Milestone:** Documentation is honest and accurate

---

### **MONTH 2: Core Functionality**

#### Week 5-6: Agent Runtime Completion
**Goal:** Multi-agent coordination working

**Tasks:**
1. **Process Management**
   - Spawn agent processes
   - Monitor health
   - Restart failed agents

   **Effort:** 1 week
   **Files:** `sabot/agents/runtime.py`

2. **Supervision Strategies**
   - ONE_FOR_ONE
   - ONE_FOR_ALL
   - REST_FOR_ONE

   **Effort:** 3-4 days
   **Files:** `sabot/agents/supervisor.py`

3. **Resource Monitoring**
   - CPU/memory tracking
   - Backpressure detection
   - Resource limits enforcement

   **Effort:** 2-3 days
   **Files:** `sabot/agents/resources.py`

**Milestone:** Multi-agent fraud demo with supervision

---

#### Week 7-8: Stream API Completion
**Goal:** User-facing API feature-complete

**Tasks:**
1. **Fix Stream API Stubs** (7 NotImplementedError)
   - Complete window operations
   - Fix join implementations
   - Wire state integration

   **Effort:** 1 week
   **Files:** `sabot/api/stream.py`

2. **Test Stream API**
   - Unit tests for each operator
   - Integration tests for pipelines
   - Performance benchmarks

   **Effort:** 3-4 days

**Milestone:** Stream API documented and tested

---

### **MONTH 3: Production Hardening**

#### Week 9-10: Error Handling & Recovery
**Goal:** Graceful degradation and recovery

**Tasks:**
1. **Error Handling**
   - Kafka consumer error policies
   - State backend failover
   - Checkpoint failure recovery
   - Dead letter queues

   **Effort:** 1 week

2. **Recovery Testing**
   - Network failures
   - State backend crashes
   - Kafka broker failures
   - Agent crashes

   **Effort:** 1 week

**Milestone:** Can recover from common failure modes

---

#### Week 11-12: Performance & Testing
**Goal:** Increase test coverage and performance

**Tasks:**
1. **Test Coverage to 30%**
   - Unit tests for core modules
   - Integration tests for features
   - E2E tests for examples

   **Effort:** 1.5 weeks

2. **Performance Benchmarks**
   - Throughput measurements
   - Latency profiling
   - Memory usage tracking
   - Optimize hot paths

   **Effort:** 3-4 days

**Milestone:** Beta release ready

---

## 📊 **Success Metrics**

### Week 1:
- [ ] CLI loads real apps
- [ ] Fraud demo runs via CLI
- [ ] No mock implementations in critical path

### Week 4:
- [ ] 3 integration tests passing
- [ ] Documentation accurate
- [ ] Arrow module decision made

### Week 8:
- [ ] Agent runtime functional
- [ ] Stream API complete
- [ ] Test coverage 15%+

### Week 12:
- [ ] Error handling complete
- [ ] Test coverage 30%+
- [ ] Performance benchmarks published
- [ ] Beta release

---

## 🚫 **What NOT to Do**

### Don't:
1. ❌ **Add new features** - Fix what exists first
2. ❌ **Complete sabot/arrow.py** - Remove or document as stub
3. ❌ **Build execution layer** - Agent runtime first
4. ❌ **Add SQL interface** - Way premature
5. ❌ **Implement cluster coordination** - Single-node first
6. ❌ **Build web UI** - Core functionality first
7. ❌ **Optimize performance** - Get it working first

### Do:
1. ✅ **Fix CLI mock**
2. ✅ **Test what exists**
3. ✅ **Document honestly**
4. ✅ **Complete agent runtime**
5. ✅ **Integration tests**
6. ✅ **Error handling**

---

## 📝 **Immediate Action Items (This Week)**

### Day 1-2: CLI Fix
**File:** `sabot/cli.py`
**Change:** Remove mock App, load from module spec
**Test:** `sabot -A examples.fraud_app:app worker`

### Day 3-4: Channel Fix
**File:** `sabot/app.py`
**Change:** Fix async/sync channel creation
**Test:** Create Kafka channel successfully

### Day 5: Integration Test
**File:** `tests/integration/test_fraud_e2e.py`
**Create:** End-to-end fraud demo test
**Verify:** Messages flow through entire pipeline

---

## 🎯 **Long-Term Vision (Beyond 3 Months)**

### Month 4-6: Production Features
- Savepoints
- Incremental checkpoints
- Metrics & monitoring
- K8s deployment

### Month 6-9: Advanced Streaming
- Event-time processing integration
- Watermark propagation
- Complex event processing
- More connectors

### Month 9-12: Differentiation
- Python-native features
- Better debugging
- Tighter Arrow integration
- Performance optimization

---

## 📈 **Progress Tracking**

Update this section weekly:

**Week of October 7, 2025:**
- [ ] CLI mock removed
- [ ] Channel creation fixed
- [ ] First integration test passing
- [ ] Documentation audit complete

**Week of October 14, 2025:**
- [ ] Agent runtime wired to Kafka
- [ ] 3 integration tests passing
- [ ] Test coverage 10%+

**Week of October 21, 2025:**
- [ ] Agent process management working
- [ ] Supervision strategies implemented
- [ ] Test coverage 15%+

---

## 🔍 **Decision Points**

### Decision 1: sabot/arrow.py
**Options:**
- A. Remove entirely (1 day)
- B. Document as stub (1 hour)
- C. Complete implementation (3-4 weeks)

**Recommendation:** Option A or B
**Deadline:** Week 1

### Decision 2: Execution Layer
**Options:**
- A. Build full execution graph (6-8 weeks)
- B. Simple agent scheduling (2-3 weeks)
- C. Defer until agent runtime works

**Recommendation:** Option C
**Deadline:** Month 2

### Decision 3: Tonbo Backend
**Options:**
- A. Complete implementation (2-3 weeks)
- B. Mark experimental, low priority
- C. Remove from codebase

**Recommendation:** Option B
**Deadline:** Month 1

---

## ✅ **Acceptance Criteria for Beta Release**

### Must Have:
1. ✅ CLI loads and runs real apps
2. ✅ Agent runtime executes Kafka consumers
3. ✅ State backends work (Memory + RocksDB)
4. ✅ Checkpoint creation succeeds
5. ✅ Fraud demo runs end-to-end
6. ✅ Test coverage 30%+
7. ✅ Documentation accurate
8. ✅ Error handling for common failures

### Nice to Have:
9. ⚠️ Checkpoint recovery tested
10. ⚠️ Multi-agent coordination
11. ⚠️ Performance benchmarks
12. ⚠️ Supervision strategies

### Not Required:
13. ❌ Execution layer
14. ❌ Cluster coordination
15. ❌ Web UI
16. ❌ SQL interface

---

## 📚 **Resources**

**Current Status:**
- `REALITY_CHECK.md` - What actually works
- `CURRENT_PRIORITIES.md` - Priority rankings
- `dev-docs/status/COMPLETE_STATUS_AUDIT_OCT2025.md` - Full audit

**Next Steps:**
- This file (NEXT_IMPLEMENTATION_GUIDE.md)
- `FLINK_PARITY_ROADMAP.md` - Long-term vision
- `MISSING_ARROW_FEATURES.md` - Arrow decisions

---

## 🎓 **Lessons Learned**

1. **Don't oversell** - Documentation claimed 88% complete, actually ~20%
2. **Test early** - 5% coverage hid major issues
3. **Avoid stubs** - 84 NotImplementedError confused users
4. **Honest LOC** - PROJECT_MAP inflated 34-37x
5. **Focus** - Too many features started, few finished

**Apply:** Build less, test more, document honestly

---

**Last Updated:** October 2, 2025
**Next Review:** Weekly, every Monday
**Responsible:** Project lead
**Status:** Active roadmap
