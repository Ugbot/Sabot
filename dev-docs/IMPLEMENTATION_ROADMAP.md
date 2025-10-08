# Sabot Implementation Roadmap

**Status**: ✅ All Plans Complete - Ready to Execute
**Date**: October 6, 2025
**Total Effort**: 326-360 hours (11-12 weeks)

---

## 🎯 Executive Summary

All 7 implementation phases completed in parallel using specialized agents. Each phase has a complete technical plan with:
- Detailed task breakdowns
- Full implementation code
- Comprehensive test suites
- Success criteria
- Risk mitigation
- Effort estimates

**Architecture Foundation**: Streaming = Batch (unified execution model)

---

## 📋 Implementation Plans Created

### ✅ Phase 1: Batch Operator API (16-20 hours)
**Plan**: [`docs/implementation/PHASE1_BATCH_OPERATOR_API.md`](./implementation/PHASE1_BATCH_OPERATOR_API.md)

**Summary**: Foundation - ensure all operators process RecordBatch → RecordBatch
- Add `__aiter__()` for async streaming
- Document batch-first contract
- Create comprehensive examples
- 80% test coverage target

**Critical**: Must complete before other phases

---

### ✅ Phase 2: Auto-Numba Compilation (22.5 hours)
**Plan**: [`docs/implementation/PHASE2_AUTO_NUMBA_COMPILATION.md`](./implementation/PHASE2_AUTO_NUMBA_COMPILATION.md)

**Summary**: Automatic JIT compilation of Python UDFs
- AST analysis for pattern detection
- @njit for loops, @vectorize for NumPy
- 10-100x speedup transparently
- LRU cache for compiled functions

**ROI**: Highest performance gain per effort

---

### ✅ Phase 3: Morsel Operators (18 hours)
**Plan**: [`docs/implementation/PHASE3_MORSEL_OPERATORS.md`](./implementation/PHASE3_MORSEL_OPERATORS.md)

**Summary**: Work-stealing parallelism within single node
- Split batches into morsels
- Parallel workers with work-stealing
- 2-4x speedup for CPU-bound ops
- NUMA-aware scheduling

**Benefit**: Single-node parallelism

---

### ✅ Phase 4: Network Shuffle (40-52 hours)
**Plan**: [`docs/implementation/PHASE4_NETWORK_SHUFFLE.md`](./implementation/PHASE4_NETWORK_SHUFFLE.md)

**Summary**: Distributed shuffle for stateful operators
- ShuffledOperator base class
- Hash partitioning by key
- Arrow Flight zero-copy transfer
- Morsel-driven pipelined shuffle

**Critical**: Required for distributed joins/aggregations

---

### ✅ Phase 5: Agent as Worker (132 hours)
**Plan**: [`docs/implementation/PHASE5_AGENT_WORKER_NODE.md`](./implementation/PHASE5_AGENT_WORKER_NODE.md)

**Summary**: Architectural shift - agents are workers, not user code
- Agent class (worker node)
- TaskExecutor (operator execution)
- @app.dataflow (returns DAG)
- JobManager deployment

**Impact**: Production-ready cluster execution

---

### ✅ Phase 6: DBOS Control Plane (54 hours)
**Plan**: [`docs/implementation/PHASE6_DBOS_CONTROL_PLANE.md`](./implementation/PHASE6_DBOS_CONTROL_PLANE.md)

**Summary**: Durable orchestration with DBOS
- Postgres-backed job state
- DBOS workflows (resumable)
- Agent health tracking
- Live rescaling
- <30s failure recovery

**Benefit**: Production-grade fault tolerance

---

### ✅ Phase 7: Plan Optimization (44 hours, Optional)
**Plan**: [`docs/implementation/PHASE7_PLAN_OPTIMIZATION.md`](./implementation/PHASE7_PLAN_OPTIMIZATION.md)

**Summary**: Query optimization inspired by DuckDB
- Filter/projection pushdown
- Join reordering
- Operator fusion
- 2-10x speedup on complex queries

**Optional**: Can defer to later

---

## 📅 Recommended Timeline

### Sprint 1: Foundation (Weeks 1-2)
- **Week 1**: Phase 1 - Batch Operator API ⭐ CRITICAL
- **Week 2**: Phase 2 - Auto-Numba Compilation

### Sprint 2: Parallelism (Weeks 3-4)
- **Week 3**: Phase 3 - Morsel Operators
- **Weeks 3-4**: Phase 4 - Network Shuffle ⭐ CRITICAL

### Sprint 3: Distribution (Weeks 5-8)
- **Weeks 5-8**: Phase 5 - Agent as Worker ⭐ CRITICAL

### Sprint 4: Control Plane (Weeks 9-10)
- **Weeks 9-10**: Phase 6 - DBOS Orchestration

### Sprint 5: Optimization (Week 11, Optional)
- **Week 11**: Phase 7 - Plan Optimizer

---

## 🔄 Critical Path

```
Phase 1 (1w) → Phase 4 (2w) → Phase 5 (4w) → Phase 6 (2w) = 9 weeks minimum
             ↓
          Phase 2 (1w) & Phase 3 (1w) can run in parallel
```

**Minimum viable**: Phases 1, 4, 5, 6 (9 weeks)
**Full implementation**: All 7 phases (11-12 weeks)

---

## 📊 Effort Distribution

| Phase | Hours | % of Total | Priority |
|-------|-------|------------|----------|
| Phase 1 | 16-20 | 5% | P0 |
| Phase 2 | 22.5 | 7% | P0 |
| Phase 3 | 18 | 5% | P1 |
| Phase 4 | 40-52 | 14% | P0 |
| Phase 5 | 132 | 38% | P0 |
| Phase 6 | 54 | 16% | P1 |
| Phase 7 | 44 | 13% | P2 |
| **Total** | **326-360** | **100%** | - |

**Team Scaling**:
- 1 developer: 11-12 weeks
- 2 developers: 6-8 weeks (parallelizing Phase 2 & 3)
- 3 developers: 4-6 weeks (parallelizing more phases)

---

## 🎯 Success Metrics

### Functional Goals
✅ Streaming and batch use identical operators
✅ Distributed execution works correctly across cluster
✅ Automatic fault tolerance with DBOS
✅ Transparent Numba compilation speeds up UDFs

### Performance Targets
✅ **10-100x** Numba speedup for Python UDFs
✅ **2-4x** morsel parallelism within node
✅ **<20%** network shuffle overhead
✅ **2-10x** optimizer gains on complex queries

### Quality Standards
✅ **85%+** test coverage across all phases
✅ **Zero** data loss in distributed mode
✅ **<30s** failure detection and recovery
✅ **100%** backward compatibility

---

## 📦 Deliverables Summary

### Documentation (3 files)
- ✅ Architecture design: `docs/design/UNIFIED_BATCH_ARCHITECTURE.md`
- ✅ Architecture decisions: `docs/design/ARCHITECTURE_DECISIONS.md`
- ✅ Implementation index: `docs/implementation/IMPLEMENTATION_INDEX.md`

### Implementation Plans (7 files)
- ✅ Phase 1: `docs/implementation/PHASE1_BATCH_OPERATOR_API.md`
- ✅ Phase 2: `docs/implementation/PHASE2_AUTO_NUMBA_COMPILATION.md`
- ✅ Phase 3: `docs/implementation/PHASE3_MORSEL_OPERATORS.md`
- ✅ Phase 4: `docs/implementation/PHASE4_NETWORK_SHUFFLE.md`
- ✅ Phase 5: `docs/implementation/PHASE5_AGENT_WORKER_NODE.md`
- ✅ Phase 6: `docs/implementation/PHASE6_DBOS_CONTROL_PLANE.md`
- ✅ Phase 7: `docs/implementation/PHASE7_PLAN_OPTIMIZATION.md`

### Code to Create (51 new files, 15 modified)
- **New Cython files**: 15
- **New Python files**: 18
- **New test files**: 25
- **New benchmark files**: 6
- **Modified files**: 15

**Total new lines**: ~12,000
**Total modified lines**: ~1,500

---

## 🚀 Getting Started

### Step 1: Review Architecture
Read the design document to understand the unified streaming/batch model:
```
docs/design/UNIFIED_BATCH_ARCHITECTURE.md
```

### Step 2: Choose Your Phase
Start with Phase 1 (foundation) or pick based on team priorities.

### Step 3: Follow the Plan
Each phase plan includes:
- Complete implementation code
- Step-by-step task breakdown
- Testing strategy
- Success criteria

### Step 4: Execute
Use the detailed specifications - every file and line count is provided.

### Step 5: Validate
Run the comprehensive test suites included in each plan.

---

## 🔑 Key Architectural Decisions

### 1. Streaming = Batch
**Decision**: Same operators for streaming and batch, only boundedness differs
**Impact**: Simplified codebase, easier testing, unified execution

### 2. Data/Control Plane Separation
**Decision**: C++/Cython for data, Python/DBOS for control
**Impact**: High performance data plane, flexible control plane

### 3. Auto-Numba UDFs
**Decision**: Transparently JIT-compile user functions
**Impact**: 10-100x speedup without user code changes

### 4. Morsel-Driven Execution
**Decision**: Work-stealing parallelism at batch and shuffle level
**Impact**: Efficient CPU utilization, NUMA-aware

### 5. Agent = Worker Node
**Decision**: Agents are cluster workers, not user programming constructs
**Impact**: Clear separation of concerns, scalable architecture

### 6. DBOS Orchestration
**Decision**: All cluster state in Postgres via DBOS workflows
**Impact**: Durable, fault-tolerant, resumable orchestration

---

## 📈 Expected Improvements

### Before Implementation
- Per-record processing overhead
- Manual parallelism management
- No distributed execution
- Fragile orchestration
- No query optimization

### After Implementation
- ✅ Batch-only processing (efficient)
- ✅ Automatic parallelism (morsels + Numba)
- ✅ Distributed shuffle (Arrow Flight)
- ✅ Durable orchestration (DBOS)
- ✅ Query optimization (DuckDB-inspired)

### Performance Gains
- **10-100x** on Python UDFs (Numba)
- **2-4x** on CPU-bound ops (morsels)
- **2-10x** on complex queries (optimizer)
- **Linear scaling** across cluster nodes

---

## ⚠️ Risks and Mitigation

### Risk: Numba compatibility
**Mitigation**: Graceful fallback to Python, detailed logging

### Risk: Network bandwidth bottleneck
**Mitigation**: Compression, adaptive batching, backpressure

### Risk: State redistribution overhead
**Mitigation**: Consistent hashing, incremental redistribution

### Risk: DBOS learning curve
**Mitigation**: Complete examples, detailed documentation

---

## 🎓 Learning Resources

### Architecture
- Main design: `docs/design/UNIFIED_BATCH_ARCHITECTURE.md`
- Decisions: `docs/design/ARCHITECTURE_DECISIONS.md`

### Implementation
- Index: `docs/implementation/IMPLEMENTATION_INDEX.md`
- Phase plans: `docs/implementation/PHASE*.md`

### Examples
- Each phase includes working examples
- Migration guides for breaking changes

---

## 🏁 Next Steps

1. **Review**: Read the architecture design document
2. **Prioritize**: Choose which phases to implement first
3. **Team**: Assign phases to developers
4. **Execute**: Follow the detailed implementation plans
5. **Test**: Run comprehensive test suites
6. **Benchmark**: Measure performance improvements
7. **Deploy**: Roll out to production incrementally

---

**All plans are complete and ready for execution! Let's build Sabot! 🚀**
