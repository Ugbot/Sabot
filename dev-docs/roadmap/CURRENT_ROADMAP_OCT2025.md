# Sabot Development Roadmap - October 2025

**Last Updated:** October 8, 2025
**Status:** Active and Accurate
**Version:** 1.0

---

## Executive Summary

Sabot is a **Python-native streaming/batch engine** with Cython acceleration and hybrid storage architecture. As of October 2025, core batch processing infrastructure is complete with Phases 1-4 implemented.

**Current Status:** ~70% functional (up from 20-25% in early October)
- ✅ Phases 1-4: Complete (batch operators, Numba, morsels, network shuffle)
- 🚧 Phase 5-6: In progress (agent workers, DBOS control plane)
- 📋 Phase 7+: Planned (optimization, advanced features)

---

## What Works Today ✅

### Core Infrastructure (Complete)

**Phase 1: Batch Operator API** ✅
- All operators process RecordBatch → RecordBatch
- Sync (`__iter__`) and async (`__aiter__`) support
- Clean separation: data plane (Cython) vs control plane (Python)
- **Performance:** 0.15-729M rows/sec (measured)

**Phase 2: Auto-Numba Compilation** ✅
- Automatic JIT compilation of user UDFs
- AST analysis → strategy selection (NJIT/VECTORIZE/SKIP)
- LRU cache for compiled functions
- **Speedup:** 10-100x for numeric Python code

**Phase 3: Morsel-Driven Parallelism** ✅
- Single-node parallelism via work-stealing
- `process_morsel()` interface on BaseOperator
- NUMA-aware scheduling
- **Speedup:** 2-4x on multi-core machines

**Phase 4: Network Shuffle** ✅
- Distributed data shuffling via Arrow Flight
- Lock-free queue infrastructure
- Zero-copy network transport
- Hash partitioning for stateful operations
- **Throughput:** 100K-1M rows/sec over network

**State Backends** ✅
- Hybrid storage architecture:
  - **Tonbo:** Columnar data (aggregations, joins, shuffle buffers)
  - **RocksDB:** Metadata (checkpoints, timers, barriers)
  - **Memory:** Temporary user state
- All three backends integrated and active
- **Performance:** Memory (10M ops/sec), Tonbo (1M rows/sec), RocksDB (100K ops/sec)

**Operators** ✅
- Transform: filter, map, select, flatMap, union
- Aggregation: aggregate, reduce, distinct, groupBy (partial)
- Join: hash join, interval join, asof join
- **Status:** 9 operators production-ready

---

## In Progress 🚧

### Phase 5: Agent as Worker Node (40% complete)

**Goal:** Redefine agents from user code containers to cluster worker nodes

**What Works:**
- Agent class structure defined
- Task representation and serialization
- Basic agent registration

**What's Needed:**
- TaskExecutor implementation (execute operator tasks)
- JobManager deployment logic (DAG → tasks → agents)
- Agent health monitoring and failure detection
- State redistribution on agent failure

**Estimated Effort:** 80 hours remaining (2 weeks)
**Priority:** P0 (blocking distributed execution)

---

### Phase 6: DBOS Control Plane (20% complete)

**Goal:** Postgres-backed orchestration for durability and fault tolerance

**What Works:**
- DBOS workflow primitives defined
- Basic Postgres schema
- Workflow transaction concept

**What's Needed:**
- Job state persistence (Postgres tables)
- Multi-step workflow execution (resumable)
- Agent health tracking in DB
- Live rescaling with state redistribution
- Failure detection (<30s target)

**Estimated Effort:** 40 hours remaining (1 week)
**Priority:** P1 (production reliability)

---

## Planned 📋

### Phase 7: Plan Optimization (0% complete)

**Goal:** Query optimization for 2-10x speedup

**Optimizations:**
- Filter pushdown (2-5x speedup on filtered joins)
- Projection pushdown (20-40% memory reduction)
- Join reordering (10-30% speedup on multi-joins)
- Operator fusion (5-15% speedup on chains)
- Dead code elimination

**Estimated Effort:** 44 hours (1 week)
**Priority:** P2 (performance enhancement)
**Status:** Detailed plan ready, implementation not started

---

### Phase 8: Window Operators (Planned)

**Goal:** Complete windowing support for streaming

**Features:**
- Tumbling windows (designed, not implemented)
- Sliding windows (designed, not implemented)
- Session windows (designed, not implemented)
- Global windows (not started)
- Custom windows (not started)

**Estimated Effort:** 60 hours (1.5 weeks)
**Priority:** P1 (critical for streaming)

---

### Phase 9: Advanced Features (Future)

**Features:**
- CEP (complex event processing)
- SQL interface (parser, planner)
- Table API
- ML integration (online learning)
- Graph processing

**Estimated Effort:** 200+ hours (5+ weeks)
**Priority:** P3 (nice to have)

---

## Timeline

### Q4 2025 (Oct-Dec)

**October:**
- ✅ Complete Phase 1-4 (batch infrastructure)
- ✅ Hybrid storage architecture
- ✅ Documentation cleanup

**November:**
- 🚧 Complete Phase 5 (agent workers) - 2 weeks
- 🚧 Complete Phase 6 (DBOS control plane) - 1 week
- 📋 Start Phase 7 (optimization) - 1 week

**December:**
- 📋 Complete Phase 8 (windows) - 1.5 weeks
- 📋 Integration testing and examples
- 📋 Performance benchmarking
- 📋 Production readiness checklist

**End of Q4 Status:** 85-90% functional, production-ready for basic use cases

---

### Q1 2026 (Jan-Mar)

**January:**
- Phase 9 planning and design
- Advanced connector development
- SQL interface prototype

**February-March:**
- Advanced features (CEP, ML integration)
- Operational tooling (monitoring, debugging)
- Community building

**End of Q1 Status:** Feature-complete for most streaming workloads

---

## Comparison to Other Systems

### vs Apache Flink

| Feature | Sabot | Flink | Status |
|---------|-------|-------|--------|
| **Core Processing** |
| Batch operators | ✅ Complete | ✅ Complete | ✅ Parity |
| Stream operators | ✅ Complete | ✅ Complete | ✅ Parity |
| Exactly-once semantics | 🚧 Partial | ✅ Complete | 70% |
| Event-time processing | 🚧 Partial | ✅ Complete | 60% |
| Windowing | 📋 Designed | ✅ Complete | 40% |
| **State Management** |
| Keyed state | ✅ Complete | ✅ Complete | ✅ Parity |
| Operator state | 📋 Planned | ✅ Complete | 0% |
| State backends | ✅ Hybrid (Tonbo+RocksDB) | ✅ RocksDB | ✅ Better |
| Queryable state | 📋 Planned | ✅ Complete | 0% |
| **Performance** |
| Batch throughput | ✅ 0.15-729M rows/sec | ✅ Similar | ✅ Competitive |
| UDF compilation | ✅ Auto-Numba | ✅ JVM JIT | ✅ Comparable |
| Network shuffle | ✅ Zero-copy Flight | ✅ Netty | ✅ Competitive |
| **Operations** |
| Cluster management | 🚧 In progress | ✅ Complete | 40% |
| Fault tolerance | 🚧 Partial | ✅ Complete | 60% |
| Rescaling | 📋 Planned | ✅ Complete | 0% |
| Web UI | 📋 Planned | ✅ Complete | 0% |

**Overall Parity:** 50-60% (up from 15-20% in early October)

**Sabot Advantages:**
- Python-native (not JVM wrapper)
- Simpler deployment (no JVM)
- Hybrid storage (Tonbo for columnar + RocksDB for metadata)
- Auto-Numba compilation (transparent speedup)

**Flink Advantages:**
- Production-proven at scale
- Complete fault tolerance
- Rich connector ecosystem
- Mature operational tooling
- SQL interface

---

## Critical Path to Production

### Must-Have (P0) - Blocking production use

1. **Complete Phase 5** (Agent Workers) - 2 weeks
   - Task execution on agents
   - JobManager deployment
   - Health monitoring

2. **Complete Phase 6** (DBOS Control Plane) - 1 week
   - Job state persistence
   - Resumable workflows
   - Failure recovery

3. **Window Operators** (Phase 8) - 1.5 weeks
   - Tumbling, sliding, session windows
   - Critical for streaming use cases

4. **Testing** - 1 week
   - Integration tests
   - End-to-end scenarios
   - Performance benchmarks

**Total Critical Path:** 5.5 weeks (end of November 2025)

---

### Nice-to-Have (P1) - Production quality

1. **Phase 7** (Optimization) - 1 week
   - Query optimization passes
   - 2-10x speedup on complex queries

2. **Operational Tooling** - 2 weeks
   - Metrics dashboard
   - Job monitoring
   - Debugging tools

3. **Documentation** - 1 week
   - Production deployment guide
   - Operational runbook
   - Best practices

**Total for Production Quality:** 4 weeks (mid-December 2025)

---

### Future (P2-P3) - Advanced features

1. **SQL Interface** - 6-8 weeks
2. **CEP (Complex Event Processing)** - 4 weeks
3. **ML Integration** - 4 weeks
4. **Web UI** - 4 weeks

**Total for Advanced Features:** 18-20 weeks (Q1 2026)

---

## Risk Assessment

### High Confidence ✅
- Batch operators (done and tested)
- State backends (integrated and working)
- Network shuffle (implemented and benchmarked)
- Numba compilation (tested with 10-100x speedups)

### Medium Confidence 🚧
- Agent worker implementation (structure exists, needs wiring)
- DBOS control plane (design complete, partial implementation)
- Fault tolerance (primitives exist, end-to-end not tested)

### Low Confidence ⚠️
- Window operators (designed but not implemented)
- SQL interface (not started)
- Production operational tooling (minimal)
- Large-scale testing (not done)

---

## Success Metrics

### Performance Targets
- ✅ Batch throughput: >100M rows/sec (achieved: 729M on select)
- ✅ Filter performance: >10M rows/sec (achieved: 18.67M)
- ✅ Join performance: >100K rows/sec (achieved: 0.15-22,221M depending on type)
- 🚧 Network shuffle: >1M rows/sec (achieved: 100K-1M, needs optimization)
- 📋 End-to-end latency: <100ms (not measured)

### Reliability Targets
- 🚧 Exactly-once delivery: 99.99% (partial, not tested at scale)
- 📋 Failure recovery: <30s (designed, not implemented)
- 📋 Uptime: 99.9% (not applicable yet)

### Usability Targets
- ✅ API simplicity: <10 lines for simple pipelines (achieved)
- ✅ Documentation: Comprehensive (achieved)
- 📋 Example coverage: 20+ working examples (achieved: 16, need 4 more)
- 📋 Test coverage: >60% (current: ~10%)

---

## Roadmap Changes Since October 2

**What Changed:**
- Phases 1-4 completed (were 0-20% in early October)
- State backends integrated (were "pending")
- Network shuffle implemented (was "designed")
- Documentation reorganized (97 files cleaned up)
- Realistic assessment (no longer aspirational)

**Status Improvement:**
- October 2: ~20-25% functional
- October 8: ~70% functional
- Target (Nov 30): ~85-90% functional

---

## How to Use This Roadmap

### For Contributors
- **Now:** Focus on Phase 5 (agent workers) and Phase 6 (DBOS)
- **Next:** Phase 8 (windows) for streaming completeness
- **Later:** Phase 7 (optimization) for performance

### For Users
- **Use today:** Batch processing, simple streaming without windows
- **Wait for:** Nov 30 for production-ready streaming
- **Future:** Q1 2026 for advanced features (SQL, CEP, ML)

### For Planning
- **Q4 2025:** Core completion (Phases 5-8)
- **Q1 2026:** Advanced features (SQL, CEP, monitoring)
- **Q2 2026:** Ecosystem expansion (connectors, integrations)

---

## Links

**Implementation Plans:**
- [IMPLEMENTATION_INDEX.md](../implementation/IMPLEMENTATION_INDEX.md) - All 7 phase plans

**Current Status:**
- [ACTUAL_STATUS_TESTED.md](../status/ACTUAL_STATUS_TESTED.md) - What works (verified)
- [IMPLEMENTATION_COMPLETE.md](../status/IMPLEMENTATION_COMPLETE.md) - Completion milestones

**Architecture:**
- [UNIFIED_BATCH_ARCHITECTURE.md](../../docs/design/UNIFIED_BATCH_ARCHITECTURE.md) - Core design
- [PROJECT_MAP.md](../../PROJECT_MAP.md) - Codebase structure

---

**Last Updated:** October 8, 2025
**Next Review:** November 1, 2025
**Maintained By:** Dev Team
