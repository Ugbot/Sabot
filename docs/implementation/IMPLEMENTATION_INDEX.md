# Sabot Implementation Plans - Master Index

**Version**: 1.0
**Date**: October 2025
**Status**: ✅ All Plans Complete - Ready for Implementation

This directory contains detailed technical implementation plans for Sabot's unified streaming/batch architecture. All 7 phases completed in parallel, production-ready with complete code examples, testing strategies, and success criteria.

---

## Quick Reference

| Phase | Plan Document | Effort | Status |
|-------|--------------|--------|--------|
| Phase 1 | [Batch Operator API](./PHASE1_BATCH_OPERATOR_API.md) | 16-20h (1w) | ✅ Ready |
| Phase 2 | [Auto-Numba Compilation](./PHASE2_AUTO_NUMBA_COMPILATION.md) | 22.5h (1w) | ✅ Ready |
| Phase 3 | [Morsel Operators](./PHASE3_MORSEL_OPERATORS.md) | 18h (1w) | ✅ Ready |
| Phase 4 | [Network Shuffle](./PHASE4_NETWORK_SHUFFLE.md) | 40-52h (2w) | ✅ Ready |
| Phase 5 | [Agent as Worker](./PHASE5_AGENT_WORKER_NODE.md) | 132h (3-4w) | ✅ Ready |
| Phase 6 | [DBOS Control Plane](./PHASE6_DBOS_CONTROL_PLANE.md) | 54h (2w) | ✅ Ready |
| Phase 7 | [Plan Optimization](./PHASE7_PLAN_OPTIMIZATION.md) | 44h (1w) | ✅ Ready |

**Total**: 326-360 hours (11-12 weeks single developer)

---

## Architecture Foundation

See: [`docs/design/UNIFIED_BATCH_ARCHITECTURE.md`](../design/UNIFIED_BATCH_ARCHITECTURE.md)

**Core Innovation**: **Streaming = Batch** (just different boundedness)
- Same operators for finite (batch) and infinite (streaming) sources
- `for batch in op` → batch mode (terminates)
- `async for batch in op` → streaming mode (runs forever)
- Data plane (C++/Cython) only sees RecordBatch objects

---

## Phase Summaries

### Phase 1: Solidify Batch-Only Operator API
**Critical foundation - must complete first**

**What**: Ensure all operators process RecordBatch → RecordBatch
**Why**: Clean contract, no per-record overhead in data plane
**How**: Add `__aiter__()` for streaming, document batch-first

**Key Changes**:
- BaseOperator supports both sync (`__iter__`) and async (`__aiter__`)
- Stream API clarifies `.records()` is sugar only
- Comprehensive examples and tests

**Impact**: Foundation for all other phases

---

### Phase 2: Auto-Numba UDF Compilation
**Performance multiplier - high ROI**

**What**: Automatically JIT-compile user Python functions
**Why**: 10-100x speedup without user code changes
**How**: AST analysis → strategy selection → Numba compilation

**Key Features**:
- Detects loops → use `@njit` (scalar JIT)
- Detects NumPy → use `@vectorize` (array JIT)
- Detects Arrow/Pandas → skip (already fast)
- LRU cache for compiled functions

**Impact**: Transparent performance boost

---

### Phase 3: Connect Operators to Morsels
**Parallelism within single node**

**What**: Operators execute via morsel-driven parallelism
**Why**: 2-4x speedup using work-stealing
**How**: Split batches → morsels → parallel workers → reassemble

**Key Features**:
- `process_morsel()` method on BaseOperator
- MorselDrivenOperator wrapper
- Integration with existing ParallelProcessor
- NUMA-aware scheduling

**Impact**: Single-node parallelism

---

### Phase 4: Integrate Network Shuffle
**Critical for distributed execution**

**What**: Stateful operators shuffle data across cluster
**Why**: Co-locate data by key for correct joins/aggregations
**How**: Hash partition → Arrow Flight → morsel-driven shuffle

**Key Features**:
- ShuffledOperator base class
- Hash partitioning strategy
- Pipelined shuffle (non-blocking)
- Zero-copy via Arrow Flight

**Impact**: Correct distributed joins/aggregations

---

### Phase 5: Agent as Worker Node
**Major architectural shift**

**What**: Redefine agent from user code to cluster worker
**Why**: Clear separation: users write DAGs, agents execute tasks
**How**: Agent class, TaskExecutor, JobManager deployment

**Key Changes**:
- Agent = worker node (executes tasks)
- Users write `@app.dataflow` (returns operator DAG)
- JobManager compiles DAG → tasks → deploys to agents
- Backward compatible (`@app.agent` deprecated but works)

**Impact**: Production-ready cluster execution

---

### Phase 6: DBOS Control Plane
**Fault tolerance and durability**

**What**: DBOS workflows for all orchestration
**Why**: Durable state, automatic recovery, live rescaling
**How**: Postgres-backed workflows, DBOS transactions

**Key Features**:
- Job state in Postgres (survives crashes)
- Multi-step workflows (resumable)
- Agent health tracking
- Live rescaling with state redistribution
- <30s failure detection

**Impact**: Production-grade reliability

---

### Phase 7: Plan Optimization (Optional)
**Query performance boost**

**What**: Optimize dataflow DAGs before execution
**Why**: 2-10x speedup on complex pipelines
**How**: Filter/projection pushdown, join reordering, fusion

**Key Optimizations**:
- Filter pushdown: 2-5x on filtered joins
- Projection pushdown: 20-40% memory reduction
- Join reordering: 10-30% on multi-joins
- Operator fusion: 5-15% on chained ops

**Impact**: Query-level optimization

---

## Implementation Timeline

### Recommended Order

**Sprint 1 (Weeks 1-2): Foundation**
1. Phase 1: Batch API (1 week) - MUST DO FIRST
2. Phase 2: Numba compilation (1 week)

**Sprint 2 (Weeks 3-4): Parallelism**
3. Phase 3: Morsel operators (1 week)
4. Phase 4: Network shuffle (2 weeks) - CRITICAL PATH

**Sprint 3 (Weeks 5-8): Distribution**
5. Phase 5: Agent as worker (3-4 weeks) - CRITICAL PATH

**Sprint 4 (Weeks 9-10): Control Plane**
6. Phase 6: DBOS orchestration (2 weeks)

**Sprint 5 (Week 11, Optional)**
7. Phase 7: Plan optimizer (1 week)

### Critical Path
```
Phase 1 → Phase 4 → Phase 5 → Phase 6
(1 week) → (2 weeks) → (4 weeks) → (2 weeks) = 9 weeks minimum
```

### Parallelization Opportunities
- Phase 2 & 3 can run concurrently after Phase 1
- Phase 7 can be skipped initially

---

## Files Created by Each Phase

### Phase 1: 5 files
- Modified: `sabot/_cython/operators/base_operator.pyx` (+50 lines)
- Modified: `sabot/api/stream.py` (+30 lines)
- New: `examples/batch_first_examples.py` (200 lines)
- New: `tests/unit/operators/test_batch_contract.py` (150 lines)
- New: `tests/unit/operators/test_async_iteration.py` (120 lines)

### Phase 2: 6 files
- New: `sabot/_cython/operators/numba_compiler.pyx` (350 lines)
- New: `sabot/_cython/operators/numba_compiler.pxd` (50 lines)
- Modified: `sabot/_cython/operators/transform.pyx` (+80 lines)
- Modified: `setup.py` (+5 lines)
- New: `tests/unit/test_numba_compilation.py` (400 lines)
- New: `benchmarks/numba_compilation_bench.py` (250 lines)

### Phase 3: 6 files
- New: `sabot/_cython/operators/base_operator.pyx` (extracted)
- New: `sabot/_cython/operators/base_operator.pxd` (50 lines)
- New: `sabot/_cython/operators/morsel_operator.pyx` (200 lines)
- New: `sabot/_cython/operators/morsel_operator.pxd` (30 lines)
- Modified: Integration with ParallelProcessor
- New: `benchmarks/morsel_operator_bench.py` (200 lines)

### Phase 4: 10 files
- New: `sabot/_cython/operators/shuffled_operator.pyx` (300 lines)
- New: `sabot/_cython/operators/shuffled_operator.pxd` (60 lines)
- Modified: `sabot/_cython/operators/joins.pyx` (+150 lines)
- Modified: `sabot/_cython/operators/aggregations.pyx` (+120 lines)
- New: `sabot/_cython/shuffle/morsel_shuffle.pyx` (400 lines)
- New: `sabot/_cython/shuffle/hash_partitioner.pyx` (200 lines)
- New: `tests/integration/test_distributed_shuffle.py` (500 lines)
- New: `tests/unit/operators/test_shuffled_operator.py` (250 lines)
- New: `benchmarks/shuffle_perf_bench.py` (300 lines)
- Modified: `setup.py` (+10 lines)

### Phase 5: 8 files
- New: `sabot/agent.py` (800 lines)
- New: `sabot/job_manager.py` (600 lines)
- Modified: `sabot/app.py` (+200 lines)
- New: `docs/AGENT_WORKER_MODEL.md` (documentation)
- New: `tests/unit/test_agent.py` (400 lines)
- New: `tests/unit/test_task_executor.py` (350 lines)
- New: `tests/integration/test_agent_deployment.py` (500 lines)
- New: `examples/agent_worker_example.py` (200 lines)

### Phase 6: 7 files
- New: `sabot/dbos_schema.sql` (database schema)
- New: `sabot/job_manager.py` (DBOS workflows, 800 lines)
- Modified: `sabot/cluster/coordinator.py` (remove custom state)
- New: `tests/unit/test_job_manager.py` (500 lines)
- New: `tests/integration/test_dbos_orchestration.py` (600 lines)
- New: `tests/integration/test_rescaling.py` (400 lines)
- New: `docs/DBOS_WORKFLOWS.md` (documentation)

### Phase 7: 9 files
- New: `sabot/compiler/plan_optimizer.py` (300 lines)
- New: `sabot/compiler/optimizations/filter_pushdown.py` (200 lines)
- New: `sabot/compiler/optimizations/projection_pushdown.py` (180 lines)
- New: `sabot/compiler/optimizations/join_reordering.py` (250 lines)
- New: `sabot/compiler/optimizations/operator_fusion.py` (200 lines)
- New: `benchmarks/optimizer_bench.py` (400 lines)
- New: `tests/unit/compiler/test_plan_optimizer.py` (500 lines)
- New: `tests/unit/compiler/test_filter_pushdown.py` (200 lines)
- Modified: `sabot/execution/job_graph.py` (+100 lines)

---

## Success Criteria Summary

### Phase 1
- ✅ All operators yield RecordBatch only
- ✅ Both sync and async iteration work
- ✅ 80% test coverage
- ✅ Zero breaking changes

### Phase 2
- ✅ 10-50x speedup for scalar loops
- ✅ 50-100x speedup for NumPy ops
- ✅ AST analysis works correctly
- ✅ Transparent to users

### Phase 3
- ✅ 2-4x speedup for CPU-bound ops
- ✅ 70%+ parallel efficiency
- ✅ No regression for small batches
- ✅ Work-stealing effective

### Phase 4
- ✅ Correct results across 2+ agents
- ✅ No data loss
- ✅ ≥50% single-agent throughput
- ✅ ≤20% shuffle overhead

### Phase 5
- ✅ Agent executes tasks
- ✅ Users write DAGs
- ✅ JobManager deploys correctly
- ✅ Backward compatible

### Phase 6
- ✅ Job submission <500ms
- ✅ Task assignment <100ms/task
- ✅ Rescaling <30s
- ✅ <30s failure detection

### Phase 7
- ✅ 2-5x filter pushdown speedup
- ✅ 20-40% memory reduction
- ✅ 10-30% multi-join speedup
- ✅ 5-15% fusion speedup

---

## Testing Coverage

**Total Test Files Created**: 25+
**Total Test Lines**: ~5,000+

**Breakdown by Type**:
- Unit tests: 15 files (~3,000 lines)
- Integration tests: 7 files (~2,500 lines)
- Benchmarks: 6 files (~1,500 lines)

**Coverage Target**: 85%+ across all phases

---

## Getting Started

### Step 1: Review Architecture
Read: [`docs/design/UNIFIED_BATCH_ARCHITECTURE.md`](../design/UNIFIED_BATCH_ARCHITECTURE.md)

### Step 2: Pick Your Phase
Start with **Phase 1** (foundation) or jump to any phase if prerequisites complete.

### Step 3: Follow the Plan
Each plan includes:
- ✅ Complete implementation code
- ✅ Testing strategy
- ✅ Success criteria
- ✅ Risk mitigation
- ✅ Effort estimates

### Step 4: Execute
Use the detailed task breakdowns - every file, every line count specified.

### Step 5: Verify
Run the test suites and benchmarks provided in each plan.

---

## Key Innovations

1. **Streaming = Batch** - Unified execution model
2. **Auto-Numba** - Transparent JIT compilation
3. **Morsel-Driven** - Work-stealing parallelism
4. **Arrow Flight Shuffle** - Zero-copy network transfer
5. **Agent = Worker** - Clean cluster architecture
6. **DBOS Workflows** - Durable orchestration
7. **DuckDB-Style Optimizer** - Query optimization

---

## Questions?

Each phase plan is self-contained with:
- Complete code examples
- Testing strategies
- Success criteria
- Risk assessment
- Timeline estimates

Start with Phase 1 and build incrementally! 🚀
