# Implementation Plans

This directory contains detailed implementation plans for Sabot's unified batch-centric architecture.

## Overview

The implementation follows the design specified in [UNIFIED_BATCH_ARCHITECTURE.md](../design/UNIFIED_BATCH_ARCHITECTURE.md), which unifies streaming and batch processing around a single operator model.

## Implementation Phases

### Phase 1: Solidify Batch-Only Operator API
**Status**: Planned
**Duration**: 1 week

**Goal**: Clarify that everything is batches, remove per-record confusion

**Key Tasks**:
- Update BaseOperator documentation
- Update all operator implementations
- Update Stream API
- Create batch-first examples

### Phase 2: Auto-Numba UDF Compilation ✅
**Status**: Planned (detailed plan available)
**Duration**: 1 week
**Plan**: [PHASE2_AUTO_NUMBA_COMPILATION.md](./PHASE2_AUTO_NUMBA_COMPILATION.md)

**Goal**: Automatically JIT-compile user functions for 10-100x speedup

**Key Tasks**:
- Create NumbaCompiler core with AST analysis
- Integrate with MapOperator
- Implement compilation caching
- Create comprehensive test suite
- Performance benchmarks

**Key Files**:
- NEW: `sabot/_cython/operators/numba_compiler.pyx`
- MODIFY: `sabot/_cython/operators/transform.pyx`
- NEW: `tests/unit/test_numba_compilation.py`
- NEW: `benchmarks/numba_compilation_bench.py`

### Phase 3: Connect Operators → Morsels
**Status**: Planned
**Duration**: 2 weeks

**Goal**: Operators execute via morsel-driven parallelism

**Key Tasks**:
- Add morsel interface to BaseOperator
- Create MorselDrivenOperator wrapper
- Benchmark morsel execution
- Update documentation

### Phase 4: Integrate Network Shuffle
**Status**: Partially Complete
**Duration**: 2 weeks
**Related**: [LOCK_FREE_SHUFFLE_SUMMARY.md](./LOCK_FREE_SHUFFLE_SUMMARY.md), [DAG_SHUFFLE_INTEGRATION.md](./DAG_SHUFFLE_INTEGRATION.md)

**Goal**: Stateful operators automatically shuffle data

**Key Tasks**:
- Create ShuffledOperator base class
- Update stateful operators (joins, aggregations)
- Implement morsel-driven shuffle
- End-to-end shuffle tests

### Phase 5: Agent as Worker Node
**Status**: Planned (detailed plan available)
**Duration**: 1 week
**Plan**: [PHASE5_AGENT_WORKER_NODE.md](./PHASE5_AGENT_WORKER_NODE.md)

**Goal**: Redefine agent as worker, not user code

**Key Tasks**:
- Create Agent class
- Create TaskExecutor
- Deprecate @app.agent decorator
- Update documentation

### Phase 6: DBOS Control Plane
**Status**: Planned (detailed plan available)
**Duration**: 2 weeks
**Plan**: [PHASE6_DBOS_CONTROL_PLANE.md](./PHASE6_DBOS_CONTROL_PLANE.md)

**Goal**: All orchestration via DBOS workflows

**Key Tasks**:
- Create JobManager with DBOS
- Database schema
- Agent health tracking
- Rescaling workflow

### Phase 7: Plan Optimization (Optional)
**Status**: Planned
**Duration**: 1 week

**Goal**: Borrow DuckDB optimization techniques

**Key Tasks**:
- Create PlanOptimizer
- Filter/projection pushdown
- Join reordering
- Benchmark optimizations

## Completed Work

### Shuffle Infrastructure ✅
- **Status**: Complete
- **Documentation**:
  - [LOCK_FREE_SHUFFLE_SUMMARY.md](./LOCK_FREE_SHUFFLE_SUMMARY.md) - Lock-free transport layer
  - [DAG_SHUFFLE_INTEGRATION.md](./DAG_SHUFFLE_INTEGRATION.md) - DAG integration
  - [SHUFFLE_IMPLEMENTATION.md](./SHUFFLE_IMPLEMENTATION.md) - Implementation details

- **Key Components**:
  - Lock-free Arrow Flight transport
  - Atomic partition storage
  - MPSC/SPSC queues
  - Zero-copy network transfer

### State Backend Integration ✅
- **Status**: Complete
- **Documentation**:
  - [TONBO_SABOT_INTEGRATION_COMPLETE.md](./TONBO_SABOT_INTEGRATION_COMPLETE.md) - Tonbo FFI integration
  - [TONBO_FFI_INTEGRATION_SUMMARY.md](./TONBO_FFI_INTEGRATION_SUMMARY.md) - FFI summary
  - [MATERIALIZATION_SUMMARY.md](./MATERIALIZATION_SUMMARY.md) - Materialization engine

- **Key Components**:
  - Tonbo state backend (Rust FFI)
  - Materialization engine
  - Streaming checkpointing

### Arrow Integration ✅
- **Status**: Partial
- **Documentation**:
  - [ARROW_INTEGRATION_STATUS.md](./ARROW_INTEGRATION_STATUS.md) - Integration status

- **Key Components**:
  - Zero-copy batch processing
  - Arrow compute kernels
  - Columnar transformations

## Implementation Priorities

### P0 - Critical (Blocking Production)
1. **Phase 2**: Auto-Numba compilation (performance)
2. **Phase 5**: Agent as worker node (architecture clarity)
3. **Phase 6**: DBOS control plane (fault tolerance)

### P1 - High Priority
1. **Phase 3**: Morsel-driven parallelism (scalability)
2. **Phase 4**: Network shuffle completion (distributed operations)
3. Test coverage expansion (60%+ coverage)

### P2 - Medium Priority
1. **Phase 7**: Plan optimization (performance)
2. Documentation improvements
3. Example applications

## Getting Started

1. **Read the architecture**: [UNIFIED_BATCH_ARCHITECTURE.md](../design/UNIFIED_BATCH_ARCHITECTURE.md)
2. **Choose a phase**: Pick from the phases above
3. **Follow the plan**: Each phase has a detailed implementation plan
4. **Update this README**: Keep status current

## File Organization

```
docs/
├── design/                           # Architecture and design specs
│   ├── UNIFIED_BATCH_ARCHITECTURE.md # Core architecture (read this first!)
│   └── ARCHITECTURE_DECISIONS.md     # Key decisions
│
├── implementation/                   # Implementation plans (this directory)
│   ├── README.md                     # This file
│   │
│   ├── PHASE2_AUTO_NUMBA_COMPILATION.md  # Phase 2 plan ✅
│   ├── PHASE5_AGENT_WORKER_NODE.md       # Phase 5 plan ✅
│   ├── PHASE6_DBOS_CONTROL_PLANE.md      # Phase 6 plan ✅
│   │
│   ├── LOCK_FREE_SHUFFLE_SUMMARY.md      # Shuffle implementation ✅
│   ├── DAG_SHUFFLE_INTEGRATION.md        # DAG integration ✅
│   ├── SHUFFLE_IMPLEMENTATION.md         # Shuffle details ✅
│   │
│   ├── TONBO_SABOT_INTEGRATION_COMPLETE.md  # Tonbo integration ✅
│   ├── TONBO_FFI_INTEGRATION_SUMMARY.md     # FFI summary ✅
│   ├── MATERIALIZATION_SUMMARY.md           # Materialization ✅
│   │
│   └── ARROW_INTEGRATION_STATUS.md       # Arrow status ✅
│
└── ... (other docs)
```

## Contributing

When implementing a phase:

1. **Create feature branch**: `git checkout -b phase-2-numba-compilation`
2. **Follow the plan**: Use the detailed task breakdown
3. **Write tests first**: TDD approach preferred
4. **Update docs**: Keep implementation plans current
5. **Benchmark**: Verify performance targets
6. **Create PR**: Link to implementation plan

## Questions?

- **Architecture questions**: See [UNIFIED_BATCH_ARCHITECTURE.md](../design/UNIFIED_BATCH_ARCHITECTURE.md)
- **Implementation details**: Check phase-specific plans in this directory
- **Status updates**: Check PROJECT_MAP.md at repository root
