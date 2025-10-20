# Next Steps: C++ Optimization Roadmap

**Current Status**: ✅ Phase 1 Complete - Foundation Built  
**Next Phase**: DuckDB Integration & Spark API

---

## Immediate Next Steps (This Week)

### 1. Copy Join Order Optimizer from DuckDB
**Files to copy** (9 files from `vendor/duckdb/src/optimizer/join_order/`):
- join_order_optimizer.cpp
- cardinality_estimator.cpp
- cost_model.cpp
- query_graph.cpp
- join_node.cpp
- plan_enumerator.cpp
- query_graph_manager.cpp
- relation_manager.cpp
- relation_statistics_helper.cpp

**Adaptation**: Add streaming semantics, preserve watermarks

**Expected**: DP-based join ordering (10-50x better than greedy)

---

### 2. Copy Expression Rewrite Rules (Top 10)
**Files to copy** (from `vendor/duckdb/src/optimizer/rule/`):
- constant_folding.cpp
- arithmetic_simplification.cpp  
- comparison_simplification.cpp
- conjunction_simplification.cpp (AND/OR optimization)
- distributivity.cpp
- in_clause_simplification_rule.cpp
- case_simplification.cpp
- like_optimizations.cpp
- move_constants.cpp
- date_part_simplification.cpp

**Expected**: 10+ expression optimizations

---

### 3. Integrate C++ Optimizer with Python
**Tasks**:
- Create Python → C++ logical plan converter
- Create C++ → Python logical plan converter
- Wire up in sabot/compiler/plan_optimizer.py
- Benchmark vs pure Python version

**Expected**: 10-100x faster query compilation

---

## Phase 2: Weeks 2-3

### Copy More DuckDB Optimizations
- Limit pushdown (limit_pushdown.cpp)
- TopN optimizer (topn_optimizer.cpp)
- Common subexpression elimination (cse_optimizer.cpp)
- Statistics propagation (statistics_propagator.cpp)
- Filter combiner (filter_combiner.cpp)

**Total**: +15 more optimizations

---

### Benchmark Suite
- TPC-H queries (compare vs DuckDB)
- Fintech workloads (joins, aggregations)
- Streaming queries (infinite sources)
- Memory profiling

---

## Phase 3: Weeks 4-5 - Spark Integration

### Move Spark DataFrame to C++
- `sabot/spark/dataframe.py` → C++
- `sabot/spark/functions.py` → C++ registry
- Cython wrappers for Python API

**Expected**: 10-20x faster DataFrame operations

---

## Phase 4: Weeks 6-10 - Distributed

### C++ Operator Registry
- Perfect hashing
- <10ns lookups
- Operator caching

### C++ Shuffle Coordination
- Sub-microsecond coordination
- Better partition assignment
- Integration with lock-free transport

### C++ Job Scheduler
- Lock-free queues
- Sub-microsecond scheduling
- Better resource management

---

## Quick Reference

### Current Files
- C++ library: `sabot_core/build/src/query/libsabot_query.a` (341 KB)
- Cython modules: `sabot/_cython/query/`, `sabot/_cython/arrow/`
- Tests: `benchmarks/cpp_optimization_benchmark.py`

### Key Commands
```bash
# Rebuild C++
cd sabot_core/build && make sabot_query

# Rebuild Cython
cd /Users/bengamble/Sabot
python setup_query_optimizer.py build_ext --inplace

# Run tests
python benchmarks/cpp_optimization_benchmark.py

# Run full build
python build.py
```

### Priority Files to Eliminate .to_numpy() From
1. `sabot/_cython/operators/*.pyx` - Hot paths
2. `sabot/api/stream.py` - User API
3. `sabot/spark/*.py` - Spark compatibility
4. `sabot/_cython/graph/compiler/*.py` - Graph queries

**Target**: Eliminate 60-80% (240-320 of 385 calls)

---

## Resources

### DuckDB Reference Code
- Optimizer: `vendor/duckdb/src/optimizer/`
- Join order: `vendor/duckdb/src/optimizer/join_order/`
- Rules: `vendor/duckdb/src/optimizer/rule/`
- Pushdown: `vendor/duckdb/src/optimizer/pushdown/`

### Documentation
- Filter pushdown: `sabot_core/include/sabot/query/filter_pushdown.h`
- Projection pushdown: `sabot_core/include/sabot/query/projection_pushdown.h`
- OptimizerType: `sabot_core/include/sabot/query/optimizer_type.h`

---

## Success Metrics

### ✅ Achieved
- C++ library compiled (341 KB)
- 3/3 Cython modules working
- 20 optimizations available
- Zero-copy helpers functional
- Memory pool integrated
- All tests passing

### ⏳ Next Milestones
- 10-100x faster query optimization (needs integration)
- 40+ DuckDB optimizations (needs copying)
- Spark DataFrame in C++ (needs porting)
- Full benchmark suite (needs creation)

---

**Ready to continue with DuckDB integration!** 🚀
