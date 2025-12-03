# Sabot Distributed SQL Execution System - Component Analysis

## Executive Summary

The Sabot distributed SQL system is **partially complete** with strong operator foundations but significant gaps in the execution pipeline. The architecture is well-designed but lacks critical implementation layers:

- **Strong**: Foundation operators (filter, join, groupby) are Cython-optimized and production-ready
- **Weak**: Execution coordination, operator dispatch, and multi-input handling are incomplete
- **Missing**: Sort, Distinct, Union, Window operators; proper join state management

---

## System Architecture

```
┌─────────────────────────────────────────────────┐
│ SQL Query (DuckDB Parser)                       │
└─────────────────────────┬───────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────┐
│ PlanBridge (plan_bridge.py)                     │
│ - Parse SQL with DuckDB EXPLAIN                 │
│ - Extract logical plan structure                │
│ - Identify operators and dependencies           │
└─────────────────────────┬───────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────┐
│ DistributedQueryPlan                            │
│ - ExecutionStage (pipeline of operators)        │
│ - ShuffleSpec (data transfer between stages)    │
│ - ExecutionWaves (dependency ordering)          │
└─────────────────────────┬───────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────┐
│ StageScheduler (stage_scheduler.py)             │
│ - Execute stages in waves                       │
│ - Call operator_builder to build operators      │
│ - Coordinate shuffles between stages            │
└─────────────────────────┬───────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────┐
│ Operator Builder (_default_operator_builder)    │
│ - Build Filter, Aggregate, Join, Projection     │
│ - Use Cython implementations when available     │
│ - Fall back to Python/Arrow compute             │
└─────────────────────────┬───────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────┐
│ Cython Operators (sabot/_cython/operators/)     │
│ - Filter, Hash Join, GroupBy, Map               │
│ - Transform, Streaming operators                │
│ - Morsel-driven parallel execution              │
└─────────────────────────────────────────────────┘
```

---

## Fully Implemented Components

### 1. **Cython Operators** (sabot/_cython/operators/) - PRODUCTION READY

**Foundation Operators** (all built and tested):
- ✅ `filter_operator.pyx` (CythonFilterOperator)
  - Arrow compute-based filtering
  - Supports >, <, >=, <=, =, !=, <>
  - AND/OR expression support
  - Built: filter_operator.cpython-311-darwin.so (149K)

- ✅ `joins.pyx` (CythonHashJoinOperator, CythonIntervalJoinOperator, CythonAsofJoinOperator)
  - Zero-copy streaming hash join
  - SpillableBuffer support for bigger-than-memory
  - Interval join with RocksDB timers
  - As-of join for time-series
  - Built: joins.cpython-313-darwin.so (365K)

- ✅ `aggregations.pyx` (CythonGroupByOperator, CythonReduceOperator, CythonAggregateOperator, CythonDistinctOperator)
  - SIMD-accelerated groupby with Arrow hash_aggregate
  - Multi-key groupby support
  - Partial aggregate merging
  - Built: aggregations.cpython-311-darwin.so (257K)

- ✅ `transform.pyx` (CythonMapOperator, CythonFilterOperator)
  - Zero-copy projection
  - Arrow compute expression evaluation
  - Numba auto-compilation support
  - Built: transform.cpython-311-darwin.so (272K)

**Supporting Operators**:
- ✅ `base_operator.pyx` (BaseOperator) - Abstract base
- ✅ `shuffled_operator.pyx` (ShuffledOperator) - Network shuffle support
- ✅ `morsel_operator.pyx` (MorselDrivenOperator) - Parallel execution wrapper
- ✅ `sql_operator.pyx` (CythonSQLOperator) - SQL-specific operators
- ✅ `streaming_groupby.pyx` (StreamingGroupByOperator) - Bigger-than-memory groupby

**Hash Join Components**:
- ✅ `hash_join_memory.pyx` (HashJoinMemoryPool) - Pre-allocated memory pools
- ✅ `hash_join_streaming.pyx` - Streaming hash join implementation

---

## Partially Implemented Components

### 1. **PlanBridge** (sabot/sql/plan_bridge.py)

**Status**: 80% - Core structure present, plan extraction incomplete

**What Works**:
- Table registration with DuckDB
- Parallelism configuration
- Return DistributedQueryPlan-compatible dictionaries

**What's Missing**:
- Lines 97-523: `_extract_plan_info()` - extracts EXPLAIN output but incompletely
- Lines 137-250: `_partition_into_stages()` - identifies shuffle boundaries
- Lines 252-290: `_build_execution_waves()` - determines execution order
- Lines 292-310: `_get_output_schema()` - derives result schema

**Issues**:
```python
# Line 100 - incomplete
explain_result = self._conn.execute(f"EXPLAIN {sql}").fetchall()
# Then what? EXPLAIN output parsing is missing
```

### 2. **StageScheduler** (sabot/sql/stage_scheduler.py)

**Status**: 70% - Core structure present, operator execution incomplete

**What Works**:
- Wave-based execution with proper dependencies
- Async task dispatch
- Shuffle coordination infrastructure
- Source data retrieval (line 250-280)
- Task failure handling

**What's Missing/Incomplete**:
```python
async def _execute_single_operator(operator: Any, input_data: ca.Table) -> ca.Table:
    """Execute a single operator."""
    # PROBLEM: Only handles operators that are callables or have execute methods
    # Does NOT handle multi-input operators (joins require LEFT and RIGHT tables)
    # Lines 331-350 show it just calls operator(input_data) - won't work for joins!
```

**Critical Issue - Multi-Input Operators**:
- Lines 309-329: `_execute_operator_pipeline()` processes operators sequentially
- **Problem**: Joins require TWO inputs (left table + right table)
- **Current**: Only passes single input_data table through pipeline
- **Should**: Have special handling for "JoinOperator" type with multiple inputs

### 3. **SQLController** (sabot/sql/controller.py)

**Status**: 50% - Many paths incomplete

**What Works**:
- Table registration
- Query execution modes (local, local_parallel, distributed)
- Async execution framework
- Plan explanation structure

**What's Missing**:
```python
async def _create_execution_plan(self, sql: str) -> SQLExecutionPlan:
    """
    Create execution plan for SQL query
    
    This will eventually call the C++ SQL engine via Cython to:
    1. Parse SQL with DuckDB
    2. Optimize with DuckDB
    3. Translate to Sabot operators
    4. Identify stage boundaries
    """
    # PLACEHOLDER - Returns hardcoded plan, never actually parses SQL!
    # Lines 254-283
```

**Other Issues**:
- Line 231: `explain()` - Returns placeholder explanation
- Line 393-399: `_execute_stages()` - Returns empty result instead of actually executing
- Line 517-565: `_default_operator_builder()` - Partial implementations with fallbacks

---

## Missing Components

### 1. **Sort Operator** ❌
- Not in Cython operators
- Not in _default_operator_builder
- Needed for: ORDER BY, window functions, range partitioning

### 2. **Distinct Operator** ❌
- CythonDistinctOperator exists in aggregations.pyx but:
  - Not exported/used in StageScheduler
  - Not handled in _default_operator_builder
  - Missing Cython .so file (not built)

### 3. **Window/OVER Operators** ❌
- spillable_window_buffer.pyx exists (built)
- Missing window function orchestration
- No handling in _default_operator_builder
- No test coverage

### 4. **Union Operator** ❌
- Defined in distributed_plan.py (OperatorType.UNION)
- No implementation in operators/
- No Cython module

### 5. **Limit/Offset Operator** ⚠️
- Exists in _default_operator_builder (line 577-583)
- Very basic: just table.slice(offset, limit)
- Doesn't integrate with partitioning
- Works only on single partition

### 6. **Projection (SELECT) Operator** ⚠️
- Implemented in _default_operator_builder (line 568-575)
- Just column selection, no expression evaluation
- Can't handle SELECT col1 + col2 AS total
- Missing Arrow compute integration

---

## Critical Gaps in Operator Dispatch

### In `_default_operator_builder()` (lines 494-587)

**What's Handled**:
```python
if spec.type == "Filter":           # ✅ Works
elif spec.type == "Aggregate":      # ⚠️  Partial (pandas fallback)
elif spec.type == "HashJoin":       # ❌ Returns None (not implemented!)
elif spec.type == "Projection":     # ⚠️  Basic column selection
elif spec.type == "Limit":          # ⚠️  Basic slicing
else:                               # ❌ Unknown operator - returns None
```

**Specific Issues**:

#### HashJoin (line 563-566)
```python
elif spec.type == "HashJoin":
    # For joins, we need special handling (two inputs)
    # Return None for now - joins handled specially in scheduler
    return None
```
**Problem**: 
- Returns None, so _execute_single_operator does nothing
- StageScheduler doesn't have "special handling" for joins
- Joins are completely broken in distributed execution

#### Aggregate (line 521-561)
```python
else:
    # Group-by aggregation using pandas for now
    df = table.to_pandas()
    agg_dict = {}
    # ... pandas groupby ...
```
**Problems**:
- Converts to pandas (memory blow-up)
- Doesn't use CythonGroupByOperator
- Can't handle bigger-than-memory data
- Performance regression vs available Cython ops

#### Filter (line 508-519)
```python
try:
    from sabot._cython.operators.filter_operator import CythonFilterOperator
    filter_op = CythonFilterOperator(filter_expr)
    return filter_op.apply
except ImportError:
    # Fallback
```
**Problem**: 
- CythonFilterOperator.apply is a method, should be instance
- Would crash if called as: operator(table)

---

## State Management Issues

### Hash Join Build State
```python
# No handling for join build phase in StageScheduler
# Current flow: Input → Operator → Output
# 
# Correct flow should be:
# LEFT input → JOIN (build hash table from RIGHT) → Output
#              ↑
#              RIGHT input needed!
```

**Missing Functionality**:
- No multi-input stage semantics
- Can't handle "broadcast join" vs "shuffle join"
- No state handoff between stages

### Aggregate Partial State
```python
# streaming_groupby.pyx has partial aggregates
# But StageScheduler doesn't merge partials from multiple stages
# 
# Should be:
# Stage 1: Compute partials
# Stage 2 (shuffle): Redistribute by group keys
# Stage 3: Merge partials → final aggregates
```

---

## Import/Availability Issues

**Successful Imports**:
```python
from sabot._cython.operators.joins import CythonHashJoinOperator          # ✅
from sabot._cython.operators.transform import CythonMapOperator           # ✅
from sabot._cython.operators.streaming_groupby import StreamingGroupByOperator # ✅
from sabot._cython.operators.morsel_operator import MorselDrivenOperator # ✅
from sabot._cython.operators.base_operator import BaseOperator            # ✅
```

**Failed Imports**:
```python
from sabot._cython.operators.filter_operator import CythonFilterOperator
# ModuleNotFoundError: No module named 'sabot._cython.operators.filter_operator'
# (filter_operator.pyx not built to .so)

from sabot._cython.operators.aggregations import CythonGroupByOperator
# ModuleNotFoundError: No module named 'sabot._cython.operators.aggregations'
# (aggregations.pyx not built to .so)
```

**These .pyx files exist but didn't build to .so**:
- filter_operator.pyx - source exists, no .so file
- aggregations.pyx - source exists, no .so file
- plan_to_operators.pyx - source exists, no .so file
- expression_translator.pyx - source exists, no .so file
- registry_optimized.pyx - source exists, no .so file
- hash_join_streaming.pyx - has .so but may not be exported properly
- hash_join_memory.pyx - has no .so file

---

## Database Schema Integration

### What's Defined
```python
@dataclass
class OperatorSpec:
    type: str                           # Operator type
    filter_expression: str              # For Filter
    group_by_keys: List[str]            # For Aggregate
    aggregate_functions: List[str]      # For Aggregate
    aggregate_columns: List[str]        # For Aggregate
    left_keys: List[str]                # For Join
    right_keys: List[str]               # For Join
    join_type: str                      # inner/left/right/outer
    projected_columns: List[str]        # For Projection
    # ... but WHERE IS THE PLAN? How do we know schema of results?
```

### Missing Information Flow
```
SQL: SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id

Should produce:
OpSpec for TableScan:
  - table_name: "orders"
  - column_names: ["order_id", "customer_id", "amount", ...]
  
OpSpec for Aggregate:
  - group_by_keys: ["customer_id"]
  - aggregate_functions: ["sum"]
  - aggregate_columns: ["amount"]
  - ??? No output column names specified
  - ??? No output types specified

Then StageScheduler receives results but:
  - Doesn't know schema of intermediate tables
  - Can't validate column references
  - Can't pass schema to next stage
```

---

## Test Coverage

**Existing Tests**:
```
tests/unit/sql/test_sql_direct.py              # SQL execution tests
tests/unit/sql/test_sabot_sql_python.py         # Python implementation tests
tests/unit/sql/test_sql_integration.py         # Integration tests
tests/unit/sql/test_sql_with_sabot_operators.py # Operator integration
tests/unit/sql/test_sabot_sql_cython.py        # Cython tests
```

**What's Tested**:
- Basic DuckDB integration
- Operator composition
- Filter pushdown

**What's NOT Tested**:
- Distributed execution (multi-stage)
- Join execution
- Aggregate merging
- Window functions
- Sort operations
- Distinct operations

---

## Component Completeness Matrix

| Component | Status | Implementation % | Blocking Issues |
|-----------|--------|------------------|-----------------|
| **Cython Filter** | ✅ Built | 100% | Not exported in setup.py |
| **Cython HashJoin** | ✅ Built | 100% | Not dispatched in controller |
| **Cython GroupBy** | ✅ Built | 100% | Not exported/dispatched |
| **Cython Map** | ✅ Built | 100% | Not used in SQL path |
| **PlanBridge** | 🟡 Partial | 80% | EXPLAIN parsing incomplete |
| **StageScheduler** | 🟡 Partial | 70% | Multi-input join broken |
| **SQLController** | 🟡 Partial | 50% | Most methods placeholder |
| **Sort Operator** | ❌ Missing | 0% | Complete rewrite needed |
| **Distinct** | 🟡 Partial | 20% | Operator exists, not wired |
| **Union** | ❌ Missing | 0% | No operator implementation |
| **Window** | 🟡 Partial | 30% | Buffer exists, orchestration missing |
| **Projection** | 🟡 Basic | 40% | Only column selection |
| **Limit** | 🟡 Basic | 60% | Single partition only |

---

## What Needs to Be Built

### Priority 1: Fix Join Execution (CRITICAL)

**Where**: sabot/sql/stage_scheduler.py

```python
# Current broken code (lines 309-329):
async def _execute_operator_pipeline(operators, input_data, partition_id):
    result = input_data
    for op_spec in operators:
        operator = self.operator_builder(op_spec, result)  # ← Single input!
        result = await self._execute_single_operator(operator, result)
    return result
```

**What's Needed**:
1. Detect "HashJoin" operator type
2. Get LEFT table from first operator (TableScan)
3. Get RIGHT table from shuffle buffer
4. Build join hash table from RIGHT
5. Probe LEFT table against hash table
6. Return joined result

### Priority 2: Export Cython Operators

**Where**: setup.py or pyproject.toml

Current setup doesn't build/export:
- filter_operator.pyx
- aggregations.pyx
- expression_translator.pyx
- plan_to_operators.pyx
- registry_optimized.pyx

### Priority 3: Implement Sort Operator

**Where**: sabot/_cython/operators/ (new file)

Needed for:
- ORDER BY
- Window functions (require sorted input)
- Range partitioning

Can use Arrow C++ sort_indices + take operations.

### Priority 4: Implement Window Functions

**Where**: sabot/sql/ (new orchestration) + existing spillable_window_buffer.pyx

Current state:
- Buffer exists: spillable_window_buffer.pyx ✅
- Orchestration missing: need window operator that:
  - Sorts by PARTITION BY + ORDER BY
  - Buffers rows per partition
  - Calls window function (ROW_NUMBER, LAG, LEAD, etc.)
  - Returns annotated rows

### Priority 5: Complete PlanBridge

**Where**: sabot/sql/plan_bridge.py

Need to:
- Parse DuckDB EXPLAIN output
- Extract operator tree
- Identify shuffle boundaries (JOIN, GROUP BY, ORDER BY distinct)
- Compute schema flow through operators
- Generate execution waves

---

## Working Example Flow

What SHOULD work if all pieces connected:

```sql
SELECT customer_id, COUNT(*), SUM(amount)
FROM orders
WHERE amount > 100
GROUP BY customer_id
```

**Current Reality** (mostly broken):

1. PlanBridge.parse_and_partition()
   - ✅ Calls DuckDB EXPLAIN (works)
   - ❌ Doesn't parse output (not implemented)
   - ❌ Returns placeholder plan

2. StageScheduler.execute()
   - ✅ Creates stages and waves (works)
   - ✅ Executes source stages (works)
   - ❌ Builds GROUP BY on single table (pandas fallback - wrong)
   - ❌ Never merges partial aggregates from multiple partitions

3. Controller._default_operator_builder()
   - ❌ Ignores CythonGroupByOperator in aggregations.pyx
   - ❌ Falls back to slow pandas groupby
   - ❌ Doesn't set up operator for result merging

**What Should Happen**:

1. Parse plan correctly
2. Create two stages:
   - Stage 0: TableScan → Filter → Partial Aggregate
   - Stage 1: Merge Partials → Final Aggregate
3. Shuffle partial aggregates by group keys
4. Merge in Stage 1 using Tonbo columnar state
5. Return final result

---

## Code Quality Issues

### Bug in Filter Operator Dispatch
```python
# Line 512-514 (controller.py)
from sabot._cython.operators.filter_operator import CythonFilterOperator
filter_op = CythonFilterOperator(filter_expr)
return filter_op.apply  # ← BUG: Returns the method, not a callable instance!
                        # Should be: return lambda table: filter_op.apply(table)
```

### Pandas Conversion Anti-Pattern
```python
# Line 547 (controller.py)
df = table.to_pandas()  # ← MEMORY BOMB for large tables
agg_dict = {}
result_df = df.groupby(group_keys).agg(agg_dict)
```

Should use CythonGroupByOperator directly (Arrow native, 10-100M rows/sec).

### No Schema Propagation
```python
# StageScheduler never validates or propagates schemas
# Stage 0: TableScan → produces schema A
# Stage 1: Receives "table" but doesn't check if schema matches
# Issues:
# - Can't validate column references in filters
# - Can't merge results with different schemas
# - Can't handle projection column reordering
```

---

## Recommendations

### Immediate (Week 1)
1. Export built Cython operators in setup.py
2. Fix join dispatch to use CythonHashJoinOperator
3. Fix aggregate dispatch to use CythonGroupByOperator
4. Add multi-input stage semantics to StageScheduler

### Short-term (Week 2)
1. Implement Sort operator (Arrow C++ sort_indices)
2. Complete PlanBridge EXPLAIN parsing
3. Add schema propagation to StageScheduler
4. Implement window operator orchestration

### Medium-term (Week 3)
1. Implement Distinct operator
2. Implement Union operator
3. Add query optimization (filter pushdown, projection pushdown)
4. Performance tuning with benchmarks

### Long-term
1. Implement Presto-style query plan optimizer
2. Cost-based join order selection
3. Adaptive query execution
4. Federated query (multiple data sources)

---

