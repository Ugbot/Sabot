# Examples Reorganization Progress

**Started:** October 8, 2025
**Goal:** Create progressive learning path from simple local examples to production distributed systems

---

## ✅ Completed

### 00_quickstart/ (Complete)

**Files created:**
- ✅ `hello_sabot.py` - Simplest JobGraph example
- ✅ `filter_and_map.py` - Basic operators with execution
- ✅ `local_join.py` - Join two data sources
- ✅ `README.md` - Learning guide

**Status:** Ready for users! Provides 10-15 minute onboarding ramp.

---

## 🚧 In Progress / TODO

### 01_local_pipelines/ (To Create)

**Purpose:** Local execution patterns (no distributed components)

**Files to create:**

1. **`batch_processing.py`**
   - Load Parquet → transform → write Parquet
   - Shows batch mode (finite data)
   - Uses PlanOptimizer for optimization
   - No agents required
   - Runtime: ~5 seconds

2. **`streaming_simulation.py`**
   - Simulate streaming with generator
   - Show identical operators work for streaming
   - Demonstrate "everything is batches" principle
   - Runtime: ~10 seconds (continuous)

3. **`window_aggregation.py`**
   - Tumbling window (5-second windows)
   - Aggregate events (count, sum, avg)
   - Local execution
   - Runtime: ~15 seconds

4. **`stateful_processing.py`**
   - Stateful aggregation (running totals)
   - Use MemoryBackend for state
   - Show state management locally
   - Runtime: ~10 seconds

5. **`README.md`**
   - Learning objectives
   - Batch vs streaming comparison
   - Run instructions

---

### 02_optimization/ (To Create)

**Purpose:** See PlanOptimizer in action

**Files to create:**

1. **`filter_pushdown_demo.py`**
   - Build pipeline: source → join → filter
   - Show optimizer pushes filter before join
   - Display before/after execution plans
   - Measure 2-5x performance improvement
   - Runtime: ~10 seconds

2. **`projection_pushdown_demo.py`**
   - Load table with 10 columns
   - Select only 2 columns at end
   - Show optimizer pushes select to source
   - Measure 20-40% memory reduction
   - Runtime: ~10 seconds

3. **`before_after_comparison.py`**
   - Run same query unoptimized vs optimized
   - Display side-by-side metrics
   - Show optimization statistics
   - Runtime: ~20 seconds

4. **`optimization_stats.py`**
   - Detailed optimizer metrics
   - Show all optimization passes
   - Explain each optimization
   - Runtime: ~5 seconds

5. **`README.md`**
   - Optimizer overview
   - Each optimization explained
   - Performance expectations

---

### 03_distributed_basics/ (To Create)

**Purpose:** Introduction to JobManager + Agents

**Files to create:**

1. **`two_agents_simple.py`** (copy from `simple_distributed_demo.py`)
   - 2 agents, JobManager
   - Simple task distribution
   - Shows agent lifecycle
   - Runtime: ~1 minute

2. **`round_robin_scheduling.py`**
   - 4 agents, 12 tasks
   - Show round-robin distribution
   - Task assignment visualization
   - Runtime: ~1 minute

3. **`agent_failure_recovery.py`**
   - Simulate agent failure
   - Show task reassignment
   - Demonstrate fault tolerance
   - Runtime: ~2 minutes

4. **`state_partitioning.py`**
   - Distributed stateful aggregation
   - State partitioned by key
   - Show state distribution across agents
   - Runtime: ~1 minute

5. **`README.md`**
   - Agent architecture
   - JobManager coordination
   - Fault tolerance explanation

---

### 04_production_patterns/ (To Create)

**Purpose:** Real-world production patterns

**Subdirectories:**

#### stream_enrichment/

1. **`local_enrichment.py`**
   - Stream-table join (local)
   - 1000 events + 10K reference table
   - Local execution
   - Runtime: ~5 seconds

2. **`distributed_enrichment.py`** (copy from `optimized_distributed_demo.py`)
   - Full JobGraph → Optimizer → JobManager → Agents
   - 10K events + 100K reference table
   - Distributed execution
   - Runtime: ~1 minute

3. **`README.md`**
   - Stream enrichment pattern
   - When to use
   - Performance characteristics

#### fraud_detection/

1. **`local_fraud.py`**
   - Simplified fraud detection (local)
   - 3 patterns: velocity, amount anomaly, geo-impossible
   - Stateful processing
   - Runtime: ~10 seconds

2. **`distributed_fraud.py`** (refactor from `fraud_app.py`)
   - Full fraud detection with JobGraph
   - Distributed stateful processing
   - Multiple agents
   - Runtime: ~2 minutes

3. **`README.md`**
   - Fraud detection patterns
   - Stateful processing explanation

#### real_time_analytics/

1. **`local_analytics.py`**
   - Windowed aggregation (local)
   - 5-minute tumbling windows
   - Count, sum, avg metrics
   - Runtime: ~15 seconds

2. **`distributed_analytics.py`**
   - Distributed windowed analytics
   - Multiple agents compute windows
   - Final aggregation
   - Runtime: ~2 minutes

3. **`README.md`**
   - Window operations
   - Aggregation patterns

---

### 05_advanced/ (To Create)

**Purpose:** Advanced features and extensions

**Files to create:**

1. **`custom_operators.py`**
   - Build custom operator class
   - Implement process_batch()
   - Register with JobGraph
   - Runtime: ~5 seconds

2. **`network_shuffle.py`**
   - Arrow Flight shuffle demo
   - Data transfer between agents
   - Zero-copy demonstration
   - Runtime: ~1 minute

3. **`numba_compilation.py`** (refactor from `numba_auto_vectorization_demo.py`)
   - Auto-Numba UDF compilation
   - Show 10-50x speedup
   - Compare compiled vs interpreted
   - Runtime: ~10 seconds

4. **`dbos_integration.py`**
   - DBOS durable execution
   - PostgreSQL state backend
   - Exactly-once semantics
   - Runtime: ~30 seconds (requires Postgres)

5. **`README.md`**
   - Advanced features overview
   - When to use each
   - Performance tuning

---

### 06_reference/ (To Organize)

**Purpose:** Production-ready reference implementations

**Subdirectories to create:**

#### fintech_pipeline/

- Move `fintech_enrichment_demo/` here
- Add `README.md` with full documentation
- Keep all existing demos:
  - `two_node_network_demo.py`
  - `optimized_distributed_demo.py`
  - `arrow_optimized_enrichment.py`

#### data_lakehouse/ (New)

- Batch ETL pipeline
- Parquet → transform → Parquet
- Full optimization stack
- Production-scale (100M+ rows)

---

## Main README Update

**File:** `examples/README.md`

**New structure:**

```markdown
# Sabot Examples - Easy Onboarding Ramp

## 🚀 Quick Start (10 minutes)

**New to Sabot? Start here:**

1. [00_quickstart/](00_quickstart/) - Your first pipeline (10 min)
   - hello_sabot.py
   - filter_and_map.py
   - local_join.py

## 📚 Learning Path

### Beginner (1 hour)

2. [01_local_pipelines/](01_local_pipelines/) - Local execution
3. [02_optimization/](02_optimization/) - See optimizer in action

### Intermediate (2 hours)

4. [03_distributed_basics/](03_distributed_basics/) - JobManager + Agents
5. [04_production_patterns/](04_production_patterns/) - Real-world use cases

### Advanced (3 hours)

6. [05_advanced/](05_advanced/) - Custom extensions
7. [06_reference/](06_reference/) - Production reference code

## 🎯 By Use Case

- **Stream enrichment** → 04_production_patterns/stream_enrichment/
- **Fraud detection** → 04_production_patterns/fraud_detection/
- **Real-time analytics** → 04_production_patterns/real_time_analytics/
- **Batch ETL** → 06_reference/data_lakehouse/
- **Financial data** → 06_reference/fintech_pipeline/

## 💡 By Feature

- **JobGraph basics** → 00_quickstart/
- **Optimization** → 02_optimization/
- **Distributed execution** → 03_distributed_basics/
- **Stateful processing** → 01_local_pipelines/stateful_processing.py
- **Windowing** → 01_local_pipelines/window_aggregation.py
- **Joins** → 00_quickstart/local_join.py
- **Custom operators** → 05_advanced/custom_operators.py
- **Numba UDFs** → 05_advanced/numba_compilation.py
```

---

## Summary Statistics

### Completed
- ✅ 00_quickstart/ - 4 files (3 examples + README)
- ✅ Directory structure created

### To Create
- ⏳ 01_local_pipelines/ - 5 files
- ⏳ 02_optimization/ - 5 files
- ⏳ 03_distributed_basics/ - 5 files
- ⏳ 04_production_patterns/ - 9 files (3 subdirs)
- ⏳ 05_advanced/ - 5 files
- ⏳ 06_reference/ - Organize existing + 1 new subdir
- ⏳ Main README update

**Total:** ~38 files to create/refactor

---

## Implementation Notes

### Reuse Existing Code

**Copy these files:**
- `simple_distributed_demo.py` → `03_distributed_basics/two_agents_simple.py`
- `optimized_distributed_demo.py` → `04_production_patterns/stream_enrichment/distributed_enrichment.py`

**Refactor these files:**
- `fraud_app.py` → `04_production_patterns/fraud_detection/distributed_fraud.py` (add JobGraph)
- `numba_auto_vectorization_demo.py` → `05_advanced/numba_compilation.py` (simplify)

**Keep as reference:**
- `fintech_enrichment_demo/` → `06_reference/fintech_pipeline/`
- `batch_first_examples.py` (keep in root for now)
- `dimension_tables_demo.py` (keep in root for now)

### Consistent Pattern

Every example should follow:

```python
#!/usr/bin/env python3
"""
Title - Brief Description
=========================

**What this demonstrates:**
- Feature 1
- Feature 2

**Prerequisites:**
- List any requirements

**Runtime:** X seconds/minutes

**Next steps:**
- Related example 1
- Related example 2

Run:
    python examples/XX_category/example.py
"""

# Imports
# Implementation
# Main function with clear steps
# Results and summary
```

---

## Next Steps

1. ✅ Complete 00_quickstart (DONE)
2. Create 01_local_pipelines (4 examples + README)
3. Create 02_optimization (4 examples + README)
4. Create 03_distributed_basics (copy + 3 new + README)
5. Create 04_production_patterns (refactor + 6 new + 3 READMEs)
6. Create 05_advanced (refactor + 3 new + README)
7. Organize 06_reference
8. Update main examples/README.md

**Estimated remaining work:** 3-4 hours

---

**Status:** Foundation complete, onboarding ramp working!
**Priority:** Complete 01_local_pipelines next for full beginner path
