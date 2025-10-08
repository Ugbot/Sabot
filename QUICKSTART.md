# Sabot Quick Start Guide

**Get started with Sabot in 5 minutes!**

---

## Installation (1 minute)

```bash
cd /Users/bengamble/Sabot
pip install -e .
```

---

## Your First Pipeline (2 minutes)

Create a simple pipeline with JobGraph:

```bash
python examples/00_quickstart/hello_sabot.py
```

**Output:**
```
✅ Created JobGraph: 'hello_sabot'
✅ Added SOURCE operator: load_numbers
✅ Added FILTER operator: filter_even
✅ Added SINK operator: output_results

Pipeline structure:
  1. load_numbers (source)
  2. filter_even (filter)
  3. output_results (sink)
```

**What you learned:**
- Create a JobGraph (logical plan)
- Add operators (SOURCE, FILTER, SINK)
- Connect operators into a pipeline

---

## Execute a Pipeline (2 minutes)

Run a pipeline with real data:

```bash
python examples/00_quickstart/filter_and_map.py
```

**Output:**
```
Pipeline executed:
  1. Loaded 1,000 transactions
  2. Filtered to high-value (amount > 300)
  3. Calculated 10% tax
  4. Selected 4 columns
  5. Output 598 rows

Sample output (first 5 rows):
    id  amount   tax category
0  201   301.0  30.1        B
1  202   302.0  30.2        A
...
```

**What you learned:**
- FILTER operator (remove rows)
- MAP operator (transform data)
- SELECT operator (project columns)
- Local execution (no infrastructure needed)

---

## Join Two Sources (1 minute)

Enrich streaming data with reference tables:

```bash
python examples/00_quickstart/local_join.py
```

**Output:**
```
Pipeline executed:
  1. Loaded 10 quotes
  2. Loaded 20 securities
  3. Joined on instrumentId=ID
  4. Output 10 enriched rows

Enriched data: 10 rows, 5 columns

   instrumentId  price  size      CUSIP         NAME
0             1  100.5   100  CUSIP0001   Security 1
1             5  200.3   200  CUSIP0005   Security 5
...
```

**What you learned:**
- HASH_JOIN operator
- Multi-source pipelines
- Stream enrichment pattern
- Native Arrow joins (830M rows/sec)

---

## Next Steps

### Learn More Operators (30 minutes)

```bash
cd examples/01_local_pipelines/
# Batch processing, streaming, windows, stateful operators
```

### See Optimization (30 minutes)

```bash
cd examples/02_optimization/
python filter_pushdown_demo.py  # 2-5x speedup!
```

### Go Distributed (1 hour)

```bash
cd examples/03_distributed_basics/
python two_agents_simple.py  # JobManager + 2 agents
```

### Production Patterns (2 hours)

```bash
cd examples/04_production_patterns/stream_enrichment/
python distributed_enrichment.py  # Full production pattern
```

---

## Learning Path

**Total time: 3-6 hours from zero to production-ready**

```
00_quickstart (15 min) ✅ YOU ARE HERE
    ↓
01_local_pipelines (30 min)
    ↓
02_optimization (30 min)
    ↓
03_distributed_basics (1 hour)
    ↓
04_production_patterns (2 hours)
    ↓
05_advanced (2 hours)
    ↓
06_reference (production code)
```

---

## Key Concepts

### JobGraph (Logical Plan)

Describes **what** you want to do:

```python
from sabot.execution.job_graph import JobGraph, StreamOperatorNode, OperatorType

graph = JobGraph(job_name="my_pipeline")

source = StreamOperatorNode(operator_type=OperatorType.SOURCE, ...)
filter_op = StreamOperatorNode(operator_type=OperatorType.FILTER, ...)
sink = StreamOperatorNode(operator_type=OperatorType.SINK, ...)

graph.add_operator(source)
graph.add_operator(filter_op)
graph.add_operator(sink)

graph.connect(source.operator_id, filter_op.operator_id)
graph.connect(filter_op.operator_id, sink.operator_id)
```

### Operators

| Operator | Purpose | Example |
|----------|---------|---------|
| SOURCE | Read data | Kafka, Parquet, CSV |
| FILTER | Remove rows | price > 100 |
| MAP | Transform | Calculate spread |
| SELECT | Project columns | Select ID, price |
| HASH_JOIN | Join streams | Enrich with reference data |
| SINK | Write results | Kafka, Parquet |

### Optimization (Automatic)

```python
from sabot.compiler.plan_optimizer import PlanOptimizer

optimizer = PlanOptimizer()
optimized = optimizer.optimize(graph)

# Optimizer automatically:
# - Pushes filters before joins (2-5x speedup)
# - Projects columns early (20-40% memory reduction)
# - Reorders operators for efficiency
```

### Distributed Execution

```python
from sabot.job_manager import JobManager

job_manager = JobManager()
result = await job_manager.submit_job(optimized_graph)

# JobManager:
# - Breaks graph into tasks
# - Distributes to agents
# - Tracks execution
# - Handles failures
```

---

## Architecture

```
User Code (Python)
    ↓
JobGraph (Logical Plan)
    ↓
PlanOptimizer (Automatic Rewrites)
    ↓
JobManager (Coordinator)
    ↓
Agents (Workers) → Execute Tasks
```

**Everything follows this pattern!**

---

## Documentation

- **Examples:** `examples/README.md` - Learning path
- **User Guide:** `docs/USER_WORKFLOW.md` - Complete workflow
- **Architecture:** `docs/ARCHITECTURE_OVERVIEW.md` - System design
- **Progress:** `examples/IMPLEMENTATION_SUMMARY.md` - What's ready

---

## Getting Help

- **Examples not working?** Check `examples/IMPLEMENTATION_SUMMARY.md`
- **Questions?** File issue at https://github.com/sabot/sabot/issues
- **Want to contribute?** See `examples/REORGANIZATION_PROGRESS.md`

---

## What Makes Sabot Different?

✅ **Python-first** - Natural Pythonic API
✅ **Auto-optimization** - Optimizer rewrites your plan automatically
✅ **Distributed by default** - Scale from laptop to cluster
✅ **Arrow-native** - Zero-copy, 830M rows/sec joins
✅ **Easy onboarding** - Running examples in < 5 minutes

---

**Ready? → Start with `examples/00_quickstart/README.md`**

**Questions? → See `docs/USER_WORKFLOW.md`**

**Advanced? → Jump to `examples/04_production_patterns/`**
