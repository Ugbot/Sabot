# 00_quickstart - Start Here!

**Time to complete:** 10-15 minutes
**Prerequisites:** None - all examples run locally

This directory contains the absolute simplest examples to get started with Sabot.

---

## Learning Objectives

After completing these examples, you will understand:

✅ How to create a JobGraph (logical plan)
✅ How to add operators (source, filter, map, join, sink)
✅ How to connect operators into a pipeline
✅ How to execute pipelines locally (no distributed setup required)

---

## Examples

### 1. **hello_sabot.py** - Your First Pipeline (2 minutes)

The simplest possible example - build a pipeline and inspect it.

```bash
python examples/00_quickstart/hello_sabot.py
```

**What you'll learn:**
- Create a JobGraph
- Add 3 operators: source → filter → sink
- Connect operators
- Inspect the pipeline structure

**No execution yet** - just building the logical plan!

---

### 2. **filter_and_map.py** - Execute a Pipeline (5 minutes)

Add MAP and SELECT operators, then execute the pipeline locally.

```bash
python examples/00_quickstart/filter_and_map.py
```

**What you'll learn:**
- FILTER - Remove rows based on conditions
- MAP - Transform data (add columns, calculations)
- SELECT - Project specific columns
- **Local execution** - Run without distributed agents

**Pipeline:**
```
Source → Filter → Map → Select → Sink
```

---

### 3. **local_join.py** - Join Two Sources (5 minutes)

Join two data sources (stream enrichment pattern).

```bash
python examples/00_quickstart/local_join.py
```

**What you'll learn:**
- HASH_JOIN - Inner join on key columns
- Multi-source pipelines (2+ inputs)
- Stream enrichment (add reference data to streaming events)
- Native Arrow joins (830M rows/sec)

**Pipeline:**
```
Quotes    ↘
            Join → Sink
Securities ↗
```

---

## Key Concepts

### JobGraph (Logical Plan)

A `JobGraph` describes **what** you want to do, not **how** to execute it:

```python
from sabot.execution.job_graph import JobGraph, StreamOperatorNode, OperatorType

graph = JobGraph(job_name="my_pipeline")
```

**Logical** - No parallelism, no agents, no physical deployment details
**Declarative** - Describe transformations, not execution
**Optimizable** - Can be rewritten for better performance

### Operators

Transformation steps in your pipeline:

| Operator | Purpose | Example |
|----------|---------|---------|
| `SOURCE` | Read data | Load from Kafka, Parquet, CSV |
| `FILTER` | Remove rows | `price > 100` |
| `MAP` | Transform | Calculate spread, add columns |
| `SELECT` | Project columns | Select ID, price, size |
| `HASH_JOIN` | Join streams | Enrich quotes with securities |
| `SINK` | Write results | Output to Kafka, Parquet |

### Local vs Distributed

**Local Execution (these examples):**
- No JobManager, no Agents
- Simple `LocalExecutor` class
- Good for learning, testing, development

**Distributed Execution (later):**
- JobManager coordinates
- Agents execute tasks
- Scales horizontally
- See `../03_distributed_basics/`

---

## Common Patterns

### Simple Pipeline
```python
Source → Filter → Sink
```

### Transformation Chain
```python
Source → Filter → Map → Select → Sink
```

### Stream Enrichment
```python
Stream       ↘
               Join → Sink
Reference Table ↗
```

---

## Next Steps

Completed all 3 examples? Great! Next:

**Option A: Learn More Operators**
→ `../01_local_pipelines/` - Batch mode, streaming, windows, stateful processing

**Option B: See Optimization**
→ `../02_optimization/` - Filter pushdown, projection pushdown, performance tuning

**Option C: Go Distributed**
→ `../03_distributed_basics/` - JobManager + Agents, task distribution

**Option D: Production Patterns**
→ `../04_production_patterns/` - Real-world use cases (fraud detection, enrichment)

---

## Troubleshooting

### Import errors
Make sure you're in the Sabot root directory:
```bash
cd /Users/bengamble/Sabot
python examples/00_quickstart/hello_sabot.py
```

### ModuleNotFoundError
Install Sabot:
```bash
pip install -e .
```

### Want to see actual data?
Run `filter_and_map.py` or `local_join.py` - they execute pipelines with real data!

---

**Next:** Choose your path above or continue to `../01_local_pipelines/README.md`
