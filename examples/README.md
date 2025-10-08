# Sabot Examples - Easy Onboarding Ramp

**Clean, focused examples with a progressive learning path from simple local execution to production distributed systems.**

All examples follow the **JobGraph → Optimizer → JobManager → Agents** pattern.

---

## 🚀 Quick Start (10 minutes)

**New to Sabot? Start here:**

### [00_quickstart/](00_quickstart/) - Your First Pipeline

Three simple examples that run completely locally (no infrastructure required):

1. **`hello_sabot.py`** (2 min) - Build your first JobGraph
2. **`filter_and_map.py`** (5 min) - Execute a pipeline with real data
3. **`local_join.py`** (5 min) - Join two data sources

```bash
# Run the quickstart examples
python examples/00_quickstart/hello_sabot.py
python examples/00_quickstart/filter_and_map.py
python examples/00_quickstart/local_join.py
```

**What you'll learn:**
- ✅ Create JobGraphs (logical plans)
- ✅ Use operators (SOURCE, FILTER, MAP, SELECT, JOIN, SINK)
- ✅ Connect operators into pipelines
- ✅ Execute locally (no distributed setup)

**Next:** Continue to [01_local_pipelines/](#) for batch and streaming modes

---

## 📚 Learning Path

### Beginner (1 hour)

#### 1. [00_quickstart/](00_quickstart/) - Basics ✅ **READY**
- hello_sabot.py - First JobGraph
- filter_and_map.py - Basic operators
- local_join.py - Join pattern

#### 2. [01_local_pipelines/](#) - Local Execution 🚧 **TODO**
- batch_processing.py - Batch mode (Parquet → Parquet)
- streaming_simulation.py - Streaming mode
- window_aggregation.py - Windowed analytics
- stateful_processing.py - Stateful operators

#### 3. [02_optimization/](#) - Optimization 🚧 **TODO**
- filter_pushdown_demo.py - Filter pushdown (2-5x speedup)
- projection_pushdown_demo.py - Column projection
- before_after_comparison.py - Side-by-side comparison
- optimization_stats.py - Detailed metrics

---

### Intermediate (2 hours)

#### 4. [03_distributed_basics/](#) - Distributed Execution 🚧 **TODO**
- two_agents_simple.py - JobManager + 2 agents
- round_robin_scheduling.py - Task distribution
- agent_failure_recovery.py - Fault tolerance
- state_partitioning.py - Distributed state

#### 5. [04_production_patterns/](#) - Real-World Patterns 🚧 **TODO**

**Stream Enrichment:**
- local_enrichment.py - Local version
- distributed_enrichment.py - Distributed version ✅ **READY**

**Fraud Detection:**
- local_fraud.py - Local version
- distributed_fraud.py - Distributed version

**Real-Time Analytics:**
- local_analytics.py - Local version
- distributed_analytics.py - Distributed version

---

### Advanced (3 hours)

#### 6. [05_advanced/](#) - Extensions 🚧 **TODO**
- custom_operators.py - Build custom operators
- network_shuffle.py - Arrow Flight shuffle
- numba_compilation.py - Auto-Numba UDFs (10-50x speedup)
- dbos_integration.py - Durable execution

#### 7. [06_reference/](#) - Production Reference 🚧 **TODO**

**Fintech Pipeline:**
- Complete production pipeline
- 10M+ row enrichment
- Full optimization stack

**Data Lakehouse:**
- Batch ETL (100M+ rows)
- Parquet processing
- Production-scale demos

---

## 🎯 Learn by Use Case

| Use Case | Example | Level |
|----------|---------|-------|
| **Stream Enrichment** | [04_production_patterns/stream_enrichment/](#) | Intermediate |
| **Fraud Detection** | [04_production_patterns/fraud_detection/](#) | Intermediate |
| **Real-Time Analytics** | [04_production_patterns/real_time_analytics/](#) | Intermediate |
| **Batch ETL** | [06_reference/data_lakehouse/](#) | Advanced |
| **Financial Data** | [06_reference/fintech_pipeline/](#) | Advanced |

---

## 💡 Learn by Feature

| Feature | Example | Level |
|---------|---------|-------|
| **JobGraph Basics** | [00_quickstart/hello_sabot.py](00_quickstart/hello_sabot.py) | Beginner |
| **Filter, Map, Select** | [00_quickstart/filter_and_map.py](00_quickstart/filter_and_map.py) | Beginner |
| **Joins** | [00_quickstart/local_join.py](00_quickstart/local_join.py) | Beginner |
| **Optimization** | [02_optimization/](#) | Beginner |
| **Distributed Execution** | [03_distributed_basics/](#) | Intermediate |
| **Stateful Processing** | [01_local_pipelines/stateful_processing.py](#) | Beginner |
| **Windowing** | [01_local_pipelines/window_aggregation.py](#) | Beginner |
| **Custom Operators** | [05_advanced/custom_operators.py](#) | Advanced |
| **Numba UDFs** | [05_advanced/numba_compilation.py](#) | Advanced |
| **DBOS Integration** | [05_advanced/dbos_integration.py](#) | Advanced |

---

## 🏃 Quick Examples

### Run a Simple Pipeline (5 minutes)

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
```

### Run a Distributed Demo (1 minute)

**Note:** This requires completing the distributed examples (TODO)

```bash
python examples/03_distributed_basics/two_agents_simple.py
```

---

## 📖 Documentation

Each example includes:

- **Header comment** with what it demonstrates, prerequisites, runtime, next steps
- **Inline comments** explaining each step
- **README.md** in each directory with learning objectives

**Key Docs:**
- [User Workflow Guide](../docs/USER_WORKFLOW.md) - Complete user guide
- [Architecture Overview](../docs/ARCHITECTURE_OVERVIEW.md) - System architecture
- [Reorganization Progress](REORGANIZATION_PROGRESS.md) - Implementation status

---

## 🔧 Setup

### Install Sabot

```bash
cd /Users/bengamble/Sabot
pip install -e .
```

### Infrastructure (Optional)

For advanced examples (Kafka, PostgreSQL):

```bash
docker compose up -d
```

**Quickstart examples don't require infrastructure!**

---

## 🌟 Existing Examples (Legacy)

These examples still work but are being reorganized:

- `fraud_app.py` - Fraud detection (being refactored into 04_production_patterns/)
- `batch_first_examples.py` - Batch-first API (reference)
- `dimension_tables_demo.py` - Dimension tables (reference)
- `fintech_enrichment_demo/` - Complete fintech pipeline (moving to 06_reference/)
- `simple_distributed_demo.py` - Simple distributed (✅ copied to 00_quickstart/)

---

## 📊 Progress

| Category | Status | Files |
|----------|--------|-------|
| **00_quickstart** | ✅ Complete | 3 examples + README |
| **01_local_pipelines** | 🚧 TODO | 4 examples + README |
| **02_optimization** | 🚧 TODO | 4 examples + README |
| **03_distributed_basics** | 🚧 TODO | 4 examples + README |
| **04_production_patterns** | 🚧 Partial | 6 examples + 3 READMEs |
| **05_advanced** | 🚧 TODO | 4 examples + README |
| **06_reference** | 🚧 TODO | Organize existing |

**See:** [REORGANIZATION_PROGRESS.md](REORGANIZATION_PROGRESS.md) for detailed status

---

## 🤝 Contributing

When adding new examples:

1. Follow the directory structure (00-06 progression)
2. Use consistent header format (see existing examples)
3. Include clear learning objectives
4. Provide next steps to related examples
5. Test examples before committing

---

## 💬 Get Help

- **Questions?** File an issue at https://github.com/sabot/sabot/issues
- **Documentation:** See `docs/` directory
- **Examples not working?** Check `REORGANIZATION_PROGRESS.md` for status

---

**Ready to start? → [00_quickstart/README.md](00_quickstart/README.md)**
