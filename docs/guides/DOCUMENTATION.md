# Sabot Documentation Guide

**Last Updated:** October 8, 2025

This guide helps you navigate Sabot's documentation structure.

---

## 📚 Documentation Structure

```
sabot/
├── README.md                    # Project overview and quick links
├── QUICKSTART.md               # 5-minute quick start guide
├── PROJECT_MAP.md              # Complete codebase map
├── DOCUMENTATION.md            # This file - documentation index
│
├── docs/                       # USER-FACING DOCUMENTATION
│   ├── USER_WORKFLOW.md        # Complete user workflow guide
│   ├── ARCHITECTURE_OVERVIEW.md # System architecture
│   ├── GETTING_STARTED.md      # Getting started guide
│   ├── API_REFERENCE.md        # API reference
│   ├── KAFKA_INTEGRATION.md    # Kafka integration guide
│   ├── CLI.md                  # CLI reference
│   ├── design/                 # Architecture design docs
│   │   ├── UNIFIED_BATCH_ARCHITECTURE.md
│   │   └── ARCHITECTURE_DECISIONS.md
│   └── user-guide/             # User guides
│       └── morsel-parallelism.md
│
├── examples/                   # EXAMPLES AND TUTORIALS
│   ├── README.md               # Examples index and learning path
│   ├── 00_quickstart/          # Get started (< 15 min)
│   ├── 01_local_pipelines/     # Local execution (30-45 min)
│   ├── 02_optimization/        # Optimization (30-45 min)
│   ├── 03_distributed_basics/  # Distributed (1 hour)
│   └── 04_production_patterns/ # Production patterns (2-3 hours)
│
└── dev-docs/                   # DEVELOPMENT DOCUMENTATION
    ├── INDEX.md                # Development docs index
    ├── implementation/         # Implementation guides (Phase 1-7)
    ├── planning/               # Planning and roadmaps
    ├── analysis/               # Code analysis
    ├── results/                # Test results and benchmarks
    ├── status/                 # Implementation status
    ├── design/                 # Design documents
    ├── roadmap/                # Feature roadmaps
    └── testing/                # Testing docs
```

---

## 🎯 Quick Links by Role

### **I'm a New User**

Start here:
1. [QUICKSTART.md](QUICKSTART.md) - 5-minute quick start
2. [examples/00_quickstart/](examples/00_quickstart/) - First examples
3. [docs/USER_WORKFLOW.md](docs/USER_WORKFLOW.md) - Complete workflow
4. [docs/GETTING_STARTED.md](docs/GETTING_STARTED.md) - Detailed setup

### **I'm Learning Sabot**

Follow the progressive learning path:
1. [examples/README.md](examples/README.md) - Learning path overview
2. [examples/00_quickstart/](examples/00_quickstart/) - 15 minutes
3. [examples/01_local_pipelines/](examples/01_local_pipelines/) - 30-45 minutes
4. [examples/02_optimization/](examples/02_optimization/) - 30-45 minutes
5. [examples/03_distributed_basics/](examples/03_distributed_basics/) - 1 hour
6. [examples/04_production_patterns/](examples/04_production_patterns/) - 2-3 hours

### **I'm Building with Sabot**

Reference documentation:
- [docs/API_REFERENCE.md](docs/API_REFERENCE.md) - Complete API
- [docs/USER_WORKFLOW.md](docs/USER_WORKFLOW.md) - End-to-end workflow
- [docs/KAFKA_INTEGRATION.md](docs/KAFKA_INTEGRATION.md) - Kafka setup
- [docs/CLI.md](docs/CLI.md) - CLI commands
- [docs/design/UNIFIED_BATCH_ARCHITECTURE.md](docs/design/UNIFIED_BATCH_ARCHITECTURE.md) - Architecture

### **I'm Contributing to Sabot**

Development docs:
- [PROJECT_MAP.md](PROJECT_MAP.md) - Complete codebase map
- [dev-docs/INDEX.md](dev-docs/INDEX.md) - Development docs index
- [dev-docs/implementation/](dev-docs/implementation/) - Phase 1-7 implementation guides
- [dev-docs/planning/](dev-docs/planning/) - Roadmaps and planning
- [dev-docs/status/](dev-docs/status/) - Implementation status

---

## 📖 Documentation by Topic

### Getting Started
- [QUICKSTART.md](QUICKSTART.md) - 5-minute quick start
- [docs/GETTING_STARTED.md](docs/GETTING_STARTED.md) - Detailed setup
- [examples/00_quickstart/](examples/00_quickstart/) - First examples

### Core Concepts
- [docs/USER_WORKFLOW.md](docs/USER_WORKFLOW.md) - Complete workflow
- [docs/ARCHITECTURE_OVERVIEW.md](docs/ARCHITECTURE_OVERVIEW.md) - System architecture
- [docs/design/UNIFIED_BATCH_ARCHITECTURE.md](docs/design/UNIFIED_BATCH_ARCHITECTURE.md) - Unified architecture

### Examples and Tutorials
- [examples/README.md](examples/README.md) - Learning path
- [examples/00_quickstart/](examples/00_quickstart/) - Quickstart (3 examples)
- [examples/01_local_pipelines/](examples/01_local_pipelines/) - Local pipelines (4 examples)
- [examples/02_optimization/](examples/02_optimization/) - Optimization (4 examples)
- [examples/03_distributed_basics/](examples/03_distributed_basics/) - Distributed (4 examples)
- [examples/04_production_patterns/](examples/04_production_patterns/) - Production patterns

### API and Reference
- [docs/API_REFERENCE.md](docs/API_REFERENCE.md) - Complete API
- [docs/CLI.md](docs/CLI.md) - CLI reference
- [PROJECT_MAP.md](PROJECT_MAP.md) - Codebase structure

### Integrations
- [docs/KAFKA_INTEGRATION.md](docs/KAFKA_INTEGRATION.md) - Kafka
- [docs/KAFKA_QUICKSTART.md](docs/KAFKA_QUICKSTART.md) - Kafka quick start

### Development
- [dev-docs/INDEX.md](dev-docs/INDEX.md) - Development index
- [dev-docs/implementation/](dev-docs/implementation/) - Implementation guides
- [dev-docs/planning/](dev-docs/planning/) - Planning docs
- [dev-docs/results/](dev-docs/results/) - Test results
- [dev-docs/status/](dev-docs/status/) - Status updates

---

## 🗂️ Directory Details

### `docs/` - User-Facing Documentation

**Purpose:** Documentation for Sabot users (not developers)

**Contents:**
- **Getting Started:** Setup, installation, first pipeline
- **User Workflow:** Complete end-to-end workflow
- **Architecture:** System design and concepts
- **API Reference:** Complete API documentation
- **Integration Guides:** Kafka, databases, etc.
- **User Guides:** Specific topics (e.g., morsel parallelism)

**Audience:** Sabot users, data engineers, developers building with Sabot

---

### `examples/` - Examples and Tutorials

**Purpose:** Progressive learning path with working examples

**Contents:**
- **00_quickstart:** Get started in < 15 minutes (3 examples)
- **01_local_pipelines:** Learn local execution (4 examples)
- **02_optimization:** Learn automatic optimization (4 examples)
- **03_distributed_basics:** Learn distributed execution (4 examples)
- **04_production_patterns:** Real-world patterns (stream enrichment, fraud detection, analytics)
- **05_advanced:** Advanced features (TODO)
- **06_reference:** Production-scale reference implementations (TODO)

**Audience:** New users, learners, anyone building with Sabot

**Total Examples:** 16 working examples across 4 directories

---

### `dev-docs/` - Development Documentation

**Purpose:** Internal development documentation, implementation guides, test results

**Contents:**
- **implementation/:** Phase 1-7 implementation guides
  - Phase 1: Batch Operator API
  - Phase 2: Auto-Numba Compilation
  - Phase 3: Morsel-Driven Operators
  - Phase 4: Network Shuffle
  - Phase 5: Agent Worker Node
  - Phase 6: DBOS Control Plane
  - Phase 7: Plan Optimization
- **planning/:** Roadmaps, reality checks, implementation plans
- **analysis/:** Code analysis, stub inventory
- **results/:** Test results, performance benchmarks
- **status/:** Implementation status, conversion status
- **design/:** Design documents
- **roadmap/:** Feature roadmaps
- **testing/:** Test organization

**Audience:** Sabot contributors, maintainers, developers working on Sabot internals

---

## 📝 Documentation Types

### User Documentation (`docs/`)
- **What:** How to use Sabot
- **Audience:** Users, data engineers
- **Examples:** USER_WORKFLOW.md, API_REFERENCE.md, KAFKA_INTEGRATION.md

### Examples (`examples/`)
- **What:** Working code examples
- **Audience:** Learners, new users
- **Examples:** 00_quickstart/, 01_local_pipelines/, etc.

### Development Documentation (`dev-docs/`)
- **What:** How Sabot is built, implementation details
- **Audience:** Contributors, maintainers
- **Examples:** Implementation guides, test results, roadmaps

---

## 🔍 Finding Documentation

### "How do I...?"

| Question | Documentation |
|----------|---------------|
| Get started in < 5 minutes? | [QUICKSTART.md](QUICKSTART.md) |
| Learn Sabot step-by-step? | [examples/README.md](examples/README.md) |
| Build my first pipeline? | [examples/00_quickstart/](examples/00_quickstart/) |
| Optimize my pipeline? | [examples/02_optimization/](examples/02_optimization/) |
| Run distributed? | [examples/03_distributed_basics/](examples/03_distributed_basics/) |
| Understand the architecture? | [docs/ARCHITECTURE_OVERVIEW.md](docs/ARCHITECTURE_OVERVIEW.md) |
| Use the API? | [docs/API_REFERENCE.md](docs/API_REFERENCE.md) |
| Integrate with Kafka? | [docs/KAFKA_INTEGRATION.md](docs/KAFKA_INTEGRATION.md) |
| Understand the codebase? | [PROJECT_MAP.md](PROJECT_MAP.md) |
| Contribute? | [dev-docs/INDEX.md](dev-docs/INDEX.md) |
| See implementation status? | [dev-docs/status/](dev-docs/status/) |
| See test results? | [dev-docs/results/](dev-docs/results/) |

---

## 🎓 Learning Paths

### Beginner Path (2-3 hours)
1. [QUICKSTART.md](QUICKSTART.md) - 5 min
2. [examples/00_quickstart/](examples/00_quickstart/) - 15 min
3. [examples/01_local_pipelines/](examples/01_local_pipelines/) - 45 min
4. [examples/02_optimization/](examples/02_optimization/) - 45 min

### Intermediate Path (3-4 hours)
1. Complete Beginner Path
2. [examples/03_distributed_basics/](examples/03_distributed_basics/) - 1 hour
3. [examples/04_production_patterns/stream_enrichment/](examples/04_production_patterns/stream_enrichment/) - 30 min
4. [docs/USER_WORKFLOW.md](docs/USER_WORKFLOW.md) - 30 min

### Advanced Path (5+ hours)
1. Complete Intermediate Path
2. [examples/04_production_patterns/](examples/04_production_patterns/) - 2 hours
3. [docs/ARCHITECTURE_OVERVIEW.md](docs/ARCHITECTURE_OVERVIEW.md) - 30 min
4. [docs/design/UNIFIED_BATCH_ARCHITECTURE.md](docs/design/UNIFIED_BATCH_ARCHITECTURE.md) - 1 hour

### Contributor Path
1. [PROJECT_MAP.md](PROJECT_MAP.md) - Understand codebase
2. [dev-docs/INDEX.md](dev-docs/INDEX.md) - Development docs
3. [dev-docs/implementation/](dev-docs/implementation/) - Implementation guides
4. [dev-docs/status/](dev-docs/status/) - Current status

---

## 🚀 Quick Start Summary

**Total time: < 5 minutes**

```bash
# 1. Install (1 min)
cd /Users/bengamble/Sabot
pip install -e .

# 2. First example (2 min)
python examples/00_quickstart/hello_sabot.py

# 3. Second example (2 min)
python examples/00_quickstart/filter_and_map.py
```

**Next:** Continue with [examples/README.md](examples/README.md) for full learning path.

---

## 📞 Getting Help

- **Examples not working?** See [dev-docs/status/IMPLEMENTATION_SUMMARY.md](dev-docs/status/IMPLEMENTATION_SUMMARY.md)
- **Questions?** File issue at https://github.com/sabot/sabot/issues
- **Want to contribute?** See [dev-docs/INDEX.md](dev-docs/INDEX.md)

---

## 📌 Key Files

| File | Purpose | Audience |
|------|---------|----------|
| [README.md](README.md) | Project overview | Everyone |
| [QUICKSTART.md](QUICKSTART.md) | 5-minute start | New users |
| [PROJECT_MAP.md](PROJECT_MAP.md) | Codebase map | Contributors |
| [docs/USER_WORKFLOW.md](docs/USER_WORKFLOW.md) | Complete workflow | Users |
| [docs/ARCHITECTURE_OVERVIEW.md](docs/ARCHITECTURE_OVERVIEW.md) | Architecture | Users + Contributors |
| [examples/README.md](examples/README.md) | Learning path | Learners |
| [dev-docs/INDEX.md](dev-docs/INDEX.md) | Dev docs index | Contributors |

---

**Last Updated:** October 8, 2025
**Documentation Version:** 1.0
**Sabot Version:** 0.1.0-alpha
