# Test Organization - October 2025

## Problem Fixed

**Before:** 32 test files scattered in root directory + more in tests/
**After:** Clean organization with tests/ and benchmarks/ directories

## New Structure

```
sabot/
├── tests/                          # All tests live here
│   ├── conftest.py                # Pytest configuration
│   ├── unit/                      # Unit tests (fast, isolated)
│   │   ├── api/                   # API layer tests
│   │   │   ├── test_api_basic.py
│   │   │   ├── test_api_standalone.py
│   │   │   └── test_stream_api.py
│   │   ├── arrow/                 # Arrow/CyArrow tests
│   │   │   ├── test_arrow_joins_simple.py
│   │   │   ├── test_arrow_windows.py
│   │   │   ├── test_cyarrow_loader.py
│   │   │   └── test_zero_copy.py
│   │   ├── cython/                # Cython module tests
│   │   │   ├── test_cython_aggregations.py
│   │   │   ├── test_cython_arrow_processing.py
│   │   │   ├── test_cython_checkpoint_coordination.py
│   │   │   ├── test_cython_demo.py
│   │   │   ├── test_cython_joins.py
│   │   │   ├── test_cython_operators.py
│   │   │   ├── test_cython_state_primitives.py
│   │   │   └── test_cython_timer_service.py
│   │   ├── state/                 # State backend tests
│   │   │   ├── test_backends_simple.py
│   │   │   ├── test_rocksdb_direct.py
│   │   │   ├── test_rocksdb_python.py
│   │   │   ├── test_rocksdb_state.py
│   │   │   ├── test_state_management.py
│   │   │   ├── test_stores.py
│   │   │   ├── test_tonbo_simple.py
│   │   │   └── test_tonbo_state_backend.py
│   │   ├── test_app.py
│   │   ├── test_app_channels.py
│   │   ├── test_arrow_integration.py
│   │   ├── test_channel_backends.py
│   │   ├── test_channel_manager.py
│   │   ├── test_channels_standalone.py
│   │   ├── test_cli_app_loading.py
│   │   ├── test_cli_basic.py
│   │   ├── test_cython_checkpoint.py
│   │   ├── test_cython_state.py
│   │   ├── test_cython_time.py
│   │   ├── test_joins.py
│   │   ├── test_minimal_components.py
│   │   ├── test_raft.py
│   │   └── test_windows_simple.py
│   └── integration/               # Integration tests (slower, E2E)
│       ├── test_all_examples.py
│       ├── test_channel_integration.py
│       ├── test_channels_comprehensive.py
│       ├── test_channels_e2e.py
│       ├── test_cli_fraud_demo.py
│       ├── test_cluster.py
│       ├── test_core_functionality.py
│       ├── test_example_basic.py
│       ├── test_full_integration.py
│       ├── test_install.py
│       ├── test_job_graph_execution.py
│       ├── test_kafka_integration.py
│       ├── test_monitoring.py
│       ├── test_raft_integration.py
│       ├── test_real_agent_runtime.py
│       ├── test_real_stream_engine.py
│       ├── test_stream_demo.py
│       ├── test_what_works.py
│       └── test_windowed_demo.py
└── benchmarks/                    # Performance benchmarks
    ├── __init__.py
    ├── benchmark_extreme.py
    ├── benchmark_peru_transactions.py
    ├── cluster_benchmarks.py
    ├── join_benchmarks.py
    ├── memory_benchmarks.py
    ├── runner.py
    ├── state_benchmarks.py
    ├── stream_benchmarks.py
    ├── test_arrow_performance.py
    ├── test_channel_performance.py
    └── test_fraud_throughput.py
```

## Organization Principles

### Unit Tests (`tests/unit/`)
- **Fast** - Run in milliseconds
- **Isolated** - No external dependencies (Kafka, Redis, etc.)
- **Focused** - Test single components

**Subdirectories:**
- `api/` - Stream API, decorators, user-facing interfaces
- `arrow/` - CyArrow operations, zero-copy, Arrow IPC
- `cython/` - Cython modules (checkpoint, state, time, operators)
- `state/` - State backends (Memory, RocksDB, Tonbo)

### Integration Tests (`tests/integration/`)
- **Slower** - May take seconds
- **Multi-component** - Test interactions between modules
- **External deps** - May use Kafka, Redis, etc.

**Examples:**
- End-to-end stream processing
- CLI execution
- Kafka integration
- Agent runtime
- Cluster coordination

### Benchmarks (`benchmarks/`)
- **Performance focused** - Measure throughput, latency
- **Not tests** - Don't assert correctness, measure speed
- **Repeatable** - Can run multiple times for comparison

**Examples:**
- Fraud detection throughput (143K-260K txn/s)
- Arrow performance (104M rows/sec joins)
- State backend operations (1M+ ops/sec)
- Channel performance
- Cluster benchmarks

## Running Tests

### All Tests
```bash
pytest tests/
```

### Unit Tests Only (Fast)
```bash
pytest tests/unit/
```

### Integration Tests Only
```bash
pytest tests/integration/
```

### Specific Module
```bash
pytest tests/unit/cython/          # All Cython tests
pytest tests/unit/arrow/           # All Arrow tests
pytest tests/unit/state/           # All state backend tests
```

### Benchmarks
```bash
python benchmarks/test_fraud_throughput.py
python benchmarks/test_arrow_performance.py
python benchmarks/runner.py        # Run all benchmarks
```

## Files Moved (32 from root → organized)

### From Root to tests/unit/cython/ (8 files)
- test_cython_timer_service.py
- test_cython_state_primitives.py
- test_cython_checkpoint_coordination.py
- test_cython_arrow_processing.py
- test_cython_operators.py
- test_cython_aggregations.py
- test_cython_joins.py
- test_cython_demo.py

### From Root to tests/unit/state/ (6 files)
- test_state_management.py
- test_rocksdb_state.py
- test_rocksdb_python.py
- test_rocksdb_direct.py
- test_tonbo_state_backend.py
- test_tonbo_simple.py

### From Root to tests/unit/arrow/ (3 files)
- test_arrow_windows.py
- test_zero_copy.py
- test_cyarrow_loader.py

### From Root to tests/unit/api/ (3 files)
- test_api_basic.py
- test_api_standalone.py
- test_stream_api.py

### From Root to tests/integration/ (11 files)
- test_real_stream_engine.py
- test_real_agent_runtime.py
- test_all_examples.py
- test_example_basic.py
- test_stream_demo.py
- test_windowed_demo.py
- test_monitoring.py
- test_cluster.py
- test_job_graph_execution.py
- test_install.py
- test_what_works.py

### From Root to benchmarks/ (4 files)
- test_arrow_performance.py
- test_fraud_throughput.py
- benchmark_peru_transactions.py (already existed)
- benchmark_extreme.py (already existed)

### From tests/ to Subdirectories (9 files)
- test_backends_simple.py → tests/unit/state/
- test_raft_integration.py → tests/integration/
- test_channels_comprehensive.py → tests/integration/
- test_full_integration.py → tests/integration/
- test_minimal_components.py → tests/unit/
- test_channels_standalone.py → tests/unit/
- test_channel_manager.py → tests/unit/
- test_channel_backends.py → tests/unit/
- test_cli_basic.py → tests/unit/
- test_windows_simple.py → tests/unit/
- test_channel_performance.py → benchmarks/
- test_channel_integration.py → tests/integration/
- test_core_functionality.py → tests/integration/
- test_kafka_integration.py → tests/integration/
- test_app.py → tests/unit/
- test_joins.py → tests/unit/
- test_raft.py → tests/unit/
- test_arrow_joins_simple.py → tests/unit/arrow/
- test_stores.py → tests/unit/state/

## Benefits

1. **Clean Root Directory**
   - No test clutter in project root
   - Easy to find actual project files

2. **Fast CI/CD**
   - Run unit tests first (fast feedback)
   - Run integration tests separately (slower)
   - Skip benchmarks in CI (run manually)

3. **Clear Test Types**
   - Unit vs integration obvious from location
   - Benchmarks clearly separated

4. **Better Organization**
   - Related tests grouped together
   - Easy to find tests for specific modules

5. **Scalable**
   - Can add more subdirectories as needed
   - Clear pattern to follow

## Git History Preserved

All moves done with `git mv` to preserve file history.

## Next Steps

1. ✅ **Organization complete** - All tests in proper locations
2. ⏭️ **Add __init__.py** - Make subdirectories proper Python packages
3. ⏭️ **Update CI** - Configure pytest to run unit/integration separately
4. ⏭️ **Coverage Reports** - Set up pytest-cov for each directory
5. ⏭️ **README Updates** - Document testing structure in main README

---

**Organized:** October 3, 2025
**Total Files Moved:** 51 files (32 from root + 19 from tests/)
**Result:** Clean project layout with proper test hierarchy
