# sabot_core - C++ Performance Library

**High-performance C++ components for Sabot's hot execution paths.**

## Overview

`sabot_core` is a C++ library containing performance-critical components that would be too slow in Python:

- **Query Optimizer** - 10-50x faster query planning
- **Logical Plans** - Better cache locality, faster traversal
- **Physical Plans** - Efficient operator instantiation  
- **Task Scheduler** - Sub-microsecond scheduling decisions
- **Shuffle Manager** - Lower-latency coordination

## Architecture

```
sabot_core/
├── include/sabot/
│   ├── query/
│   │   ├── logical_plan.h    # Logical plan nodes
│   │   └── optimizer.h       # Query optimizer
│   ├── execution/
│   │   └── scheduler.h       # Task scheduler (future)
│   └── shuffle/
│       └── manager.h         # Shuffle coordinator (future)
├── src/
│   ├── query/
│   │   ├── optimizer.cpp     # Optimizer implementation
│   │   ├── logical_plan.cpp  # Plan implementations
│   │   └── rules.cpp         # Optimization rules
│   └── ...
└── CMakeLists.txt           # Build system
```

## Building

```bash
cd sabot_core
mkdir build && cd build
cmake ..
make -j$(nproc)
```

## Integration with Python

Via Cython bindings:

```python
# Python API (sabot/query/_cython/optimizer.pyx)
from sabot_core.query cimport CppQueryOptimizer

cdef class QueryOptimizer:
    cdef CppQueryOptimizer* _optimizer
    
    def optimize(self, plan):
        # Zero-copy call to C++
        cdef CppLogicalPlan* optimized = self._optimizer.Optimize(plan)
        return wrap_plan(optimized)
```

## Performance Targets

| Component | Python | C++ | Speedup |
|-----------|--------|-----|---------|
| Query optimization | ~50ms | < 5ms | 10x |
| Plan validation | ~1ms | < 100μs | 10x |
| Join reordering | ~10ms | < 1ms | 10x |

## Design Principles

1. **Zero-copy:** All Arrow data as pointers
2. **Cache-friendly:** Compact memory layout
3. **SIMD-ready:** Vectorizable where possible
4. **Minimal allocations:** Object pooling
5. **Clean API:** Easy to wrap in Cython

## Status

**Phase 1:** ✅ Structure created  
**Phase 2:** 🔄 Core components implemented  
**Phase 3:** ⏳ Cython bindings  
**Phase 4:** ⏳ Integration with Sabot

**Current:** Foundation complete, ready for implementation

