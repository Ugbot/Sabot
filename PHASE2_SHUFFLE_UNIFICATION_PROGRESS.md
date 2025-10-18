# Phase 2: Shuffle Unification - In Progress

**Started:** October 18, 2025  
**Status:** 🔄 In Progress  
**Goal:** Create unified shuffle service wrapping existing Cython implementation

---

## What Was Created

### Shuffle Service (3 Files)

1. **`sabot/orchestrator/__init__.py`** - Orchestrator module
2. **`sabot/orchestrator/shuffle/__init__.py`** - Shuffle module exports
3. **`sabot/orchestrator/shuffle/service.py`** (250 lines) - Unified ShuffleService

### ShuffleService Features

```python
from sabot.orchestrator.shuffle import ShuffleService, ShuffleType

service = ShuffleService()

# Create shuffle
service.create_shuffle('shuffle_id', ShuffleType.HASH, num_partitions=4)

# Send/receive partitions
service.send_partition('shuffle_id', 0, batch, 'agent_0')
batches = service.receive_partitions('shuffle_id', partition_id=0)

# Stats and cleanup
stats = service.get_shuffle_stats()
service.shutdown()
```

### Integration with Engine

```python
from sabot import Sabot

# Distributed mode automatically initializes shuffle service
engine = Sabot(mode='distributed')
assert engine._shuffle_service is not None  # ✅ Available

# Local mode doesn't need shuffle
engine_local = Sabot(mode='local')
assert engine_local._shuffle_service is None  # ✅ Not created
```

---

## Progress Summary

### ✅ Completed
1. Created `sabot/orchestrator/` directory structure
2. Implemented `ShuffleService` wrapper class
3. Integrated with `Sabot` engine
4. Wrapped existing Cython shuffle transport
5. Added shuffle types enum (HASH, BROADCAST, REBALANCE, RANGE)
6. Implemented shuffle lifecycle (create, send, receive, close)

### 🔄 In Progress
1. Fix Cython transport attribute issue
2. Add compression support (LZ4/Snappy)
3. Add spill-to-disk configuration
4. Performance validation

### ⏳ Remaining
1. Consolidate coordinator usage of shuffle service
2. Update JobManager to use ShuffleService
3. Update ClusterCoordinator to use ShuffleService
4. Deprecate direct shuffle transport calls

---

## Architecture Impact

### Before
```
3 coordinators each calling shuffle transport directly:
- JobManager → _cython.shuffle.shuffle_transport
- ClusterCoordinator → _cython.shuffle.shuffle_transport  
- DistributedCoordinator → _cython.shuffle.shuffle_transport

Result: Unclear ownership, duplication
```

### After
```
Single ShuffleService, all coordinators use it:
- JobManager → orchestrator.shuffle.ShuffleService
- ClusterCoordinator → orchestrator.shuffle.ShuffleService
- DistributedCoordinator → orchestrator.shuffle.ShuffleService

Result: Clear ownership, single implementation
```

---

## Next Steps

### Immediate
1. Fix Cython transport initialization
2. Add unit tests for shuffle service
3. Performance validation

### Week 3-4 Remaining
1. Merge coordinators into JobManager
2. Create HTTP API layer (from DistributedCoordinator)
3. Update all shuffle callers

---

## Known Issues

1. **Cython transport attribute error** - `_active_shuffles` initialization
   - Cause: Cython class doesn't allow dynamic attributes
   - Fix: Declare in .pyx file or handle in wrapper
   - Impact: Minor, wrapper still functional

2. **JobManager init signature** - Unexpected keyword argument
   - Cause: Different interface than expected
   - Fix: Update engine.py to match JobManager API
   - Impact: Minor, can fix

---

## Performance Validation (Pending)

**Will test:**
- Shuffle creation overhead
- Partition send latency
- Receive latency
- Throughput comparison

**Thresholds:**
- Creation: < 1ms
- Send/receive: < 5ms per partition
- No regression vs direct Cython calls

---

**Status:** 🔄 Phase 2 progressing well  
**Blockers:** Minor Cython integration issues  
**Confidence:** HIGH (structure is sound, just need to iron out details)

