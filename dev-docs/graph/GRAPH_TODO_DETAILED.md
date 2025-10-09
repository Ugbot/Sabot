# Sabot Graph Detailed TODO List

## Document Progress
- [ ] Phase 2: Traversal Algorithms (50+ tasks)
- [ ] Phase 3: Pattern Matching (30+ tasks)
- [ ] Phase 4: Query Languages (40+ tasks)
- [ ] Phase 5: Temporal Graphs (25+ tasks)
- [ ] Phase 6: Streaming Graphs (30+ tasks)
- [ ] Phase 7: Distributed Graphs (25+ tasks)
- [ ] Phase 8: RDF & Reasoning (35+ tasks)

## How to Use This Document

This document provides granular, actionable tasks for implementing Sabot's graph features. Each task includes:

- **Task ID:** Unique identifier (e.g., 2.1.1)
- **Description:** 2-3 sentences explaining the task
- **Files to Create/Modify:** Specific file paths
- **Dependencies:** Which tasks must complete first
- **Priority:** P0 (critical), P1 (high), P2 (nice-to-have)
- **Estimated Hours:** Time estimate for a senior engineer
- **Implementation Notes:** Key technical details
- **Testing Requirements:** How to verify correctness
- **Performance Targets:** Expected performance metrics

**Status Legend:**
- ⏳ Not started
- 🚧 In progress
- ✅ Complete
- ⛔ Blocked

---

## Phase 2: Traversal Algorithms

### Task 2.1: Implement BFS Kernel

**Goal:** Breadth-first search over CSR adjacency with vectorized operations.

---

#### Task 2.1.1: Create BFS C++ Kernel ⏳
**Description:** Implement level-synchronous BFS in C++ that operates on CSR adjacency structure. Use Arrow arrays for frontier and visited sets. Support single-source and multi-source BFS.

**Files:**
- CREATE: `MarbleDB/include/marble/graph/bfs.h`
- CREATE: `MarbleDB/include/marble/graph/bfs.cpp`

**Dependencies:** Phase 1 complete (CSR adjacency)

**Priority:** P0

**Estimated:** 15 hours

**Implementation Notes:**
- Use two frontier arrays (current, next) to avoid allocations
- Visited set as Arrow boolean array (1 bit per vertex)
- Level-synchronous: Process all vertices at distance d before d+1
- Return distances as Int64Array

**Code Outline:**
```cpp
// bfs.h
class BFS {
public:
    // Single-source BFS
    static Result<shared_ptr<Int64Array>>
    SingleSource(const CSRAdjacency& graph, int64_t source);

    // Multi-source BFS (multiple start vertices)
    static Result<shared_ptr<Int64Array>>
    MultiSource(const CSRAdjacency& graph, const Int64Array& sources);

    // BFS with max depth limit
    static Result<shared_ptr<Int64Array>>
    WithMaxDepth(const CSRAdjacency& graph, int64_t source, int64_t max_depth);
};
```

**Testing:**
- Unit test: BFS on 10-vertex graph, verify distances
- Unit test: BFS with unreachable vertices (distance = -1)
- Unit test: Multi-source BFS
- Benchmark: 1M vertex graph, source = 0, measure traversal rate

**Performance Target:** 10-50M edges/sec traversal rate

---

#### Task 2.1.2: Create BFS Cython Bindings ⏳
**Description:** Wrap BFS C++ kernel in Cython for Python access. Support both single-source and multi-source BFS. Return Arrow Int64Array with distances.

**Files:**
- CREATE: `sabot/_cython/graph/traversal/bfs.pxd`
- CREATE: `sabot/_cython/graph/traversal/bfs.pyx`

**Dependencies:** Task 2.1.1

**Priority:** P0

**Estimated:** 8 hours

**Implementation Notes:**
- Cython class PyBFS wrapping C++ BFS class
- Accept PyPropertyGraph or PyCSRAdjacency as input
- Return distances as Arrow Int64Array
- Handle errors (invalid source vertex, etc.)

**Code Outline:**
```cython
# bfs.pyx
cdef class PyBFS:
    @staticmethod
    def single_source(PyCSRAdjacency graph, int64_t source):
        cdef shared_ptr[CInt64Array] result
        result = BFS.SingleSource(graph.c_csr.get()[0], source)
        return ca.pyarrow_wrap_array(result)

    @staticmethod
    def multi_source(PyCSRAdjacency graph, ca.Int64Array sources):
        # Similar implementation
        pass
```

**Testing:**
- Test same graphs as C++ tests
- Test error handling (invalid source)
- Test integration with PropertyGraph

**Performance Target:** <1ms overhead vs C++ implementation

---

#### Task 2.1.3: Implement K-Hop Neighbors ⏳
**Description:** Return all vertices reachable within K hops from source. Use BFS with max depth = K. Return set of vertex IDs.

**Files:**
- MODIFY: `MarbleDB/include/marble/graph/bfs.h`
- MODIFY: `MarbleDB/include/marble/graph/bfs.cpp`

**Dependencies:** Task 2.1.1

**Priority:** P0

**Estimated:** 6 hours

**Implementation Notes:**
- BFS with early termination at depth K
- Return array of vertex IDs (not distances)
- Deduplicate results (same vertex reachable via multiple paths)

**Code Outline:**
```cpp
static Result<shared_ptr<Int64Array>>
KHopNeighbors(const CSRAdjacency& graph, int64_t source, int64_t k);
```

**Testing:**
- Unit test: 2-hop neighbors on small graph
- Unit test: 5-hop neighbors
- Edge case: k=0 (returns only source)
- Edge case: k > diameter (returns all reachable)

**Performance Target:** <10ms for k=3 on 100K vertex graph

---

[Continue with remaining 47 tasks for Phase 2...]

---

## Phase 3: Pattern Matching

[30+ tasks...]

---

## Phase 4: Query Languages

[40+ tasks...]

---

## Phase 5: Temporal Graphs

[25+ tasks...]

---

## Phase 6: Streaming Graphs

[30+ tasks...]

---

## Phase 7: Distributed Graphs

[25+ tasks...]

---

## Phase 8: RDF & Reasoning

[35+ tasks...]

---

**Document Status:** In Progress (Structure created)
**Last Updated:** October 2025
**Total Tasks:** 235+ (will exceed 200 when complete)
**Next:** Fill in all remaining tasks with same level of detail