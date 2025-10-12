# QLever vs SabotQL Feature Comparison

**Date:** October 12, 2025
**Purpose:** Identify missing features and opportunities for code reuse

## Overview

**QLever:**
- Mature SPARQL 1.1 engine (10+ years development)
- ~150+ C++ source files
- Full ANTLR4-based SPARQL parser
- Production-grade query optimizer
- Advanced features: Property paths, SPARQL Update, full-text search, spatial queries

**SabotQL:**
- New Arrow-native SPARQL engine (Phase 4)
- ~31 C++ source files (10 in SPARQL module)
- Programmatic query builder (no text parser yet)
- Basic query planner with join optimization
- Core features: SELECT, BGP, FILTER, DISTINCT, LIMIT

---

## Major Feature Gaps

### 1. SPARQL Text Parser ❌ **CRITICAL GAP**

**QLever Has:**
```cpp
// src/parser/SparqlParser.h
class SparqlParser {
  static ParsedQuery parseQuery(
      const EncodedIriManager* encodedIriManager,
      std::string query,
      const std::vector<DatasetClause>& datasets = {});
};
```
- Full ANTLR4-based SPARQL 1.1 grammar
- Supports complete SPARQL syntax
- Error messages for unsupported features
- ~60 parser source files

**SabotQL Has:**
- ❌ No text parser
- ✅ Complete AST (ast.h)
- ✅ SPARQLBuilder fluent API (programmatic only)
- Must build queries manually

**Priority:** 🔴 **P0 - Blocking production use**

**Options:**
1. Use ANTLR4 with official SPARQL grammar (recommended)
2. Borrow QLever's ANTLR4 parser (GPL license issue?)
3. Hand-written recursive descent parser (high effort)

---

### 2. Advanced SPARQL Features

#### Property Paths ❌ **HIGH PRIORITY**

**QLever Has:**
```
src/parser/PropertyPath.h
src/engine/PathSearch.h
src/engine/TransitivePathBase.h
src/engine/TransitivePathBinSearch.h
src/engine/TransitivePathHashMap.h
```

**Property path operators:**
- `/` - Sequence (A/B)
- `|` - Alternative (A|B)
- `*` - Zero or more (A*)
- `+` - One or more (A+)
- `?` - Zero or one (A?)
- `^` - Inverse (^A)
- `!` - Negation (!A)

**Use cases:**
```sparql
# Find all ancestors (transitive)
SELECT ?person ?ancestor WHERE {
  ?person foaf:knows+ ?ancestor .
}

# Find reachable nodes within 3 hops
SELECT ?x ?y WHERE {
  ?x foaf:knows{1,3} ?y .
}
```

**SabotQL Status:**
- ❌ No property path support
- ❌ No transitive closure operators

**Priority:** 🟡 **P1 - Important for graph queries**

---

#### SPARQL Update (INSERT/DELETE) ❌

**QLever Has:**
```
src/parser/UpdateClause.h
src/parser/UpdateTriples.h
src/engine/ExecuteUpdate.h
src/engine/Load.h
```

**SPARQL Update operations:**
- `INSERT DATA` - Insert triples
- `DELETE DATA` - Delete triples
- `DELETE WHERE` - Delete matching triples
- `INSERT/DELETE` - Combined operations
- `LOAD` - Load RDF from URL
- `CLEAR` - Clear graph
- `CREATE/DROP` - Manage graphs

**SabotQL Status:**
- ❌ Read-only (SELECT queries only)
- ❌ No update support

**Priority:** 🟡 **P1 - Needed for writable applications**

---

#### SERVICE (Federated Queries) ❌

**QLever Has:**
```
src/engine/Service.h
src/parser/MagicServiceQuery.h
```

**Federated SPARQL:**
```sparql
SELECT ?person ?name WHERE {
  ?person a :Person .
  SERVICE <http://dbpedia.org/sparql> {
    ?person foaf:name ?name .
  }
}
```

**SabotQL Status:**
- ❌ No federated query support

**Priority:** 🟢 **P2 - Nice to have**

---

#### VALUES Clause ❌

**QLever Has:**
```
src/engine/Values.h
```

**VALUES clause:**
```sparql
SELECT ?person ?name WHERE {
  VALUES (?person) { (<http://example.org/Alice>) (<http://example.org/Bob>) }
  ?person :name ?name .
}
```

**SabotQL Status:**
- ❌ No VALUES support

**Priority:** 🟡 **P1 - Useful for parameterized queries**

---

#### BIND Clause ❌

**QLever Has:**
```
src/engine/Bind.h
```

**BIND clause:**
```sparql
SELECT ?person ?fullName WHERE {
  ?person :firstName ?first ; :lastName ?last .
  BIND(CONCAT(?first, " ", ?last) AS ?fullName)
}
```

**SabotQL Status:**
- ❌ No BIND support
- ✅ Expression evaluator exists (could be adapted)

**Priority:** 🟡 **P1 - Common SPARQL pattern**

---

#### MINUS Operator ❌

**QLever Has:**
```
src/engine/Minus.h
src/engine/MinusRowHandler.h
```

**MINUS operator:**
```sparql
SELECT ?person WHERE {
  ?person a :Person .
  MINUS { ?person :banned true }
}
```

**SabotQL Status:**
- ❌ No MINUS support

**Priority:** 🟢 **P2 - Less common**

---

### 3. Query Operators

#### ORDER BY ⚠️ **INTERFACE EXISTS, NEED IMPLEMENTATION**

**QLever Has:**
```cpp
// src/engine/OrderBy.h
class OrderBy : public Operation {
  OrderBy(std::shared_ptr<QueryExecutionTree> subtree,
          std::vector<ColumnIndex> sortedColumns);
  // Full ORDER BY implementation with ASC/DESC
};

// src/engine/Sort.h
class Sort : public Operation {
  // General-purpose sort operator
};

// src/engine/SortPerformanceEstimator.h
class SortPerformanceEstimator {
  // Cost estimation for sorting
};
```

**SabotQL Has:**
- ✅ AST support (OrderBy clause in ast.h)
- ✅ Planner interface (PlanOrderBy() stub)
- ❌ No SortOperator implementation
- ❌ No Arrow SortIndices integration

**Priority:** 🔴 **P0 - Next in Phase 4**

**Estimated Effort:** 200-300 lines
- Create SortOperator class
- Integrate Arrow compute SortIndices
- Support ASC/DESC
- Support multiple sort keys
- Cost estimation

---

#### UNION ❌

**QLever Has:**
```cpp
// src/engine/Union.h
class Union : public Operation {
  Union(std::shared_ptr<QueryExecutionTree> left,
        std::shared_ptr<QueryExecutionTree> right,
        std::vector<std::array<ColumnIndex, 2>> columnOrigins);
};

// src/engine/SortedUnionImpl.h
// Optimized sorted union for pre-sorted inputs
```

**SabotQL Has:**
- ✅ AST support (UnionPattern in ast.h)
- ✅ Planner interface (PlanUnion() stub)
- ❌ No UnionOperator implementation

**Priority:** 🟡 **P1 - Common SPARQL pattern**

**Estimated Effort:** 300-400 lines
- Create UnionOperator
- Schema unification
- Deduplication logic
- Sorted union optimization

---

#### OPTIONAL (Left Outer Join) ❌

**QLever Has:**
```cpp
// src/engine/OptionalJoin.h
class OptionalJoin : public Operation {
  OptionalJoin(std::shared_ptr<QueryExecutionTree> left,
               std::shared_ptr<QueryExecutionTree> right,
               std::vector<ColumnIndex> leftJoinColumns,
               std::vector<ColumnIndex> rightJoinColumns);
};

// src/engine/NeutralOptional.h
// Optimization for OPTIONAL patterns that can be ignored
```

**SabotQL Has:**
- ✅ AST support (OptionalPattern in ast.h)
- ✅ Planner interface (PlanOptional() stub)
- ✅ HashJoinOperator (inner join only)
- ❌ No left outer join support

**Priority:** 🟡 **P1 - Important for optional data**

**Estimated Effort:** 400-500 lines
- Extend HashJoinOperator for LEFT OUTER JOIN
- Handle UNDEF values (nulls in Arrow)
- Update planner to create left outer joins

---

#### Cartesian Product Join ❌

**QLever Has:**
```cpp
// src/engine/CartesianProductJoin.h
class CartesianProductJoin : public Operation {
  // For joins with no shared variables
};
```

**SabotQL Has:**
- ❌ Returns error for cross products
- Only hash join with shared variables

**Priority:** 🟢 **P2 - Less common, usually unintentional**

---

#### Multi-Column Join ⚠️ **PARTIAL SUPPORT**

**QLever Has:**
```cpp
// src/engine/MultiColumnJoin.h
class MultiColumnJoin : public Operation {
  // Optimized joins on multiple columns
};
```

**SabotQL Has:**
- ✅ HashJoinOperator supports multiple keys
- ⚠️ Limited optimization for multi-column joins

**Priority:** 🟢 **P2 - Optimization opportunity**

---

### 4. Aggregation Features

#### GROUP BY ⚠️ **OPERATOR EXISTS, NOT WIRED TO SPARQL**

**QLever Has:**
```cpp
// src/engine/GroupBy.h
class GroupBy : public Operation {
  GroupBy(ParsedQuery::GroupByClause groupByClause,
          std::vector<Alias> aliases);
};

// src/engine/GroupByHashMapOptimization.h
// Hash-based GROUP BY optimization

// src/engine/LazyGroupBy.h
// Lazy evaluation for GROUP BY
```

**SabotQL Has:**
- ✅ GroupByOperator exists (operators/aggregate.h)
- ✅ AggregateOperator exists (COUNT, SUM, AVG, etc.)
- ❌ Not wired to SPARQL parser/planner

**Priority:** 🟡 **P1 - Common SPARQL pattern**

**Estimated Effort:** 200-300 lines
- Wire GROUP BY AST → GroupByOperator
- Wire aggregate functions → AggregateOperator
- Support GROUP BY expressions
- HAVING clause support (uses FILTER)

---

#### HAVING Clause ❌

**QLever Has:**
- Full HAVING support (filters after GROUP BY)
- Parsed in ParsedQuery::_havingClauses

**SabotQL Has:**
- ❌ No HAVING support
- ✅ Could use FilterOperator after GroupByOperator

**Priority:** 🟢 **P2 - Depends on GROUP BY**

---

### 5. Full-Text Search ❌

**QLever Has:**
```
src/index/FTSAlgorithms.h
src/index/TextIndexBuilder.h
src/engine/TextIndexScanForWord.h
src/engine/TextIndexScanForEntity.h
src/engine/TextLimit.h
src/parser/TextSearchQuery.h
```

**Full-text search features:**
- Inverted text index
- Text search operators
- Ranking/scoring
- Prefix matching
- Text snippets

**Example:**
```sparql
SELECT ?entity ?text WHERE {
  ?entity ql:contains-word "neural network" .
  ?entity rdfs:label ?text .
}
```

**SabotQL Status:**
- ❌ No full-text search
- Only exact triple matching

**Priority:** 🟢 **P2 - Advanced feature**

---

### 6. Spatial Queries ❌

**QLever Has:**
```
src/engine/SpatialJoin.h
src/engine/SpatialJoinAlgorithms.h
src/parser/SpatialQuery.h
```

**Spatial features:**
- Geometric shapes (points, polygons)
- Distance calculations
- Spatial joins (within, intersects)

**Example:**
```sparql
SELECT ?place WHERE {
  ?place geo:asWKT ?geo .
  FILTER (ql:distance(?geo, "POINT(10 20)") < 1000)
}
```

**SabotQL Status:**
- ❌ No spatial query support

**Priority:** 🟢 **P3 - Niche feature**

---

### 7. Storage & Indexing

#### Vocabulary Management ⚠️ **DIFFERENT APPROACH**

**QLever Has:**
```cpp
// src/index/Vocabulary.h
class Vocabulary {
  // Prefix-compressed string storage
  // Binary search on compressed strings
  // LRU cache for lookups
};

// src/index/VocabularyMerger.h
// Merge vocabularies from multiple sources

// src/index/LocalVocabEntry.h
// Local vocabulary for query-time terms
```

**SabotQL Has:**
```cpp
// include/sabot_ql/storage/vocabulary.h
class Vocabulary {
  // MarbleDB-backed vocabulary
  // LRU cache (10K entries)
  // ValueId encoding (2 type bits + 62-bit ID)
};
```

**Key Differences:**
- QLever: Prefix-compressed, binary search
- SabotQL: MarbleDB KV store, LRU cache

**Priority:** ✅ **Already implemented, different approach**

---

#### Triple Store Indexes ⚠️ **PARTIAL**

**QLever Has:**
```
src/index/Permutation.h  - All 6 permutations (SPO, SOP, PSO, POS, OSP, OPS)
src/index/CompressedRelation.h  - Compressed triple storage
src/index/MetaDataHandler.h  - Index metadata
```

**6 permutations for optimal query performance:**
- SPO - Subject-Predicate-Object
- SOP - Subject-Object-Predicate
- PSO - Predicate-Subject-Object
- POS - Predicate-Object-Subject
- OSP - Object-Subject-Predicate
- OPS - Object-Predicate-Subject

**SabotQL Has:**
```cpp
// include/sabot_ql/storage/triple_store.h
enum class IndexType {
  SPO,  // Subject-Predicate-Object
  POS,  // Predicate-Object-Subject
  OSP   // Object-Subject-Predicate
};
```

**Only 3 permutations:**
- SPO, POS, OSP

**Priority:** 🟡 **P1 - Missing 3 permutations hurts some queries**

---

#### Delta Triples (Incremental Updates) ❌

**QLever Has:**
```
src/index/DeltaTriples.h
src/index/LocatedTriples.h
```

**Delta triples:**
- Incremental index updates
- No full rebuild required
- In-memory overlay on disk index

**SabotQL Status:**
- ❌ No delta triple support
- Full rebuild required for updates

**Priority:** 🟢 **P2 - Optimization for updates**

---

### 8. Query Optimization

#### Query Planner ⚠️ **BASIC VERSION**

**QLever Has:**
```cpp
// src/engine/QueryPlanner.h
class QueryPlanner {
  // Dynamic programming join ordering
  // Cost-based optimization
  // Filter pushdown
  // Index selection
  // Prefilter optimization
  // Subquery optimization
};

// src/engine/QueryPlanningCostFactors.h
// Configurable cost model
```

**Advanced optimizations:**
- Dynamic programming for join order (not just greedy)
- Prefilter pushdown to IndexScan
- Cardinality estimation from index stats
- Subquery flattening
- OPTIONAL placement optimization

**SabotQL Has:**
```cpp
// include/sabot_ql/sparql/planner.h
class QueryPlanner {
  // Basic join planning
  // Greedy join ordering (smallest cardinality first)
  // Simple cardinality estimation
};

class QueryOptimizer {
  // Basic BGP reordering
  // Join variable detection
};
```

**Priority:** 🟡 **P1 - Optimization opportunity**

**Gap:** Dynamic programming vs greedy, filter pushdown, prefilter optimization

---

#### Filter Pushdown ❌

**QLever Has:**
- Pushes FILTER predicates down to IndexScan
- Reduces intermediate result size
- Significant performance gain (10-100x for selective filters)

**SabotQL Has:**
- ❌ Filters applied after join
- No filter pushdown optimization

**Priority:** 🟡 **P1 - Major performance optimization**

---

#### Prefilter Expressions ❌

**QLever Has:**
```cpp
// Prefilter optimization for expressions
virtual std::optional<std::shared_ptr<QueryExecutionTree>>
setPrefilterGetUpdatedQueryExecutionTree(
    const std::vector<PrefilterVariablePair>& prefilterPairs) const;
```

**Prefilter:**
- Evaluate filter predicates during index scan
- Avoids materializing filtered-out rows

**SabotQL Status:**
- ❌ No prefilter support

**Priority:** 🟡 **P1 - Performance optimization**

---

### 9. Execution Engine Features

#### Lazy Evaluation ❌

**QLever Has:**
```cpp
enum class ComputationMode {
  FULLY_MATERIALIZED,
  ONLY_IF_CACHED,
  LAZY_IF_SUPPORTED
};

// src/engine/LazyGroupBy.h
class LazyGroupBy : public Operation {
  // Generator-based lazy evaluation
};
```

**Lazy evaluation:**
- Process results incrementally
- Reduce memory usage
- Early termination with LIMIT

**SabotQL Has:**
- ❌ Fully materialized results only
- ✅ Batch-oriented processing (Arrow batches)

**Priority:** 🟢 **P2 - Memory optimization**

---

#### Result Caching ❌

**QLever Has:**
```cpp
// src/engine/NamedResultCache.h
class NamedResultCache {
  // Cache query results by cache key
  // LRU eviction
  // Pinned results (for subqueries)
};
```

**SabotQL Has:**
- ❌ No query result caching
- Only vocabulary LRU cache

**Priority:** 🟢 **P2 - Performance optimization**

---

#### Query Cancellation ❌

**QLever Has:**
```cpp
// util/CancellationHandle.h
class CancellationHandle {
  // Thread-safe cancellation
  // Propagates to all child operations
};

// In Operation.h:
AD_ALWAYS_INLINE void checkCancellation() const {
  cancellationHandle_->throwIfCancelled(location,
                                        [this]() { return getDescriptor(); });
}
```

**SabotQL Has:**
- ❌ No query cancellation
- Long queries cannot be interrupted

**Priority:** 🟡 **P1 - Production requirement**

---

#### Query Timeouts ❌

**QLever Has:**
```cpp
void recursivelySetTimeConstraint(
    std::chrono::duration<Rep, Period> duration);

std::chrono::milliseconds remainingTime() const;
```

**SabotQL Has:**
- ❌ No query timeouts

**Priority:** 🟡 **P1 - Production requirement**

---

### 10. Server & Protocol

#### SPARQL Protocol ❌

**QLever Has:**
```
src/engine/SparqlProtocol.h
src/engine/Server.h
src/engine/ParsedRequestBuilder.h
```

**SPARQL 1.1 Protocol:**
- HTTP endpoint (/sparql)
- Content negotiation (JSON, XML, TSV, CSV)
- Accept header handling
- Error responses (400, 500)

**SabotQL Has:**
- ❌ No HTTP server
- ❌ No SPARQL protocol endpoint

**Priority:** 🟡 **P1 - Needed for web access**

---

#### Graph Store Protocol ❌

**QLever Has:**
```
src/engine/GraphStoreProtocol.h
```

**Graph Store Protocol:**
- REST API for graph management
- GET/PUT/DELETE/POST operations
- Named graph management

**SabotQL Has:**
- ❌ No Graph Store Protocol

**Priority:** 🟢 **P2 - Less critical**

---

### 11. Monitoring & Observability

#### Runtime Information ❌

**QLever Has:**
```cpp
// src/engine/RuntimeInformation.h
struct RuntimeInformation {
  size_t numRows_;
  size_t numCols_;
  std::chrono::microseconds totalTime_;
  ad_utility::CacheStatus cacheStatus_;
  std::vector<std::shared_ptr<RuntimeInformation>> children_;
  std::string descriptor_;

  std::string toString() const;  // Human-readable output
};
```

**Detailed query profiling:**
- Per-operator timings
- Cache hit/miss stats
- Row/column counts
- Memory usage
- Tree visualization

**SabotQL Has:**
```cpp
// include/sabot_ql/operators/operator.h
struct OperatorStats {
  size_t rows_processed = 0;
  size_t batches_processed = 0;
  size_t bytes_processed = 0;
  double execution_time_ms = 0.0;
};
```

**Basic stats only:**
- No cache tracking
- No tree visualization
- No detailed profiling

**Priority:** 🟡 **P1 - Critical for debugging/optimization**

---

#### Query Logging ❌

**QLever Has:**
- Query logging with timing
- Error logging
- Access logs

**SabotQL Has:**
- ❌ No query logging

**Priority:** 🟢 **P2 - Operational requirement**

---

## Summary Tables

### Feature Completeness

| Category | QLever | SabotQL | Gap |
|----------|--------|---------|-----|
| **SPARQL Text Parser** | ✅ ANTLR4 | ❌ None | 🔴 **CRITICAL** |
| **Basic SELECT** | ✅ | ✅ | ✅ |
| **Basic Graph Patterns** | ✅ | ✅ | ✅ |
| **FILTER Expressions** | ✅ | ✅ | ✅ |
| **ORDER BY** | ✅ | ⚠️ Interface only | 🔴 **High Priority** |
| **GROUP BY** | ✅ | ⚠️ Operator exists | 🟡 **Medium** |
| **DISTINCT** | ✅ | ✅ | ✅ |
| **UNION** | ✅ | ❌ | 🟡 **Medium** |
| **OPTIONAL** | ✅ | ❌ | 🟡 **Medium** |
| **Property Paths** | ✅ | ❌ | 🟡 **High** |
| **SPARQL Update** | ✅ | ❌ | 🟡 **Medium** |
| **VALUES** | ✅ | ❌ | 🟡 **Medium** |
| **BIND** | ✅ | ❌ | 🟡 **Medium** |
| **MINUS** | ✅ | ❌ | 🟢 **Low** |
| **SERVICE** | ✅ | ❌ | 🟢 **Low** |
| **Full-Text Search** | ✅ | ❌ | 🟢 **Low** |
| **Spatial Queries** | ✅ | ❌ | 🟢 **Low** |
| **Query Cancellation** | ✅ | ❌ | 🟡 **High** |
| **Query Timeouts** | ✅ | ❌ | 🟡 **High** |
| **Result Caching** | ✅ | ❌ | 🟢 **Medium** |
| **SPARQL Protocol** | ✅ | ❌ | 🟡 **High** |

### Operator Completeness

| Operator | QLever | SabotQL | Status |
|----------|--------|---------|--------|
| **IndexScan** | ✅ (6 permutations) | ✅ (3 permutations) | ⚠️ Missing 3 |
| **HashJoin** | ✅ | ✅ | ✅ |
| **OptionalJoin** | ✅ | ❌ | 🔴 Missing |
| **Union** | ✅ | ❌ | 🔴 Missing |
| **Filter** | ✅ | ✅ | ✅ |
| **OrderBy/Sort** | ✅ | ❌ | 🔴 Missing |
| **GroupBy** | ✅ | ⚠️ Not wired | 🟡 Partial |
| **Aggregate** | ✅ | ⚠️ Not wired | 🟡 Partial |
| **Distinct** | ✅ | ✅ | ✅ |
| **Limit** | ✅ | ✅ | ✅ |
| **Project** | ✅ | ✅ | ✅ |
| **CartesianProduct** | ✅ | ❌ | 🟢 Low priority |
| **Bind** | ✅ | ❌ | 🟡 Missing |
| **Values** | ✅ | ❌ | 🟡 Missing |
| **Service** | ✅ | ❌ | 🟢 Low priority |
| **PathSearch** | ✅ | ❌ | 🟡 Missing |
| **TextIndexScan** | ✅ | ❌ | 🟢 Low priority |
| **SpatialJoin** | ✅ | ❌ | 🟢 Low priority |

---

## Phase 4 Priorities (Next Steps)

### 🔴 **P0 - Critical (Blocking Production)**

1. **ORDER BY Implementation** (200-300 lines)
   - Create SortOperator
   - Arrow SortIndices integration
   - ASC/DESC support

2. **SPARQL Text Parser** (Major effort)
   - Option A: ANTLR4 with official grammar
   - Option B: Hand-written recursive descent
   - Option C: Borrow from QLever (license?)

3. **Query Cancellation** (300-400 lines)
   - CancellationHandle implementation
   - Thread-safe cancellation
   - Propagate to all operators

### 🟡 **P1 - High Priority (Core SPARQL)**

4. **UNION Operator** (300-400 lines)
5. **OPTIONAL (Left Outer Join)** (400-500 lines)
6. **Wire GROUP BY to SPARQL** (200-300 lines)
7. **VALUES Clause** (200-300 lines)
8. **BIND Clause** (200-300 lines)
9. **Property Paths** (800-1000 lines)
10. **SPARQL Protocol Server** (500-700 lines)
11. **Enhanced Runtime Information** (300-400 lines)
12. **Query Timeouts** (200 lines)

### 🟢 **P2 - Medium Priority (Optimizations)**

13. **Filter Pushdown** (400-500 lines)
14. **Prefilter Optimization** (300-400 lines)
15. **Result Caching** (400-500 lines)
16. **Additional Index Permutations** (SOP, PSO, OPS)
17. **MINUS Operator** (300-400 lines)
18. **SPARQL Update** (800-1000 lines)
19. **Dynamic Programming Join Ordering**

### 🟢 **P3 - Low Priority (Advanced Features)**

20. **Full-Text Search**
21. **Spatial Queries**
22. **SERVICE (Federated Queries)**
23. **Lazy Evaluation**
24. **Delta Triples**

---

## Code Reuse Opportunities

### Can Borrow from QLever

**License Check Required:** QLever is GPL - may conflict with SabotQL license

**Potential Reuse:**
1. **ANTLR4 SPARQL Grammar** - Parser definition
2. **Cost Model Constants** - Tuned for real workloads
3. **Algorithm Ideas** - Join ordering, filter pushdown strategies
4. **Test Queries** - SPARQL query test suite

**Cannot Directly Use:**
- QLever's storage layer (incompatible with MarbleDB)
- QLever's operator implementations (different architecture)

### Can Leverage QLever Ideas

**Architecture patterns:**
- Operation base class design
- Variable-to-column mapping approach
- Runtime information tree structure
- Cancellation handle pattern

**Optimization strategies:**
- Prefilter pushdown to index scans
- Dynamic programming join ordering
- Filter selectivity estimation

---

## Estimated Effort to Reach QLever Parity

**Core SPARQL 1.1 (P0 + P1):**
- **~6,000-8,000 lines of code**
- **8-12 weeks of development**
- Covers: Parser, ORDER BY, UNION, OPTIONAL, GROUP BY, BIND, VALUES, Property Paths

**Production-Ready (+ P2):**
- **~10,000-12,000 lines of code**
- **16-20 weeks of development**
- Adds: Optimizations, caching, monitoring, SPARQL Update

**Full QLever Parity (+ P3):**
- **~15,000-20,000 lines of code**
- **24-32 weeks of development**
- Adds: Full-text search, spatial queries, federated queries

---

## Recommendations

### Immediate Next Steps (Phase 4 Completion)

1. **Complete ORDER BY** (1-2 days)
   - Highest ROI, smallest effort
   - Makes queries much more useful

2. **Start SPARQL Text Parser** (2-3 weeks)
   - Blocking for all text-based queries
   - ANTLR4 recommended (official grammar)

3. **Implement UNION** (3-4 days)
   - Common SPARQL pattern
   - Relatively straightforward

4. **Add Query Cancellation** (2-3 days)
   - Critical for production
   - Prevents runaway queries

### Medium-Term Goals (Phase 5)

5. **OPTIONAL Support** (5-7 days)
6. **Wire GROUP BY** (3-4 days)
7. **BIND and VALUES** (4-5 days)
8. **SPARQL Protocol Server** (1 week)

### Long-Term Goals (Phase 6+)

9. **Property Paths** (2-3 weeks)
10. **Filter Pushdown Optimization** (1 week)
11. **SPARQL Update** (2-3 weeks)

---

## Conclusion

**Current Status:**
- SabotQL has ~35% of QLever's features
- Core SELECT queries work well
- Missing text parser is critical gap
- Arrow-native architecture is advantage over QLever

**Key Strengths:**
- ✅ Clean Arrow-native design
- ✅ Modern C++20 codebase
- ✅ MarbleDB storage (Raft-enabled)
- ✅ Zero-copy operations

**Key Gaps:**
- ❌ No SPARQL text parser
- ❌ Missing ORDER BY, UNION, OPTIONAL
- ❌ No property paths
- ❌ Limited optimization

**Path Forward:**
Focus on P0/P1 items to reach core SPARQL 1.1 compliance, then optimize.

---

**Next Document:** PHASE5_PLAN.md - ORDER BY and UNION implementation
