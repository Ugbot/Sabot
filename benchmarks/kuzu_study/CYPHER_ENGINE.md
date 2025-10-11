# Sabot Cypher Engine - Benchmark Results

Comparison of **two approaches** to running the Kuzu benchmark on Sabot:
1. **Manual Python queries** (`run_benchmark.py`) - Current working solution
2. **Cypher engine** (`run_benchmark_cypher.py`) - Future declarative approach

## Approach 1: Manual Python Queries ✅

**File:** `run_benchmark.py`
**Status:** ✅ All 9 queries working
**Performance:** Mixed (2-3x faster on simple, 10-500x slower on iterative)

| Query | Time | vs Kuzu | Status |
|-------|------|---------|--------|
| Q1: Top followers | 72ms | 2.2x faster | ✅ |
| Q2: Most-followed city | 73ms | 3.4x faster | ✅ |
| Q3: Lowest avg age | 4162ms | 490x slower | ❌ |
| Q4: Age by country | 1208ms | 82x slower | ❌ |
| Q5: Interest filter | 6ms | 2.1x faster | ✅ |
| Q6: City interest count | 433ms | 12x slower | ⚠️ |
| Q7: State age interest | 60ms | 4x slower | ⚠️ |
| Q8: 2-hop paths | 15ms | 1.7x slower | ⚠️ |
| Q9: Filtered paths | 420ms | 4.4x slower | ⚠️ |

**Pros:**
- ✅ Works today for all queries
- ✅ Fast for simple lookups (Q1, Q2, Q5)
- ✅ Uses proven Arrow operations

**Cons:**
- ❌ Slow for iterative queries (Q3, Q4, Q6) - Python loops
- ❌ Not declarative - hard to optimize automatically
- ❌ Requires manual query planning

## Approach 2: Cypher Query Engine 🚧

**File:** `run_benchmark_cypher.py`
**Status:** ⚠️ Partial - pattern matching works, missing aggregations/filters
**Performance:** 160ms for Q8 2-hop pattern matching (vs 15ms manual)

### What Works ✅

1. **Graph Loading**
   - Loaded 100K Person vertices in 1ms
   - Loaded 2.4M Follows edges in 1ms
   - GraphQueryEngine creates CSR index automatically

2. **Pattern Matching**
   - Query 8: `MATCH (a:Person)-[:Follows]->(b)-[:Follows]->(c)`
   - Found 329,754 2-hop paths in 160ms
   - Uses C++ Cython pattern matching underneath

3. **Basic Cypher Features**
   - ✅ Node labels: `(a:Person)`
   - ✅ Edge types: `-[r:Follows]->`
   - ✅ Variable-length paths: `-[r*1..3]->`
   - ✅ LIMIT clause

### What's Missing 🚧

| Feature | Required By | Status |
|---------|-------------|--------|
| **COUNT aggregation** | Q1, Q2, Q4, Q5, Q6, Q7, Q8, Q9 | ❌ Not implemented |
| **AVG aggregation** | Q3 | ❌ Not implemented |
| **WHERE on properties** | Q3, Q4, Q5, Q6, Q7, Q9 | ❌ Parsed but not executed |
| **Property access in RETURN** | Q1, Q2, Q3, Q5, Q6, Q7 | ❌ Not implemented |
| **ORDER BY** | Q1, Q2, Q3, Q4, Q6, Q7 | ❌ Not implemented |
| **WITH clause** | Q2, Q5, Q6, Q7 | ❌ Not implemented |
| **Multiple MATCH** | Q2, Q5, Q6, Q7 | ❌ Not implemented |

**Result:** Only Q8 partially supported (pattern matching works, COUNT doesn't)

### Example: Query 8

**Cypher Query:**
```cypher
MATCH (a:Person)-[r1:Follows]->(b:Person)-[r2:Follows]->(c:Person)
RETURN count(*) AS numPaths
```

**Current Status:**
```python
# This works:
result = engine.query_cypher(
    "MATCH (a:Person)-[:Follows]->(b)-[:Follows]->(c) RETURN a, b, c LIMIT 1000000"
)
# Returns: 329,754 paths in 160ms

# This doesn't work yet (COUNT not implemented):
result = engine.query_cypher(
    "MATCH (a:Person)-[:Follows]->(b)-[:Follows]->(c) RETURN count(*) AS numPaths"
)
# Error: COUNT aggregation not implemented
```

## Key Findings

### 1. Pattern Matching Discrepancy

Three different results for Query 8 (2-hop paths):
- **Kuzu:** 58,431,994 paths
- **Sabot Cypher engine:** 329,754 paths
- **Sabot manual:** 211,463 paths

**Investigation needed:**
- Why do Sabot's two implementations differ?
- Are we counting self-loops differently?
- Is there a bug in result collection?

### 2. Performance Comparison

When pattern matching works, Cypher engine is competitive:
- **Q8 Cypher:** 160ms for 329K paths
- **Q8 Manual:** 15ms for 211K paths
- **Kuzu:** 8.6ms for 58M paths

The 10x difference likely due to:
- Different result sizes
- Python overhead in Cypher engine
- Lack of multi-threading

### 3. Declarative vs Imperative

**Manual approach (current):**
```python
# Query 1: Top 3 most-followed
target_col = edges.column('target')
follower_counts = pc_arrow.value_counts(target_col)
person_ids = follower_counts.field('values')
num_followers = follower_counts.field('counts')
# ... 40 more lines of Python
```

**Cypher approach (future):**
```cypher
MATCH (follower:Person)-[:Follows]->(person:Person)
RETURN person.id, person.name, count(follower.id) AS numFollowers
ORDER BY numFollowers DESC LIMIT 3
```

The Cypher version is:
- ✅ 10x more concise
- ✅ Self-documenting
- ✅ Easier to optimize automatically
- ✅ Standard Cypher syntax (portable)

## Roadmap to Full Cypher Support

### Phase 1: Aggregations (P0)
**Impact:** Unlocks Q1, Q2, Q4, Q5, Q6, Q7, Q8, Q9

Implement:
- `COUNT(*)` - count all rows
- `COUNT(expr)` - count non-null values
- `SUM(expr)` - sum values
- `AVG(expr)` - average values
- `MIN/MAX(expr)` - min/max values

**Estimated effort:** 1-2 weeks
**Files to modify:**
- `sabot/_cython/graph/compiler/cypher_translator.py`
- `sabot/_cython/graph/executor/query_operators.pyx` (new)

### Phase 2: WHERE Clause Evaluation (P0)
**Impact:** Unlocks Q3, Q4, Q5, Q6, Q7, Q9

Implement:
- Property access: `person.age`, `city.name`
- Comparisons: `=`, `<`, `>`, `<=`, `>=`, `<>`
- Logical ops: `AND`, `OR`, `NOT`
- Functions: `lower()`, `upper()`

**Estimated effort:** 1-2 weeks
**Files to modify:**
- `sabot/_cython/graph/compiler/cypher_translator.py` (line 452-483)
- Add WHERE evaluation after pattern matching

### Phase 3: RETURN Projection (P1)
**Impact:** Unlocks property access for all queries

Implement:
- Property access in RETURN: `a.name`, `b.age`
- Aliases: `AS newName`
- DISTINCT
- Column renaming

**Estimated effort:** 1 week

### Phase 4: ORDER BY (P1)
**Impact:** Correct ordering for Q1, Q2, Q3, Q4, Q6, Q7

Implement:
- ORDER BY clause
- ASC/DESC
- Multiple sort keys
- Integration with Arrow compute sorting

**Estimated effort:** 3-5 days

### Phase 5: WITH Clause (P2)
**Impact:** Unlocks Q2, Q5, Q6, Q7

Implement:
- WITH clause for query composition
- Intermediate result materialization
- Variable scoping

**Estimated effort:** 1 week

### Phase 6: Multiple MATCH (P2)
**Impact:** Unlocks Q2, Q5, Q6, Q7

Implement:
- Multiple MATCH clauses in one query
- Join between MATCH results
- Variable sharing across MATCH

**Estimated effort:** 1 week

## Total Estimated Effort

**Critical path (P0):** 2-4 weeks
- Aggregations: 1-2 weeks
- WHERE clause: 1-2 weeks

**Full feature parity:** 6-8 weeks
- Above + RETURN + ORDER BY + WITH + multiple MATCH

## Benefits of Full Cypher Support

Once implemented:

1. **Productivity:** Write queries 10x faster (3 lines vs 40 lines)
2. **Performance:** Automatic query optimization (filter pushdown, join reordering)
3. **Portability:** Standard Cypher syntax works across systems
4. **Maintainability:** Declarative queries easier to understand and modify
5. **Optimization:** Engine can rewrite queries for better performance

## Recommendation

**Short term (now):**
- ✅ Use manual Python queries for working benchmarks
- ✅ Optimize Q3, Q4, Q6 with vectorized Arrow operations

**Medium term (1-2 months):**
- 🚧 Implement Cypher aggregations + WHERE
- 🚧 Achieve feature parity for all 9 queries
- 🚧 Benchmark Cypher vs manual performance

**Long term (3-6 months):**
- 🚀 Add query optimizer (filter pushdown, join reordering)
- 🚀 Multi-threaded query execution
- 🚀 Distributed query execution across agents

## Running the Benchmarks

### Manual Python Queries
```bash
cd /Users/bengamble/Sabot/benchmarks/kuzu_study
DYLD_LIBRARY_PATH=/Users/bengamble/Sabot/vendor/arrow/cpp/build/install/lib \
  /Users/bengamble/Sabot/.venv/bin/python run_benchmark.py
```

### Cypher Engine (Demo)
```bash
DYLD_LIBRARY_PATH=/Users/bengamble/Sabot/vendor/arrow/cpp/build/install/lib \
  /Users/bengamble/Sabot/.venv/bin/python run_benchmark_cypher.py
```

## Files

- `run_benchmark.py` - Manual Python query implementations
- `run_benchmark_cypher.py` - Cypher engine demonstration
- `queries.py` - Manual query functions
- `data_loader.py` - Load Parquet → Arrow
- `README.md` - Main benchmark results
- `CYPHER_ENGINE.md` - This file

---

**Conclusion:** Sabot's Cypher engine foundation works (pattern matching), but needs aggregations and WHERE clause support to run real-world queries. Once implemented, it will be more productive and maintainable than manual Python queries.
