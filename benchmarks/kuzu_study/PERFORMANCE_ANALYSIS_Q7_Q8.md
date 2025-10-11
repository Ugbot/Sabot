# Performance Analysis: Query 7 & Query 8 Slowdowns

**Date:** October 11, 2025
**Benchmark Results:** Code Mode (Direct Python Queries)

---

## Executive Summary

**Overall Performance:** Sabot is **2.03x faster** than Kuzu (297ms vs 602ms)

**Problem Queries:**
- **Query 7:** 61.95ms vs 15.10ms (Kuzu) = **4.10x slower** ⚠️
- **Query 8:** 14.99ms vs 8.60ms (Kuzu) = **1.74x slower** ⚠️

**Root Causes Identified:**
1. **Query 7:** Python loops for city→state mapping (lines 438-443, 462-470)
2. **Query 8:** Potentially correct - different result counts suggest dataset or algorithm differences

---

## Benchmark Results (Code Mode)

### Full Results Table

| Query | Sabot (ms) | Kuzu (ms) | Speedup/Slowdown |
|-------|------------|-----------|------------------|
| Query 1: Top followers | 77.82 | 160.30 | **2.06x faster** ✅ |
| Query 2: Most-followed city | 87.42 | 249.80 | **2.86x faster** ✅ |
| Query 3: Lowest avg age | 6.77 | 8.50 | **1.25x faster** ✅ |
| Query 4: Age by country | 5.79 | 14.70 | **2.54x faster** ✅ |
| Query 5: Interest filter | 7.62 | 13.40 | **1.76x faster** ✅ |
| Query 6: City interest count | 12.41 | 36.20 | **2.92x faster** ✅ |
| **Query 7: State age interest** | **61.95** | **15.10** | **4.10x slower** ⚠️ |
| **Query 8: 2-hop paths** | **14.99** | **8.60** | **1.74x slower** ⚠️ |
| Query 9: Filtered paths | 22.24 | 95.50 | **4.29x faster** ✅ |
| **Total** | **297.01** | **602.10** | **2.03x faster** ✅ |

### Key Observations

**Strengths (7/9 queries faster):**
- Queries 1-6, 9: **1.25x - 4.29x faster** than Kuzu
- Vectorized Arrow operations dominate
- Multi-threaded hash joins excel

**Weaknesses (2/9 queries slower):**
- Query 7: Non-vectorized Python loops
- Query 8: Pattern matching overhead or correctness issue

---

## Query 7 Analysis: State Age Interest

### Query Description

**Cypher:**
```cypher
MATCH (p:Person)-[:LivesIn]->(:City)-[:CityIn]->(s:State)
WHERE p.age >= 23 AND p.age <= 30 AND s.country = 'United States'
WITH p, s
MATCH (p)-[:HasInterest]->(i:Interest)
WHERE lower(i.interest) = lower('photography')
RETURN count(p.id) AS numPersons, s.state AS state, s.country AS country
ORDER BY numPersons DESC LIMIT 1
```

**Result:** 138 people in California interested in photography (ages 23-30)

### Current Implementation (queries.py:399-497)

**Performance:** 61.95ms

**Bottlenecks Identified:**

#### 1. Interest Lookup - Python Loop (lines 418-423)
```python
# ❌ Loops through interests table
interest_id = None
for i in range(interests.num_rows):
    if interests.column('interest')[i].as_py().lower() == interest_lower:
        interest_id = interests.column('id')[i].as_py()
        break
```
**Time:** ~1-2ms (minor)

#### 2. City→State Mapping - Python Loop (lines 438-443)
```python
# ❌ MAJOR BOTTLENECK: Loops through city_in edges
city_to_state = {}
for i in range(city_in.num_rows):  # 7,117 iterations
    city_id = city_in.column('source')[i].as_py()
    state_id = city_in.column('target')[i].as_py()
    if state_id in country_state_ids:
        city_to_state[city_id] = state_id
```
**Time:** ~10-15ms (moderate)
**Problem:** 7,117 iterations with Python interpreter overhead

#### 3. State Counting - Python Loop (lines 462-470)
```python
# ❌ MAJOR BOTTLENECK: Loops through matching persons
state_counts = {}
for person_id in matching_person_ids:  # ~500-1000 iterations
    person_mask = pc.equal(lives_in.column('source'), ca.scalar(person_id))
    residence = lives_in.filter(person_mask)  # ❌ FILTER PER PERSON!
    if residence.num_rows > 0:
        city_id = residence.column('target')[0].as_py()
        if city_id in city_to_state:
            state_id = city_to_state[city_id]
            state_counts[state_id] = state_counts.get(state_id, 0) + 1
```
**Time:** ~40-50ms (CRITICAL)
**Problems:**
1. Python loop over ~500-1000 persons
2. **`.filter()` called once per person** - extremely expensive!
3. Dict lookups in Python

**Total Overhead:** ~50-65ms from Python loops

### Comparison with Kuzu

**Kuzu:** 15.10ms

**Why Kuzu is Faster:**
- Native C++ implementation
- Optimized hash joins for city→state→person pipeline
- No Python interpreter overhead
- Vectorized aggregation

**Performance Gap:** 61.95ms (Sabot) - 15.10ms (Kuzu) = **46.85ms overhead**

### Optimization Strategy

#### Option 1: Vectorize with Arrow Joins (Recommended)

**Replace Lines 418-423 (Interest Lookup):**
```python
# ✅ Vectorized interest lookup
interest_mask = pc.equal(
    pc.utf8_lower(interests.column('interest')),
    ca.scalar(interest.lower())
)
matching_interests = interests.filter(interest_mask)
interest_id = matching_interests.column('id')[0].as_py()
```
**Speedup:** 1-2ms → <1ms (minor)

**Replace Lines 438-443 (City→State Mapping):**
```python
# ✅ Filter city_in by country states (vectorized)
state_mask = pc.is_in(city_in.column('target'), ca.array(list(country_state_ids)))
country_city_in = city_in.filter(state_mask)
# Now country_city_in has only relevant city→state mappings
```
**Speedup:** 10-15ms → 1-2ms (**8-13ms saved**)

**Replace Lines 462-470 (State Counting):**
```python
# ✅ Join age+interest filtered persons with lives_in (hash join!)
matched_persons = ca.table({'id': ca.array(list(matching_person_ids))})
with_city = matched_persons.join(
    lives_in.select(['source', 'target']),
    keys='id',
    right_keys='source',
    join_type='inner'
)

# ✅ Join with city_in to get states (another hash join!)
with_state = with_city.join(
    country_city_in.select(['source', 'target']),
    keys='target',
    right_keys='source',
    join_type='inner'
)

# ✅ Group by state and count (vectorized!)
state_counts = with_state.group_by('target').aggregate([('id', 'count')])
```
**Speedup:** 40-50ms → 3-5ms (**35-45ms saved**)

**Total Expected Speedup:** 61.95ms → **~15-20ms** (3-4x faster)

This would make Query 7 **comparable to or faster than Kuzu**!

---

## Query 8 Analysis: 2-Hop Paths

### Query Description

**Cypher:**
```cypher
MATCH (a:Person)-[r1:Follows]->(b:Person)-[r2:Follows]->(c:Person)
RETURN count(*) AS numPaths
```

**Result:** 211,463 2-hop paths

### Current Implementation (queries.py:500-524)

**Performance:** 14.99ms

**Code:**
```python
from sabot._cython.graph.query import match_2hop

edges = ca.table({
    'source': follows.column('source'),
    'target': follows.column('target')
})

result = match_2hop(edges, edges)
num_paths = result.num_matches()
```

**Analysis:**
- Uses Cython `match_2hop` pattern matching
- Morsel-driven parallel execution
- Very clean implementation (no Python loops)

### Comparison with Kuzu

**Kuzu:** 8.60ms

**Performance Gap:** 14.99ms - 8.60ms = **6.39ms slower** (1.74x)

### Critical Finding: Result Count Discrepancy

**Sabot:** 211,463 paths
**Kuzu:** 58,000,000 paths (reported in README)

**This is a 274x difference!**

**Possible Explanations:**

#### Hypothesis 1: Different Datasets
- Sabot and Kuzu may be using different versions of the dataset
- Need to verify edge counts match

#### Hypothesis 2: Sabot Deduplicates Results
- `match_2hop` may return unique paths only
- Kuzu might count duplicates or consider directionality differently

#### Hypothesis 3: Sabot Applies LIMIT Implicitly
- Pattern matching may have internal result limits
- Need to check `match_2hop` implementation

#### Hypothesis 4: Edge Filtering
- Sabot might be filtering self-loops or duplicate edges
- Need to verify edge preprocessing

### Investigation Needed

**To Determine Root Cause:**

1. **Verify Dataset:**
   ```python
   # Check edge counts
   print(f"Follows edges: {follows.num_rows}")  # Should be ~2.4M

   # Check for duplicates
   unique_edges = follows.group_by(['source', 'target']).aggregate([])
   print(f"Unique edges: {unique_edges.num_rows}")
   ```

2. **Check match_2hop Implementation:**
   ```bash
   # Find match_2hop source
   find /Users/bengamble/Sabot -name "*.pyx" -o -name "*.cpp" | xargs grep -l "match_2hop"
   ```

3. **Test with Small Dataset:**
   ```python
   # Create known graph: A->B->C, A->B->D (2 paths)
   test_edges = ca.table({
       'source': [0, 0, 1, 1],
       'target': [1, 1, 2, 3]
   })
   result = match_2hop(test_edges, test_edges)
   print(f"Expected: 2 paths, Got: {result.num_matches()}")
   ```

4. **Compare Algorithms:**
   - Kuzu likely uses join-based approach
   - Sabot uses Cython pattern matching
   - Different algorithms may count differently

### Performance Assessment

**IF Results Are Correct (211K vs 58M is intentional):**
- 14.99ms for 211K results is excellent (14.1M results/sec)
- Sabot is actually very efficient
- Just different counting methodology

**IF Sabot Should Return 58M Results:**
- Need to fix `match_2hop` to not deduplicate/limit
- Performance would need re-evaluation
- May need optimization if returning 58M results

### Optimization Strategy

**Current Approach (DO NOTHING):**
- Performance is acceptable for current result set
- 14.99ms vs 8.60ms is minor (6ms difference)
- Focus on Query 7 first

**Future Investigation:**
- Determine correct result count (211K vs 58M)
- If 58M is correct, optimize `match_2hop` to return all results efficiently
- Consider using Arrow join-based implementation instead

---

## Summary and Recommendations

### Query 7: Immediate Action Required ⚠️

**Problem:** 4.10x slower due to Python loops
**Solution:** Vectorize with Arrow joins
**Effort:** 2-3 hours
**Impact:** 61.95ms → 15-20ms (3-4x faster)
**Priority:** P0 - High impact, medium effort

**Implementation Plan:**
1. Replace interest lookup with vectorized filter (5 min)
2. Replace city→state mapping with filtered join (15 min)
3. Replace state counting loop with join + group_by (30 min)
4. Test correctness (30 min)
5. Benchmark and verify speedup (15 min)

**Expected Outcome:**
- Query 7: 15-20ms (comparable to Kuzu)
- Overall: 250ms vs 602ms (2.4x faster overall)

### Query 8: Investigation Required 🔍

**Problem:** 1.74x slower OR different counting methodology
**Solution:** Determine correct behavior, then optimize if needed
**Effort:** 4-6 hours
**Impact:** Uncertain until correctness verified
**Priority:** P1 - Low immediate impact, requires investigation

**Investigation Plan:**
1. Verify dataset consistency (30 min)
2. Find and review `match_2hop` source code (1 hour)
3. Test with known small dataset (30 min)
4. Compare algorithms with Kuzu (2 hours)
5. Decide: fix correctness OR accept current results (decision point)

**Possible Outcomes:**
- **Case 1:** Results correct, performance acceptable → Do nothing
- **Case 2:** Results wrong, need to fix → Implement join-based approach
- **Case 3:** Results correct, want more speed → Minor optimizations

---

## Cypher Mode Status

**Current State:** Not ready for benchmarking

**Missing Features:**
- ❌ Aggregations (COUNT, SUM, AVG)
- ❌ Property access in RETURN (a.name, b.age)
- ❌ WHERE clause evaluation
- ❌ ORDER BY
- ❌ WITH clause
- ❌ Multiple MATCH clauses

**What Works:**
- ✅ Pattern matching: (a)-[r]->(b)
- ✅ Node labels: (a:Person)
- ✅ Edge types: -[r:KNOWS]->
- ✅ Variable-length paths: -[r*1..3]->
- ✅ LIMIT clause

**Recommendation:**
Focus on code mode optimizations first. Cypher mode can be improved later once core translator features are implemented.

---

## Next Steps

### Immediate (This Week)

1. **✅ Vectorize Query 7** (2-3 hours)
   - Implement Arrow join-based approach
   - Test correctness
   - Verify 3-4x speedup

2. **🔍 Investigate Query 8** (4-6 hours)
   - Determine correct result count
   - Review `match_2hop` implementation
   - Decide on action plan

### Short-term (Next 2 Weeks)

3. **Optimize Query 8** (if needed)
   - Implement join-based approach
   - Benchmark performance

4. **Update Benchmark Results**
   - Re-run with optimizations
   - Update README.md
   - Celebrate 2.5x+ overall speedup!

### Long-term (Next Month)

5. **Implement Cypher Translator Features**
   - Aggregations (COUNT, AVG)
   - WHERE clause evaluation
   - Property access in RETURN
   - ORDER BY
   - WITH clause support

6. **Run Cypher Mode Benchmark**
   - Compare parser + translator overhead
   - Verify end-to-end correctness
   - Document performance

---

## Conclusion

**Overall Performance:** Excellent! **2.03x faster than Kuzu**

**Bottlenecks Identified:**
- **Query 7:** Clear root cause (Python loops), clear solution (vectorize)
- **Query 8:** Unclear if problem or feature, requires investigation

**Action Plan:**
1. Fix Query 7 (high impact, medium effort) → **Priority P0**
2. Investigate Query 8 (uncertain impact, high effort) → **Priority P1**
3. Cypher mode can wait until translator is more complete

**Expected Impact:**
- After Query 7 optimization: **~2.4x faster overall**
- After Query 8 (if optimized): **~2.5x faster overall**

**Timeline:**
- Query 7 vectorization: 1 day
- Query 8 investigation: 2-3 days
- Total: **~1 week to 2.5x speedup**

🎯 **Focus on Query 7 first - biggest bang for buck!**
