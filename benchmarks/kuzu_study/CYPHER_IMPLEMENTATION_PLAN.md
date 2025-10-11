# Cypher Engine: Complete Implementation Plan

**Date:** October 11, 2025
**Current Status:** Phase 1 & 2 complete (functional, needs optimization)
**Remaining Work:** ~3-4 hours total

---

## Overall Architecture

```
Cypher Query String
    ↓
[Lark Parser] → Parses openCypher grammar
    ↓
Cypher AST (CypherQuery, MatchClause, WhereClause, ReturnClause)
    ↓
[CypherTranslator] → Translates AST to Sabot operations
    ↓
Pattern Matching Kernels (match_2hop, match_3hop, match_variable_length_path)
    ↓
Arrow Tables (vertices, edges, pattern results)
    ↓
Filter/Project/Aggregate (WHERE, RETURN, WITH)
    ↓
QueryResult (Arrow table + metadata)
```

---

## Phase Status Overview

| Phase | Status | Time Estimate | Priority |
|-------|--------|---------------|----------|
| **Phase 0: LIMIT/SKIP bug fix** | ✅ Complete | Done | N/A |
| **Phase 1: Multi-node loading** | ✅ Complete | Done | N/A |
| **Phase 2: WHERE clause** | ⚠️ Functional, slow | +30 min (optimization) | **P0** |
| **Phase 3: Multiple MATCH** | ❌ Not started | 1-2 hours | **P1** |
| **Phase 4: WITH clause** | ❌ Not started | 1 hour | **P1** |

**Total Remaining:** ~3-4 hours to full Cypher benchmark support

---

## Phase 2: WHERE Clause Optimization (P0 - CRITICAL)

### Current Problem

**File:** `sabot/_cython/graph/compiler/cypher_translator.py:816-837`

**Bottleneck:**
```python
def _evaluate_property_access(self, table: pa.Table, prop_access: PropertyAccess) -> pa.Array:
    """Evaluate property access by joining with vertex table."""
    # ...

    # ❌ THIS IS THE BOTTLENECK - O(N * M) complexity!
    result_values = []
    for vid in vertex_ids.to_pylist():  # Loop over 329K rows
        mask = pc.equal(vertices.column('id'), pa.scalar(vid))
        matching = vertices.filter(mask)  # 329K filter operations!
        if matching.num_rows > 0 and prop_name in matching.column_names:
            result_values.append(matching.column(prop_name)[0].as_py())
        else:
            result_values.append(None)

    return pa.array(result_values)
```

**Impact:**
- Query 9 with `WHERE b.age < 50 AND c.age > 25` times out after 60s
- 329K pattern match results × 2 property accesses = 658K filter operations
- Makes WHERE clause unusable for real queries

---

### Solution: Hash Join Optimization

**Strategy:** Replace per-row filtering with Arrow hash join (O(N + M))

**Implementation:**

```python
def _evaluate_property_access(self, table: pa.Table, prop_access: PropertyAccess) -> pa.Array:
    """
    Evaluate property access using hash join (optimized).

    Before: O(N * M) - filter vertices 329K times
    After:  O(N + M) - single hash join

    Expected speedup: 100-1000x
    """
    var_name = prop_access.variable
    prop_name = prop_access.property_name

    # Get vertex IDs from pattern match result
    id_col_name = f"{var_name}_id"
    if id_col_name not in table.column_names:
        raise ValueError(f"Variable {var_name} not found in pattern match results")

    # Get vertices table
    vertices = self.graph_engine._vertices_table
    if vertices is None:
        raise ValueError("No vertices loaded in graph")

    # Check if property exists in any vertex type
    if prop_name not in vertices.column_names:
        # Property doesn't exist - return nulls
        return pa.array([None] * table.num_rows)

    # Hash join: table[var_id] LEFT JOIN vertices[id, prop]
    # Arrow's join is O(N + M) with hash table
    joined = table.join(
        vertices.select(['id', prop_name]),
        keys=id_col_name,
        right_keys='id',
        join_type='left'  # Keep all rows from pattern match
    )

    # Extract property column from joined result
    # Column will be named 'prop_name' from right table
    return joined.column(prop_name)
```

**Key Changes:**
1. Replace `for vid in vertex_ids.to_pylist()` loop with single `table.join()`
2. Use Arrow's built-in hash join (C++ implementation, multi-threaded)
3. Use `left` join type to preserve all pattern match results
4. Select only needed columns (`['id', prop_name]`) to minimize join cost

**Testing:**

```python
# Test with Query 9
query9 = """
MATCH (a:Person)-[:Follows]->(b:Person)-[:Follows]->(c:Person)
WHERE b.age < 50 AND c.age > 25
RETURN count(*) AS numPaths
"""

# Before: Timeout >60s
# After:  Expected <100ms (600x speedup)
```

**Edge Cases to Handle:**
1. Property doesn't exist in vertex table → return nulls
2. Vertex ID not found (orphaned edge) → left join handles this
3. Multiple properties accessed in same query → each join is separate (acceptable)

**Time Estimate:** 30 minutes

**Files to Modify:**
- `sabot/_cython/graph/compiler/cypher_translator.py` (lines 816-837)

---

## Phase 3: Multiple MATCH Support (P1)

### Goal

Support sequential MATCH clauses in a single query.

**Example Query (Query 2):**
```cypher
MATCH (follower:Person)-[:Follows]->(person:Person)
WITH person, count(follower.id) as numFollowers
ORDER BY numFollowers DESC LIMIT 1
MATCH (person)-[:LivesIn]->(city:City)  # ← Second MATCH uses 'person' from first
RETURN person.name, numFollowers, city.city
```

### Current Blocker

**File:** `sabot/_cython/graph/compiler/cypher_translator.py:81-85`

```python
def translate(self, query: CypherQuery) -> QueryResult:
    # ...

    # ❌ BLOCKER: Only supports single MATCH
    if len(query.match_clauses) != 1:
        raise NotImplementedError(
            f"Multiple MATCH clauses not yet supported. "
            f"Found {len(query.match_clauses)} MATCH clauses."
        )
```

### Implementation Strategy

**Step 1: Remove blocker and add multi-MATCH loop**

```python
def translate(self, query: CypherQuery) -> QueryResult:
    """Translate Cypher query with multiple MATCH clauses."""
    import time
    start_time = time.time()

    # Process first MATCH clause
    current_table = self._translate_match(query.match_clauses[0])

    # Apply WITH clause if present (projection + filtering)
    if query.with_clauses and len(query.with_clauses) > 0:
        current_table = self._apply_with(current_table, query.with_clauses[0])

    # Process additional MATCH clauses (if any)
    for i in range(1, len(query.match_clauses)):
        match_clause = query.match_clauses[i]

        # Join with previous results on shared variables
        current_table = self._translate_match_with_context(
            match_clause,
            current_table
        )

        # Apply WITH clause if present
        if i < len(query.with_clauses):
            current_table = self._apply_with(current_table, query.with_clauses[i])

    # Apply final RETURN projection
    if query.return_clause:
        current_table = self._apply_return(current_table, query.return_clause)

    # Create result
    execution_time = (time.time() - start_time) * 1000
    metadata = QueryMetadata(
        query=str(query),
        language='cypher',
        num_results=current_table.num_rows,
        execution_time_ms=execution_time
    )

    return QueryResult(current_table, metadata)
```

**Step 2: Implement `_translate_match_with_context()`**

```python
def _translate_match_with_context(
    self,
    match: MatchClause,
    context_table: pa.Table
) -> pa.Table:
    """
    Translate MATCH clause with context from previous MATCH.

    Strategy:
    1. Execute pattern matching normally
    2. Identify shared variables between context and new pattern
    3. Join pattern results with context on shared variables

    Example:
        Context table (from first MATCH):
            person_id | numFollowers
            --------- | ------------
            123       | 50

        New MATCH: (person)-[:LivesIn]->(city:City)
        Pattern result:
            source | target
            ------ | ------
            123    | 456
            789    | 456

        Join on person_id = source:
            person_id | numFollowers | city_id
            --------- | ------------ | -------
            123       | 50           | 456

    Args:
        match: MatchClause to execute
        context_table: Result table from previous MATCH/WITH

    Returns:
        Joined table with combined results
    """
    # Execute pattern matching
    pattern_result = self._translate_match(match)

    # Find shared variables between context and pattern
    shared_vars = self._find_shared_variables(context_table, match.pattern)

    if not shared_vars:
        # No shared variables - Cartesian product (rare, expensive)
        # This shouldn't happen in well-formed queries
        raise ValueError(
            "No shared variables between MATCH clauses. "
            "This would create a Cartesian product. "
            "Ensure MATCH clauses share at least one variable."
        )

    # Join on shared variables
    joined_table = self._join_on_shared_variables(
        context_table,
        pattern_result,
        shared_vars
    )

    return joined_table
```

**Step 3: Implement helper methods**

```python
def _find_shared_variables(
    self,
    context_table: pa.Table,
    pattern: Pattern
) -> List[str]:
    """
    Find variables that exist in both context and pattern.

    Args:
        context_table: Table from previous MATCH/WITH
        pattern: Pattern from new MATCH clause

    Returns:
        List of shared variable names

    Example:
        Context columns: ['person_id', 'numFollowers']
        Pattern variables: ['person', 'city']
        Shared: ['person']
    """
    # Extract variable names from context table
    # Pattern: variable names end with '_id' (e.g., 'person_id')
    context_vars = set()
    for col_name in context_table.column_names:
        if col_name.endswith('_id'):
            var_name = col_name[:-3]  # Remove '_id' suffix
            context_vars.add(var_name)

    # Extract variable names from pattern
    pattern_vars = set()
    for element in pattern.elements:
        for node in element.nodes:
            if node.variable:
                pattern_vars.add(node.variable)

    # Find intersection
    shared = list(context_vars & pattern_vars)

    return shared


def _join_on_shared_variables(
    self,
    context_table: pa.Table,
    pattern_table: pa.Table,
    shared_vars: List[str]
) -> pa.Table:
    """
    Join two tables on shared variables.

    Strategy:
    - For single shared variable: simple join
    - For multiple shared variables: multi-key join

    Args:
        context_table: Table from previous MATCH/WITH
        pattern_table: Table from pattern matching
        shared_vars: List of variable names to join on

    Returns:
        Joined table
    """
    if len(shared_vars) == 1:
        # Single variable join (most common)
        var = shared_vars[0]

        # Join keys: context[var_id] = pattern[var_id]
        context_key = f"{var}_id"
        pattern_key = f"{var}_id"

        # Rename pattern columns to avoid conflicts (except join key)
        # If context has 'person_id', pattern has 'person_id' and 'city_id'
        # After join: keep only one 'person_id', add 'city_id'

        joined = context_table.join(
            pattern_table,
            keys=context_key,
            right_keys=pattern_key,
            join_type='inner'  # Only keep matching rows
        )

        # Note: Arrow join will create duplicate columns with suffixes
        # We need to drop the duplicate join key from right side
        # This is handled automatically by Arrow's join

        return joined

    else:
        # Multiple variable join (rare, e.g., graph patterns)
        # Use multi-key join
        context_keys = [f"{var}_id" for var in shared_vars]
        pattern_keys = [f"{var}_id" for var in shared_vars]

        joined = context_table.join(
            pattern_table,
            keys=context_keys,
            right_keys=pattern_keys,
            join_type='inner'
        )

        return joined
```

### Testing

**Test Query (Query 2):**
```cypher
MATCH (follower:Person)-[:Follows]->(person:Person)
WITH person, count(follower.id) as numFollowers
ORDER BY numFollowers DESC LIMIT 1
MATCH (person)-[:LivesIn]->(city:City)
RETURN person.name AS name, numFollowers, city.city AS city
```

**Expected Result:**
```
name             | numFollowers | city
---------------- | ------------ | ----
[Most followed]  | [count]      | [their city]
```

**Edge Cases:**
1. No shared variables → Error (prevent Cartesian product)
2. Multiple shared variables → Multi-key join
3. Empty first MATCH result → Second MATCH returns empty (inner join)

**Time Estimate:** 1-2 hours

**Files to Modify:**
- `sabot/_cython/graph/compiler/cypher_translator.py`
  - `translate()` method (lines 64-105)
  - Add new methods: `_translate_match_with_context()`, `_find_shared_variables()`, `_join_on_shared_variables()`

---

## Phase 4: WITH Clause Support (P1)

### Goal

Support WITH clause for intermediate projections and filtering.

**Example Query (Query 5):**
```cypher
MATCH (p:Person)-[:HasInterest]->(i:Interest)
WHERE lower(i.interest) = lower($interest)
AND lower(p.gender) = lower($gender)
WITH p, i  # ← Intermediate projection
MATCH (p)-[:LivesIn]->(c:City)
WHERE c.city = $city AND c.country = $country
RETURN count(p) AS numPersons
```

### Current Status

- ✅ WITH clause parsed by Lark parser
- ✅ AST node `WithClause` defined in `cypher_ast.py`
- ❌ `_apply_with()` method not implemented

### Implementation Strategy

**WITH clause is similar to RETURN but:**
1. Results are passed to next MATCH (not returned to user)
2. Can have WHERE filtering after projection
3. Can have ORDER BY + LIMIT for top-K selection

**Step 1: Implement `_apply_with()`**

```python
def _apply_with(self, table: pa.Table, with_clause: WithClause) -> pa.Table:
    """
    Apply WITH clause projection to intermediate results.

    WITH clause is like RETURN but for intermediate results:
    - Projects/aggregates columns
    - Can filter with WHERE
    - Can sort with ORDER BY
    - Can limit with LIMIT
    - Results flow to next MATCH

    Example:
        WITH person, count(follower.id) as numFollowers
        ORDER BY numFollowers DESC LIMIT 1

    Args:
        table: Result table from MATCH
        with_clause: WithClause AST node

    Returns:
        Projected table for next MATCH
    """
    # WITH clause has same structure as RETURN:
    # - items: List[ReturnItem] (expressions + aliases)
    # - distinct: bool
    # - order_by: List[OrderBy]
    # - skip: Optional[int]
    # - limit: Optional[int]

    # Check if query has aggregations
    has_aggregations = any(
        isinstance(item.expression, FunctionCall)
        for item in with_clause.items
    )

    if has_aggregations:
        # Aggregation query
        table = self._apply_aggregation_for_with(table, with_clause)
    else:
        # Regular projection
        table = self._apply_projection_for_with(table, with_clause)

    # Apply DISTINCT if requested
    if with_clause.distinct:
        table = table.group_by(table.column_names).aggregate([])

    # Apply WHERE filtering (WITH can have WHERE after projection)
    if with_clause.where:
        # Create a dummy pattern element (not used for WITH)
        # WHERE after WITH filters the projected results
        table = self._apply_where_filter_simple(table, with_clause.where)

    # Apply ORDER BY
    if with_clause.order_by:
        table = self._apply_order_by(table, with_clause.order_by)

    # Apply SKIP
    if with_clause.skip:
        table = table.slice(with_clause.skip)

    # Apply LIMIT
    if with_clause.limit:
        if with_clause.skip:
            table = table.slice(0, with_clause.limit)
        else:
            table = table.slice(0, with_clause.limit)

    return table
```

**Step 2: Implement simplified WHERE for WITH**

```python
def _apply_where_filter_simple(self, table: pa.Table, where: Expression) -> pa.Table:
    """
    Apply WHERE filter to projected table (used after WITH).

    Difference from _apply_where_filter():
    - No pattern element context (just table columns)
    - Property access uses column names directly (not vertex lookups)

    Example:
        WITH p, count(*) as cnt
        WHERE cnt > 10  # ← Filter on projected column

    Args:
        table: Projected table from WITH
        where: WHERE clause expression

    Returns:
        Filtered table
    """
    # Evaluate WHERE expression (similar to main WHERE)
    mask = self._evaluate_where_expression_simple(table, where)
    return table.filter(mask)


def _evaluate_where_expression_simple(self, table: pa.Table, expr: Expression) -> pa.Array:
    """
    Evaluate WHERE expression for projected table.

    Difference from _evaluate_where_expression():
    - No property access (columns are already in table)
    - Just evaluate comparisons on table columns
    """
    if isinstance(expr, Comparison):
        left_val = self._evaluate_filter_operand_simple(table, expr.left)
        right_val = self._evaluate_filter_operand_simple(table, expr.right)

        op = expr.operator.lower()
        if op == '=':
            return pc.equal(left_val, right_val)
        elif op == '!=':
            return pc.not_equal(left_val, right_val)
        elif op == '<':
            return pc.less(left_val, right_val)
        elif op == '<=':
            return pc.less_equal(left_val, right_val)
        elif op == '>':
            return pc.greater(left_val, right_val)
        elif op == '>=':
            return pc.greater_equal(left_val, right_val)
        else:
            raise NotImplementedError(f"Comparison operator not supported: {op}")

    elif isinstance(expr, BinaryOp):
        left_mask = self._evaluate_where_expression_simple(table, expr.left)
        right_mask = self._evaluate_where_expression_simple(table, expr.right)

        op = expr.operator.upper()
        if op == 'AND':
            return pc.and_(left_mask, right_mask)
        elif op == 'OR':
            return pc.or_(left_mask, right_mask)
        else:
            raise NotImplementedError(f"Logical operator not supported: {op}")

    elif isinstance(expr, UnaryOp):
        if expr.operator.upper() == 'NOT':
            inner_mask = self._evaluate_where_expression_simple(table, expr.operand)
            return pc.invert(inner_mask)
        else:
            raise NotImplementedError(f"Unary operator not supported: {expr.operator}")

    else:
        raise NotImplementedError(f"WHERE expression not supported: {type(expr)}")


def _evaluate_filter_operand_simple(self, table: pa.Table, operand: Expression):
    """
    Evaluate filter operand for projected table.

    Simpler than _evaluate_filter_operand():
    - Variables refer to columns in table (not vertex IDs)
    - No property access (already projected)
    """
    if isinstance(operand, Variable):
        # Variable refers to column in projected table
        var_name = operand.name
        if var_name in table.column_names:
            return table.column(var_name)
        else:
            raise ValueError(f"Column {var_name} not found in projected table")

    elif isinstance(operand, Literal):
        return pa.scalar(operand.value)

    elif isinstance(operand, FunctionCall):
        return self._evaluate_filter_function_simple(table, operand)

    else:
        raise NotImplementedError(f"Filter operand not supported: {type(operand)}")
```

**Step 3: Reuse projection/aggregation logic**

WITH clause uses same projection/aggregation logic as RETURN:
- `_apply_aggregation_for_with()` → calls existing `_apply_aggregation()`
- `_apply_projection_for_with()` → calls existing `_apply_projection()`

**Just need thin wrappers:**

```python
def _apply_projection_for_with(self, table: pa.Table, with_clause: WithClause) -> pa.Table:
    """Apply projection for WITH clause (reuses RETURN logic)."""
    return self._apply_projection(table, with_clause)

def _apply_aggregation_for_with(self, table: pa.Table, with_clause: WithClause) -> pa.Table:
    """Apply aggregation for WITH clause (reuses RETURN logic)."""
    return self._apply_aggregation(table, with_clause)
```

### Testing

**Test Query (Query 5):**
```cypher
MATCH (p:Person)-[:HasInterest]->(i:Interest)
WHERE lower(i.interest) = 'fine dining'
WITH p, i
MATCH (p)-[:LivesIn]->(c:City)
WHERE c.city = 'London' AND c.country = 'United Kingdom'
RETURN count(p) AS numPersons
```

**Expected Result:**
```
numPersons
----------
[count of men in London UK interested in fine dining]
```

**Edge Cases:**
1. WITH without aggregation → Simple projection
2. WITH with aggregation → Group by
3. WITH + WHERE → Filter projected results
4. WITH + ORDER BY + LIMIT → Top-K selection
5. Multiple WITH clauses → Sequential projection

**Time Estimate:** 1 hour

**Files to Modify:**
- `sabot/_cython/graph/compiler/cypher_translator.py`
  - Add `_apply_with()` method
  - Add simplified WHERE methods for projected tables

---

## Integration & Testing Plan

### Test Coverage

**After all phases complete, test all 9 benchmark queries:**

| Query | Features Required | Expected Status |
|-------|-------------------|-----------------|
| Q1 | COUNT, property access, ORDER BY | ✅ Should work |
| Q2 | WITH, COUNT, multiple MATCH, property access | ✅ Should work |
| Q3 | Variable-length paths, AVG, WHERE, ORDER BY | ✅ Should work |
| Q4 | WHERE, COUNT, property access, ORDER BY | ✅ Should work |
| Q5 | WHERE + lower(), WITH, multiple MATCH, COUNT | ✅ Should work |
| Q6 | WHERE + lower(), WITH, COUNT, property access, ORDER BY | ✅ Should work |
| Q7 | WHERE, WITH, multiple MATCH, COUNT, ORDER BY | ✅ Should work |
| Q8 | Pattern matching, COUNT | ✅ Already works |
| Q9 | Pattern matching, WHERE, COUNT | ✅ Already works (after opt) |

### Testing Strategy

**Step 1: Unit tests for each phase**
```bash
# Phase 2 optimization
python -c "test_where_clause_performance()"

# Phase 3: Multiple MATCH
python -c "test_multiple_match_simple()"
python -c "test_multiple_match_with_join()"

# Phase 4: WITH clause
python -c "test_with_projection()"
python -c "test_with_aggregation()"
python -c "test_with_where_filter()"
```

**Step 2: Integration test with benchmark runner**
```bash
python run_benchmark_cypher.py
```

**Expected output after all phases:**
```
======================================================================
RUNNING ALL 9 QUERIES
======================================================================

Query 1: Top 3 most-followed persons
  ✅ Result: 3 persons
  Time: ~50-100ms

Query 2: City where most-followed person lives
  ✅ Result: 1 city
  Time: ~80-150ms

[... all 9 queries ...]

======================================================================
SUMMARY
======================================================================
Total time: ~500-1000ms
Queries completed: 9/9
Performance vs Kuzu: [to be measured]
```

### Performance Targets

| Query | Target Time | Kuzu Time | Target vs Kuzu |
|-------|-------------|-----------|----------------|
| Q1 | <100ms | 160ms | 1.6x faster |
| Q2 | <150ms | 250ms | 1.7x faster |
| Q3 | <50ms | 9ms | 5.5x slower (acceptable) |
| Q4 | <50ms | 15ms | 3x slower (acceptable) |
| Q5 | <80ms | 13ms | 6x slower (acceptable) |
| Q6 | <100ms | 36ms | 2.8x slower (acceptable) |
| Q7 | <100ms | 15ms | 6.7x slower (acceptable) |
| Q8 | <300ms | 9ms | 33x slower (needs work) |
| Q9 | <150ms | 96ms | 1.6x faster |

**Notes:**
- Queries 3-7: Acceptable to be slower initially (complex multi-hop + aggregations)
- Query 8: Known issue (path counting difference - 329K vs 58M)
- Focus on correctness first, optimization later

---

## File Summary

### Files Modified

1. **`benchmarks/kuzu_study/run_benchmark_cypher.py`**
   - Lines 164-294: Multi-node/edge loading (Phase 1) ✅

2. **`sabot/_cython/graph/compiler/cypher_translator.py`**
   - Lines 452-626: WHERE clause evaluation (Phase 2) ✅
   - Lines 816-837: Property access optimization (Phase 2 opt) ⏳
   - Lines 64-105: Multiple MATCH support (Phase 3) ⏳
   - New methods: `_apply_with()` and helpers (Phase 4) ⏳

### Files to Create

None - all changes are in existing files.

---

## Dependencies & Requirements

### Python Dependencies (already satisfied)
- `sabot`: Graph query engine
- `pyarrow`/`cyarrow`: Arrow operations
- `polars`: Data loading
- `lark-parser`: Cypher parsing

### C++ Dependencies (already built)
- Pattern matching kernels: `match_2hop`, `match_3hop`, `match_variable_length_path`
- Arrow C++ library

### Data Dependencies
- Kuzu benchmark dataset (100K persons, 2.7M edges)
- Generated via `reference/data/generate_data.sh`

---

## Risk Assessment

### Low Risk ✅
- **Phase 2 optimization:** Standard Arrow join operation, well-tested
- **Phase 4 (WITH):** Reuses existing projection/aggregation logic

### Medium Risk ⚠️
- **Phase 3 (Multiple MATCH):** Join logic needs careful testing
  - Edge case: Empty first MATCH result
  - Edge case: No shared variables (should error)
  - Edge case: Multiple shared variables

### Known Issues 🔴
1. **Query 8 result discrepancy:** 329K paths vs 58M paths (Kuzu)
   - May be dataset difference or counting methodology
   - Needs investigation (separate from this plan)

2. **Performance gap on complex queries:** Some queries 5-6x slower than Kuzu
   - Acceptable for initial implementation
   - Optimization can come later (Phase 5+)

---

## Success Criteria

### Phase 2 Optimization ✅
- Query 9 completes in <150ms (currently times out)
- WHERE clause with property access 100x+ faster
- All WHERE clause unit tests pass

### Phase 3 (Multiple MATCH) ✅
- Query 2 executes successfully
- All queries with multiple MATCH work (Q2, Q5, Q6, Q7)
- Shared variable join logic correct

### Phase 4 (WITH Clause) ✅
- All 9 benchmark queries execute successfully
- WITH projections work correctly
- WITH + WHERE filtering works
- WITH + ORDER BY + LIMIT works (top-K)

### Overall Success ✅
- **9/9 queries working:** All benchmark queries return results
- **Correctness:** Results match expected values (where comparable)
- **Performance:** Reasonable execution times (<1s per query)
- **Stability:** No crashes, consistent results across runs

---

## Timeline Estimate

| Phase | Task | Time | Dependencies |
|-------|------|------|--------------|
| **Phase 2 opt** | Property access hash join | 30 min | None |
| **Phase 3** | Multiple MATCH support | 1-2 hours | Phase 2 opt (recommended) |
| **Phase 4** | WITH clause support | 1 hour | Phase 3 |
| **Testing** | Integration testing | 30 min | All phases |
| **Documentation** | Update README, status docs | 30 min | All phases |

**Total:** ~3-4 hours to complete all phases + testing + documentation

**Recommended sequence:**
1. Phase 2 optimization (30 min) - Critical for usability
2. Phase 3: Multiple MATCH (1-2 hours) - Required for Q2, Q5, Q6, Q7
3. Phase 4: WITH clause (1 hour) - Required for Q2, Q5, Q6, Q7
4. Testing & documentation (1 hour)

---

## Future Enhancements (Post-Benchmark)

### Phase 5: Performance Optimization (2-4 hours)
- [ ] Filter pushdown into pattern matching
- [ ] Join reordering for multi-MATCH queries
- [ ] Predicate pushdown for WHERE clauses
- [ ] Index usage hints

### Phase 6: Advanced Features (4-8 hours)
- [ ] UNION queries
- [ ] OPTIONAL MATCH (left join semantics)
- [ ] Path expressions in RETURN
- [ ] Aggregations in WHERE (HAVING-like)
- [ ] Subqueries

### Phase 7: Query Optimizer (8-16 hours)
- [ ] Cost-based optimization
- [ ] Statistics-based query planning
- [ ] Adaptive execution
- [ ] Query result caching

---

## Appendix: Key Files & Line Numbers

### Cypher Translator
**File:** `sabot/_cython/graph/compiler/cypher_translator.py`

| Lines | Component | Status |
|-------|-----------|--------|
| 45-62 | `__init__` - Initialize translator | ✅ Complete |
| 64-105 | `translate()` - Main entry point | ⚠️ Needs multi-MATCH |
| 270-310 | `_translate_match()` - MATCH clause translation | ✅ Complete |
| 312-356 | `_translate_2hop()` - 2-hop pattern | ✅ Complete |
| 358-403 | `_translate_3hop()` - 3-hop pattern | ✅ Complete |
| 405-450 | `_translate_variable_length()` - Var-length paths | ✅ Complete |
| 452-473 | `_apply_where_filter()` - WHERE clause main | ✅ Complete |
| 475-530 | `_evaluate_where_expression()` - WHERE evaluation | ✅ Complete |
| 532-572 | `_evaluate_filter_operand()` - Operand evaluation | ✅ Complete |
| 574-626 | `_evaluate_filter_function()` - Function calls | ✅ Complete |
| 628-679 | `_apply_return()` - RETURN clause | ✅ Complete |
| 681-740 | `_apply_aggregation()` - Aggregations | ✅ Complete |
| 780-807 | `_apply_projection()` - Projections | ✅ Complete |
| 809-814 | `_apply_order_by()` - ORDER BY | ✅ Complete |
| 816-837 | `_evaluate_property_access()` - Property access | ⚠️ NEEDS OPTIMIZATION |

**New methods to add:**
- `_translate_match_with_context()` - Phase 3
- `_find_shared_variables()` - Phase 3
- `_join_on_shared_variables()` - Phase 3
- `_apply_with()` - Phase 4
- `_apply_where_filter_simple()` - Phase 4
- `_evaluate_where_expression_simple()` - Phase 4

### Benchmark Runner
**File:** `benchmarks/kuzu_study/run_benchmark_cypher.py`

| Lines | Component | Status |
|-------|-----------|--------|
| 155-163 | `__init__` - Initialize runner | ✅ Complete |
| 164-294 | `load_data()` - Multi-node/edge loading | ✅ Complete |
| 296-339 | `show_query_support()` - Display status | ⏳ Update after phases |
| 341-381 | `run_supported_queries()` - Execute queries | ⏳ Update after phases |

---

**Document Version:** 1.0
**Last Updated:** October 11, 2025
**Next Review:** After Phase 2 optimization complete
