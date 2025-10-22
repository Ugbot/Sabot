# Cypher Engine Status

**Last Updated:** October 11, 2025

---

## Component Status

### ✅ Cypher Parser (Lark-based) - COMPLETE

**Status:** 100% functional for all 9 benchmark queries

**What Works:**
- ✅ All 9 queries parse successfully
- ✅ WITH clauses (Q2, Q5, Q6, Q7)
- ✅ Complex WHERE clauses with functions
- ✅ Aggregations in AST (COUNT, AVG)
- ✅ Property access (a.name, b.age)
- ✅ ORDER BY, LIMIT, SKIP
- ✅ Variable-length paths (*1..3)
- ✅ Multiple MATCH clauses

**Parser Test Results:**
```
Query 1: ✅ PARSED (1 MATCH, 0 WITH, RETURN)
Query 2: ✅ PARSED (2 MATCH, 1 WITH, RETURN)  ← WITH clause!
Query 3: ✅ PARSED (1 MATCH, 0 WITH, RETURN)
Query 4: ✅ PARSED (1 MATCH, 0 WITH, RETURN)
Query 5: ✅ PARSED (2 MATCH, 1 WITH, RETURN)  ← WITH clause!
Query 6: ✅ PARSED (2 MATCH, 1 WITH, RETURN)  ← WITH clause!
Query 7: ✅ PARSED (2 MATCH, 1 WITH, RETURN)  ← WITH clause!
Query 8: ✅ PARSED (1 MATCH, 0 WITH, RETURN)
Query 9: ✅ PARSED (1 MATCH, 0 WITH, RETURN)

Result: 9/9 queries parsed successfully (100%)
```

**Implementation:**
- File: `/grammar/cypher.lark` (267 lines)
- File: `/sabot/_cython/graph/compiler/lark_transformer.py` (~700 lines)
- File: `/sabot/_cython/graph/compiler/cypher_parser.py` (~100 lines)
- Based on: Official openCypher M23 EBNF grammar
- Algorithm: Earley parser (handles ambiguous grammars)

---

### ❌ CypherTranslator (Execution Engine) - INCOMPLETE

**Status:** ~20% functional - pattern matching only

**What Works:**
- ✅ Pattern matching: (a)-[r]->(b)
- ✅ Node labels: (a:Person)
- ✅ Edge types: -[r:KNOWS]->
- ✅ Variable-length paths: -[r*1..3]->
- ✅ Basic RETURN (variable names only)
- ✅ LIMIT clause

**What's Missing (Blocks All 9 Queries):**
- ❌ Aggregations (COUNT, SUM, AVG, etc.)
- ❌ Property access in RETURN (a.name, b.age)
- ❌ WHERE clause evaluation
- ❌ ORDER BY
- ❌ WITH clause execution
- ❌ Multiple MATCH clauses
- ❌ SKIP clause

**Translator Test Results:**
```
Query 1: ❌ NOT SUPPORTED (needs COUNT, property access, ORDER BY)
Query 2: ❌ NOT SUPPORTED (needs WITH, COUNT, multiple MATCH)
Query 3: ❌ NOT SUPPORTED (needs AVG, WHERE, ORDER BY)
Query 4: ❌ NOT SUPPORTED (needs WHERE, COUNT, ORDER BY)
Query 5: ❌ NOT SUPPORTED (needs WHERE, WITH, COUNT)
Query 6: ❌ NOT SUPPORTED (needs WHERE, WITH, COUNT, ORDER BY)
Query 7: ❌ NOT SUPPORTED (needs WHERE, WITH, COUNT, ORDER BY)
Query 8: ⚠️  PARTIAL (pattern matching works, COUNT missing)
Query 9: ⚠️  PARTIAL (pattern matching works, WHERE + COUNT missing)

Result: 0/9 queries fully supported, 2/9 partial
```

**Implementation:**
- File: `/sabot/_cython/graph/compiler/cypher_translator.py` (~850 lines)
- Current: Only translates basic pattern matching to `match_2hop`, `match_3hop`
- Missing: All query features beyond pattern matching

---

## Why the Confusion?

### The Parser Works, The Executor Doesn't

```
┌─────────────────┐
│  Cypher Query   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  CypherParser   │  ✅ COMPLETE - Parses query into AST
│   (Lark-based)  │     All 9 queries parse successfully
└────────┬────────┘
         │
         │  Produces: CypherQuery AST
         │            ├── match_clauses
         │            ├── with_clauses
         │            ├── where_clause
         │            └── return_clause
         ▼
┌─────────────────┐
│CypherTranslator │  ❌ INCOMPLETE - Can't execute most AST features
│  (Executor)     │     Only handles basic pattern matching
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Query Result  │  ❌ FAILS - Missing feature implementation
└─────────────────┘
```

**Example: Query 1**
```cypher
MATCH (follower:Person)-[:Follows]->(person:Person)
RETURN person.id AS personID, person.name AS name, count(follower.id) AS numFollowers
ORDER BY numFollowers DESC LIMIT 3
```

**Parser:** ✅ Parses perfectly
```python
CypherQuery(
    match_clauses=[
        MatchClause(pattern=PatternElement(...))
    ],
    return_clause=ReturnClause(
        items=[
            ProjectionItem(expression=PropertyAccess('person', 'id'), alias='personID'),
            ProjectionItem(expression=PropertyAccess('person', 'name'), alias='name'),
            ProjectionItem(expression=FunctionCall('count', ...), alias='numFollowers')
        ],
        order_by=[OrderBy(expression=Variable('numFollowers'), ascending=False)],
        limit=3
    )
)
```

**Translator:** ❌ Fails
```
Error: Property access not implemented in RETURN clause
Error: COUNT aggregation not implemented
Error: ORDER BY not implemented
```

---

## Current Workaround: Code Mode

Since the Cypher engine is incomplete, we use **direct Python queries** instead:

```python
# Instead of Cypher:
# MATCH (a)-[:KNOWS]->(b) RETURN count(*) AS n

# We write Python:
from sabot._cython.graph.query import match_2hop

edges = ca.table({'source': [...], 'target': [...]})
result = match_2hop(edges, edges)
count = result.num_matches()
```

**This is why the benchmark uses `queries.py` (Python) not Cypher!**

---

## Why Is Parser Complete But Translator Incomplete?

### Parser Was Easy
- Official openCypher grammar already existed
- Lark handles grammar parsing automatically
- Just needed to convert EBNF → Lark format
- Transformer maps parse tree → AST
- **Effort:** ~1 day

### Translator Is Hard
- Must implement every Cypher feature
- Requires deep graph query optimization
- Need to map Cypher semantics → Arrow operations
- Complex cases: nested aggregations, correlated subqueries
- **Effort:** ~3-6 weeks

---

## Implementation Priority

### Current Status (Oct 11, 2025)

1. ✅ **Parser:** Complete (1 day effort)
2. ❌ **Translator:** 20% complete (~1 week of 3-6 weeks)

### What We Have

**High-Performance Python Queries** (Code Mode)
- Query 1-9 implemented in `queries.py` / `queries_vectorized.py`
- Performance: 297ms total (2.03x faster than Kuzu)
- Fully functional, production-ready

**Incomplete Cypher Engine**
- Parser works
- Translator doesn't
- Can't run benchmark queries yet

### What We Need for Cypher Mode

**Translator Features (Priority Order):**

1. **P0 - Aggregations** (5-7 days)
   - COUNT, SUM, AVG, MIN, MAX
   - Group-by support
   - Global vs grouped aggregation

2. **P0 - Property Access** (2-3 days)
   - Join with vertex table
   - Extract properties in RETURN
   - Property filters in WHERE

3. **P0 - WHERE Clause** (3-4 days)
   - Expression evaluation
   - Comparison operators
   - Boolean logic (AND, OR, NOT)
   - Function calls (lower, upper, etc.)

4. **P1 - ORDER BY** (1-2 days)
   - Sort by expression
   - ASC/DESC
   - Multiple sort keys

5. **P1 - WITH Clause** (4-5 days)
   - Pipeline execution
   - Intermediate results
   - Variable scoping

6. **P2 - Multiple MATCH** (3-4 days)
   - Sequential pattern matching
   - Join intermediate results

**Total Effort:** ~3-4 weeks of full-time work

---

## Recommendation

### Option 1: Continue with Code Mode (Recommended)

**Pros:**
- Already working and fast
- Can optimize Query 7 now (vectorize for 3-4x speedup)
- Production-ready
- No waiting for translator

**Cons:**
- Not using new parser
- Less "elegant" than Cypher

**Timeline:**
- Query 7 optimization: 1 day
- Ready to deploy

### Option 2: Complete Cypher Translator First

**Pros:**
- Use new Lark parser
- "Pure" Cypher execution
- Nice demo

**Cons:**
- 3-4 weeks of work
- No immediate performance benefit
- Delays Query 7 optimization

**Timeline:**
- Translator implementation: 3-4 weeks
- Then Query 7 optimization: 1 day
- Ready to deploy: 1 month

### Option 3: Hybrid Approach

**Phase 1 (This Week):**
- Optimize Query 7 in code mode (1 day)
- Deploy with 2.5x overall speedup

**Phase 2 (Next Month):**
- Implement translator features (3-4 weeks)
- Run Cypher benchmark
- Compare modes

**Best of Both Worlds:**
- Immediate performance wins
- Future Cypher support
- Phased development

---

## Conclusion

**Parser Status:** ✅ **COMPLETE**
- Parses all 9 benchmark queries perfectly
- Handles complex Cypher syntax (WITH, aggregations, etc.)
- Production-ready

**Translator Status:** ❌ **20% COMPLETE**
- Only basic pattern matching works
- Missing all query features (aggregations, WHERE, ORDER BY, WITH, etc.)
- 3-4 weeks of work remaining

**Current Approach:** Use code mode (Python queries)
- Already 2.03x faster than Kuzu
- Query 7 can be optimized for 2.5x overall
- No dependency on incomplete translator

**Recommendation:**
1. Optimize Query 7 in code mode (1 day) ← **Do this now**
2. Complete translator later (3-4 weeks) ← **Nice to have**

**The parser is done. The executor is not. But we don't need it yet because code mode is already winning!**
