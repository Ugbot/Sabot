# Kuzu Integration Plan

**Date:** October 12, 2025  
**Status:** Ready to integrate  
**Current:** All operators implemented, ready for frontend

---

## Overview

The Kuzu frontend integration connects Cypher text parsing to our Arrow execution layer.

**Current State:**
```
❌ Cypher Text → [Missing: Parser] → ✅ ArrowPlan → ✅ Execute
```

**Target State:**
```
✅ Cypher Text → Kuzu Parse/Bind/Optimize → LogicalPlan → Translate → ArrowPlan → Execute
```

---

## Integration Approach

### Option 1: Full Kuzu Build (Complex)

**Pros:**
- Get all Kuzu functionality
- Complete parser/binder/optimizer

**Cons:**
- Complex build dependencies
- Large binary size
- References to deleted storage/processor

**Verdict:** Too complex for initial integration

---

### Option 2: Minimal Kuzu Frontend (Recommended) ✅

**Approach:**
1. Copy only parser source files we need
2. Create stub implementations for missing dependencies
3. Build just enough to get LogicalPlan

**Pros:**
- Clean, minimal
- Fast builds
- No storage dependencies

**Cons:**
- May miss some Kuzu optimizations
- Requires understanding dependencies

**Verdict:** **Use this approach**

---

### Option 3: Use Existing Lark Parser (Pragmatic) ⭐

**We already have a working Cypher parser!**

**Location:** `/Users/bengamble/Sabot/sabot/_cython/graph/compiler/`
- `cypher_parser.py` - Lark-based parser
- `lark_transformer.py` - AST builder
- `cypher_ast.py` - Cypher AST classes

**Status:** ✅ Parses all 9 benchmark queries (100%)

**Integration:**
1. Use existing Lark parser for Cypher → AST
2. Create AST → ArrowPlan translator (skip Kuzu)
3. Execute with our operators

**Pros:**
- Already working
- No complex Kuzu build
- Faster implementation (2-3 days vs 1-2 weeks)
- Proven on Q1-Q9

**Cons:**
- Miss Kuzu's optimizer
- Need to implement our own query planning

**Verdict:** **RECOMMENDED - Use existing parser!**

---

## Recommended Integration Path

### Phase 1: Use Existing Lark Parser (This Week)

**Step 1: Create Cypher AST → ArrowPlan Translator**

Location: `src/cypher/cypher_ast_translator.cpp`

```cpp
// Translate Cypher AST (from Python parser) to ArrowPlan
class CypherASTTranslator {
    arrow::Result<ArrowPlan> Translate(const CypherQuery& query) {
        ArrowPlan plan;
        
        // Translate MATCH clauses
        for (auto& match : query.match_clauses) {
            TranslateMatch(match, plan);
        }
        
        // Translate WHERE
        if (query.where_clause) {
            TranslateWhere(query.where_clause, plan);
        }
        
        // Translate RETURN
        TranslateReturn(query.return_clause, plan);
        
        return plan;
    }
};
```

**Step 2: Python Bridge**

```python
# sabot_cypher/python_bridge.py
from sabot._cython.graph.compiler.cypher_parser import CypherParser
import sabot_cypher

parser = CypherParser()
bridge = sabot_cypher.SabotCypherBridge.create()

def execute_cypher(query_text, vertices, edges):
    # Parse with existing Lark parser
    ast = parser.parse(query_text)
    
    # Convert AST to ArrowPlan (via C++ or Python)
    plan = translate_ast_to_arrow_plan(ast)
    
    # Execute with sabot_cypher
    bridge.register_graph(vertices, edges)
    result = bridge.execute_plan(plan)  # New method
    
    return result
```

**Step 3: Test with Q1-Q9**

All 9 queries already parse successfully with Lark parser.

**Timeline:** 2-3 days

---

### Phase 2: Add Kuzu Frontend (Later - Optional)

**Once we have queries working with Lark:**
1. Build Kuzu frontend library
2. Compare Kuzu optimizer vs our simple planner
3. Use Kuzu if significantly better

**Timeline:** 1-2 weeks (optional enhancement)

---

## Implementation Plan: Lark Integration

### Day 1: AST Translator

**Create:** `src/cypher/cypher_ast_translator.cpp` (~300 lines)

**Translate:**
- MatchClause → Scan + Extend operators
- WhereClause → Filter operator
- ReturnClause → Project + Aggregate + OrderBy + Limit
- WithClause → Intermediate operators

**Test:** Parse Q1, translate to ArrowPlan, verify operators

---

### Day 2: Python Integration

**Create:** `sabot_cypher/python_bridge.py` (~150 lines)

**Features:**
- Import Lark parser
- Parse Cypher text
- Call C++ translator
- Execute with Arrow

**Test:** Execute Q1 end-to-end

---

### Day 3: Q1-Q9 Validation

**Run all 9 queries:**
- Parse with Lark ✅ (already works)
- Translate to ArrowPlan
- Execute with our operators
- Validate results

**Test:** All 9 queries return results

---

## Comparison: Lark vs Kuzu

| Aspect | Lark Parser | Kuzu Frontend |
|--------|-------------|---------------|
| **Parser** | ✅ Working | Complex build |
| **Q1-Q9 Support** | ✅ 100% | Unknown |
| **Optimizer** | Basic | Advanced |
| **Integration** | 2-3 days | 1-2 weeks |
| **Maintenance** | Easy | Complex |
| **Status** | Ready now | Needs work |

**Recommendation:** Start with Lark, add Kuzu later if needed.

---

## Decision Matrix

### For Immediate Q1-Q9 Support: Use Lark ⭐

**Reasons:**
1. Already parsing all 9 queries
2. 2-3 days to working queries
3. No complex build dependencies
4. Proven parser

**Approach:**
- AST translator (1 day)
- Python bridge (1 day)
- Q1-Q9 validation (1 day)

**Result:** Q1-Q9 working in 3 days

---

### For Long-term Optimization: Add Kuzu Later

**Reasons:**
1. Get queries working first
2. Benchmark with Lark parser
3. Add Kuzu optimizer if needed
4. Incremental enhancement

**Approach:**
- Get Q1-Q9 working with Lark
- Measure performance
- Build Kuzu frontend if optimization needed
- Compare results

**Result:** Data-driven decision

---

## Recommended Next Steps

### This Week (Days 1-3)

**1. Create CypherASTTranslator** (Day 1)
- Input: Cypher AST (from Lark)
- Output: ArrowPlan
- Test: Q1 translation

**2. Python Integration** (Day 2)
- Import Lark parser
- Call translator
- Execute queries
- Test: Q1 end-to-end

**3. Q1-Q9 Validation** (Day 3)
- Run all benchmarks
- Validate correctness
- Measure performance
- Document results

**Deliverable:** Q1-Q9 working with Lark parser

---

### Next Week (Optional)

**4. Build Kuzu Frontend** (if needed for optimization)
- Create minimal CMakeLists
- Build frontend library
- Link with sabot_cypher
- Compare performance

**Deliverable:** Kuzu-optimized queries (if beneficial)

---

## Implementation Code Snippet

### AST Translator (Preview)

```cpp
arrow::Result<ArrowPlan> CypherASTTranslator::TranslateMatch(
    const MatchClause& match) {
    
    ArrowPlan plan;
    
    // For pattern: (a:Person)-[:KNOWS]->(b:Person)
    // Generate:
    // 1. Scan(table="vertices", label="Person")
    // 2. Extend(edge_type="KNOWS")
    // 3. Scan(table="vertices", label="Person")  // for b
    
    for (auto& pattern : match.patterns) {
        for (auto& node : pattern.nodes) {
            if (node.label) {
                plan.operators.push_back({
                    "Scan",
                    {{"table", "vertices"},
                     {"label", node.label}}
                });
            }
        }
        
        for (auto& edge : pattern.edges) {
            plan.operators.push_back({
                "Extend",
                {{"edge_type", edge.type}}
            });
        }
    }
    
    return plan;
}
```

---

## Timeline Comparison

### With Kuzu Frontend Build
```
Week 1: Build Kuzu frontend        (3-4 days)
Week 2: Link and test               (2-3 days)
Week 3: LogicalPlan translation     (2-3 days)
Week 4: Q1-Q9 validation            (2-3 days)
────────────────────────────────────────────
Total:  3-4 weeks
```

### With Existing Lark Parser
```
Day 1: AST translator               (1 day)
Day 2: Python integration           (1 day)
Day 3: Q1-Q9 validation             (1 day)
────────────────────────────────────────────
Total:  3 days (THIS WEEK!)
```

**Savings:** 3 weeks → 3 days (7x faster)

---

## Recommendation

### ⭐ **USE EXISTING LARK PARSER**

**Rationale:**
1. Already working (100% on Q1-Q9)
2. 7x faster implementation
3. Simpler maintenance
4. Can add Kuzu optimizer later if needed

**Next Steps:**
1. Create `CypherASTTranslator` class
2. Parse Cypher AST → ArrowPlan
3. Test Q1-Q9
4. Ship it!

**Then optionally:**
5. Build Kuzu frontend (for advanced optimization)
6. Compare performance
7. Use whichever is better

---

## Conclusion

**We don't need to build Kuzu frontend immediately!**

We have a **working Cypher parser** (Lark) that handles all Q1-Q9 queries. We can:
1. Use it to get Q1-Q9 working **this week**
2. Add Kuzu frontend later if we need better optimization

**Recommended:** Proceed with Lark integration for fast Q1-Q9 support.

---

**Status:** Ready to implement Lark integration  
**Timeline:** 3 days to Q1-Q9  
**Confidence:** Very high (parser already proven)

