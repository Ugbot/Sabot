borrow # KuzuDB Cypher Reference for Sabot Phase 4.3

**Quick reference for implementing Cypher query compiler using KuzuDB as inspiration**

---

## Overview

KuzuDB provides a complete Cypher implementation that we can use as reference. This document highlights the most useful parts for Sabot's Phase 4.3: Cypher Query Compiler.

---

## 1. Cypher Grammar (ANTLR4)

**File:** `vendor/kuzu/src/antlr4/Cypher.g4`

### Key Grammar Rules for Pattern Matching

#### MATCH Clause (line 312)
```antlr
oC_Match
    : ( OPTIONAL SP )? MATCH SP? oC_Pattern ( SP oC_Where )? ( SP kU_Hint )? ;
```

#### Pattern Structure (line 386)
```antlr
oC_Pattern
    : oC_PatternPart ( SP? ',' SP? oC_PatternPart )* ;

oC_PatternPart
    :  ( oC_Variable SP? '=' SP? oC_AnonymousPatternPart )
        | oC_AnonymousPatternPart ;

oC_PatternElement
    : ( oC_NodePattern ( SP? oC_PatternElementChain )* )
        | ( '(' oC_PatternElement ')' )
        ;
```

#### Node Pattern (line 401)
```antlr
oC_NodePattern
    : '(' SP? ( oC_Variable SP? )? ( oC_NodeLabels SP? )? ( kU_Properties SP? )? ')' ;

oC_NodeLabels
    :  ':' SP? oC_LabelName ( SP? ('|' ':'? | ':') SP? oC_LabelName )* ;
```

Examples:
- `(a)` - Node with variable `a`
- `(a:Person)` - Node with variable `a` and label `Person`
- `(a:Person {age: 25})` - Node with variable, label, and properties
- `(a:Person|Company)` - Node with multiple possible labels

#### Edge Pattern (line 407)
```antlr
oC_RelationshipPattern
    : ( oC_LeftArrowHead SP? oC_Dash SP? oC_RelationshipDetail? SP? oC_Dash )  // <-[]- (incoming)
        | ( oC_Dash SP? oC_RelationshipDetail? SP? oC_Dash SP? oC_RightArrowHead )  // -[]-> (outgoing)
        | ( oC_Dash SP? oC_RelationshipDetail? SP? oC_Dash )  // -[]- (undirected)
        ;

oC_RelationshipDetail
    : '[' SP? ( oC_Variable SP? )? ( oC_RelationshipTypes SP? )? ( kU_RecursiveDetail SP? )? ( kU_Properties SP? )? ']' ;

oC_RelationshipTypes
    :  ':' SP? oC_RelTypeName ( SP? '|' ':'? SP? oC_RelTypeName )* ;
```

Examples:
- `-[r]->` - Outgoing edge with variable `r`
- `-[r:KNOWS]->` - Outgoing edge with type `KNOWS`
- `-[r:KNOWS|FOLLOWS]->` - Edge with multiple possible types
- `-[r:KNOWS {since: 2020}]->` - Edge with properties
- `-[r:KNOWS*1..3]->` - Variable-length path (1-3 hops)

#### Variable-Length Paths (line 428)
```antlr
kU_RecursiveDetail
    : '*' ( SP? kU_RecursiveType)? ( SP? oC_RangeLiteral )? ( SP? kU_RecursiveComprehension )? ;

oC_RangeLiteral
    :  oC_LowerBound? SP? DOTDOT SP? oC_UpperBound?
        | oC_IntegerLiteral ;
```

Examples:
- `*` - Any length
- `*2` - Exactly 2 hops
- `*1..3` - Between 1 and 3 hops
- `*..5` - Up to 5 hops
- `*3..` - At least 3 hops

---

## 2. Query Structure

### Complete Query Flow (line 257)
```antlr
oC_Query
    : oC_RegularQuery ;

oC_RegularQuery
    : oC_SingleQuery ( SP? oC_Union )* ;

oC_SingleQuery
    : oC_SinglePartQuery
        | oC_MultiPartQuery ;

oC_SinglePartQuery
    : ( oC_ReadingClause SP? )* oC_Return
        | ( ( oC_ReadingClause SP? )* oC_UpdatingClause ( SP? oC_UpdatingClause )* ( SP? oC_Return )? ) ;
```

**Reading Clauses** (line 292):
- `MATCH` - Pattern matching
- `UNWIND` - List expansion
- `LOAD FROM` - Data loading
- `CALL` - Procedure calls

**Updating Clauses** (line 285):
- `CREATE` - Create nodes/edges
- `MERGE` - Create if not exists
- `SET` - Update properties
- `DELETE` - Delete nodes/edges

### WHERE Clause (line 383)
```antlr
oC_Where
    : WHERE SP oC_Expression ;
```

### RETURN Clause (line 351)
```antlr
oC_Return
    : RETURN oC_ProjectionBody ;

oC_ProjectionBody
    : ( SP? DISTINCT )? SP oC_ProjectionItems (SP oC_Order )? ( SP oC_Skip )? ( SP oC_Limit )? ;

oC_ProjectionItems
    : ( STAR ( SP? ',' SP? oC_ProjectionItem )* )
        | ( oC_ProjectionItem ( SP? ',' SP? oC_ProjectionItem )* ) ;

oC_ProjectionItem
    : ( oC_Expression SP AS SP oC_Variable )
        | oC_Expression ;
```

Examples:
- `RETURN *` - Return all variables
- `RETURN a, b` - Return specific variables
- `RETURN a.name AS name, b.age` - Return properties with aliases
- `RETURN DISTINCT a.name` - Return unique values
- `RETURN a ORDER BY a.age DESC LIMIT 10` - Ordered and limited results

---

## 3. Expression Grammar

### Expression Hierarchy (line 460)
```antlr
oC_Expression
    : oC_OrExpression ;

oC_OrExpression
    : oC_XorExpression ( SP OR SP oC_XorExpression )* ;

oC_XorExpression
    : oC_AndExpression ( SP XOR SP oC_AndExpression )* ;

oC_AndExpression
    : oC_NotExpression ( SP AND SP oC_NotExpression )* ;

oC_NotExpression
    : ( NOT SP? )*  oC_ComparisonExpression;

oC_ComparisonExpression
    : kU_BitwiseOrOperatorExpression ( SP? kU_ComparisonOperator SP? kU_BitwiseOrOperatorExpression )? ;

kU_ComparisonOperator : '=' | '<>' | '<' | '<=' | '>' | '>=' ;
```

**Operator Precedence** (highest to lowest):
1. Property access (`.`)
2. Power (`^`)
3. Unary operators (`-`, `NOT`)
4. Multiplication, division, modulo (`*`, `/`, `%`)
5. Addition, subtraction (`+`, `-`)
6. Bit shift (`>>`, `<<`)
7. Bitwise AND (`&`)
8. Bitwise OR (`|`)
9. Comparison (`=`, `<>`, `<`, `<=`, `>`, `>=`)
10. NOT (`NOT`)
11. AND (`AND`)
12. XOR (`XOR`)
13. OR (`OR`)

### Property Access (line 620)
```antlr
oC_PropertyLookup
    : '.' SP? ( oC_PropertyKeyName | STAR ) ;

oC_PropertyExpression
    : oC_Atom SP? oC_PropertyLookup ;
```

Examples:
- `a.name` - Single property
- `a.address.city` - Nested property
- `a.*` - All properties

### Function Calls (line 595)
```antlr
oC_FunctionInvocation
    : COUNT SP? '(' SP? '*' SP? ')'
        | CAST SP? '(' SP? kU_FunctionParameter SP? ( ( AS SP? kU_DataType ) | ( ',' SP? kU_FunctionParameter ) ) SP? ')'
        | oC_FunctionName SP? '(' SP? ( DISTINCT SP? )? ( kU_FunctionParameter SP? ( ',' SP? kU_FunctionParameter SP? )* )? ')' ;
```

Examples:
- `count(*)` - Count all rows
- `count(DISTINCT a.name)` - Count unique names
- `sum(a.age)` - Sum ages
- `avg(a.salary)` - Average salary

---

## 4. Example Cypher Queries with Grammar Mapping

### Example 1: Simple MATCH
```cypher
MATCH (a:Person)-[:KNOWS]->(b:Person)
RETURN a.name, b.name
```

**Grammar Breakdown:**
1. `oC_Match`: MATCH clause
2. `oC_Pattern`: `(a:Person)-[:KNOWS]->(b:Person)`
   - `oC_NodePattern`: `(a:Person)` - node with variable `a` and label `Person`
   - `oC_RelationshipPattern`: `-[:KNOWS]->` - outgoing edge with type `KNOWS`
   - `oC_NodePattern`: `(b:Person)` - node with variable `b` and label `Person`
3. `oC_Return`: RETURN clause
   - `oC_ProjectionItem`: `a.name` - property expression
   - `oC_ProjectionItem`: `b.name` - property expression

### Example 2: MATCH with WHERE
```cypher
MATCH (a:Person)-[:KNOWS]->(b:Person)
WHERE a.age > 18 AND b.age < 65
RETURN a.name, b.name, a.age
ORDER BY a.age DESC
LIMIT 10
```

**Grammar Breakdown:**
1. `oC_Match`: MATCH with pattern
2. `oC_Where`: WHERE clause
   - `oC_AndExpression`: `a.age > 18 AND b.age < 65`
     - `oC_ComparisonExpression`: `a.age > 18`
     - `oC_ComparisonExpression`: `b.age < 65`
3. `oC_Return`: RETURN with ORDER BY and LIMIT
   - `oC_ProjectionItems`: `a.name, b.name, a.age`
   - `oC_Order`: `ORDER BY a.age DESC`
   - `oC_Limit`: `LIMIT 10`

### Example 3: Variable-Length Path
```cypher
MATCH (a:Person)-[:KNOWS*1..3]->(b:Person)
WHERE a.name = 'Alice'
RETURN b.name, length(path) AS hops
```

**Grammar Breakdown:**
1. `oC_Match`: MATCH with variable-length path
2. `oC_RelationshipDetail`: `[:KNOWS*1..3]`
   - `kU_RecursiveDetail`: `*1..3` - 1 to 3 hops
3. `oC_Where`: Filter by Alice
4. `oC_Return`: Return name and path length

---

## 5. Translation Strategy for Sabot

### Step 1: Parse Cypher Query
**Options:**
1. Use ANTLR4 with KuzuDB grammar (most compatible)
2. Use pyparsing or Lark (Python-native)
3. Use custom recursive descent parser (lightweight)

**Recommendation:** Start with pyparsing or Lark for Python integration

### Step 2: Build AST
**AST Node Types:**
```python
# Query nodes
class CypherQuery:
    clauses: List[Clause]

class MatchClause:
    pattern: Pattern
    where: Optional[Expression]
    optional: bool = False

class ReturnClause:
    items: List[ProjectionItem]
    distinct: bool = False
    order_by: Optional[OrderBy] = None
    skip: Optional[int] = None
    limit: Optional[int] = None

# Pattern nodes
class Pattern:
    parts: List[PatternPart]

class PatternPart:
    variable: Optional[str]
    element: PatternElement

class NodePattern:
    variable: Optional[str]
    labels: List[str]
    properties: Dict[str, Any]

class EdgePattern:
    variable: Optional[str]
    types: List[str]
    direction: str  # '->', '<-', '-'
    var_length: Optional[Tuple[int, int]]  # (min, max) hops
    properties: Dict[str, Any]

# Expression nodes
class PropertyAccess:
    variable: str
    property: str

class Comparison:
    left: Expression
    op: str  # '=', '<>', '<', '<=', '>', '>='
    right: Expression

class BinaryOp:
    left: Expression
    op: str  # 'AND', 'OR', '+', '-', etc.
    right: Expression
```

### Step 3: Translate to Sabot Pattern Matching
**Mapping Cypher to Sabot:**

```python
def translate_match_clause(match: MatchClause, graph: GraphQueryEngine):
    """
    Translate Cypher MATCH to Sabot pattern matching.

    Example:
        MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.age > 18
        →
        persons = filter_vertices_by_label(graph, 'Person')
        persons_filtered = filter_by_property(persons, 'age', '>', 18)
        knows_edges = filter_edges_by_type(graph, 'KNOWS')
        result = match_2hop(persons_filtered, knows_edges)
    """
    # Extract pattern
    pattern = match.pattern

    # Identify pattern type
    if is_2hop_pattern(pattern):
        return translate_2hop(pattern, match.where, graph)
    elif is_3hop_pattern(pattern):
        return translate_3hop(pattern, match.where, graph)
    elif is_var_length_pattern(pattern):
        return translate_var_length(pattern, match.where, graph)
    else:
        raise NotImplementedError(f"Pattern not supported: {pattern}")

def translate_2hop(pattern, where, graph):
    """Translate 2-hop pattern to match_2hop call."""
    # Parse pattern: (a:Label1)-[:EdgeType]->(b:Label2)
    node1, edge, node2 = parse_2hop_pattern(pattern)

    # Filter vertices by label
    vertices1 = filter_vertices_by_label(graph, node1.labels[0])
    vertices2 = filter_vertices_by_label(graph, node2.labels[0])

    # Filter edges by type
    edges = filter_edges_by_type(graph, edge.types[0])

    # Apply WHERE filters
    if where:
        vertices1 = apply_where_filter(vertices1, where, node1.variable)

    # Execute pattern match
    result = match_2hop(vertices1, edges)

    return result
```

---

## 6. Implementation Checklist for Phase 4.3

### Minimal Viable Cypher Compiler

**Priority 1: Core MATCH**
- [ ] Parse simple MATCH pattern: `(a)-[r]->(b)`
- [ ] Parse node labels: `(a:Person)`
- [ ] Parse edge types: `-[r:KNOWS]->`
- [ ] Parse WHERE filters: `WHERE a.age > 18`
- [ ] Translate to `match_2hop()`

**Priority 2: RETURN**
- [ ] Parse RETURN clause: `RETURN a, b`
- [ ] Parse property access: `RETURN a.name, b.age`
- [ ] Parse LIMIT: `LIMIT 10`
- [ ] Project result columns

**Priority 3: Variable-Length Paths**
- [ ] Parse `*1..3` syntax
- [ ] Translate to `match_variable_length_path()`

**Priority 4: Advanced Features**
- [ ] 3-hop patterns
- [ ] Multiple MATCH clauses
- [ ] ORDER BY
- [ ] DISTINCT
- [ ] Aggregations (COUNT, SUM, AVG)

---

## 7. Quick Reference: Common Cypher Patterns

### Friend-of-Friend
```cypher
MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)
WHERE a.name = 'Alice' AND a <> c
RETURN c.name
```

### Shortest Path
```cypher
MATCH path = (a:Person)-[:KNOWS*..5]->(b:Person)
WHERE a.name = 'Alice' AND b.name = 'Bob'
RETURN path
ORDER BY length(path)
LIMIT 1
```

### Graph Analytics
```cypher
MATCH (a:Person)-[:FOLLOWS]->(b:Person)
RETURN a.name, count(b) AS follower_count
ORDER BY follower_count DESC
LIMIT 10
```

### Fraud Detection
```cypher
MATCH (a:Account)-[:TRANSFER {amount: > 10000}]->(b:Account)
WHERE a.risk_score > 0.8
RETURN a.id, b.id, a.risk_score
```

---

## 8. KuzuDB Source Files to Study

### For Parser Implementation
1. `vendor/kuzu/src/antlr4/Cypher.g4` - **START HERE** - Grammar definition
2. `vendor/kuzu/src/parser/transformer.cpp` - AST construction
3. `vendor/kuzu/src/parser/expression/` - Expression parsing

### For Semantic Analysis
1. `vendor/kuzu/src/binder/bind/bind_graph_pattern.cpp` - Pattern binding
2. `vendor/kuzu/src/binder/bind/bind_where.cpp` - WHERE clause binding

### For Optimization
1. `vendor/kuzu/src/planner/join_order/join_order_enumerator.cpp`
2. `vendor/kuzu/src/optimizer/filter_push_down_optimizer.cpp`

---

## Next Steps

1. **Study the grammar**: Read through `Cypher.g4` to understand full Cypher syntax
2. **Choose parser library**: pyparsing, Lark, or ANTLR4
3. **Implement AST nodes**: Define Python classes for Cypher AST
4. **Build translator**: Map Cypher patterns to Sabot pattern matching
5. **Test with examples**: Start with simple MATCH queries, gradually add complexity

---

**This reference will be updated as Phase 4.3 progresses!**
