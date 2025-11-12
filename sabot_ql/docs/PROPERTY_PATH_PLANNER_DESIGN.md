# Property Path Planner Integration Design

## Current Status

**AST**: ✅ Complete - PropertyPath, PropertyPathElement, PropertyPathModifier enums all defined
**Operators**: ✅ Complete - C++ kernels for transitive closure, sequence, alternative, filter
**Planner**: ❌ Returns `NotImplemented` at line 282-284 in planner.cpp

## Integration Strategy

### 1. Property Path Expansion Approach

Instead of trying to execute property paths directly, we'll **expand** them into equivalent triple patterns that use the graph operators.

**Example Query**:
```sparql
SELECT ?person ?ancestor
WHERE {
  ?person foaf:knows+ ?ancestor .
}
```

**Expansion**:
```
1. Build CSR graph from foaf:knows edges
2. Run transitive_closure(csr, all_persons, min_dist=1)
3. Bind results to ?person and ?ancestor variables
```

### 2. Implementation Plan

#### Phase 5.1: Property Path Planner Function (planner.cpp:282-400)

Replace the `NotImplemented` error with:

```cpp
if (pattern.IsPredicatePropertyPath()) {
    const auto& path = std::get<PropertyPath>(pattern.predicate);
    return PlanPropertyPath(pattern, path, ctx);
}
```

#### Phase 5.2: Property Path Planning Logic

**File**: `sabot_ql/src/sparql/property_path_planner.cpp` (new)

**Key Functions**:

1. **`PlanPropertyPath(TriplePattern, PropertyPath, Context)`**
   - Main entry point
   - Analyzes property path structure
   - Dispatches to appropriate operator

2. **`PlanTransitivePath(subject, pred, object, quantifier, ctx)`**
   - Handles p+, p*, p{m,n}
   - Builds CSR from predicate edges
   - Calls transitive_closure operator
   - Returns RecordBatch with (subject, object) bindings

3. **`PlanSequencePath(subject, path_elements, object, ctx)`**
   - Handles p/q composition
   - Plans each sub-path recursively
   - Calls sequence_path operator
   - Joins intermediate results

4. **`PlanAlternativePath(subject, path_alternatives, object, ctx)`**
   - Handles p|q alternatives
   - Plans each alternative
   - Calls alternative_path operator
   - Unions results

5. **`PlanNegatedPath(subject, excluded_preds, object, ctx)`**
   - Handles !p negation
   - Loads all edges
   - Calls filter_by_predicate operator
   - Returns filtered edges

### 3. Operator Integration

**Graph Build**:
```cpp
// Load edges for specific predicate
auto edges = store->ScanPredicate(predicate_id);

// Build CSR
ARROW_ASSIGN_OR_RAISE(auto csr, sabot::graph::BuildCSRGraph(
    edges, "subject", "object"
));
```

**Transitive Closure**:
```cpp
// Get start nodes (bound subjects or all subjects)
auto start_nodes = GetBoundVariables(subject, ctx);

// Run transitive closure
ARROW_ASSIGN_OR_RAISE(auto result, sabot::graph::TransitiveClosure(
    csr, start_nodes, min_dist, max_dist
));
```

**Sequence/Alternative**:
```cpp
// Plan sub-paths
auto path_p = PlanPropertyPath(subject, path.elements[0], intermediate_var, ctx);
auto path_q = PlanPropertyPath(intermediate_var, path.elements[1], object, ctx);

// Compose
ARROW_ASSIGN_OR_RAISE(auto result, sabot::graph::SequencePath(path_p, path_q));
```

### 4. Variable Binding

Property path results must bind to SPARQL variables:

```cpp
// Result has columns (start, end, dist)
// Map to (?subject, ?object) variables

VariableBindings bindings;
bindings[subject_var] = result.column("start");
bindings[object_var] = result.column("end");

return bindings;
```

### 5. Query Examples

#### Simple Transitive Path (p+):
```sparql
?person foaf:knows+ ?friend
```
→ `transitive_closure(csr_knows, [person_id], min_dist=1)`

#### Bounded Path (p{2,3}):
```sparql
?person foaf:knows{2,3} ?friend
```
→ `transitive_closure(csr_knows, [person_id], min_dist=2, max_dist=3)`

#### Sequence Path (p/q):
```sparql
?person foaf:knows/foaf:name ?name
```
→ `sequence_path(knows_edges, name_edges)`

#### Alternative Path (p|q):
```sparql
?person (foaf:knows | foaf:worksWith) ?contact
```
→ `alternative_path(knows_edges, worksWith_edges)`

#### Inverse Path (^p):
```sparql
?person ^foaf:knows ?knower  # who knows person?
```
→ `transitive_closure(reverse_csr_knows, [person_id], min_dist=1, max_dist=1)`

#### Negated Path (!p):
```sparql
?person !foaf:knows ?other  # any relationship except knows
```
→ `filter_by_predicate(all_edges, excluded=[knows_id])`

### 6. Optimization Notes

1. **CSR Caching**: Cache CSR graphs per predicate to avoid rebuilding
2. **Partial Evaluation**: If subject is bound, only traverse from that node
3. **Index Selection**: Use SPO/POS/OSP indexes based on bound variables
4. **Result Cardinality**: Estimate result size for join order optimization

### 7. Testing Strategy

**Unit Tests**: Test each operator in isolation
**Integration Tests**: End-to-end SPARQL queries with property paths
**Performance Tests**: Compare to QLever on standard benchmarks

### 8. Implementation Order

1. ✅ Phase 1-3: Operators implemented
2. **Phase 5.1**: Add `PlanPropertyPath` stub to planner.cpp
3. **Phase 5.2**: Implement transitive path planning (p+, p*)
4. **Phase 5.3**: Implement sequence/alternative planning
5. **Phase 5.4**: Implement inverse/negated planning
6. **Phase 5.5**: Variable binding integration
7. **Phase 5.6**: End-to-end testing

### 9. Success Criteria

- [ ] All property path queries parse correctly
- [ ] Results match expected SPARQL 1.1 semantics
- [ ] Performance within 2x of QLever
- [ ] No memory leaks or crashes
- [ ] Olympics dataset queries work (50K triples)

## Next Steps

Start with Phase 5.1: Create property_path_planner.cpp and wire it into planner.cpp.
