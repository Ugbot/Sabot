# SPARQL Test Suite Implementation Plan

**Date**: October 23, 2025
**Test Source**: W3C RDF Tests Community Group
**Location**: `sabot_ql/tests/rdf-tests/`
**Total Tests**: 389 SPARQL 1.1 queries

---

## Test Suite Overview

### Official W3C Test Repository

**Source**: https://github.com/w3c/rdf-tests
**Submodule**: `sabot_ql/tests/rdf-tests`

**Coverage**:
- ✅ SPARQL 1.0 (sparql10/)
- ✅ SPARQL 1.1 (sparql11/) - **PRIMARY TARGET**
- ✅ SPARQL 1.2 (sparql12/)

### SPARQL 1.1 Test Categories (48 categories)

**Query Features** (18 categories):
1. `aggregates/` - AVG, SUM, COUNT, MIN, MAX, GROUP_CONCAT
2. `bind/` - BIND clause
3. `bindings/` - VALUES clause
4. `construct/` - CONSTRUCT queries
5. `exists/` - EXISTS/NOT EXISTS
6. `function-library/` - Built-in functions
7. `functions/` - Custom functions
8. `grouping/` - GROUP BY
9. `negation/` - FILTER NOT EXISTS, MINUS
10. `project-expression/` - SELECT expressions
11. `property-path/` - Property paths (*, +, ?, |)
12. `subquery/` - Nested SELECT queries
13. `cast/` - Type casting
14. `csv-tsv-res/` - CSV/TSV result formats
15. `json-res/` - JSON result format
16. `service/` - Federated queries (SPARQL 1.1 Federation)
17. `service-description/` - Service descriptions
18. `entailment/` - RDF entailment regimes

**Update Operations** (10 categories):
1. `add/` - ADD operation
2. `basic-update/` - Basic update operations
3. `clear/` - CLEAR operation
4. `copy/` - COPY operation
5. `delete/` - DELETE operation
6. `delete-data/` - DELETE DATA
7. `delete-insert/` - DELETE/INSERT
8. `delete-where/` - DELETE WHERE
9. `drop/` - DROP operation
10. `move/` - MOVE operation

**Syntax** (3 categories):
1. `syntax-fed/` - Federation syntax
2. `syntax-query/` - Query syntax
3. `syntax-update-1/` - Update syntax (basic)
4. `syntax-update-2/` - Update syntax (extended)

**Protocol** (2 categories):
1. `protocol/` - SPARQL Protocol
2. `http-rdf-update/` - HTTP update operations

### Test File Structure

Each test consists of:
- **Query**: `test-name.rq` - SPARQL query
- **Expected Result**: `test-name.srx` (XML) or `test-name.srj` (JSON)
- **Data** (optional): `test-name.ttl` - RDF data in Turtle format
- **Manifest**: `manifest.ttl` - Test metadata and organization

**Example**:
```
aggregates/
├── agg-avg-01.rq          # Query: SELECT (AVG(?o) AS ?avg)
├── agg-avg-01.srx         # Expected result
├── agg01.ttl              # Test data
└── manifest.ttl           # Test organization
```

---

## Implementation Plan

### Phase 1: Test Infrastructure ✅ (Layer 2 Complete)

**Status**: Ready to start

**Prerequisites** (DONE):
- ✅ Layer 1: Storage with MarbleDB
- ✅ Layer 2: Generic operators (Scan, Join, Filter)
- ✅ Parser exists (needs integration)

**What We Have**:
- Parser: `src/sparql/parser.cpp` (parses SPARQL to AST)
- Storage: `src/storage/triple_store_impl.cpp` (persistent triples)
- Operators: `src/execution/{scan,zipper_join,filter}_operator.cpp`

**What We Need**:
- Query planner (Layer 3)
- Executor integration
- Test runner framework

---

### Phase 2: Basic Test Runner (1-2 sessions)

**Goal**: Run simplest W3C tests end-to-end

**Components**:

#### 1. Test Case Parser (C++ or Python)

```cpp
struct TestCase {
    std::string name;
    std::string query_file;      // .rq
    std::string expected_file;   // .srx or .srj
    std::string data_file;       // .ttl (optional)
    std::string category;        // "aggregates", "bind", etc.
};

class W3CTestLoader {
    // Parse manifest.ttl
    std::vector<TestCase> LoadManifest(const std::string& manifest_path);

    // Read test files
    std::string ReadQuery(const TestCase& test);
    arrow::Result<arrow::Table> ReadExpectedResults(const TestCase& test);
    arrow::Result<arrow::Table> ReadData(const TestCase& test);
};
```

#### 2. Test Runner

```cpp
class SPARQLTestRunner {
public:
    SPARQLTestRunner(std::shared_ptr<TripleStore> store,
                     std::shared_ptr<Vocabulary> vocab);

    // Run single test
    TestResult RunTest(const TestCase& test);

    // Run test category
    std::vector<TestResult> RunCategory(const std::string& category);

    // Run all tests
    TestReport RunAll();

private:
    std::shared_ptr<TripleStore> store_;
    std::shared_ptr<Vocabulary> vocab_;
    std::shared_ptr<SPARQLParser> parser_;
    std::shared_ptr<QueryPlanner> planner_;
    std::shared_ptr<QueryExecutor> executor_;
};

struct TestResult {
    std::string test_name;
    bool passed;
    std::string error_message;
    double execution_time_ms;
    size_t num_results;

    // For debugging
    arrow::Table actual_results;
    arrow::Table expected_results;
};
```

#### 3. Result Comparator

```cpp
class ResultComparator {
public:
    // Compare Arrow tables with tolerance for floating point
    bool AreEqual(const arrow::Table& actual,
                  const arrow::Table& expected,
                  double epsilon = 1e-6);

    // Detailed diff for debugging
    std::string GetDiff(const arrow::Table& actual,
                        const arrow::Table& expected);
};
```

#### 4. Simple CLI

```bash
# Run specific test
./sparql_test_runner --test aggregates/agg-avg-01

# Run category
./sparql_test_runner --category aggregates

# Run all tests
./sparql_test_runner --all

# Benchmark mode (measure performance)
./sparql_test_runner --benchmark --category aggregates
```

---

### Phase 3: Integration with Query Planner (Layer 3)

**Dependencies**: Requires Layer 3 query planner

**Pipeline**:

```
SPARQL Query (.rq)
    ↓
Parser (exists)
    ↓
AST
    ↓
Query Planner (Layer 3)
    ↓
Operator Tree (uses Layer 2 operators)
    ↓
Execute
    ↓
Arrow Table Results
    ↓
Compare with Expected (.srx/.srj)
    ↓
PASS / FAIL
```

**Example Test Execution**:

```cpp
// Test: aggregates/agg-avg-01
// Query: SELECT (AVG(?o) AS ?avg) WHERE { ?s :dec ?o }

// 1. Load test data
ARROW_ASSIGN_OR_RAISE(auto data_table, LoadTurtle("agg01.ttl"));
ARROW_RETURN_NOT_OK(store->BulkLoad(data_table));

// 2. Parse query
std::string query = ReadFile("agg-avg-01.rq");
ARROW_ASSIGN_OR_RAISE(auto ast, parser->Parse(query));

// 3. Plan query (Layer 3)
ARROW_ASSIGN_OR_RAISE(auto plan, planner->BuildPlan(ast, store));
// Plan might be:
//   Aggregate(AVG(?o) AS ?avg)
//     └─ Scan(?s :dec ?o)

// 4. Execute
ARROW_ASSIGN_OR_RAISE(auto actual_results, plan->Execute());

// 5. Load expected results
ARROW_ASSIGN_OR_RAISE(auto expected, LoadSPARQLResults("agg-avg-01.srx"));

// 6. Compare
if (AreEqual(actual_results, expected)) {
    std::cout << "✅ PASS: aggregates/agg-avg-01\n";
} else {
    std::cout << "❌ FAIL: aggregates/agg-avg-01\n";
    std::cout << GetDiff(actual_results, expected);
}
```

---

### Phase 4: Prioritized Test Categories

**Start with simplest, build up to complex**:

#### Priority 1: Basic Patterns (Week 1)
- `basic-update/` - Simple triple patterns
- `bind/` - Variable binding
- `bindings/` - VALUES clause

**Why**: Tests basic scan + filter, no complex joins

#### Priority 2: Joins & Filters (Week 2)
- `negation/` - FILTER, NOT EXISTS
- `exists/` - EXISTS patterns
- Simple multi-pattern queries

**Why**: Tests our zipper join operator

#### Priority 3: Aggregates (Week 3)
- `aggregates/` - AVG, SUM, COUNT, etc.
- `grouping/` - GROUP BY

**Why**: Tests aggregation operators (needs implementation)

#### Priority 4: Advanced Features (Week 4+)
- `subquery/` - Nested queries
- `property-path/` - Transitive patterns
- `construct/` - CONSTRUCT queries
- `project-expression/` - Complex projections

**Why**: Tests query planner optimization

#### Priority 5: Updates (Later)
- All update operations (ADD, DELETE, etc.)

**Why**: Requires write path, not just reads

---

### Phase 5: Benchmark Suite

**Goal**: Measure performance on W3C tests

**Metrics to Track**:

1. **Execution Time**
   - Parse time
   - Planning time
   - Execution time
   - Total time

2. **Memory Usage**
   - Peak memory
   - Intermediate result sizes

3. **Operator Statistics**
   - Scans performed
   - Joins executed
   - Rows processed

4. **Comparison Benchmarks**
   - vs QLever (if available)
   - vs Jena
   - vs Virtuoso

**Output Format**:

```json
{
  "test_suite": "W3C SPARQL 1.1",
  "total_tests": 389,
  "passed": 350,
  "failed": 39,
  "categories": {
    "aggregates": {
      "total": 34,
      "passed": 32,
      "failed": 2,
      "avg_time_ms": 12.5,
      "failed_tests": [
        "agg-err-01: Unsupported aggregate function",
        "agg-groupconcat-05: String handling issue"
      ]
    }
  },
  "performance": {
    "total_time_sec": 45.2,
    "avg_query_time_ms": 116.2,
    "median_query_time_ms": 8.5,
    "p95_query_time_ms": 450.0
  }
}
```

---

## Implementation Steps

### Step 1: Basic Test Runner (Python Prototype)

**File**: `tests/run_w3c_tests.py`

**Why Python First**:
- Faster iteration
- Easy RDF parsing (rdflib)
- Easy SPARQL result parsing
- Prototype test infrastructure

**Example**:

```python
import rdflib
from pathlib import Path

class W3CTestRunner:
    def __init__(self, sabot_ql_path):
        self.sabot_ql = sabot_ql_path  # C++ library via Cython

    def load_test_manifest(self, manifest_path):
        """Parse manifest.ttl to get test cases"""
        g = rdflib.Graph()
        g.parse(manifest_path, format='turtle')
        # Extract test cases
        return test_cases

    def run_test(self, test_case):
        """Run single test"""
        # 1. Load data (.ttl) into TripleStore
        # 2. Parse query (.rq)
        # 3. Execute query
        # 4. Load expected results (.srx/.srj)
        # 5. Compare
        pass

    def run_category(self, category):
        """Run all tests in category"""
        manifest = f"tests/rdf-tests/sparql/sparql11/{category}/manifest.ttl"
        tests = self.load_test_manifest(manifest)

        results = []
        for test in tests:
            result = self.run_test(test)
            results.append(result)

        return results
```

**Usage**:
```bash
python tests/run_w3c_tests.py --category bind
python tests/run_w3c_tests.py --all --benchmark
```

### Step 2: Integrate with Layer 3 Planner

Once query planner is implemented:
- Connect parser → planner → executor
- Use real operator trees
- Full end-to-end execution

### Step 3: Port to C++ (if needed)

If Python is too slow:
- Rewrite test runner in C++
- Use native RDF parsing (e.g., Raptor)
- Direct libsabot_ql.dylib calls

### Step 4: Continuous Integration

Add to CI pipeline:
```yaml
name: SPARQL W3C Tests

on: [push, pull_request]

jobs:
  w3c-tests:
    runs-on: macos-latest
    steps:
      - uses: actions/checkout@v2
        with:
          submodules: recursive

      - name: Build sabot_ql
        run: |
          cd sabot_ql/build
          cmake ..
          make -j8

      - name: Run W3C Tests
        run: |
          python tests/run_w3c_tests.py --all

      - name: Upload Results
        uses: actions/upload-artifact@v2
        with:
          name: w3c-test-results
          path: tests/results.json
```

---

## Expected Outcomes

### Short Term (Phase 1-2)
- ✅ Basic test runner working
- ✅ Can run simple tests (bind, basic patterns)
- ✅ Infrastructure for comparing results

### Medium Term (Phase 3-4)
- ✅ 50%+ of W3C tests passing
- ✅ All basic patterns working
- ✅ Joins and filters working
- ⚠️ Some aggregates working (needs implementation)

### Long Term (Phase 5)
- ✅ 90%+ of W3C tests passing
- ✅ Comprehensive benchmark suite
- ✅ Performance comparison vs other engines
- ✅ Continuous integration

---

## Dependencies

### Current Status
- ✅ Layer 1: Storage (MarbleDB integration complete)
- ✅ Layer 2: Operators (Scan, Join, Filter complete)
- ⏳ Layer 3: Query planner (needed for test execution)

### Blocking Issues
- **Query Planner**: Need to connect parser → planner → operators
- **Aggregate Operators**: Need AVG, SUM, COUNT, etc. (distinct from filter/join)
- **Property Path**: Need transitive closure operators (*, +)
- **CONSTRUCT**: Need result construction (different from SELECT)

### Non-Blocking
- Test runner can be built in parallel with Layer 3
- Test infrastructure independent of query execution
- Can mock/stub query execution for initial testing

---

## File Organization

```
sabot_ql/
├── tests/
│   ├── rdf-tests/                    # W3C test submodule (existing)
│   │   └── sparql/sparql11/
│   │       ├── aggregates/
│   │       ├── bind/
│   │       └── ...
│   │
│   ├── w3c/                           # NEW: W3C test runner
│   │   ├── test_runner.py            # Python test runner
│   │   ├── test_loader.py            # Load .ttl, .rq, .srx files
│   │   ├── result_comparator.py      # Compare results
│   │   ├── benchmark_runner.py       # Benchmark mode
│   │   └── README.md
│   │
│   ├── w3c_results/                   # NEW: Test results
│   │   ├── latest.json               # Latest test run
│   │   ├── 2025-10-23.json           # Historical results
│   │   └── benchmark.json            # Performance data
│   │
│   └── test_operator_composition.cpp  # Existing unit test
│
└── SPARQL_TEST_PLAN.md                # This file
```

---

## Next Actions

### Immediate (This Session)
1. ✅ Created test plan document
2. ⏳ Stub out test runner structure
3. ⏳ Try running 1-2 simplest tests manually

### Short Term (Next Session)
1. Build Python test runner prototype
2. Implement test loader (parse .ttl, .rq, .srx files)
3. Run first test category (bind or bindings)

### Medium Term (Week 1-2)
1. Complete Layer 3 query planner
2. Integrate planner with test runner
3. Run Priority 1 test categories

### Long Term (Month 1)
1. Implement missing operators (aggregates, property paths)
2. Achieve 50%+ test pass rate
3. Build comprehensive benchmark suite

---

## Success Metrics

### Phase 1: Infrastructure
- ✅ Test runner can load W3C tests
- ✅ Can parse queries
- ✅ Can load test data
- ✅ Can compare results

### Phase 2: Basic Execution
- ✅ Can execute simple patterns
- ✅ 10+ tests passing
- ✅ Clear error messages for failures

### Phase 3: Comprehensive Coverage
- ✅ 50%+ tests passing (195+ tests)
- ✅ All basic features working
- ✅ Joins, filters, bindings working

### Phase 4: Production Ready
- ✅ 90%+ tests passing (350+ tests)
- ✅ Competitive performance
- ✅ CI integration

---

## Conclusion

**Current State**:
- Layer 2 operators complete ✅
- W3C test suite available (389 tests) ✅
- Ready to build test infrastructure ✅

**Next Steps**:
1. Stub out basic test runner
2. Complete Layer 3 query planner
3. Run first W3C tests end-to-end

**Timeline**:
- Test infrastructure: 1-2 sessions
- Layer 3 + first tests: 1-2 weeks
- 50% coverage: 2-4 weeks
- 90% coverage: 1-2 months

**Blocking**: Layer 3 query planner needed for actual test execution

---

**End of SPARQL Test Plan**
