# W3C SPARQL Test Suite Analysis

**Date**: October 23, 2025
**Test Runner**: `/tests/w3c_sparql_test_runner.py`
**Test Suite Location**: `sabot_ql/tests/rdf-tests/sparql/`

## Executive Summary

Attempted to run the official W3C SPARQL test suites against Sabot's RDF/SPARQL implementation. While the test infrastructure is now in place, the majority of W3C tests cannot be run due to **parser limitations** rather than execution failures.

**Key Finding**: Sabot's SPARQL parser has specific syntax requirements that differ from the full W3C SPARQL 1.1 specification, particularly around:
1. BASE declarations (not supported)
2. Empty PREFIX names (e.g., `PREFIX : <...>` - may not be supported)
3. Certain query forms

## Test Results Summary

### SPARQL 1.0 Basic Tests
- **Total**: 27 tests
- **Passed**: 0
- **Failed**: 1
- **Errors**: 26 (mostly BASE declaration parse errors)
- **Pass Rate**: 0%

**Primary Failure Mode**: `Expected CaselessKeyword 'SELECT'` - Parser doesn't support BASE declarations

### SPARQL 1.1 Aggregates Tests
- **Total**: 29 tests
- **Passed**: 0
- **Failed**: 0
- **Errors**: 29 (mostly PREFIX parse errors or empty data)
- **Pass Rate**: 0%

**Primary Failure Modes**:
1. `Expected CaselessKeyword 'SELECT'` - Likely PREFIX : <...> not supported
2. `Store is empty - add triples before querying` - Data loading issues

## Root Cause Analysis

### 1. BASE Declaration Not Supported

**Example Failing Query**:
```sparql
BASE <http://example.org/x/>
PREFIX : <>

SELECT * WHERE { :x ?p ?v }
```

**Error**: `Expected CaselessKeyword 'SELECT' (at char 0)`

**Impact**: ~50% of W3C tests use BASE declarations

**Recommendation**: Implement BASE declaration parsing or preprocess queries to expand BASE URIs

### 2. Empty PREFIX Syntax

**Example Query**:
```sparql
PREFIX : <http://www.example.org/>
SELECT (AVG(?o) AS ?avg)
WHERE { ?s :dec ?o }
```

**Potential Issue**: Empty prefix name (`:`) may not be recognized

**Impact**: ~30% of W3C tests use empty prefix

**Recommendation**: Verify if parser handles `PREFIX :` correctly, fix if needed

### 3. Data Loading Issues

Some tests had empty stores even after data loading, suggesting:
- Turtle files with blank nodes not loading correctly
- Named graph data not being loaded
- RDF collections not being expanded

## What We Know Works

Based on previous hand-crafted tests (35 tests, 97% pass rate):

### ✅ Confirmed Working

**SPARQL Features**:
- SELECT queries with explicit PREFIX declarations
- WHERE clause with triple patterns
- Variable bindings (?s, ?p, ?o)
- Multi-pattern queries with automatic joins
- **FILTER expressions** (=, !=, <, <=, >, >=) - Fixed October 23
- Logical operators (AND, OR, NOT)
- LIMIT and OFFSET
- DISTINCT
- ORDER BY
- GROUP BY
- Aggregates: COUNT, SUM, AVG, MIN, MAX

**RDF Storage**:
- IRI triples
- Literal triples with datatypes
- Language tags
- 3-index strategy (SPO, POS, OSP)
- Vocabulary management

**Performance**:
- 3-37M pattern matches/sec
- 23,798 queries/sec parsing (for supported syntax)

## What Doesn't Work

### ❌ Parser Limitations

1. **BASE declarations**: Not implemented
2. **Empty PREFIX syntax**: Unclear if supported (needs verification)
3. **CONSTRUCT queries**: Unknown (no tests run)
4. **ASK queries**: Unknown (no tests run)
5. **DESCRIBE queries**: Unknown (no tests run)

### ❌ Feature Limitations

1. **Blank nodes**: Not implemented
2. **Named graphs**: GRAPH keyword not supported
3. **Property paths**: Unknown (no tests run)
4. **BIND expressions**: Unknown (no tests run)
5. **Subqueries**: Unknown (no tests run)
6. **UPDATE/INSERT/DELETE**: Not implemented (read-only)
7. **Federation**: Not implemented

## Accurate Feature Coverage Estimate

Based on test analysis and hand-crafted tests:

| Category | Support | Evidence |
|----------|---------|----------|
| **Core SELECT** | 95% | 35 hand-crafted tests pass |
| **Triple Patterns** | 95% | Working in all tests |
| **FILTER Expressions** | 90% | Basic comparisons work, complex expressions untested |
| **Aggregates** | 85% | COUNT, SUM, AVG, MIN, MAX implemented, GROUP_CONCAT unknown |
| **PREFIX Support** | 60% | Named prefixes work, empty prefix unclear |
| **BASE Support** | 0% | Not implemented |
| **Blank Nodes** | 0% | Not implemented |
| **Named Graphs** | 0% | Not implemented |
| **Property Paths** | 0% | Likely not implemented |
| **UPDATE Operations** | 0% | Not implemented |

**Overall SPARQL 1.1 Coverage**: ~40-50% (vs. claimed 95%)

## Recommendations

### Immediate Actions

1. **Fix PREFIX : Parser** (1-2 hours)
   - Verify if `PREFIX : <...>` syntax is supported
   - If not, fix parser to handle empty prefix names

2. **Implement BASE Declaration** (2-4 hours)
   - Add BASE parsing to SPARQL parser
   - Expand relative URIs using BASE in query planning

3. **Fix W3C Test Runner** (2-3 hours)
   - Add BASE pre-processing as workaround
   - Improve data loading to handle blank nodes
   - Better error categorization

### Medium-Term Actions

1. **Run Curated Test Subset** (4-6 hours)
   - Manually select 50-100 W3C tests that match supported syntax
   - Convert BASE queries to full IRIs
   - Run tests and generate accurate pass rate

2. **Feature Gap Analysis** (2-3 hours)
   - Test CONSTRUCT, ASK, DESCRIBE
   - Test property paths
   - Test BIND expressions
   - Test subqueries
   - Document which features work

3. **Update Documentation** (1-2 hours)
   - Replace "95% SPARQL 1.1 support" with feature table
   - List specific features that work vs don't work
   - Provide syntax examples of what's supported

## Test Infrastructure Status

### ✅ What's Built

The W3C test runner (`tests/w3c_sparql_test_runner.py`) includes:
- Manifest.ttl parser to discover tests
- Turtle data loader using rdflib
- SPARQL Results XML (.srx) parser
- Result comparison framework
- Markdown report generator

### ⚠️ What Needs Work

1. **Query preprocessing**: Strip/expand BASE declarations
2. **Data loading**: Better blank node handling
3. **Result comparison**: Currently only checks row/column counts
4. **Test filtering**: Ability to skip tests by feature

## Conclusion

The W3C test suite cannot currently be used to validate Sabot's SPARQL implementation due to parser syntax incompatibilities, primarily:
- No BASE declaration support
- Possible empty PREFIX issues

However, hand-crafted tests show that **core SPARQL 1.1 SELECT functionality works well** for queries using:
- Explicit PREFIX declarations (non-empty)
- No BASE declarations
- Supported features (SELECT, WHERE, FILTER, GROUP BY, aggregates)

**Actual Feature Coverage**: ~40-50% of SPARQL 1.1 (not 95%)
- Core SELECT queries: Excellent
- Advanced features: Mostly untested/unimplemented

**Recommendation**: Focus on documenting what **does** work clearly rather than claiming broad spec coverage.
