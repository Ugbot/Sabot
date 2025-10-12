# Sabot RDF/SPARQL Benchmarks

Benchmark suite for testing Sabot's SPARQL query engine with RDF triple stores.

## Overview

This benchmark suite measures the performance of Sabot's SPARQL implementation by running a collection of queries against RDF datasets. It uses the [KROWN benchmark framework](https://github.com/kg-construct/KROWN) as a submodule to provide standardized RDF test data.

**Key Features:**
- RDF data loading (N-Triples, Turtle, RDF/XML)
- 10 SPARQL queries (basic to complex)
- Performance metrics (execution time, throughput)
- JSON and CSV result export

## Quick Start

### 1. Install Dependencies

```bash
cd benchmarks/rdf_benchmarks
pip install rdflib
```

**Note:** Sabot uses vendored Arrow (cyarrow), not pip pyarrow.

### 2. Run Benchmark with Synthetic Data

The fastest way to test the benchmark:

```bash
python run_benchmark.py --data synthetic --queries basic --runs 5
```

This will:
- Generate a small synthetic RDF dataset (5 persons, 18 triples)
- Run 5 basic SPARQL queries (Q1-Q5)
- Execute each query 5 times and average results
- Save results to `results/benchmark_results.json` and `results/benchmark_results.csv`

### 3. View Results

```bash
cat results/benchmark_results.csv
```

Expected output:
```
query_id,description,complexity,status,num_runs,avg_time_ms,...
Q1_simple_pattern,"Simple pattern match (find all persons)",low,success,5,2.34,...
Q2_property_access,"Property access with 2-pattern join",low,success,5,3.12,...
...
```

## Usage

### Data Sources

The benchmark supports three data sources:

#### 1. Synthetic Data (Default)
Small test dataset generated in-memory:

```bash
python run_benchmark.py --data synthetic
```

**Dataset:** 5 persons, 18 triples (types, names, ages, friendships)

#### 2. Test File
N-Triples file included in the repository:

```bash
python run_benchmark.py --data file:test_data.nt
```

**Dataset:** 5 persons, 18 triples (same as synthetic)

#### 3. KROWN Samples
Use KROWN benchmark sample datasets:

```bash
# Initialize KROWN submodule (first time only)
git submodule update --init --recursive

# Run with KROWN sample
python run_benchmark.py --data krown:raw
```

**Available KROWN samples:**
- `raw` - Basic scenario (rows, columns, cell size)
- `duplicates` - Duplicate value scenarios
- `empty` - Empty value scenarios
- `mappings` - Mapping complexity scenarios
- `joins` - Join scenarios

**Note:** KROWN samples are designed for materialization benchmarks (CSV → RDF conversion). We use them as input RDF datasets for SPARQL query benchmarks.

#### 4. Custom RDF File
Load any RDF file (N-Triples, Turtle, RDF/XML):

```bash
python run_benchmark.py --data file:/path/to/your/data.nt
```

Supported formats:
- `.nt` - N-Triples
- `.ttl` - Turtle
- `.rdf`, `.xml` - RDF/XML
- `.n3` - Notation3

### Query Suites

#### Basic Suite (Q1-Q5)
Fast queries for quick testing:

```bash
python run_benchmark.py --queries basic
```

**Queries:**
- Q1: Simple pattern match (find all persons)
- Q2: Property access (2-pattern join)
- Q3: Filtered query (age > 30)
- Q4: Relationship query (who knows whom)
- Q5: 3-pattern join (person + name + age)

#### Full Suite (Q1-Q10)
Complete query collection:

```bash
python run_benchmark.py --queries all
```

**Additional Queries:**
- Q6: String filter (names starting with 'A')
- Q7: Range filter (age 25-35)
- Q8: Transitive friendship (friends of friends)
- Q9: Optional pattern (LEFT JOIN) - **May not work yet**
- Q10: Complex boolean filter (young OR old)

### Configuration Options

```bash
python run_benchmark.py \
  --data <source>       # Data source (synthetic, file:<path>, krown:<sample>)
  --queries <suite>     # Query suite (basic, all)
  --runs <N>            # Runs per query (default: 5)
  --output <dir>        # Output directory (default: results)
```

**Examples:**

```bash
# Run all queries with 10 runs each
python run_benchmark.py --data synthetic --queries all --runs 10

# Test with KROWN 'duplicates' sample
python run_benchmark.py --data krown:duplicates --queries basic

# Custom RDF file with 3 runs
python run_benchmark.py --data file:custom_data.ttl --queries all --runs 3
```

## File Structure

```
benchmarks/rdf_benchmarks/
├── README.md                  # This file
├── rdf_loader.py              # RDF data loader
├── sparql_queries.py          # SPARQL query suite
├── run_benchmark.py           # Benchmark runner
├── test_data.nt               # Small test dataset (N-Triples)
├── KROWN/                     # KROWN benchmark (submodule)
│   ├── data-generator/        # RDF data generator
│   ├── execution-framework/   # Benchmark execution framework
│   └── samples/               # Sample RDF datasets
└── results/                   # Benchmark results (generated)
    ├── benchmark_results.json
    └── benchmark_results.csv
```

## Implementation Details

### RDF Data Loader (`rdf_loader.py`)

**Purpose:** Convert RDF files to Sabot's `PyRDFTripleStore` format.

**Architecture:**
1. Parse RDF file with `rdflib`
2. Build term dictionary (RDF term → int64 ID)
3. Convert triples to `(subject_id, predicate_id, object_id)` format
4. Create PyArrow tables for triples and terms
5. Initialize `PyRDFTripleStore`

**Key Classes:**
- `TermDictionary` - Maps RDF terms to int64 IDs
- Functions: `load_rdf_file()`, `load_krown_sample()`, `create_synthetic_dataset()`

**Term Dictionary Schema:**
```python
{
    'id': int64,           # Unique term ID
    'lex': string,         # Lexical form (e.g., "Alice", "http://example.org/alice")
    'kind': uint8,         # 0=IRI, 1=Literal, 2=Blank Node
    'lang': string,        # Language tag (e.g., "en")
    'datatype': string     # Datatype IRI (e.g., "xsd:integer")
}
```

**Triples Schema:**
```python
{
    's': int64,  # Subject ID
    'p': int64,  # Predicate ID
    'o': int64   # Object ID
}
```

### SPARQL Query Suite (`sparql_queries.py`)

**10 SPARQL queries** organized by complexity:

**Low Complexity (Q1-Q2):**
- Single or 2-pattern joins
- No filters

**Medium Complexity (Q3-Q7):**
- 3-4 pattern joins
- FILTER expressions
- String and numeric comparisons

**High Complexity (Q8-Q10):**
- Multi-way joins (4+ patterns)
- Optional patterns (LEFT JOIN)
- Complex boolean filters

**Query Metadata:**
```python
{
    'query': str,              # SPARQL query string
    'description': str,        # Human-readable description
    'complexity': str,         # 'low', 'medium', 'high'
    'expected_speedup': str    # Expected optimization speedup
}
```

### Benchmark Runner (`run_benchmark.py`)

**Purpose:** Execute queries, collect metrics, generate reports.

**Metrics Collected:**
- Execution time (min, max, avg, median, stddev)
- Result count
- Throughput (queries per second)
- Status (success/failed)

**Output Formats:**
1. **JSON** (`benchmark_results.json`) - Full metrics with execution times array
2. **CSV** (`benchmark_results.csv`) - Spreadsheet-compatible summary

**Example JSON Output:**
```json
{
  "query_id": "Q3_filtered",
  "description": "Filtered query (age > 30)",
  "complexity": "medium",
  "status": "success",
  "num_runs": 5,
  "execution_times_ms": [3.12, 3.05, 3.18, 3.09, 3.14],
  "avg_time_ms": 3.12,
  "median_time_ms": 3.12,
  "min_time_ms": 3.05,
  "max_time_ms": 3.18,
  "stddev_time_ms": 0.05,
  "result_count": 2,
  "throughput_qps": 320.51
}
```

## KROWN Integration

### What is KROWN?

KROWN (Knowledge gRaph cOnstruction Work beNchmark) is a benchmark for RDF materialization systems. It measures how fast systems convert relational data (CSV, databases) to RDF using declarative mappings (RML).

**Original Purpose:** Benchmark RDF materialization (CSV → RDF)
**Our Use:** Benchmark SPARQL queries (RDF → query results)

### How We Use KROWN

1. **Data Generator:** Use KROWN's data generator to create test RDF datasets
2. **Samples:** Use pre-generated KROWN samples as input RDF data
3. **Adaptation:** We query the RDF, not materialize it

### KROWN Submodule Setup

```bash
# Initialize submodule (first time only)
git submodule update --init --recursive

# Update KROWN to latest version
cd KROWN
git pull origin main
cd ..
```

### Generating Custom KROWN Data

```bash
cd KROWN/data-generator

# Install dependencies
pip install -r requirements.txt

# Generate scenario
./exgentool --scenario=config/benchmark-raw-rmlstreamer.json generate

# Use generated data
cd ../..
python run_benchmark.py --data krown:raw
```

See [KROWN documentation](https://github.com/kg-construct/KROWN) for more details.

## Troubleshooting

### ImportError: rdflib not installed

```bash
pip install rdflib
```

### ImportError: No module named 'sabot'

Make sure you're running from the correct directory:

```bash
cd /Users/bengamble/Sabot/benchmarks/rdf_benchmarks
python run_benchmark.py --data synthetic --queries basic
```

### KROWN submodule not found

Initialize the submodule:

```bash
git submodule update --init --recursive
```

### Query execution failed

Some queries may not work yet (e.g., Q9 with OPTIONAL). Check Sabot's SPARQL implementation status:

```bash
cat ../../benchmarks/kuzu_study/SPARQL_IMPLEMENTATION_STATUS.md
```

### Arrow library issues

Sabot uses vendored Arrow (cyarrow), not pip pyarrow. Make sure you're importing from Sabot:

```python
from sabot import cyarrow as pa  # ✅ Correct
import pyarrow as pa             # ❌ Wrong
```

## Expected Results

With synthetic data (5 persons, 18 triples):

| Query | Description | Expected Time | Results |
|-------|-------------|---------------|---------|
| Q1 | Simple pattern | <5ms | 5 rows |
| Q2 | Property access | <10ms | 5 rows |
| Q3 | Filtered (age > 30) | <10ms | 2 rows |
| Q4 | Relationships | <15ms | 3 rows |
| Q5 | 3-pattern join | <15ms | 5 rows |

**Note:** Actual performance depends on:
- Hardware (CPU, memory)
- Sabot build configuration
- Query optimization effectiveness

## Next Steps

### Phase 1: Basic Benchmarking ✅
- [x] RDF data loader
- [x] SPARQL query suite
- [x] Benchmark runner
- [x] Test data
- [x] Documentation

### Phase 2: Enhanced Testing (TODO)
- [ ] Test with KROWN samples
- [ ] Large-scale datasets (10K-1M triples)
- [ ] Compare with other SPARQL engines (Jena, Blazegraph, QLever)
- [ ] Memory usage tracking
- [ ] Query plan visualization (EXPLAIN)

### Phase 3: Optimization Analysis (TODO)
- [ ] Measure optimization impact (filter pushdown, join reordering)
- [ ] Selectivity estimation accuracy
- [ ] Cache effectiveness
- [ ] Parallel query execution

## References

- [KROWN Benchmark](https://github.com/kg-construct/KROWN)
- [Sabot SPARQL Implementation Status](../../benchmarks/kuzu_study/SPARQL_IMPLEMENTATION_STATUS.md)
- [Sabot Graph Query Engine Docs](../../docs/GRAPH_QUERY_ENGINE.md)
- [W3C SPARQL 1.1 Specification](https://www.w3.org/TR/sparql11-query/)

## Contact

For issues or questions about this benchmark:
- Check [SPARQL Implementation Status](../../benchmarks/kuzu_study/SPARQL_IMPLEMENTATION_STATUS.md)
- Review [Sabot Documentation](../../docs/)
- Open an issue in the Sabot repository

---

**Last Updated:** October 12, 2025
**Status:** Phase 1 Complete ✅
