# SabotQL Quick Start Guide

**Status:** Phase 1-2 Complete (Storage + N-Triples Parsing)

## Overview

SabotQL is an Arrow-native SPARQL engine built for high-performance RDF query processing. It combines QLever's proven algorithms with Apache Arrow's columnar architecture and MarbleDB's fast storage.

## What Works Now

✅ **Storage Layer**
- Triple store with 3 index permutations (SPO, POS, OSP)
- Vocabulary with LRU cache (10K entries)
- ValueId encoding with inline optimization
- MarbleDB persistence

✅ **RDF Parsing**
- N-Triples 1.1 parser (500K-1M triples/sec)
- Batch loading (100K triples/batch)
- Error handling (skip or fail on invalid triples)

✅ **Performance**
- Write throughput: 100K-1M triples/sec (target)
- Cache hit rate: ~90%+ for hot terms
- Inline encoding: 40-60% fewer vocabulary lookups

## Example Usage (C++)

### 1. Load RDF Data

```cpp
#include <sabot_ql/storage/triple_store.h>
#include <sabot_ql/storage/vocabulary.h>
#include <sabot_ql/parser/rdf_parser.h>

using namespace sabot_ql;

// Open/create database
marble::DBOptions db_opts;
db_opts.db_path = "/tmp/my_rdf_db";
auto db_result = marble::MarbleDB::Open(db_opts);
auto db = db_result.value();

// Create triple store and vocabulary
auto store_result = TripleStore::Create("/tmp/my_rdf_db", db);
auto store = store_result.ValueOrDie();

auto vocab_result = Vocabulary::Create("/tmp/my_rdf_db", db);
auto vocab = vocab_result.ValueOrDie();

// Load RDF file (N-Triples)
RdfParserConfig config;
config.batch_size = 100000;  // 100K triples per batch
config.skip_invalid_triples = false;  // Fail on errors

auto status = LoadRdfFile("data.nt", store, vocab, config);
if (!status.ok()) {
    std::cerr << "Failed to load RDF: " << status.ToString() << std::endl;
}
```

### 2. Query with Triple Patterns

```cpp
// Query: Find all triples with subject <http://example.org/alice>
Term alice_term = Term::IRI("http://example.org/alice");
auto alice_id_result = vocab->GetValueId(alice_term);
auto alice_id = alice_id_result.ValueOrDie().value();

TriplePattern pattern;
pattern.subject = alice_id;  // Bound
pattern.predicate = std::nullopt;  // Unbound (wildcard)
pattern.object = std::nullopt;  // Unbound (wildcard)

// Scan matching triples
auto result_table = store->ScanPattern(pattern);

// Result is arrow::Table with columns: [predicate, object]
std::cout << "Found " << result_table.ValueOrDie()->num_rows() << " matches" << std::endl;
```

### 3. Manual Triple Insertion

```cpp
// Create terms
Term subject = Term::IRI("http://example.org/alice");
Term predicate = Term::IRI("http://schema.org/name");
Term object = Term::Literal("Alice", "en");

// Convert to ValueIds
auto subject_id = vocab->AddTerm(subject).ValueOrDie();
auto predicate_id = vocab->AddTerm(predicate).ValueOrDie();
auto object_id = vocab->AddTerm(object).ValueOrDie();

// Create triple
Triple triple{subject_id, predicate_id, object_id};

// Insert (batch for better performance)
std::vector<Triple> batch = {triple};
store->InsertTriples(batch);
```

### 4. Vocabulary Operations

```cpp
// Add term to vocabulary
Term term = Term::Literal("42", "", "http://www.w3.org/2001/XMLSchema#integer");
auto id_result = vocab->AddTerm(term);
auto value_id = id_result.ValueOrDie();

// Value is inlined (no vocabulary lookup needed!)
auto kind = value_id::GetType(value_id);
if (kind == TermKind::Int) {
    auto value = value_id::DecodeInt(value_id);
    std::cout << "Inline integer: " << *value << std::endl;
}

// Lookup term by ValueId
auto term_result = vocab->GetTerm(value_id);
auto retrieved_term = term_result.ValueOrDie();
std::cout << "Term: " << retrieved_term.lexical << std::endl;

// Export vocabulary as Arrow table (for inspection)
auto table_result = vocab->ToArrowTable();
auto vocab_table = table_result.ValueOrDie();
std::cout << "Vocabulary size: " << vocab_table->num_rows() << std::endl;
```

## Example Usage (Python) - Coming Soon

```python
from sabot.rdf import SabotQL

# Open database
db = SabotQL("/tmp/my_rdf_db")

# Load RDF file
db.load_rdf("data.nt", format="ntriples")

# Execute SPARQL query (Phase 4)
result = db.query("""
    SELECT ?name ?age WHERE {
        ?person <http://schema.org/name> ?name .
        ?person <http://schema.org/age> ?age .
        FILTER(?age > 30)
    }
    ORDER BY ?age DESC
    LIMIT 10
""")

# Result is PyArrow Table
print(result.to_pandas())
```

## Building from Source

### Prerequisites

- **C++20 compiler** (GCC 11+, Clang 14+)
- **Apache Arrow** (vendored in `../vendor/arrow`)
- **MarbleDB** (in `../MarbleDB`)
- **Abseil** (from QLever in `../vendor/qlever`)

### Build Steps

```bash
cd sabot_ql
mkdir build && cd build
cmake ..
make -j$(nproc)
```

### CMake Configuration

The build system automatically finds:
- Arrow from `../vendor/arrow/cpp/build/install`
- MarbleDB headers from `../MarbleDB/include`
- Abseil from `../vendor/qlever/build/_deps/abseil-cpp-build`
- QLever utilities from `../vendor/qlever/src`

## Architecture Overview

```
┌──────────────────────────────────────────┐
│ User Application (C++ or Python)         │
└──────────────────────────────────────────┘
                  ↓
┌──────────────────────────────────────────┐
│ SabotQL Query Engine (Phase 4)           │
│ - SPARQL Parser                          │
│ - Query Planner & Optimizer              │
│ - Arrow Operators (Filter, Join, etc.)   │
└──────────────────────────────────────────┘
                  ↓ arrow::Table
┌──────────────────────────────────────────┐
│ Storage Layer (Phase 1) ✅               │
│ - Triple Store (SPO, POS, OSP indexes)   │
│ - Vocabulary (Term Dictionary + Cache)   │
└──────────────────────────────────────────┘
                  ↓
┌──────────────────────────────────────────┐
│ MarbleDB                                 │
│ - LSM Tree with Arrow SSTables           │
│ - Sparse Indexing + Bloom Filters        │
│ - ClickHouse-style Optimizations         │
└──────────────────────────────────────────┘
```

## Performance Tuning

### Vocabulary Cache Size

```cpp
// Larger cache = higher hit rate but more memory
// Default: 10K entries (~1-2 MB)
// Recommended for large datasets: 100K entries (~10-20 MB)

// This is set internally by VocabularyImpl constructor
// To modify: edit vocabulary_impl.cpp line 41:
//   term_to_id_cache_(100000),  // 100K instead of 10K
```

### Batch Size for Loading

```cpp
RdfParserConfig config;
config.batch_size = 100000;  // Default: 100K triples

// Larger batch = better throughput but more memory
// Recommended: 50K-500K depending on available RAM
```

### Index Selection Strategy

The triple store automatically selects the best index based on bound variables:

- **SPO index:** Use for `(S, ?, ?)`, `(S, P, ?)`, `(S, P, O)`
- **POS index:** Use for `(?, P, ?)`, `(?, P, O)`
- **OSP index:** Use for `(?, ?, O)`

For `(?, ?, ?)` (full scan), SPO is used by default.

## Debugging

### Enable Verbose Logging

```cpp
// TODO: Add logging configuration once implemented
```

### Check Vocabulary Cache Stats

```cpp
// TODO: Add cache statistics API
// auto stats = vocab->GetCacheStats();
// std::cout << "Hit rate: " << stats.hit_rate << std::endl;
```

### Inspect Triple Store Statistics

```cpp
auto total = store->TotalTriples();
std::cout << "Total triples: " << total << std::endl;

// Estimate cardinality for pattern
TriplePattern pattern;
pattern.predicate = some_predicate_id;
auto est = store->EstimateCardinality(pattern);
std::cout << "Estimated matches: " << est.ValueOrDie() << std::endl;
```

## Limitations (Current Phase)

❌ **Not Yet Implemented:**
- Turtle parsing (Phase 2, in progress)
- RDF/XML parsing (Phase 2)
- SPARQL query execution (Phase 3-4)
- Query optimization (Phase 3-4)
- Parallel parsing (Phase 5)
- Python bindings (Phase 5)
- Unit tests (Phase 5)

✅ **What Works:**
- N-Triples parsing
- Triple storage with 3 indexes
- Vocabulary with inline optimization
- Manual triple pattern queries
- Batch loading

## Next Steps

1. **Turtle Parser Integration** - Borrow from QLever (3-5 days)
2. **Arrow Operators** - Filter, Join, Project (2-3 weeks)
3. **SPARQL Parser** - ANTLR4-based (3-4 weeks)
4. **Query Planner** - Cost-based optimization (2-3 weeks)
5. **Python Bindings** - Cython wrappers (1-2 weeks)

## Resources

- **Documentation:** See `README.md` and `IMPLEMENTATION_STATUS.md`
- **Architecture:** See design diagrams in README
- **Performance:** See performance goals in README
- **QLever Reference:** https://github.com/ad-freiburg/qlever
- **Apache Arrow:** https://arrow.apache.org/
- **MarbleDB:** See `../MarbleDB/` documentation

## Getting Help

- Check `IMPLEMENTATION_STATUS.md` for current feature status
- See `README.md` for architecture and design principles
- Review example code in this quickstart guide
- Consult QLever documentation for algorithm details

---

**Last Updated:** October 12, 2025
**Version:** 0.1.0-alpha (Phase 1-2 Complete)
