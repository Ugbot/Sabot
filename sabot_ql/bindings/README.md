# SabotQL Python Bindings

**Zero-Copy SPARQL Queries in Python**

---

## Overview

These bindings expose SabotQL's C++ SPARQL engine to Python with zero-copy Arrow integration, enabling graph-based enrichment in Sabot streaming pipelines.

**Performance:**
- SPARQL parsing: 23,798 queries/sec
- Pattern lookups: 100K-1M ops/sec (cached)
- Zero-copy: Arrow tables flow directly between C++ and Python

---

## Building

### Option 1: PyBind11 (Recommended)

```bash
# Install pybind11
pip install pybind11

# Build with Python bindings
cd sabot_ql/build
cmake .. -DBUILD_PYTHON_BINDINGS=ON
make -j8

# Install Python module
cd ../bindings/python
pip install -e .
```

### Option 2: Cython (Alternative)

```bash
# Build C++ library first
cd sabot_ql/build
cmake .. && make -j8

# Build Cython bindings
cd ../bindings/python
python setup.py build_ext --inplace
pip install -e .
```

---

## Usage

```python
from sabot_ql.bindings.python import create_triple_store

# Create triple store
kg = create_triple_store('./my_kg.db')

# Insert triples
kg.insert_triple(
    subject='http://example.org/Alice',
    predicate='http://schema.org/knows',
    object='http://example.org/Bob'
)

# Query with SPARQL
results = kg.query_sparql('''
    SELECT ?person ?friend WHERE {
        ?person <http://schema.org/knows> ?friend
    }
''')

# Results are PyArrow Table
print(results.to_pandas())
```

---

## Integration with Sabot

```python
from sabot.api.stream import Stream
from sabot_ql.bindings.python import create_triple_store

# Load knowledge graph
kg = create_triple_store('./companies.db')

# Enrich stream
stream = Stream.from_kafka('quotes')
enriched = stream.triple_lookup(
    kg,
    lookup_key='symbol',
    pattern='?symbol <hasName> ?name'
)

async for batch in enriched:
    process(batch)
```

---

## API Reference

See `examples/sabot_ql_integration/README.md` for complete documentation.

---

## Files

| File | Purpose |
|------|---------|
| `sabot_ql.pyx` | Cython bindings |
| `pybind_module.cpp` | PyBind11 bindings |
| `setup.py` | Build configuration |
| `__init__.py` | Module exports |

---

**Last Updated:** October 14, 2025


