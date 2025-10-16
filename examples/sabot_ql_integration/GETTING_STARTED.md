# Getting Started with SabotQL + Sabot

**3-Step Guide to Graph Enrichment in Pipelines**

---

## Step 1: Build (5 minutes)

```bash
cd sabot_ql/bindings/python
./build.sh
```

**Output:**
```
[1/3] Building C++ library... ✅
[2/3] Building Python bindings... ✅
[3/3] Installing Python package... ✅

✅ SabotQL Python bindings are ready!
```

---

## Step 2: Run Quickstart (1 minute)

```bash
cd ../../examples/sabot_ql_integration
python quickstart.py
```

**You'll see:**
```
Step 1: Creating knowledge graph... ✅
Step 2: Loading company data... ✅
Step 3: Creating sample stream... ✅
Step 4: Enriching stream... ✅
Step 5: Processing enriched data... ✅

Quickstart Complete! 🎉
```

---

## Step 3: Use in Your Code

```python
from sabot.api.stream import Stream
from sabot_ql.bindings.python import create_triple_store

# Load knowledge graph
kg = create_triple_store('./your_kg.db')
kg.load_ntriples('your_data.nt')

# Add to pipeline
stream = Stream.from_kafka('your-topic')
enriched = stream.triple_lookup(
    kg,
    lookup_key='your_key_column',
    pattern='?key <hasProperty> ?value'
)

# Process
async for batch in enriched:
    # batch now has original columns + graph data
    process(batch)
```

---

## What You Just Did

1. ✅ Built SabotQL C++ library + Python bindings
2. ✅ Ran working example with graph enrichment
3. ✅ Learned the API pattern

**Time elapsed:** < 10 minutes

---

## Next Steps

### Try the Examples

```bash
# Basic enrichment
python example1_company_enrichment.py

# Fraud detection
python example2_fraud_detection.py

# Recommendations
python example3_recommendation.py
```

### Read the Docs

- **Quick start:** `QUICKSTART_SABOT.md` (in `sabot_ql/`)
- **Full guide:** `README.md` (this directory)
- **Visual guide:** `VISUAL_GUIDE.md`

### Run Benchmarks

```bash
python benchmark_triple_lookup.py
```

**Expected:** 100K-1M ops/sec

### Run Tests

```bash
python test_triple_enrichment.py
```

**Expected:** All tests pass

---

## Common Patterns

### Pattern 1: Simple Lookup

```python
# Just one property
enriched = stream.triple_lookup(
    kg, 'product_id',
    predicate='<hasName>',
    object=None
)
```

### Pattern 2: Multi-Property

```python
# Multiple properties in one query
enriched = stream.triple_lookup(
    kg, 'company',
    pattern='''
        ?company <hasName> ?name .
        ?company <hasSector> ?sector .
        ?company <hasCountry> ?country
    '''
)
```

### Pattern 3: Graph Traversal

```python
# Multi-hop relationships
enriched = stream.triple_lookup(
    kg, 'person',
    pattern='''
        ?person <knows> ?friend .
        ?friend <worksAt> ?company .
        ?company <hasName> ?company_name
    '''
)
```

---

## Troubleshooting

### Build Fails?

```bash
# Make sure C++ library builds first
cd sabot_ql/build
cmake .. && make -j8

# Then try bindings again
cd ../bindings/python
./build.sh
```

### Import Error?

```bash
# Install in development mode
cd sabot_ql/bindings/python
pip install -e .
```

### Slow Performance?

```python
# Enable optimizations
stream.triple_lookup(
    kg, 'key', pattern='...',
    batch_lookups=True,  # ← 10-100x faster
    cache_size=10000     # ← Cache hot keys
)
```

---

## What's Available

### Files in This Directory

| File | Purpose | Time |
|------|---------|------|
| `GETTING_STARTED.md` | This file | Start here |
| `quickstart.py` | Runnable intro | 1 min |
| `README.md` | Complete guide | 30 min |
| `VISUAL_GUIDE.md` | Architecture diagrams | 10 min |
| `example1_*.py` | Company enrichment | 10 min |
| `example2_*.py` | Fraud detection | 15 min |
| `example3_*.py` | Recommendations | 15 min |
| `complete_pipeline_example.py` | Full pipeline | 20 min |
| `test_*.py` | Integration tests | Run & verify |
| `benchmark_*.py` | Performance tests | Run & verify |

### Start Path

```
GETTING_STARTED.md (you are here)
    ↓
quickstart.py (run this first)
    ↓
README.md (read for details)
    ↓
example1_company_enrichment.py (basic pattern)
    ↓
example2_fraud_detection.py (advanced pattern)
    ↓
Your own pipeline!
```

---

## Success Criteria

After completing this guide, you should be able to:

- [x] Build SabotQL Python bindings
- [x] Create a triple store
- [x] Load RDF data
- [x] Enrich a stream with SPARQL queries
- [x] Process enriched batches
- [x] Understand performance characteristics
- [x] Use in production pipelines

**All achievable in < 30 minutes! ✅**

---

## Quick Commands

```bash
# Build everything
make build

# Run quickstart
make quickstart

# Run tests
make test

# Run benchmarks
make benchmark

# Clean up
make clean
```

---

**Last Updated:** October 14, 2025  
**Difficulty:** Easy (< 30 minutes)  
**Prerequisites:** Python 3.11+, CMake, C++ compiler  
**Status:** ✅ Ready to Use


