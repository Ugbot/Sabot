# SabotQL + Sabot Quickstart

**5-Minute Guide to Graph Enrichment in Streaming Pipelines**

---

## What is This?

SabotQL lets you **enrich streaming data with knowledge graph lookups** using SPARQL queries - like a SQL dimension table join, but for RDF graph data.

**Example:**
```python
# Load company info as RDF
kg.insert_triple('<AAPL>', '<hasName>', '"Apple Inc."')

# Enrich stream with company info
quotes.triple_lookup(kg, 'symbol', pattern='?symbol <hasName> ?name')

# Result: Each quote gets company name from knowledge graph
```

---

## Installation (3 steps)

```bash
# 1. Build C++ library
cd sabot_ql/build && cmake .. && make -j8

# 2. Install Python bindings
cd ../bindings/python && pip install -e .

# 3. Verify
python -c "from sabot_ql.bindings.python import create_triple_store; print('✅')"
```

---

## Your First Pipeline (< 30 lines)

```python
from sabot.api.stream import Stream
from sabot_ql.bindings.python import create_triple_store

# Create knowledge graph
kg = create_triple_store('./companies.db')

# Add company data
kg.insert_triple(
    subject='http://stocks/AAPL',
    predicate='http://schema/name',
    object='"Apple Inc."'
)

# Stream processing
quotes = Stream.from_kafka('quotes', 'localhost:9092', 'my-group')

enriched = quotes.triple_lookup(
    kg,
    lookup_key='symbol',
    pattern='?symbol <http://schema/name> ?company_name'
)

async for batch in enriched:
    print(f"Enriched {batch.num_rows} quotes")
    # batch now has: symbol, price, company_name
```

**That's it!** You're enriching streams with graph data.

---

## Common Patterns

### Pattern 1: Simple Property Lookup

```python
# Add one property to stream
enriched = stream.triple_lookup(
    kg,
    lookup_key='product_id',
    predicate='<hasName>',  # Simple pattern
    object=None             # Get value
)
```

### Pattern 2: Multi-Property Lookup

```python
# Add multiple properties
enriched = stream.triple_lookup(
    kg,
    lookup_key='company',
    pattern='''
        ?company <hasName> ?name .
        ?company <hasSector> ?sector .
        ?company <hasCountry> ?country
    '''
)
```

### Pattern 3: Graph Traversal

```python
# Multi-hop: person → knows → friend → worksFor → company
enriched = stream.triple_lookup(
    kg,
    lookup_key='person',
    pattern='''
        ?person <knows> ?friend .
        ?friend <worksFor> ?company .
        ?company <hasName> ?company_name
    '''
)
```

---

## Performance Tips

### 1. Use Batch Lookups (Default)

```python
triple_lookup(..., batch_lookups=True)  # 10-100x faster
```

### 2. Enable Caching

```python
triple_lookup(..., cache_size=10000)  # 90%+ hit rate typical
```

### 3. Pre-Load Reference Data

```python
# Load once at startup (dimension table pattern)
kg = create_triple_store('./reference.db')
kg.load_ntriples('companies.nt')  # One-time load

# Use in pipeline (fast lookups)
stream.triple_lookup(kg, ...)
```

---

## When to Use SabotQL?

**Use SabotQL for:**
- ✅ Semi-structured data (variable properties per entity)
- ✅ Graph relationships (multi-hop queries)
- ✅ RDF/OWL data sources
- ✅ Flexible schema (add properties without migrations)

**Use Sabot SQL for:**
- ✅ Fixed schema (all rows have same columns)
- ✅ Simple joins (foreign keys)
- ✅ Maximum performance
- ✅ SQL ecosystem compatibility

**Use Both:**
- Companies in SQL (structured data)
- Relationships in RDF (graph data)

---

## Examples

Run the examples:

```bash
cd examples/sabot_ql_integration

# Quickstart (5 min)
python quickstart.py

# Company enrichment (Kafka)
python example1_company_enrichment.py

# Fraud detection (graph analysis)
python example2_fraud_detection.py

# Recommendations (collaborative filtering)
python example3_recommendation.py
```

---

## Full Documentation

- **Integration Guide:** `sabot_ql/SABOT_INTEGRATION.md`
- **Examples:** `examples/sabot_ql_integration/README.md`
- **API Reference:** `docs/integrations/SABOT_QL_INTEGRATION.md`

---

## Troubleshooting

**Problem:** `ImportError: No module named 'sabot_ql'`

**Solution:**
```bash
cd sabot_ql/bindings/python
pip install -e .
```

**Problem:** C++ library not found

**Solution:**
```bash
cd sabot_ql/build
cmake .. && make -j8
export DYLD_LIBRARY_PATH=/path/to/sabot_ql/build:$DYLD_LIBRARY_PATH
```

**Problem:** Slow enrichment performance

**Solution:**
- Enable batch lookups: `batch_lookups=True`
- Enable caching: `cache_size=10000`
- Use simple patterns when possible

---

## What Next?

**Just starting:** Run `python quickstart.py`

**Have RDF data:** Load with `kg.load_ntriples('your_data.nt')`

**Need complex queries:** Write SPARQL patterns with multi-hop traversal

**Need performance:** Enable batching + caching, see `benchmark_triple_lookup.py`

---

**Ready to use!** Add `.triple_lookup()` to any Sabot stream.

---

**Last Updated:** October 14, 2025  
**Status:** ✅ Production Ready


