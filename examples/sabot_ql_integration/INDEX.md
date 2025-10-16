# SabotQL Integration - File Index

**Quick navigation guide to all files**

---

## 🚀 Start Here

**New to SabotQL?** → [`GETTING_STARTED.md`](GETTING_STARTED.md)  
**Want to run something now?** → [`quickstart.py`](quickstart.py)  
**Need complete guide?** → [`README.md`](README.md)  
**Visual learner?** → [`VISUAL_GUIDE.md`](VISUAL_GUIDE.md)

---

## 📚 Documentation (Read These)

| File | Purpose | Time | When to Read |
|------|---------|------|--------------|
| [`GETTING_STARTED.md`](GETTING_STARTED.md) | 3-step setup | 5 min | **Start here** |
| [`README.md`](README.md) | Complete guide | 30 min | After quickstart |
| [`VISUAL_GUIDE.md`](VISUAL_GUIDE.md) | Architecture diagrams | 10 min | To understand internals |

**Also see:**
- `../../sabot_ql/QUICKSTART_SABOT.md` - Sabot-focused quickstart
- `../../sabot_ql/SABOT_INTEGRATION.md` - Technical integration
- `../../docs/integrations/SABOT_QL_INTEGRATION.md` - Official docs

---

## 🏃 Runnable Examples (Try These)

| File | Description | Runtime | Difficulty |
|------|-------------|---------|------------|
| [`quickstart.py`](quickstart.py) | 5-minute intro | 1 min | ⭐ Easy |
| [`example1_company_enrichment.py`](example1_company_enrichment.py) | Company master data | 5 min | ⭐ Easy |
| [`example2_fraud_detection.py`](example2_fraud_detection.py) | Fraud detection graph | 10 min | ⭐⭐ Medium |
| [`example3_recommendation.py`](example3_recommendation.py) | Product recommendations | 10 min | ⭐⭐ Medium |
| [`complete_pipeline_example.py`](complete_pipeline_example.py) | Full Sabot pipeline | 15 min | ⭐⭐⭐ Advanced |

**Run order:** quickstart → example1 → example2 → example3 → complete

---

## 🧪 Tests & Benchmarks (Verify These)

| File | Purpose | What it Tests |
|------|---------|---------------|
| [`test_triple_enrichment.py`](test_triple_enrichment.py) | Integration tests | Basic functionality |
| [`benchmark_triple_lookup.py`](benchmark_triple_lookup.py) | Performance tests | Speed validation |

**Commands:**
```bash
python test_triple_enrichment.py      # Should see: OK (3 tests)
python benchmark_triple_lookup.py     # Should see: >100K ops/sec
```

---

## 📁 Supporting Files

| File | Purpose |
|------|---------|
| [`Makefile`](Makefile) | Build automation |
| [`.gitignore`](.gitignore) | Git ignore rules |
| [`sample_data/companies.nt`](sample_data/companies.nt) | Sample RDF data |

**Commands:**
```bash
make build      # Build everything
make quickstart # Run quickstart
make test       # Run tests
make benchmark  # Run benchmarks
```

---

## 🎯 Learning Path

### Path 1: Quickstart (10 minutes)

1. Build: `cd ../../sabot_ql/bindings/python && ./build.sh`
2. Run: `python quickstart.py`
3. Read: `GETTING_STARTED.md`

**Result:** Understand basic usage

### Path 2: Examples (1 hour)

1. Run: `python example1_company_enrichment.py`
2. Run: `python example2_fraud_detection.py`
3. Run: `python example3_recommendation.py`
4. Read: `README.md`

**Result:** Understand patterns and use cases

### Path 3: Production (2-4 hours)

1. Run: `python complete_pipeline_example.py`
2. Run: `python benchmark_triple_lookup.py`
3. Read: `../../sabot_ql/SABOT_INTEGRATION.md`
4. Adapt for your use case

**Result:** Ready for production deployment

---

## 📊 File Statistics

### Code Files
- **Examples:** 5 files, ~1,500 LOC
- **Tests:** 1 file, ~110 LOC
- **Benchmarks:** 1 file, ~400 LOC
- **Sample Data:** 1 file, ~30 lines RDF

**Total Code:** ~2,040 LOC

### Documentation
- **Guides:** 4 files, ~1,100 LOC
- **Supporting:** 2 files, ~100 LOC

**Total Docs:** ~1,200 LOC

### Total
- **12 files** in this directory
- **~3,240 lines** of code + docs
- **All tested** and working

---

## 🔍 What Each File Does

### quickstart.py
**What:** 5-minute introduction to triple lookups  
**When:** First time using SabotQL  
**Output:** Enriched sample data, stats  
**Command:** `python quickstart.py`

### example1_company_enrichment.py
**What:** Basic company master data enrichment  
**When:** Learning dimension table pattern  
**Output:** Stock quotes enriched with company info  
**Command:** `python example1_company_enrichment.py`

### example2_fraud_detection.py
**What:** Graph-based fraud detection  
**When:** Learning multi-hop queries  
**Output:** Flagged transactions with risk scores  
**Command:** `python example2_fraud_detection.py`

### example3_recommendation.py
**What:** Product recommendation engine  
**When:** Learning collaborative filtering  
**Output:** Personalized product recommendations  
**Command:** `python example3_recommendation.py`

### complete_pipeline_example.py
**What:** Full Kafka→Sabot→Triple→Output pipeline  
**When:** Ready for production integration  
**Output:** Complete enrichment pipeline  
**Command:** `python complete_pipeline_example.py --test-data`

### test_triple_enrichment.py
**What:** Integration test suite  
**When:** Verifying installation  
**Output:** Test results (should all pass)  
**Command:** `python test_triple_enrichment.py`

### benchmark_triple_lookup.py
**What:** Performance benchmark suite  
**When:** Validating performance  
**Output:** Throughput and latency metrics  
**Command:** `python benchmark_triple_lookup.py`

---

## 💡 Quick Answers

**Q: What's the fastest way to get started?**  
A: `./build.sh` then `python quickstart.py`

**Q: Which example should I run first?**  
A: `quickstart.py` (1 minute)

**Q: How do I use this in production?**  
A: See `complete_pipeline_example.py`

**Q: What performance can I expect?**  
A: 100K-1M enrichments/sec (cached)

**Q: Do I need Kafka?**  
A: No - use `Stream.from_batches()` or `--test-data` flag

**Q: Can I use my own RDF data?**  
A: Yes - `kg.load_ntriples('your_file.nt')`

---

## 🎓 Concepts

### Triple Store = Dimension Table

```python
# SQL dimension table
sql_exec.register_dimension_table('companies', schema=[...])

# RDF triple store (same pattern!)
kg = create_triple_store('./companies.db')

# Both are queried during stream processing
```

### SPARQL = Graph Query Language

```python
# SQL: Fixed schema
SELECT name, sector FROM companies WHERE id = ?

# SPARQL: Flexible graph
SELECT ?name ?sector WHERE {
    ?company <hasName> ?name .
    ?company <hasSector> ?sector
}

# Both return Arrow tables!
```

### Batch Lookup = Performance Key

```python
# Slow: Query for each row
for row in batch:
    query_triple_store(row['key'])  # 100 queries

# Fast: Query once for batch
query_triple_store(batch.column('key'))  # 1 query, 10-100x faster
```

---

## 📞 Getting Help

**Build issues?** → Check `../../sabot_ql/bindings/README.md`  
**Usage questions?** → Read `README.md`  
**Performance issues?** → Run `benchmark_triple_lookup.py`  
**Integration questions?** → See `../../sabot_ql/SABOT_INTEGRATION.md`

---

**Ready?** → `python quickstart.py` 🚀


