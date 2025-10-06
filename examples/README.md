# Sabot Examples

**Clean, focused examples demonstrating Sabot's core capabilities.**

All examples use vendored Apache Arrow (no pip pyarrow dependency) and showcase real-world streaming patterns.

## 🎯 Featured Examples

### 1. **Fraud Detection** (`fraud_app.py`) ⭐

Real-time fraud detection with stateful pattern matching.

```bash
# Start worker
sabot -A examples.fraud_app:app worker
```

**Demonstrates:** Multi-pattern fraud detection, stateful processing, 143K-260K txn/sec

### 2. **Batch-First API** (`batch_first_examples.py`) ⭐

Everything is batches in Sabot - unified batch/streaming model.

```bash
python examples/batch_first_examples.py
```

### 3. **Dimension Tables** (`dimension_tables_demo.py`) ⭐

Stream enrichment with materialized dimension tables.

```bash
python examples/dimension_tables_demo.py
```

### 4. **Fintech Enrichment** (`fintech_enrichment_demo/`) ⭐

Complete real-world pipeline with 10M+ rows, multi-stream joins.

```bash
cd examples/fintech_enrichment_demo
python arrow_optimized_enrichment.py --securities 100000
```

See [fintech_enrichment_demo/README.md](fintech_enrichment_demo/README.md)

## 📁 Directory Structure

- `fraud_app.py`, `batch_first_examples.py`, `dimension_tables_demo.py` - Featured demos
- `data/` - Arrow operations showcase
- `fintech/` - Financial market data processing demos
  - `inventory_topn_csv.py` - TopN ranking with best bid/offer calculation (CSV)
  - `inventory_topn_kafka.py` - TopN ranking with Kafka streaming
- `fintech_enrichment_demo/` - Production pipeline (see dedicated README)

**Total:** 30 examples (cleaned from 58)

## 🚀 Quick Start

```bash
# Install (builds vendored Arrow)
pip install -e .

# Start infrastructure
docker compose up -d

# Run standalone example
python examples/batch_first_examples.py

# Or run with Kafka
sabot -A examples.fraud_app:app worker
```

## 📖 Learn More

- [Fintech Demo Guide](fintech_enrichment_demo/README.md)
- [Main README](../README.md)
- [Arrow Migration](../docs/ARROW_MIGRATION.md)
