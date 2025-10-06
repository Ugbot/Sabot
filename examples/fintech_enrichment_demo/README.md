# Fintech Data Enrichment Demo

**Production-scale streaming pipeline: 10M securities, 1.2M quotes, 1M trades**

Demonstrates multi-stream joins, window operations, and 100M+ rows/sec throughput.

## 🚀 Quick Start

### 1. Generate Data (one-time, ~5-10 minutes)

```bash
cd examples/fintech_enrichment_demo
python master_security_synthesiser.py    # 10M rows, 5.5GB
python invenory_rows_synthesiser.py      # 1.2M rows, 193MB  
python trax_trades_synthesiser.py        # 1M rows, 1.2GB
```

### 2. Convert to Arrow IPC (10-100x faster loading)

```bash
python convert_csv_to_arrow.py
```

### 3. Run Demo

```bash
# Arrow IPC (recommended - 52x faster than CSV)
python arrow_optimized_enrichment.py --securities 100000 --quotes 50000

# CSV (for comparison)
python csv_enrichment_demo.py --batch-securities 100000
```

## 📊 Performance

| Operation | Arrow IPC | CSV | Speedup |
|-----------|-----------|-----|---------|
| Load 10M rows | 2.0s | 103s | **52x** |
| Hash join | 0.107s | N/A | 104M rows/sec |
| Total pipeline | 2.3s | 103s+ | **45x** |

## 📁 Files

**Core Demos:**
- `arrow_optimized_enrichment.py` - Main demo (Arrow IPC)
- `csv_enrichment_demo.py` - CSV variant  
- `convert_csv_to_arrow.py` - CSV → Arrow converter

**Data Generators:**
- `master_security_synthesiser.py` - 10M securities
- `invenory_rows_synthesiser.py` - 1.2M quotes
- `trax_trades_synthesiser.py` - 1M trades

**Kafka Producers:** 
- `kafka_security_producer.py`, `kafka_inventory_producer.py`, `kafka_trades_producer.py`

**Custom Operators:**
- `operators/csv_source.py`, `enrichment.py`, `windowing.py`, `ranking.py`

## 💡 Key Learnings

1. **Arrow IPC is 52x faster than CSV** (memory-mapped, zero-copy)
2. **Batch size matters** (100K rows = sweet spot)
3. **Zero-copy joins** (104M rows/sec with SIMD)
4. **Multi-stream enrichment** (fact ⋈ dimension pattern)

## 🐛 Troubleshooting

**Out of memory:** Reduce batch sizes `--securities 10000`

**Files not found:** Run data generators first

**Slow loading:** Verify vendored Arrow: `python -c "from sabot import cyarrow; print(cyarrow.USING_ZERO_COPY)"`

## 📖 More Info

See [examples/README.md](../README.md) for all examples
