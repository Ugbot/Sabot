# Flink-Style Fraud Detection Demo

Real-time fraud detection on 200K banking transactions using Sabot's Cython-accelerated agent/actor system with **CLI-based deployment**.

## Architecture

The demo consists of **three separate processes** (recommended setup):

1. **Kafka/Redpanda** (via Docker Compose): Message broker with 3-partition topic
2. **Sabot Worker** (via CLI): Fraud detection agents consuming from Kafka
3. **Producer** (Python script): Reads CSV files and produces transactions to Kafka

This three-process architecture demonstrates a production-ready streaming setup where infrastructure, processing, and data ingestion are completely decoupled.

### NEW: CLI-Based Deployment

The fraud detector now uses **Sabot's App API** and can be started with the **Faust-style CLI**:
```bash
sabot -A examples.fraud_app:app worker
```

This provides:
- ✅ Clean separation of concerns
- ✅ Production-ready worker management
- ✅ Automatic Kafka consumer coordination
- ✅ Built-in checkpoint management
- ✅ Simplified deployment

## Features

### Cython Modules Used
- ✅ **FastArrowAgent** - Agent/actor processing model
- ✅ **BarrierTracker** - Distributed checkpoint coordination
- ✅ **WatermarkTracker** - Event-time processing
- ✅ **UltraFastMemoryBackend** - High-performance state management (1M+ ops/sec)

### Fraud Detection Patterns
- **Velocity Check**: Multiple transactions in short time window (60s)
- **Amount Anomaly**: Unusually large transactions (>3σ from mean)
- **Geographic Anomaly**: Impossible travel between cities

### Data
- 200K real banking transactions from Chile and Peru
- Multiple currencies (CLP, PEN)
- Various transaction types (deposits, withdrawals, transfers, payments)
- Multiple cities across both countries

## Prerequisites

### 1. Install Dependencies

```bash
# Sabot package (with Cython extensions)
cd sabot
pip install -e .

# Kafka Python client (already included in requirements)
pip install confluent-kafka
```

### 2. Start Docker Services

```bash
# From sabot root directory
docker compose up -d

# This starts:
# - Redpanda (Kafka-compatible broker) on port 19092
# - Redpanda Console (web UI) on port 8080
# - PostgreSQL (for DBOS durable execution) on port 5432
# - Redis (for distributed state) on port 6379
```

### 3. Verify Services Are Running

```bash
# Check Redpanda is healthy
docker compose ps

# Or check via Redpanda Console
open http://localhost:8080
```

## Running the Demo (Three-Terminal Setup)

### Terminal 1: Start Kafka/Redpanda

```bash
# From sabot root directory
docker compose up -d

# Check logs
docker compose logs -f redpanda
```

You should see:
```
redpanda  | Successfully started Redpanda!
redpanda  | Kafka API listening on 0.0.0.0:19092
```

### Terminal 2: Start Sabot Worker with CLI ✨ NEW

```bash
# From sabot root directory
sabot -A examples.fraud_app:app worker --loglevel=INFO

# Or with short flags (Faust-style)
sabot -A examples.fraud_app:app worker -l INFO
```

You should see:
```
🤖 Sabot Fraud Detection System
======================================================================
📦 Using Sabot Modules:
   ✅ BarrierTracker (distributed checkpointing)
   ✅ WatermarkTracker (event-time processing)
   ✅ MemoryBackend (state management)

🎯 Architecture:
   • Sabot App API with @app.agent decorator
   • Automatic Kafka consumer management
   • Multi-pattern fraud detection
   • Clean CLI integration

✅ Initializing...
✅ Fraud detection agent started
```

### Terminal 3: Start the Producer

```bash
# From sabot directory
python examples/flink_fraud_producer.py
```

You should see:
```
🏦 Bank Transaction Producer
📡 Setting up Kafka...
✅ Created Kafka topic 'bank-transactions' with 3 partitions
📤 Loading Chile transactions...
📤 Loading Peru transactions...
```

### What You'll See

**Terminal 2 (Sabot Worker)** will show:
- Real-time fraud alerts as they're detected
- Performance metrics every 10 seconds
- Throughput and latency statistics
- Alert counts

**Terminal 3 (Producer)** will show:
- Progress loading CSV files
- Throughput statistics
- Completion status

## Expected Results

### Performance Targets
- **Throughput**: 3,000-6,000 txn/s (limited by rate limiter)
- **Latency**: <1ms p99 for fraud detection
- **Memory**: <500MB for all agents
- **Checkpoints**: Every 5 seconds

### Sample Output

```
🚨 FRAUD [0]: VELOCITY - Account 897013 - 3 transactions in 60s
🟠 FRAUD [1]: AMOUNT_ANOMALY - Account 764706 - Amount 15000.00 CLP is 4.2σ from mean 850.00
🚨 FRAUD [2]: GEO_IMPOSSIBLE - Santiago→Punta Arenas in 2.5min (2089km)

📊 Sabot Agent Metrics
Agent 0:
  Processed: 67,234 txns
  Throughput: 3,361 txn/s
  Latency p50/p95/p99: 0.12ms / 0.45ms / 0.89ms
  Checkpoints: 4
  Fraud Alerts: 142

🎯 Total Processed: 200,001 transactions
🚨 Total Fraud Alerts: 428
```

## Architecture Details

### Producer Process
- Reads 2 CSV files concurrently (Chilean + Peruvian)
- Rate-limited to 3,000 txn/s per country
- Uses account_number as Kafka partition key
- Produces to 3-partition topic

### Detector Process (3 Sabot Agents)
- Each agent processes one Kafka partition independently
- Agents coordinate via distributed checkpoints (Chandy-Lamport algorithm)
- Event-time processing with watermarks
- Cython-accelerated state management
- Zero-copy batch operations

### Exactly-Once Processing
- Manual offset commits after successful processing
- Distributed checkpointing with barrier alignment
- State snapshots coordinated across all agents
- Fault-tolerant recovery (state can be restored from checkpoints)

## Troubleshooting

### "Connection refused" errors
- Kafka is not running on localhost:9092
- Start Kafka (see Prerequisites section)
- Or update `bootstrap_servers` in the scripts

### "confluent-kafka not installed"
```bash
pip install confluent-kafka
```

### "No module named 'sabot'"
```bash
# From sabot root directory
pip install -e .
```

### Agents not receiving messages
- Make sure producer completes first, OR
- Start detector before producer, OR
- Check Kafka topic has data:
```bash
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic bank-transactions --from-beginning --max-messages 10
```

### Low throughput
- Check CPU usage (should be <50% per agent)
- Increase batch size in producer
- Add more partitions to Kafka topic

## Next Steps

### Enhancements to Try
1. **Add more fraud patterns**: Time-of-day anomalies, spending category changes
2. **ML integration**: Use trained model for anomaly detection
3. **Windowed aggregations**: Add tumbling/sliding windows for statistics
4. **External enrichment**: Join with customer data from external database
5. **Alert routing**: Send high-severity alerts to different systems
6. **Dashboards**: Add Grafana/Prometheus monitoring

### Performance Tuning
1. **Increase parallelism**: More partitions = more agents
2. **Tune batch sizes**: Balance latency vs throughput
3. **Optimize state**: Use RocksDB for larger state
4. **Add caching**: Cache frequent lookups

## Architecture Comparison

| Feature | This Demo | Apache Flink |
|---------|-----------|--------------|
| Language | Python + Cython | Java/Scala |
| Agent/Actor Model | ✅ FastArrowAgent | ❌ TaskManager |
| Checkpointing | ✅ Chandy-Lamport | ✅ Asynchronous Barriers |
| Event-Time | ✅ Watermarks | ✅ Watermarks |
| State Backend | ✅ Cython/RocksDB | ✅ RocksDB |
| Performance | 5K-10K txn/s | 100K+ txn/s |
| Memory | <500MB | 2-4GB |

## Files

- **`fraud_app.py`** ✨ NEW - Sabot App with CLI support (recommended)
- `flink_fraud_producer.py` - Data producer (separate process)
- `flink_fraud_detector.py` - Standalone detector (legacy, still works)
- `flink_fraud_demo.py` - Original combined version (deprecated)
- `FRAUD_DEMO_README.md` - This file
- `../docker-compose.yml` - Docker services (Redpanda, Postgres, Redis)

## Quick Reference

### Start Everything (Three Commands)
```bash
# Terminal 1: Infrastructure
docker compose up -d

# Terminal 2: Sabot Worker (NEW CLI method)
sabot -A examples.fraud_app:app worker

# Terminal 3: Producer
python examples/flink_fraud_producer.py
```

### Stop Everything
```bash
# Ctrl+C in Terminal 2 and 3
# Then stop Docker:
docker compose down
```

### Monitor
- **Redpanda Console**: http://localhost:8080
- **Metrics**: Watch Terminal 2 (Sabot worker output)
- **Logs**: `docker compose logs -f redpanda`

### Alternative: Legacy Standalone Method
```bash
# Instead of Terminal 2 CLI command, you can still use:
python examples/flink_fraud_detector.py
```

## Comparison: CLI vs Standalone

| Feature | CLI (`fraud_app.py`) | Standalone (`flink_fraud_detector.py`) |
|---------|---------------------|----------------------------------------|
| Deployment | `sabot -A` command | `python` script |
| Worker management | Automatic | Manual |
| Kafka consumers | Managed by Sabot | Manual assignment |
| Checkpoints | Built-in | Custom implementation |
| Production-ready | ✅ Yes | ⚠️ Demo only |
| Best for | Production, scaling | Learning, debugging |

## License

Part of the Sabot streaming framework.
