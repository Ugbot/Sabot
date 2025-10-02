# Sabot CLI Guide

Complete command-line reference for deploying and managing Sabot streaming applications.

## Overview

Sabot provides a **Faust-style CLI** for production deployments, making it easy to run streaming applications without writing boilerplate startup code.

```bash
# Basic usage
sabot -A myapp:app worker

# With options
sabot -A myapp:app worker --concurrency 4 --loglevel INFO
```

## Installation

```bash
# Install Sabot with CLI
pip install -e .

# Verify installation
sabot --version
```

## Quick Start

### 1. Create Your App

**`myapp.py`:**
```python
import sabot as sb

app = sb.App('my-app', broker='kafka://localhost:19092')

@app.agent('my-topic')
async def process_events(stream):
    async for event in stream:
        print(f"Processing: {event}")
        yield event
```

### 2. Start Worker

```bash
# Start single worker
sabot -A myapp:app worker

# Start with concurrency
sabot -A myapp:app worker --concurrency 4
```

That's it! Sabot handles all the infrastructure setup.

---

## Commands

### `worker`

Start a Sabot worker to process streaming data.

**Signature:**
```bash
sabot -A <module:app> worker [OPTIONS]
```

**Required Arguments:**
- `-A, --app <module:app>`: Module path to your Sabot app (e.g., `myapp:app` or `examples.fraud_app:app`)

**Options:**

| Flag | Description | Default | Example |
|------|-------------|---------|---------|
| `-c, --concurrency <N>` | Number of concurrent workers | 1 | `-c 4` |
| `-b, --broker <URL>` | Override broker URL | From app config | `-b kafka://prod:9092` |
| `-l, --loglevel <LEVEL>` | Logging level | INFO | `-l DEBUG` |

**Examples:**

```bash
# Start single worker
sabot -A myapp:app worker

# Multiple workers (concurrency)
sabot -A myapp:app worker -c 8

# Override broker (for different environments)
sabot -A myapp:app worker -b kafka://prod.example.com:9092

# Debug mode
sabot -A myapp:app worker --loglevel DEBUG

# Combine options
sabot -A examples.fraud_app:app worker \
  --concurrency 4 \
  --broker kafka://localhost:19092 \
  --loglevel INFO
```

---

## Global Options

### `-A, --app` (Required)

Specifies the Python module path to your Sabot application.

**Format:** `<module>:<variable>`

**Examples:**
```bash
# Simple module
sabot -A myapp:app worker

# Nested package
sabot -A examples.fraud_app:app worker

# Deep package structure
sabot -A myproject.streaming.apps.fraud:app worker
```

**How it works:**
1. CLI imports the Python module (`myproject.streaming.apps.fraud`)
2. Retrieves the app variable (`app`)
3. Calls `app.run()` to start the streaming application

### `--version`

Display Sabot version.

```bash
sabot --version
# Output: Sabot v0.1.0
```

### `--help`

Show help message.

```bash
sabot --help        # Global help
sabot worker --help # Command-specific help
```

---

## Environment Variables

Sabot respects environment variables for configuration:

| Variable | Description | Example |
|----------|-------------|---------|
| `SABOT_BROKER` | Default Kafka broker URL | `kafka://localhost:19092` |
| `SABOT_LOGLEVEL` | Default log level | `INFO` |
| `SABOT_DATA_DIR` | Data directory for demos | `/data/banking` |
| `SABOT_STATE_DIR` | State storage directory | `./state` |

**Usage:**
```bash
# Set broker via environment
export SABOT_BROKER=kafka://prod:9092
sabot -A myapp:app worker

# Or inline
SABOT_LOGLEVEL=DEBUG sabot -A myapp:app worker
```

---

## Deployment Patterns

### Local Development

```bash
# Start infrastructure
docker compose up -d

# Run worker
sabot -A myapp:app worker --loglevel DEBUG
```

### Production Deployment

#### Single Machine

```bash
# Start multiple workers for throughput
sabot -A myapp:app worker --concurrency 8 --loglevel INFO
```

#### Distributed (Multiple Machines)

**Machine 1:**
```bash
sabot -A myapp:app worker -c 4 -b kafka://prod:9092
```

**Machine 2:**
```bash
sabot -A myapp:app worker -c 4 -b kafka://prod:9092
```

Kafka consumer groups automatically handle load balancing.

#### Docker Container

**Dockerfile:**
```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY . /app

RUN pip install -e .

CMD ["sabot", "-A", "myapp:app", "worker", "--concurrency", "4"]
```

**Run:**
```bash
docker build -t myapp .
docker run -e SABOT_BROKER=kafka://kafka:9092 myapp
```

#### Kubernetes

**deployment.yaml:**
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: sabot-worker
spec:
  replicas: 3
  selector:
    matchLabels:
      app: sabot-worker
  template:
    metadata:
      labels:
        app: sabot-worker
    spec:
      containers:
      - name: sabot
        image: myapp:latest
        command: ["sabot", "-A", "myapp:app", "worker", "-c", "4"]
        env:
        - name: SABOT_BROKER
          value: "kafka://kafka-service:9092"
        resources:
          limits:
            memory: "1Gi"
            cpu: "1000m"
```

**Deploy:**
```bash
kubectl apply -f deployment.yaml
kubectl get pods
kubectl logs -f sabot-worker-<pod-id>
```

---

## Advanced Usage

### Multiple Apps

Run different apps on different workers:

```bash
# Terminal 1: Fraud detection
sabot -A apps.fraud:app worker -c 4

# Terminal 2: Analytics
sabot -A apps.analytics:app worker -c 2

# Terminal 3: Alerting
sabot -A apps.alerts:app worker -c 1
```

### Custom State Backends

**App code:**
```python
import sabot as sb

# RocksDB for large state
rocksdb_config = sb.BackendConfig(
    backend_type="rocksdb",
    path="./state/rocksdb"
)
backend = sb.RocksDBBackend(rocksdb_config)

app = sb.App('myapp', broker='kafka://localhost:19092')
```

**Run:**
```bash
# State persisted to ./state/rocksdb
sabot -A myapp:app worker
```

### Redis Distributed State

**App code:**
```python
app = sb.App(
    'distributed-app',
    broker='kafka://localhost:19092',
    enable_distributed_state=True,
    redis_host='redis.prod.example.com',
    redis_port=6379
)
```

**Run:**
```bash
# Multiple workers share Redis state
sabot -A myapp:app worker -c 4
```

### PostgreSQL Durable Execution

**App code:**
```python
app = sb.App(
    'durable-app',
    broker='kafka://localhost:19092',
    database_url='postgresql://user:pass@db.example.com/sabot'
)
```

**Run:**
```bash
# Checkpoints persisted to PostgreSQL
sabot -A myapp:app worker
```

---

## Monitoring & Debugging

### Log Levels

```bash
# DEBUG: Verbose output, all events
sabot -A myapp:app worker --loglevel DEBUG

# INFO: Standard output, important events
sabot -A myapp:app worker --loglevel INFO

# WARNING: Only warnings and errors
sabot -A myapp:app worker --loglevel WARNING

# ERROR: Only errors
sabot -A myapp:app worker --loglevel ERROR
```

### Viewing Logs

**Stdout/stderr:**
```bash
# Default: logs to console
sabot -A myapp:app worker

# Redirect to file
sabot -A myapp:app worker > logs/worker.log 2>&1

# Tail logs
tail -f logs/worker.log
```

**Structured logging (JSON):**
```python
import logging
import json

class JsonFormatter(logging.Formatter):
    def format(self, record):
        return json.dumps({
            'timestamp': record.created,
            'level': record.levelname,
            'message': record.getMessage()
        })

# Configure in app
logging.basicConfig(
    level=logging.INFO,
    handlers=[logging.StreamHandler()]
)
```

### Metrics

Sabot provides built-in metrics:

```python
from sabot.metrics import metrics_manager

@app.agent('events')
async def process(stream):
    async for event in stream:
        # Metrics tracked automatically
        metrics_manager.increment('events_processed')
        yield event
```

**View metrics:**
```bash
# Metrics logged every 10 seconds
sabot -A myapp:app worker --loglevel INFO
```

---

## Troubleshooting

### "Module not found: myapp"

**Problem:** CLI cannot find your app module.

**Solutions:**
```bash
# 1. Ensure you're in the correct directory
cd /path/to/project

# 2. Check PYTHONPATH
export PYTHONPATH=/path/to/project:$PYTHONPATH
sabot -A myapp:app worker

# 3. Install as editable package
pip install -e .
sabot -A myapp:app worker
```

### "AttributeError: module 'myapp' has no attribute 'app'"

**Problem:** Variable name doesn't match `-A` argument.

**Solution:**
```python
# Ensure variable is named correctly
# File: myapp.py
import sabot as sb
app = sb.App('myapp')  # ✅ Variable named 'app'

# CLI expects:
sabot -A myapp:app worker
```

### "Kafka connection refused"

**Problem:** Cannot connect to Kafka broker.

**Solutions:**
```bash
# 1. Check if Kafka is running
docker compose ps
docker compose logs redpanda

# 2. Verify broker URL
sabot -A myapp:app worker -b kafka://localhost:19092

# 3. Test connectivity
telnet localhost 19092

# 4. Check firewall
sudo ufw status
```

### "Port already in use"

**Problem:** Another process is using the port.

**Solutions:**
```bash
# Find process using port
lsof -i :19092

# Kill process
kill -9 <PID>

# Or change port in app
app = sb.App('myapp', broker='kafka://localhost:19093')
```

### Worker Crashes Immediately

**Problem:** Worker starts but crashes.

**Debug steps:**
```bash
# 1. Enable debug logging
sabot -A myapp:app worker --loglevel DEBUG

# 2. Check for import errors
python -c "import myapp; print(myapp.app)"

# 3. Verify dependencies
pip list | grep sabot
pip list | grep confluent-kafka

# 4. Test app manually
python -c "
import asyncio
import myapp
asyncio.run(myapp.app.start())
"
```

### High Memory Usage

**Problem:** Worker consuming too much memory.

**Solutions:**
```bash
# 1. Reduce concurrency
sabot -A myapp:app worker -c 1

# 2. Use RocksDB for large state
# (Configure in app code)

# 3. Enable TTL on state
config = sb.BackendConfig(
    backend_type="memory",
    ttl_seconds=300.0  # 5 minutes
)
```

### Slow Processing

**Problem:** Low throughput.

**Solutions:**
```bash
# 1. Increase concurrency
sabot -A myapp:app worker -c 8

# 2. Use batching in app
@app.agent('events')
async def process(stream):
    async for batch in stream.batch(size=100):
        process_batch(batch)

# 3. Profile with cProfile
python -m cProfile -o profile.out -m sabot -A myapp:app worker
```

---

## Comparison to Other CLIs

### Faust

```bash
# Faust
faust -A myapp:app worker

# Sabot (identical syntax!)
sabot -A myapp:app worker
```

### Flink

```bash
# Flink (complex)
flink run -c com.example.MyJob myapp.jar --arg1 value1

# Sabot (simple)
sabot -A myapp:app worker
```

### Kafka Streams

```bash
# Kafka Streams (no CLI, requires Java main)
java -jar myapp.jar

# Sabot
sabot -A myapp:app worker
```

---

## Best Practices

### 1. Use Environment-Specific Configs

**config.py:**
```python
import os

BROKER = os.getenv('SABOT_BROKER', 'kafka://localhost:19092')
LOGLEVEL = os.getenv('SABOT_LOGLEVEL', 'INFO')
```

**myapp.py:**
```python
import sabot as sb
from config import BROKER

app = sb.App('myapp', broker=BROKER)
```

**Deploy:**
```bash
# Dev
SABOT_BROKER=kafka://localhost:19092 sabot -A myapp:app worker

# Prod
SABOT_BROKER=kafka://prod:9092 sabot -A myapp:app worker
```

### 2. Use Systemd for Production

**`/etc/systemd/system/sabot-worker.service`:**
```ini
[Unit]
Description=Sabot Worker
After=network.target

[Service]
Type=simple
User=sabot
WorkingDirectory=/opt/myapp
Environment="SABOT_BROKER=kafka://prod:9092"
ExecStart=/usr/bin/sabot -A myapp:app worker -c 4
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

**Commands:**
```bash
sudo systemctl enable sabot-worker
sudo systemctl start sabot-worker
sudo systemctl status sabot-worker
sudo journalctl -u sabot-worker -f
```

### 3. Use Process Managers

**Supervisor:**
```ini
[program:sabot-worker]
command=/usr/bin/sabot -A myapp:app worker -c 4
directory=/opt/myapp
user=sabot
autostart=true
autorestart=true
redirect_stderr=true
stdout_logfile=/var/log/sabot/worker.log
```

**PM2:**
```bash
pm2 start "sabot -A myapp:app worker -c 4" --name sabot-worker
pm2 save
pm2 startup
```

### 4. Graceful Shutdown

Sabot handles SIGINT/SIGTERM gracefully:

```bash
# Send interrupt (Ctrl+C)
# Worker will:
# 1. Stop consuming new messages
# 2. Finish processing current batch
# 3. Commit offsets
# 4. Close connections
# 5. Exit cleanly
```

---

## Examples

### Real-World Deployments

#### E-commerce Order Processing

```bash
# orders_app.py
import sabot as sb

app = sb.App('orders', broker='kafka://prod:9092')

@app.agent('orders')
async def process_orders(stream):
    async for order in stream:
        # Validate, charge, fulfill
        yield {'order_id': order['id'], 'status': 'fulfilled'}

# Deploy
sabot -A orders_app:app worker -c 8 --loglevel INFO
```

#### Real-Time Analytics

```bash
# analytics_app.py
import sabot as sb

app = sb.App('analytics', broker='kafka://prod:9092')

backend = sb.MemoryBackend(
    sb.BackendConfig(backend_type="memory", max_size=1000000)
)

@app.agent('events')
async def track_metrics(stream):
    async for event in stream:
        await update_metrics(event)
        yield event

# Deploy
sabot -A analytics_app:app worker -c 4
```

---

## Getting Help

```bash
# Global help
sabot --help

# Command help
sabot worker --help

# Version
sabot --version
```

**Resources:**
- **Documentation**: [docs/](.)
- **Examples**: [examples/](../examples)
- **Issues**: [GitHub Issues](https://github.com/yourusername/sabot/issues)

---

**Next**: Try the [Fraud Detection Demo](../examples/FRAUD_DEMO_README.md) for a complete example!
