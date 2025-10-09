# MySQL CDC Examples

Comprehensive examples demonstrating MySQL Change Data Capture with Sabot.

## Quick Start

### 1. Start MySQL

```bash
cd /Users/bengamble/Sabot
docker compose up -d mysql

# Wait for MySQL to be ready
docker compose logs mysql | grep "ready for connections"
```

### 2. Run Examples

```bash
# Basic CDC streaming
python examples/mysql_cdc/01_basic_cdc.py

# E-commerce order processing
python examples/mysql_cdc/02_ecommerce_orders.py

# Real-time analytics
python examples/mysql_cdc/03_realtime_analytics.py

# Cache invalidation
python examples/mysql_cdc/04_cache_invalidation.py

# Data replication
python examples/mysql_cdc/05_data_replication.py

# Audit logging
python examples/mysql_cdc/06_audit_log.py
```

## Examples Overview

### Core Examples

1. **`01_basic_cdc.py`** - Basic CDC setup and streaming
   - Connect to MySQL binlog
   - Stream INSERT/UPDATE/DELETE events
   - Process events in real-time

2. **`mysql_cdc_demo.py`** - Simple demo
   - Minimal setup
   - Event printing
   - Good for learning

### Feature Examples

3. **`mysql_cdc_auto_demo.py`** - Auto-configuration
   - Configuration validation
   - Wildcard table patterns
   - Automatic setup

4. **`mysql_cdc_ddl_demo.py`** - DDL tracking
   - Schema change detection
   - ALTER TABLE monitoring
   - Schema version management

5. **`mysql_cdc_routing_demo.py`** - Per-table routing
   - Route events by table
   - Multiple processing pipelines
   - Event filtering

### Real-World Use Cases

6. **`02_ecommerce_orders.py`** - E-commerce order processing
   - Order state machine
   - Inventory updates
   - Payment processing
   - Real-time dashboards

7. **`03_realtime_analytics.py`** - Streaming analytics
   - Aggregate metrics
   - Windowed computations
   - Live KPIs

8. **`04_cache_invalidation.py`** - Cache management
   - Redis cache invalidation
   - Smart cache updates
   - TTL management

9. **`05_data_replication.py`** - Cross-database replication
   - MySQL → DuckDB
   - MySQL → Parquet
   - Data warehousing

10. **`06_audit_log.py`** - Compliance and auditing
    - Change tracking
    - User activity logging
    - Compliance reports

### Performance

11. **`mysql_cdc_benchmark.py`** - Performance benchmark
    - 10K record throughput
    - Latency measurements
    - Batch processing metrics

## Docker Services

### MySQL Configuration

```yaml
mysql:
  image: mysql:8.0
  ports:
    - "3307:3306"
  environment:
    MYSQL_ROOT_PASSWORD: sabot
    MYSQL_DATABASE: sabot
  command:
    - --server-id=1
    - --log-bin=mysql-bin
    - --binlog-format=ROW
    - --binlog-row-image=FULL
    - --gtid-mode=ON
    - --enforce-gtid-consistency=ON
```

### Connect to MySQL

```bash
# MySQL CLI
docker compose exec mysql mysql -u root -psabot sabot

# From host
mysql -h localhost -P 3307 -u root -psabot sabot
```

## Common Patterns

### Basic CDC Stream

```python
from sabot._cython.connectors.mysql_cdc import MySQLCDCConnector, MySQLCDCConfig

config = MySQLCDCConfig(
    host="localhost",
    port=3307,
    user="root",
    password="sabot",
    database="sabot",
    only_tables=["sabot.orders"],
)

connector = MySQLCDCConnector(config)

async with connector:
    async for batch in connector.stream_batches():
        # Process batch
        process_events(batch)
```

### Auto-Configuration

```python
connector = MySQLCDCConnector.auto_configure(
    host="localhost",
    port=3307,
    user="root",
    password="sabot",
    table_patterns=["sabot.orders", "sabot.users"],
    validate_config=True,
)
```

### Per-Table Routing

```python
from sabot._cython.connectors.mysql_stream_router import MySQLStreamRouter

router = MySQLStreamRouter()
router.add_route("sabot", "orders", output_key="orders")
router.add_route("sabot", "users", output_key="users")

# Route events
routed = router.route_batch(batch)
await process_orders(routed["orders"])
await process_users(routed["users"])
```

## Troubleshooting

### MySQL Not Ready

```bash
# Check status
docker compose ps mysql

# View logs
docker compose logs mysql

# Restart if needed
docker compose restart mysql
```

### Connection Refused

Check if MySQL is listening on port 3307:

```bash
lsof -i :3307
netstat -an | grep 3307
```

### Binlog Not Enabled

Verify binlog configuration:

```sql
SHOW VARIABLES LIKE 'log_bin';
SHOW VARIABLES LIKE 'binlog_format';
SHOW VARIABLES LIKE 'binlog_row_image';
```

### No Events Received

Check binlog position:

```sql
SHOW MASTER STATUS;
```

Insert test data:

```sql
INSERT INTO sabot.orders (customer_id, total_amount)
VALUES (1, 99.99);
```

## Performance Tips

1. **Batch Size**: Adjust `batch_size` for your workload
   - Small batches (10-100): Low latency
   - Large batches (1000-10000): High throughput

2. **Table Filtering**: Use `only_tables` to reduce overhead
   ```python
   only_tables=["sabot.orders", "sabot.users"]
   ```

3. **Event Filtering**: Filter events early
   ```python
   only_events=[WriteRowsEvent, UpdateRowsEvent]  # Skip deletes
   ```

4. **Disable Features**: Turn off unused features
   ```python
   track_ddl=False  # Skip DDL tracking
   emit_ddl_events=False  # Don't emit DDL to stream
   ```

## Next Steps

1. Review examples to understand CDC patterns
2. Adapt examples to your use case
3. Run benchmarks to measure performance
4. Deploy to production with monitoring

## See Also

- [MySQL CDC Documentation](../../docs/MYSQL_CDC.md)
- [PostgreSQL CDC Examples](../postgres_cdc/)
- [Benchmark README](BENCHMARK_README.md)
