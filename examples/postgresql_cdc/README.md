# PostgreSQL CDC Examples

Comprehensive examples demonstrating PostgreSQL Change Data Capture with Sabot using logical replication and wal2json.

## Quick Start

### 1. Start PostgreSQL

```bash
cd /Users/bengamble/Sabot
docker compose up -d postgres

# Wait for PostgreSQL to be ready
docker compose logs postgres | grep "database system is ready"
```

### 2. Initialize CDC

CDC initialization happens automatically when the Postgres container starts (via docker-compose volume mount). If you need to run it manually:

```bash
# Option 1: Execute from Python module
python -c "from sabot._cython.connectors.postgresql.init_cdc import execute_init_sql; import psycopg2; conn = psycopg2.connect(host='localhost', port=5433, user='sabot', password='sabot', database='sabot'); execute_init_sql(conn); conn.close()"

# Option 2: Execute SQL file directly
docker compose exec postgres psql -U sabot -d sabot -f /docker-entrypoint-initdb.d/01_init_cdc.sql
```

### 3. Run Examples

```bash
# Basic CDC streaming
python examples/postgresql_cdc/01_basic_cdc.py

# E-commerce order processing
python examples/postgresql_cdc/02_ecommerce_orders.py

# Real-time analytics
python examples/postgresql_cdc/03_realtime_analytics.py

# Cache invalidation
python examples/postgresql_cdc/04_cache_invalidation.py
```

## Examples Overview

### Core Examples

1. **`01_basic_cdc.py`** - Basic CDC setup and streaming
   - Connect to PostgreSQL logical replication
   - Stream INSERT/UPDATE/DELETE events
   - Process events in real-time
   - wal2json output parsing

2. **`02_ecommerce_orders.py`** - E-commerce order processing
   - Order state machine
   - Inventory updates
   - Payment processing
   - Real-time metrics

3. **`03_realtime_analytics.py`** - Streaming analytics
   - Aggregate metrics
   - Windowed computations
   - Live KPIs

4. **`04_cache_invalidation.py`** - Cache management
   - Redis cache invalidation
   - Smart cache updates
   - TTL management

## Docker Services

### PostgreSQL Configuration

```yaml
postgres:
  image: postgres:14
  ports:
    - "5433:5432"
  environment:
    POSTGRES_USER: sabot
    POSTGRES_PASSWORD: sabot
    POSTGRES_DB: sabot
  command:
    - -c
    - wal_level=logical
    - -c
    - max_replication_slots=4
    - -c
    - max_wal_senders=4
```

### Connect to PostgreSQL

```bash
# PostgreSQL CLI
docker compose exec postgres psql -U sabot -d sabot

# From host
psql -h localhost -p 5433 -U sabot -d sabot
```

## Common Patterns

### Basic CDC Stream

```python
from sabot._cython.connectors.postgresql import (
    PostgreSQLCDCConnector,
    PostgreSQLCDCConfig
)

config = PostgreSQLCDCConfig(
    host="localhost",
    port=5433,
    user="sabot",
    password="sabot",
    database="sabot",
    slot_name="sabot_cdc_slot",
    publication_name="sabot_cdc",
)

connector = PostgreSQLCDCConnector(config)

async with connector:
    async for batch in connector.stream_batches():
        # Process batch
        process_events(batch)
```

### With Table Filtering

```python
config = PostgreSQLCDCConfig(
    ...
    tables=["public.orders", "public.users"],
)
```

## PostgreSQL CDC vs MySQL CDC

### Key Differences

| Feature | PostgreSQL | MySQL |
|---------|------------|-------|
| **Protocol** | Logical replication | Binlog replication |
| **Output** | wal2json | ROW format |
| **Position** | LSN (Log Sequence Number) | Log file + position |
| **GTID** | Not required | Optional (recommended) |
| **Setup** | Publication + Slot | Binlog enabled |

### Setup Requirements

**PostgreSQL:**
```sql
-- Enable wal2json
CREATE EXTENSION IF NOT EXISTS wal2json;

-- Create publication
CREATE PUBLICATION sabot_cdc FOR ALL TABLES;

-- Create replication slot
SELECT pg_create_logical_replication_slot('sabot_cdc_slot', 'wal2json');
```

**MySQL:**
```ini
[mysqld]
server-id = 1
log-bin = mysql-bin
binlog-format = ROW
binlog-row-image = FULL
```

## Troubleshooting

### PostgreSQL Not Ready

```bash
# Check status
docker compose ps postgres

# View logs
docker compose logs postgres

# Restart if needed
docker compose restart postgres
```

### Connection Refused

Check if PostgreSQL is listening on port 5433:

```bash
lsof -i :5433
netstat -an | grep 5433
```

### Replication Slot Issues

Check replication slots:

```sql
SELECT * FROM pg_replication_slots;
```

Drop and recreate if stuck:

```sql
SELECT pg_drop_replication_slot('sabot_cdc_slot');
SELECT pg_create_logical_replication_slot('sabot_cdc_slot', 'wal2json');
```

### No Events Received

Check publication:

```sql
SELECT * FROM pg_publication;
SELECT * FROM pg_publication_tables;
```

Insert test data:

```sql
INSERT INTO products (name, price) VALUES ('Test', 99.99);
```

## Performance Tips

1. **Replication Lag**: Monitor with `pg_stat_replication`
   ```sql
   SELECT * FROM pg_stat_replication;
   ```

2. **Slot Cleanup**: Drop unused slots to prevent WAL buildup
   ```sql
   SELECT pg_drop_replication_slot('unused_slot');
   ```

3. **WAL Retention**: Monitor disk usage
   ```sql
   SELECT pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn))
   FROM pg_replication_slots;
   ```

4. **Batch Size**: Adjust for your workload
   ```python
   batch_size=100  # Process 100 events per batch
   ```

## Advanced Features

### Transaction Boundaries

PostgreSQL CDC preserves transaction boundaries:

```python
for batch in connector.stream_batches():
    # All events in batch are from same transaction
    transaction_id = batch.transaction_id
    process_atomically(batch)
```

### Schema Changes

Handle DDL changes:

```python
if event.type == 'schema_change':
    update_schema_cache(event.schema, event.table)
```

### Multi-Table Joins

Use transaction info to correlate changes:

```python
if event.transaction_id == last_tx_id:
    # Same transaction - related changes
    join_events(event, previous_events)
```

## Next Steps

1. Review examples to understand CDC patterns
2. Adapt examples to your use case
3. Monitor replication lag in production
4. Setup alerting for slot issues

## See Also

- [PostgreSQL CDC Documentation](../../docs/POSTGRESQL_CDC.md)
- [MySQL CDC Examples](../mysql_cdc/)
- [wal2json Documentation](https://github.com/eulerto/wal2json)
- [PostgreSQL Logical Replication](https://www.postgresql.org/docs/current/logical-replication.html)
