# PostgreSQL CDC Connector

Sabot's PostgreSQL CDC (Change Data Capture) connector provides real-time streaming of database changes using PostgreSQL's logical replication with wal2json output plugin.

## Overview

The CDC connector streams database changes as they happen, enabling:
- Real-time data pipelines
- Event-driven architectures
- Incremental ETL processes
- Audit logging and compliance
- Cache invalidation
- Cross-system data synchronization

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   PostgreSQL    │ => │  Logical Rep     │ => │   wal2json      │
│   Database      │    │  Replication     │    │   JSON Output   │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│  libpq (C)      │ => │  Python Parser   │ => │  Sabot Stream   │
│  Replication    │    │  wal2json        │    │  Processing     │
│  Connection     │    │  Format v1/v2    │    │  Pipeline       │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

## Requirements

### PostgreSQL Setup

1. **Version**: PostgreSQL 10 or higher
2. **Logical Replication**: Enable in postgresql.conf:
   ```ini
   wal_level = logical
   ```
3. **Restart** PostgreSQL after configuration changes
4. **wal2json Extension**: Install the extension:
   ```sql
   CREATE EXTENSION wal2json;
   ```

### User Permissions

Create a user with replication privileges:
```sql
CREATE USER cdc_user WITH PASSWORD 'cdc_password' REPLICATION;
GRANT CONNECT ON DATABASE your_database TO cdc_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO cdc_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO cdc_user;
```

## Quick Start

### Basic Usage

```python
from sabot import Stream

# Create CDC stream
stream = Stream.from_postgres_cdc(
    host="localhost",
    database="ecommerce",
    user="cdc_user",
    password="cdc_password",
    replication_slot="sabot_cdc"
)

# Process changes
async for batch in stream:
    for event in batch.to_pylist():
        print(f"Change: {event['event_type']} on {event['schema']}.{event['table']}")
        if event['event_type'] == 'insert':
            print(f"New data: {event['data']}")
```

### Advanced Configuration

```python
stream = Stream.from_postgres_cdc(
    host="prod-db.example.com",
    port=5432,
    database="analytics",
    user="cdc_user",
    password="secure_password",
    replication_slot="analytics_cdc",

    # Filter specific tables
    table_filter=["public.users", "public.orders", "public.events"],

    # Control batching
    batch_size=50,

    # wal2json options
    include_xids=True,
    include_timestamps=True,
    include_types=True,
    pretty_print=False,

    # Connection options
    connection_timeout=30,
)
```

## CDC Event Format

Each CDC event is a structured record with:

```python
{
    'event_type': 'insert' | 'update' | 'delete' | 'begin' | 'commit' | 'message',
    'schema': 'public',
    'table': 'users',
    'data': {'id': 123, 'name': 'Alice', 'email': 'alice@example.com'},  # INSERT/UPDATE
    'old_data': {'name': 'Alice'},  # UPDATE (old values)
    'key_data': {'id': 123},        # UPDATE/DELETE (primary key)
    'lsn': '0/15D6B88',            # Log Sequence Number
    'timestamp': '2025-01-15T10:30:45.123456+00:00',
    'transaction_id': 12345,
    'origin': 'pg_123456',         # Replication origin (if any)
}
```

### Event Types

- **`insert`**: New row inserted
  - `data`: Complete row data
- **`update`**: Existing row modified
  - `data`: New row data
  - `old_data`: Previous values (if available)
  - `key_data`: Primary key values
- **`delete`**: Row deleted
  - `key_data`: Primary key of deleted row
- **`begin`**: Transaction started
  - `transaction_id`: Transaction ID
- **`commit`**: Transaction committed
  - `transaction_id`: Transaction ID
- **`message`**: Custom logical replication message
  - `message_prefix`: Message prefix
  - `message_content`: Message content
  - `transactional`: Whether message is transactional

## Configuration Options

### Connection Settings
- `host`: PostgreSQL host (default: "localhost")
- `port`: PostgreSQL port (default: 5432)
- `database`: Database name (default: "postgres")
- `user`: Username
- `password`: Password
- `connection_timeout`: Connection timeout in seconds (default: 30)

### Replication Settings
- `replication_slot`: Logical replication slot name (default: "sabot_cdc_slot")
- `output_plugin`: Output plugin (default: "wal2json")
- `format_version`: wal2json format version (1 or 2, default: 2)

### Filtering Options
- `table_filter`: List of tables to monitor (e.g., `["public.users", "public.*"]`)
- `add_tables`: Alternative table filter (only these tables)
- `filter_origins`: Filter by replication origin
- `add_msg_prefixes`: Include messages with these prefixes
- `filter_msg_prefixes`: Exclude messages with these prefixes

### wal2json Options
- `include_xids`: Include transaction IDs (default: True)
- `include_timestamps`: Include timestamps (default: True)
- `include_lsn`: Include LSNs (default: True)
- `include_schemas`: Include schema names (default: True)
- `include_types`: Include data types (default: True)
- `include_type_oids`: Include type OIDs (default: False)
- `include_typmod`: Include typmod info (default: False)
- `include_domain_data_type`: Include underlying domain types (default: False)
- `include_column_positions`: Include column positions (default: False)
- `include_not_null`: Include NOT NULL constraints (default: False)
- `include_default`: Include default values (default: False)
- `include_pk`: Include primary key info (default: True)
- `pretty_print`: Pretty-print JSON (default: False)
- `write_in_chunks`: Write in chunks (v1 only, default: False)
- `numeric_data_types_as_string`: Return numeric types as strings (default: False)

### Streaming Options
- `batch_size`: Events per batch (default: 100)
- `max_poll_interval`: Max time to wait for data (default: 1.0s)
- `heartbeat_interval`: Send heartbeat every N seconds (default: 10.0s)

## Processing Patterns

### Filter by Event Type

```python
# Only process data changes
data_changes = stream.filter(
    lambda batch: batch.column('event_type').isin(['insert', 'update', 'delete'])
)
```

### Filter by Table

```python
# Only user-related changes
user_changes = stream.filter(
    lambda batch: batch.column('table') == 'users'
)
```

### Real-time Aggregation

```python
# Count events by type
event_counts = (
    stream
    .group_by('event_type')
    .aggregate(count=('event_type', 'count'))
)
```

### Write to Multiple Sinks

```python
# Process and write to different destinations
(
    stream
    .filter(lambda b: b.column('table') == 'orders')
    .to_kafka('order-events')
    .filter(lambda b: b.column('event_type') == 'insert')
    .to_parquet('/data/cdc-archive/', partition_by='table')
)
```

## Replication Slot Management

### Automatic Slot Creation

The connector automatically creates replication slots if they don't exist:

```python
# Slot created automatically
stream = Stream.from_postgres_cdc(replication_slot="my_slot")
```

### Manual Slot Management

For production use, you may want to create slots manually:

```sql
-- Create slot
SELECT * FROM pg_create_logical_replication_slot('my_slot', 'wal2json');

-- Monitor slot lag
SELECT slot_name, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn))
FROM pg_replication_slots WHERE slot_name = 'my_slot';

-- Drop slot when done
SELECT pg_drop_replication_slot('my_slot');
```

## Performance Considerations

### Throughput

- **Typical throughput**: 10,000 - 100,000 events/second
- **Factors**:
  - Network latency between PostgreSQL and application
  - Event batching (`batch_size`)
  - wal2json format version (v2 recommended)
  - Table filtering (reduces data transfer)

### Memory Usage

- **Base memory**: ~50MB per connector
- **Per-event overhead**: ~1KB
- **Batch size impact**: Larger batches reduce GC pressure

### Replication Lag

Monitor replication lag:

```sql
SELECT slot_name,
       pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) as lag_bytes
FROM pg_replication_slots;
```

## Troubleshooting

### Common Issues

1. **"wal2json extension not available"**
   - Install wal2json extension
   - Check PostgreSQL version compatibility

2. **"Replication slot doesn't exist"**
   - Check user has replication privileges
   - Verify logical replication is enabled

3. **"Connection refused"**
   - Check PostgreSQL is running
   - Verify connection parameters
   - Check firewall/network settings

4. **High replication lag**
   - Increase `batch_size`
   - Reduce `max_poll_interval`
   - Check network bandwidth
   - Monitor PostgreSQL WAL disk usage

### Debugging

Enable debug logging:

```python
import logging
logging.getLogger('sabot._cython.connectors.postgresql').setLevel(logging.DEBUG)
```

## Examples

See `examples/postgres_cdc_demo.py` for a complete working example.

## Limitations

- PostgreSQL 10+ required
- Logical replication must be enabled
- Only supports tables with primary keys (for UPDATE/DELETE events)
- Large transactions may impact memory usage
- DDL changes are not captured (only DML)

## Future Enhancements

- Support for multiple database schemas
- Custom message formats
- Parallel replication streams
- Automatic failover handling
- Schema evolution support






