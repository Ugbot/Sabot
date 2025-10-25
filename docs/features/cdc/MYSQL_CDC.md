# MySQL CDC Connector

Sabot's MySQL CDC (Change Data Capture) connector provides real-time streaming of database changes using MySQL's binlog replication.

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
│   MySQL         │ => │  Binlog          │ => │   python-mysql- │
│   Database      │    │  Replication     │    │   replication   │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│  MySQL Binlog   │ => │  Event Parser    │ => │  Sabot Stream   │
│  (ROW format)   │    │  INSERT/UPDATE   │    │  Arrow Batches  │
│  GTID support   │    │  DELETE/DDL      │    │  Processing     │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

## Requirements

### MySQL Setup

1. **Version**: MySQL 5.5+ or MariaDB 10.6+ (MySQL 8.0+ recommended)
2. **Binary Logging**: Enable in my.cnf:
   ```ini
   [mysqld]
   server-id = 1
   log-bin = mysql-bin
   binlog-format = ROW
   binlog-row-image = FULL
   binlog-row-metadata = FULL  # MySQL 8.0.14+
   ```
3. **GTID** (recommended for failover):
   ```ini
   gtid-mode = ON
   enforce-gtid-consistency = ON
   ```
4. **Restart** MySQL after configuration changes

### User Permissions

Create a user with replication privileges:

```sql
CREATE USER 'cdc_user'@'%' IDENTIFIED BY 'cdc_password';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_user'@'%';
GRANT SELECT ON *.* TO 'cdc_user'@'%';
FLUSH PRIVILEGES;
```

## Quick Start

### Basic Usage

```python
from sabot.connectors.mysql_cdc import MySQLCDCConfig, MySQLCDCConnector

# Create CDC connector
config = MySQLCDCConfig(
    host="localhost",
    port=3306,
    user="cdc_user",
    password="cdc_password",
    database="ecommerce",  # Monitor specific database
)

connector = MySQLCDCConnector(config)

# Stream changes
async with connector:
    async for batch in connector.stream_batches():
        for i in range(batch.num_rows):
            event = {
                'type': batch.column('event_type')[i].as_py(),
                'table': batch.column('table')[i].as_py(),
                'data': batch.column('data')[i].as_py(),
            }
            print(f"Change: {event['type']} on {event['table']}")
```

### Advanced Configuration

```python
config = MySQLCDCConfig(
    host="prod-mysql.example.com",
    port=3306,
    user="cdc_user",
    password="secure_password",
    database="production",

    # GTID-based resume (recommended)
    auto_position=True,
    gtid_set="00000000-0000-0000-0000-000000000000:1-1000",

    # Filter specific tables
    only_tables=["production.users", "production.orders"],

    # Or use file+position resume
    # log_file="mysql-bin.000042",
    # log_pos=1234567,

    # Control batching
    batch_size=100,
    max_poll_interval=2.0,

    # Unique replication client ID
    server_id=1000001,
)
```

## CDC Event Format

Each CDC event is a row in an Arrow RecordBatch with schema:

| Column      | Type           | Description                        |
|-------------|----------------|-------------------------------------|
| event_type  | string         | 'insert', 'update', 'delete', 'ddl' |
| schema      | string         | Database name                       |
| table       | string         | Table name                          |
| timestamp   | timestamp[us]  | Event timestamp                     |
| data        | string (JSON)  | New/changed row data                |
| old_data    | string (JSON)  | Old row data (UPDATE only)          |
| log_file    | string         | Binlog file name                    |
| log_pos     | int64          | Binlog position                     |
| gtid        | string         | GTID (if enabled)                   |
| query       | string         | DDL query (if DDL event)            |

### Event Types

- **`insert`**: New row inserted
  - `data`: Complete row data
- **`update`**: Existing row modified
  - `data`: New row data
  - `old_data`: Previous row data
- **`delete`**: Row deleted
  - `data`: Deleted row data
- **`ddl`**: Schema change (CREATE, ALTER, DROP)
  - `query`: DDL statement
- **`gtid`**: GTID marker
  - `gtid`: GTID value

### Example Event Data

```json
{
  "event_type": "insert",
  "schema": "ecommerce",
  "table": "orders",
  "timestamp": "2025-10-09T12:34:56.789012",
  "data": "{\"id\": 123, \"customer_id\": 456, \"amount\": 99.99}",
  "old_data": null,
  "log_file": "mysql-bin.000042",
  "log_pos": 1234567,
  "gtid": "3E11FA47-71CA-11E1-9E33-C80AA9429562:23",
  "query": null
}
```

## Configuration Options

### Connection Settings
- `host`: MySQL host (default: "localhost")
- `port`: MySQL port (default: 3306)
- `user`: Username
- `password`: Password
- `database`: Database name (None = monitor all databases)
- `connection_timeout`: Connection timeout in seconds (default: 30)

### Replication Settings
- `server_id`: Unique replication client ID (default: 1000001)
- `blocking`: Block on read (default: False for streaming)
- `resume_stream`: Resume from last position (default: True)

### Position/GTID Settings
- `log_file`: Binlog file to start from (e.g., "mysql-bin.000042")
- `log_pos`: Binlog position to start from
- `auto_position`: Use GTID auto-positioning (default: False)
- `gtid_set`: GTID set to resume from (e.g., "uuid:1-100")

### Filtering Options
- `only_tables`: List of tables to monitor (e.g., `["db.users", "db.orders"]`)
- `ignored_tables`: List of tables to ignore
- `only_schemas`: List of schemas to monitor
- `ignored_schemas`: List of schemas to ignore
- `only_events`: List of event types to capture
- `ignored_events`: List of event types to ignore

### Streaming Settings
- `batch_size`: Max events per batch (default: 100)
- `max_poll_interval`: Max time to wait for new data (default: 1.0s)
- `heartbeat_interval`: Heartbeat frequency (default: 10.0s)

### Error Handling
- `max_retry_attempts`: Max retry attempts on error (default: 3)
- `retry_backoff_seconds`: Backoff time between retries (default: 1.0s)

## Processing Patterns

### Filter by Event Type

```python
# Only process data changes (ignore DDL)
async for batch in connector.stream_batches():
    mask = batch.column('event_type').isin(['insert', 'update', 'delete'])
    filtered = batch.filter(mask)
    process(filtered)
```

### Filter by Table

```python
# Only process specific tables
async for batch in connector.stream_batches():
    mask = batch.column('table').isin(['users', 'orders'])
    filtered = batch.filter(mask)
    process(filtered)
```

### Checkpoint Position

```python
# Save binlog position for recovery
connector = MySQLCDCConnector(config)

async with connector:
    async for batch in connector.stream_batches():
        process(batch)

        # Get current position for checkpointing
        position = connector.get_position()
        save_checkpoint(position)  # Save to disk/DB

# Resume from saved position
saved_position = load_checkpoint()
config = MySQLCDCConfig(
    ...,
    log_file=saved_position['log_file'],
    log_pos=saved_position['log_pos'],
)
```

### GTID-based Resume

```python
# Use GTID for multi-master or failover scenarios
config = MySQLCDCConfig(
    host="mysql-master.example.com",
    auto_position=True,  # Enable GTID auto-positioning
    gtid_set="3E11FA47-71CA-11E1-9E33-C80AA9429562:1-1000",
)
```

## Performance Considerations

### Throughput

- **Typical throughput**: 10,000 - 100,000 events/second (Python)
- **Future (Cython)**: 50,000 - 1,000,000 events/second
- **Factors**:
  - Network latency between MySQL and application
  - Event batching (`batch_size`)
  - Table filtering (reduces data transfer)
  - Number of columns per table

### Memory Usage

- **Base memory**: ~30MB per connector
- **Per-event overhead**: ~0.5-1KB
- **Batch size impact**: Larger batches reduce GC pressure

### Binlog Retention

Monitor binlog disk usage:

```sql
SHOW BINARY LOGS;
SHOW VARIABLES LIKE 'binlog_expire_logs_seconds';
```

Set retention policy:

```sql
SET GLOBAL binlog_expire_logs_seconds = 604800;  -- 7 days
```

## Docker Setup

### Using docker-compose.yml

Sabot includes a MySQL service with binlog pre-configured:

```bash
# Start MySQL
docker compose up mysql -d

# Wait for health check
docker compose ps mysql

# Connect and create test table
docker compose exec mysql mysql -u root -psabot -e "
USE sabot;
CREATE TABLE orders (
    id INT PRIMARY KEY AUTO_INCREMENT,
    customer_id INT,
    amount DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"

# Insert test data
docker compose exec mysql mysql -u root -psabot sabot -e "
INSERT INTO orders (customer_id, amount) VALUES (1, 99.99), (2, 149.99);
"
```

### MySQL Configuration

The docker-compose MySQL service is pre-configured with:
- `binlog-format=ROW`
- `binlog-row-image=FULL`
- `binlog-row-metadata=FULL`
- `gtid-mode=ON`
- Port 3307 (to avoid conflicts with local MySQL)

## Troubleshooting

### Common Issues

1. **"ERROR 1236: Could not find first log file name in binary log index file"**
   - Binlog has rotated/expired since last position
   - Solution: Start from current position or use GTID

2. **"ERROR 1227: Access denied; you need REPLICATION SLAVE privilege"**
   - User lacks replication privileges
   - Solution: `GRANT REPLICATION SLAVE ON *.* TO 'user'@'%'`

3. **"No binlog events received"**
   - Binlog not enabled or wrong format
   - Check: `SHOW VARIABLES LIKE 'log_bin'` (should be ON)
   - Check: `SHOW VARIABLES LIKE 'binlog_format'` (should be ROW)

4. **High memory usage**
   - Large transactions or batches
   - Solution: Reduce `batch_size` or increase `max_poll_interval`

5. **Connection timeouts**
   - Network issues or MySQL overload
   - Solution: Increase `connection_timeout`

### Debugging

Enable debug logging:

```python
import logging
logging.getLogger('sabot.connectors.mysql_cdc').setLevel(logging.DEBUG)
logging.getLogger('pymysqlreplication').setLevel(logging.DEBUG)
```

Check binlog status:

```sql
SHOW MASTER STATUS;
SHOW BINARY LOGS;
```

## Examples

See `examples/mysql_cdc_demo.py` for a complete working example.

## Limitations

- MySQL 5.5+ required
- Binary logging must be enabled with ROW format
- Large transactions (>1GB) may impact memory usage
- DDL changes are captured but schema evolution requires manual handling
- Only supports tables with primary keys for UPDATE/DELETE events

## Migration Path

**Phase 1 (Current):** Python implementation using `python-mysql-replication`
- ✅ Functional CDC with Arrow output
- ✅ GTID support
- ✅ Table filtering
- Throughput: 10K-100K events/sec

**Phase 2 (Future):** C++/Cython binlog parser
- Zero-copy binlog parsing
- Direct Arrow RecordBatch construction
- Throughput: 50K-1M events/sec
- 10-100x performance improvement

**Phase 3 (Production):** Full integration
- Sabot checkpoint integration
- Schema registry
- Multi-table parallel streams
- Automatic failover

## Advanced Features

### Auto-Configuration

Auto-configure CDC with validation and table discovery:

```python
from sabot.connectors.mysql_cdc import MySQLCDCConnector

# Auto-configure with wildcard patterns
connector = MySQLCDCConnector.auto_configure(
    host="localhost",
    port=3306,
    user="root",
    password="admin_pass",
    database="ecommerce",
    table_patterns=["ecommerce.*", "analytics.events_*"],
    validate_config=True,  # Validate MySQL configuration
    create_user=True,      # Create dedicated CDC user
    cdc_user="cdc_user",
    cdc_password="cdc_pass"
)

# Connector is ready to use
async with connector:
    async for batch in connector.stream_batches():
        process_batch(batch)
```

**Features:**
- Configuration validation with fix instructions
- Wildcard table pattern matching
- Automatic table discovery
- Optional CDC user creation
- Binlog position tracking

### Wildcard Table Selection

Use Flink CDC-compatible patterns to select tables:

```python
# Single table
connector = MySQLCDCConnector.auto_configure(
    ...,
    table_patterns="ecommerce.orders"
)

# All tables in database
connector = MySQLCDCConnector.auto_configure(
    ...,
    table_patterns="ecommerce.*"
)

# Wildcard prefix/suffix
connector = MySQLCDCConnector.auto_configure(
    ...,
    table_patterns=["ecommerce.user_*", "ecommerce.*_log"]
)

# Regex patterns
connector = MySQLCDCConnector.auto_configure(
    ...,
    table_patterns="ecommerce.partition_[0-9]+"
)

# Multiple patterns
connector = MySQLCDCConnector.auto_configure(
    ...,
    table_patterns=[
        "ecommerce.*",
        "analytics.events_*",
        "audit.*_log"
    ]
)
```

**Pattern Syntax:**
- `*` - Match any characters
- `?` - Match single character
- `[0-9]+` - Regex patterns supported
- Case-sensitive by default (configurable)

### DDL Change Tracking

Track schema evolution automatically:

```python
config = MySQLCDCConfig(
    ...,
    track_ddl=True,        # Enable DDL tracking
    emit_ddl_events=True   # Include DDL in stream
)

connector = MySQLCDCConnector(config)
schema_tracker = connector.get_schema_tracker()

async with connector:
    async for batch in connector.stream_batches():
        batch_dict = batch.to_pydict()

        for i in range(batch.num_rows):
            if batch_dict['event_type'][i] == 'ddl':
                # DDL change detected
                query = batch_dict['query'][i]
                print(f"Schema change: {query}")

                # Check schema tracker
                tracked = schema_tracker.get_tracked_tables()
                print(f"Tracking {len(tracked)} tables")
```

**Tracked DDL Operations:**
- CREATE TABLE - Start tracking new tables
- DROP TABLE - Remove from tracking
- ALTER TABLE - Invalidate cached schema
- RENAME TABLE - Update tracking
- TRUNCATE TABLE - Detect data clearing
- CREATE/DROP DATABASE - Database lifecycle

**Schema Cache:**
```python
# Get cached schema
schema = schema_tracker.get_schema("ecommerce", "users")
print(f"Columns: {schema.columns}")
print(f"Primary key: {schema.primary_key}")
print(f"Version: {schema.schema_version}")

# Check if table is tracked
is_tracked = schema_tracker.is_tracked("ecommerce", "users")

# Manually invalidate schema
schema_tracker.invalidate_table("ecommerce", "users")
```

### Per-Table Stream Routing

Route events from different tables to separate streams:

```python
from sabot.connectors.mysql_stream_router import MySQLStreamRouter

# Create router
router = MySQLStreamRouter()
router.add_route("ecommerce", "orders", output_key="orders")
router.add_route("ecommerce", "users", output_key="users")
router.add_route("ecommerce", "events_click", output_key="events")

# Process CDC stream
async with connector:
    async for batch in connector.stream_batches():
        # Route batch to separate outputs
        routed_batches = router.route_batch(batch)

        # Process each route separately
        if "orders" in routed_batches:
            await process_orders(routed_batches["orders"])

        if "users" in routed_batches:
            await process_users(routed_batches["users"])

        if "events" in routed_batches:
            await process_events(routed_batches["events"])
```

**Advanced Routing:**
```python
# Route with predicate filter
router.add_route(
    "ecommerce",
    "orders",
    output_key="high_value_orders",
    predicate=lambda row: float(json.loads(row['data'])['amount']) > 1000
)

# Multiple tables to same output
router.add_route("ecommerce", "users", output_key="user_data")
router.add_route("ecommerce", "user_profiles", output_key="user_data")

# Get routing statistics
routes = router.get_routes()
outputs = router.get_output_keys()
tables = router.get_tables_for_output("user_data")
```

**Use Cases:**
- Per-table processing pipelines
- Different transformations per table
- Table-specific error handling
- Distributed routing with shuffle
- Priority-based processing

### Table Discovery

Discover tables programmatically:

```python
from sabot.connectors.mysql_discovery import MySQLTableDiscovery

discovery = MySQLTableDiscovery(
    host="localhost",
    port=3306,
    user="root",
    password="password"
)

# Discover all tables in database
tables = discovery.discover_tables(database="ecommerce")
print(f"Found {len(tables)} tables: {tables}")

# Get table schema
schema = discovery.get_table_schema("ecommerce", "users")
print(f"Columns: {schema['columns']}")
print(f"Primary key: {schema['primary_key']}")
print(f"Engine: {schema['engine']}")

# Expand wildcard patterns
matched = discovery.expand_patterns(
    ["ecommerce.*", "analytics.events_*"],
    database=None  # Search all databases
)

# Get table statistics
stats = discovery.get_table_stats("ecommerce", "users")
print(f"Rows: {stats['row_count']}")
print(f"Size: {stats['total_size']} bytes")
```

### Configuration Validation

Validate MySQL configuration before starting CDC:

```python
from sabot.connectors.mysql_auto_config import MySQLAutoConfigurator

configurator = MySQLAutoConfigurator(
    host="localhost",
    port=3306,
    admin_user="root",
    admin_password="password"
)

# Validate configuration
issues = configurator.validate_configuration(require_gtid=False)

if issues:
    configurator.print_fix_instructions(issues)

    # Check for errors
    from sabot.connectors.mysql_auto_config import Severity
    errors = [i for i in issues if i.severity == Severity.ERROR]

    if errors:
        print("❌ MySQL has configuration errors")
        exit(1)
else:
    print("✅ MySQL is properly configured for CDC")

# Get current binlog position
position = configurator.get_current_position()
print(f"Position: {position['log_file']}:{position['log_pos']}")
if 'gtid_executed' in position:
    print(f"GTID: {position['gtid_executed']}")

# Create replication user
configurator.create_replication_user(
    user="cdc_user",
    password="cdc_pass",
    host="%"
)
```

**Validation Checks:**
- `log_bin` = ON (required)
- `binlog_format` = ROW (required)
- `binlog_row_image` = FULL (recommended)
- `binlog_row_metadata` = FULL (recommended, MySQL 8.0.14+)
- `server_id` > 0 (required)
- `gtid_mode` = ON (if required)
- `binlog_expire_logs_seconds` (retention warning)

## Examples

See the `examples/` directory for complete demos:

- `mysql_cdc_demo.py` - Basic CDC streaming
- `mysql_cdc_auto_demo.py` - Auto-configuration with wildcards
- `mysql_cdc_ddl_demo.py` - DDL change tracking
- `mysql_cdc_routing_demo.py` - Per-table stream routing

## See Also

- [PostgreSQL CDC](POSTGRESQL_CDC.md) - Similar CDC for PostgreSQL
- [MySQL CDC Feature Parity](MYSQL_CDC_FEATURE_PARITY.md) - Implementation details
- [Connectors](../sabot/connectors/) - Other data connectors
- [Examples](../examples/) - More CDC examples
