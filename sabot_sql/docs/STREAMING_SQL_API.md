# Streaming SQL API Documentation

## Overview

SabotSQL provides a comprehensive streaming SQL API for real-time data processing with Flink-style semantics. The API supports stateful operations, checkpointing, watermarks, and distributed execution across multiple agents.

## Architecture

### Core Components

- **StreamingSQLExecutor**: Main Python API for streaming SQL execution
- **SourceConnector**: Generic C++ interface for streaming data sources
- **WindowOperator**: Stateful window aggregation with MarbleDB backend
- **WatermarkTracker**: Event-time processing and watermark management
- **CheckpointCoordinator**: Exactly-once processing with barrier injection
- **ObservabilityManager**: Comprehensive monitoring and metrics collection

### State Management

All state is managed through **MarbleDB** with RAFT replication:

- **Dimension Tables**: `is_raft_replicated=true` (broadcast to all agents)
- **Connector Offsets**: `is_raft_replicated=true` (fault tolerance)
- **Streaming State**: `is_raft_replicated=false` (local per agent)
- **Timers/Watermarks**: MarbleDB Timer API

## Python API

### StreamingSQLExecutor

The main entry point for streaming SQL execution.

```python
from sabot_sql import StreamingSQLExecutor

# Initialize executor
executor = StreamingSQLExecutor(
    state_backend='marbledb',      # State storage backend
    timer_backend='marbledb',      # Timer/watermark backend
    state_path='./streaming_state', # State storage path
    checkpoint_interval_seconds=60, # Checkpoint frequency
    max_parallelism=16             # Max Kafka partitions
)

# Register dimension tables
executor.register_dimension_table(
    'securities',
    securities_table,
    is_raft_replicated=True
)

# Execute DDL for streaming sources
executor.execute_ddl("""
    CREATE TABLE trades (
        symbol VARCHAR,
        price DOUBLE,
        volume BIGINT,
        ts TIMESTAMP
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'trades',
        'bootstrap.servers' = 'localhost:9092',
        'group.id' = 'sabot_streaming'
    )
""")

# Execute streaming SQL
async for batch in executor.execute_streaming_sql("""
    SELECT 
        symbol,
        TUMBLE(ts, INTERVAL '1' HOUR) as window_start,
        COUNT(*) as count,
        AVG(price) as avg_price,
        SUM(volume) as total_volume
    FROM trades
    GROUP BY symbol, TUMBLE(ts, INTERVAL '1' HOUR)
"""):
    print(f"Window result: {batch}")
```

### Configuration Options

```python
executor = StreamingSQLExecutor(
    # State management
    state_backend='marbledb',           # 'marbledb' | 'tonbo'
    timer_backend='marbledb',           # 'marbledb' | 'rocksdb'
    state_path='./streaming_state',     # State storage path
    
    # Execution configuration
    checkpoint_interval_seconds=60,     # Checkpoint frequency
    max_parallelism=16,                # Max Kafka partitions
    watermark_idle_timeout_ms=30000,   # Watermark idle timeout
    
    # Error handling
    enable_error_recovery=True,         # Enable automatic error recovery
    max_retries=3,                      # Max retry attempts
    retry_delay_ms=1000,                # Base retry delay
    
    # Monitoring
    enable_metrics=True,                # Enable metrics collection
    metrics_export_interval_ms=5000,   # Metrics export frequency
    enable_health_checks=True,          # Enable health monitoring
)
```

## C++ API

### SourceConnector Interface

Generic interface for streaming data sources.

```cpp
#include "sabot_sql/streaming/source_connector.h"

class CustomConnector : public sabot_sql::streaming::SourceConnector {
public:
    arrow::Status Init(const SourceConfig& config) override {
        // Initialize connector
        config_ = config;
        return arrow::Status::OK();
    }
    
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> FetchNextBatch() override {
        // Fetch next batch of data
        // Return nullptr when no more data
        return nullptr;
    }
    
    arrow::Status CommitOffset(int64_t offset) override {
        // Commit offset for exactly-once processing
        return arrow::Status::OK();
    }
    
    arrow::Result<int64_t> GetCurrentOffset() override {
        // Get current offset
        return 0;
    }
    
    arrow::Result<int64_t> GetWatermark() override {
        // Get current watermark
        return 0;
    }
    
    std::string GetName() const override { return "custom"; }
    std::string GetTopic() const override { return config_.topic; }
    int32_t GetPartition() const override { return config_.partition; }
    
private:
    SourceConfig config_;
};
```

### WindowOperator

Stateful window aggregation operator.

```cpp
#include "sabot_sql/streaming/window_operator.h"

// Create window operator
auto window_op = std::make_shared<sabot_sql::streaming::WindowOperator>();

// Initialize with configuration
WindowConfig config;
config.window_type = "TUMBLE";
config.window_size_ms = 3600000;  // 1 hour
config.window_slide_ms = 3600000; // 1 hour
config.key_columns = {"symbol"};
config.timestamp_column = "ts";
config.aggregate_functions = {"COUNT", "AVG", "SUM"};

auto status = window_op->Initialize(config);
if (!status.ok()) {
    // Handle error
}

// Process data
auto result = window_op->ProcessBatch(input_batch);
if (result.ok()) {
    auto output_batch = result.ValueOrDie();
    // Process output batch
}
```

### WatermarkTracker

Event-time processing and watermark management.

```cpp
#include "sabot_sql/streaming/watermark_tracker.h"

// Create watermark tracker
auto tracker = std::make_shared<sabot_sql::streaming::WatermarkTracker>();

// Initialize
auto status = tracker->Initialize("ts", 30000);  // 30s idle timeout
if (!status.ok()) {
    // Handle error
}

// Update watermark
auto watermark = tracker->UpdateWatermark(batch);
if (watermark.ok()) {
    int64_t current_watermark = watermark.ValueOrDie();
    // Use watermark for window triggering
}

// Get current watermark
auto current = tracker->GetCurrentWatermark();
if (current.ok()) {
    int64_t watermark_value = current.ValueOrDie();
    // Check if windows should be triggered
}
```

### CheckpointCoordinator

Exactly-once processing with checkpoint coordination.

```cpp
#include "sabot_sql/streaming/checkpoint_coordinator.h"

// Create checkpoint coordinator
auto coordinator = std::make_shared<sabot_sql::streaming::CheckpointCoordinator>();

// Initialize
auto status = coordinator->Initialize("coordinator_1", 60000, nullptr);
if (!status.ok()) {
    // Handle error
}

// Register participant
auto participant = std::make_shared<MyCheckpointParticipant>();
coordinator->RegisterParticipant("operator_1", participant);

// Trigger checkpoint
auto checkpoint_id = coordinator->TriggerCheckpoint();
if (checkpoint_id.ok()) {
    int64_t id = checkpoint_id.ValueOrDie();
    // Wait for checkpoint completion
    auto completion = coordinator->WaitForCheckpointCompletion(id);
    if (completion.ok()) {
        // Checkpoint completed successfully
    }
}
```

## Error Handling

### Error Types

- **CONNECTOR_ERROR**: Kafka connection, partition assignment
- **STATE_ERROR**: MarbleDB state operations
- **WATERMARK_ERROR**: Watermark processing, late data
- **CHECKPOINT_ERROR**: Checkpoint coordination, recovery
- **OPERATOR_ERROR**: Window aggregation, join operations
- **ORCHESTRATOR_ERROR**: Sabot orchestrator communication
- **NETWORK_ERROR**: Network connectivity issues
- **TIMEOUT_ERROR**: Operation timeouts
- **VALIDATION_ERROR**: Data validation failures

### Error Recovery Strategies

- **RETRY**: Retry operation with exponential backoff
- **SKIP**: Skip current batch/record
- **FAIL_FAST**: Fail immediately
- **DEGRADE**: Degrade functionality
- **RESTART_COMPONENT**: Restart component
- **RESTART_EXECUTION**: Restart entire execution
- **MANUAL_INTERVENTION**: Require manual intervention

### Error Handler Usage

```cpp
#include "sabot_sql/streaming/error_handler.h"

// Create error handler
auto error_handler = std::make_shared<sabot_sql::streaming::DefaultErrorHandler>();

// Configure retry parameters
error_handler->SetRetryConfig(3, 1000, 30000);  // 3 retries, 1s base delay, 30s max

// Configure error thresholds
error_handler->SetErrorThresholds(10, 100, 1000);  // warning, error, critical thresholds

// Register error callback
error_handler->RegisterErrorCallback(
    sabot_sql::streaming::ErrorType::CONNECTOR_ERROR,
    [](const sabot_sql::streaming::ErrorContext& context) -> arrow::Status {
        // Custom error handling logic
        return arrow::Status::OK();
    }
);

// Handle error
sabot_sql::streaming::ErrorContext context;
context.component = "kafka_connector";
context.operation = "fetch_batch";
context.execution_id = "exec_123";

auto status = error_handler->HandleError(
    sabot_sql::streaming::ErrorType::CONNECTOR_ERROR,
    sabot_sql::streaming::ErrorSeverity::ERROR,
    "Failed to fetch batch from Kafka",
    context,
    sabot_sql::streaming::RecoveryStrategy::RETRY
);
```

## Monitoring and Observability

### Metrics Collection

```cpp
#include "sabot_sql/streaming/monitoring.h"

// Create observability manager
auto observability = std::make_shared<sabot_sql::streaming::ObservabilityManager>();

// Initialize
observability->Initialize("execution_123");

// Get metrics collector
auto metrics = observability->GetMetricsCollector();

// Record metrics
metrics->RecordCounter("batches_processed", 1);
metrics->RecordGauge("memory_usage_mb", 512.5);
metrics->RecordTimer("batch_latency", 150);
metrics->RecordHistogram("batch_size", 1000.0);

// Get all metrics
auto all_metrics = metrics->GetAllMetrics();
for (const auto& [name, value] : all_metrics) {
    std::cout << name << ": " << value << std::endl;
}

// Export metrics to JSON
std::string json_metrics = observability->ExportMetricsToJson();
std::cout << json_metrics << std::endl;
```

### Health Monitoring

```cpp
// Get health checker
auto health_checker = observability->GetHealthChecker();

// Register health check components
health_checker->RegisterComponent("kafka_connector", []() -> arrow::Status {
    // Check Kafka connectivity
    return arrow::Status::OK();
});

health_checker->RegisterComponent("marbledb_state", []() -> arrow::Status {
    // Check MarbleDB state
    return arrow::Status::OK();
});

// Perform health check
auto health_status = health_checker->CheckHealth();
if (health_status.ok()) {
    std::cout << "System is healthy" << std::endl;
} else {
    std::cout << "System is unhealthy: " << health_status.message() << std::endl;
}

// Get health details
auto health_details = health_checker->GetHealthDetails();
for (const auto& [component, status] : health_details) {
    std::cout << component << ": " << status << std::endl;
}
```

### Performance Profiling

```cpp
#include "sabot_sql/streaming/monitoring.h"

// Create performance profiler
auto profiler = std::make_shared<sabot_sql::streaming::PerformanceProfiler>();

// Profile operations
profiler->StartProfile("batch_processing");
// ... perform operation ...
profiler->EndProfile("batch_processing");

profiler->StartProfile("window_aggregation");
// ... perform operation ...
profiler->EndProfile("window_aggregation");

// Get profiling results
auto results = profiler->GetResults();
for (const auto& [name, value] : results) {
    std::cout << name << ": " << value << std::endl;
}

// Get operation statistics
auto stats = profiler->GetOperationStats();
for (const auto& [name, stat] : stats) {
    std::cout << name << " - Calls: " << stat.call_count
              << ", Avg: " << stat.avg_time_ms << "ms"
              << ", Min: " << stat.min_time_ms << "ms"
              << ", Max: " << stat.max_time_ms << "ms" << std::endl;
}
```

## Examples

### Basic Streaming Query

```python
import asyncio
from sabot_sql import StreamingSQLExecutor

async def basic_streaming_example():
    # Initialize executor
    executor = StreamingSQLExecutor(
        state_backend='marbledb',
        checkpoint_interval_seconds=60,
        max_parallelism=8
    )
    
    # Create streaming source
    executor.execute_ddl("""
        CREATE TABLE sensor_data (
            sensor_id VARCHAR,
            temperature DOUBLE,
            humidity DOUBLE,
            ts TIMESTAMP
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'sensors',
            'bootstrap.servers' = 'localhost:9092'
        )
    """)
    
    # Execute streaming query
    async for batch in executor.execute_streaming_sql("""
        SELECT 
            sensor_id,
            TUMBLE(ts, INTERVAL '5' MINUTE) as window_start,
            AVG(temperature) as avg_temp,
            AVG(humidity) as avg_humidity,
            COUNT(*) as reading_count
        FROM sensor_data
        GROUP BY sensor_id, TUMBLE(ts, INTERVAL '5' MINUTE)
    """):
        print(f"Sensor window: {batch}")
        
        # Process batch
        for i in range(batch.num_rows):
            sensor_id = batch.column('sensor_id')[i]
            avg_temp = batch.column('avg_temp')[i]
            avg_humidity = batch.column('avg_humidity')[i]
            count = batch.column('reading_count')[i]
            
            print(f"  {sensor_id}: {avg_temp:.2f}°C, {avg_humidity:.2f}%, {count} readings")

# Run example
asyncio.run(basic_streaming_example())
```

### Dimension Table Join

```python
import asyncio
from sabot_sql import StreamingSQLExecutor
import pyarrow as pa

async def dimension_join_example():
    # Initialize executor
    executor = StreamingSQLExecutor(
        state_backend='marbledb',
        checkpoint_interval_seconds=60
    )
    
    # Create dimension table
    securities_schema = pa.schema([
        ('symbol', pa.string()),
        ('company_name', pa.string()),
        ('sector', pa.string()),
        ('market_cap', pa.float64())
    ])
    
    securities_data = [
        ['AAPL', 'Apple Inc.', 'Technology', 3000000000000.0],
        ['MSFT', 'Microsoft Corp.', 'Technology', 2800000000000.0],
        ['GOOGL', 'Alphabet Inc.', 'Technology', 1800000000000.0]
    ]
    
    securities_table = pa.table(securities_data, schema=securities_schema)
    
    # Register dimension table (RAFT replicated)
    executor.register_dimension_table(
        'securities',
        securities_table,
        is_raft_replicated=True
    )
    
    # Create streaming source
    executor.execute_ddl("""
        CREATE TABLE trades (
            symbol VARCHAR,
            price DOUBLE,
            volume BIGINT,
            ts TIMESTAMP
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'trades',
            'bootstrap.servers' = 'localhost:9092'
        )
    """)
    
    # Execute streaming query with dimension join
    async for batch in executor.execute_streaming_sql("""
        SELECT 
            t.symbol,
            s.company_name,
            s.sector,
            TUMBLE(t.ts, INTERVAL '1' HOUR) as window_start,
            AVG(t.price) as avg_price,
            SUM(t.volume) as total_volume,
            COUNT(*) as trade_count
        FROM trades t
        LEFT JOIN securities s ON t.symbol = s.symbol
        GROUP BY t.symbol, s.company_name, s.sector, TUMBLE(t.ts, INTERVAL '1' HOUR)
    """):
        print(f"Trading window: {batch}")
        
        # Process batch
        for i in range(batch.num_rows):
            symbol = batch.column('symbol')[i]
            company = batch.column('company_name')[i]
            sector = batch.column('sector')[i]
            avg_price = batch.column('avg_price')[i]
            total_volume = batch.column('total_volume')[i]
            trade_count = batch.column('trade_count')[i]
            
            print(f"  {symbol} ({company}, {sector}): "
                  f"${avg_price:.2f}, {total_volume:,} shares, {trade_count} trades")

# Run example
asyncio.run(dimension_join_example())
```

### Error Handling Example

```python
import asyncio
from sabot_sql import StreamingSQLExecutor

async def error_handling_example():
    # Initialize executor with error handling
    executor = StreamingSQLExecutor(
        state_backend='marbledb',
        checkpoint_interval_seconds=60,
        enable_error_recovery=True,
        max_retries=3,
        retry_delay_ms=1000
    )
    
    try:
        # Create streaming source
        executor.execute_ddl("""
            CREATE TABLE error_test (
                id BIGINT,
                value DOUBLE,
                ts TIMESTAMP
            ) WITH (
                'connector' = 'kafka',
                'topic' = 'error_test',
                'bootstrap.servers' = 'localhost:9092'
            )
        """)
        
        # Execute streaming query with error handling
        async for batch in executor.execute_streaming_sql("""
            SELECT 
                id,
                TUMBLE(ts, INTERVAL '1' MINUTE) as window_start,
                AVG(value) as avg_value,
                COUNT(*) as count
            FROM error_test
            GROUP BY id, TUMBLE(ts, INTERVAL '1' MINUTE)
        """):
            print(f"Processed batch: {batch.num_rows} rows")
            
    except Exception as e:
        print(f"Streaming execution failed: {e}")
        # Error handling logic here

# Run example
asyncio.run(error_handling_example())
```

## Best Practices

### Performance Optimization

1. **Batch Size**: Use appropriate batch sizes (10K-100K records) for optimal throughput
2. **Parallelism**: Set `max_parallelism` to match Kafka partition count
3. **Checkpointing**: Use reasonable checkpoint intervals (30-300 seconds)
4. **State Management**: Use MarbleDB for all state operations
5. **Watermarks**: Configure appropriate watermark idle timeouts

### Error Handling

1. **Retry Logic**: Implement exponential backoff for transient errors
2. **Circuit Breakers**: Use circuit breakers for external dependencies
3. **Monitoring**: Monitor error rates and recovery success
4. **Alerting**: Set up alerts for critical error thresholds

### Monitoring

1. **Metrics**: Collect comprehensive metrics for all operations
2. **Health Checks**: Implement health checks for all components
3. **Profiling**: Use performance profiling to identify bottlenecks
4. **Logging**: Implement structured logging with correlation IDs

### State Management

1. **RAFT Replication**: Use RAFT for dimension tables and connector offsets
2. **Local State**: Use local tables for streaming state (window aggregates)
3. **Checkpointing**: Ensure atomic checkpoint commits
4. **Recovery**: Test recovery procedures regularly

## Troubleshooting

### Common Issues

1. **Kafka Connection Errors**: Check bootstrap servers and network connectivity
2. **MarbleDB State Errors**: Verify MarbleDB client initialization and permissions
3. **Watermark Errors**: Check timestamp column configuration and data quality
4. **Checkpoint Errors**: Verify checkpoint coordinator configuration and network
5. **Memory Issues**: Monitor memory usage and adjust batch sizes

### Debugging

1. **Enable Debug Logging**: Set log level to DEBUG for detailed information
2. **Use Metrics**: Monitor metrics to identify performance bottlenecks
3. **Health Checks**: Use health checks to identify component failures
4. **Profiling**: Use performance profiling to identify slow operations

### Recovery Procedures

1. **Component Restart**: Restart individual components for transient errors
2. **Execution Restart**: Restart entire execution for persistent errors
3. **State Recovery**: Use checkpoint recovery for state restoration
4. **Manual Intervention**: Escalate to manual intervention for critical failures
