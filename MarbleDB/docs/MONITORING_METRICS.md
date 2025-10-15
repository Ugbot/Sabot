# MarbleDB Monitoring and Metrics

This document describes MarbleDB's comprehensive monitoring, metrics collection, error handling, and observability features for production deployments.

## Overview

MarbleDB provides enterprise-grade monitoring capabilities including:

- **Metrics Collection**: Performance metrics, operation counters, latency tracking
- **Health Monitoring**: Component health checks and system status
- **Error Handling**: Detailed error context and structured error reporting
- **Logging**: Structured logging with configurable levels
- **Export Formats**: Prometheus and JSON metrics export for monitoring systems

## Core Components

### MetricsCollector

The central metrics collection system supporting multiple metric types:

```cpp
#include <marble/metrics.h>

// Get global metrics collector
auto metrics = global_metrics_collector;

// Counter operations (monotonically increasing)
metrics->incrementCounter("read_operations", 1);
int64_t reads = metrics->getCounter("read_operations");

// Gauge operations (point-in-time values)
metrics->setGauge("active_connections", 42);
int64_t connections = metrics->getGauge("active_connections");

// Histogram operations (value distributions)
metrics->recordHistogram("read_latency_ms", 15);
auto latencies = metrics->getHistogram("read_latency_ms");

// Timer operations (automatic duration measurement)
{
    MetricsCollector::Timer timer(*metrics, "operation_duration");
    // Operation code here
    // Duration automatically recorded when timer goes out of scope
}
```

### Logger

Structured logging with configurable levels and component-based organization:

```cpp
#include <marble/metrics.h>

// Initialize logging
initializeLogging(Logger::Level::INFO);

// Log messages with component and optional details
MARBLE_LOG_INFO("storage_engine", "SSTable compaction completed");
MARBLE_LOG_ERROR("network_manager", "Connection timeout", "peer=192.168.1.100");
MARBLE_LOG_DEBUG("cache_system", "Cache hit ratio improved", "ratio=0.95");
```

### StatusWithMetrics

Enhanced status reporting with error context and automatic metrics recording:

```cpp
// Success status
auto status = StatusWithMetrics::OK();

// Error status with context
auto context = std::make_unique<ErrorContext>("file_operation", "io_manager");
context->addDetail("file_path", "/data/db/manifest");
context->addDetail("operation", "read");
context->setErrorCode(13);

auto error_status = StatusWithMetrics(
    StatusWithMetrics::Code::IO_ERROR,
    "Failed to read manifest file",
    std::move(context)
);

// Check status and record metrics automatically
if (!status.ok()) {
    status.recordMetrics(*metrics_collector, "file_read");
    MARBLE_LOG_ERROR("io_manager", status.toString());
}
```

## Predefined Metrics

MarbleDB collects comprehensive metrics across all operations:

### Operation Counters
- `marble.read.operations` - Total read operations
- `marble.write.operations` - Total write operations
- `marble.errors.count` - Total error count

### Latency Histograms
- `marble.read.latency_ms` - Read operation latency
- `marble.write.latency_ms` - Write operation latency
- `marble.compaction.latency_ms` - Compaction operation latency
- `marble.raft.latency_ms` - RAFT operation latency

### Gauges
- `marble.connections.active` - Active connections
- `marble.connections.total` - Total connections
- `marble.sstable.count` - Number of SSTable files
- `marble.memtable.size_bytes` - Memtable memory usage

### Performance Metrics
- `marble.bloom.hits` - Bloom filter hits
- `marble.bloom.misses` - Bloom filter misses
- `marble.cache.hits` - Cache hits
- `marble.cache.misses` - Cache misses
- `marble.compaction.count` - Compaction operations

### Triple Store Metrics
- `marble.triple_store.queries` - SPARQL query count
- `marble.triple_store.latency_ms` - SPARQL query latency

## Health Monitoring

MarbleDB provides comprehensive health checks for production monitoring:

```cpp
// Get health status of all components
auto health_status = db->GetHealthStatus();

// Individual component checks
bool storage_healthy = db->GetMetricsCollector()->isHealthy("storage_engine");
bool cache_healthy = db->GetMetricsCollector()->isHealthy("cache_system");

// Perform comprehensive health check
auto health_check_result = db->PerformHealthCheck();
if (!health_check_result.ok()) {
    MARBLE_LOG_ERROR("health_monitor", health_check_result.toString());
}
```

### Health Check Components

- **storage_engine**: LSM-tree storage health
- **cache_system**: Cache subsystem health
- **compaction_threads**: Background compaction health
- **network_connectivity**: Network connectivity (RAFT)
- **memory_usage**: Memory usage validation
- **disk_space**: Available disk space
- **file_system**: File system accessibility

## Metrics Export

MarbleDB supports multiple export formats for integration with monitoring systems:

### Prometheus Format

```cpp
std::string prometheus_metrics = db->ExportMetricsPrometheus();
// Output suitable for Prometheus /metrics endpoint
```

Example output:
```
# TYPE marble_read_operations counter
marble_read_operations 15432

# TYPE marble_active_connections gauge
marble_active_connections 12

# TYPE marble_read_latency_ms histogram
marble_read_latency_ms_count 15432
marble_read_latency_ms_sum 234567
marble_read_latency_ms_50 15
marble_read_latency_ms_95 89
marble_read_latency_ms_99 234
```

### JSON Format

```cpp
std::string json_metrics = db->ExportMetricsJSON();
// Structured JSON for custom monitoring
```

Example output:
```json
{
  "counters": {
    "marble.read.operations": 15432,
    "marble.write.operations": 8765
  },
  "gauges": {
    "marble.active_connections": 12,
    "marble.sstable.count": 45
  },
  "histograms": {
    "marble.read.latency_ms": {
      "count": 15432,
      "sum": 234567,
      "min": 1,
      "max": 1500,
      "median": 15,
      "p95": 89,
      "p99": 234
    }
  },
  "health_checks": {
    "storage_engine": true,
    "cache_system": true,
    "network_connectivity": false
  },
  "timestamp": 1703123456
}
```

## Error Handling

### Error Context

Detailed error reporting with contextual information:

```cpp
try {
    // Database operation
    auto status = db->Get(options, key, &record);
    if (!status.ok()) {
        // Create detailed error context
        auto context = std::make_unique<ErrorContext>("key_lookup", "storage_engine");
        context->addDetail("key", key.ToString());
        context->addDetail("table", table_name);
        context->addDetail("operation_type", "point_lookup");

        // Enhance status with context
        auto enhanced_status = StatusWithMetrics(
            StatusWithMetrics::Code::NOT_FOUND,
            "Key not found in database",
            std::move(context)
        );

        // Record metrics and log
        enhanced_status.recordMetrics(*metrics_collector, "read");
        MARBLE_LOG_ERROR("storage_engine", enhanced_status.toString());

        return enhanced_status;
    }
} catch (const std::exception& e) {
    auto context = std::make_unique<ErrorContext>("key_lookup", "storage_engine");
    context->addDetail("exception_type", typeid(e).name());
    context->addDetail("exception_message", e.what());

    return StatusWithMetrics(
        StatusWithMetrics::Code::RUNTIME_ERROR,
        "Unexpected error during key lookup",
        std::move(context)
    );
}
```

### Error Codes

MarbleDB defines specific error codes for different failure scenarios:

- `OK` - Operation successful
- `NOT_FOUND` - Key or resource not found
- `CORRUPTION` - Data corruption detected
- `NOT_SUPPORTED` - Operation not supported
- `INVALID_ARGUMENT` - Invalid input parameters
- `IO_ERROR` - I/O operation failed
- `NETWORK_ERROR` - Network communication error
- `TIMEOUT` - Operation timeout
- `BUSY` - System busy, retry later
- `RUNTIME_ERROR` - Unexpected runtime error
- `ABORTED` - Operation was aborted
- `UNKNOWN` - Unknown error condition

## Logging Configuration

### Log Levels

- `TRACE` - Detailed execution tracing
- `DEBUG` - Debug information for development
- `INFO` - General information about operations
- `WARN` - Warning conditions that don't stop operations
- `ERROR` - Error conditions that may affect operations
- `FATAL` - Critical errors that require immediate attention

### Configuration

```cpp
// Initialize with specific log level
initializeLogging(Logger::Level::INFO);

// Change log level at runtime
if (global_logger) {
    static_cast<StandardLogger*>(global_logger.get())->setMinLevel(Logger::Level::DEBUG);
}
```

### Structured Logging

All log entries include:
- Timestamp with millisecond precision
- Log level
- Component name
- Thread ID
- Message
- Optional details

Example log output:
```
[2024-01-15 14:30:25.123] [INFO] [storage_engine] [140234567890] SSTable compaction completed | duration_ms=1250 tables_compacted=3
```

## Production Integration

### Prometheus Integration

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'marbledb'
    static_configs:
      - targets: ['marbledb-host:8080']
    metrics_path: '/metrics'
```

### Health Check Endpoint

```cpp
// HTTP health check endpoint
void handleHealthCheck(const httplib::Request& req, httplib::Response& res) {
    auto health_status = db->GetHealthStatus();
    auto metrics_json = db->ExportMetricsJSON();

    nlohmann::json response;
    response["status"] = "healthy";  // Simplified - check individual components
    response["timestamp"] = std::chrono::system_clock::now().time_since_epoch().count();
    response["health_checks"] = health_status;
    response["metrics"] = nlohmann::json::parse(metrics_json);

    res.set_content(response.dump(2), "application/json");
}
```

### Monitoring Dashboard

Example Grafana dashboard panels:

1. **Request Rate**: `rate(marble_read_operations[5m])`
2. **Latency P95**: `histogram_quantile(0.95, rate(marble_read_latency_ms_bucket[5m]))`
3. **Error Rate**: `rate(marble_errors_count[5m])`
4. **Active Connections**: `marble_connections_active`
5. **Health Status**: Health check status indicators

## Performance Impact

The monitoring system is designed for minimal performance impact:

- **Metrics Collection**: Lock-free atomic operations where possible
- **Logging**: Asynchronous logging to avoid blocking operations
- **Health Checks**: Lightweight checks that don't interfere with normal operations
- **Memory Usage**: Bounded data structures to prevent unbounded growth

Typical overhead:
- **CPU**: < 1% for typical workloads
- **Memory**: ~10MB for metrics storage
- **Latency**: < 1μs per metrics operation

## Best Practices

### Metrics Naming

- Use consistent naming conventions: `marble.<component>.<metric>`
- Include units in metric names where applicable: `latency_ms`, `size_bytes`
- Use descriptive names that indicate the metric's purpose

### Error Handling

- Always provide detailed error context for debugging
- Log errors at appropriate levels (ERROR for user-facing, DEBUG for internal)
- Include relevant operation parameters in error context
- Use specific error codes rather than generic ones

### Health Checks

- Implement fast health checks that don't impact performance
- Check critical dependencies (storage, network, memory)
- Use health checks for load balancer integration
- Alert on health check failures

### Logging

- Use appropriate log levels for different scenarios
- Include component names for easy filtering
- Add structured details for better debugging
- Avoid logging sensitive information

## Testing

Comprehensive test coverage for monitoring features:

```bash
# Run monitoring tests
cd MarbleDB/build
ctest -R monitoring

# Run specific test
./tests/test_monitoring_metrics --gtest_filter=MonitoringMetricsTest.BasicOperations
```

Tests cover:
- Metrics collection and retrieval
- Health check functionality
- Error context and status reporting
- Concurrent access patterns
- Export format validation
- Performance impact validation

