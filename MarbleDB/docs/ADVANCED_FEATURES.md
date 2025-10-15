# MarbleDB Advanced Features

This document describes MarbleDB's advanced features including Time To Live (TTL) for automatic data expiration, Schema Evolution for online schema changes, and Compaction Tuning for performance optimization.

## Overview

MarbleDB provides enterprise-grade advanced features that enable:

- **TTL (Time To Live)**: Automatic cleanup of expired streaming data
- **Schema Evolution**: Online schema changes without downtime
- **Compaction Tuning**: Intelligent storage optimization based on workload patterns

These features are essential for production streaming SQL deployments where data lifecycle management and schema flexibility are critical.

## Time To Live (TTL) Management

TTL automatically removes expired data based on timestamps, crucial for streaming SQL state management where old data needs to be cleaned up to prevent unbounded growth.

### Basic TTL Configuration

```cpp
#include <marble/ttl.h>

// Configure TTL for a table
TTLManager::TTLConfig ttl_config("stream_state", "event_timestamp", std::chrono::hours(24));
ttl_config.cleanup_interval = std::chrono::minutes(30);  // Check every 30 minutes

auto ttl_manager = global_advanced_features_manager->GetTTLManager();
auto status = ttl_manager->AddTTLConfig(ttl_config);
if (!status.ok()) {
    MARBLE_LOG_ERROR("TTLConfig", "Failed to configure TTL: " + status.ToString());
}
```

### TTL Operations

```cpp
// Start background cleanup (recommended for production)
ttl_manager->StartBackgroundCleanup();

// Manual cleanup operations
auto current_time = std::chrono::system_clock::now();

// Mark expired records (soft delete)
status = ttl_manager->MarkExpiredRecords("stream_state", current_time);

// Physically remove expired records
status = ttl_manager->CleanupExpiredRecords("stream_state");

// Get TTL statistics
auto stats = ttl_manager->GetStats();
MARBLE_LOG_INFO("TTL", "Expired records: " + std::to_string(stats.total_records_expired));

// Stop background cleanup when shutting down
ttl_manager->StopBackgroundCleanup();
```

### TTL for Streaming SQL

TTL is essential for streaming SQL state management:

```cpp
// Configure TTL for different state tables
TTLManager::TTLConfig window_state_ttl("window_aggregates", "window_end", std::chrono::hours(24));
TTLManager::TTLConfig join_state_ttl("join_state", "last_updated", std::chrono::hours(12));
TTLManager::TTLConfig dedup_state_ttl("deduplication", "first_seen", std::chrono::days(7));

// Different TTL policies for different data types
// - Window aggregates: Keep for 24 hours (analysis window)
// - Join state: Keep for 12 hours (typical query patterns)
// - Deduplication: Keep for 7 days (compliance requirements)
```

## Schema Evolution

Schema evolution allows modifying table schemas without downtime, enabling applications to evolve their data models over time.

### Online Schema Changes

```cpp
#include <marble/ttl.h>

auto schema_manager = global_advanced_features_manager->GetSchemaEvolutionManager();

// Add a new column
auto status = schema_manager->AddColumn("user_events", "device_type", arrow::utf8(), "Added device tracking");
if (!status.ok()) {
    MARBLE_LOG_ERROR("Schema", "Failed to add column: " + status.ToString());
}

// Rename an existing column
status = schema_manager->RenameColumn("user_events", "old_field", "new_field_name");

// Change column type (for compatible types)
status = schema_manager->ModifyColumnType("user_events", "user_id", arrow::int64());

// Remove unused column
status = schema_manager->DropColumn("user_events", "deprecated_field");
```

### Schema Compatibility Checking

```cpp
// Check if a new schema is compatible with existing data
std::vector<std::shared_ptr<arrow::Field>> new_fields = {
    arrow::field("id", arrow::int64()),
    arrow::field("name", arrow::utf8()),
    arrow::field("email", arrow::utf8()),  // New field
    arrow::field("created_at", arrow::timestamp(arrow::TimeUnit::MICRO))
};

auto new_schema = std::make_shared<arrow::Schema>(new_fields);

auto status = schema_manager->CheckSchemaCompatibility("users", *new_schema);
if (status.ok()) {
    // Schema is compatible, perform migration
    status = schema_manager->MigrateDataToNewSchema("users", *old_schema, *new_schema);
}
```

### Schema Change History and Rollback

```cpp
// View change history
auto history = schema_manager->GetChangeHistory("user_events");
for (const auto& change : history) {
    MARBLE_LOG_INFO("Schema", "Change: " + change.change_description +
                   " at " + std::to_string(change.change_timestamp));
}

// Rollback the last change if needed
status = schema_manager->RollbackLastChange("user_events");
if (status.ok()) {
    MARBLE_LOG_INFO("Schema", "Successfully rolled back last change");
}
```

### Schema Evolution for Streaming SQL

Schema evolution enables streaming SQL applications to adapt to changing business requirements:

```cpp
// Example: Evolving a user analytics schema
// Version 1: Basic user tracking
schema_manager->AddColumn("user_analytics", "user_id", arrow::utf8());
schema_manager->AddColumn("user_analytics", "page_views", arrow::int64());

// Version 2: Add session tracking
schema_manager->AddColumn("user_analytics", "session_id", arrow::utf8());
schema_manager->AddColumn("user_analytics", "session_duration", arrow::int64());

// Version 3: Add geographic data
schema_manager->AddColumn("user_analytics", "country", arrow::utf8());
schema_manager->AddColumn("user_analytics", "city", arrow::utf8());
```

## Compaction Tuning

Compaction tuning provides intelligent storage optimization based on workload patterns and data characteristics.

### Workload-Based Optimization

```cpp
#include <marble/ttl.h>

auto compaction_tuner = global_advanced_features_manager->GetCompactionTuner();

// Set workload hint for automatic optimization
auto status = compaction_tuner->SetWorkloadHint("user_events", CompactionTuner::WorkloadHint::TIME_SERIES);
if (!status.ok()) {
    MARBLE_LOG_ERROR("Compaction", "Failed to set workload hint: " + status.ToString());
}

// Configure custom compaction settings
CompactionTuner::CompactionConfig config("user_events");
config.target_file_size_mb = 256;
config.max_files_per_level = 6;
config.strategy = CompactionTuner::Strategy::TIME_BASED;
config.time_window = std::chrono::hours(24);

status = compaction_tuner->SetCompactionConfig("user_events", config);
```

### Adaptive Tuning

```cpp
// Analyze workload patterns
status = compaction_tuner->AnalyzeWorkload("user_events", std::chrono::hours(1));

// Apply adaptive tuning based on analysis
status = compaction_tuner->ApplyAdaptiveTuning("user_events");

// The system will automatically adjust:
// - File size targets
// - Level limits
// - Compaction aggressiveness
// Based on observed read/write patterns
```

### Specialized Optimization

```cpp
// Time series data optimization
status = compaction_tuner->OptimizeForTimeSeries("metrics_data", std::chrono::days(30));

// Hot data optimization (keep recent data fast)
status = compaction_tuner->OptimizeForHotData("active_sessions", 0.1);  // 10% hot data
```

### Manual Compaction Control

```cpp
// Force immediate compaction
std::vector<std::string> input_files = {"data_001.sst", "data_002.sst"};
status = compaction_tuner->ForceCompaction("user_events", input_files);

// Adjust compaction priority
status = compaction_tuner->SetCompactionPriority("critical_table", 10);  // High priority

// Pause/resume compaction for maintenance
status = compaction_tuner->PauseCompaction("user_events");
// Perform maintenance operations...
status = compaction_tuner->ResumeCompaction("user_events");
```

### Compaction Monitoring

```cpp
// Get compaction metrics
auto metrics = compaction_tuner->GetCompactionMetrics("user_events");
MARBLE_LOG_INFO("Compaction", "Total compactions: " + std::to_string(metrics.total_compactions) +
               ", Avg ratio: " + std::to_string(metrics.avg_compaction_ratio));

// Get all compaction metrics
auto all_metrics = compaction_tuner->GetAllCompactionMetrics();
for (const auto& [table, table_metrics] : all_metrics) {
    MARBLE_METRICS_SET_GAUGE(("marble.compaction.files_created." + table).c_str(),
                            table_metrics.files_created);
}
```

## Integrated Advanced Features

The `AdvancedFeaturesManager` provides unified access to all advanced features:

### Complete Table Configuration

```cpp
// Configure all advanced features for a streaming SQL state table
TTLManager::TTLConfig ttl_config("stream_state", "event_time", std::chrono::hours(24));

CompactionTuner::CompactionConfig compaction_config("stream_state");
compaction_config.strategy = CompactionTuner::Strategy::TIME_BASED;
compaction_config.time_window = std::chrono::hours(24);

auto status = global_advanced_features_manager->ConfigureTableAdvancedFeatures(
    "stream_state",
    ttl_config,
    compaction_config,
    CompactionTuner::WorkloadHint::TIME_SERIES
);
```

### Health Monitoring

```cpp
// Perform comprehensive health check
auto health_check = global_advanced_features_manager->PerformAdvancedFeaturesHealthCheck();
if (!health_check.ok()) {
    MARBLE_LOG_ERROR("Health", "Advanced features health check failed: " + health_check.toString());
}
```

### Statistics and Monitoring

```cpp
// Get comprehensive statistics
auto stats = global_advanced_features_manager->GetAdvancedFeaturesStats();

// TTL statistics
MARBLE_METRICS_SET_GAUGE("marble.ttl.total_expired", stats.ttl_stats.total_records_expired);
MARBLE_METRICS_SET_GAUGE("marble.ttl.total_cleaned", stats.ttl_stats.total_records_cleaned);

// Schema evolution statistics
MARBLE_METRICS_SET_GAUGE("marble.schema.total_changes", stats.schema_stats.total_changes);

// Compaction statistics
for (const auto& [table, metrics] : stats.compaction_stats) {
    MARBLE_METRICS_SET_GAUGE(("marble.compaction.total." + table).c_str(),
                            metrics.total_compactions);
}
```

## Production Deployment Examples

### Streaming SQL State Management

```cpp
class StreamingStateManager {
public:
    void initializeStreamingState() {
        // Configure window state with TTL
        TTLManager::TTLConfig window_ttl("window_state", "window_end", std::chrono::hours(24));
        window_ttl.cleanup_interval = std::chrono::minutes(15);

        CompactionTuner::CompactionConfig window_compaction("window_state");
        window_compaction.strategy = CompactionTuner::Strategy::TIME_BASED;

        global_advanced_features_manager->ConfigureTableAdvancedFeatures(
            "window_state", window_ttl, window_compaction,
            CompactionTuner::WorkloadHint::TIME_SERIES
        );

        // Configure join state with shorter TTL
        TTLManager::TTLConfig join_ttl("join_state", "last_access", std::chrono::hours(6));

        global_advanced_features_manager->ConfigureTableAdvancedFeatures(
            "join_state", join_ttl, CompactionTuner::CompactionConfig("join_state"),
            CompactionTuner::WorkloadHint::MIXED
        );

        // Start background cleanup
        global_advanced_features_manager->GetTTLManager()->StartBackgroundCleanup();
    }

    void evolveSchema() {
        auto schema_manager = global_advanced_features_manager->GetSchemaEvolutionManager();

        // Add new fields for enhanced analytics
        schema_manager->AddColumn("window_state", "device_category", arrow::utf8());
        schema_manager->AddColumn("window_state", "geographic_region", arrow::utf8());
    }

    void optimizeForHighLoad() {
        auto compaction_tuner = global_advanced_features_manager->GetCompactionTuner();

        // Optimize for read-heavy analytics queries
        compaction_tuner->SetWorkloadHint("window_state", CompactionTuner::WorkloadHint::READ_HEAVY);
        compaction_tuner->ApplyAdaptiveTuning("window_state");
    }
};
```

### Real-Time Analytics Pipeline

```cpp
class AnalyticsPipeline {
public:
    void setupAnalyticsTables() {
        // Raw events table - short TTL, time-series optimization
        TTLManager::TTLConfig events_ttl("raw_events", "event_timestamp", std::chrono::hours(2));

        global_advanced_features_manager->ConfigureTableAdvancedFeatures(
            "raw_events", events_ttl, CompactionTuner::CompactionConfig("raw_events"),
            CompactionTuner::WorkloadHint::TIME_SERIES
        );

        // Aggregated metrics - longer retention, read optimization
        TTLManager::TTLConfig metrics_ttl("aggregated_metrics", "aggregation_window", std::chrono::days(30));

        CompactionTuner::CompactionConfig metrics_compaction("aggregated_metrics");
        metrics_compaction.target_file_size_mb = 512;  // Larger files for analytics

        global_advanced_features_manager->ConfigureTableAdvancedFeatures(
            "aggregated_metrics", metrics_ttl, metrics_compaction,
            CompactionTuner::WorkloadHint::READ_HEAVY
        );
    }

    void handleSchemaEvolution() {
        auto schema_manager = global_advanced_features_manager->GetSchemaEvolutionManager();

        // Add new event types dynamically
        schema_manager->AddColumn("raw_events", "event_category", arrow::utf8());
        schema_manager->AddColumn("raw_events", "user_segment", arrow::utf8());

        // Add new metrics
        schema_manager->AddColumn("aggregated_metrics", "conversion_rate", arrow::float64());
        schema_manager->AddColumn("aggregated_metrics", "engagement_score", arrow::float64());
    }
};
```

## Performance Considerations

### TTL Performance

- **Background Cleanup**: Minimal impact when running continuously
- **Memory Usage**: O(number of configured tables)
- **I/O Patterns**: Batched cleanup operations reduce overhead
- **Typical Overhead**: < 2% CPU for normal operation

### Schema Evolution Performance

- **Online Changes**: No downtime required
- **Migration Cost**: Proportional to data size
- **Rollback Safety**: Fast rollback for recent changes
- **Compatibility Checks**: Lightweight validation

### Compaction Tuning Performance

- **Adaptive Analysis**: Low-overhead workload monitoring
- **Strategy Switching**: Minimal disruption during changes
- **Manual Controls**: Immediate response for critical operations
- **Monitoring Overhead**: < 1% for metrics collection

## Best Practices

### TTL Configuration

1. **Choose Appropriate TTL Values**:
   - Streaming state: 24-72 hours
   - Analytics data: 30-90 days
   - Audit logs: 1-7 years (regulatory requirements)

2. **Set Cleanup Intervals**:
   - Frequent for high-volume tables (< 1 hour)
   - Less frequent for large historical tables (4-24 hours)

3. **Monitor Cleanup Performance**:
   - Track cleanup latency and throughput
   - Adjust intervals based on system load

### Schema Evolution

1. **Plan Schema Changes**:
   - Test compatibility before deployment
   - Have rollback plans for critical changes
   - Communicate changes to dependent systems

2. **Use Compatible Types**:
   - Prefer compatible type changes (int32→int64)
   - Avoid incompatible changes without migration

3. **Version Schema Changes**:
   - Track schema versions in application code
   - Test applications against new schemas

### Compaction Tuning

1. **Understand Workload Patterns**:
   - Analyze read/write ratios
   - Identify hot/cold data patterns
   - Monitor query performance

2. **Choose Appropriate Strategies**:
   - Time-series: For timestamp-ordered data
   - Read-heavy: For analytical workloads
   - Write-heavy: For high ingestion rates

3. **Monitor Compaction Effectiveness**:
   - Track compaction ratios and I/O patterns
   - Adjust parameters based on observed performance
   - Balance storage efficiency vs. query performance

## Monitoring and Alerting

### Key Metrics to Monitor

```cpp
// TTL Health
MARBLE_METRICS_SET_GAUGE("marble.ttl.cleanup_latency",
                        ttl_manager->GetStats().last_cleanup_timestamp);

// Schema Health
MARBLE_METRICS_SET_GAUGE("marble.schema.failed_migrations",
                        schema_manager->GetStats().failed_migrations);

// Compaction Health
for (const auto& [table, metrics] : compaction_tuner->GetAllCompactionMetrics()) {
    MARBLE_METRICS_SET_GAUGE(("marble.compaction.ratio." + table).c_str(),
                            metrics.avg_compaction_ratio);
}
```

### Alert Conditions

- TTL cleanup falling behind schedule
- Schema migration failures
- Compaction queue growing too large
- Storage efficiency dropping significantly

## Testing

Comprehensive test coverage ensures reliability:

```bash
# Run advanced features tests
cd MarbleDB/build
ctest -R advanced_features

# Run specific test
./tests/test_advanced_features --gtest_filter=AdvancedFeaturesTest.TTLManagerBasicOperations
```

Tests cover:
- ✅ TTL configuration and cleanup operations
- ✅ Schema evolution with compatibility checking
- ✅ Compaction tuning strategies and optimization
- ✅ Integrated advanced features management
- ✅ Concurrent access and thread safety
- ✅ Error handling and edge cases
- ✅ Performance and metrics validation

