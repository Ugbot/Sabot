/**
 * MarbleDB Advanced Features Demo
 *
 * Demonstrates TTL, Schema Evolution, and Compaction Tuning
 * in a realistic streaming SQL state management scenario.
 */

#include <marble/db.h>
#include <marble/ttl.h>
#include <marble/status.h>

#include <iostream>
#include <thread>
#include <chrono>
#include <memory>

using namespace marble;

/**
 * Streaming SQL State Manager Example
 *
 * Shows how to configure and use advanced features for streaming SQL state management.
 */
class StreamingSQLStateManager {
public:
    StreamingSQLStateManager() {
        // Initialize MarbleDB (simplified for demo)
        initializeMetrics();
        initializeLogging(Logger::Level::INFO);
        initializeAdvancedFeatures();

        MARBLE_LOG_INFO("StreamingStateManager", "Initialized advanced features demo");
    }

    ~StreamingSQLStateManager() {
        // Cleanup
        if (global_advanced_features_manager &&
            global_advanced_features_manager->GetTTLManager()->IsBackgroundCleanupRunning()) {
            global_advanced_features_manager->GetTTLManager()->StopBackgroundCleanup();
        }

        shutdownAdvancedFeatures();
        shutdownMonitoring();
    }

    /**
     * Initialize streaming state tables with appropriate advanced features
     */
    void initializeStreamingState() {
        MARBLE_LOG_INFO("StreamingStateManager", "Setting up streaming state tables...");

        // 1. Window Aggregates Table
        // - Short TTL (24 hours) for sliding windows
        // - Time-series compaction for efficient timestamp-based queries
        setupWindowAggregatesTable();

        // 2. Join State Table
        // - Medium TTL (12 hours) for join state
        // - Mixed workload optimization
        setupJoinStateTable();

        // 3. Deduplication Table
        // - Long TTL (7 days) for compliance
        // - Read-heavy optimization for dedup lookups
        setupDeduplicationTable();

        // 4. Raw Events Table
        // - Very short TTL (2 hours) for raw event buffering
        // - Write-heavy optimization for high ingestion
        setupRawEventsTable();

        MARBLE_LOG_INFO("StreamingStateManager", "All streaming state tables configured");

        // Start background TTL cleanup
        global_advanced_features_manager->GetTTLManager()->StartBackgroundCleanup();
        MARBLE_LOG_INFO("StreamingStateManager", "Background TTL cleanup started");
    }

    /**
     * Demonstrate schema evolution during runtime
     */
    void demonstrateSchemaEvolution() {
        MARBLE_LOG_INFO("StreamingStateManager", "Demonstrating schema evolution...");

        auto schema_manager = global_advanced_features_manager->GetSchemaEvolutionManager();

        // Add new analytics fields to window aggregates
        auto status = schema_manager->AddColumn("window_aggregates", "device_category",
                                               arrow::utf8(), "Added device categorization");
        if (!status.ok()) {
            MARBLE_LOG_ERROR("SchemaEvolution", "Failed to add device_category: " + status.ToString());
            return;
        }

        status = schema_manager->AddColumn("window_aggregates", "geographic_region",
                                          arrow::utf8(), "Added geographic segmentation");
        if (!status.ok()) {
            MARBLE_LOG_ERROR("SchemaEvolution", "Failed to add geographic_region: " + status.ToString());
            return;
        }

        // Rename a field
        status = schema_manager->RenameColumn("window_aggregates", "old_metric", "engagement_score");
        if (!status.ok()) {
            MARBLE_LOG_ERROR("SchemaEvolution", "Failed to rename field: " + status.ToString());
            return;
        }

        // Show change history
        auto history = schema_manager->GetChangeHistory("window_aggregates");
        MARBLE_LOG_INFO("SchemaEvolution", "Applied " + std::to_string(history.size()) +
                       " schema changes to window_aggregates");

        for (const auto& change : history) {
            MARBLE_LOG_INFO("SchemaEvolution", "  - " + change.change_description);
        }

        MARBLE_LOG_INFO("StreamingStateManager", "Schema evolution completed successfully");
    }

    /**
     * Demonstrate compaction tuning under different workloads
     */
    void demonstrateCompactionTuning() {
        MARBLE_LOG_INFO("StreamingStateManager", "Demonstrating compaction tuning...");

        auto compaction_tuner = global_advanced_features_manager->GetCompactionTuner();

        // Analyze current workload
        auto status = compaction_tuner->AnalyzeWorkload("window_aggregates", std::chrono::hours(1));
        if (status.ok()) {
            MARBLE_LOG_INFO("CompactionTuner", "Workload analysis completed for window_aggregates");
        }

        // Apply adaptive tuning
        status = compaction_tuner->ApplyAdaptiveTuning("window_aggregates");
        if (status.ok()) {
            MARBLE_LOG_INFO("CompactionTuner", "Adaptive tuning applied to window_aggregates");
        }

        // Force compaction on high-priority table
        status = compaction_tuner->ForceCompaction("deduplication");
        if (status.ok()) {
            MARBLE_LOG_INFO("CompactionTuner", "Forced compaction completed on deduplication table");
        }

        // Optimize raw events for write-heavy workload
        status = compaction_tuner->SetWorkloadHint("raw_events", CompactionTuner::WorkloadHint::WRITE_HEAVY);
        status = compaction_tuner->ApplyAdaptiveTuning("raw_events");
        if (status.ok()) {
            MARBLE_LOG_INFO("CompactionTuner", "Optimized raw_events for write-heavy workload");
        }

        MARBLE_LOG_INFO("StreamingStateManager", "Compaction tuning demonstration completed");
    }

    /**
     * Demonstrate TTL operations and monitoring
     */
    void demonstrateTTLOperations() {
        MARBLE_LOG_INFO("StreamingStateManager", "Demonstrating TTL operations...");

        auto ttl_manager = global_advanced_features_manager->GetTTLManager();

        // Manual cleanup operations
        auto current_time = std::chrono::system_clock::now();

        // Mark expired records
        auto status = ttl_manager->MarkExpiredRecords("window_aggregates", current_time);
        if (status.ok()) {
            MARBLE_LOG_INFO("TTLManager", "Marked expired records in window_aggregates");
        }

        // Clean up expired records
        status = ttl_manager->CleanupExpiredRecords("window_aggregates");
        if (status.ok()) {
            MARBLE_LOG_INFO("TTLManager", "Cleaned up expired records in window_aggregates");
        }

        // Get and display statistics
        auto stats = ttl_manager->GetStats();
        MARBLE_LOG_INFO("TTLManager", "TTL Statistics:");
        MARBLE_LOG_INFO("TTLManager", "  Total records expired: " + std::to_string(stats.total_records_expired));
        MARBLE_LOG_INFO("TTLManager", "  Total records cleaned: " + std::to_string(stats.total_records_cleaned));
        MARBLE_LOG_INFO("TTLManager", "  Tables with TTL: " + std::to_string(stats.per_table_expired.size()));

        MARBLE_LOG_INFO("StreamingStateManager", "TTL operations demonstration completed");
    }

    /**
     * Run health checks and monitoring
     */
    void runHealthChecks() {
        MARBLE_LOG_INFO("StreamingStateManager", "Running health checks...");

        // Perform advanced features health check
        auto health_check = global_advanced_features_manager->PerformAdvancedFeaturesHealthCheck();
        if (health_check.ok()) {
            MARBLE_LOG_INFO("HealthCheck", "Advanced features health check passed");
        } else {
            MARBLE_LOG_ERROR("HealthCheck", "Advanced features health check failed: " +
                           health_check.toString());
        }

        // Get comprehensive statistics
        auto stats = global_advanced_features_manager->GetAdvancedFeaturesStats();

        MARBLE_LOG_INFO("HealthCheck", "System Statistics:");
        MARBLE_LOG_INFO("HealthCheck", "  TTL - Expired: " +
                       std::to_string(stats.ttl_stats.total_records_expired) +
                       ", Cleaned: " + std::to_string(stats.ttl_stats.total_records_cleaned));
        MARBLE_LOG_INFO("HealthCheck", "  Schema - Changes: " +
                       std::to_string(stats.schema_stats.total_changes) +
                       ", Successful migrations: " + std::to_string(stats.schema_stats.successful_migrations));
        MARBLE_LOG_INFO("HealthCheck", "  Compaction - Tables configured: " +
                       std::to_string(stats.compaction_stats.size()));

        MARBLE_LOG_INFO("StreamingStateManager", "Health checks completed");
    }

    /**
     * Simulate streaming workload to demonstrate features
     */
    void simulateStreamingWorkload(int duration_seconds = 30) {
        MARBLE_LOG_INFO("StreamingStateManager", "Starting streaming workload simulation for " +
                       std::to_string(duration_seconds) + " seconds...");

        auto start_time = std::chrono::steady_clock::now();
        int operations = 0;

        while (std::chrono::steady_clock::now() - start_time < std::chrono::seconds(duration_seconds)) {
            // Simulate periodic health checks
            if (operations % 10 == 0) {
                runHealthChecks();
            }

            // Simulate TTL operations
            if (operations % 5 == 0) {
                demonstrateTTLOperations();
            }

            operations++;
            std::this_thread::sleep_for(std::chrono::seconds(5));
        }

        MARBLE_LOG_INFO("StreamingStateManager", "Streaming workload simulation completed");
    }

private:
    void setupWindowAggregatesTable() {
        // Window aggregates: 24-hour sliding windows
        TTLManager::TTLConfig ttl_config("window_aggregates", "window_end", std::chrono::hours(24));
        ttl_config.cleanup_interval = std::chrono::minutes(30);

        CompactionTuner::CompactionConfig compaction_config("window_aggregates");
        compaction_config.strategy = CompactionTuner::Strategy::TIME_BASED;
        compaction_config.time_window = std::chrono::hours(24);

        auto status = global_advanced_features_manager->ConfigureTableAdvancedFeatures(
            "window_aggregates", ttl_config, compaction_config,
            CompactionTuner::WorkloadHint::TIME_SERIES
        );

        if (status.ok()) {
            MARBLE_LOG_INFO("StreamingStateManager", "Configured window_aggregates table");
        } else {
            MARBLE_LOG_ERROR("StreamingStateManager", "Failed to configure window_aggregates: " +
                           status.ToString());
        }
    }

    void setupJoinStateTable() {
        // Join state: 12-hour retention
        TTLManager::TTLConfig ttl_config("join_state", "last_updated", std::chrono::hours(12));
        ttl_config.cleanup_interval = std::chrono::minutes(15);

        auto status = global_advanced_features_manager->ConfigureTableAdvancedFeatures(
            "join_state", ttl_config, CompactionTuner::CompactionConfig("join_state"),
            CompactionTuner::WorkloadHint::MIXED
        );

        if (status.ok()) {
            MARBLE_LOG_INFO("StreamingStateManager", "Configured join_state table");
        } else {
            MARBLE_LOG_ERROR("StreamingStateManager", "Failed to configure join_state: " +
                           status.ToString());
        }
    }

    void setupDeduplicationTable() {
        // Deduplication: 7-day compliance retention
        TTLManager::TTLConfig ttl_config("deduplication", "first_seen", std::chrono::days(7));
        ttl_config.cleanup_interval = std::chrono::hours(4);

        CompactionTuner::CompactionConfig compaction_config("deduplication");
        compaction_config.target_file_size_mb = 512;  // Larger files for read-heavy workload

        auto status = global_advanced_features_manager->ConfigureTableAdvancedFeatures(
            "deduplication", ttl_config, compaction_config,
            CompactionTuner::WorkloadHint::READ_HEAVY
        );

        if (status.ok()) {
            MARBLE_LOG_INFO("StreamingStateManager", "Configured deduplication table");
        } else {
            MARBLE_LOG_ERROR("StreamingStateManager", "Failed to configure deduplication: " +
                           status.ToString());
        }
    }

    void setupRawEventsTable() {
        // Raw events: 2-hour buffering
        TTLManager::TTLConfig ttl_config("raw_events", "event_timestamp", std::chrono::hours(2));
        ttl_config.cleanup_interval = std::chrono::minutes(10);

        auto status = global_advanced_features_manager->ConfigureTableAdvancedFeatures(
            "raw_events", ttl_config, CompactionTuner::CompactionConfig("raw_events"),
            CompactionTuner::WorkloadHint::WRITE_HEAVY
        );

        if (status.ok()) {
            MARBLE_LOG_INFO("StreamingStateManager", "Configured raw_events table");
        } else {
            MARBLE_LOG_ERROR("StreamingStateManager", "Failed to configure raw_events: " +
                           status.ToString());
        }
    }
};

/**
 * Real-Time Analytics Pipeline Example
 */
class RealTimeAnalyticsPipeline {
public:
    RealTimeAnalyticsPipeline() {
        initializeMetrics();
        initializeLogging(Logger::Level::INFO);
        initializeAdvancedFeatures();

        MARBLE_LOG_INFO("AnalyticsPipeline", "Initialized real-time analytics pipeline");
    }

    ~RealTimeAnalyticsPipeline() {
        shutdownAdvancedFeatures();
        shutdownMonitoring();
    }

    void setupAnalyticsInfrastructure() {
        MARBLE_LOG_INFO("AnalyticsPipeline", "Setting up analytics infrastructure...");

        // Raw events table - high ingestion, short retention
        setupRawEventsTable();

        // User sessions table - session-based TTL
        setupUserSessionsTable();

        // Aggregated metrics - long retention, optimized for analytics
        setupAggregatedMetricsTable();

        // Materialized views - computed results with TTL
        setupMaterializedViewsTable();

        MARBLE_LOG_INFO("AnalyticsPipeline", "Analytics infrastructure setup complete");
    }

    void demonstrateAdaptiveOptimization() {
        MARBLE_LOG_INFO("AnalyticsPipeline", "Demonstrating adaptive optimization...");

        auto compaction_tuner = global_advanced_features_manager->GetCompactionTuner();

        // Simulate high-write period (ingestion spike)
        MARBLE_LOG_INFO("AnalyticsPipeline", "Simulating high-write period...");
        compaction_tuner->SetWorkloadHint("raw_events", CompactionTuner::WorkloadHint::WRITE_HEAVY);
        compaction_tuner->ApplyAdaptiveTuning("raw_events");

        std::this_thread::sleep_for(std::chrono::seconds(2));

        // Switch to read-heavy period (analytics queries)
        MARBLE_LOG_INFO("AnalyticsPipeline", "Switching to read-heavy analytics period...");
        compaction_tuner->SetWorkloadHint("aggregated_metrics", CompactionTuner::WorkloadHint::READ_HEAVY);
        compaction_tuner->ApplyAdaptiveTuning("aggregated_metrics");

        std::this_thread::sleep_for(std::chrono::seconds(2));

        // Time-series optimization for trend analysis
        MARBLE_LOG_INFO("AnalyticsPipeline", "Optimizing for time-series trend analysis...");
        compaction_tuner->OptimizeForTimeSeries("user_sessions", std::chrono::days(30));

        MARBLE_LOG_INFO("AnalyticsPipeline", "Adaptive optimization demonstration completed");
    }

    void demonstrateSchemaEvolution() {
        MARBLE_LOG_INFO("AnalyticsPipeline", "Demonstrating schema evolution...");

        auto schema_manager = global_advanced_features_manager->GetSchemaEvolutionManager();

        // Add new event tracking fields
        schema_manager->AddColumn("raw_events", "event_category", arrow::utf8());
        schema_manager->AddColumn("raw_events", "user_segment", arrow::utf8());
        schema_manager->AddColumn("raw_events", "campaign_id", arrow::utf8());

        // Add new metrics to aggregated data
        schema_manager->AddColumn("aggregated_metrics", "conversion_rate", arrow::float64());
        schema_manager->AddColumn("aggregated_metrics", "engagement_score", arrow::float64());
        schema_manager->AddColumn("aggregated_metrics", "session_depth", arrow::int64());

        // Add computed fields to materialized views
        schema_manager->AddColumn("materialized_views", "predicted_churn_risk", arrow::float64());
        schema_manager->AddColumn("materialized_views", "lifetime_value", arrow::float64());

        auto history = schema_manager->GetChangeHistory("aggregated_metrics");
        MARBLE_LOG_INFO("AnalyticsPipeline", "Applied " + std::to_string(history.size()) +
                       " schema changes to support enhanced analytics");

        MARBLE_LOG_INFO("AnalyticsPipeline", "Schema evolution completed");
    }

private:
    void setupRawEventsTable() {
        TTLManager::TTLConfig ttl_config("raw_events", "timestamp", std::chrono::hours(2));

        global_advanced_features_manager->ConfigureTableAdvancedFeatures(
            "raw_events", ttl_config, CompactionTuner::CompactionConfig("raw_events"),
            CompactionTuner::WorkloadHint::WRITE_HEAVY
        );
    }

    void setupUserSessionsTable() {
        TTLManager::TTLConfig ttl_config("user_sessions", "session_start", std::chrono::days(30));

        CompactionTuner::CompactionConfig compaction_config("user_sessions");
        compaction_config.strategy = CompactionTuner::Strategy::TIME_BASED;

        global_advanced_features_manager->ConfigureTableAdvancedFeatures(
            "user_sessions", ttl_config, compaction_config,
            CompactionTuner::WorkloadHint::TIME_SERIES
        );
    }

    void setupAggregatedMetricsTable() {
        TTLManager::TTLConfig ttl_config("aggregated_metrics", "aggregation_window", std::chrono::days(90));

        CompactionTuner::CompactionConfig compaction_config("aggregated_metrics");
        compaction_config.target_file_size_mb = 512;

        global_advanced_features_manager->ConfigureTableAdvancedFeatures(
            "aggregated_metrics", ttl_config, compaction_config,
            CompactionTuner::WorkloadHint::READ_HEAVY
        );
    }

    void setupMaterializedViewsTable() {
        TTLManager::TTLConfig ttl_config("materialized_views", "computed_at", std::chrono::days(7));

        global_advanced_features_manager->ConfigureTableAdvancedFeatures(
            "materialized_views", ttl_config, CompactionTuner::CompactionConfig("materialized_views"),
            CompactionTuner::WorkloadHint::MIXED
        );
    }
};

int main() {
    std::cout << "=== MarbleDB Advanced Features Demo ===\n" << std::endl;

    try {
        // Part 1: Streaming SQL State Management
        std::cout << "1. Streaming SQL State Management Demo" << std::endl;
        std::cout << "=====================================" << std::endl;

        StreamingSQLStateManager state_manager;
        state_manager.initializeStreamingState();

        // Demonstrate schema evolution
        state_manager.demonstrateSchemaEvolution();

        // Demonstrate compaction tuning
        state_manager.demonstrateCompactionTuning();

        // Run a short simulation
        state_manager.simulateStreamingWorkload(15);  // 15 seconds

        std::cout << "\n2. Real-Time Analytics Pipeline Demo" << std::endl;
        std::cout << "===================================" << std::endl;

        // Part 2: Real-Time Analytics Pipeline
        RealTimeAnalyticsPipeline analytics_pipeline;
        analytics_pipeline.setupAnalyticsInfrastructure();

        // Demonstrate adaptive optimization
        analytics_pipeline.demonstrateAdaptiveOptimization();

        // Demonstrate schema evolution
        analytics_pipeline.demonstrateSchemaEvolution();

        std::cout << "\n=== Demo completed successfully! ===" << std::endl;
        std::cout << "Advanced features demonstrated:" << std::endl;
        std::cout << "✓ TTL (Time To Live) for automatic data expiration" << std::endl;
        std::cout << "✓ Schema Evolution for online schema changes" << std::endl;
        std::cout << "✓ Compaction Tuning for workload-optimized storage" << std::endl;
        std::cout << "✓ Integrated monitoring and health checks" << std::endl;

    } catch (const std::exception& e) {
        std::cerr << "Demo failed with exception: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}

