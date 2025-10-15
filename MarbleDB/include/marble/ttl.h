#pragma once

#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <arrow/api.h>
#include <arrow/type.h>

#include "marble/status.h"
#include "marble/record.h"
#include "marble/metrics.h"

namespace marble {

/**
 * @brief Time To Live (TTL) system for automatic data expiration
 *
 * Provides automatic cleanup of expired data based on timestamps,
 * crucial for streaming SQL state management and time-series data.
 */
class TTLManager {
public:
    // TTL configuration per table
    struct TTLConfig {
        std::string table_name;
        std::string timestamp_column;  // Column containing timestamp
        std::chrono::seconds ttl_seconds;  // Time to live duration
        bool enabled = true;
        std::chrono::seconds cleanup_interval = std::chrono::seconds(3600);  // Run cleanup hourly

        TTLConfig() = default;
        TTLConfig(const std::string& table, const std::string& ts_col,
                 std::chrono::seconds ttl, bool enable = true)
            : table_name(table), timestamp_column(ts_col), ttl_seconds(ttl), enabled(enable) {}
    };

    TTLManager();
    ~TTLManager() = default;

    // Configuration management
    Status AddTTLConfig(const TTLConfig& config);
    Status RemoveTTLConfig(const std::string& table_name);
    Status UpdateTTLConfig(const TTLConfig& config);
    std::vector<TTLConfig> GetAllTTLConfigs() const;

    // TTL operations
    Status MarkExpiredRecords(const std::string& table_name,
                             std::chrono::system_clock::time_point current_time);
    Status CleanupExpiredRecords(const std::string& table_name);

    // Background processing
    void StartBackgroundCleanup();
    void StopBackgroundCleanup();
    bool IsBackgroundCleanupRunning() const;

    // Statistics
    struct TTLStats {
        uint64_t total_records_expired = 0;
        uint64_t total_records_cleaned = 0;
        uint64_t last_cleanup_timestamp = 0;
        std::unordered_map<std::string, uint64_t> per_table_expired;
        std::unordered_map<std::string, uint64_t> per_table_cleaned;
    };

    TTLStats GetStats() const;

    // Reset stats (for testing)
    void ResetStats();

private:
    mutable std::mutex mutex_;
    std::unordered_map<std::string, TTLConfig> ttl_configs_;
    TTLStats stats_;
    std::atomic<bool> background_cleanup_running_{false};
    std::thread background_thread_;

    // Background cleanup loop
    void BackgroundCleanupLoop();

    // Helper functions
    std::chrono::system_clock::time_point GetCurrentTime() const;
    bool IsRecordExpired(const Record& record, const TTLConfig& config,
                        std::chrono::system_clock::time_point current_time) const;
};

/**
 * @brief Schema evolution system for online schema changes
 *
 * Allows modifying table schemas without downtime, with backward compatibility
 * and migration support.
 */
class SchemaEvolutionManager {
public:
    // Schema change types
    enum class ChangeType {
        ADD_COLUMN,
        DROP_COLUMN,
        MODIFY_COLUMN_TYPE,
        RENAME_COLUMN,
        ADD_INDEX,
        DROP_INDEX
    };

    // Schema change representation
    struct SchemaChange {
        ChangeType type;
        std::string table_name;
        std::string column_name;
        std::string new_column_name;  // For renames
        std::shared_ptr<arrow::DataType> new_type;  // For type changes
        std::shared_ptr<arrow::Field> new_field;    // For new columns
        uint64_t change_timestamp;
        std::string change_description;

        SchemaChange(ChangeType t, const std::string& table, const std::string& col)
            : type(t), table_name(table), column_name(col),
              change_timestamp(std::chrono::duration_cast<std::chrono::seconds>(
                  std::chrono::system_clock::now().time_since_epoch()).count()) {}
    };

    SchemaEvolutionManager();
    ~SchemaEvolutionManager() = default;

    // Schema change operations
    Status AddColumn(const std::string& table_name, const std::string& column_name,
                    std::shared_ptr<arrow::DataType> data_type,
                    const std::string& description = "");

    Status DropColumn(const std::string& table_name, const std::string& column_name,
                     const std::string& description = "");

    Status ModifyColumnType(const std::string& table_name, const std::string& column_name,
                           std::shared_ptr<arrow::DataType> new_type,
                           const std::string& description = "");

    Status RenameColumn(const std::string& table_name, const std::string& old_name,
                       const std::string& new_name, const std::string& description = "");

    // Schema compatibility checking
    Status CheckSchemaCompatibility(const std::string& table_name,
                                   const arrow::Schema& new_schema) const;

    Status MigrateDataToNewSchema(const std::string& table_name,
                                 const arrow::Schema& old_schema,
                                 const arrow::Schema& new_schema);

    // Change history and rollback
    std::vector<SchemaChange> GetChangeHistory(const std::string& table_name) const;
    Status RollbackLastChange(const std::string& table_name);

    // Statistics
    struct SchemaStats {
        uint64_t total_changes = 0;
        uint64_t successful_migrations = 0;
        uint64_t failed_migrations = 0;
        std::unordered_map<std::string, uint64_t> changes_per_table;
        std::unordered_map<ChangeType, uint64_t> changes_by_type;
    };

    SchemaStats GetStats() const;

private:
    mutable std::mutex mutex_;
    std::unordered_map<std::string, std::vector<SchemaChange>> change_history_;
    SchemaStats stats_;

    // Helper functions
    Status ApplySchemaChange(const SchemaChange& change);
    Status ValidateSchemaChange(const SchemaChange& change) const;
    std::shared_ptr<arrow::Schema> GetCurrentSchema(const std::string& table_name) const;
};

/**
 * @brief Advanced compaction tuning and strategies
 *
 * Provides sophisticated compaction controls for optimizing storage and performance
 * based on workload patterns and data characteristics.
 */
class CompactionTuner {
public:
    // Compaction strategies
    enum class Strategy {
        SIZE_BASED,      // Leveled compaction (size-based)
        TIME_BASED,      // Time-window compaction
        USAGE_BASED,     // Access pattern based
        HYBRID,          // Combination of strategies
        ADAPTIVE         // Automatically adapt based on workload
    };

    // Compaction configuration
    struct CompactionConfig {
        std::string table_name;
        Strategy strategy = Strategy::SIZE_BASED;
        size_t target_file_size_mb = 64;      // Target SSTable size
        size_t max_files_per_level = 10;      // Max files per level
        std::chrono::seconds time_window = std::chrono::hours(24);  // For time-based
        double hot_data_ratio = 0.1;          // Ratio of hot data to keep optimized
        bool enable_aggressive_compaction = false;  // More aggressive but CPU intensive
        size_t max_concurrent_compactions = 2;     // Max parallel compactions

        CompactionConfig() = default;
        CompactionConfig(const std::string& table)
            : table_name(table) {}
    };

    // Performance hints
    enum class WorkloadHint {
        READ_HEAVY,      // Optimize for read performance
        WRITE_HEAVY,     // Optimize for write throughput
        MIXED,          // Balanced read/write optimization
        TIME_SERIES,    // Time-series data patterns
        KEY_VALUE       // Simple key-value access
    };

    CompactionTuner();
    ~CompactionTuner() = default;

    // Configuration management
    Status SetCompactionConfig(const std::string& table_name, const CompactionConfig& config);
    Status SetWorkloadHint(const std::string& table_name, WorkloadHint hint);
    CompactionConfig GetCompactionConfig(const std::string& table_name) const;

    // Adaptive tuning
    Status AnalyzeWorkload(const std::string& table_name,
                          std::chrono::seconds analysis_window = std::chrono::hours(1));
    Status ApplyAdaptiveTuning(const std::string& table_name);

    // Manual tuning controls
    Status ForceCompaction(const std::string& table_name,
                          const std::vector<std::string>& input_files = {});
    Status SetCompactionPriority(const std::string& table_name, int priority);
    Status PauseCompaction(const std::string& table_name);
    Status ResumeCompaction(const std::string& table_name);

    // Performance monitoring
    struct CompactionMetrics {
        uint64_t total_compactions = 0;
        uint64_t bytes_compacted = 0;
        uint64_t compaction_time_ms = 0;
        double avg_compaction_ratio = 0.0;
        uint64_t files_created = 0;
        uint64_t files_removed = 0;
        std::chrono::system_clock::time_point last_compaction;
    };

    CompactionMetrics GetCompactionMetrics(const std::string& table_name) const;
    std::unordered_map<std::string, CompactionMetrics> GetAllCompactionMetrics() const;

    // Strategy-specific methods
    Status OptimizeForTimeSeries(const std::string& table_name,
                                std::chrono::seconds window_size = std::chrono::hours(24));
    Status OptimizeForHotData(const std::string& table_name,
                             double hot_data_percentage = 0.1);

private:
    mutable std::mutex mutex_;
    std::unordered_map<std::string, CompactionConfig> compaction_configs_;
    std::unordered_map<std::string, WorkloadHint> workload_hints_;
    std::unordered_map<std::string, CompactionMetrics> compaction_metrics_;

    // Helper functions
    Status ValidateCompactionConfig(const CompactionConfig& config) const;
    Strategy RecommendStrategy(WorkloadHint hint) const;
    Status UpdateCompactionMetrics(const std::string& table_name,
                                  const CompactionMetrics& metrics);
};

/**
 * @brief Advanced features manager combining TTL, schema evolution, and compaction tuning
 */
class AdvancedFeaturesManager {
public:
    AdvancedFeaturesManager();
    ~AdvancedFeaturesManager() = default;

    // Component access
    std::shared_ptr<TTLManager> GetTTLManager() { return ttl_manager_; }
    std::shared_ptr<SchemaEvolutionManager> GetSchemaEvolutionManager() { return schema_manager_; }
    std::shared_ptr<CompactionTuner> GetCompactionTuner() { return compaction_tuner_; }

    // Integrated operations
    Status ConfigureTableAdvancedFeatures(
        const std::string& table_name,
        const TTLManager::TTLConfig& ttl_config,
        const CompactionTuner::CompactionConfig& compaction_config,
        CompactionTuner::WorkloadHint workload_hint = CompactionTuner::WorkloadHint::MIXED
    );

    // Health checks for advanced features
    StatusWithMetrics PerformAdvancedFeaturesHealthCheck() const;

    // Global statistics
    struct AdvancedFeaturesStats {
        TTLManager::TTLStats ttl_stats;
        SchemaEvolutionManager::SchemaStats schema_stats;
        std::unordered_map<std::string, CompactionTuner::CompactionMetrics> compaction_stats;
        uint64_t total_tables_configured = 0;
        std::chrono::system_clock::time_point last_health_check;
    };

    AdvancedFeaturesStats GetAdvancedFeaturesStats() const;

private:
    std::shared_ptr<TTLManager> ttl_manager_;
    std::shared_ptr<SchemaEvolutionManager> schema_manager_;
    std::shared_ptr<CompactionTuner> compaction_tuner_;
};

// Global advanced features instance
extern std::unique_ptr<AdvancedFeaturesManager> global_advanced_features_manager;

// Initialization functions
void initializeAdvancedFeatures();
void shutdownAdvancedFeatures();

// Helper function for type compatibility checking
bool AreTypesCompatible(std::shared_ptr<arrow::DataType> old_type,
                        std::shared_ptr<arrow::DataType> new_type);

}  // namespace marble
