/**
 * MarbleDB Advanced Features Test Suite
 *
 * Tests comprehensive advanced features including:
 * - Time To Live (TTL) for automatic data expiration
 * - Schema Evolution for online schema changes
 * - Compaction Tuning for performance optimization
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <marble/ttl.h>
#include <marble/status.h>
#include <marble/db.h>

#include <thread>
#include <chrono>
#include <filesystem>

namespace marble {
namespace test {

using ::testing::_;
using ::testing::Return;
using ::testing::Invoke;
using ::testing::NiceMock;

class AdvancedFeaturesTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Initialize monitoring and advanced features
        initializeMetrics();
        initializeLogging(Logger::Level::DEBUG);
        initializeAdvancedFeatures();

        // Create fresh managers for each test
        ttl_manager_ = std::make_shared<TTLManager>();
        schema_manager_ = std::make_shared<SchemaEvolutionManager>();
        compaction_tuner_ = std::make_shared<CompactionTuner>();
        advanced_manager_ = std::make_shared<AdvancedFeaturesManager>();
    }

    void TearDown() override {
        // Clean up managers
        ttl_manager_.reset();
        schema_manager_.reset();
        compaction_tuner_.reset();
        advanced_manager_.reset();

        // Shutdown global instances
        shutdownAdvancedFeatures();
        shutdownMonitoring();
    }

    std::shared_ptr<TTLManager> ttl_manager_;
    std::shared_ptr<SchemaEvolutionManager> schema_manager_;
    std::shared_ptr<CompactionTuner> compaction_tuner_;
    std::shared_ptr<AdvancedFeaturesManager> advanced_manager_;
};

/**
 * Test TTLManager basic functionality
 */
TEST_F(AdvancedFeaturesTest, TTLManagerBasicOperations) {
    // Test adding TTL config
    TTLManager::TTLConfig config("test_table", "timestamp", std::chrono::hours(24));
    auto status = ttl_manager_->AddTTLConfig(config);
    EXPECT_TRUE(status.ok());

    // Test getting all configs
    auto configs = ttl_manager_->GetAllTTLConfigs();
    EXPECT_EQ(configs.size(), 1);
    EXPECT_EQ(configs[0].table_name, "test_table");
    EXPECT_EQ(configs[0].timestamp_column, "timestamp");
    EXPECT_EQ(configs[0].ttl_seconds, std::chrono::hours(24));

    // Test updating config
    config.ttl_seconds = std::chrono::hours(48);
    status = ttl_manager_->UpdateTTLConfig(config);
    EXPECT_TRUE(status.ok());

    configs = ttl_manager_->GetAllTTLConfigs();
    EXPECT_EQ(configs[0].ttl_seconds, std::chrono::hours(48));

    // Test removing config
    status = ttl_manager_->RemoveTTLConfig("test_table");
    EXPECT_TRUE(status.ok());

    configs = ttl_manager_->GetAllTTLConfigs();
    EXPECT_EQ(configs.size(), 0);
}

/**
 * Test TTL cleanup operations
 */
TEST_F(AdvancedFeaturesTest, TTLCleanupOperations) {
    // Add TTL config
    TTLManager::TTLConfig config("test_table", "timestamp", std::chrono::hours(24));
    auto status = ttl_manager_->AddTTLConfig(config);
    EXPECT_TRUE(status.ok());

    // Mark expired records
    auto current_time = std::chrono::system_clock::now();
    status = ttl_manager_->MarkExpiredRecords("test_table", current_time);
    EXPECT_TRUE(status.ok());

    // Cleanup expired records
    status = ttl_manager_->CleanupExpiredRecords("test_table");
    EXPECT_TRUE(status.ok());

    // Check stats
    auto stats = ttl_manager_->GetStats();
    EXPECT_GE(stats.total_records_expired, 0);
    EXPECT_GE(stats.total_records_cleaned, 0);
}

/**
 * Test TTL background cleanup
 */
TEST_F(AdvancedFeaturesTest, TTLBackgroundCleanup) {
    // Add TTL config
    TTLManager::TTLConfig config("test_table", "timestamp", std::chrono::hours(24));
    auto status = ttl_manager_->AddTTLConfig(config);
    EXPECT_TRUE(status.ok());

    // Start background cleanup
    ttl_manager_->StartBackgroundCleanup();
    EXPECT_TRUE(ttl_manager_->IsBackgroundCleanupRunning());

    // Let it run for a short time
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Stop background cleanup
    ttl_manager_->StopBackgroundCleanup();
    EXPECT_FALSE(ttl_manager_->IsBackgroundCleanupRunning());
}

/**
 * Test TTL error handling
 */
TEST_F(AdvancedFeaturesTest, TTLErrorHandling) {
    // Test adding config for non-existent table
    TTLManager::TTLConfig config("", "timestamp", std::chrono::hours(24));
    auto status = ttl_manager_->AddTTLConfig(config);
    EXPECT_FALSE(status.ok());  // Should fail due to empty table name

    // Test updating non-existent config
    status = ttl_manager_->UpdateTTLConfig(config);
    EXPECT_FALSE(status.ok());

    // Test removing non-existent config
    status = ttl_manager_->RemoveTTLConfig("non_existent_table");
    EXPECT_FALSE(status.ok());

    // Test operations on non-existent table
    status = ttl_manager_->MarkExpiredRecords("non_existent_table", std::chrono::system_clock::now());
    EXPECT_FALSE(status.ok());
}

/**
 * Test SchemaEvolutionManager basic operations
 */
TEST_F(AdvancedFeaturesTest, SchemaEvolutionBasicOperations) {
    // Test adding a column
    auto status = schema_manager_->AddColumn("test_table", "new_column", arrow::int64());
    EXPECT_TRUE(status.ok());

    // Test dropping a column
    status = schema_manager_->DropColumn("test_table", "old_column");
    EXPECT_TRUE(status.ok());

    // Test renaming a column
    status = schema_manager_->RenameColumn("test_table", "old_name", "new_name");
    EXPECT_TRUE(status.ok());

    // Test modifying column type
    status = schema_manager_->ModifyColumnType("test_table", "column", arrow::utf8());
    EXPECT_TRUE(status.ok());

    // Check change history
    auto history = schema_manager_->GetChangeHistory("test_table");
    EXPECT_EQ(history.size(), 4);

    // Check stats
    auto stats = schema_manager_->GetStats();
    EXPECT_EQ(stats.total_changes, 4);
    EXPECT_EQ(stats.changes_per_table["test_table"], 4);
}

/**
 * Test schema compatibility checking
 */
TEST_F(AdvancedFeaturesTest, SchemaCompatibility) {
    // Create test schemas
    std::vector<std::shared_ptr<arrow::Field>> old_fields = {
        arrow::field("id", arrow::int64()),
        arrow::field("name", arrow::utf8())
    };
    auto old_schema = std::make_shared<arrow::Schema>(old_fields);

    std::vector<std::shared_ptr<arrow::Field>> new_fields = {
        arrow::field("id", arrow::int64()),
        arrow::field("name", arrow::utf8()),
        arrow::field("age", arrow::int32())
    };
    auto new_schema = std::make_shared<arrow::Schema>(new_fields);

    // Test compatibility (should pass)
    auto status = schema_manager_->CheckSchemaCompatibility("test_table", *new_schema);
    EXPECT_TRUE(status.ok());

    // Test migration
    status = schema_manager_->MigrateDataToNewSchema("test_table", *old_schema, *new_schema);
    EXPECT_TRUE(status.ok());
}

/**
 * Test schema rollback functionality
 */
TEST_F(AdvancedFeaturesTest, SchemaRollback) {
    // Add a column
    auto status = schema_manager_->AddColumn("test_table", "test_column", arrow::int64());
    EXPECT_TRUE(status.ok());

    // Check history before rollback
    auto history = schema_manager_->GetChangeHistory("test_table");
    EXPECT_EQ(history.size(), 1);

    // Rollback the change
    status = schema_manager_->RollbackLastChange("test_table");
    EXPECT_TRUE(status.ok());

    // Check history after rollback
    history = schema_manager_->GetChangeHistory("test_table");
    EXPECT_EQ(history.size(), 2);  // Original + rollback

    // Test rollback on empty history
    status = schema_manager_->RollbackLastChange("empty_table");
    EXPECT_FALSE(status.ok());
}

/**
 * Test CompactionTuner basic operations
 */
TEST_F(AdvancedFeaturesTest, CompactionTunerBasicOperations) {
    // Test setting compaction config
    CompactionTuner::CompactionConfig config("test_table");
    config.target_file_size_mb = 128;
    config.max_files_per_level = 8;

    auto status = compaction_tuner_->SetCompactionConfig("test_table", config);
    EXPECT_TRUE(status.ok());

    // Test getting config
    auto retrieved_config = compaction_tuner_->GetCompactionConfig("test_table");
    EXPECT_EQ(retrieved_config.table_name, "test_table");
    EXPECT_EQ(retrieved_config.target_file_size_mb, 128);
    EXPECT_EQ(retrieved_config.max_files_per_level, 8);

    // Test setting workload hint
    status = compaction_tuner_->SetWorkloadHint("test_table", CompactionTuner::WorkloadHint::READ_HEAVY);
    EXPECT_TRUE(status.ok());
}

/**
 * Test compaction tuning strategies
 */
TEST_F(AdvancedFeaturesTest, CompactionStrategies) {
    // Test time series optimization
    auto status = compaction_tuner_->OptimizeForTimeSeries("time_series_table", std::chrono::hours(24));
    EXPECT_TRUE(status.ok());

    // Test hot data optimization
    status = compaction_tuner_->OptimizeForHotData("hot_data_table", 0.3);
    EXPECT_TRUE(status.ok());

    // Verify configs were set correctly
    auto time_config = compaction_tuner_->GetCompactionConfig("time_series_table");
    EXPECT_EQ(time_config.strategy, CompactionTuner::Strategy::TIME_BASED);
    EXPECT_EQ(time_config.time_window, std::chrono::hours(24));

    auto hot_config = compaction_tuner_->GetCompactionConfig("hot_data_table");
    EXPECT_EQ(hot_config.strategy, CompactionTuner::Strategy::USAGE_BASED);
    EXPECT_EQ(hot_config.hot_data_ratio, 0.3);
}

/**
 * Test adaptive tuning
 */
TEST_F(AdvancedFeaturesTest, AdaptiveTuning) {
    // Set workload hint first
    auto status = compaction_tuner_->SetWorkloadHint("test_table", CompactionTuner::WorkloadHint::READ_HEAVY);
    EXPECT_TRUE(status.ok());

    // Set initial config
    CompactionTuner::CompactionConfig config("test_table");
    status = compaction_tuner_->SetCompactionConfig("test_table", config);
    EXPECT_TRUE(status.ok());

    // Analyze workload
    status = compaction_tuner_->AnalyzeWorkload("test_table", std::chrono::hours(1));
    EXPECT_TRUE(status.ok());

    // Apply adaptive tuning
    status = compaction_tuner_->ApplyAdaptiveTuning("test_table");
    EXPECT_TRUE(status.ok());

    // Verify tuning was applied
    auto tuned_config = compaction_tuner_->GetCompactionConfig("test_table");
    EXPECT_EQ(tuned_config.max_files_per_level, 5);  // Should be optimized for reads
    EXPECT_FALSE(tuned_config.enable_aggressive_compaction);
}

/**
 * Test compaction control operations
 */
TEST_F(AdvancedFeaturesTest, CompactionControl) {
    // Force compaction
    std::vector<std::string> input_files = {"file1.sst", "file2.sst", "file3.sst"};
    auto status = compaction_tuner_->ForceCompaction("test_table", input_files);
    EXPECT_TRUE(status.ok());

    // Set compaction priority
    status = compaction_tuner_->SetCompactionPriority("test_table", 5);
    EXPECT_TRUE(status.ok());

    // Pause compaction
    status = compaction_tuner_->PauseCompaction("test_table");
    EXPECT_TRUE(status.ok());

    // Resume compaction
    status = compaction_tuner_->ResumeCompaction("test_table");
    EXPECT_TRUE(status.ok());

    // Check metrics were updated
    auto metrics = compaction_tuner_->GetCompactionMetrics("test_table");
    EXPECT_EQ(metrics.total_compactions, 1);
}

/**
 * Test compaction metrics
 */
TEST_F(AdvancedFeaturesTest, CompactionMetrics) {
    // Force a compaction to generate metrics
    auto status = compaction_tuner_->ForceCompaction("test_table");
    EXPECT_TRUE(status.ok());

    // Get metrics for the table
    auto metrics = compaction_tuner_->GetCompactionMetrics("test_table");
    EXPECT_EQ(metrics.total_compactions, 1);

    // Get all metrics
    auto all_metrics = compaction_tuner_->GetAllCompactionMetrics();
    EXPECT_TRUE(all_metrics.find("test_table") != all_metrics.end());
}

/**
 * Test AdvancedFeaturesManager integration
 */
TEST_F(AdvancedFeaturesTest, AdvancedFeaturesManagerIntegration) {
    // Configure advanced features for a table
    TTLManager::TTLConfig ttl_config("integrated_table", "timestamp", std::chrono::hours(24));

    CompactionTuner::CompactionConfig compaction_config("integrated_table");
    compaction_config.strategy = CompactionTuner::Strategy::ADAPTIVE;

    auto status = advanced_manager_->ConfigureTableAdvancedFeatures(
        "integrated_table", ttl_config, compaction_config,
        CompactionTuner::WorkloadHint::MIXED
    );
    EXPECT_TRUE(status.ok());

    // Perform health check
    auto health_check = advanced_manager_->PerformAdvancedFeaturesHealthCheck();
    EXPECT_TRUE(health_check.ok());

    // Get advanced features stats
    auto stats = advanced_manager_->GetAdvancedFeaturesStats();
    EXPECT_GE(stats.ttl_stats.total_records_expired, 0);
    EXPECT_GE(stats.schema_stats.total_changes, 0);
    EXPECT_TRUE(stats.compaction_stats.find("integrated_table") != stats.compaction_stats.end());
}

/**
 * Test configuration validation
 */
TEST_F(AdvancedFeaturesTest, ConfigurationValidation) {
    // Test invalid TTL config
    TTLManager::TTLConfig invalid_ttl("", "timestamp", std::chrono::hours(24));
    auto status = ttl_manager_->AddTTLConfig(invalid_ttl);
    EXPECT_FALSE(status.ok());

    // Test invalid compaction config
    CompactionTuner::CompactionConfig invalid_compaction("test_table");
    invalid_compaction.target_file_size_mb = 0;  // Invalid
    status = compaction_tuner_->SetCompactionConfig("test_table", invalid_compaction);
    EXPECT_FALSE(status.ok());

    invalid_compaction.target_file_size_mb = 128;
    invalid_compaction.hot_data_ratio = 1.5;  // Invalid
    status = compaction_tuner_->SetCompactionConfig("test_table", invalid_compaction);
    EXPECT_FALSE(status.ok());
}

/**
 * Test concurrent access to advanced features
 */
TEST_F(AdvancedFeaturesTest, ConcurrentAccess) {
    const int num_threads = 5;
    std::vector<std::thread> threads;

    // Launch multiple threads performing operations
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([this, i]() {
            std::string table_name = "concurrent_table_" + std::to_string(i);

            // Add TTL config
            TTLManager::TTLConfig ttl_config(table_name, "timestamp", std::chrono::hours(24));
            ttl_manager_->AddTTLConfig(ttl_config);

            // Add schema change
            schema_manager_->AddColumn(table_name, "new_col", arrow::int64());

            // Set compaction config
            CompactionTuner::CompactionConfig compaction_config(table_name);
            compaction_tuner_->SetCompactionConfig(table_name, compaction_config);

            // Perform operations
            ttl_manager_->MarkExpiredRecords(table_name, std::chrono::system_clock::now());
            compaction_tuner_->ForceCompaction(table_name);
        });
    }

    // Wait for all threads to complete
    for (auto& thread : threads) {
        thread.join();
    }

    // Verify operations completed successfully
    for (int i = 0; i < num_threads; ++i) {
        std::string table_name = "concurrent_table_" + std::to_string(i);

        auto ttl_configs = ttl_manager_->GetAllTTLConfigs();
        bool found_ttl = false;
        for (const auto& config : ttl_configs) {
            if (config.table_name == table_name) {
                found_ttl = true;
                break;
            }
        }
        EXPECT_TRUE(found_ttl);

        auto history = schema_manager_->GetChangeHistory(table_name);
        EXPECT_EQ(history.size(), 1);

        auto metrics = compaction_tuner_->GetCompactionMetrics(table_name);
        EXPECT_EQ(metrics.total_compactions, 1);
    }
}

/**
 * Test statistics and metrics integration
 */
TEST_F(AdvancedFeaturesTest, StatisticsIntegration) {
    // Perform various operations to generate statistics
    TTLManager::TTLConfig ttl_config("stats_table", "timestamp", std::chrono::hours(24));
    ttl_manager_->AddTTLConfig(ttl_config);

    schema_manager_->AddColumn("stats_table", "test_col", arrow::int64());
    schema_manager_->DropColumn("stats_table", "old_col");

    CompactionTuner::CompactionConfig compaction_config("stats_table");
    compaction_tuner_->SetCompactionConfig("stats_table", compaction_config);
    compaction_tuner_->ForceCompaction("stats_table");

    // Check individual component statistics
    auto ttl_stats = ttl_manager_->GetStats();
    EXPECT_EQ(ttl_stats.per_table_expired.size(), 1);  // One table configured

    auto schema_stats = schema_manager_->GetStats();
    EXPECT_EQ(schema_stats.total_changes, 2);  // Two schema changes
    EXPECT_EQ(schema_stats.changes_per_table["stats_table"], 2);

    auto compaction_metrics = compaction_tuner_->GetCompactionMetrics("stats_table");
    EXPECT_EQ(compaction_metrics.total_compactions, 1);

    // Check integrated statistics
    auto advanced_stats = advanced_manager_->GetAdvancedFeaturesStats();
    EXPECT_EQ(advanced_stats.ttl_stats.per_table_expired.size(), 1);
    EXPECT_EQ(advanced_stats.schema_stats.total_changes, 2);
    EXPECT_TRUE(advanced_stats.compaction_stats.find("stats_table") != advanced_stats.compaction_stats.end());
}

/**
 * Test advanced features cleanup and reset
 */
TEST_F(AdvancedFeaturesTest, CleanupAndReset) {
    // Add various configurations
    TTLManager::TTLConfig ttl_config("cleanup_table", "timestamp", std::chrono::hours(24));
    ttl_manager_->AddTTLConfig(ttl_config);

    schema_manager_->AddColumn("cleanup_table", "cleanup_col", arrow::int64());

    CompactionTuner::CompactionConfig compaction_config("cleanup_table");
    compaction_tuner_->SetCompactionConfig("cleanup_table", compaction_config);

    // Reset TTL stats
    ttl_manager_->ResetStats();
    auto stats = ttl_manager_->GetStats();
    EXPECT_EQ(stats.total_records_expired, 0);
    EXPECT_EQ(stats.total_records_cleaned, 0);

    // Verify configurations still exist
    auto ttl_configs = ttl_manager_->GetAllTTLConfigs();
    EXPECT_EQ(ttl_configs.size(), 1);

    auto history = schema_manager_->GetChangeHistory("cleanup_table");
    EXPECT_EQ(history.size(), 1);

    auto retrieved_config = compaction_tuner_->GetCompactionConfig("cleanup_table");
    EXPECT_EQ(retrieved_config.table_name, "cleanup_table");
}

/**
 * Test global advanced features management
 */
TEST_F(AdvancedFeaturesTest, GlobalAdvancedFeaturesManagement) {
    // Test initialization
    initializeAdvancedFeatures();
    EXPECT_NE(global_advanced_features_manager, nullptr);

    // Test accessing components through global manager
    auto ttl_manager = global_advanced_features_manager->GetTTLManager();
    auto schema_manager = global_advanced_features_manager->GetSchemaEvolutionManager();
    auto compaction_tuner = global_advanced_features_manager->GetCompactionTuner();

    EXPECT_NE(ttl_manager, nullptr);
    EXPECT_NE(schema_manager, nullptr);
    EXPECT_NE(compaction_tuner, nullptr);

    // Test integrated configuration
    TTLManager::TTLConfig ttl_config("global_test", "timestamp", std::chrono::hours(12));
    CompactionTuner::CompactionConfig compaction_config("global_test");

    auto status = global_advanced_features_manager->ConfigureTableAdvancedFeatures(
        "global_test", ttl_config, compaction_config,
        CompactionTuner::WorkloadHint::TIME_SERIES
    );
    EXPECT_TRUE(status.ok());

    // Test health check
    auto health_check = global_advanced_features_manager->PerformAdvancedFeaturesHealthCheck();
    EXPECT_TRUE(health_check.ok());

    // Test cleanup
    shutdownAdvancedFeatures();
    EXPECT_EQ(global_advanced_features_manager, nullptr);
}

}  // namespace test
}  // namespace marble

// Main test runner
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::testing::InitGoogleMock(&argc, argv);

    return RUN_ALL_TESTS();
}

