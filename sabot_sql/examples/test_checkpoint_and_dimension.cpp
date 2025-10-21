#include <iostream>
#include <arrow/api.h>
#include <arrow/builder.h>
#include "sabot_sql/streaming/checkpoint_coordinator.h"
#include "sabot_sql/streaming/dimension_broadcast.h"

using namespace sabot_sql::streaming;

// Mock MarbleDB client for testing
class MockMarbleClient {
public:
    MockMarbleClient() = default;
    ~MockMarbleClient() = default;
    
    // Mock methods - in real implementation these would interface with MarbleDB
    arrow::Status CreateTable(const std::string& table_name, const arrow::Schema& schema) {
        std::cout << "Mock: Created table " << table_name << std::endl;
        return arrow::Status::OK();
    }
    
    arrow::Status WriteTable(const std::string& table_name, std::shared_ptr<arrow::Table> table) {
        std::cout << "Mock: Wrote " << table->num_rows() << " rows to " << table_name << std::endl;
        return arrow::Status::OK();
    }
    
    arrow::Result<std::shared_ptr<arrow::Table>> ReadTable(const std::string& table_name) {
        std::cout << "Mock: Reading table " << table_name << std::endl;
        return nullptr;  // Mock returns null
    }
};

// Mock checkpoint participant for testing
class MockCheckpointParticipant : public CheckpointParticipant {
public:
    MockCheckpointParticipant(const std::string& id) : operator_id_(id) {}
    
    std::string GetOperatorId() const override {
        return operator_id_;
    }
    
    arrow::Result<std::unordered_map<std::string, std::string>> SnapshotState(int64_t checkpoint_id) override {
        std::unordered_map<std::string, std::string> state;
        state["checkpoint_id"] = std::to_string(checkpoint_id);
        state["operator_id"] = operator_id_;
        state["timestamp"] = std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count());
        
        std::cout << "Mock: Snapshot state for " << operator_id_ << " at checkpoint " << checkpoint_id << std::endl;
        return state;
    }
    
    arrow::Status RestoreState(
        int64_t checkpoint_id,
        const std::unordered_map<std::string, std::string>& state_snapshot
    ) override {
        std::cout << "Mock: Restore state for " << operator_id_ << " from checkpoint " << checkpoint_id << std::endl;
        return arrow::Status::OK();
    }
    
    arrow::Status HandleCheckpointBarrier(int64_t checkpoint_id) override {
        std::cout << "Mock: Handle barrier " << checkpoint_id << " for " << operator_id_ << std::endl;
        return arrow::Status::OK();
    }
    
private:
    std::string operator_id_;
};

// Helper function to create test data
arrow::Result<std::shared_ptr<arrow::Table>> CreateTestSecuritiesTable() {
    // Create schema
    auto schema = arrow::schema({
        arrow::field("symbol", arrow::utf8()),
        arrow::field("company_name", arrow::utf8()),
        arrow::field("sector", arrow::utf8()),
        arrow::field("market_cap", arrow::float64())
    });
    
    // Create empty table for testing
    std::vector<std::shared_ptr<arrow::Array>> empty_arrays;
    return arrow::Table::Make(schema, empty_arrays);
}

int main() {
    std::cout << "=== Testing Checkpoint Coordinator and Dimension Broadcast ===" << std::endl;
    
    // Create mock MarbleDB client
    auto mock_client = std::make_shared<MockMarbleClient>();
    
    // ========== Test Checkpoint Coordinator ==========
    std::cout << "\n1. Testing Checkpoint Coordinator..." << std::endl;
    
    CheckpointCoordinator coordinator;
    
    // Initialize coordinator
    auto init_result = coordinator.Initialize("test_coordinator", 5000, nullptr);
    if (!init_result.ok()) {
        std::cerr << "❌ Failed to initialize coordinator: " << init_result.message() << std::endl;
        return 1;
    }
    std::cout << "✅ Checkpoint coordinator initialized" << std::endl;
    
    // Register participants
    auto participant1 = std::make_shared<MockCheckpointParticipant>("kafka_source_0");
    auto participant2 = std::make_shared<MockCheckpointParticipant>("window_aggregator");
    
    if (!coordinator.RegisterParticipant("kafka_source_0", "kafka_state_table").ok()) {
        std::cerr << "❌ Failed to register participant 1" << std::endl;
        return 1;
    }
    
    if (!coordinator.RegisterParticipant("window_aggregator", "window_state_table").ok()) {
        std::cerr << "❌ Failed to register participant 2" << std::endl;
        return 1;
    }
    
    std::cout << "✅ Registered 2 participants" << std::endl;
    
    // Trigger checkpoint
    auto checkpoint_result = coordinator.TriggerCheckpoint();
    if (!checkpoint_result.ok()) {
        std::cerr << "❌ Failed to trigger checkpoint: " << checkpoint_result.status().message() << std::endl;
        return 1;
    }
    
    int64_t checkpoint_id = checkpoint_result.ValueOrDie();
    std::cout << "✅ Triggered checkpoint: " << checkpoint_id << std::endl;
    
    // Acknowledge checkpoint from participants
    std::unordered_map<std::string, std::string> state1;
    state1["offset"] = "12345";
    state1["partition"] = "0";
    
    std::unordered_map<std::string, std::string> state2;
    state2["window_count"] = "5";
    state2["last_watermark"] = "1000";
    
    if (!coordinator.AcknowledgeCheckpoint("kafka_source_0", checkpoint_id, state1).ok()) {
        std::cerr << "❌ Failed to acknowledge checkpoint from participant 1" << std::endl;
        return 1;
    }
    
    if (!coordinator.AcknowledgeCheckpoint("window_aggregator", checkpoint_id, state2).ok()) {
        std::cerr << "❌ Failed to acknowledge checkpoint from participant 2" << std::endl;
        return 1;
    }
    
    std::cout << "✅ Checkpoint acknowledged by all participants" << std::endl;
    
    // Wait for checkpoint completion
    if (!coordinator.WaitForCheckpoint(checkpoint_id, 5000).ok()) {
        std::cerr << "❌ Checkpoint did not complete in time" << std::endl;
        return 1;
    }
    
    std::cout << "✅ Checkpoint completed successfully" << std::endl;
    
    // Get statistics
    auto stats = coordinator.GetStats();
    std::cout << "✅ Coordinator stats:" << std::endl;
    std::cout << "   Total checkpoints: " << stats.total_checkpoints << std::endl;
    std::cout << "   Completed checkpoints: " << stats.completed_checkpoints << std::endl;
    std::cout << "   Registered participants: " << stats.registered_participants << std::endl;
    
    // ========== Test Dimension Broadcast Manager ==========
    std::cout << "\n2. Testing Dimension Broadcast Manager..." << std::endl;
    
    DimensionBroadcastManager dim_manager;
    
    // Initialize manager with embedded MarbleDB
    if (!dim_manager.Initialize("test_dim_manager", "./test_dim_marbledb", false).ok()) {
        std::cerr << "❌ Failed to initialize dimension manager" << std::endl;
        return 1;
    }
    std::cout << "✅ Dimension broadcast manager initialized" << std::endl;
    
    // Create test securities table
    auto securities_result = CreateTestSecuritiesTable();
    if (!securities_result.ok()) {
        std::cerr << "❌ Failed to create test securities table" << std::endl;
        return 1;
    }
    
    auto securities_table = securities_result.ValueOrDie();
    std::cout << "✅ Created test securities table: " << securities_table->num_rows() << " rows" << std::endl;
    
    // Register dimension table
    if (!dim_manager.RegisterDimensionTable("securities", securities_table, true).ok()) {
        std::cerr << "❌ Failed to register dimension table" << std::endl;
        return 1;
    }
    std::cout << "✅ Registered dimension table 'securities'" << std::endl;
    
    // Check if table is suitable for broadcast
    auto suitable_result = dim_manager.IsSuitableForBroadcast("securities", 1024 * 1024);  // 1MB limit
    if (!suitable_result.ok()) {
        std::cerr << "❌ Failed to check broadcast suitability" << std::endl;
        return 1;
    }
    
    bool is_suitable = suitable_result.ValueOrDie();
    std::cout << "✅ Table suitable for broadcast: " << (is_suitable ? "yes" : "no") << std::endl;
    
    // Get dimension table
    auto table_result = dim_manager.GetDimensionTable("securities");
    if (!table_result.ok()) {
        std::cerr << "❌ Failed to get dimension table" << std::endl;
        return 1;
    }
    
    auto retrieved_table = table_result.ValueOrDie();
    std::cout << "✅ Retrieved dimension table: " << retrieved_table->num_rows() << " rows" << std::endl;
    
    // Get table metadata
    auto metadata_result = dim_manager.GetDimensionTableMetadata("securities");
    if (!metadata_result.ok()) {
        std::cerr << "❌ Failed to get table metadata" << std::endl;
        return 1;
    }
    
    auto metadata = metadata_result.ValueOrDie();
    std::cout << "✅ Table metadata:" << std::endl;
    std::cout << "   Name: " << metadata.table_name << std::endl;
    std::cout << "   Raft replicated: " << (metadata.is_raft_replicated ? "yes" : "no") << std::endl;
    std::cout << "   Row count: " << metadata.row_count << std::endl;
    std::cout << "   Column count: " << metadata.column_count << std::endl;
    
    // Get manager statistics
    auto dim_stats = dim_manager.GetStats();
    std::cout << "✅ Dimension manager stats:" << std::endl;
    std::cout << "   Total tables: " << dim_stats.total_tables << std::endl;
    std::cout << "   Raft replicated: " << dim_stats.raft_replicated_tables << std::endl;
    std::cout << "   Local tables: " << dim_stats.local_tables << std::endl;
    std::cout << "   Total rows: " << dim_stats.total_rows << std::endl;
    
    // ========== Test Dimension Table Registry ==========
    std::cout << "\n3. Testing Dimension Table Registry..." << std::endl;
    
    DimensionTableRegistry registry;
    auto manager_ptr = std::make_shared<DimensionBroadcastManager>();
    auto init_status = manager_ptr->Initialize("test_dim_manager", nullptr);
    if (!init_status.ok()) {
        std::cerr << "❌ Failed to initialize manager: " << init_status.message() << std::endl;
        return 1;
    }
    if (!registry.Initialize(manager_ptr).ok()) {
        std::cerr << "❌ Failed to initialize registry" << std::endl;
        return 1;
    }
    std::cout << "✅ Dimension table registry initialized" << std::endl;
    
    // List tables
    auto table_names = registry.ListTables();
    std::cout << "✅ Registry tables: " << table_names.size() << " tables" << std::endl;
    for (const auto& name : table_names) {
        std::cout << "   - " << name << std::endl;
    }
    
    // ========== Cleanup ==========
    std::cout << "\n4. Cleaning up..." << std::endl;
    
    if (!coordinator.Shutdown().ok()) {
        std::cerr << "❌ Failed to shutdown coordinator" << std::endl;
        return 1;
    }
    
    if (!dim_manager.Shutdown().ok()) {
        std::cerr << "❌ Failed to shutdown dimension manager" << std::endl;
        return 1;
    }
    
    std::cout << "✅ Cleanup complete" << std::endl;
    
    std::cout << "\n=== Checkpoint Coordinator and Dimension Broadcast Test Complete ===" << std::endl;
    std::cout << "✅ All tests passed!" << std::endl;
    
    return 0;
}
