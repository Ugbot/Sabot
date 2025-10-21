#include <iostream>
#include <arrow/api.h>
#include "sabot_sql/streaming/marbledb_integration.h"
#include "sabot_sql/streaming/morsel_plan_extensions.h"

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

int main() {
    std::cout << "=== Testing MarbleDB Integration and MorselPlan Extensions ===" << std::endl;
    
    // ========== Test MarbleDB Integration ==========
    std::cout << "\n1. Testing MarbleDB Integration..." << std::endl;
    
    MarbleDBIntegration integration;
    
    // Initialize embedded integration
    auto init_result = integration.Initialize("test_integration", "./test_marbledb", false);
    if (!init_result.ok()) {
        std::cerr << "❌ Failed to initialize embedded integration: " << init_result.message() << std::endl;
        return 1;
    }
    std::cout << "✅ MarbleDB integration initialized" << std::endl;
    
    // Create test table schema
    auto schema = arrow::schema({
        arrow::field("id", arrow::int64()),
        arrow::field("name", arrow::utf8()),
        arrow::field("value", arrow::float64()),
        arrow::field("timestamp", arrow::timestamp(arrow::TimeUnit::MILLI))
    });
    
    // Create table configuration
    MarbleDBIntegration::TableConfig table_config("test_table", schema, true);  // RAFT replicated
    table_config.description = "Test table for streaming SQL";
    
    // Create table
    if (!integration.CreateTable(table_config).ok()) {
        std::cerr << "❌ Failed to create table" << std::endl;
        return 1;
    }
    std::cout << "✅ Created table 'test_table'" << std::endl;
    
    // Check if table exists
    auto exists_result = integration.TableExists("test_table");
    if (!exists_result.ok()) {
        std::cerr << "❌ Failed to check table existence" << std::endl;
        return 1;
    }
    
    bool exists = exists_result.ValueOrDie();
    std::cout << "✅ Table exists: " << (exists ? "yes" : "no") << std::endl;
    
    // Get table schema
    auto schema_result = integration.GetTableSchema("test_table");
    if (!schema_result.ok()) {
        std::cerr << "❌ Failed to get table schema" << std::endl;
        return 1;
    }
    
    auto retrieved_schema = schema_result.ValueOrDie();
    std::cout << "✅ Retrieved table schema: " << retrieved_schema->num_fields() << " fields" << std::endl;
    
    // Test RAFT operations
    auto raft_result = integration.IsTableRaftReplicated("test_table");
    if (!raft_result.ok()) {
        std::cerr << "❌ Failed to check RAFT replication" << std::endl;
        return 1;
    }
    
    bool is_raft = raft_result.ValueOrDie();
    std::cout << "✅ Table is RAFT replicated: " << (is_raft ? "yes" : "no") << std::endl;
    
    // Test checkpoint operations
    std::unordered_map<std::string, std::string> state_snapshots;
    state_snapshots["operator_1"] = "{\"count\": 100, \"sum\": 1000.0}";
    state_snapshots["operator_2"] = "{\"window_count\": 5, \"last_watermark\": 1000}";
    
    if (!integration.CreateCheckpoint(1, state_snapshots).ok()) {
        std::cerr << "❌ Failed to create checkpoint" << std::endl;
        return 1;
    }
    std::cout << "✅ Created checkpoint 1" << std::endl;
    
    // Commit checkpoint
    if (!integration.CommitCheckpoint(1).ok()) {
        std::cerr << "❌ Failed to commit checkpoint" << std::endl;
        return 1;
    }
    std::cout << "✅ Committed checkpoint 1" << std::endl;
    
    // Get last committed checkpoint
    auto last_checkpoint_result = integration.GetLastCommittedCheckpoint();
    if (!last_checkpoint_result.ok()) {
        std::cerr << "❌ Failed to get last checkpoint" << std::endl;
        return 1;
    }
    
    int64_t last_checkpoint = last_checkpoint_result.ValueOrDie();
    std::cout << "✅ Last committed checkpoint: " << last_checkpoint << std::endl;
    
    // Test timer operations
    MarbleDBIntegration::TimerConfig timer_config("test_timer", 1000, "callback_data");
    timer_config.is_recurring = false;
    
    if (!integration.RegisterTimer(timer_config).ok()) {
        std::cerr << "❌ Failed to register timer" << std::endl;
        return 1;
    }
    std::cout << "✅ Registered timer 'test_timer'" << std::endl;
    
    // Get integration statistics
    auto stats = integration.GetStats();
    std::cout << "✅ Integration stats:" << std::endl;
    std::cout << "   Total tables: " << stats.total_tables << std::endl;
    std::cout << "   RAFT replicated: " << stats.raft_replicated_tables << std::endl;
    std::cout << "   Local tables: " << stats.local_tables << std::endl;
    std::cout << "   Active timers: " << stats.active_timers << std::endl;
    std::cout << "   Last checkpoint: " << stats.last_checkpoint_id << std::endl;
    
    // ========== Test MorselPlan Extensions ==========
    std::cout << "\n2. Testing MorselPlan Extensions..." << std::endl;
    
    StreamingPlanBuilder builder;
    
    // Build a streaming plan
    auto plan_result = builder
        .SetQueryId("test_query_123")
        .SetQueryText("SELECT symbol, COUNT(*) FROM trades GROUP BY TUMBLE(timestamp, 1h)")
        .SetMaxParallelism(8)
        .SetCheckpointInterval("30s")
        .SetStateBackend("marbledb")
        .SetTimerBackend("marbledb")
        .AddKafkaSource("trades", 0, "timestamp")
        .AddKafkaSource("trades", 1, "timestamp")
        .AddTumbleWindow("timestamp", "1h", {"symbol"})
        .AddWindowState("window_agg_1", "window_state_query_123", false)
        .AddDimensionTable("securities", "s", true)
        .AddKafkaPartitionSource("kafka_source_0", "trades", 0)
        .AddKafkaPartitionSource("kafka_source_1", "trades", 1)
        .AddWindowAggregateOperator("window_agg_1", "TUMBLE", "1h", "timestamp")
        .AddBroadcastJoinOperator("join_1", "securities", {"symbol"})
        .AddCheckpointOperator("checkpoint_1", "30s")
        .AddKafkaOutput("results", {{"bootstrap.servers", "localhost:9092"}})
        .Build();
    
    if (!plan_result.ok()) {
        std::cerr << "❌ Failed to build streaming plan: " << plan_result.status().message() << std::endl;
        return 1;
    }
    
    auto plan = plan_result.ValueOrDie();
    std::cout << "✅ Built streaming plan successfully" << std::endl;
    
    // Validate the plan
    if (!plan.Validate().ok()) {
        std::cerr << "❌ Plan validation failed" << std::endl;
        return 1;
    }
    std::cout << "✅ Plan validation passed" << std::endl;
    
    // Print plan summary
    std::cout << "✅ Plan summary:" << std::endl;
    std::cout << plan.GetSummary() << std::endl;
    
    // Test plan accessors
    auto kafka_source = plan.GetStreamingSource("trades");
    if (kafka_source) {
        std::cout << "✅ Found Kafka source for topic 'trades'" << std::endl;
        std::cout << "   Partition: " << kafka_source->partition_id << std::endl;
        std::cout << "   Watermark column: " << kafka_source->watermark_column << std::endl;
    }
    
    auto window_op = plan.GetOperator("window_agg_1");
    if (window_op) {
        std::cout << "✅ Found window operator 'window_agg_1'" << std::endl;
        std::cout << "   Type: " << window_op->operator_type << std::endl;
        std::cout << "   Is stateful: " << (window_op->is_stateful ? "yes" : "no") << std::endl;
        std::cout << "   Window type: " << window_op->parameters["window_type"] << std::endl;
        std::cout << "   Window size: " << window_op->parameters["window_size"] << std::endl;
    }
    
    auto state_config = plan.GetStateConfig("window_agg_1");
    if (state_config) {
        std::cout << "✅ Found state config for 'window_agg_1'" << std::endl;
        std::cout << "   State table: " << state_config->state_table_name << std::endl;
        std::cout << "   State type: " << state_config->state_type << std::endl;
        std::cout << "   RAFT replicated: " << (state_config->is_raft_replicated ? "yes" : "no") << std::endl;
    }
    
    auto dimension_table = plan.GetDimensionTable("securities");
    if (dimension_table) {
        std::cout << "✅ Found dimension table 'securities'" << std::endl;
        std::cout << "   Alias: " << dimension_table->alias << std::endl;
        std::cout << "   RAFT replicated: " << (dimension_table->is_raft_replicated ? "yes" : "no") << std::endl;
    }
    
    // ========== Test MarbleDB State Backend ==========
    std::cout << "\n3. Testing MarbleDB State Backend..." << std::endl;
    
    MarbleDBStateBackend state_backend;
    
    // Initialize state backend
    auto integration_ptr = std::make_shared<MarbleDBIntegration>();
    auto init_status = integration_ptr->Initialize("test_integration", nullptr);
    if (!init_status.ok()) {
        std::cerr << "❌ Failed to initialize integration: " << init_status.message() << std::endl;
        return 1;
    }
    
    if (!state_backend.Initialize(integration_ptr).ok()) {
        std::cerr << "❌ Failed to initialize state backend" << std::endl;
        return 1;
    }
    std::cout << "✅ State backend initialized" << std::endl;
    
    // Test state operations
    MarbleDBStateBackend::StateKey key("window_agg_1", "AAPL", 1000, 2000);
    MarbleDBStateBackend::StateValue value({{"count", "10"}, {"sum", "1500.0"}}, 1500);
    
    if (!state_backend.SetState(key, value).ok()) {
        std::cerr << "❌ Failed to set state" << std::endl;
        return 1;
    }
    std::cout << "✅ Set state for key: " << key.ToString() << std::endl;
    
    auto get_state_result = state_backend.GetState(key);
    if (!get_state_result.ok()) {
        std::cerr << "❌ Failed to get state" << std::endl;
        return 1;
    }
    std::cout << "✅ Retrieved state successfully" << std::endl;
    
    // List state keys
    auto list_keys_result = state_backend.ListStateKeys("window_agg_1");
    if (!list_keys_result.ok()) {
        std::cerr << "❌ Failed to list state keys" << std::endl;
        return 1;
    }
    
    auto keys = list_keys_result.ValueOrDie();
    std::cout << "✅ Listed " << keys.size() << " state keys" << std::endl;
    
    // ========== Cleanup ==========
    std::cout << "\n4. Cleaning up..." << std::endl;
    
    if (!integration.Shutdown().ok()) {
        std::cerr << "❌ Failed to shutdown integration" << std::endl;
        return 1;
    }
    
    std::cout << "✅ Cleanup complete" << std::endl;
    
    std::cout << "\n=== MarbleDB Integration and MorselPlan Extensions Test Complete ===" << std::endl;
    std::cout << "✅ All tests passed!" << std::endl;
    
    return 0;
}
