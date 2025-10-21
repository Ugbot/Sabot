#include <iostream>
#include <arrow/api.h>
#include <arrow/table.h>
#include <arrow/array.h>
#include <arrow/builder.h>
#include "sabot_sql/streaming/checkpoint_barrier_injector.h"
#include "sabot_sql/streaming/marbledb_integration.h"
#include "sabot_sql/streaming/checkpoint_coordinator.h"

using namespace sabot_sql::streaming;

int main() {
    std::cout << "=== Testing Checkpoint Barrier Injector ===" << std::endl;

    try {
        // 1. Test CheckpointBarrierInjector initialization
        std::cout << "\n1. Testing CheckpointBarrierInjector initialization..." << std::endl;
        
        CheckpointBarrierInjector injector;
        
        // Initialize embedded MarbleDB integration
        auto marbledb = std::make_shared<MarbleDBIntegration>();
        auto status = marbledb->Initialize("test_barrier_injector", "./test_barrier_marbledb", false);
        if (!status.ok()) {
            std::cerr << "❌ Failed to initialize embedded MarbleDB: " << status.message() << std::endl;
            return 1;
        }
        std::cout << "✅ MarbleDB integration initialized" << std::endl;
        
        // Initialize checkpoint coordinator
        auto checkpoint_coordinator = std::make_shared<CheckpointCoordinator>();
        status = checkpoint_coordinator->Initialize("test_coordinator", 30000, nullptr);
        if (!status.ok()) {
            std::cerr << "❌ Failed to initialize checkpoint coordinator: " << status.message() << std::endl;
            return 1;
        }
        std::cout << "✅ Checkpoint coordinator initialized" << std::endl;
        
        // Initialize barrier injector
        status = injector.Initialize(checkpoint_coordinator, marbledb);
        if (!status.ok()) {
            std::cerr << "❌ Failed to initialize barrier injector: " << status.message() << std::endl;
            return 1;
        }
        std::cout << "✅ Barrier injector initialized successfully" << std::endl;

        // 2. Test partition source registration
        std::cout << "\n2. Testing partition source registration..." << std::endl;
        
        std::string execution_id = "test_execution_123";
        
        // Register Kafka partition sources
        std::unordered_map<std::string, std::string> kafka_config;
        kafka_config["bootstrap.servers"] = "localhost:9092";
        kafka_config["group.id"] = "sabot_sql_group";
        kafka_config["topic"] = "trades";
        
        status = injector.RegisterPartitionSource(execution_id, "trades_partition_0", "kafka", kafka_config);
        if (!status.ok()) {
            std::cerr << "❌ Failed to register Kafka partition: " << status.message() << std::endl;
            return 1;
        }
        std::cout << "✅ Kafka partition registered" << std::endl;
        
        status = injector.RegisterPartitionSource(execution_id, "trades_partition_1", "kafka", kafka_config);
        if (!status.ok()) {
            std::cerr << "❌ Failed to register second Kafka partition: " << status.message() << std::endl;
            return 1;
        }
        std::cout << "✅ Second Kafka partition registered" << std::endl;
        
        // Register file partition source
        std::unordered_map<std::string, std::string> file_config;
        file_config["file_path"] = "/data/streaming/file1.csv";
        file_config["format"] = "csv";
        
        status = injector.RegisterPartitionSource(execution_id, "file_partition_1", "file", file_config);
        if (!status.ok()) {
            std::cerr << "❌ Failed to register file partition: " << status.message() << std::endl;
            return 1;
        }
        std::cout << "✅ File partition registered" << std::endl;

        // 3. Test barrier injection
        std::cout << "\n3. Testing barrier injection..." << std::endl;
        
        CheckpointBarrierInjector::BarrierConfig barrier_config;
        barrier_config.checkpoint_id = 1;
        barrier_config.execution_id = execution_id;
        barrier_config.partition_sources = {"trades_partition_0", "trades_partition_1", "file_partition_1"};
        barrier_config.timeout_ms = 30000;
        barrier_config.enable_state_snapshots = true;
        barrier_config.enable_offset_commits = true;
        
        auto checkpoint_id_result = injector.InjectBarrier(barrier_config);
        if (!checkpoint_id_result.ok()) {
            std::cerr << "❌ Failed to inject barrier: " << checkpoint_id_result.status().message() << std::endl;
            return 1;
        }
        
        int64_t checkpoint_id = checkpoint_id_result.ValueOrDie();
        std::cout << "✅ Barrier injected with checkpoint ID: " << checkpoint_id << std::endl;

        // 4. Test barrier status tracking
        std::cout << "\n4. Testing barrier status tracking..." << std::endl;
        
        auto status_result = injector.GetBarrierStatus(checkpoint_id);
        if (!status_result.ok()) {
            std::cerr << "❌ Failed to get barrier status: " << status_result.status().message() << std::endl;
            return 1;
        }
        
        auto barrier_status = status_result.ValueOrDie();
        std::cout << "✅ Barrier status retrieved:" << std::endl;
        std::cout << "   Checkpoint ID: " << barrier_status.checkpoint_id << std::endl;
        std::cout << "   Execution ID: " << barrier_status.execution_id << std::endl;
        std::cout << "   Is completed: " << (barrier_status.is_completed ? "true" : "false") << std::endl;
        std::cout << "   Has failed: " << (barrier_status.has_failed ? "true" : "false") << std::endl;
        std::cout << "   Partition count: " << barrier_status.partition_status.size() << std::endl;

        // 5. Test barrier alignment wait
        std::cout << "\n5. Testing barrier alignment wait..." << std::endl;
        
        status = injector.WaitForBarrierAlignment(checkpoint_id, 5000);
        if (!status.ok()) {
            std::cerr << "❌ Barrier alignment failed: " << status.message() << std::endl;
            return 1;
        }
        std::cout << "✅ Barrier alignment completed" << std::endl;

        // 6. Test state snapshot
        std::cout << "\n6. Testing state snapshot..." << std::endl;
        
        std::unordered_map<std::string, std::string> operator_state;
        operator_state["window_state"] = "{\"AAPL\": {\"count\": 1000, \"sum\": 150000.0}}";
        operator_state["watermark"] = "100500";
        operator_state["last_processed_offset"] = "12345";
        
        status = injector.SnapshotOperatorState(checkpoint_id, "window_agg_1", operator_state);
        if (!status.ok()) {
            std::cerr << "❌ Failed to snapshot operator state: " << status.message() << std::endl;
            return 1;
        }
        std::cout << "✅ Operator state snapshotted" << std::endl;
        
        // Restore operator state
        auto restored_state_result = injector.RestoreOperatorState(checkpoint_id, "window_agg_1");
        if (!restored_state_result.ok()) {
            std::cerr << "❌ Failed to restore operator state: " << restored_state_result.status().message() << std::endl;
            return 1;
        }
        
        auto restored_state = restored_state_result.ValueOrDie();
        std::cout << "✅ Operator state restored:" << std::endl;
        for (const auto& [key, value] : restored_state) {
            std::cout << "   " << key << ": " << value << std::endl;
        }

        // 7. Test offset management
        std::cout << "\n7. Testing offset management..." << std::endl;
        
        std::unordered_map<std::string, int64_t> partition_offsets;
        partition_offsets["trades_partition_0"] = 12345;
        partition_offsets["trades_partition_1"] = 67890;
        partition_offsets["file_partition_1"] = 100000;
        
        status = injector.CommitPartitionOffsets(checkpoint_id, partition_offsets);
        if (!status.ok()) {
            std::cerr << "❌ Failed to commit partition offsets: " << status.message() << std::endl;
            return 1;
        }
        std::cout << "✅ Partition offsets committed" << std::endl;
        
        // Get committed offsets
        auto offsets_result = injector.GetCommittedOffsets(checkpoint_id);
        if (!offsets_result.ok()) {
            std::cerr << "❌ Failed to get committed offsets: " << offsets_result.status().message() << std::endl;
            return 1;
        }
        
        auto committed_offsets = offsets_result.ValueOrDie();
        std::cout << "✅ Committed offsets retrieved:" << std::endl;
        for (const auto& [partition_id, offset] : committed_offsets) {
            std::cout << "   " << partition_id << ": " << offset << std::endl;
        }

        // 8. Test active barriers
        std::cout << "\n8. Testing active barriers..." << std::endl;
        
        auto active_barriers_result = injector.GetActiveBarriers();
        if (!active_barriers_result.ok()) {
            std::cerr << "❌ Failed to get active barriers: " << active_barriers_result.status().message() << std::endl;
            return 1;
        }
        
        auto active_barriers = active_barriers_result.ValueOrDie();
        std::cout << "✅ Found " << active_barriers.size() << " active barriers" << std::endl;

        // 9. Test barrier completion
        std::cout << "\n9. Testing barrier completion..." << std::endl;
        
        auto is_completed_result = injector.IsBarrierCompleted(checkpoint_id);
        if (!is_completed_result.ok()) {
            std::cerr << "❌ Failed to check barrier completion: " << is_completed_result.status().message() << std::endl;
            return 1;
        }
        
        bool is_completed = is_completed_result.ValueOrDie();
        std::cout << "✅ Barrier completion status: " << (is_completed ? "completed" : "pending") << std::endl;

        // 10. Test partition source unregistration
        std::cout << "\n10. Testing partition source unregistration..." << std::endl;
        
        status = injector.UnregisterPartitionSource(execution_id, "trades_partition_0");
        if (!status.ok()) {
            std::cerr << "❌ Failed to unregister partition: " << status.message() << std::endl;
            return 1;
        }
        std::cout << "✅ Partition unregistered" << std::endl;

        // 11. Test barrier cancellation
        std::cout << "\n11. Testing barrier cancellation..." << std::endl;
        
        status = injector.CancelBarrier(checkpoint_id);
        if (!status.ok()) {
            std::cerr << "❌ Failed to cancel barrier: " << status.message() << std::endl;
            return 1;
        }
        std::cout << "✅ Barrier cancelled" << std::endl;

        // 12. Test recovery
        std::cout << "\n12. Testing recovery..." << std::endl;
        
        status = injector.RecoverFromCheckpoint(checkpoint_id);
        if (!status.ok()) {
            std::cerr << "❌ Failed to recover from checkpoint: " << status.message() << std::endl;
            return 1;
        }
        std::cout << "✅ Recovery completed" << std::endl;
        
        // Get available checkpoints
        auto checkpoints_result = injector.GetAvailableCheckpoints();
        if (!checkpoints_result.ok()) {
            std::cerr << "❌ Failed to get available checkpoints: " << checkpoints_result.status().message() << std::endl;
            return 1;
        }
        
        auto available_checkpoints = checkpoints_result.ValueOrDie();
        std::cout << "✅ Found " << available_checkpoints.size() << " available checkpoints" << std::endl;

        // 13. Test shutdown
        std::cout << "\n13. Testing shutdown..." << std::endl;
        
        status = injector.Shutdown();
        if (!status.ok()) {
            std::cerr << "❌ Failed to shutdown injector: " << status.message() << std::endl;
            return 1;
        }
        std::cout << "✅ Injector shutdown successfully" << std::endl;

        std::cout << "\n=== Checkpoint Barrier Injector Test Complete ===" << std::endl;
        std::cout << "✅ All tests passed!" << std::endl;

    } catch (const std::exception& e) {
        std::cerr << "❌ Test failed with exception: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
