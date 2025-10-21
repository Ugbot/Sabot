#include <iostream>
#include <arrow/api.h>
#include "sabot_sql/streaming/checkpoint_coordinator.h"

using namespace sabot_sql::streaming;

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

int main() {
    std::cout << "=== Testing Checkpoint Coordinator (Simple) ===" << std::endl;
    
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
    
    // ========== Cleanup ==========
    std::cout << "\n2. Cleaning up..." << std::endl;
    
    if (!coordinator.Shutdown().ok()) {
        std::cerr << "❌ Failed to shutdown coordinator" << std::endl;
        return 1;
    }
    
    std::cout << "✅ Cleanup complete" << std::endl;
    
    std::cout << "\n=== Checkpoint Coordinator Test Complete ===" << std::endl;
    std::cout << "✅ All tests passed!" << std::endl;
    
    return 0;
}
