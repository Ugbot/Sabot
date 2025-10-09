/************************************************************************
Copyright 2024 MarbleDB Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
**************************************************************************/

#include "marble/raft.h"
#include <iostream>
#include <memory>
#include <thread>
#include <chrono>
#include <vector>

using namespace marble;

// Simple state machine that just counts operations
class CounterStateMachine : public RaftStateMachine {
public:
    Status ApplyOperation(const RaftOperation& operation) override {
        std::lock_guard<std::mutex> lock(mutex_);
        operation_count_++;
        std::cout << "Node applied operation: " << operation.data
                  << " (total operations: " << operation_count_ << ")" << std::endl;
        return Status::OK();
    }

    Status CreateSnapshot(uint64_t log_index) override {
        std::cout << "Creating snapshot at index: " << log_index << std::endl;
        return Status::OK();
    }

    Status RestoreFromSnapshot(uint64_t log_index) override {
        std::cout << "Restoring from snapshot at index: " << log_index << std::endl;
        return Status::OK();
    }

    uint64_t GetLastAppliedIndex() const override {
        return operation_count_;
    }

    uint64_t GetOperationCount() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return operation_count_;
    }

private:
    mutable std::mutex mutex_;
    uint64_t operation_count_ = 0;
};

// Raft node implementation
class RaftNode {
public:
    RaftNode(int node_id, const std::string& endpoint, const std::vector<std::string>& all_endpoints)
        : node_id_(node_id), endpoint_(endpoint), all_endpoints_(all_endpoints) {}

    ~RaftNode() {
        Stop();
    }

    Status Start() {
        std::cout << "Starting Raft node " << node_id_ << " at " << endpoint_ << std::endl;

        // Create components
        state_machine_ = std::make_unique<CounterStateMachine>();
        log_store_ = CreateInMemoryLogStore();
        transport_ = CreateArrowFlightTransport(endpoint_);

        // Create Raft server
        raft_server_ = CreateRaftServer(
            std::move(state_machine_),
            std::move(log_store_),
            std::move(transport_)
        );

        if (!raft_server_) {
            return Status::InternalError("Failed to create Raft server");
        }

        // Configure cluster
        RaftClusterConfig config;
        config.cluster_id = "marble-distributed-cluster";
        config.node_endpoints = all_endpoints_;
        config.election_timeout_ms = 1000 + (node_id_ * 100);  // Vary timeouts slightly
        config.heartbeat_interval_ms = 100;

        // Initialize and start
        auto status = raft_server_->Initialize(config);
        if (!status.ok()) {
            return status;
        }

        status = raft_server_->Start();
        if (!status.ok()) {
            return status;
        }

        running_ = true;
        std::cout << "Raft node " << node_id_ << " started successfully" << std::endl;
        return Status::OK();
    }

    Status Stop() {
        if (!running_) return Status::OK();

        std::cout << "Stopping Raft node " << node_id_ << std::endl;
        running_ = false;

        if (raft_server_) {
            return raft_server_->Stop();
        }

        return Status::OK();
    }

    Status ProposeOperation(const std::string& data) {
        if (!raft_server_ || !running_) {
            return Status::InvalidArgument("Raft server not running");
        }

        RaftOperation operation;
        operation.type = RaftOperationType::kCustom;
        operation.data = data;
        operation.timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        operation.node_id = std::to_string(node_id_);

        return raft_server_->ProposeOperation(
            std::make_unique<RaftOperation>(operation));
    }

    void PrintStatus() {
        if (!raft_server_) return;

        std::cout << "Node " << node_id_ << " status:" << std::endl;
        std::cout << "  Role: " << raft_server_->GetRole() << std::endl;
        std::cout << "  Leader: " << raft_server_->GetLeaderEndpoint() << std::endl;
        std::cout << "  Running: " << (running_ ? "Yes" : "No") << std::endl;
        std::cout << "  Operations applied: " << GetOperationCount() << std::endl;
        std::cout << raft_server_->GetClusterStatus() << std::endl;
    }

    uint64_t GetOperationCount() const {
        if (auto* counter = dynamic_cast<CounterStateMachine*>(state_machine_.get())) {
            return counter->GetOperationCount();
        }
        return 0;
    }

private:
    int node_id_;
    std::string endpoint_;
    std::vector<std::string> all_endpoints_;
    bool running_ = false;

    std::unique_ptr<RaftStateMachine> state_machine_;
    std::unique_ptr<RaftLogStore> log_store_;
    std::unique_ptr<RaftTransport> transport_;
    std::unique_ptr<RaftServer> raft_server_;
};

void demonstrate_distributed_raft() {
    std::cout << "MarbleDB Distributed Raft Cluster Demo" << std::endl;
    std::cout << "=====================================" << std::endl;

    // Define cluster endpoints
    std::vector<std::string> endpoints = {
        "localhost:50051",
        "localhost:50052",
        "localhost:50053"
    };

    // Create and start nodes
    std::vector<std::unique_ptr<RaftNode>> nodes;
    for (size_t i = 0; i < endpoints.size(); ++i) {
        auto node = std::make_unique<RaftNode>(i + 1, endpoints[i], endpoints);
        nodes.push_back(std::move(node));
    }

    std::cout << "\nStarting cluster nodes..." << std::endl;

    // Start all nodes
    for (auto& node : nodes) {
        auto status = node->Start();
        if (!status.ok()) {
            std::cerr << "Failed to start node: " << status.ToString() << std::endl;
            return;
        }
    }

    // Wait for cluster to stabilize
    std::cout << "\nWaiting for cluster to stabilize..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(3));

    // Print initial status
    std::cout << "\nInitial cluster status:" << std::endl;
    for (auto& node : nodes) {
        node->PrintStatus();
        std::cout << std::endl;
    }

    // Propose operations from different nodes
    std::cout << "\nProposing operations..." << std::endl;

    std::vector<std::string> operations = {
        "Create table users",
        "Insert user data",
        "Update user profile",
        "Create index on email",
        "Run analytics query"
    };

    for (size_t i = 0; i < operations.size(); ++i) {
        int node_idx = i % nodes.size();
        std::cout << "Node " << (node_idx + 1) << " proposing: " << operations[i] << std::endl;

        auto status = nodes[node_idx]->ProposeOperation(operations[i]);
        if (!status.ok()) {
            std::cerr << "Failed to propose operation: " << status.ToString() << std::endl;
        }

        // Small delay between operations
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    // Wait for operations to propagate
    std::cout << "\nWaiting for operations to propagate..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // Print final status
    std::cout << "\nFinal cluster status:" << std::endl;
    for (auto& node : nodes) {
        node->PrintStatus();
        std::cout << std::endl;
    }

    // Verify consistency
    std::cout << "Verifying cluster consistency..." << std::endl;
    uint64_t expected_count = operations.size();
    bool consistent = true;

    for (auto& node : nodes) {
        uint64_t count = node->GetOperationCount();
        if (count != expected_count) {
            std::cerr << "Inconsistency detected! Node has " << count
                      << " operations, expected " << expected_count << std::endl;
            consistent = false;
        }
    }

    if (consistent) {
        std::cout << "✅ Cluster is consistent - all nodes have "
                  << expected_count << " operations" << std::endl;
    } else {
        std::cout << "❌ Cluster inconsistency detected!" << std::endl;
    }

    // Stop nodes
    std::cout << "\nStopping cluster..." << std::endl;
    for (auto& node : nodes) {
        node->Stop();
    }

    std::cout << "\nDistributed Raft demo completed!" << std::endl;
    std::cout << "\nKey features demonstrated:" << std::endl;
    std::cout << "• Multi-node cluster setup" << std::endl;
    std::cout << "• Leader election and consensus" << std::endl;
    std::cout << "• Operation replication across nodes" << std::endl;
    std::cout << "• Arrow Flight transport layer" << std::endl;
    std::cout << "• Fault-tolerant distributed state" << std::endl;
}

int main(int argc, char** argv) {
    try {
        demonstrate_distributed_raft();
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Exception: " << e.what() << std::endl;
        return 1;
    }
}
