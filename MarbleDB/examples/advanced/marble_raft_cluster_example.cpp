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
#include "marble/wal.h"
#include "marble/task_scheduler.h"
#include <iostream>
#include <memory>
#include <thread>
#include <chrono>
#include <vector>
#include <atomic>

using namespace marble;

// MarbleDB Raft Node with WAL and Schema state machines
class MarbleRaftNode {
public:
    MarbleRaftNode(int node_id, const std::string& endpoint,
                   const std::vector<std::string>& all_endpoints,
                   const std::string& data_path)
        : node_id_(node_id)
        , endpoint_(endpoint)
        , all_endpoints_(all_endpoints)
        , data_path_(data_path)
        , running_(false) {}

    ~MarbleRaftNode() {
        Stop();
    }

    Status Start() {
        std::cout << "Starting MarbleDB Raft node " << node_id_ << " at " << endpoint_ << std::endl;

        try {
            // Create task scheduler for WAL operations
            task_scheduler_ = std::make_unique<TaskScheduler>(4);

            // Create WAL manager with proper configuration
            wal_manager_ = CreateWalManager(task_scheduler_.get());

            // Configure WAL options
            WalOptions wal_options;
            wal_options.wal_path = data_path_ + "/wal";
            wal_options.sync_mode = WalOptions::SyncMode::kBatch;
            wal_options.enable_checksum = true;

            // Open WAL manager
            auto status = wal_manager_->Open(wal_options);
            if (!status.ok()) {
                std::cerr << "Failed to open WAL manager: " << status.ToString() << std::endl;
                return status;
            }

            // Create state machines
            wal_state_machine_ = CreateMarbleWalStateMachine(std::move(wal_manager_));
            schema_state_machine_ = CreateMarbleSchemaStateMachine();

            // For this demo, we'll use the WAL state machine as the primary
            // In a real implementation, you might have multiple state machines
            // or a composite state machine

            // Create persistent log store
            log_store_ = CreateMarbleLogStore(data_path_ + "/raft_logs");

            // Create Arrow Flight transport
            transport_ = CreateArrowFlightTransport(endpoint_);

            // Create Raft server with WAL state machine
            raft_server_ = CreateRaftServer(
                std::move(wal_state_machine_),
                std::move(log_store_),
                std::move(transport_)
            );

            if (!raft_server_) {
                return Status::InternalError("Failed to create Raft server");
            }

            // Configure cluster
            RaftClusterConfig config;
            config.cluster_id = "marble-cluster";
            config.node_endpoints = all_endpoints_;
            config.election_timeout_ms = 1000 + (node_id_ * 100);
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
            std::cout << "MarbleDB Raft node " << node_id_ << " started successfully" << std::endl;
            return Status::OK();

        } catch (const std::exception& e) {
            return Status::InternalError(std::string("Failed to start MarbleDB node: ") + e.what());
        }
    }

    Status Stop() {
        if (!running_) return Status::OK();

        std::cout << "Stopping MarbleDB Raft node " << node_id_ << std::endl;
        running_ = false;

        if (raft_server_) {
            return raft_server_->Stop();
        }

        return Status::OK();
    }

    // WAL Operations
    Status ReplicateWalEntry(const std::string& operation_data) {
        if (!raft_server_ || !running_) {
            return Status::InvalidArgument("Raft server not running");
        }

        RaftOperation operation;
        operation.type = RaftOperationType::kWalEntry;
        operation.data = operation_data;
        operation.timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        operation.node_id = std::to_string(node_id_);

        return raft_server_->ProposeOperation(
            std::make_unique<RaftOperation>(operation));
    }

    // Schema Operations
    Status ExecuteSchemaChange(const std::string& ddl_statement) {
        if (!raft_server_ || !running_) {
            return Status::InvalidArgument("Raft server not running");
        }

        RaftOperation operation;
        operation.type = RaftOperationType::kSchemaChange;
        operation.data = ddl_statement;
        operation.timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        operation.node_id = std::to_string(node_id_);

        return raft_server_->ProposeOperation(
            std::make_unique<RaftOperation>(operation));
    }

    Status CreateTable(const std::string& table_definition) {
        if (!raft_server_ || !running_) {
            return Status::InvalidArgument("Raft server not running");
        }

        RaftOperation operation;
        operation.type = RaftOperationType::kTableCreate;
        operation.data = table_definition;
        operation.timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        operation.node_id = std::to_string(node_id_);

        return raft_server_->ProposeOperation(
            std::make_unique<RaftOperation>(operation));
    }

    void PrintStatus() {
        if (!raft_server_) return;

        std::cout << "=== MarbleDB Node " << node_id_ << " Status ===" << std::endl;
        std::cout << "Role: " << raft_server_->GetRole() << std::endl;
        std::cout << "Leader: " << raft_server_->GetLeaderEndpoint() << std::endl;
        std::cout << "Running: " << (running_ ? "Yes" : "No") << std::endl;
        std::cout << raft_server_->GetClusterStatus() << std::endl;
        std::cout << "=====================================" << std::endl;
    }

    int GetNodeId() const { return node_id_; }
    bool IsRunning() const { return running_; }

private:
    int node_id_;
    std::string endpoint_;
    std::vector<std::string> all_endpoints_;
    std::string data_path_;
    std::atomic<bool> running_;

    std::unique_ptr<TaskScheduler> task_scheduler_;
    std::unique_ptr<WalManager> wal_manager_;
    std::unique_ptr<RaftStateMachine> wal_state_machine_;
    std::unique_ptr<RaftStateMachine> schema_state_machine_;
    std::unique_ptr<RaftLogStore> log_store_;
    std::unique_ptr<RaftTransport> transport_;
    std::unique_ptr<RaftServer> raft_server_;
};

void demonstrate_marble_raft_cluster() {
    std::cout << "MarbleDB Distributed Raft Cluster Demo" << std::endl;
    std::cout << "=====================================" << std::endl;

    // Define cluster configuration
    std::vector<std::string> endpoints = {
        "localhost:50051",
        "localhost:50052",
        "localhost:50053"
    };

    // Create MarbleDB nodes
    std::vector<std::unique_ptr<MarbleRaftNode>> nodes;
    for (size_t i = 0; i < endpoints.size(); ++i) {
        std::string data_path = "/tmp/marble_node_" + std::to_string(i + 1);
        auto node = std::make_unique<MarbleRaftNode>(i + 1, endpoints[i], endpoints, data_path);
        nodes.push_back(std::move(node));
    }

    std::cout << "\nStarting MarbleDB cluster nodes..." << std::endl;

    // Start all nodes
    for (auto& node : nodes) {
        auto status = node->Start();
        if (!status.ok()) {
            std::cerr << "Failed to start node " << node->GetNodeId()
                      << ": " << status.ToString() << std::endl;
            return;
        }
    }

    // Wait for cluster to stabilize
    std::cout << "\nWaiting for cluster to stabilize..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(5));

    // Print initial status
    std::cout << "\nInitial cluster status:" << std::endl;
    for (auto& node : nodes) {
        node->PrintStatus();
        std::cout << std::endl;
    }

    // Execute schema changes
    std::cout << "\nExecuting schema changes..." << std::endl;

    std::vector<std::string> schema_operations = {
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)",
        "CREATE INDEX idx_users_email ON users(email)",
        "ALTER TABLE users ADD COLUMN created_at TIMESTAMP"
    };

    for (size_t i = 0; i < schema_operations.size(); ++i) {
        int node_idx = i % nodes.size();
        std::cout << "Node " << (node_idx + 1) << " executing: " << schema_operations[i] << std::endl;

        auto status = nodes[node_idx]->ExecuteSchemaChange(schema_operations[i]);
        if (!status.ok()) {
            std::cerr << "Failed to execute schema change: " << status.ToString() << std::endl;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }

    // Wait for schema changes to propagate
    std::cout << "\nWaiting for schema changes to propagate..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(3));

    // Execute WAL operations (data changes)
    std::cout << "\nExecuting data operations (WAL entries)..." << std::endl;

    std::vector<std::string> wal_operations = {
        "INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')",
        "INSERT INTO users VALUES (2, 'Bob', 'bob@example.com')",
        "INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com')",
        "UPDATE users SET name = 'Alice Smith' WHERE id = 1",
        "DELETE FROM users WHERE id = 2"
    };

    for (size_t i = 0; i < wal_operations.size(); ++i) {
        int node_idx = i % nodes.size();
        std::cout << "Node " << (node_idx + 1) << " executing: " << wal_operations[i] << std::endl;

        auto status = nodes[node_idx]->ReplicateWalEntry(wal_operations[i]);
        if (!status.ok()) {
            std::cerr << "Failed to replicate WAL entry: " << status.ToString() << std::endl;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(800));
    }

    // Wait for WAL operations to propagate
    std::cout << "\nWaiting for WAL operations to propagate..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(4));

    // Print final status
    std::cout << "\nFinal cluster status:" << std::endl;
    for (auto& node : nodes) {
        node->PrintStatus();
        std::cout << std::endl;
    }

    // Demonstrate leader failure and recovery
    std::cout << "\nDemonstrating leader failure..." << std::endl;

    // Find and stop the current leader
    std::string leader_endpoint;
    std::unique_ptr<MarbleRaftNode>* leader_node = nullptr;

    for (auto& node : nodes) {
        if (node->IsRunning()) {
            std::string role = node->GetRole();
            if (role == "leader") {
                leader_node = &node;
                break;
            }
        }
    }

    if (leader_node) {
        std::cout << "Stopping current leader (Node " << (*leader_node)->GetNodeId() << ")..." << std::endl;
        (*leader_node)->Stop();

        std::cout << "Waiting for new leader election..." << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(5));

        std::cout << "\nCluster status after leader failure:" << std::endl;
        for (auto& node : nodes) {
            if (node->IsRunning()) {
                node->PrintStatus();
                std::cout << std::endl;
            }
        }

        // Try some operations with the new leader
        std::cout << "Executing operations with new leader..." << std::endl;
        for (auto& node : nodes) {
            if (node->IsRunning()) {
                auto status = node->ReplicateWalEntry("INSERT INTO users VALUES (4, 'David', 'david@example.com')");
                if (status.ok()) {
                    std::cout << "Successfully executed operation on Node " << node->GetNodeId() << std::endl;
                    break;  // Just do it on one node
                }
            }
        }

        std::this_thread::sleep_for(std::chrono::seconds(2));
    }

    // Stop remaining nodes
    std::cout << "\nStopping all cluster nodes..." << std::endl;
    for (auto& node : nodes) {
        if (node->IsRunning()) {
            node->Stop();
        }
    }

    std::cout << "\nMarbleDB Raft cluster demo completed!" << std::endl;
    std::cout << "\nKey features demonstrated:" << std::endl;
    std::cout << "• Distributed schema management (DDL replication)" << std::endl;
    std::cout << "• WAL entry replication across nodes" << std::endl;
    std::cout << "• Leader election and automatic failover" << std::endl;
    std::cout << "• Persistent log storage with MarbleDB integration" << std::endl;
    std::cout << "• Fault tolerance and data consistency" << std::endl;
    std::cout << "• Arrow Flight transport for high-performance communication" << std::endl;
}

int main(int argc, char** argv) {
    try {
        demonstrate_marble_raft_cluster();
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Exception: " << e.what() << std::endl;
        return 1;
    }
}
