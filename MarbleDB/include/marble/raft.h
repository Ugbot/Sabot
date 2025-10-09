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

#pragma once

#include "marble/status.h"
#include "marble/wal.h"

#include <memory>
#include <string>
#include <vector>
#include <functional>

namespace marble {

// Forward declarations
class RaftServer;
class RaftStateMachine;
class RaftLogStore;
class RaftStateManager;
class RaftTransport;

/**
 * Raft operation types for MarbleDB
 */
enum class RaftOperationType {
    kWalEntry,         // WAL entry replication
    kSchemaChange,     // Schema modification
    kTableCreate,      // Table creation
    kTableDrop,        // Table deletion
    kIndexCreate,      // Index creation
    kIndexDrop,        // Index deletion
    kConfigChange,     // Cluster configuration change
    kSnapshot,         // Snapshot operation
    kCustom           // Custom operation
};

/**
 * Raft operation payload
 */
struct RaftOperation {
    RaftOperationType type;
    std::string data;
    uint64_t timestamp;
    uint64_t sequence_number;
    std::string node_id;

    // Serialize to string for Raft log
    std::string Serialize() const;

    // Deserialize from string
    static std::unique_ptr<RaftOperation> Deserialize(const std::string& data);
};

/**
 * Raft cluster configuration
 */
struct RaftClusterConfig {
    std::string cluster_id;
    std::vector<std::string> node_endpoints;  // "host:port" format
    int election_timeout_ms = 1000;
    int heartbeat_interval_ms = 100;
    int snapshot_distance = 10000;
    size_t max_log_size = 1024 * 1024 * 1024;  // 1GB
    bool enable_ssl = false;
    bool enable_pre_vote = true;
};

/**
 * Raft server interface
 */
class RaftServer {
public:
    virtual ~RaftServer() = default;

    // Initialize the Raft server
    virtual marble::Status Initialize(const RaftClusterConfig& config) = 0;

    // Start the Raft server
    virtual marble::Status Start() = 0;

    // Stop the Raft server
    virtual marble::Status Stop() = 0;

    // Check if server is running
    virtual bool IsRunning() const = 0;

    // Get server role (leader/follower/candidate)
    virtual std::string GetRole() const = 0;

    // Get leader endpoint
    virtual std::string GetLeaderEndpoint() const = 0;

    // Propose an operation to the Raft cluster
    virtual marble::Status ProposeOperation(std::unique_ptr<RaftOperation> operation,
                                   std::function<void(marble::Status)> callback = nullptr) = 0;

    // Force leader election (for testing)
    virtual marble::Status TriggerElection() = 0;

    // Get cluster status
    virtual std::string GetClusterStatus() const = 0;
};

/**
 * Raft state machine interface for MarbleDB operations
 */
class RaftStateMachine {
public:
    virtual ~RaftStateMachine() = default;

    // Called when an operation is committed by Raft
    virtual marble::Status ApplyOperation(const RaftOperation& operation) = 0;

    // Create snapshot of current state
    virtual marble::Status CreateSnapshot(uint64_t log_index) = 0;

    // Restore from snapshot
    virtual marble::Status RestoreFromSnapshot(uint64_t log_index) = 0;

    // Get last applied operation index
    virtual uint64_t GetLastAppliedIndex() const = 0;
};

/**
 * Raft log store interface
 */
class RaftLogStore {
public:
    virtual ~RaftLogStore() = default;

    // Store log entry
    virtual marble::Status StoreLogEntry(uint64_t index, const std::string& data) = 0;

    // Retrieve log entry
    virtual marble::Status GetLogEntry(uint64_t index, std::string* data) const = 0;

    // Delete logs up to index
    virtual marble::Status DeleteLogs(uint64_t up_to_index) = 0;

    // Get last log index
    virtual uint64_t GetLastLogIndex() const = 0;

    // Get log term for index
    virtual uint64_t GetLogTerm(uint64_t index) const = 0;
};

/**
 * Raft transport layer interface (will use Arrow Flight)
 */
class RaftTransport {
public:
    virtual ~RaftTransport() = default;

    // Send message to peer
    virtual marble::Status SendMessage(const std::string& peer_endpoint,
                              const std::string& message) = 0;

    // Receive message from peer
    virtual marble::Status ReceiveMessage(std::string* message) = 0;

    // Establish connection to peer
    virtual marble::Status ConnectToPeer(const std::string& peer_endpoint) = 0;

    // Disconnect from peer
    virtual marble::Status DisconnectFromPeer(const std::string& peer_endpoint) = 0;
};

/**
 * Create a new Raft server instance
 */
std::unique_ptr<RaftServer> CreateRaftServer(
    std::unique_ptr<RaftStateMachine> state_machine,
    std::unique_ptr<RaftLogStore> log_store,
    std::unique_ptr<RaftTransport> transport);

/**
 * Create a Raft log store using MarbleDB's storage
 */
std::unique_ptr<RaftLogStore> CreateMarbleLogStore(const std::string& path);

/**
 * Create a Raft transport layer using Arrow Flight
 */
std::unique_ptr<RaftTransport> CreateArrowFlightTransport(
    const std::string& local_endpoint);

/**
 * Create a simple in-memory Raft log store (for testing)
 */
std::unique_ptr<RaftLogStore> CreateInMemoryLogStore();

/**
 * Create a MarbleDB WAL replication state machine
 */
std::unique_ptr<RaftStateMachine> CreateMarbleWalStateMachine(
    std::unique_ptr<WalManager> wal_manager);

/**
 * Create a MarbleDB schema change state machine
 */
std::unique_ptr<RaftStateMachine> CreateMarbleSchemaStateMachine();

// Forward declarations for cluster management
class RaftConfigManager;
class RaftClusterManager;

/**
 * Create a Raft configuration manager
 */
std::unique_ptr<RaftConfigManager> CreateRaftConfigManager(const std::string& config_path);

/**
 * Create a Raft cluster manager
 */
std::unique_ptr<RaftClusterManager> CreateRaftClusterManager(const std::string& config_path);

}  // namespace marble
