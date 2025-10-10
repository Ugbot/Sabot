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
#include "marble/status.h"

#include <libnuraft/nuraft.hxx>
#include <libnuraft/state_machine.hxx>
#include <libnuraft/log_store.hxx>
#include <libnuraft/state_mgr.hxx>
#include <libnuraft/asio_service.hxx>

#include <memory>
#include <mutex>
#include <thread>
#include <atomic>
#include <iostream>

using namespace nuraft;

namespace marble {

// NuRaft-based implementation of RaftServer
class NuRaftServer : public RaftServer {
public:
    NuRaftServer(std::unique_ptr<RaftStateMachine> state_machine,
                 std::unique_ptr<RaftLogStore> log_store,
                 std::unique_ptr<RaftTransport> transport)
        : state_machine_(std::move(state_machine))
        , log_store_(std::move(log_store))
        , transport_(std::move(transport))
        , running_(false)
        , initialized_(false) {}

    ~NuRaftServer() override {
        Stop();
    }

    marble::Status Initialize(const RaftClusterConfig& config) override {
        if (initialized_) {
            return marble::Status::InvalidArgument("Raft server already initialized");
        }

        config_ = config;

        // Create NuRaft components
        raft_params params;
        params.election_timeout_lower_bound_ = config.election_timeout_ms;
        params.election_timeout_upper_bound_ = config.election_timeout_ms * 2;
        params.heart_beat_interval_ = config.heartbeat_interval_ms;
        params.snapshot_distance_ = config.snapshot_distance;
        params.max_append_size_ = 1024 * 1024;  // 1MB

        // Create state manager
        state_manager_ = create_state_manager(config);

        // Create log store wrapper
        nuraft_log_store_ = create_log_store_wrapper();

        // Create state machine wrapper
        nuraft_state_machine_ = create_state_machine_wrapper();

        // Create logger
        logger_ = create_logger();

        initialized_ = true;
        return marble::Status::OK();
    }

    marble::Status Start() override {
        if (!initialized_) {
            return marble::Status::InvalidArgument("Raft server not initialized");
        }

        if (running_) {
            return marble::Status::InvalidArgument("Raft server already running");
        }

        try {
            asio_service::options asio_options;

            raft_launcher launcher;

            // Initialize Raft server
            // Signature: init(state_machine, state_mgr, logger, port, asio_options, params)
            int port = 9000; // TODO: Extract from config_.node_endpoints[0]
            raft_server_ = launcher.init(
                nuraft_state_machine_,
                state_manager_,
                logger_,
                port,
                asio_options,
                params_
            );

            // Wait for initialization
            int retries = 50;  // 5 seconds max
            while (!raft_server_->is_initialized() && retries > 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                retries--;
            }

            if (!raft_server_->is_initialized()) {
                return marble::Status::InternalError("Failed to initialize Raft server");
            }

            running_ = true;
            return marble::Status::OK();

        } catch (const std::exception& e) {
            return marble::Status::InternalError(std::string("Failed to start Raft server: ") + e.what());
        }
    }

    Status Stop() override {
        if (!running_) {
            return marble::Status::OK();
        }

        try {
            if (raft_server_) {
                raft_server_->shutdown();
            }
            running_ = false;
            return marble::Status::OK();
        } catch (const std::exception& e) {
            return marble::Status::InternalError(std::string("Failed to stop Raft server: ") + e.what());
        }
    }

    bool IsRunning() const override {
        return running_ && raft_server_ && raft_server_->is_initialized();
    }

    std::string GetRole() const override {
        if (!raft_server_) {
            return "unknown";
        }

        // NuRaft doesn't expose get_role() directly - use is_leader() and infer
        if (raft_server_->is_leader()) {
            return "leader";
        }
        // TODO: Distinguish between follower and candidate if needed
        // For now, assume non-leader means follower
        return "follower";
    }

    std::string GetLeaderEndpoint() const override {
        if (!raft_server_) {
            return "";
        }

        int32 leader_id = raft_server_->get_leader();
        if (leader_id >= 0) {
            ptr<srv_config> leader_config = raft_server_->get_srv_config(leader_id);
            if (leader_config) {
                return leader_config->get_endpoint();
            }
        }
        return "";
    }

    Status ProposeOperation(std::unique_ptr<RaftOperation> operation,
                           std::function<void(Status)> callback) override {
        if (!running_ || !raft_server_) {
            return marble::Status::InvalidArgument("Raft server not running");
        }

        try {
            // Serialize operation
            std::string serialized_data = operation->Serialize();

            // Create buffer for Raft log
            ptr<buffer> log_data = buffer::alloc(serialized_data.size());
            ::memcpy(log_data->data(), serialized_data.data(), serialized_data.size());

            // Append to Raft log
            ptr<cmd_result<ptr<buffer>>> result = raft_server_->append_entries({log_data});

            // Store callback for when operation is committed
            if (callback) {
                // In a real implementation, we'd need to track this
                // For now, just call immediately (simplified)
                callback(marble::Status::OK());
            }

            return marble::Status::OK();

        } catch (const std::exception& e) {
            return marble::Status::InternalError(std::string("Failed to propose operation: ") + e.what());
        }
    }

    Status TriggerElection() override {
        if (!running_ || !raft_server_) {
            return marble::Status::InvalidArgument("Raft server not running");
        }

        try {
            // NuRaft doesn't have request_election() - elections happen automatically
            // If we're leader, we can yield_leadership() to trigger re-election
            if (raft_server_->is_leader()) {
                raft_server_->yield_leadership();
            }
            // Note: Followers will naturally trigger elections on timeout
            return marble::Status::OK();
        } catch (const std::exception& e) {
            return marble::Status::InternalError(std::string("Failed to trigger election: ") + e.what());
        }
    }

    std::string GetClusterStatus() const override {
        if (!raft_server_) {
            return "Server not initialized";
        }

        std::stringstream ss;
        ss << "Role: " << GetRole() << "\n";
        ss << "Leader: " << GetLeaderEndpoint() << "\n";
        ss << "Initialized: " << (raft_server_->is_initialized() ? "Yes" : "No") << "\n";

        ptr<cluster_config> cluster_conf = raft_server_->get_config();
        if (cluster_conf) {
            ss << "Cluster size: " << cluster_conf->get_servers().size() << "\n";
        }

        return ss.str();
    }

private:
    // NuRaft component wrappers
    ptr<state_machine> create_state_machine_wrapper();
    ptr<log_store> create_log_store_wrapper();
    ptr<state_mgr> create_state_manager(const RaftClusterConfig& config);
    ptr<logger> create_logger();

    // Configuration and state
    RaftClusterConfig config_;
    std::atomic<bool> running_;
    std::atomic<bool> initialized_;

    // MarbleDB components
    std::unique_ptr<RaftStateMachine> state_machine_;
    std::unique_ptr<RaftLogStore> log_store_;
    std::unique_ptr<RaftTransport> transport_;

    // NuRaft components
    ptr<raft_server> raft_server_;
    ptr<state_machine> nuraft_state_machine_;
    ptr<log_store> nuraft_log_store_;
    ptr<state_mgr> state_manager_;
    ptr<logger> logger_;
    raft_params params_;
};

// Wrapper class implementations
class MarbleStateMachineWrapper : public state_machine {
public:
    MarbleStateMachineWrapper() = default;

    ptr<buffer> pre_commit(const ulong log_idx, buffer& data) override {
        return nullptr; // No pre-commit needed
    }

    ptr<buffer> commit(const ulong log_idx, buffer& data) override {
        // Apply the log entry
        // TODO: Connect to MarbleDB's LSM tree for state persistence
        return nullptr;
    }

    void commit_config(const ulong log_idx, ptr<cluster_config>& new_conf) override {
        // Config changes handled elsewhere
    }

    void rollback(const ulong log_idx, buffer& data) override {
        // Rollback not implemented
    }

    ptr<snapshot> last_snapshot() override {
        return nullptr; // Snapshots not implemented yet
    }

    ulong last_commit_index() override {
        return 0; // TODO: Track commit index
    }

    void create_snapshot(snapshot& s, async_result<bool>::handler_type& when_done) override {
        if (when_done) {
            bool result = true;
            std::shared_ptr<std::exception> err = nullptr;
            when_done(result, err);
        }
    }

    bool apply_snapshot(snapshot& s) override {
        // TODO: Implement snapshot application
        return true;
    }
};

class MarbleLogStoreWrapper : public log_store {
public:
    explicit MarbleLogStoreWrapper(std::unique_ptr<RaftLogStore> ls)
        : log_store_impl_(std::move(ls)) {}

    ulong next_slot() const override {
        return 0; // TODO: Implement
    }

    ulong start_index() const override {
        return 1;
    }

    ptr<log_entry> last_entry() const override {
        return nullptr; // TODO: Implement
    }

    ulong append(ptr<log_entry>& entry) override {
        return 0; // TODO: Implement
    }

    void write_at(ulong index, ptr<log_entry>& entry) override {
        // TODO: Implement
    }

    ptr<std::vector<ptr<log_entry>>> log_entries(ulong start, ulong end) override {
        return cs_new<std::vector<ptr<log_entry>>>();
    }

    ptr<log_entry> entry_at(ulong index) override {
        return nullptr; // TODO: Implement
    }

    ulong term_at(ulong index) override {
        return 0; // TODO: Implement
    }

    ptr<buffer> pack(ulong index, int32 cnt) override {
        return nullptr; // TODO: Implement
    }

    void apply_pack(ulong index, buffer& pack) override {
        // TODO: Implement
    }

    bool compact(ulong last_log_index) override {
        return false; // TODO: Implement
    }

    bool flush() override {
        return true;
    }

private:
    std::unique_ptr<RaftLogStore> log_store_impl_;
};

class SimpleStateManager : public state_mgr {
public:
    SimpleStateManager(int srv_id, const std::string& endpoint)
        : my_id_(srv_id), my_endpoint_(endpoint) {}

    ptr<cluster_config> load_config() override {
        ptr<cluster_config> config = cs_new<cluster_config>(0, 0);
        config->get_servers().push_back(cs_new<srv_config>(my_id_, my_endpoint_));
        return config;
    }

    void save_config(const cluster_config& config) override {
        // TODO: Persist config
    }

    void save_state(const srv_state& state) override {
        // TODO: Persist state
    }

    ptr<srv_state> read_state() override {
        return cs_new<srv_state>();
    }

    ptr<log_store> load_log_store() override {
        return nullptr; // Log store created separately
    }

    int32 server_id() override {
        return my_id_;
    }

    void system_exit(const int exit_code) override {
        // Don't actually exit
    }

private:
    int my_id_;
    std::string my_endpoint_;
};

class SimpleLogger : public logger {
public:
    SimpleLogger() = default;

    void put_details(int level,
                    const char* source_file,
                    const char* func_name,
                    size_t line_number,
                    const std::string& msg) override {
        // Simple console logging
        std::cout << "[NuRaft " << level << "] " << msg << std::endl;
    }

    void set_level(int l) override {
        level_ = l;
    }

    int get_level() override {
        return level_;
    }

private:
    int level_ = 6; // Default to info level
};

// Implementation of wrapper creation methods
ptr<state_machine> NuRaftServer::create_state_machine_wrapper() {
    return cs_new<MarbleStateMachineWrapper>();
}

ptr<log_store> NuRaftServer::create_log_store_wrapper() {
    return cs_new<MarbleLogStoreWrapper>(std::move(log_store_));
}

ptr<state_mgr> NuRaftServer::create_state_manager(const RaftClusterConfig& config) {
    // Extract server ID from endpoint (simplified)
    int server_id = 1;  // TODO: Parse from endpoint
    std::string endpoint = config.node_endpoints.empty() ? "localhost:12345" : config.node_endpoints[0];
    return cs_new<SimpleStateManager>(server_id, endpoint);
}

ptr<logger> NuRaftServer::create_logger() {
    return cs_new<SimpleLogger>();
}

std::unique_ptr<RaftServer> CreateRaftServer(
    std::unique_ptr<RaftStateMachine> state_machine,
    std::unique_ptr<RaftLogStore> log_store,
    std::unique_ptr<RaftTransport> transport) {

    return std::make_unique<NuRaftServer>(
        std::move(state_machine),
        std::move(log_store),
        std::move(transport)
    );
}

// In-memory log store implementation for testing
class InMemoryLogStore : public RaftLogStore {
public:
    marble::Status StoreLogEntry(uint64_t index, const std::string& data) override {
        std::lock_guard<std::mutex> lock(mutex_);
        logs_[index] = data;
        return marble::Status::OK();
    }

    marble::Status GetLogEntry(uint64_t index, std::string* data) const override {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = logs_.find(index);
        if (it == logs_.end()) {
            return marble::Status::NotFound("Log entry not found");
        }
        *data = it->second;
        return marble::Status::OK();
    }

    marble::Status DeleteLogs(uint64_t up_to_index) override {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = logs_.begin();
        while (it != logs_.end() && it->first <= up_to_index) {
            it = logs_.erase(it);
        }
        return marble::Status::OK();
    }

    uint64_t GetLastLogIndex() const override {
        std::lock_guard<std::mutex> lock(mutex_);
        return logs_.empty() ? 0 : logs_.rbegin()->first;
    }

    uint64_t GetLogTerm(uint64_t index) const override {
        // For simplicity, return a fixed term
        return 1;
    }

private:
    mutable std::mutex mutex_;
    std::map<uint64_t, std::string> logs_;
};

std::unique_ptr<RaftLogStore> CreateMarbleLogStore(const std::string& path) {
    // TODO: Implement persistent log store using path
    // For now, use in-memory implementation
    return std::make_unique<InMemoryLogStore>();
}

std::unique_ptr<RaftTransport> CreateArrowFlightTransport(
    const std::string& local_endpoint) {
    // TODO: Implement using Arrow Flight
    return nullptr;
}

std::unique_ptr<RaftLogStore> CreateInMemoryLogStore() {
    return std::make_unique<InMemoryLogStore>();
}

}  // namespace marble
