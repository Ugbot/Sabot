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
#include <libnuraft/logger.hxx>

#include <memory>
#include <mutex>
#include <iostream>

using namespace nuraft;

namespace marble {

// Wrapper for MarbleDB's RaftStateMachine to NuRaft's state_machine
class MarbleStateMachineWrapper : public state_machine {
public:
    explicit MarbleStateMachineWrapper(std::unique_ptr<RaftStateMachine> marble_sm)
        : marble_sm_(std::move(marble_sm))
        , last_committed_idx_(0) {}

    ~MarbleStateMachineWrapper() override = default;

    ptr<buffer> commit(const ulong log_idx, buffer& data) override {
        try {
            // Deserialize operation
            buffer_serializer bs(data);
            std::string serialized_data;
            serialized_data.resize(data.size());
            ::memcpy(&serialized_data[0], data.data(), data.size());

            auto operation = RaftOperation::Deserialize(serialized_data);
            if (!operation) {
                std::cerr << "Failed to deserialize Raft operation" << std::endl;
                return nullptr;
            }

            // Apply to MarbleDB state machine
            Status status = marble_sm_->ApplyOperation(*operation);
            if (!status.ok()) {
                std::cerr << "Failed to apply operation: " << status.ToString() << std::endl;
                return nullptr;
            }

            last_committed_idx_ = log_idx;
            return nullptr;

        } catch (const std::exception& e) {
            std::cerr << "Exception in commit: " << e.what() << std::endl;
            return nullptr;
        }
    }

    void rollback(const ulong log_idx, buffer& data) override {
        // For now, we don't implement rollback in MarbleDB
        // In production, this would need to be implemented
        std::cout << "Rolling back operation at index " << log_idx << std::endl;
    }

    int read_logical_snp_obj(snapshot& s, void*& user_snp_ctx,
                            ulong obj_id, ptr<buffer>& data_out, bool& is_last_obj) override {
        // Placeholder for snapshot reading
        data_out = buffer::alloc(sizeof(int32_t));
        buffer_serializer bs(data_out);
        bs.put_i32(0);
        is_last_obj = true;
        return 0;
    }

    void save_logical_snp_obj(snapshot& s, ulong& obj_id, buffer& data,
                             bool is_first_obj, bool is_last_obj) override {
        // Placeholder for snapshot saving
        std::cout << "Saving snapshot object " << obj_id << std::endl;
    }

    bool apply_snapshot(snapshot& s) override {
        try {
            // Restore from snapshot
            Status status = marble_sm_->RestoreFromSnapshot(s.get_last_log_idx());
            return status.ok();
        } catch (const std::exception& e) {
            std::cerr << "Exception in apply_snapshot: " << e.what() << std::endl;
            return false;
        }
    }

    ptr<snapshot> last_snapshot() override {
        // Return last snapshot (simplified)
        ptr<snapshot> snap = cs_new<snapshot>(last_committed_idx_, 0, cs_new<cluster_config>());
        return snap;
    }

    ulong last_commit_index() override {
        return last_committed_idx_;
    }

private:
    std::unique_ptr<RaftStateMachine> marble_sm_;
    std::atomic<ulong> last_committed_idx_;
};

// Wrapper for MarbleDB's RaftLogStore to NuRaft's log_store
class MarbleLogStoreWrapper : public log_store {
public:
    explicit MarbleLogStoreWrapper(std::unique_ptr<RaftLogStore> marble_ls)
        : marble_ls_(std::move(marble_ls)) {}

    ~MarbleLogStoreWrapper() override = default;

    ulong next_slot() const override {
        return marble_ls_->GetLastLogIndex() + 1;
    }

    ulong start_index() const override {
        // Assume logs start from index 1
        return 1;
    }

    ptr<log_entry> last_entry() const override {
        ulong last_idx = marble_ls_->GetLastLogIndex();
        if (last_idx == 0) {
            return nullptr;
        }

        std::string data;
        Status status = marble_ls_->GetLogEntry(last_idx, &data);
        if (!status.ok()) {
            return nullptr;
        }

        ptr<buffer> buf = buffer::alloc(data.size());
        ::memcpy(buf->data(), data.data(), data.size());

        ulong term = marble_ls_->GetLogTerm(last_idx);
        return cs_new<log_entry>(term, buf, log_val_type::app_log);
    }

    ulong append(ptr<log_entry>& entry) override {
        ulong index = next_slot();
        std::string data;
        data.resize(entry->get_buf().size());
        ::memcpy(&data[0], entry->get_buf().data(), entry->get_buf().size());

        Status status = marble_ls_->StoreLogEntry(index, data);
        if (!status.ok()) {
            throw std::runtime_error("Failed to store log entry");
        }

        return index;
    }

    void write_at(ulong index, ptr<log_entry>& entry) override {
        std::string data;
        data.resize(entry->get_buf().size());
        ::memcpy(&data[0], entry->get_buf().data(), entry->get_buf().size());

        Status status = marble_ls_->StoreLogEntry(index, data);
        if (!status.ok()) {
            throw std::runtime_error("Failed to write log entry");
        }
    }

    ptr<std::vector<ptr<log_entry>>> log_entries(ulong start, ulong end) override {
        ptr<std::vector<ptr<log_entry>>> result = cs_new<std::vector<ptr<log_entry>>>();

        for (ulong i = start; i < end; ++i) {
            std::string data;
            Status status = marble_ls_->GetLogEntry(i, &data);
            if (!status.ok()) {
                break;
            }

            ptr<buffer> buf = buffer::alloc(data.size());
            ::memcpy(buf->data(), data.data(), data.size());

            ulong term = marble_ls_->GetLogTerm(i);
            ptr<log_entry> entry = cs_new<log_entry>(term, buf, log_val_type::app_log);
            result->push_back(entry);
        }

        return result;
    }

    ptr<log_entry> entry_at(ulong index) override {
        std::string data;
        Status status = marble_ls_->GetLogEntry(index, &data);
        if (!status.ok()) {
            return nullptr;
        }

        ptr<buffer> buf = buffer::alloc(data.size());
        ::memcpy(buf->data(), data.data(), data.size());

        ulong term = marble_ls_->GetLogTerm(index);
        return cs_new<log_entry>(term, buf, log_val_type::app_log);
    }

    ulong term_at(ulong index) override {
        return marble_ls_->GetLogTerm(index);
    }

    ptr<buffer> pack(ulong index, int32 cnt) override {
        // Simplified implementation
        return nullptr;
    }

    void apply_pack(ulong index, buffer& pack) override {
        // Simplified implementation
    }

    bool compact(ulong last_log_index) override {
        Status status = marble_ls_->DeleteLogs(last_log_index);
        return status.ok();
    }

    bool flush() override {
        // Assume MarbleDB handles flushing
        return true;
    }

private:
    std::unique_ptr<RaftLogStore> marble_ls_;
};

// Simple in-memory state manager for NuRaft
class SimpleStateManager : public state_mgr {
public:
    SimpleStateManager(int server_id, const std::string& endpoint)
        : server_id_(server_id), endpoint_(endpoint) {}

    ~SimpleStateManager() override = default;

    ptr<cluster_config> load_config() override {
        ptr<cluster_config> config = cs_new<cluster_config>();
        ptr<srv_config> srv_conf = cs_new<srv_config>(server_id_, endpoint_);
        config->get_servers().push_back(srv_conf);
        return config;
    }

    void save_config(const cluster_config& config) override {
        // In a real implementation, persist the config
        std::cout << "Saving cluster config" << std::endl;
    }

    void save_state(const srv_state& state) override {
        // In a real implementation, persist the server state
        std::cout << "Saving server state" << std::endl;
    }

    ptr<srv_state> read_state() override {
        // Return default state
        return cs_new<srv_state>();
    }

    ptr<log_store> load_log_store() override {
        // This should not be called since we provide our own log store
        return nullptr;
    }

    int32 server_id() override {
        return server_id_;
    }

    void system_exit(const int exit_code) override {
        // Handle system exit
        std::cout << "System exit requested with code: " << exit_code << std::endl;
    }

private:
    int server_id_;
    std::string endpoint_;
};

// Simple logger wrapper
class SimpleLogger : public logger {
public:
    SimpleLogger() : level_(6) {}  // INFO level
    ~SimpleLogger() override = default;

    void debug(const std::string& log_line) override {
        if (level_ >= 7) std::cout << "[DEBUG] " << log_line << std::endl;
    }

    void info(const std::string& log_line) override {
        if (level_ >= 6) std::cout << "[INFO] " << log_line << std::endl;
    }

    void warn(const std::string& log_line) override {
        if (level_ >= 4) std::cout << "[WARN] " << log_line << std::endl;
    }

    void err(const std::string& log_line) override {
        if (level_ >= 3) std::cerr << "[ERROR] " << log_line << std::endl;
    }

    void set_level(int level) override {
        level_ = level;
    }

    int get_level() override {
        return level_;
    }

private:
    std::atomic<int> level_;
};

}  // namespace marble
