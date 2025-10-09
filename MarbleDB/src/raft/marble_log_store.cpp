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

#include <fstream>
#include <filesystem>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <iostream>
#include <cstring>

namespace marble {
namespace fs = std::filesystem;

// MarbleDB Log Store Implementation
// Uses the local filesystem to store Raft log entries persistently
class MarbleLogStore : public RaftLogStore {
public:
    explicit MarbleLogStore(const std::string& base_path)
        : base_path_(base_path)
        , start_index_(1) {  // Raft logs typically start from index 1

        // Ensure the base directory exists
        try {
            fs::create_directories(base_path_);
        } catch (const std::exception& e) {
            std::cerr << "Failed to create log store directory: " << e.what() << std::endl;
        }

        // Load existing logs if any
        LoadExistingLogs();
    }

    ~MarbleLogStore() override = default;

    marble::Status StoreLogEntry(uint64_t index, const std::string& data) override {
        std::lock_guard<std::mutex> lock(mutex_);

        try {
            // Write the log entry to disk
            std::string filename = GetLogFilePath(index);
            std::ofstream file(filename, std::ios::binary | std::ios::trunc);

            if (!file.is_open()) {
                return marble::Status::IOError("Failed to open log file for writing: " + filename);
            }

            // Write format: [index][term][data_size][data]
            // For simplicity, we use term=1 for all entries
            uint64_t term = 1;
            uint32_t data_size = static_cast<uint32_t>(data.size());

            file.write(reinterpret_cast<const char*>(&index), sizeof(uint64_t));
            file.write(reinterpret_cast<const char*>(&term), sizeof(uint64_t));
            file.write(reinterpret_cast<const char*>(&data_size), sizeof(uint32_t));
            file.write(data.data(), data_size);

            if (!file.good()) {
                return marble::Status::IOError("Failed to write log entry to file: " + filename);
            }

            file.close();

            // Update in-memory cache
            logs_[index] = data;
            terms_[index] = term;

            // Update last index if this is the highest
            if (index > last_index_) {
                last_index_ = index;
            }

            return marble::Status::OK();

        } catch (const std::exception& e) {
            return marble::Status::InternalError(std::string("Exception storing log entry: ") + e.what());
        }
    }

    marble::Status GetLogEntry(uint64_t index, std::string* data) const override {
        std::lock_guard<std::mutex> lock(mutex_);

        // Check in-memory cache first
        auto it = logs_.find(index);
        if (it != logs_.end()) {
            *data = it->second;
            return marble::Status::OK();
        }

        // Load from disk
        try {
            std::string filename = GetLogFilePath(index);
            std::ifstream file(filename, std::ios::binary);

            if (!file.is_open()) {
                return marble::Status::NotFound("Log file not found: " + filename);
            }

            // Read format: [index][term][data_size][data]
            uint64_t file_index, term;
            uint32_t data_size;

            file.read(reinterpret_cast<char*>(&file_index), sizeof(uint64_t));
            file.read(reinterpret_cast<char*>(&term), sizeof(uint64_t));
            file.read(reinterpret_cast<char*>(&data_size), sizeof(uint32_t));

            if (!file.good() || file_index != index) {
                return marble::Status::Corruption("Corrupted log file: " + filename);
            }

            data->resize(data_size);
            file.read(&(*data)[0], data_size);

            if (!file.good()) {
                return marble::Status::Corruption("Failed to read log data from file: " + filename);
            }

            file.close();

            // Cache the loaded entry
            const_cast<MarbleLogStore*>(this)->logs_[index] = *data;
            const_cast<MarbleLogStore*>(this)->terms_[index] = term;

            return marble::Status::OK();

        } catch (const std::exception& e) {
            return marble::Status::InternalError(std::string("Exception reading log entry: ") + e.what());
        }
    }

    marble::Status DeleteLogs(uint64_t up_to_index) override {
        std::lock_guard<std::mutex> lock(mutex_);

        try {
            // Delete log files up to the specified index
            for (uint64_t i = start_index_; i <= up_to_index; ++i) {
                std::string filename = GetLogFilePath(i);
                if (fs::exists(filename)) {
                    fs::remove(filename);
                }

                // Remove from cache
                logs_.erase(i);
                terms_.erase(i);
            }

            // Update start index if we're deleting from the beginning
            if (up_to_index >= start_index_) {
                start_index_ = up_to_index + 1;
            }

            std::cout << "Deleted log entries up to index: " << up_to_index << std::endl;
            return marble::Status::OK();

        } catch (const std::exception& e) {
            return marble::Status::InternalError(std::string("Exception deleting logs: ") + e.what());
        }
    }

    uint64_t GetLastLogIndex() const override {
        std::lock_guard<std::mutex> lock(mutex_);
        return last_index_;
    }

    uint64_t GetLogTerm(uint64_t index) const override {
        std::lock_guard<std::mutex> lock(mutex_);

        // Check cache first
        auto it = terms_.find(index);
        if (it != terms_.end()) {
            return it->second;
        }

        // Load from disk if not cached
        try {
            std::string filename = GetLogFilePath(index);
            std::ifstream file(filename, std::ios::binary);

            if (!file.is_open()) {
                return 0;  // Term 0 indicates not found
            }

            uint64_t file_index, term;
            file.read(reinterpret_cast<char*>(&file_index), sizeof(uint64_t));
            file.read(reinterpret_cast<char*>(&term), sizeof(uint64_t));

            if (!file.good() || file_index != index) {
                return 0;
            }

            file.close();

            // Cache the term
            const_cast<MarbleLogStore*>(this)->terms_[index] = term;
            return term;

        } catch (const std::exception&) {
            return 0;
        }
    }

private:
    std::string GetLogFilePath(uint64_t index) const {
        // Use a simple file naming scheme: log_0000000001, log_0000000002, etc.
        char filename[32];
        std::snprintf(filename, sizeof(filename), "log_%010llu", index);
        return base_path_ + "/" + filename;
    }

    void LoadExistingLogs() {
        try {
            // Scan the directory for existing log files
            if (!fs::exists(base_path_)) {
                return;
            }

            uint64_t max_index = 0;
            uint64_t min_index = UINT64_MAX;

            for (const auto& entry : fs::directory_iterator(base_path_)) {
                if (entry.is_regular_file()) {
                    std::string filename = entry.path().filename().string();

                    // Parse filename to extract index
                    if (filename.substr(0, 4) == "log_") {
                        try {
                            uint64_t index = std::stoull(filename.substr(4));
                            max_index = std::max(max_index, index);
                            min_index = std::min(min_index, index);
                        } catch (const std::exception&) {
                            // Skip invalid filenames
                            continue;
                        }
                    }
                }
            }

            if (max_index > 0) {
                last_index_ = max_index;
                start_index_ = min_index;

                std::cout << "Loaded existing logs: " << min_index << " to " << max_index << std::endl;
            }

        } catch (const std::exception& e) {
            std::cerr << "Error loading existing logs: " << e.what() << std::endl;
        }
    }

    std::string base_path_;
    mutable std::mutex mutex_;

    uint64_t start_index_;
    mutable uint64_t last_index_ = 0;

    // In-memory caches for better performance
    mutable std::unordered_map<uint64_t, std::string> logs_;
    mutable std::unordered_map<uint64_t, uint64_t> terms_;
};

// Factory function
std::unique_ptr<RaftLogStore> CreateMarbleLogStore(const std::string& path) {
    return std::make_unique<MarbleLogStore>(path);
}

}  // namespace marble
