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
#include <filesystem>
#include <fstream>
#include <sstream>
#include <iostream>
#include <algorithm>

namespace fs = std::filesystem;

namespace marble {

// Raft Cluster Configuration Manager
// Manages cluster membership, configuration changes, and persistence
class RaftConfigManager {
public:
    explicit RaftConfigManager(const std::string& config_path)
        : config_path_(config_path) {
        // Ensure config directory exists
        try {
            fs::create_directories(fs::path(config_path_).parent_path());
        } catch (const std::exception& e) {
            std::cerr << "Failed to create config directory: " << e.what() << std::endl;
        }
    }

    ~RaftConfigManager() = default;

    // Load cluster configuration from disk
    Status LoadConfig(RaftClusterConfig* config) {
        try {
            std::ifstream file(config_path_);
            if (!file.is_open()) {
                return Status::NotFound("Configuration file not found: " + config_path_);
            }

            std::string line;
            while (std::getline(file, line)) {
                // Skip comments and empty lines
                if (line.empty() || line[0] == '#') {
                    continue;
                }

                // Parse configuration lines
                auto colon_pos = line.find(':');
                if (colon_pos == std::string::npos) {
                    continue;
                }

                std::string key = line.substr(0, colon_pos);
                std::string value = line.substr(colon_pos + 1);

                // Trim whitespace
                key.erase(key.begin(), std::find_if(key.begin(), key.end(), [](int ch) {
                    return !std::isspace(ch);
                }));
                key.erase(std::find_if(key.rbegin(), key.rend(), [](int ch) {
                    return !std::isspace(ch);
                }).base(), key.end());

                value.erase(value.begin(), std::find_if(value.begin(), value.end(), [](int ch) {
                    return !std::isspace(ch);
                }));
                value.erase(std::find_if(value.rbegin(), value.rend(), [](int ch) {
                    return !std::isspace(ch);
                }).base(), value.end());

                // Parse configuration values
                if (key == "cluster_id") {
                    config->cluster_id = value;
                } else if (key == "election_timeout_ms") {
                    config->election_timeout_ms = std::stoi(value);
                } else if (key == "heartbeat_interval_ms") {
                    config->heartbeat_interval_ms = std::stoi(value);
                } else if (key == "node") {
                    config->node_endpoints.push_back(value);
                }
            }

            file.close();
            return Status::OK();

        } catch (const std::exception& e) {
            return Status::InternalError(std::string("Exception loading config: ") + e.what());
        }
    }

    // Save cluster configuration to disk
    Status SaveConfig(const RaftClusterConfig& config) {
        try {
            std::ofstream file(config_path_);
            if (!file.is_open()) {
                return Status::IOError("Failed to open config file for writing: " + config_path_);
            }

            // Write configuration header
            file << "# MarbleDB Raft Cluster Configuration" << std::endl;
            file << "# Auto-generated - do not edit manually" << std::endl;
            file << std::endl;

            // Write cluster settings
            file << "cluster_id: " << config.cluster_id << std::endl;
            file << "election_timeout_ms: " << config.election_timeout_ms << std::endl;
            file << "heartbeat_interval_ms: " << config.heartbeat_interval_ms << std::endl;
            file << std::endl;

            // Write node endpoints
            file << "# Cluster nodes" << std::endl;
            for (const auto& endpoint : config.node_endpoints) {
                file << "node: " << endpoint << std::endl;
            }

            file.close();
            return Status::OK();

        } catch (const std::exception& e) {
            return Status::InternalError(std::string("Exception saving config: ") + e.what());
        }
    }

    // Add a new node to the cluster
    Status AddNode(RaftClusterConfig* config, const std::string& endpoint) {
        // Check if node already exists
        if (std::find(config->node_endpoints.begin(), config->node_endpoints.end(), endpoint)
            != config->node_endpoints.end()) {
            return Status::InvalidArgument("Node already exists in cluster: " + endpoint);
        }

        config->node_endpoints.push_back(endpoint);

        // Save updated configuration
        return SaveConfig(*config);
    }

    // Remove a node from the cluster
    Status RemoveNode(RaftClusterConfig* config, const std::string& endpoint) {
        auto it = std::find(config->node_endpoints.begin(), config->node_endpoints.end(), endpoint);
        if (it == config->node_endpoints.end()) {
            return Status::NotFound("Node not found in cluster: " + endpoint);
        }

        // Don't allow removing the last node
        if (config->node_endpoints.size() <= 1) {
            return Status::InvalidArgument("Cannot remove the last node from cluster");
        }

        config->node_endpoints.erase(it);

        // Save updated configuration
        return SaveConfig(*config);
    }

    // Validate cluster configuration
    Status ValidateConfig(const RaftClusterConfig& config) {
        if (config.cluster_id.empty()) {
            return Status::InvalidArgument("Cluster ID cannot be empty");
        }

        if (config.node_endpoints.empty()) {
            return Status::InvalidArgument("Cluster must have at least one node");
        }

        if (config.node_endpoints.size() > 7) {
            return Status::InvalidArgument("Raft clusters should not exceed 7 nodes for optimal performance");
        }

        if (config.election_timeout_ms < 100 || config.election_timeout_ms > 30000) {
            return Status::InvalidArgument("Election timeout must be between 100ms and 30000ms");
        }

        if (config.heartbeat_interval_ms >= config.election_timeout_ms) {
            return Status::InvalidArgument("Heartbeat interval must be less than election timeout");
        }

        // Validate endpoint formats
        for (const auto& endpoint : config.node_endpoints) {
            auto colon_pos = endpoint.find(':');
            if (colon_pos == std::string::npos) {
                return Status::InvalidArgument("Invalid endpoint format (expected host:port): " + endpoint);
            }

            std::string port_str = endpoint.substr(colon_pos + 1);
            try {
                int port = std::stoi(port_str);
                if (port < 1024 || port > 65535) {
                    return Status::InvalidArgument("Port must be between 1024 and 65535: " + endpoint);
                }
            } catch (const std::exception&) {
                return Status::InvalidArgument("Invalid port number in endpoint: " + endpoint);
            }
        }

        return Status::OK();
    }

    // Get configuration file path
    std::string GetConfigPath() const {
        return config_path_;
    }

private:
    std::string config_path_;
};

// Raft Cluster Manager
// Manages the lifecycle of a Raft cluster and provides cluster-level operations
class RaftClusterManager {
public:
    explicit RaftClusterManager(const std::string& config_path)
        : config_manager_(config_path) {}

    ~RaftClusterManager() = default;

    // Initialize cluster from configuration
    Status Initialize() {
        RaftClusterConfig config;
        auto status = config_manager_.LoadConfig(&config);
        if (!status.ok()) {
            // If config doesn't exist, create a default one
            if (status.code() == StatusCode::kNotFound) {
                return CreateDefaultConfig();
            }
            return status;
        }

        // Validate loaded configuration
        status = config_manager_.ValidateConfig(config);
        if (!status.ok()) {
            return status;
        }

        current_config_ = config;
        initialized_ = true;

        std::cout << "Initialized Raft cluster '" << config.cluster_id
                  << "' with " << config.node_endpoints.size() << " nodes" << std::endl;

        return Status::OK();
    }

    // Create a new cluster with default configuration
    Status CreateCluster(const std::string& cluster_id,
                        const std::vector<std::string>& endpoints) {
        RaftClusterConfig config;
        config.cluster_id = cluster_id;
        config.node_endpoints = endpoints;
        config.election_timeout_ms = 1000;
        config.heartbeat_interval_ms = 100;

        auto status = config_manager_.ValidateConfig(config);
        if (!status.ok()) {
            return status;
        }

        status = config_manager_.SaveConfig(config);
        if (!status.ok()) {
            return status;
        }

        current_config_ = config;
        initialized_ = true;

        std::cout << "Created new Raft cluster '" << cluster_id << "'" << std::endl;
        return Status::OK();
    }

    // Add a node to the running cluster
    Status AddNodeToCluster(const std::string& endpoint) {
        if (!initialized_) {
            return Status::InvalidArgument("Cluster not initialized");
        }

        auto status = config_manager_.AddNode(&current_config_, endpoint);
        if (!status.ok()) {
            return status;
        }

        std::cout << "Added node to cluster: " << endpoint << std::endl;
        return Status::OK();
    }

    // Remove a node from the running cluster
    Status RemoveNodeFromCluster(const std::string& endpoint) {
        if (!initialized_) {
            return Status::InvalidArgument("Cluster not initialized");
        }

        auto status = config_manager_.RemoveNode(&current_config_, endpoint);
        if (!status.ok()) {
            return status;
        }

        std::cout << "Removed node from cluster: " << endpoint << std::endl;
        return Status::OK();
    }

    // Get current cluster configuration
    const RaftClusterConfig& GetConfig() const {
        return current_config_;
    }

    // Check if cluster is initialized
    bool IsInitialized() const {
        return initialized_;
    }

    // Get cluster status summary
    std::string GetClusterStatus() const {
        if (!initialized_) {
            return "Cluster not initialized";
        }

        std::stringstream ss;
        ss << "Cluster ID: " << current_config_.cluster_id << std::endl;
        ss << "Nodes: " << current_config_.node_endpoints.size() << std::endl;
        for (size_t i = 0; i < current_config_.node_endpoints.size(); ++i) {
            ss << "  " << (i + 1) << ". " << current_config_.node_endpoints[i] << std::endl;
        }
        ss << "Election timeout: " << current_config_.election_timeout_ms << "ms" << std::endl;
        ss << "Heartbeat interval: " << current_config_.heartbeat_interval_ms << "ms" << std::endl;

        return ss.str();
    }

private:
    Status CreateDefaultConfig() {
        // Create a minimal single-node cluster for development
        std::vector<std::string> endpoints = {"localhost:50051"};

        return CreateCluster("marble-default-cluster", endpoints);
    }

    RaftConfigManager config_manager_;
    RaftClusterConfig current_config_;
    bool initialized_ = false;
};

// Factory functions
std::unique_ptr<RaftConfigManager> CreateRaftConfigManager(const std::string& config_path) {
    return std::make_unique<RaftConfigManager>(config_path);
}

std::unique_ptr<RaftClusterManager> CreateRaftClusterManager(const std::string& config_path) {
    return std::make_unique<RaftClusterManager>(config_path);
}

}  // namespace marble
