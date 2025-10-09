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

#include <arrow/api.h>
#include <arrow/flight/api.h>
#include <arrow/flight/server.h>
#include <arrow/flight/client.h>
#include <arrow/buffer.h>
#include <arrow/result.h>

#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <iostream>
#include <queue>
#include <condition_variable>
#include <atomic>

using namespace arrow;
using namespace arrow::flight;

namespace marble {

// Arrow Flight service for Raft communication
class RaftFlightService : public arrow::flight::FlightServerBase {
public:
    RaftFlightService(std::function<void(const std::string&, const std::string&)> message_handler)
        : message_handler_(std::move(message_handler)) {}

    ~RaftFlightService() override = default;

    arrow::Status DoAction(
        const arrow::flight::ServerCallContext& context,
        const arrow::flight::Action& action,
        std::unique_ptr<arrow::flight::ResultStream>* result) override {

        if (action.type == "raft_message") {
            // Handle Raft message
            std::string message_data(reinterpret_cast<const char*>(action.body->data()), 
                                    action.body->size());
            std::string sender_endpoint = context.peer();

            // Extract peer information and call handler
            message_handler_(sender_endpoint, message_data);

            // Return success response - no result stream needed for Raft messages
            *result = nullptr;
            return arrow::Status::OK();
        }

        return arrow::Status::NotImplemented("Unknown action: " + action.type);
    }

private:
    std::function<void(const std::string&, const std::string&)> message_handler_;
};

// Arrow Flight-based transport implementation
class ArrowFlightTransport : public RaftTransport {
public:
    explicit ArrowFlightTransport(const std::string& local_endpoint)
        : local_endpoint_(local_endpoint)
        , running_(false)
        , server_thread_(nullptr) {}

    ~ArrowFlightTransport() override {
        Stop();
    }

    marble::Status SendMessage(const std::string& peer_endpoint,
                              const std::string& message) override {
        try {
            // Parse peer endpoint (format: host:port)
            auto colon_pos = peer_endpoint.find(':');
            if (colon_pos == std::string::npos) {
                return marble::Status::InvalidArgument("Invalid peer endpoint format: " + peer_endpoint);
            }

            std::string host = peer_endpoint.substr(0, colon_pos);
            int port = std::stoi(peer_endpoint.substr(colon_pos + 1));

            // Create Flight client
            arrow::flight::FlightClientOptions options;
            auto location_result = arrow::flight::Location::ForGrpcTcp(host, port);
            if (!location_result.ok()) {
                return marble::Status::InternalError("Failed to create location: " + location_result.status().ToString());
            }
            auto location = location_result.ValueUnsafe();
            
            auto client_result = arrow::flight::FlightClient::Connect(location, options);
            if (!client_result.ok()) {
                return marble::Status::InternalError("Failed to connect to peer: " + client_result.status().ToString());
            }
            auto client = std::move(client_result).ValueUnsafe();

            // Create action for Raft message
            arrow::flight::Action action;
            action.type = "raft_message";
            // Buffer::FromString returns shared_ptr<Buffer> directly (not Result<>)
            action.body = arrow::Buffer::FromString(message);

            // Send the action
            auto result_stream_result = client->DoAction(action);
            if (!result_stream_result.ok()) {
                return marble::Status::InternalError("Failed to send action: " + result_stream_result.status().ToString());
            }
            auto result_stream = std::move(result_stream_result).ValueUnsafe();

            // Wait for response
            auto result_result = result_stream->Next();
            if (!result_result.ok()) {
                return marble::Status::InternalError("Failed to get result: " + result_result.status().ToString());
            }
            auto result = std::move(result_result).ValueUnsafe();
            if (!result) {
                return marble::Status::InternalError("No response from peer");
            }

            return marble::Status::OK();

        } catch (const std::exception& e) {
            return marble::Status::InternalError(std::string("Failed to send message: ") + e.what());
        }
    }

    marble::Status ReceiveMessage(std::string* message) override {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        if (message_queue_.empty()) {
            return marble::Status::NotFound("No messages available");
        }

        *message = std::move(message_queue_.front());
        message_queue_.pop();
        return marble::Status::OK();
    }

    marble::Status ConnectToPeer(const std::string& peer_endpoint) override {
        // Arrow Flight clients connect on-demand, so this is a no-op
        // We could add connection pooling here in the future
        return marble::Status::OK();
    }

    marble::Status DisconnectFromPeer(const std::string& peer_endpoint) override {
        // Arrow Flight clients handle disconnection automatically
        return marble::Status::OK();
    }

    marble::Status Start() {
        if (running_) {
            return marble::Status::InvalidArgument("Transport already running");
        }

        try {
            // Parse local endpoint
            auto colon_pos = local_endpoint_.find(':');
            if (colon_pos == std::string::npos) {
                return marble::Status::InvalidArgument("Invalid local endpoint format: " + local_endpoint_);
            }

            std::string host = local_endpoint_.substr(0, colon_pos);
            int port = std::stoi(local_endpoint_.substr(colon_pos + 1));

            // Create Flight server
            ARROW_ASSIGN_OR_RAISE(auto server_location, arrow::flight::Location::ForGrpcTcp(host, port));
            arrow::flight::FlightServerOptions options(server_location);

            // Create service with message handler
            auto service = std::make_shared<RaftFlightService>(
                [this](const std::string& sender, const std::string& message) {
                    this->HandleIncomingMessage(sender, message);
                });

            server_ = std::make_unique<arrow::flight::FlightServerBase>();
            service_ = std::move(service);

            // Start server in background thread
            server_thread_ = std::make_unique<std::thread>([this, options]() {
                auto status = server_->Init(options);
                if (!status.ok()) {
                    std::cerr << "Failed to start Arrow Flight server: " << status.ToString() << std::endl;
                    return;
                }

                auto serve_status = server_->Serve();
                if (!serve_status.ok()) {
                    std::cerr << "Failed to serve Flight server: " << serve_status.ToString() << std::endl;
                }
            });

            running_ = true;
            return marble::Status::OK();

        } catch (const std::exception& e) {
            return marble::Status::InternalError(std::string("Failed to start transport: ") + e.what());
        }
    }

    marble::Status Stop() {
        if (!running_) {
            return marble::Status::OK();
        }

        try {
            if (server_) {
                server_->Shutdown();
            }

            if (server_thread_ && server_thread_->joinable()) {
                server_thread_->join();
            }

            running_ = false;
            return marble::Status::OK();

        } catch (const std::exception& e) {
            return marble::Status::InternalError(std::string("Failed to stop transport: ") + e.what());
        }
    }

private:
    void HandleIncomingMessage(const std::string& sender, const std::string& message) {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        message_queue_.push(message);
        queue_cv_.notify_one();
    }

    std::string local_endpoint_;
    std::atomic<bool> running_;

    // Server components
    std::unique_ptr<FlightServerBase> server_;
    std::shared_ptr<RaftFlightService> service_;
    std::unique_ptr<std::thread> server_thread_;

    // Message queue for incoming messages
    std::queue<std::string> message_queue_;
    std::mutex queue_mutex_;
    std::condition_variable queue_cv_;
};

// Factory function implementation
std::unique_ptr<RaftTransport> CreateArrowFlightTransport(
    const std::string& local_endpoint) {
    return std::make_unique<ArrowFlightTransport>(local_endpoint);
}

}  // namespace marble
