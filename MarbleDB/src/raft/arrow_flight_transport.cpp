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

#ifdef MARBLE_ENABLE_ARROW_FLIGHT
#include <arrow/api.h>
#include <arrow/flight/api.h>
#include <arrow/flight/server.h>
#include <arrow/flight/client.h>
#include <arrow/buffer.h>
#include <arrow/result.h>
#endif

#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <iostream>
#include <queue>
#include <condition_variable>
#include <atomic>

#ifdef MARBLE_ENABLE_ARROW_FLIGHT
using namespace arrow;
using namespace arrow::flight;
#endif

namespace marble {

#ifdef MARBLE_ENABLE_ARROW_FLIGHT
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

            // Call message handler
            message_handler_(sender_endpoint, message_data);

            // Return empty result stream for Raft messages
            *result = std::unique_ptr<arrow::flight::ResultStream>(nullptr);
            return arrow::Status::OK();
        }

        return arrow::Status::NotImplemented("Unknown action: " + action.type);
    }

    arrow::Status DoPut(
        const arrow::flight::ServerCallContext& context,
        std::unique_ptr<arrow::flight::FlightMessageReader> reader,
        std::unique_ptr<arrow::flight::FlightMetadataWriter> writer) override {
        return arrow::Status::NotImplemented("DoPut not supported for Raft transport");
    }

    arrow::Status DoGet(
        const arrow::flight::ServerCallContext& context,
        const arrow::flight::Ticket& request,
        std::unique_ptr<arrow::flight::FlightDataStream>* stream) override {
        return arrow::Status::NotImplemented("DoGet not supported for Raft transport");
    }

    arrow::Status DoExchange(
        const arrow::flight::ServerCallContext& context,
        std::unique_ptr<arrow::flight::FlightMessageReader> reader,
        std::unique_ptr<arrow::flight::FlightMessageWriter> writer) override {
        return arrow::Status::NotImplemented("DoExchange not supported for Raft transport");
    }

    arrow::Status ListActions(
        const arrow::flight::ServerCallContext& context,
        std::vector<arrow::flight::ActionType>* actions) override {
        actions->push_back({"raft_message", "Send Raft message"});
        return arrow::Status::OK();
    }

private:
    std::function<void(const std::string&, const std::string&)> message_handler_;
};
#endif

#ifdef MARBLE_ENABLE_ARROW_FLIGHT
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

    marble::Status Start() override {
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

            // Create Flight server location
            ARROW_ASSIGN_OR_RAISE(auto server_location, arrow::flight::Location::ForGrpcTcp(host, port));

            // Create service with message handler
            auto service = std::make_shared<RaftFlightService>(
                [this](const std::string& sender, const std::string& message) {
                    this->HandleIncomingMessage(sender, message);
                });

            // Create Flight server with our service
            arrow::flight::FlightServerOptions options(server_location);
            server_ = std::make_unique<arrow::flight::FlightServer>(std::move(service), options);

            // Start server in background thread
            server_thread_ = std::make_unique<std::thread>([this]() {
                try {
                    auto status = server_->Serve();
                    if (!status.ok()) {
                        std::cerr << "Failed to serve Arrow Flight server: " << status.ToString() << std::endl;
                    }
                } catch (const std::exception& e) {
                    std::cerr << "Exception in Arrow Flight server thread: " << e.what() << std::endl;
                }
            });

            running_ = true;
            return marble::Status::OK();

        } catch (const std::exception& e) {
            return marble::Status::InternalError(std::string("Failed to start transport: ") + e.what());
        }
    }

    marble::Status Stop() override {
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
    std::unique_ptr<arrow::flight::FlightServer> server_;
    std::unique_ptr<std::thread> server_thread_;

    // Message queue for incoming messages
    std::queue<std::string> message_queue_;
    std::mutex queue_mutex_;
    std::condition_variable queue_cv_;
};
#else
// Fallback transport when Arrow Flight is not available
class ArrowFlightTransport : public RaftTransport {
public:
    explicit ArrowFlightTransport(const std::string& local_endpoint)
        : local_endpoint_(local_endpoint) {}

    ~ArrowFlightTransport() override = default;

    marble::Status SendMessage(const std::string& peer_endpoint,
                              const std::string& message) override {
        return marble::Status::NotImplemented("Arrow Flight transport not available - rebuild with Arrow Flight support");
    }

    marble::Status ReceiveMessage(std::string* message) override {
        return marble::Status::NotImplemented("Arrow Flight transport not available - rebuild with Arrow Flight support");
    }

    marble::Status ConnectToPeer(const std::string& peer_endpoint) override {
        return marble::Status::NotImplemented("Arrow Flight transport not available - rebuild with Arrow Flight support");
    }

    marble::Status DisconnectFromPeer(const std::string& peer_endpoint) override {
        return marble::Status::NotImplemented("Arrow Flight transport not available - rebuild with Arrow Flight support");
    }

    marble::Status Start() override {
        return marble::Status::NotImplemented("Arrow Flight transport not available - rebuild with Arrow Flight support");
    }

    marble::Status Stop() override {
        return marble::Status::OK();
    }

private:
    std::string local_endpoint_;
};
#endif

// Factory function implementation
std::unique_ptr<RaftTransport> CreateArrowFlightTransport(
    const std::string& local_endpoint) {
    return std::make_unique<ArrowFlightTransport>(local_endpoint);
}

}  // namespace marble
