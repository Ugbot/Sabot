#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <chrono>
#include <csignal>
#include <marble/db.h>
#include <marble/status.h>

using namespace marble;

namespace {

// Global flag for graceful shutdown
volatile bool g_shutdown_requested = false;

// Signal handler for graceful shutdown
void signal_handler(int signal) {
    std::cout << "\nReceived signal " << signal << ", initiating graceful shutdown..." << std::endl;
    g_shutdown_requested = true;
}

// Server configuration
struct ServerConfig {
    std::string host = "0.0.0.0";
    int port = 8080;
    std::string data_dir = "./marble_data";
    bool enable_raft = false;
    std::string raft_cluster = "";
    int raft_port = 9090;
    bool enable_flight = true;
    int flight_port = 8081;
};

class MarbleServer {
public:
    MarbleServer(const ServerConfig& config) : config_(config) {}
    ~MarbleServer() = default;

    Status Start() {
        std::cout << "==========================================" << std::endl;
        std::cout << "🚀 Starting MarbleDB Server" << std::endl;
        std::cout << "==========================================" << std::endl;

        std::cout << "Configuration:" << std::endl;
        std::cout << "  • Data directory: " << config_.data_dir << std::endl;
        std::cout << "  • Host: " << config_.host << ":" << config_.port << std::endl;
        if (config_.enable_flight) {
            std::cout << "  • Arrow Flight: " << config_.host << ":" << config_.flight_port << std::endl;
        }
        if (config_.enable_raft) {
            std::cout << "  • Raft consensus: enabled on port " << config_.raft_port << std::endl;
        }
        std::cout << std::endl;

        // Initialize database
        db_ = CreateDatabase(config_.data_dir);
        if (!db_) {
            return Status::InternalError("Failed to create database");
        }

        std::cout << "✅ Database initialized successfully" << std::endl;

        // Start server threads
        if (config_.enable_flight) {
            flight_thread_ = std::thread(&MarbleServer::RunFlightServer, this);
            std::cout << "✅ Arrow Flight server started on port " << config_.flight_port << std::endl;
        }

        if (config_.enable_raft) {
            raft_thread_ = std::thread(&MarbleServer::RunRaftServer, this);
            std::cout << "✅ Raft consensus server started on port " << config_.raft_port << std::endl;
        }

        // Main server loop
        RunMainLoop();

        // Wait for threads to finish
        if (flight_thread_.joinable()) {
            flight_thread_.join();
        }
        if (raft_thread_.joinable()) {
            raft_thread_.join();
        }

        std::cout << "✅ MarbleDB server shut down gracefully" << std::endl;
        return Status::OK();
    }

private:
    void RunMainLoop() {
        std::cout << "🔄 Server running. Press Ctrl+C to stop." << std::endl;

        while (!g_shutdown_requested) {
            // Periodic maintenance tasks
            std::this_thread::sleep_for(std::chrono::seconds(1));

            // Check database health
            if (db_) {
                // Could add health checks here
            }
        }

        std::cout << "📤 Shutting down server..." << std::endl;
    }

    void RunFlightServer() {
        // TODO: Implement Arrow Flight server
        std::cout << "📡 Arrow Flight server thread started" << std::endl;

        while (!g_shutdown_requested) {
            // Flight server logic would go here
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        std::cout << "📡 Arrow Flight server thread stopped" << std::endl;
    }

    void RunRaftServer() {
        // TODO: Implement Raft consensus server
        std::cout << "⚖️  Raft consensus server thread started" << std::endl;

        while (!g_shutdown_requested) {
            // Raft server logic would go here
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        std::cout << "⚖️  Raft consensus server thread stopped" << std::endl;
    }

    ServerConfig config_;
    std::shared_ptr<Database> db_;
    std::thread flight_thread_;
    std::thread raft_thread_;
};

ServerConfig ParseCommandLine(int argc, char* argv[]) {
    ServerConfig config;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];

        if (arg == "--host" && i + 1 < argc) {
            config.host = argv[++i];
        } else if (arg == "--port" && i + 1 < argc) {
            config.port = std::stoi(argv[++i]);
        } else if (arg == "--data-dir" && i + 1 < argc) {
            config.data_dir = argv[++i];
        } else if (arg == "--enable-raft") {
            config.enable_raft = true;
        } else if (arg == "--raft-port" && i + 1 < argc) {
            config.raft_port = std::stoi(argv[++i]);
        } else if (arg == "--raft-cluster" && i + 1 < argc) {
            config.raft_cluster = argv[++i];
        } else if (arg == "--disable-flight") {
            config.enable_flight = false;
        } else if (arg == "--flight-port" && i + 1 < argc) {
            config.flight_port = std::stoi(argv[++i]);
        } else if (arg == "--help" || arg == "-h") {
            std::cout << "MarbleDB Server" << std::endl;
            std::cout << std::endl;
            std::cout << "Usage: " << argv[0] << " [options]" << std::endl;
            std::cout << std::endl;
            std::cout << "Options:" << std::endl;
            std::cout << "  --host HOST          Server host (default: 0.0.0.0)" << std::endl;
            std::cout << "  --port PORT          Server port (default: 8080)" << std::endl;
            std::cout << "  --data-dir DIR       Data directory (default: ./marble_data)" << std::endl;
            std::cout << "  --enable-raft        Enable Raft consensus" << std::endl;
            std::cout << "  --raft-port PORT     Raft port (default: 9090)" << std::endl;
            std::cout << "  --raft-cluster NODES Raft cluster nodes" << std::endl;
            std::cout << "  --disable-flight     Disable Arrow Flight server" << std::endl;
            std::cout << "  --flight-port PORT   Flight port (default: 8081)" << std::endl;
            std::cout << "  --help, -h           Show this help message" << std::endl;
            exit(0);
        }
    }

    return config;
}

} // namespace

int main(int argc, char* argv[]) {
    // Set up signal handlers
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    try {
        // Parse command line arguments
        ServerConfig config = ParseCommandLine(argc, argv);

        // Create and start server
        MarbleServer server(config);
        auto status = server.Start();

        if (!status.ok()) {
            std::cerr << "Failed to start server: " << status.message() << std::endl;
            return 1;
        }

        return 0;

    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        return 1;
    }
}
