# MarbleDB Raft Consensus Integration

This document describes the Raft consensus integration in MarbleDB for distributed, fault-tolerant database clusters.

## Overview

MarbleDB now includes Raft consensus support using the NuRaft library, enabling distributed database clusters with automatic leader election, fault tolerance, and strong consistency guarantees.

## Architecture

### Key Components

1. **Raft Interface** (`include/marble/raft.h`)
   - `RaftServer`: Main consensus interface for cluster operations
   - `RaftStateMachine`: Applies committed operations to MarbleDB state
   - `RaftLogStore`: Persistent storage for Raft log entries
   - `RaftTransport`: Inter-node communication (Arrow Flight-based)
   - `RaftOperation`: Structured operation payloads
   - `RaftClusterConfig`: Cluster configuration settings

2. **Transport Layer** (`src/raft/arrow_flight_transport.cpp`)
   - Arrow Flight-based communication between Raft nodes
   - High-performance wire protocol for data services
   - Asynchronous message passing

3. **State Machines** (`src/raft/`)
   - CounterStateMachine: Simple example state machine
   - MarbleDB-specific state machines (planned):
     - WAL replication state machine
     - Schema change state machine
     - Transaction coordination state machine

### Operation Types

```cpp
enum class RaftOperationType {
    kWalEntry,         // WAL entry replication
    kSchemaChange,     // Schema modifications
    kTableCreate,      // Table creation
    kTableDrop,        // Table deletion
    kIndexCreate,      // Index creation
    kIndexDrop,        // Index deletion
    kConfigChange,     // Cluster configuration change
    kSnapshot,         // Snapshot operation
    kCustom           // Custom operation
};
```

## Building

Raft support is enabled by default. To build with Raft support:

```bash
cd MarbleDB/build
cmake .. -DMARBLE_ENABLE_RAFT=ON
make
```

## Usage

### Basic Cluster Setup

```cpp
#include "marble/raft.h"

// Create cluster configuration
RaftClusterConfig config;
config.cluster_id = "my-cluster";
config.node_endpoints = {"localhost:50051", "localhost:50052", "localhost:50053"};
config.election_timeout_ms = 1000;
config.heartbeat_interval_ms = 100;

// Create components
auto state_machine = std::make_unique<MyStateMachine>();
auto log_store = CreateInMemoryLogStore();  // or CreateMarbleLogStore()
auto transport = CreateArrowFlightTransport("localhost:50051");

// Create and start Raft server
auto raft_server = CreateRaftServer(
    std::move(state_machine),
    std::move(log_store),
    std::move(transport)
);

raft_server->Initialize(config);
raft_server->Start();

// Propose operations
RaftOperation op;
op.type = RaftOperationType::kCustom;
op.data = "my operation data";

raft_server->ProposeOperation(std::make_unique<RaftOperation>(op));
```

### Distributed Example

See `examples/distributed_raft_example.cpp` for a complete example of setting up a 3-node Raft cluster.

## Configuration

### Cluster Configuration

- **cluster_id**: Unique identifier for the cluster
- **node_endpoints**: List of "host:port" endpoints for all cluster nodes
- **election_timeout_ms**: Leader election timeout (default: 1000ms)
- **heartbeat_interval_ms**: Heartbeat interval between nodes (default: 100ms)
- **snapshot_distance**: Number of log entries between snapshots
- **max_entry_size**: Maximum size of individual log entries

### Transport Configuration

The Arrow Flight transport automatically handles:
- Connection management between nodes
- Message serialization/deserialization
- Asynchronous message passing
- Error handling and retries

## State Machines

### Implementing Custom State Machines

```cpp
class MyStateMachine : public RaftStateMachine {
public:
    Status ApplyOperation(const RaftOperation& operation) override {
        // Apply the operation to your state
        switch (operation.type) {
            case RaftOperationType::kCustom:
                // Handle custom operation
                break;
            // ... other cases
        }
        return Status::OK();
    }

    Status CreateSnapshot(uint64_t log_index) override {
        // Create a snapshot of current state
        return Status::OK();
    }

    Status RestoreFromSnapshot(uint64_t log_index) override {
        // Restore state from snapshot
        return Status::OK();
    }

    uint64_t GetLastAppliedIndex() const override {
        return last_applied_index_;
    }

private:
    uint64_t last_applied_index_ = 0;
};
```

## Log Storage

### In-Memory Log Store

For testing and development:
```cpp
auto log_store = CreateInMemoryLogStore();
```

### MarbleDB Log Store (Planned)

For production use with persistent storage:
```cpp
auto log_store = CreateMarbleLogStore("/path/to/log/storage");
```

## Transport Layer

### Arrow Flight Transport

```cpp
auto transport = CreateArrowFlightTransport("localhost:50051");
```

The Arrow Flight transport provides:
- High-performance inter-node communication
- Automatic connection management
- Message queuing and delivery guarantees
- Integration with Arrow's columnar format

## Examples

### Running the Distributed Example

```bash
cd MarbleDB/build
make distributed_raft_example
./distributed_raft_example
```

This will start a 3-node Raft cluster and demonstrate:
- Leader election
- Operation replication
- Fault tolerance
- Consistency verification

### Demo Script

```bash
./raft_demo.sh
```

Shows the current integration status and capabilities.

## Current Status

✅ **Completed:**
- NuRaft submodule integration
- Raft consensus interface design
- Arrow Flight transport layer
- Basic state machine examples
- Distributed cluster example
- CMake build integration

🔄 **In Progress:**
- MarbleDB-specific state machines (WAL, Schema, Transactions)
- Integration with existing MarbleDB storage layer

⏳ **Planned:**
- Performance benchmarking
- Production deployment examples
- Advanced features (snapshots, reconfiguration)

## Benefits

- **Distributed Clusters**: Scale MarbleDB across multiple nodes
- **Fault Tolerance**: Automatic failover and data replication
- **Strong Consistency**: Raft guarantees linearizable operations
- **High Performance**: Arrow Flight for efficient communication
- **Extensible**: Clean interfaces for adding new operation types

## Dependencies

- **NuRaft**: C++ Raft consensus implementation
- **Apache Arrow Flight**: High-performance data transport
- **Boost.Asio**: Networking (included via NuRaft)
- **OpenSSL**: TLS support (optional)

## Testing

The integration includes:
- Unit tests for individual components (planned)
- Integration tests for cluster operations (planned)
- Performance benchmarks (planned)
- Fault injection testing (planned)

## Future Work

1. **MarbleDB State Machines**: Implement state machines for WAL replication, schema changes, and transaction coordination
2. **Persistent Log Store**: Integrate with MarbleDB's storage engine for durable log storage
3. **Snapshot Support**: Implement efficient snapshot creation and restoration
4. **Dynamic Configuration**: Support for adding/removing nodes from running clusters
5. **Security**: TLS encryption for inter-node communication
6. **Monitoring**: Metrics and observability for cluster health

## Contributing

When contributing to the Raft integration:

1. Follow the existing code patterns in `src/raft/`
2. Add comprehensive tests for new functionality
3. Update documentation for new features
4. Ensure compatibility with existing MarbleDB APIs

## References

- [Raft Consensus Algorithm](https://raft.github.io/)
- [NuRaft Documentation](https://github.com/eBay/NuRaft)
- [Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html)
