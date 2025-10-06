#pragma once

#include <memory>
#include <string>
#include <vector>
#include <functional>
#include <marble/status.h>
#include <marble/record.h>
#include <arrow/api.h>

namespace marble {

// Stream options for configuration
struct StreamOptions {
    // Maximum number of records to buffer in memory
    size_t max_buffer_size = 1000;

    // Maximum time to wait for new data (milliseconds)
    uint64_t max_wait_time_ms = 1000;

    // Enable compression for network transport
    bool enable_compression = true;

    enum class CompressionType {
        kLZ4,
        kZSTD,
        kSnappy
    };
    CompressionType compression_type = CompressionType::kLZ4;

    // Stream persistence options
    bool persistent = false;  // Keep stream data on disk
    std::string persistence_path;  // Path for persistent storage
};

// Change data capture types for incremental updates
enum class ChangeType {
    kInsert,
    kUpdate,
    kDelete,
    kCheckpoint  // Full state snapshot
};

struct ChangeRecord {
    ChangeType type;
    std::shared_ptr<Key> key;
    std::shared_ptr<Record> old_value;  // For updates/deletes
    std::shared_ptr<Record> new_value;  // For inserts/updates
    uint64_t timestamp;
    uint64_t sequence_number;  // For ordering
};

// Stream interface for continuous data flow between operators/agents
class Stream {
public:
    Stream() = default;
    virtual ~Stream() = default;

    // Producer interface - sending data to next operator
    virtual Status SendRecord(std::shared_ptr<Record> record) = 0;
    virtual Status SendRecords(const std::vector<std::shared_ptr<Record>>& records) = 0;

    // Send change data capture records for incremental updates
    virtual Status SendChange(const ChangeRecord& change) = 0;
    virtual Status SendChanges(const std::vector<ChangeRecord>& changes) = 0;

    // Consumer interface - receiving data from previous operator
    virtual Status ReceiveRecord(std::shared_ptr<Record>* record, bool blocking = true) = 0;
    virtual Status ReceiveRecords(std::vector<std::shared_ptr<Record>>* records,
                                 size_t max_count = 0, bool blocking = true) = 0;

    // Receive change data capture records
    virtual Status ReceiveChange(ChangeRecord* change, bool blocking = true) = 0;
    virtual Status ReceiveChanges(std::vector<ChangeRecord>* changes,
                                 size_t max_count = 0, bool blocking = true) = 0;

    // Batch processing with Arrow RecordBatches for efficiency
    virtual Status SendRecordBatch(std::shared_ptr<arrow::RecordBatch> batch) = 0;
    virtual Status ReceiveRecordBatch(std::shared_ptr<arrow::RecordBatch>* batch, bool blocking = true) = 0;

    // Stream control
    virtual Status Flush() = 0;  // Ensure all data is sent
    virtual Status Close() = 0;  // Close the stream

    // Stream status
    virtual bool IsOpen() const = 0;
    virtual size_t GetPendingRecords() const = 0;
    virtual uint64_t GetLastSequenceNumber() const = 0;

    // Stream metadata
    virtual const std::string& GetName() const = 0;
    virtual const StreamOptions& GetOptions() const = 0;

    // Disable copying
    Stream(const Stream&) = delete;
    Stream& operator=(const Stream&) = delete;
};

// Stream factory for creating different types of streams
class StreamFactory {
public:
    virtual ~StreamFactory() = default;

    // Create an in-memory stream (for same-process communication)
    virtual Status CreateMemoryStream(const std::string& name,
                                     const StreamOptions& options,
                                     std::unique_ptr<Stream>* stream) = 0;

    // Create a file-based stream (for persistence and cross-process)
    virtual Status CreateFileStream(const std::string& name,
                                   const std::string& file_path,
                                   const StreamOptions& options,
                                   std::unique_ptr<Stream>* stream) = 0;

    // Create a network stream (for distributed processing)
    virtual Status CreateNetworkStream(const std::string& name,
                                      const std::string& endpoint,
                                      const StreamOptions& options,
                                      std::unique_ptr<Stream>* stream) = 0;

    // Create a shared memory stream (for high-performance local communication)
    virtual Status CreateSharedMemoryStream(const std::string& name,
                                           const std::string& shm_key,
                                           const StreamOptions& options,
                                           std::unique_ptr<Stream>* stream) = 0;
};

// Checkpoint interface for state persistence and restoration
class CheckpointManager {
public:
    virtual ~CheckpointManager() = default;

    // Create a checkpoint of current state
    virtual Status CreateCheckpoint(const std::string& checkpoint_id,
                                   const std::string& path) = 0;

    // Restore state from a checkpoint
    virtual Status RestoreCheckpoint(const std::string& checkpoint_id,
                                    const std::string& path) = 0;

    // List available checkpoints
    virtual Status ListCheckpoints(std::vector<std::string>* checkpoints) const = 0;

    // Delete a checkpoint
    virtual Status DeleteCheckpoint(const std::string& checkpoint_id) = 0;

    // Get checkpoint metadata
    virtual Status GetCheckpointMetadata(const std::string& checkpoint_id,
                                        std::string* metadata) const = 0;

    // Incremental checkpointing
    virtual Status CreateIncrementalCheckpoint(const std::string& base_checkpoint_id,
                                              const std::string& new_checkpoint_id,
                                              const std::string& path) = 0;
};

// Operator/Agent communication interface
class OperatorInterface {
public:
    virtual ~OperatorInterface() = default;

    // Initialize operator with input/output streams
    virtual Status Initialize(const std::vector<std::unique_ptr<Stream>>& input_streams,
                             const std::vector<std::unique_ptr<Stream>>& output_streams) = 0;

    // Process data from input streams to output streams
    virtual Status Process() = 0;

    // Handle checkpoint requests
    virtual Status Checkpoint(const std::string& checkpoint_id) = 0;
    virtual Status Restore(const std::string& checkpoint_id) = 0;

    // Get operator status and metrics
    virtual Status GetStatus(std::string* status) const = 0;
    virtual Status GetMetrics(std::string* metrics) const = 0;

    // Graceful shutdown
    virtual Status Shutdown() = 0;
};

// Serialization utilities for network transport
class Serializer {
public:
    virtual ~Serializer() = default;

    // Serialize a record for network transport
    virtual Status SerializeRecord(const Record& record,
                                  std::shared_ptr<arrow::Buffer>* buffer) = 0;

    // Deserialize a record from network transport
    virtual Status DeserializeRecord(const std::shared_ptr<arrow::Buffer>& buffer,
                                    std::shared_ptr<Record>* record) = 0;

    // Serialize change records
    virtual Status SerializeChange(const ChangeRecord& change,
                                  std::shared_ptr<arrow::Buffer>* buffer) = 0;

    virtual Status DeserializeChange(const std::shared_ptr<arrow::Buffer>& buffer,
                                    ChangeRecord* change) = 0;

    // Batch serialization for efficiency
    virtual Status SerializeRecordBatch(const std::shared_ptr<arrow::RecordBatch>& batch,
                                       std::shared_ptr<arrow::Buffer>* buffer) = 0;

    virtual Status DeserializeRecordBatch(const std::shared_ptr<arrow::Buffer>& buffer,
                                         std::shared_ptr<arrow::RecordBatch>* batch) = 0;
};

// Factory functions
std::unique_ptr<StreamFactory> CreateStreamFactory();
std::unique_ptr<CheckpointManager> CreateCheckpointManager(const std::string& base_path);
std::unique_ptr<Serializer> CreateSerializer(std::shared_ptr<Schema> schema);

} // namespace marble
