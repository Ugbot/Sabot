#pragma once

#include <arrow/api.h>
#include <arrow/result.h>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace sabot_sql {
namespace streaming {

/**
 * Connector offset for exactly-once processing
 * Stored in MarbleDB RAFT table for fault tolerance
 */
struct Offset {
    std::string connector_id;  // Unique connector instance ID
    int partition;             // Partition number (-1 for non-partitioned sources)
    int64_t offset;            // Offset within partition
    int64_t timestamp;         // Event time at this offset
    
    // Serialize for MarbleDB storage
    std::string ToJson() const;
    static arrow::Result<Offset> FromJson(const std::string& json);
};

/**
 * Configuration for source connector instances
 */
struct ConnectorConfig {
    std::string connector_type;  // "kafka", "pulsar", "kinesis", "file", etc.
    std::string connector_id;    // Unique ID for this connector instance
    std::unordered_map<std::string, std::string> properties;
    
    // Watermark configuration
    std::string watermark_column;  // Column name for event time
    std::string watermark_strategy = "bounded";  // "bounded" | "periodic"
    int64_t max_out_of_orderness_ms = 5000;  // Max delay for out-of-order events
    
    // Batch configuration
    size_t batch_size = 10000;  // Target rows per batch
    int64_t batch_timeout_ms = 1000;  // Max wait time for batch
    
    // Partition configuration (for partitioned sources like Kafka)
    int partition_id = -1;  // Specific partition to consume (-1 = all)
    size_t max_parallelism = 16;  // Max partition consumers
};

/**
 * Generic source connector interface
 * 
 * Extensible interface for building streaming data sources.
 * Implementations: Kafka, Pulsar, Kinesis, files, etc.
 */
class SourceConnector {
public:
    virtual ~SourceConnector() = default;
    
    // ========== Lifecycle ==========
    
    /**
     * Initialize connector with configuration
     * Sets up client connections, authenticates, discovers partitions
     */
    virtual arrow::Status Initialize(const ConnectorConfig& config) = 0;
    
    /**
     * Shutdown connector gracefully
     * Commits final offsets, closes connections
     */
    virtual arrow::Status Shutdown() = 0;
    
    // ========== Data Ingestion ==========
    
    /**
     * Fetch next batch of records
     * 
     * @param max_rows Maximum rows to return (hint, may return less)
     * @return RecordBatch with schema matching source definition
     * 
     * Returns empty batch if no data available (non-blocking)
     * Returns error on connector failure
     */
    virtual arrow::Result<std::shared_ptr<arrow::RecordBatch>> 
    GetNextBatch(size_t max_rows) = 0;
    
    /**
     * Check if source has more data
     * For unbounded sources (Kafka, Pulsar), always returns true
     * For bounded sources (files), returns false at end
     */
    virtual bool HasMore() const = 0;
    
    // ========== Offset Management (Exactly-Once) ==========
    
    /**
     * Commit offset to persistent storage (MarbleDB RAFT)
     * Called during checkpoint to ensure exactly-once semantics
     */
    virtual arrow::Status CommitOffset(const Offset& offset) = 0;
    
    /**
     * Get current read position
     * Used for checkpoint and monitoring
     */
    virtual arrow::Result<Offset> GetCurrentOffset() const = 0;
    
    /**
     * Seek to specific offset (for recovery)
     * Called when resuming from checkpoint
     */
    virtual arrow::Status SeekToOffset(const Offset& offset) = 0;
    
    // ========== Watermark Extraction (Event-Time) ==========
    
    /**
     * Extract watermark from batch
     * Watermark = max(event_time) - max_out_of_orderness
     * 
     * @param batch Batch to extract watermark from
     * @return Watermark timestamp in milliseconds
     */
    virtual arrow::Result<int64_t> 
    ExtractWatermark(const std::shared_ptr<arrow::RecordBatch>& batch) = 0;
    
    // ========== Partitioning Info ==========
    
    /**
     * Get total partition count for this source
     * Used to determine parallelism (one consumer per partition)
     */
    virtual size_t GetPartitionCount() const = 0;
    
    /**
     * Get connector type identifier
     */
    virtual std::string GetConnectorType() const = 0;
    
    /**
     * Get connector instance ID
     */
    virtual std::string GetConnectorId() const = 0;
    
    /**
     * Get schema of output batches
     */
    virtual arrow::Result<std::shared_ptr<arrow::Schema>> GetSchema() const = 0;
};

/**
 * Factory for creating connector instances
 */
class ConnectorFactory {
public:
    /**
     * Register a connector type
     */
    using ConnectorCreator = std::function<
        std::unique_ptr<SourceConnector>(const ConnectorConfig&)
    >;
    
    static void RegisterConnector(
        const std::string& type,
        ConnectorCreator creator
    );
    
    /**
     * Create connector instance by type
     */
    static arrow::Result<std::unique_ptr<SourceConnector>> 
    CreateConnector(const ConnectorConfig& config);
    
private:
    static std::unordered_map<std::string, ConnectorCreator>& GetRegistry();
};

/**
 * Helper macro for registering connectors
 */
#define REGISTER_CONNECTOR(type, class_name) \
    namespace { \
        struct class_name##_Registrar { \
            class_name##_Registrar() { \
                ConnectorFactory::RegisterConnector( \
                    type, \
                    [](const ConnectorConfig& config) { \
                        return std::make_unique<class_name>(); \
                    } \
                ); \
            } \
        }; \
        static class_name##_Registrar class_name##_registrar; \
    }

} // namespace streaming
} // namespace sabot_sql

