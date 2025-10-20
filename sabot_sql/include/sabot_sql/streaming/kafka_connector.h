#pragma once

#include "sabot_sql/streaming/source_connector.h"
#include <librdkafka/rdkafkacpp.h>
#include <memory>
#include <atomic>

namespace marble {
    class Client;  // Forward declare MarbleDB client
}

namespace sabot_sql {
namespace streaming {

/**
 * Kafka source connector implementation
 * 
 * Features:
 * - One consumer per partition for maximum parallelism
 * - Offset storage in MarbleDB RAFT for fault tolerance
 * - Watermark extraction from configured event-time column
 * - Batched consumption with configurable batch size
 * - Zero-copy Arrow conversion where possible
 */
class KafkaConnector : public SourceConnector {
public:
    KafkaConnector();
    ~KafkaConnector() override;
    
    // ========== Lifecycle ==========
    arrow::Status Initialize(const ConnectorConfig& config) override;
    arrow::Status Shutdown() override;
    
    // ========== Data Ingestion ==========
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> 
    GetNextBatch(size_t max_rows) override;
    
    bool HasMore() const override { return true; }  // Kafka is unbounded
    
    // ========== Offset Management ==========
    arrow::Status CommitOffset(const Offset& offset) override;
    arrow::Result<Offset> GetCurrentOffset() const override;
    arrow::Status SeekToOffset(const Offset& offset) override;
    
    // ========== Watermark Extraction ==========
    arrow::Result<int64_t> 
    ExtractWatermark(const std::shared_ptr<arrow::RecordBatch>& batch) override;
    
    // ========== Partitioning Info ==========
    size_t GetPartitionCount() const override;
    std::string GetConnectorType() const override { return "kafka"; }
    std::string GetConnectorId() const override { return config_.connector_id; }
    arrow::Result<std::shared_ptr<arrow::Schema>> GetSchema() const override;
    
private:
    // Configuration
    ConnectorConfig config_;
    
    // Kafka client
    std::unique_ptr<RdKafka::KafkaConsumer> consumer_;
    std::string topic_;
    int partition_id_;
    
    // Schema (inferred or provided)
    std::shared_ptr<arrow::Schema> schema_;
    
    // MarbleDB client for offset storage (RAFT table)
    std::shared_ptr<marble::Client> offset_store_;
    
    // Current state
    mutable std::atomic<int64_t> current_offset_{0};
    mutable std::atomic<int64_t> last_watermark_{0};
    size_t partition_count_{0};
    
    // Helper methods
    arrow::Status InitializeKafkaConsumer();
    arrow::Status InitializeOffsetStore();
    arrow::Status DiscoverPartitions();
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> 
    ConvertMessagesToBatch(const std::vector<RdKafka::Message*>& messages);
    arrow::Result<int64_t> ExtractTimestampFromBatch(
        const std::shared_ptr<arrow::RecordBatch>& batch,
        const std::string& column_name
    );
};

} // namespace streaming
} // namespace sabot_sql

