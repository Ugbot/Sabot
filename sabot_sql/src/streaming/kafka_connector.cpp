#include "sabot_sql/streaming/kafka_connector.h"
#include <arrow/compute/api.h>
#include <sstream>

namespace sabot_sql {
namespace streaming {

KafkaConnector::KafkaConnector() = default;
KafkaConnector::~KafkaConnector() {
    if (consumer_) {
        consumer_->close();
    }
}

// ========== Lifecycle ==========

arrow::Status KafkaConnector::Initialize(const ConnectorConfig& config) {
    config_ = config;
    
    // Extract Kafka-specific properties
    auto it = config.properties.find("topic");
    if (it == config.properties.end()) {
        return arrow::Status::Invalid("Kafka connector requires 'topic' property");
    }
    topic_ = it->second;
    
    // Set partition ID (default to config partition_id)
    partition_id_ = config.partition_id;
    
    // Initialize Kafka consumer
    ARROW_RETURN_NOT_OK(InitializeKafkaConsumer());
    
    // Discover partition count
    ARROW_RETURN_NOT_OK(DiscoverPartitions());
    
    // Initialize MarbleDB offset store
    ARROW_RETURN_NOT_OK(InitializeOffsetStore());
    
    return arrow::Status::OK();
}

arrow::Status KafkaConnector::Shutdown() {
    if (consumer_) {
        consumer_->close();
        consumer_.reset();
    }
    return arrow::Status::OK();
}

arrow::Status KafkaConnector::InitializeKafkaConsumer() {
    std::string errstr;
    
    // Create Kafka configuration
    auto conf = std::unique_ptr<RdKafka::Conf>(
        RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL)
    );
    
    // Set bootstrap servers
    auto it = config_.properties.find("bootstrap.servers");
    if (it != config_.properties.end()) {
        if (conf->set("bootstrap.servers", it->second, errstr) != RdKafka::Conf::CONF_OK) {
            return arrow::Status::Invalid("Failed to set bootstrap.servers: " + errstr);
        }
    } else {
        return arrow::Status::Invalid("Kafka connector requires 'bootstrap.servers' property");
    }
    
    // Set group ID
    it = config_.properties.find("group.id");
    if (it != config_.properties.end()) {
        if (conf->set("group.id", it->second, errstr) != RdKafka::Conf::CONF_OK) {
            return arrow::Status::Invalid("Failed to set group.id: " + errstr);
        }
    } else {
        // Default group ID
        if (conf->set("group.id", "sabot_sql_" + config_.connector_id, errstr) != RdKafka::Conf::CONF_OK) {
            return arrow::Status::Invalid("Failed to set default group.id: " + errstr);
        }
    }
    
    // Disable auto commit (we commit via MarbleDB RAFT)
    if (conf->set("enable.auto.commit", "false", errstr) != RdKafka::Conf::CONF_OK) {
        return arrow::Status::Invalid("Failed to disable auto commit: " + errstr);
    }
    
    // Set other properties from config
    for (const auto& [key, value] : config_.properties) {
        if (key != "topic" && key != "bootstrap.servers" && key != "group.id") {
            conf->set(key, value, errstr);  // Ignore errors for optional properties
        }
    }
    
    // Create consumer
    consumer_.reset(RdKafka::KafkaConsumer::create(conf.get(), errstr));
    if (!consumer_) {
        return arrow::Status::Invalid("Failed to create Kafka consumer: " + errstr);
    }
    
    // Subscribe to topic or assign partition
    if (partition_id_ >= 0) {
        // Specific partition assignment
        std::vector<RdKafka::TopicPartition*> partitions;
        partitions.push_back(
            RdKafka::TopicPartition::create(topic_, partition_id_)
        );
        
        RdKafka::ErrorCode err = consumer_->assign(partitions);
        
        // Clean up
        for (auto* p : partitions) {
            delete p;
        }
        
        if (err != RdKafka::ERR_NO_ERROR) {
            return arrow::Status::Invalid(
                "Failed to assign partition: " + RdKafka::err2str(err)
            );
        }
    } else {
        // Subscribe to all partitions
        std::vector<std::string> topics = {topic_};
        RdKafka::ErrorCode err = consumer_->subscribe(topics);
        if (err != RdKafka::ERR_NO_ERROR) {
            return arrow::Status::Invalid(
                "Failed to subscribe to topic: " + RdKafka::err2str(err)
            );
        }
    }
    
    return arrow::Status::OK();
}

arrow::Status KafkaConnector::InitializeOffsetStore() {
    // TODO: Initialize MarbleDB client for offset storage
    // For now, we'll use Kafka's internal offset management
    // In production, this would connect to MarbleDB RAFT table
    return arrow::Status::OK();
}

arrow::Status KafkaConnector::DiscoverPartitions() {
    // Get metadata for topic
    RdKafka::Metadata* metadata = nullptr;
    RdKafka::ErrorCode err = consumer_->metadata(
        false,  // all topics = false
        nullptr,  // only_rkt = nullptr (use topic filter)
        &metadata,
        5000  // timeout_ms
    );
    
    if (err != RdKafka::ERR_NO_ERROR) {
        return arrow::Status::Invalid(
            "Failed to get topic metadata: " + RdKafka::err2str(err)
        );
    }
    
    std::unique_ptr<RdKafka::Metadata> metadata_ptr(metadata);
    
    // Find our topic and count partitions
    for (auto it = metadata->topics()->begin(); it != metadata->topics()->end(); ++it) {
        if ((*it)->topic() == topic_) {
            partition_count_ = (*it)->partitions()->size();
            break;
        }
    }
    
    if (partition_count_ == 0) {
        return arrow::Status::Invalid("Topic not found or has no partitions: " + topic_);
    }
    
    return arrow::Status::OK();
}

// ========== Data Ingestion ==========

arrow::Result<std::shared_ptr<arrow::RecordBatch>> 
KafkaConnector::GetNextBatch(size_t max_rows) {
    std::vector<RdKafka::Message*> messages;
    messages.reserve(max_rows);
    
    // Poll messages up to max_rows
    size_t timeout_ms = config_.batch_timeout_ms;
    size_t poll_interval_ms = 100;
    size_t elapsed_ms = 0;
    
    while (messages.size() < max_rows && elapsed_ms < timeout_ms) {
        RdKafka::Message* msg = consumer_->consume(poll_interval_ms);
        
        if (msg->err() == RdKafka::ERR_NO_ERROR) {
            messages.push_back(msg);
            current_offset_ = msg->offset();
        } else if (msg->err() == RdKafka::ERR__PARTITION_EOF) {
            // End of partition, no more messages currently available
            delete msg;
            break;
        } else if (msg->err() != RdKafka::ERR__TIMED_OUT) {
            // Real error
            std::string error = msg->errstr();
            delete msg;
            for (auto* m : messages) {
                delete m;
            }
            return arrow::Status::IOError("Kafka consume error: " + error);
        } else {
            // Timeout, continue polling
            delete msg;
        }
        
        elapsed_ms += poll_interval_ms;
    }
    
    // Convert messages to Arrow batch
    auto result = ConvertMessagesToBatch(messages);
    
    // Clean up messages
    for (auto* msg : messages) {
        delete msg;
    }
    
    return result;
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> 
KafkaConnector::ConvertMessagesToBatch(
    const std::vector<RdKafka::Message*>& messages
) {
    if (messages.empty()) {
        // Return empty batch with correct schema
        if (!schema_) {
            // Create default schema (will be overridden when we get first message)
            schema_ = arrow::schema({
                arrow::field("key", arrow::utf8()),
                arrow::field("value", arrow::utf8()),
                arrow::field("timestamp", arrow::timestamp(arrow::TimeUnit::MILLI)),
                arrow::field("partition", arrow::int32()),
                arrow::field("offset", arrow::int64())
            });
        }
        std::vector<std::shared_ptr<arrow::Array>> empty_arrays;
        return arrow::RecordBatch::Make(schema_, 0, empty_arrays);
    }
    
    // For now, simple JSON parsing of message values
    // TODO: Support Avro, Protobuf, etc. via schema registry
    
    // Build arrays for each column
    arrow::StringBuilder key_builder;
    arrow::StringBuilder value_builder;
    arrow::TimestampBuilder timestamp_builder(
        arrow::timestamp(arrow::TimeUnit::MILLI),
        arrow::default_memory_pool()
    );
    arrow::Int32Builder partition_builder;
    arrow::Int64Builder offset_builder;
    
    for (const auto* msg : messages) {
        // Key (may be null)
        if (msg->key() && msg->key_len() > 0) {
            ARROW_RETURN_NOT_OK(key_builder.Append(
                reinterpret_cast<const char*>(msg->key()),
                static_cast<int32_t>(msg->key_len())
            ));
        } else {
            ARROW_RETURN_NOT_OK(key_builder.AppendNull());
        }
        
        // Value
        ARROW_RETURN_NOT_OK(value_builder.Append(
            reinterpret_cast<const char*>(msg->payload()),
            static_cast<int32_t>(msg->len())
        ));
        
        // Timestamp
        ARROW_RETURN_NOT_OK(timestamp_builder.Append(msg->timestamp().timestamp));
        
        // Partition
        ARROW_RETURN_NOT_OK(partition_builder.Append(msg->partition()));
        
        // Offset
        ARROW_RETURN_NOT_OK(offset_builder.Append(msg->offset()));
    }
    
    // Finish arrays
    std::shared_ptr<arrow::Array> key_array;
    std::shared_ptr<arrow::Array> value_array;
    std::shared_ptr<arrow::Array> timestamp_array;
    std::shared_ptr<arrow::Array> partition_array;
    std::shared_ptr<arrow::Array> offset_array;
    
    ARROW_RETURN_NOT_OK(key_builder.Finish(&key_array));
    ARROW_RETURN_NOT_OK(value_builder.Finish(&value_array));
    ARROW_RETURN_NOT_OK(timestamp_builder.Finish(&timestamp_array));
    ARROW_RETURN_NOT_OK(partition_builder.Finish(&partition_array));
    ARROW_RETURN_NOT_OK(offset_builder.Finish(&offset_array));
    
    // Create schema if not exists
    if (!schema_) {
        schema_ = arrow::schema({
            arrow::field("key", arrow::utf8()),
            arrow::field("value", arrow::utf8()),
            arrow::field("timestamp", arrow::timestamp(arrow::TimeUnit::MILLI)),
            arrow::field("partition", arrow::int32()),
            arrow::field("offset", arrow::int64())
        });
    }
    
    // Create record batch
    return arrow::RecordBatch::Make(
        schema_,
        messages.size(),
        {key_array, value_array, timestamp_array, partition_array, offset_array}
    );
}

// ========== Offset Management ==========

arrow::Status KafkaConnector::CommitOffset(const Offset& offset) {
    // Create topic partition with offset
    std::vector<RdKafka::TopicPartition*> partitions;
    partitions.push_back(
        RdKafka::TopicPartition::create(
            topic_,
            offset.partition,
            offset.offset
        )
    );
    
    // Commit to Kafka
    RdKafka::ErrorCode err = consumer_->commitSync(partitions);
    
    // Clean up
    for (auto* p : partitions) {
        delete p;
    }
    
    if (err != RdKafka::ERR_NO_ERROR) {
        return arrow::Status::IOError(
            "Failed to commit offset: " + RdKafka::err2str(err)
        );
    }
    
    // TODO: Also commit to MarbleDB RAFT for fault tolerance
    
    return arrow::Status::OK();
}

arrow::Result<Offset> KafkaConnector::GetCurrentOffset() const {
    Offset offset;
    offset.connector_id = config_.connector_id;
    offset.partition = partition_id_;
    offset.offset = current_offset_.load();
    offset.timestamp = last_watermark_.load();
    return offset;
}

arrow::Status KafkaConnector::SeekToOffset(const Offset& offset) {
    std::vector<RdKafka::TopicPartition*> partitions;
    partitions.push_back(
        RdKafka::TopicPartition::create(
            topic_,
            offset.partition,
            offset.offset
        )
    );
    
    RdKafka::ErrorCode err = consumer_->assign(partitions);
    
    // Clean up
    for (auto* p : partitions) {
        delete p;
    }
    
    if (err != RdKafka::ERR_NO_ERROR) {
        return arrow::Status::IOError(
            "Failed to seek to offset: " + RdKafka::err2str(err)
        );
    }
    
    current_offset_ = offset.offset;
    return arrow::Status::OK();
}

// ========== Watermark Extraction ==========

arrow::Result<int64_t> 
KafkaConnector::ExtractWatermark(
    const std::shared_ptr<arrow::RecordBatch>& batch
) {
    if (batch->num_rows() == 0) {
        return last_watermark_.load();
    }
    
    // Extract timestamp from watermark column
    ARROW_ASSIGN_OR_RAISE(
        int64_t max_timestamp,
        ExtractTimestampFromBatch(batch, config_.watermark_column)
    );
    
    // Watermark = max(event_time) - max_out_of_orderness
    int64_t watermark = max_timestamp - config_.max_out_of_orderness_ms;
    
    // Watermarks must be monotonically increasing
    int64_t current_watermark = last_watermark_.load();
    if (watermark > current_watermark) {
        last_watermark_ = watermark;
        return watermark;
    }
    
    return current_watermark;
}

arrow::Result<int64_t> 
KafkaConnector::ExtractTimestampFromBatch(
    const std::shared_ptr<arrow::RecordBatch>& batch,
    const std::string& column_name
) {
    // Find timestamp column
    auto schema = batch->schema();
    int col_index = schema->GetFieldIndex(column_name);
    
    if (col_index < 0) {
        return arrow::Status::Invalid(
            "Watermark column not found: " + column_name
        );
    }
    
    auto column = batch->column(col_index);
    
    // Compute max timestamp
    ARROW_ASSIGN_OR_RAISE(
        auto max_result,
        arrow::compute::CallFunction("max", {column})
    );
    
    // Extract scalar value
    auto max_scalar = std::dynamic_pointer_cast<arrow::TimestampScalar>(
        max_result.scalar()
    );
    
    if (!max_scalar) {
        return arrow::Status::Invalid("Watermark column is not a timestamp");
    }
    
    return max_scalar->value;
}

// ========== Partitioning Info ==========

size_t KafkaConnector::GetPartitionCount() const {
    return partition_count_;
}

arrow::Result<std::shared_ptr<arrow::Schema>> 
KafkaConnector::GetSchema() const {
    if (!schema_) {
        return arrow::Status::Invalid("Schema not yet available (no data consumed)");
    }
    return schema_;
}

// ========== Connector Registration ==========

namespace {
    struct KafkaConnectorRegistrar {
        KafkaConnectorRegistrar() {
            ConnectorFactory::RegisterConnector(
                "kafka",
                [](const ConnectorConfig& config) {
                    return std::make_unique<KafkaConnector>();
                }
            );
        }
    };
    static KafkaConnectorRegistrar kafka_connector_registrar;
}

} // namespace streaming
} // namespace sabot_sql

