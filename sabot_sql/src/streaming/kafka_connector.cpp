#include "sabot_sql/streaming/kafka_connector.h"
// #include "sabot_sql/streaming/avro_decoder.h"  // TODO: Fix and re-enable
// #include "sabot_sql/streaming/protobuf_decoder.h"  // TODO: Fix and re-enable
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
    
    // Initialize Schema Registry if URL is provided
    ARROW_RETURN_NOT_OK(InitializeSchemaRegistry());
    
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
    // Use Schema Registry if configured, otherwise use simdjson for JSON
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> result;
    
    if (schema_registry_) {
        // Check if first message has Schema Registry wire format
        if (!messages.empty() && messages[0]->len() >= 5) {
            const uint8_t* data = reinterpret_cast<const uint8_t*>(messages[0]->payload());
            if (data[0] == 0x00) {
                // Magic byte present - use Schema Registry decoding
                result = DecodeWithSchemaRegistry(messages);
            } else {
                // No magic byte - use regular JSON decoding
                result = ConvertMessagesToBatch(messages);
            }
        } else {
            result = ConvertMessagesToBatch(messages);
        }
    } else {
        // No Schema Registry - use simdjson for JSON
        result = ConvertMessagesToBatch(messages);
    }
    
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
    
    // Use simdjson for fast JSON parsing
    std::vector<simdjson::padded_string> json_docs;
    json_docs.reserve(messages.size());
    
    // First pass: Parse all JSON documents
    for (const auto* msg : messages) {
        if (msg->len() > 0) {
            json_docs.emplace_back(
                reinterpret_cast<const char*>(msg->payload()),
                msg->len()
            );
        }
    }
    
    // Infer or validate schema from first document
    if (!schema_ && !json_docs.empty()) {
        auto first_doc = json_parser_.iterate(json_docs[0]);
        if (first_doc.error()) {
            return arrow::Status::Invalid("Failed to parse first JSON document");
        }
        ARROW_ASSIGN_OR_RAISE(schema_, InferSchemaFromJSON(first_doc.value()));
    }
    
    // Build arrays for metadata columns (always present)
    arrow::StringBuilder key_builder;
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
        
        // Note: Value is parsed later via simdjson, not stored as raw string
        
        // Timestamp
        ARROW_RETURN_NOT_OK(timestamp_builder.Append(msg->timestamp().timestamp));
        
        // Partition
        ARROW_RETURN_NOT_OK(partition_builder.Append(msg->partition()));
        
        // Offset
        ARROW_RETURN_NOT_OK(offset_builder.Append(msg->offset()));
    }
    
    // Finish metadata arrays
    std::shared_ptr<arrow::Array> key_array;
    std::shared_ptr<arrow::Array> timestamp_array;
    std::shared_ptr<arrow::Array> partition_array;
    std::shared_ptr<arrow::Array> offset_array;
    
    ARROW_RETURN_NOT_OK(key_builder.Finish(&key_array));
    ARROW_RETURN_NOT_OK(timestamp_builder.Finish(&timestamp_array));
    ARROW_RETURN_NOT_OK(partition_builder.Finish(&partition_array));
    ARROW_RETURN_NOT_OK(offset_builder.Finish(&offset_array));
    
    // Build data arrays from JSON using simdjson
    std::vector<std::shared_ptr<arrow::Array>> data_arrays;
    if (schema_ && schema_->num_fields() > 4) {
        // Build arrays for JSON fields (schema fields beyond metadata)
        for (int i = 4; i < schema_->num_fields(); ++i) {
            auto field = schema_->field(i);
            ARROW_ASSIGN_OR_RAISE(auto array, BuildArrayFromJSON(json_docs, field));
            data_arrays.push_back(array);
        }
    }
    
    // Combine metadata and data arrays
    std::vector<std::shared_ptr<arrow::Array>> all_arrays = {
        key_array, timestamp_array, partition_array, offset_array
    };
    all_arrays.insert(all_arrays.end(), data_arrays.begin(), data_arrays.end());
    
    // Update schema to include both metadata and data fields
    if (!schema_ || schema_->num_fields() <= 4) {
        std::vector<std::shared_ptr<arrow::Field>> all_fields = {
            arrow::field("key", arrow::utf8()),
            arrow::field("timestamp", arrow::timestamp(arrow::TimeUnit::MILLI)),
            arrow::field("partition", arrow::int32()),
            arrow::field("offset", arrow::int64())
        };
        
        // Add data fields if schema exists
        if (schema_) {
            for (int i = 4; i < schema_->num_fields(); ++i) {
                all_fields.push_back(schema_->field(i));
            }
        }
        
        schema_ = arrow::schema(all_fields);
    }
    
    // Create record batch
    return arrow::RecordBatch::Make(schema_, messages.size(), all_arrays);
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

// ========== Schema Registry Integration ==========

arrow::Status KafkaConnector::InitializeSchemaRegistry() {
    // Check if Schema Registry URL is configured
    auto it = config_.properties.find("schema.registry.url");
    if (it == config_.properties.end()) {
        // Schema Registry not configured - will use plain JSON
        return arrow::Status::OK();
    }
    
    std::string registry_url = it->second;
    
    // Create Schema Registry client
    schema_registry_ = std::make_unique<SchemaRegistryClient>(registry_url);
    
    return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> 
KafkaConnector::DecodeWithSchemaRegistry(
    const std::vector<RdKafka::Message*>& messages) {
    
    if (!schema_registry_) {
        return arrow::Status::Invalid("Schema Registry not initialized");
    }
    
    // Extract schema ID from first message
    if (messages.empty() || messages[0]->len() < 5) {
        return arrow::Status::Invalid("Message too short for Schema Registry wire format");
    }
    
    const uint8_t* first_payload = reinterpret_cast<const uint8_t*>(messages[0]->payload());
    ARROW_ASSIGN_OR_RAISE(
        int schema_id,
        SchemaRegistryClient::ExtractSchemaId(first_payload, messages[0]->len())
    );
    
    // Get schema from registry (cached)
    ARROW_ASSIGN_OR_RAISE(auto registered_schema, schema_registry_->GetSchemaById(schema_id));
    
    // Extract payloads (skip wire format header)
    std::vector<const uint8_t*> payloads;
    std::vector<size_t> lengths;
    
    for (const auto* msg : messages) {
        const uint8_t* data = reinterpret_cast<const uint8_t*>(msg->payload());
        size_t len = msg->len();
        
        ARROW_ASSIGN_OR_RAISE(
            auto payload_info,
            SchemaRegistryClient::ExtractPayload(data, len)
        );
        
        payloads.push_back(payload_info.first);
        lengths.push_back(payload_info.second);
    }
    
    // Decode based on schema type
    if (registered_schema.schema_type == "AVRO") {
        // TODO: Re-enable Avro decoder
        return arrow::Status::NotImplemented("Avro decoding temporarily disabled");
        
    } else if (registered_schema.schema_type == "PROTOBUF") {
        // TODO: Re-enable Protobuf decoder
        return arrow::Status::NotImplemented("Protobuf decoding temporarily disabled");
        
    } else if (registered_schema.schema_type == "JSON") {
        // Use simdjson for JSON Schema
        // For now, just parse as regular JSON
        std::vector<simdjson::padded_string> json_docs;
        for (size_t i = 0; i < payloads.size(); ++i) {
            json_docs.emplace_back(
                reinterpret_cast<const char*>(payloads[i]),
                lengths[i]
            );
        }
        
        // Infer schema from first doc if not available
        if (!schema_) {
            auto first_doc = json_parser_.iterate(json_docs[0]);
            if (first_doc.error()) {
                return arrow::Status::Invalid("Failed to parse JSON document");
            }
            ARROW_ASSIGN_OR_RAISE(schema_, InferSchemaFromJSON(first_doc.value()));
        }
        
        // Build arrays using simdjson
        std::vector<std::shared_ptr<arrow::Array>> arrays;
        for (const auto& field : schema_->fields()) {
            ARROW_ASSIGN_OR_RAISE(auto array, BuildArrayFromJSON(json_docs, field));
            arrays.push_back(array);
        }
        
        return arrow::RecordBatch::Make(schema_, json_docs.size(), arrays);
    }
    
    return arrow::Status::Invalid("Unknown schema type: " + registered_schema.schema_type);
}

arrow::Result<std::shared_ptr<arrow::Schema>> 
KafkaConnector::AvroSchemaToArrow(const std::string& avro_schema) {
    // TODO: Re-enable Avro decoder
    return arrow::Status::NotImplemented("Avro schema conversion temporarily disabled");
}

arrow::Result<std::shared_ptr<arrow::Array>> 
KafkaConnector::DecodeAvroArray(
    const std::vector<const uint8_t*>& payloads,
    const std::vector<size_t>& lengths,
    const std::shared_ptr<arrow::Field>& field,
    const std::string& avro_schema) {
    
    // TODO: Re-enable Avro decoder
    return arrow::Status::NotImplemented("Avro array decoding temporarily disabled");
}

// ========== simdjson JSON Parsing Helpers ==========

arrow::Result<std::shared_ptr<arrow::Schema>> 
KafkaConnector::InferSchemaFromJSON(simdjson::ondemand::document& doc) {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    
    auto obj = doc.get_object();
    if (obj.error()) {
        return arrow::Status::Invalid("JSON document is not an object");
    }
    
    // Iterate over JSON fields to infer schema
    for (auto field : obj.value()) {
        std::string_view field_name = field.unescaped_key();
        auto value = field.value();
        
        std::shared_ptr<arrow::DataType> arrow_type;
        
        // Infer Arrow type from JSON type
        auto value_type_result = value.type();
        if (value_type_result.error()) {
            continue;  // Skip this field
        }
        auto value_type = value_type_result.value();
        
        switch (value_type) {
            case simdjson::ondemand::json_type::number: {
                // Check if integer or double
                auto int_val = value.get_int64();
                if (!int_val.error()) {
                    arrow_type = arrow::int64();
                } else {
                    arrow_type = arrow::float64();
                }
                break;
            }
            case simdjson::ondemand::json_type::string:
                arrow_type = arrow::utf8();
                break;
            case simdjson::ondemand::json_type::boolean:
                arrow_type = arrow::boolean();
                break;
            case simdjson::ondemand::json_type::array:
                // For now, serialize arrays as JSON strings
                // TODO: Support nested arrays
                arrow_type = arrow::utf8();
                break;
            case simdjson::ondemand::json_type::object:
                // For now, serialize objects as JSON strings
                // TODO: Support nested structs
                arrow_type = arrow::utf8();
                break;
            case simdjson::ondemand::json_type::null:
                // Default to string for null fields
                arrow_type = arrow::utf8();
                break;
        }
        
        fields.push_back(arrow::field(std::string(field_name), arrow_type));
    }
    
    return arrow::schema(fields);
}

arrow::Result<std::shared_ptr<arrow::Array>> 
KafkaConnector::BuildArrayFromJSON(
    const std::vector<simdjson::padded_string>& json_docs,
    const std::shared_ptr<arrow::Field>& field) {
    
    std::string field_name = field->name();
    
    switch (field->type()->id()) {
        case arrow::Type::INT64: {
            arrow::Int64Builder builder;
            for (const auto& json_str : json_docs) {
                auto doc = json_parser_.iterate(json_str);
                if (doc.error()) {
                    ARROW_RETURN_NOT_OK(builder.AppendNull());
                    continue;
                }
                
                auto value = doc.value()[field_name].get_int64();
                if (value.error()) {
                    ARROW_RETURN_NOT_OK(builder.AppendNull());
                } else {
                    ARROW_RETURN_NOT_OK(builder.Append(value.value()));
                }
            }
            return builder.Finish();
        }
        
        case arrow::Type::DOUBLE: {
            arrow::DoubleBuilder builder;
            for (const auto& json_str : json_docs) {
                auto doc = json_parser_.iterate(json_str);
                if (doc.error()) {
                    ARROW_RETURN_NOT_OK(builder.AppendNull());
                    continue;
                }
                
                auto value = doc.value()[field_name].get_double();
                if (value.error()) {
                    ARROW_RETURN_NOT_OK(builder.AppendNull());
                } else {
                    ARROW_RETURN_NOT_OK(builder.Append(value.value()));
                }
            }
            return builder.Finish();
        }
        
        case arrow::Type::STRING: {
            arrow::StringBuilder builder;
            for (const auto& json_str : json_docs) {
                auto doc = json_parser_.iterate(json_str);
                if (doc.error()) {
                    ARROW_RETURN_NOT_OK(builder.AppendNull());
                    continue;
                }
                
                auto value = doc.value()[field_name].get_string();
                if (value.error()) {
                    ARROW_RETURN_NOT_OK(builder.AppendNull());
                } else {
                    ARROW_RETURN_NOT_OK(builder.Append(
                        std::string(value.value())
                    ));
                }
            }
            return builder.Finish();
        }
        
        case arrow::Type::BOOL: {
            arrow::BooleanBuilder builder;
            for (const auto& json_str : json_docs) {
                auto doc = json_parser_.iterate(json_str);
                if (doc.error()) {
                    ARROW_RETURN_NOT_OK(builder.AppendNull());
                    continue;
                }
                
                auto value = doc.value()[field_name].get_bool();
                if (value.error()) {
                    ARROW_RETURN_NOT_OK(builder.AppendNull());
                } else {
                    ARROW_RETURN_NOT_OK(builder.Append(value.value()));
                }
            }
            return builder.Finish();
        }
        
        case arrow::Type::TIMESTAMP: {
            arrow::TimestampBuilder builder(
                arrow::timestamp(arrow::TimeUnit::MILLI),
                arrow::default_memory_pool()
            );
            for (const auto& json_str : json_docs) {
                auto doc = json_parser_.iterate(json_str);
                if (doc.error()) {
                    ARROW_RETURN_NOT_OK(builder.AppendNull());
                    continue;
                }
                
                auto value = doc.value()[field_name].get_int64();
                if (value.error()) {
                    ARROW_RETURN_NOT_OK(builder.AppendNull());
                } else {
                    ARROW_RETURN_NOT_OK(builder.Append(value.value()));
                }
            }
            return builder.Finish();
        }
        
        default:
            return arrow::Status::NotImplemented(
                "Arrow type not yet supported for JSON: " + field->type()->ToString()
            );
    }
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

