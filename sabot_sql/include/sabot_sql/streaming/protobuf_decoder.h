#pragma once

#include <arrow/api.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/compiler/parser.h>
#include <memory>
#include <string>
#include <vector>

namespace sabot_sql {
namespace streaming {

/**
 * Protobuf decoder for Kafka messages
 * 
 * Converts Protobuf binary data to Arrow RecordBatches using dynamic messages.
 * Supports:
 * - All primitive types (int32, int64, float, double, string, bool, bytes)
 * - Repeated fields (maps to Arrow list types)
 * - Nested messages (maps to Arrow struct types)
 * - Maps (maps to Arrow map type)
 * - Enums (maps to Arrow int32 or string)
 */
class ProtobufDecoder {
public:
    /**
     * Initialize decoder with Protobuf schema
     * 
     * @param proto_schema Protobuf schema (FileDescriptorProto serialized)
     * @param message_type Fully qualified message type name
     */
    explicit ProtobufDecoder(
        const std::string& proto_schema,
        const std::string& message_type
    );
    
    /**
     * Convert Protobuf schema to Arrow schema
     * 
     * @return Arrow schema
     */
    arrow::Result<std::shared_ptr<arrow::Schema>> GetArrowSchema() const;
    
    /**
     * Decode multiple Protobuf binary messages to RecordBatch
     * 
     * @param payloads Vector of Protobuf binary payloads
     * @param lengths Vector of payload lengths
     * @return Arrow RecordBatch
     */
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> 
    DecodeBatch(
        const std::vector<const uint8_t*>& payloads,
        const std::vector<size_t>& lengths
    );
    
private:
    google::protobuf::DescriptorPool descriptor_pool_;
    google::protobuf::DynamicMessageFactory message_factory_;
    const google::protobuf::Descriptor* message_descriptor_;
    std::shared_ptr<arrow::Schema> arrow_schema_;
    
    // Schema conversion helpers
    arrow::Result<std::shared_ptr<arrow::DataType>> 
    ProtoTypeToArrowType(const google::protobuf::FieldDescriptor* field);
    
    arrow::Result<std::shared_ptr<arrow::Field>>
    ProtoFieldToArrowField(const google::protobuf::FieldDescriptor* field);
    
    // Decoding helpers
    arrow::Result<std::shared_ptr<arrow::Array>>
    DecodeField(
        const std::vector<std::unique_ptr<google::protobuf::Message>>& messages,
        const google::protobuf::FieldDescriptor* field,
        const std::shared_ptr<arrow::Field>& arrow_field
    );
    
    arrow::Result<std::shared_ptr<arrow::Array>>
    DecodeInt32Array(
        const std::vector<std::unique_ptr<google::protobuf::Message>>& messages,
        const google::protobuf::FieldDescriptor* field
    );
    
    arrow::Result<std::shared_ptr<arrow::Array>>
    DecodeInt64Array(
        const std::vector<std::unique_ptr<google::protobuf::Message>>& messages,
        const google::protobuf::FieldDescriptor* field
    );
    
    arrow::Result<std::shared_ptr<arrow::Array>>
    DecodeFloatArray(
        const std::vector<std::unique_ptr<google::protobuf::Message>>& messages,
        const google::protobuf::FieldDescriptor* field
    );
    
    arrow::Result<std::shared_ptr<arrow::Array>>
    DecodeDoubleArray(
        const std::vector<std::unique_ptr<google::protobuf::Message>>& messages,
        const google::protobuf::FieldDescriptor* field
    );
    
    arrow::Result<std::shared_ptr<arrow::Array>>
    DecodeStringArray(
        const std::vector<std::unique_ptr<google::protobuf::Message>>& messages,
        const google::protobuf::FieldDescriptor* field
    );
    
    arrow::Result<std::shared_ptr<arrow::Array>>
    DecodeBoolArray(
        const std::vector<std::unique_ptr<google::protobuf::Message>>& messages,
        const google::protobuf::FieldDescriptor* field
    );
    
    arrow::Result<std::shared_ptr<arrow::Array>>
    DecodeBytesArray(
        const std::vector<std::unique_ptr<google::protobuf::Message>>& messages,
        const google::protobuf::FieldDescriptor* field
    );
};

} // namespace streaming
} // namespace sabot_sql

