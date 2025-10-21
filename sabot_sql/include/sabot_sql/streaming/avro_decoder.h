#pragma once

#include <arrow/api.h>
#include <avro/Compiler.hh>
#include <avro/Decoder.hh>
#include <avro/Generic.hh>
#include <memory>
#include <string>
#include <vector>

namespace sabot_sql {
namespace streaming {

/**
 * Avro decoder for Kafka messages
 * 
 * Converts Avro binary data to Arrow RecordBatches.
 * Supports:
 * - Primitive types (int, long, float, double, string, boolean, bytes)
 * - Complex types (record, array, map, union, enum)
 * - Logical types (timestamp-millis, timestamp-micros, decimal, etc.)
 */
class AvroDecoder {
public:
    /**
     * Initialize decoder with Avro schema
     * 
     * @param avro_schema_json Avro schema in JSON format
     */
    explicit AvroDecoder(const std::string& avro_schema_json);
    
    /**
     * Convert Avro schema to Arrow schema
     * 
     * @return Arrow schema
     */
    arrow::Result<std::shared_ptr<arrow::Schema>> GetArrowSchema() const;
    
    /**
     * Decode multiple Avro binary messages to RecordBatch
     * 
     * @param payloads Vector of Avro binary payloads
     * @param lengths Vector of payload lengths
     * @return Arrow RecordBatch
     */
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> 
    DecodeBatch(
        const std::vector<const uint8_t*>& payloads,
        const std::vector<size_t>& lengths
    );
    
private:
    avro::ValidSchema avro_schema_;
    std::shared_ptr<arrow::Schema> arrow_schema_;
    
    // Schema conversion helpers
    arrow::Result<std::shared_ptr<arrow::DataType>> 
    AvroTypeToArrowType(const avro::NodePtr& node);
    
    arrow::Result<std::shared_ptr<arrow::Field>>
    AvroFieldToArrowField(const avro::NodePtr& node, const std::string& name);
    
    // Decoding helpers
    arrow::Result<std::shared_ptr<arrow::Array>>
    DecodeArray(
        const std::vector<const uint8_t*>& payloads,
        const std::vector<size_t>& lengths,
        const std::shared_ptr<arrow::Field>& field
    );
    
    arrow::Result<std::shared_ptr<arrow::Array>>
    DecodeIntArray(const std::vector<avro::GenericDatum>& values);
    
    arrow::Result<std::shared_ptr<arrow::Array>>
    DecodeLongArray(const std::vector<avro::GenericDatum>& values);
    
    arrow::Result<std::shared_ptr<arrow::Array>>
    DecodeFloatArray(const std::vector<avro::GenericDatum>& values);
    
    arrow::Result<std::shared_ptr<arrow::Array>>
    DecodeDoubleArray(const std::vector<avro::GenericDatum>& values);
    
    arrow::Result<std::shared_ptr<arrow::Array>>
    DecodeStringArray(const std::vector<avro::GenericDatum>& values);
    
    arrow::Result<std::shared_ptr<arrow::Array>>
    DecodeBoolArray(const std::vector<avro::GenericDatum>& values);
    
    arrow::Result<std::shared_ptr<arrow::Array>>
    DecodeBytesArray(const std::vector<avro::GenericDatum>& values);
};

} // namespace streaming
} // namespace sabot_sql

