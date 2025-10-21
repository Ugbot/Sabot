#include "sabot_sql/streaming/avro_decoder.h"
#include <avro/Stream.hh>
#include <avro/Specific.hh>
#include <sstream>

namespace sabot_sql {
namespace streaming {

AvroDecoder::AvroDecoder(const std::string& avro_schema_json) {
    // Compile Avro schema from JSON
    std::istringstream schema_stream(avro_schema_json);
    avro::compileJsonSchema(schema_stream, avro_schema_);
    
    // Convert to Arrow schema
    auto arrow_schema = GetArrowSchema();
    if (arrow_schema.ok()) {
        arrow_schema_ = arrow_schema.ValueOrDie();
    }
}

arrow::Result<std::shared_ptr<arrow::Schema>> AvroDecoder::GetArrowSchema() const {
    const avro::NodePtr& root = avro_schema_.root();
    
    if (root->type() != avro::AVRO_RECORD) {
        return arrow::Status::Invalid("Avro schema must be a RECORD type");
    }
    
    std::vector<std::shared_ptr<arrow::Field>> fields;
    
    // Convert each Avro field to Arrow field
    for (size_t i = 0; i < root->names(); ++i) {
        std::string field_name = root->nameAt(i);
        const avro::NodePtr& field_node = root->leafAt(i);
        
        ARROW_ASSIGN_OR_RAISE(
            auto arrow_field,
            AvroFieldToArrowField(field_node, field_name)
        );
        
        fields.push_back(arrow_field);
    }
    
    return arrow::schema(fields);
}

arrow::Result<std::shared_ptr<arrow::DataType>> 
AvroDecoder::AvroTypeToArrowType(const avro::NodePtr& node) {
    switch (node->type()) {
        case avro::AVRO_NULL:
            return arrow::null();
        
        case avro::AVRO_BOOL:
            return arrow::boolean();
        
        case avro::AVRO_INT:
            return arrow::int32();
        
        case avro::AVRO_LONG:
            return arrow::int64();
        
        case avro::AVRO_FLOAT:
            return arrow::float32();
        
        case avro::AVRO_DOUBLE:
            return arrow::float64();
        
        case avro::AVRO_STRING:
            return arrow::utf8();
        
        case avro::AVRO_BYTES:
            return arrow::binary();
        
        case avro::AVRO_ARRAY: {
            // Get element type
            const avro::NodePtr& element_node = node->leafAt(0);
            ARROW_ASSIGN_OR_RAISE(
                auto element_type,
                AvroTypeToArrowType(element_node)
            );
            return arrow::list(element_type);
        }
        
        case avro::AVRO_MAP: {
            // Avro maps are string -> value
            const avro::NodePtr& value_node = node->leafAt(1);
            ARROW_ASSIGN_OR_RAISE(
                auto value_type,
                AvroTypeToArrowType(value_node)
            );
            return arrow::map(arrow::utf8(), value_type);
        }
        
        case avro::AVRO_RECORD:
            // Nested records become structs
            // For simplicity, convert to JSON string
            return arrow::utf8();
        
        case avro::AVRO_ENUM:
            return arrow::utf8();
        
        case avro::AVRO_UNION: {
            // Check if it's a nullable union (null + type)
            if (node->leaves() == 2) {
                // Find non-null type
                for (size_t i = 0; i < node->leaves(); ++i) {
                    const avro::NodePtr& branch = node->leafAt(i);
                    if (branch->type() != avro::AVRO_NULL) {
                        return AvroTypeToArrowType(branch);
                    }
                }
            }
            // Complex union - convert to string
            return arrow::utf8();
        }
        
        case avro::AVRO_FIXED:
            return arrow::fixed_size_binary(static_cast<int>(node->fixedSize()));
        
        default:
            return arrow::Status::NotImplemented(
                "Avro type not supported: " + std::to_string(node->type())
            );
    }
}

arrow::Result<std::shared_ptr<arrow::Field>>
AvroDecoder::AvroFieldToArrowField(const avro::NodePtr& node, const std::string& name) {
    ARROW_ASSIGN_OR_RAISE(auto data_type, AvroTypeToArrowType(node));
    
    // Check if field is nullable (union with null)
    bool nullable = false;
    if (node->type() == avro::AVRO_UNION) {
        for (size_t i = 0; i < node->leaves(); ++i) {
            if (node->leafAt(i)->type() == avro::AVRO_NULL) {
                nullable = true;
                break;
            }
        }
    }
    
    return arrow::field(name, data_type, nullable);
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> 
AvroDecoder::DecodeBatch(
    const std::vector<const uint8_t*>& payloads,
    const std::vector<size_t>& lengths) {
    
    if (payloads.empty()) {
        return nullptr;
    }
    
    // Decode all messages to GenericDatum
    std::vector<avro::GenericDatum> records;
    records.reserve(payloads.size());
    
    for (size_t i = 0; i < payloads.size(); ++i) {
        // Create input stream from bytes
        auto input_stream = avro::memoryInputStream(payloads[i], lengths[i]);
        
        // Create decoder
        avro::DecoderPtr decoder = avro::binaryDecoder();
        decoder->init(*input_stream);
        
        // Decode to generic datum
        avro::GenericDatum datum(avro_schema_);
        avro::decode(*decoder, datum.value<avro::GenericRecord>());
        
        records.push_back(std::move(datum));
    }
    
    // Build Arrow arrays for each field
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    
    for (const auto& field : arrow_schema_->fields()) {
        ARROW_ASSIGN_OR_RAISE(
            auto array,
            DecodeArray(payloads, lengths, field)
        );
        arrays.push_back(array);
    }
    
    return arrow::RecordBatch::Make(arrow_schema_, payloads.size(), arrays);
}

arrow::Result<std::shared_ptr<arrow::Array>>
AvroDecoder::DecodeArray(
    const std::vector<const uint8_t*>& payloads,
    const std::vector<size_t>& lengths,
    const std::shared_ptr<arrow::Field>& field) {
    
    // Decode all messages and extract field values
    std::vector<avro::GenericDatum> values;
    values.reserve(payloads.size());
    
    for (size_t i = 0; i < payloads.size(); ++i) {
        auto input_stream = avro::memoryInputStream(payloads[i], lengths[i]);
        avro::DecoderPtr decoder = avro::binaryDecoder();
        decoder->init(*input_stream);
        
        avro::GenericDatum datum(avro_schema_);
        avro::decode(*decoder, datum.value<avro::GenericRecord>());
        
        // Extract field value
        const avro::GenericRecord& record = datum.value<avro::GenericRecord>();
        values.push_back(record.field(field->name()));
    }
    
    // Build Arrow array based on type
    switch (field->type()->id()) {
        case arrow::Type::INT32:
            return DecodeIntArray(values);
        case arrow::Type::INT64:
            return DecodeLongArray(values);
        case arrow::Type::FLOAT:
            return DecodeFloatArray(values);
        case arrow::Type::DOUBLE:
            return DecodeDoubleArray(values);
        case arrow::Type::STRING:
            return DecodeStringArray(values);
        case arrow::Type::BOOL:
            return DecodeBoolArray(values);
        case arrow::Type::BINARY:
            return DecodeBytesArray(values);
        default:
            return arrow::Status::NotImplemented(
                "Arrow type not supported: " + field->type()->ToString()
            );
    }
}

arrow::Result<std::shared_ptr<arrow::Array>>
AvroDecoder::DecodeIntArray(const std::vector<avro::GenericDatum>& values) {
    arrow::Int32Builder builder;
    
    for (const auto& datum : values) {
        if (datum.type() == avro::AVRO_NULL) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
        } else {
            ARROW_RETURN_NOT_OK(builder.Append(datum.value<int32_t>()));
        }
    }
    
    return builder.Finish();
}

arrow::Result<std::shared_ptr<arrow::Array>>
AvroDecoder::DecodeLongArray(const std::vector<avro::GenericDatum>& values) {
    arrow::Int64Builder builder;
    
    for (const auto& datum : values) {
        if (datum.type() == avro::AVRO_NULL) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
        } else {
            ARROW_RETURN_NOT_OK(builder.Append(datum.value<int64_t>()));
        }
    }
    
    return builder.Finish();
}

arrow::Result<std::shared_ptr<arrow::Array>>
AvroDecoder::DecodeFloatArray(const std::vector<avro::GenericDatum>& values) {
    arrow::FloatBuilder builder;
    
    for (const auto& datum : values) {
        if (datum.type() == avro::AVRO_NULL) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
        } else {
            ARROW_RETURN_NOT_OK(builder.Append(datum.value<float>()));
        }
    }
    
    return builder.Finish();
}

arrow::Result<std::shared_ptr<arrow::Array>>
AvroDecoder::DecodeDoubleArray(const std::vector<avro::GenericDatum>& values) {
    arrow::DoubleBuilder builder;
    
    for (const auto& datum : values) {
        if (datum.type() == avro::AVRO_NULL) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
        } else {
            ARROW_RETURN_NOT_OK(builder.Append(datum.value<double>()));
        }
    }
    
    return builder.Finish();
}

arrow::Result<std::shared_ptr<arrow::Array>>
AvroDecoder::DecodeStringArray(const std::vector<avro::GenericDatum>& values) {
    arrow::StringBuilder builder;
    
    for (const auto& datum : values) {
        if (datum.type() == avro::AVRO_NULL) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
        } else {
            ARROW_RETURN_NOT_OK(builder.Append(datum.value<std::string>()));
        }
    }
    
    return builder.Finish();
}

arrow::Result<std::shared_ptr<arrow::Array>>
AvroDecoder::DecodeBoolArray(const std::vector<avro::GenericDatum>& values) {
    arrow::BooleanBuilder builder;
    
    for (const auto& datum : values) {
        if (datum.type() == avro::AVRO_NULL) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
        } else {
            ARROW_RETURN_NOT_OK(builder.Append(datum.value<bool>()));
        }
    }
    
    return builder.Finish();
}

arrow::Result<std::shared_ptr<arrow::Array>>
AvroDecoder::DecodeBytesArray(const std::vector<avro::GenericDatum>& values) {
    arrow::BinaryBuilder builder;
    
    for (const auto& datum : values) {
        if (datum.type() == avro::AVRO_NULL) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
        } else {
            const std::vector<uint8_t>& bytes = datum.value<std::vector<uint8_t>>();
            ARROW_RETURN_NOT_OK(builder.Append(bytes.data(), bytes.size()));
        }
    }
    
    return builder.Finish();
}

} // namespace streaming
} // namespace sabot_sql

