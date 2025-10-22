# Kafka C++ Integration with librdkafka and simdjson

## Overview

Sabot's Kafka integration uses:
- **librdkafka** (vendored) - High-performance Kafka client library
- **simdjson** (vendored) - Ultra-fast JSON parsing with SIMD
- **Schema Registry** - Confluent Schema Registry support for Avro, Protobuf, JSON Schema
- **MarbleDB RAFT** - Distributed offset storage for fault tolerance

## Architecture

### Two-Level Integration

1. **C++ Level (SabotSQL)** - High-performance streaming SQL
   - Located in: `sabot_sql/src/streaming/kafka_connector.cpp`
   - Uses librdkafka for Kafka operations
   - Uses simdjson for JSON parsing
   - Stores offsets in MarbleDB RAFT tables
   - Provides zero-copy Arrow conversion

2. **Python Level (Stream API)** - User-facing API
   - Located in: `sabot/kafka/source.py` and `sabot/kafka/sink.py`
   - Uses aiokafka for async operations
   - Provides codec abstraction (JSON, Avro, Protobuf, etc.)
   - Integrates with Stream API

## Current Status

### ✅ Implemented (Python Layer)

**Location**: `sabot/kafka/`

- ✅ Full codec support (JSON, Avro, Protobuf, JSON Schema, MessagePack, String, Bytes)
- ✅ Schema Registry client
- ✅ Consumer group management
- ✅ Offset management (earliest, latest, committed)
- ✅ Error handling (skip, fail, retry, dead_letter)
- ✅ Backpressure handling
- ✅ Graceful shutdown
- ✅ Stream API integration

### ⚠️ Partially Implemented (C++ Layer)

**Location**: `sabot_sql/src/streaming/kafka_connector.cpp`

- ✅ librdkafka integration
- ✅ Basic message consumption
- ✅ Offset management
- ✅ MarbleDB RAFT offset storage
- ✅ Partition discovery
- ✅ Arrow conversion
- ⚠️ JSON parsing (simple, not using simdjson yet)
- ❌ Schema Registry integration (TODO)
- ❌ Avro support (TODO)
- ❌ Protobuf support (TODO)

## Integration Plan

### Phase 1: simdjson Integration ✨ NEW

**Objective**: Replace simple JSON parsing with SIMD-accelerated simdjson

**Files to Update**:
- `sabot_sql/CMakeLists.txt` - Add simdjson
- `sabot_sql/src/streaming/kafka_connector.cpp` - Use simdjson parser
- `sabot_sql/include/sabot_sql/streaming/kafka_connector.h` - Add simdjson headers

**Benefits**:
- 2-3x faster JSON parsing
- Lower CPU usage
- Better throughput for JSON-heavy workloads

**Code Changes**:

```cpp
// Before (simple string parsing)
arrow::StringBuilder value_builder;
ARROW_RETURN_NOT_OK(value_builder.Append(
    reinterpret_cast<const char*>(msg->payload()),
    static_cast<int32_t>(msg->len())
));

// After (simdjson parsing)
#include "simdjson.h"

simdjson::ondemand::parser parser;
auto json = parser.iterate(
    reinterpret_cast<const char*>(msg->payload()),
    msg->len()
);

// Extract fields based on schema
for (const auto& field : schema_->fields()) {
    auto value = json[field->name()];
    // Convert to Arrow based on field type
    // ...
}
```

### Phase 2: Schema Registry Integration

**Objective**: Support Avro, Protobuf, JSON Schema via Confluent Schema Registry

**Dependencies**:
- libserdes (Confluent C library for Schema Registry)
- OR implement Schema Registry REST client in C++

**Files to Create/Update**:
- `sabot_sql/include/sabot_sql/streaming/schema_registry_client.h`
- `sabot_sql/src/streaming/schema_registry_client.cpp`
- `sabot_sql/src/streaming/kafka_connector.cpp` - Integrate registry

**Benefits**:
- Schema evolution support
- Backward/forward compatibility
- Type safety
- Automatic serialization/deserialization

### Phase 3: Avro Support

**Objective**: Fast Avro deserialization using Apache Avro C++

**Dependencies**:
- avro-cpp (already vendored in Arrow)

**Files to Create/Update**:
- `sabot_sql/include/sabot_sql/streaming/avro_deserializer.h`
- `sabot_sql/src/streaming/avro_deserializer.cpp`

**Benefits**:
- Compact binary format
- Schema evolution
- Industry standard

### Phase 4: Protobuf Support

**Objective**: Fast Protobuf deserialization

**Dependencies**:
- protobuf (need to vendor or use system)

**Files to Create/Update**:
- `sabot_sql/include/sabot_sql/streaming/protobuf_deserializer.h`
- `sabot_sql/src/streaming/protobuf_deserializer.cpp`

## simdjson Integration Details

### Step 1: Add to CMake

**File**: `sabot_sql/CMakeLists.txt`

```cmake
# Add simdjson
add_subdirectory(${CMAKE_SOURCE_DIR}/../vendor/simdjson simdjson EXCLUDE_FROM_ALL)

# Link to KafkaConnector
target_link_libraries(sabot_sql
    PRIVATE
    sabot_sql_streaming
    arrow
    rdkafka++
    simdjson
)

target_include_directories(sabot_sql_streaming
    PUBLIC
    ${CMAKE_SOURCE_DIR}/../vendor/simdjson/include
)
```

### Step 2: Update KafkaConnector Header

**File**: `sabot_sql/include/sabot_sql/streaming/kafka_connector.h`

```cpp
#pragma once

#include "sabot_sql/streaming/source_connector.h"
#include <librdkafka/rdkafkacpp.h>
#include <simdjson.h>  // ADD THIS
#include <memory>
#include <atomic>

namespace sabot_sql {
namespace streaming {

class KafkaConnector : public SourceConnector {
public:
    // ... existing methods ...
    
private:
    // Configuration
    ConnectorConfig config_;
    
    // Kafka client
    std::unique_ptr<RdKafka::KafkaConsumer> consumer_;
    std::string topic_;
    int partition_id_;
    
    // Schema (inferred or provided)
    std::shared_ptr<arrow::Schema> schema_;
    
    // JSON parser (SIMD-accelerated)  // ADD THIS
    simdjson::ondemand::parser json_parser_;  // ADD THIS
    
    // ... rest of members ...
};

} // namespace streaming
} // namespace sabot_sql
```

### Step 3: Implement Fast JSON Parsing

**File**: `sabot_sql/src/streaming/kafka_connector.cpp`

```cpp
arrow::Result<std::shared_ptr<arrow::RecordBatch>> 
KafkaConnector::ConvertMessagesToBatch(const std::vector<RdKafka::Message*>& messages) {
    if (messages.empty()) {
        return nullptr;
    }
    
    // Use simdjson for fast parsing
    std::vector<simdjson::ondemand::document> docs;
    docs.reserve(messages.size());
    
    // Parse all JSON documents
    for (const auto* msg : messages) {
        if (msg->len() > 0) {
            auto padded_string = simdjson::padded_string(
                reinterpret_cast<const char*>(msg->payload()),
                msg->len()
            );
            
            auto doc = json_parser_.iterate(padded_string);
            if (!doc.error()) {
                docs.push_back(std::move(doc.value()));
            } else {
                // Handle parse error based on error policy
                continue;
            }
        }
    }
    
    // Convert JSON documents to Arrow based on schema
    if (!schema_) {
        // Infer schema from first document
        ARROW_ASSIGN_OR_RAISE(schema_, InferSchemaFromJSON(docs[0]));
    }
    
    // Build Arrow arrays for each field
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    for (const auto& field : schema_->fields()) {
        ARROW_ASSIGN_OR_RAISE(auto array, BuildArrayFromJSON(docs, field));
        arrays.push_back(array);
    }
    
    // Create RecordBatch
    return arrow::RecordBatch::Make(schema_, docs.size(), arrays);
}

arrow::Result<std::shared_ptr<arrow::Array>> 
KafkaConnector::BuildArrayFromJSON(
    const std::vector<simdjson::ondemand::document>& docs,
    const std::shared_ptr<arrow::Field>& field) {
    
    // Handle different Arrow types
    switch (field->type()->id()) {
        case arrow::Type::INT64: {
            arrow::Int64Builder builder;
            for (auto& doc : docs) {
                int64_t value;
                auto result = doc[field->name()].get_int64();
                if (!result.error()) {
                    ARROW_RETURN_NOT_OK(builder.Append(result.value()));
                } else {
                    ARROW_RETURN_NOT_OK(builder.AppendNull());
                }
            }
            return builder.Finish();
        }
        
        case arrow::Type::DOUBLE: {
            arrow::DoubleBuilder builder;
            for (auto& doc : docs) {
                double value;
                auto result = doc[field->name()].get_double();
                if (!result.error()) {
                    ARROW_RETURN_NOT_OK(builder.Append(result.value()));
                } else {
                    ARROW_RETURN_NOT_OK(builder.AppendNull());
                }
            }
            return builder.Finish();
        }
        
        case arrow::Type::STRING: {
            arrow::StringBuilder builder;
            for (auto& doc : docs) {
                auto result = doc[field->name()].get_string();
                if (!result.error()) {
                    ARROW_RETURN_NOT_OK(builder.Append(result.value()));
                } else {
                    ARROW_RETURN_NOT_OK(builder.AppendNull());
                }
            }
            return builder.Finish();
        }
        
        case arrow::Type::BOOL: {
            arrow::BooleanBuilder builder;
            for (auto& doc : docs) {
                bool value;
                auto result = doc[field->name()].get_bool();
                if (!result.error()) {
                    ARROW_RETURN_NOT_OK(builder.Append(result.value()));
                } else {
                    ARROW_RETURN_NOT_OK(builder.AppendNull());
                }
            }
            return builder.Finish();
        }
        
        default:
            return arrow::Status::NotImplemented(
                "Type not yet supported: " + field->type()->ToString()
            );
    }
}
```

### Step 4: Schema Inference

```cpp
arrow::Result<std::shared_ptr<arrow::Schema>> 
KafkaConnector::InferSchemaFromJSON(simdjson::ondemand::document& doc) {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    
    // Iterate over JSON object fields
    for (auto field : doc.get_object()) {
        std::string field_name(field.key());
        auto value = field.value();
        
        // Infer Arrow type from JSON value type
        std::shared_ptr<arrow::DataType> arrow_type;
        switch (value.type()) {
            case simdjson::ondemand::json_type::number:
                // Check if integer or floating point
                if (value.is_integer()) {
                    arrow_type = arrow::int64();
                } else {
                    arrow_type = arrow::float64();
                }
                break;
            case simdjson::ondemand::json_type::string:
                arrow_type = arrow::utf8();
                break;
            case simdjson::ondemand::json_type::boolean:
                arrow_type = arrow::boolean();
                break;
            case simdjson::ondemand::json_type::array:
                // TODO: Handle arrays
                arrow_type = arrow::utf8(); // Fallback to string
                break;
            case simdjson::ondemand::json_type::object:
                // TODO: Handle nested objects
                arrow_type = arrow::utf8(); // Fallback to string
                break;
            case simdjson::ondemand::json_type::null:
                arrow_type = arrow::utf8(); // Default to string for nulls
                break;
        }
        
        fields.push_back(arrow::field(field_name, arrow_type));
    }
    
    return arrow::schema(fields);
}
```

## Performance Comparison

### Before (simple string parsing)
- **Throughput**: ~50K msg/sec
- **CPU Usage**: 80%
- **Latency p99**: 100ms

### After (simdjson)
- **Throughput**: ~150K msg/sec (3x improvement)
- **CPU Usage**: 40% (2x reduction)
- **Latency p99**: 30ms (3x improvement)

*Note: Benchmarks are estimates. Actual performance depends on message size and complexity.*

## Usage Example (C++ Layer)

```cpp
#include "sabot_sql/streaming/kafka_connector.h"

// Create connector config
sabot_sql::streaming::ConnectorConfig config;
config.connector_id = "my-kafka-source";
config.partition_id = 0;
config.properties = {
    {"topic", "transactions"},
    {"bootstrap.servers", "localhost:9092"},
    {"group.id", "my-consumer-group"}
};

// Create and initialize connector
auto connector = std::make_unique<sabot_sql::streaming::KafkaConnector>();
auto status = connector->Initialize(config);

// Fetch batches (with fast simdjson parsing)
while (true) {
    auto batch_result = connector->GetNextBatch(1000);
    if (batch_result.ok() && batch_result.ValueOrDie()) {
        auto batch = batch_result.ValueOrDie();
        // Process Arrow RecordBatch
        std::cout << "Received " << batch->num_rows() << " rows" << std::endl;
    }
}
```

## Usage Example (Python Layer)

```python
from sabot.api.stream import Stream

# High-level API (uses C++ connector under the hood for performance)
stream = Stream.from_kafka(
    bootstrap_servers="localhost:9092",
    topic="transactions",
    group_id="my-group",
    codec_type="json"  # Will use simdjson in C++ layer
)

# Process stream
processed = stream.filter(lambda b: b['amount'] > 1000.0)
processed.to_kafka("localhost:9092", "flagged-transactions")
```

## Next Steps

### Immediate
1. ✅ Vendor simdjson
2. ⏳ Update CMakeLists.txt to include simdjson
3. ⏳ Implement simdjson parsing in KafkaConnector
4. ⏳ Add benchmarks comparing before/after
5. ⏳ Update documentation

### Short-term
1. Add Schema Registry client (C++)
2. Implement Avro support
3. Implement Protobuf support
4. Add JSON Schema support

### Long-term
1. Zero-copy optimizations
2. Custom allocators for Arrow builders
3. SIMD optimizations for type conversion
4. Parallel deserialization across partitions

## File Structure

```
vendor/
  librdkafka/          # Kafka C++ client (vendored)
  simdjson/            # Fast JSON parser (vendored) ✨ NEW
  
sabot_sql/
  include/sabot_sql/streaming/
    kafka_connector.h           # Kafka connector header
    schema_registry_client.h    # TODO: Schema Registry
    avro_deserializer.h         # TODO: Avro support
    protobuf_deserializer.h     # TODO: Protobuf support
  
  src/streaming/
    kafka_connector.cpp         # Kafka connector implementation
    schema_registry_client.cpp  # TODO
    avro_deserializer.cpp       # TODO
    protobuf_deserializer.cpp   # TODO

sabot/kafka/
  source.py              # Python Kafka source (aiokafka)
  sink.py                # Python Kafka sink (aiokafka)
  schema_registry.py     # Python Schema Registry client
  codecs.py              # Codec abstraction layer
```

## Benefits of This Architecture

1. **Performance**: C++ + simdjson provides maximum throughput
2. **Flexibility**: Python layer provides easy-to-use API
3. **Compatibility**: Support for all major serialization formats
4. **Fault Tolerance**: MarbleDB RAFT for offset storage
5. **Scalability**: One consumer per partition model
6. **Integration**: Seamless with Stream API and SabotSQL

## References

- [librdkafka Documentation](https://github.com/confluentinc/librdkafka)
- [simdjson Documentation](https://github.com/simdjson/simdjson)
- [Confluent Schema Registry](https://docs.confluent.io/platform/current/schema-registry/)
- [Apache Avro](https://avro.apache.org/)
- [Protocol Buffers](https://protobuf.dev/)
