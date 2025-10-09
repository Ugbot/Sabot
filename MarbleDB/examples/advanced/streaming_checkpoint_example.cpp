#include <marble/marble.h>
#include <arrow/api.h>
#include <iostream>
#include <thread>
#include <chrono>
#include <vector>
#include <memory>

// Example user record for testing
class UserRecord : public marble::Record {
public:
    UserRecord(int64_t id, std::string name, int32_t age)
        : id_(id), name_(std::move(name)), age_(age) {}

    std::shared_ptr<marble::Key> GetKey() const override {
        // Simple integer key
        class Int64Key : public marble::Key {
        public:
            Int64Key(int64_t value) : value_(value) {}
            int Compare(const marble::Key& other) const override {
                const Int64Key* other_key = dynamic_cast<const Int64Key*>(&other);
                if (!other_key) return -1;
                if (value_ < other_key->value_) return -1;
                if (value_ > other_key->value_) return 1;
                return 0;
            }
            arrow::Result<std::shared_ptr<arrow::Scalar>> ToArrowScalar() const override {
                return std::make_shared<arrow::Int64Scalar>(value_);
            }
            std::shared_ptr<marble::Key> Clone() const override {
                return std::make_shared<Int64Key>(value_);
            }
            std::string ToString() const override {
                return std::to_string(value_);
            }
            size_t Hash() const override {
                return std::hash<int64_t>{}(value_);
            }
        private:
            int64_t value_;
        };
        return std::make_shared<Int64Key>(id_);
    }

    arrow::Result<std::shared_ptr<arrow::RecordBatch>> ToRecordBatch() const override {
        // Create Arrow schema
        auto schema = arrow::schema({
            arrow::field("id", arrow::int64()),
            arrow::field("name", arrow::utf8()),
            arrow::field("age", arrow::int32())
        });

        // Create arrays
        arrow::Int64Builder id_builder;
        arrow::StringBuilder name_builder;
        arrow::Int32Builder age_builder;

        ARROW_RETURN_NOT_OK(id_builder.Append(id_));
        ARROW_RETURN_NOT_OK(name_builder.Append(name_));
        ARROW_RETURN_NOT_OK(age_builder.Append(age_));

        std::shared_ptr<arrow::Array> id_array, name_array, age_array;
        ARROW_RETURN_NOT_OK(id_builder.Finish(&id_array));
        ARROW_RETURN_NOT_OK(name_builder.Finish(&name_array));
        ARROW_RETURN_NOT_OK(age_builder.Finish(&age_array));

        return arrow::RecordBatch::Make(schema, 1, {id_array, name_array, age_array});
    }

    std::shared_ptr<arrow::Schema> GetArrowSchema() const override {
        return arrow::schema({
            arrow::field("id", arrow::int64()),
            arrow::field("name", arrow::utf8()),
            arrow::field("age", arrow::int32())
        });
    }

    std::unique_ptr<marble::RecordRef> AsRecordRef() const override {
        // Return a simple implementation that wraps the record
        // In a full implementation, this would provide zero-copy access
        return nullptr; // Placeholder
    }

private:
    int64_t id_;
    std::string name_;
    int32_t age_;
};

// Example schema
class UserSchema : public marble::Schema {
public:
    UserSchema() {
        fields_ = {
            {"id", arrow::int64()},
            {"name", arrow::utf8()},
            {"age", arrow::int32()}
        };
    }

    std::shared_ptr<arrow::Schema> ToArrowSchema() const {
        std::vector<std::shared_ptr<arrow::Field>> arrow_fields;
        for (const auto& field : fields_) {
            arrow_fields.push_back(arrow::field(field.first, field.second));
        }
        return arrow::schema(arrow_fields);
    }

    std::shared_ptr<arrow::Schema> GetArrowSchema() const override {
        return ToArrowSchema();
    }

    const std::vector<size_t>& GetPrimaryKeyIndices() const override {
        static const std::vector<size_t> pk_indices = {0}; // id field
        return pk_indices;
    }

    arrow::Result<std::shared_ptr<marble::Key>> KeyFromScalar(
        const std::shared_ptr<arrow::Scalar>& scalar) const override {
        if (!scalar || scalar->type->id() != arrow::Type::INT64) {
            return arrow::Status::Invalid("Expected int64 scalar for key");
        }

        auto int_scalar = std::static_pointer_cast<arrow::Int64Scalar>(scalar);
        class Int64Key : public marble::Key {
        public:
            Int64Key(int64_t value) : value_(value) {}
            int Compare(const marble::Key& other) const override {
                const Int64Key* other_key = dynamic_cast<const Int64Key*>(&other);
                if (!other_key) return -1;
                if (value_ < other_key->value_) return -1;
                if (value_ > other_key->value_) return 1;
                return 0;
            }
            arrow::Result<std::shared_ptr<arrow::Scalar>> ToArrowScalar() const override {
                return std::make_shared<arrow::Int64Scalar>(value_);
            }
            std::shared_ptr<marble::Key> Clone() const override {
                return std::make_shared<Int64Key>(value_);
            }
            std::string ToString() const override {
                return std::to_string(value_);
            }
            size_t Hash() const override {
                return std::hash<int64_t>{}(value_);
            }
        private:
            int64_t value_;
        };

        return std::make_shared<Int64Key>(int_scalar->value);
    }

    arrow::Result<std::shared_ptr<marble::Record>> RecordFromRow(
        const std::shared_ptr<arrow::RecordBatch>& batch, int row_index) const override {
        if (row_index >= batch->num_rows()) {
            return arrow::Status::Invalid("Row index out of bounds");
        }

        // Extract values from the batch
        auto id_scalar = batch->GetColumnByName("id")->GetScalar(row_index);
        auto name_scalar = batch->GetColumnByName("name")->GetScalar(row_index);
        auto age_scalar = batch->GetColumnByName("age")->GetScalar(row_index);

        if (!id_scalar.ok() || !name_scalar.ok() || !age_scalar.ok()) {
            return arrow::Status::Invalid("Failed to extract scalar values");
        }

        int64_t id = std::static_pointer_cast<arrow::Int64Scalar>(id_scalar.ValueUnsafe())->value;
        std::string name = std::static_pointer_cast<arrow::StringScalar>(name_scalar.ValueUnsafe())->value->ToString();
        int32_t age = std::static_pointer_cast<arrow::Int32Scalar>(age_scalar.ValueUnsafe())->value;

        return std::make_shared<UserRecord>(id, name, age);
    }

private:
    std::vector<std::pair<std::string, std::shared_ptr<arrow::DataType>>> fields_;
};

// Example demonstrating checkpointing and streaming for operator/agent communication

class DataProcessor : public marble::OperatorInterface {
public:
    explicit DataProcessor(std::string name) : name_(std::move(name)), processed_count_(0) {}

    marble::Status Initialize(const std::vector<std::unique_ptr<marble::Stream>>& input_streams,
                             const std::vector<std::unique_ptr<marble::Stream>>& output_streams) override {
        if (input_streams.size() != 1 || output_streams.size() != 1) {
            return marble::Status::InvalidArgument("Processor requires exactly 1 input and 1 output stream");
        }

        input_stream_ = input_streams[0].get();
        output_stream_ = output_streams[0].get();
        return marble::Status::OK();
    }

    marble::Status Process() override {
        const size_t batch_size = 10;

        while (true) {
            // Receive batch of records
            std::vector<std::shared_ptr<marble::Record>> records;
            auto status = input_stream_->ReceiveRecords(&records, batch_size, false);

            if (!status.ok()) {
                if (status.IsNotFound()) {
                    // No more data available
                    break;
                }
                return status;
            }

            // Process records (example: add processing timestamp)
            std::vector<std::shared_ptr<marble::Record>> processed_records;
            for (auto& record : records) {
                processed_records.push_back(record); // In real processing, modify record
                processed_count_++;
            }

            // Send processed records to next operator
            status = output_stream_->SendRecords(processed_records);
            if (!status.ok()) return status;

            // Periodic checkpointing
            if (processed_count_ % 100 == 0) {
                std::string checkpoint_id = "checkpoint_" + std::to_string(processed_count_);
                status = Checkpoint(checkpoint_id);
                if (!status.ok()) {
                    std::cerr << "Checkpoint failed: " << status.ToString() << std::endl;
                }
            }
        }

        // Final flush
        return output_stream_->Flush();
    }

    marble::Status Checkpoint(const std::string& checkpoint_id) override {
        std::cout << "[" << name_ << "] Creating checkpoint: " << checkpoint_id
                  << " (processed: " << processed_count_ << ")" << std::endl;

        // In a full implementation, this would save state to persistent storage
        // For now, just log the checkpoint
        return marble::Status::OK();
    }

    marble::Status Restore(const std::string& checkpoint_id) override {
        std::cout << "[" << name_ << "] Restoring from checkpoint: " << checkpoint_id << std::endl;

        // In a full implementation, this would restore state from persistent storage
        return marble::Status::OK();
    }

    marble::Status GetStatus(std::string* status) const override {
        *status = "Processor " + name_ + ": processed " + std::to_string(processed_count_) + " records";
        return marble::Status::OK();
    }

    marble::Status GetMetrics(std::string* metrics) const override {
        *metrics = R"({
            "name": ")" + name_ + R"(",
            "processed_count": )" + std::to_string(processed_count_) + R"(
        })";
        return marble::Status::OK();
    }

    marble::Status Shutdown() override {
        std::cout << "[" << name_ << "] Shutting down" << std::endl;
        return marble::Status::OK();
    }

private:
    std::string name_;
    marble::Stream* input_stream_;
    marble::Stream* output_stream_;
    size_t processed_count_;
};

int main() {
    std::cout << "MarbleDB Streaming and Checkpointing Example" << std::endl;
    std::cout << "============================================" << std::endl;

    // Create stream factory
    auto stream_factory = marble::CreateStreamFactory();

    // Create streams for operator communication
    marble::StreamOptions stream_options;
    stream_options.max_buffer_size = 100;
    stream_options.enable_compression = true;

    std::unique_ptr<marble::Stream> input_stream;
    auto status = stream_factory->CreateMemoryStream("input_stream", stream_options, &input_stream);
    if (!status.ok()) {
        std::cerr << "Failed to create input stream: " << status.ToString() << std::endl;
        return 1;
    }

    std::unique_ptr<marble::Stream> output_stream;
    status = stream_factory->CreateMemoryStream("output_stream", stream_options, &output_stream);
    if (!status.ok()) {
        std::cerr << "Failed to create output stream: " << status.ToString() << std::endl;
        return 1;
    }

    // Create serializer for record serialization
    auto schema = std::make_shared<UserSchema>();
    auto serializer = marble::CreateSerializer(schema);

    // Create data processor
    auto processor = std::make_unique<DataProcessor>("DataProcessor-1");

    // Initialize processor with streams
    std::vector<std::unique_ptr<marble::Stream>> input_streams;
    input_streams.push_back(std::move(input_stream));

    std::vector<std::unique_ptr<marble::Stream>> output_streams;
    output_streams.push_back(std::move(output_stream));

    status = processor->Initialize(input_streams, output_streams);
    if (!status.ok()) {
        std::cerr << "Failed to initialize processor: " << status.ToString() << std::endl;
        return 1;
    }

    // Simulate data producer (running in separate thread)
    auto producer_thread = std::thread([input_stream_ptr = input_streams[0].get()]() {
        for (int i = 0; i < 50; ++i) {
            auto record = std::make_shared<UserRecord>(i, "User" + std::to_string(i), 20 + (i % 50));

            marble::ChangeRecord change{
                marble::ChangeType::kInsert,
                record->GetKey(),
                nullptr,
                record,
                static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count()),
                static_cast<uint64_t>(i + 1)
            };

            auto status = input_stream_ptr->SendChange(change);
            if (!status.ok()) {
                std::cerr << "Failed to send record: " << status.ToString() << std::endl;
                break;
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }

        // Close input stream to signal end of data
        input_stream_ptr->Close();
    });

    // Process data
    std::cout << "Starting data processing..." << std::endl;
    status = processor->Process();
    if (!status.ok()) {
        std::cerr << "Processing failed: " << status.ToString() << std::endl;
        producer_thread.join();
        return 1;
    }

    // Wait for producer to finish
    producer_thread.join();

    // Get final status
    std::string status_str;
    status = processor->GetStatus(&status_str);
    if (status.ok()) {
        std::cout << "Final status: " << status_str << std::endl;
    }

    std::string metrics;
    status = processor->GetMetrics(&metrics);
    if (status.ok()) {
        std::cout << "Final metrics: " << metrics << std::endl;
    }

    // Demonstrate checkpointing
    std::cout << "\nDemonstrating checkpointing..." << std::endl;
    status = processor->Checkpoint("final_checkpoint");
    if (status.ok()) {
        std::cout << "Checkpoint created successfully" << std::endl;
    }

    // Create checkpoint manager
    auto checkpoint_manager = marble::CreateCheckpointManager("/tmp/marble_checkpoints");

    std::vector<std::string> checkpoints;
    status = checkpoint_manager->ListCheckpoints(&checkpoints);
    std::cout << "Available checkpoints: " << checkpoints.size() << std::endl;

    // Demonstrate serialization
    std::cout << "\nDemonstrating record serialization..." << std::endl;
    auto test_record = std::make_shared<UserRecord>(999, "TestUser", 25);

    std::shared_ptr<arrow::Buffer> serialized_buffer;
    status = serializer->SerializeRecord(*test_record, &serialized_buffer);
    if (status.ok()) {
        std::cout << "Record serialized successfully, size: " << serialized_buffer->size() << " bytes" << std::endl;

        // Deserialize back
        std::shared_ptr<marble::Record> deserialized_record;
        status = serializer->DeserializeRecord(serialized_buffer, &deserialized_record);
        if (status.ok()) {
            std::cout << "Record deserialized successfully" << std::endl;
        } else {
            std::cout << "Deserialization failed: " << status.ToString() << std::endl;
        }
    } else {
        std::cout << "Serialization failed: " << status.ToString() << std::endl;
    }

    // Shutdown
    status = processor->Shutdown();
    if (status.ok()) {
        std::cout << "Processor shut down successfully" << std::endl;
    }

    std::cout << "\nStreaming and checkpointing example completed successfully!" << std::endl;
    return 0;
}
