#include <marble/marble.h>
#include <arrow/api.h>
#include <iostream>
#include <memory>

// Example user record
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
        ARROW_RETURN_NOT_OK(id_builder.Append(id_));
        std::shared_ptr<arrow::Array> id_array;
        ARROW_ASSIGN_OR_RAISE(id_array, id_builder.Finish());

        arrow::StringBuilder name_builder;
        ARROW_RETURN_NOT_OK(name_builder.Append(name_));
        std::shared_ptr<arrow::Array> name_array;
        ARROW_ASSIGN_OR_RAISE(name_array, name_builder.Finish());

        arrow::Int32Builder age_builder;
        ARROW_RETURN_NOT_OK(age_builder.Append(age_));
        std::shared_ptr<arrow::Array> age_array;
        ARROW_ASSIGN_OR_RAISE(age_array, age_builder.Finish());

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

    // Getters
    int64_t id() const { return id_; }
    const std::string& name() const { return name_; }
    int32_t age() const { return age_; }

private:
    int64_t id_;
    std::string name_;
    int32_t age_;
};

// Example schema implementation
class UserSchema : public marble::Schema {
public:
    std::shared_ptr<arrow::Schema> GetArrowSchema() const override {
        return arrow::schema({
            arrow::field("id", arrow::int64()),
            arrow::field("name", arrow::utf8()),
            arrow::field("age", arrow::int32())
        });
    }

    const std::vector<size_t>& GetPrimaryKeyIndices() const override {
        static const std::vector<size_t> indices = {0}; // id field
        return indices;
    }

    arrow::Result<std::shared_ptr<marble::Record>> RecordFromRow(
        const std::shared_ptr<arrow::RecordBatch>& batch, int row_index) const override {

        auto id_array = std::static_pointer_cast<arrow::Int64Array>(batch->column(0));
        auto name_array = std::static_pointer_cast<arrow::StringArray>(batch->column(1));
        auto age_array = std::static_pointer_cast<arrow::Int32Array>(batch->column(2));

        int64_t id = id_array->Value(row_index);
        std::string name = name_array->GetString(row_index);
        int32_t age = age_array->Value(row_index);

        return std::make_shared<UserRecord>(id, name, age);
    }

    arrow::Result<std::shared_ptr<marble::Key>> KeyFromScalar(
        const std::shared_ptr<arrow::Scalar>& scalar) const override {
        auto int_scalar = std::static_pointer_cast<arrow::Int64Scalar>(scalar);
        return std::make_shared<UserRecord>(int_scalar->value, "", 0)->GetKey();
    }
};

int main() {
    std::cout << "MarbleDB Basic Example" << std::endl;
    std::cout << "====================" << std::endl;

    // Create some records
    auto record1 = std::make_shared<UserRecord>(1, "Alice", 30);
    auto record2 = std::make_shared<UserRecord>(2, "Bob", 25);

    std::cout << "Created records for Alice and Bob" << std::endl;

    // Test record serialization
    auto key1 = record1->GetKey();
    std::cout << "Record 1 key: " << key1->ToString() << std::endl;

    // Convert to Arrow RecordBatch
    auto batch_result = record1->ToRecordBatch();
    if (batch_result.ok()) {
        auto batch = batch_result.ValueUnsafe();
        std::cout << "Successfully converted record to Arrow RecordBatch" << std::endl;
        std::cout << "Schema: " << batch->schema()->ToString() << std::endl;
        std::cout << "Num rows: " << batch->num_rows() << std::endl;
    } else {
        std::cout << "Failed to convert record to RecordBatch: " << batch_result.status().ToString() << std::endl;
    }

    // Create schema
    auto schema = std::make_shared<UserSchema>();
    std::cout << "Created schema with " << schema->GetArrowSchema()->num_fields() << " fields" << std::endl;

    std::cout << "Basic example completed successfully!" << std::endl;
    return 0;
}
