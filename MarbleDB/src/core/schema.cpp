#include <marble/record.h>
#include <arrow/api.h>
#include <iostream>

// Simple implementation demonstrating Arrow integration

namespace marble {

// Simple string key implementation
class StringKey : public Key {
public:
    StringKey(std::string value) : value_(std::move(value)) {}

    int Compare(const Key& other) const override {
        const StringKey* other_key = dynamic_cast<const StringKey*>(&other);
        if (!other_key) return -1;
        return value_.compare(other_key->value_);
    }

    arrow::Result<std::shared_ptr<arrow::Scalar>> ToArrowScalar() const override {
        return std::make_shared<arrow::StringScalar>(value_);
    }

    std::shared_ptr<Key> Clone() const override {
        return std::make_shared<StringKey>(value_);
    }

    std::string ToString() const override {
        return value_;
    }

    size_t Hash() const override {
        return std::hash<std::string>{}(value_);
    }

private:
    std::string value_;
};

// Simple record implementation
class SimpleRecord : public Record {
public:
    SimpleRecord(std::string id, std::string name, int32_t age)
        : id_(std::move(id)), name_(std::move(name)), age_(age) {}

    std::shared_ptr<Key> GetKey() const override {
        return std::make_shared<StringKey>(id_);
    }

    arrow::Result<std::shared_ptr<arrow::RecordBatch>> ToRecordBatch() const override {
        auto schema = arrow::schema({
            arrow::field("id", arrow::utf8()),
            arrow::field("name", arrow::utf8()),
            arrow::field("age", arrow::int32())
        });

        arrow::StringBuilder id_builder;
        ARROW_RETURN_NOT_OK(id_builder.Append(id_));

        arrow::StringBuilder name_builder;
        ARROW_RETURN_NOT_OK(name_builder.Append(name_));

        arrow::Int32Builder age_builder;
        ARROW_RETURN_NOT_OK(age_builder.Append(age_));

        std::shared_ptr<arrow::Array> id_array, name_array, age_array;
        ARROW_RETURN_NOT_OK(id_builder.Finish(&id_array));
        ARROW_RETURN_NOT_OK(name_builder.Finish(&name_array));
        ARROW_RETURN_NOT_OK(age_builder.Finish(&age_array));

        return arrow::RecordBatch::Make(schema, 1, {id_array, name_array, age_array});
    }

    std::shared_ptr<arrow::Schema> GetArrowSchema() const override {
        return arrow::schema({
            arrow::field("id", arrow::utf8()),
            arrow::field("name", arrow::utf8()),
            arrow::field("age", arrow::int32())
        });
    }

private:
    std::string id_;
    std::string name_;
    int32_t age_;
};

// Simple schema implementation
class SimpleSchema : public Schema {
public:
    std::shared_ptr<arrow::Schema> GetArrowSchema() const override {
        return arrow::schema({
            arrow::field("id", arrow::utf8()),
            arrow::field("name", arrow::utf8()),
            arrow::field("age", arrow::int32())
        });
    }

    const std::vector<size_t>& GetPrimaryKeyIndices() const override {
        static const std::vector<size_t> indices = {0}; // id field
        return indices;
    }

    arrow::Result<std::shared_ptr<Record>> RecordFromRow(
        const std::shared_ptr<arrow::RecordBatch>& batch, int row_index) const override {

        auto id_array = std::static_pointer_cast<arrow::StringArray>(batch->column(0));
        auto name_array = std::static_pointer_cast<arrow::StringArray>(batch->column(1));
        auto age_array = std::static_pointer_cast<arrow::Int32Array>(batch->column(2));

        std::string id = id_array->GetString(row_index);
        std::string name = name_array->GetString(row_index);
        int32_t age = age_array->Value(row_index);

        return std::make_shared<SimpleRecord>(id, name, age);
    }

    arrow::Result<std::shared_ptr<Key>> KeyFromScalar(
        const std::shared_ptr<arrow::Scalar>& scalar) const override {
        auto str_scalar = std::static_pointer_cast<arrow::StringScalar>(scalar);
        return std::make_shared<StringKey>(str_scalar->value->ToString());
    }
};

} // namespace marble
