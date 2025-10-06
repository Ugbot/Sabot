#include <marble/query.h>
#include <marble/db.h>
#include <marble/record.h>
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <algorithm>
#include <memory>
#include <future>

namespace marble {

// Simple key implementation for seeking
class SimpleKey : public Key {
public:
    explicit SimpleKey(const std::string& value) : value_(value) {}

    int Compare(const Key& other) const override {
        const SimpleKey& other_key = static_cast<const SimpleKey&>(other);
        if (value_ < other_key.value_) return -1;
        if (value_ > other_key.value_) return 1;
        return 0;
    }

    arrow::Result<std::shared_ptr<arrow::Scalar>> ToArrowScalar() const override {
        return arrow::MakeScalar(value_);
    }

    std::shared_ptr<Key> Clone() const override {
        return std::make_shared<SimpleKey>(value_);
    }

    std::string ToString() const override {
        return value_;
    }

    size_t Hash() const override {
        return std::hash<std::string>()(value_);
    }

private:
    std::string value_;
};

// Forward declarations
class MarbleDB;

// Predicate evaluator implementation
PredicateEvaluator::PredicateEvaluator(const std::vector<ColumnPredicate>& predicates)
    : predicates_(predicates) {
    // Separate predicates into key-range and record-level predicates
    for (const auto& pred : predicates) {
        if (pred.CanPushDownToKeyRange()) {
            key_range_predicates_.push_back(pred);
        } else {
            record_predicates_.push_back(pred);
        }
    }
}

bool PredicateEvaluator::Evaluate(const std::shared_ptr<Record>& record) const {
    // For now, simple evaluation - all records pass
    // TODO: Implement actual predicate evaluation against record data
    return true;
}

std::vector<ColumnPredicate> PredicateEvaluator::GetKeyRangePredicates() const {
    return key_range_predicates_;
}

std::vector<ColumnPredicate> PredicateEvaluator::GetRecordPredicates() const {
    return record_predicates_;
}

// Column projector implementation
ColumnProjector::ColumnProjector(const std::vector<std::string>& columns)
    : projected_columns_(columns), full_projection_(columns.empty()) {}

Status ColumnProjector::ProjectToArrow(const std::vector<std::shared_ptr<Record>>& records,
                                      std::vector<std::shared_ptr<arrow::Array>>* arrays) const {
    if (records.empty()) {
        return Status::OK();
    }

    arrays->clear();

    if (full_projection_) {
        // Full projection - create arrays for all columns
        arrow::Int64Builder id_builder;
        arrow::StringBuilder name_builder;
        arrow::StringBuilder value_builder;

        for (const auto& record : records) {
            // Placeholder: extract data from record
            auto id_status = id_builder.Append(static_cast<int64_t>(std::hash<std::string>()(record->GetKey()->ToString()) % 1000));
            if (!id_status.ok()) return Status::InternalError("Failed to append ID");

            auto name_status = name_builder.Append("record_" + record->GetKey()->ToString());
            if (!name_status.ok()) return Status::InternalError("Failed to append name");

            auto value_status = value_builder.Append("value_" + record->GetKey()->ToString());
            if (!value_status.ok()) return Status::InternalError("Failed to append value");
        }

        std::shared_ptr<arrow::Array> id_array, name_array, value_array;
        auto id_finish = id_builder.Finish(&id_array);
        if (!id_finish.ok()) return Status::InternalError("Failed to finish ID array");

        auto name_finish = name_builder.Finish(&name_array);
        if (!name_finish.ok()) return Status::InternalError("Failed to finish name array");

        auto value_finish = value_builder.Finish(&value_array);
        if (!value_finish.ok()) return Status::InternalError("Failed to finish value array");

        arrays->push_back(id_array);
        arrays->push_back(name_array);
        arrays->push_back(value_array);
    } else {
        // Selective projection - only create arrays for requested columns
        for (const auto& col : projected_columns_) {
            if (col == "id") {
                arrow::Int64Builder builder;
                for (const auto& record : records) {
                    auto status = builder.Append(static_cast<int64_t>(std::hash<std::string>()(record->GetKey()->ToString()) % 1000));
                    if (!status.ok()) return Status::InternalError("Failed to append ID");
                }
                std::shared_ptr<arrow::Array> array;
                auto finish_status = builder.Finish(&array);
                if (!finish_status.ok()) return Status::InternalError("Failed to finish ID array");
                arrays->push_back(array);
            } else if (col == "name") {
                arrow::StringBuilder builder;
                for (const auto& record : records) {
                    auto status = builder.Append("record_" + record->GetKey()->ToString());
                    if (!status.ok()) return Status::InternalError("Failed to append name");
                }
                std::shared_ptr<arrow::Array> array;
                auto finish_status = builder.Finish(&array);
                if (!finish_status.ok()) return Status::InternalError("Failed to finish name array");
                arrays->push_back(array);
            } else if (col == "value") {
                arrow::StringBuilder builder;
                for (const auto& record : records) {
                    auto status = builder.Append("value_" + record->GetKey()->ToString());
                    if (!status.ok()) return Status::InternalError("Failed to append value");
                }
                std::shared_ptr<arrow::Array> array;
                auto finish_status = builder.Finish(&array);
                if (!finish_status.ok()) return Status::InternalError("Failed to finish value array");
                arrays->push_back(array);
            }
        }
    }

    return Status::OK();
}

std::shared_ptr<arrow::Schema> ColumnProjector::GetProjectedSchema() const {
    std::vector<std::shared_ptr<arrow::Field>> fields;

    if (full_projection_) {
        fields.push_back(arrow::field("id", arrow::int64()));
        fields.push_back(arrow::field("name", arrow::utf8()));
        fields.push_back(arrow::field("value", arrow::utf8()));
    } else {
        for (const auto& col : projected_columns_) {
            if (col == "id") {
                fields.push_back(arrow::field("id", arrow::int64()));
            } else if (col == "name") {
                fields.push_back(arrow::field("name", arrow::utf8()));
            } else if (col == "value") {
                fields.push_back(arrow::field("value", arrow::utf8()));
            }
        }
    }

    return arrow::schema(fields);
}

bool ColumnProjector::IsFullProjection() const {
    return full_projection_;
}

// QueryResult implementation that returns Arrow RecordBatches
class ArrowQueryResult : public QueryResult {
public:
    ArrowQueryResult(std::vector<std::shared_ptr<arrow::RecordBatch>> batches,
                    std::shared_ptr<arrow::Schema> schema)
        : batches_(std::move(batches)), schema_(std::move(schema)), current_index_(0) {}

    bool HasNext() const override {
        return current_index_ < static_cast<int64_t>(batches_.size());
    }

    Status Next(std::shared_ptr<arrow::RecordBatch>* batch) override {
        if (!HasNext()) {
            return Status::InvalidArgument("No more batches available");
        }
        *batch = batches_[current_index_++];
        return Status::OK();
    }

    Status NextAsync(std::function<void(Status, std::shared_ptr<arrow::RecordBatch>)> callback) override {
        std::shared_ptr<arrow::RecordBatch> batch;
        Status status = Next(&batch);
        callback(status, batch);
        return status;
    }

    std::shared_ptr<arrow::Schema> schema() const override {
        return schema_;
    }

    int64_t num_rows() const override {
        int64_t total = 0;
        for (const auto& batch : batches_) {
            total += batch->num_rows();
        }
        return total;
    }

    int64_t num_batches() const override {
        return current_index_;
    }

private:
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches_;
    std::shared_ptr<arrow::Schema> schema_;
    int64_t current_index_;
};

// Streaming query result for large datasets
StreamingQueryResult::StreamingQueryResult(std::shared_ptr<arrow::Schema> schema, MarbleDB* db,
                                          const std::vector<ColumnPredicate>& predicates,
                                          const QueryOptions& options)
    : schema_(std::move(schema)), db_(db), predicates_(predicates), options_(options),
      current_batch_(0), total_rows_(0), finished_(false) {
    // Set up predicate evaluator and column projector
    predicate_evaluator_ = CreatePredicateEvaluator(predicates_);
    column_projector_ = CreateColumnProjector(options_.projection_columns);

    // Initialize iterator for streaming with predicate pushdown
    ReadOptions read_opts;
    KeyRange key_range = KeyRange::All();

    // Use predicate pushdown to convert key-range predicates to key ranges
    if (options_.use_predicate_pushdown && predicate_evaluator_) {
        auto key_range_preds = predicate_evaluator_->GetKeyRangePredicates();
        if (!key_range_preds.empty()) {
            key_range = PredicatesToKeyRange(key_range_preds);
        }
    }

    Status status = db_->NewIterator(read_opts, key_range, &iterator_);
    if (status.ok() && iterator_) {
        // Seek to the beginning of the key range
        if (key_range.start()) {
            iterator_->Seek(*key_range.start());
        } else {
            iterator_->Seek(SimpleKey(""));
        }
    }
}

StreamingQueryResult::~StreamingQueryResult() = default;

bool StreamingQueryResult::HasNext() const {
    return !finished_ && iterator_ && iterator_->Valid();
}

Status StreamingQueryResult::Next(std::shared_ptr<arrow::RecordBatch>* batch) {
    if (!HasNext()) {
        return Status::InvalidArgument("No more data available");
    }

    // Collect records for this batch
    std::vector<std::shared_ptr<Record>> batch_records;
    size_t collected = 0;

    while (collected < static_cast<size_t>(options_.batch_size) &&
           iterator_->Valid()) {

        auto record = iterator_->value();
        if (!predicate_evaluator_ || predicate_evaluator_->Evaluate(record)) {
            batch_records.push_back(record);
            collected++;
        }
        iterator_->Next();
    }

    // Check if we've reached the end
    if (!iterator_->Valid()) {
        finished_ = true;
    }

    // Convert records to Arrow RecordBatch
    if (batch_records.empty()) {
        return Status::InvalidArgument("No data in batch");
    }

    Status status = RecordsToArrowBatch(batch_records, batch);
    if (status.ok()) {
        current_batch_++;
        total_rows_ += batch_records.size();
    }

    return status;
}

Status StreamingQueryResult::NextAsync(std::function<void(Status, std::shared_ptr<arrow::RecordBatch>)> callback) {
    // Run Next() in a separate thread for async operation
    auto future = std::async(std::launch::async, [this]() -> std::pair<Status, std::shared_ptr<arrow::RecordBatch>> {
        std::shared_ptr<arrow::RecordBatch> batch;
        Status status = Next(&batch);
        return {status, batch};
    });

    // When the async operation completes, call the callback
    std::thread([future = std::move(future), callback = std::move(callback)]() mutable {
        auto [status, batch] = future.get();
        callback(status, batch);
    }).detach();

    return Status::OK();
}

std::shared_ptr<arrow::Schema> StreamingQueryResult::schema() const {
    return schema_;
}

int64_t StreamingQueryResult::num_rows() const {
    return total_rows_;
}

int64_t StreamingQueryResult::num_batches() const {
    return current_batch_;
}


Status StreamingQueryResult::RecordsToArrowBatch(const std::vector<std::shared_ptr<Record>>& records,
                                                std::shared_ptr<arrow::RecordBatch>* batch) const {
    if (records.empty()) {
        return Status::InvalidArgument("No records to convert");
    }

    // Use column projector if available
    if (column_projector_) {
        std::vector<std::shared_ptr<arrow::Array>> arrays;
        Status status = column_projector_->ProjectToArrow(records, &arrays);
        if (!status.ok()) {
            return status;
        }

        *batch = arrow::RecordBatch::Make(schema_, records.size(), arrays);
        return Status::OK();
    }

    // Fallback: create full projection
    arrow::Int64Builder id_builder;
    arrow::StringBuilder name_builder;
    arrow::StringBuilder value_builder;

    for (size_t i = 0; i < records.size(); ++i) {
        auto record = records[i];

        auto id_status = id_builder.Append(static_cast<int64_t>(total_rows_ + i));
        if (!id_status.ok()) return Status::InternalError("Failed to append ID");

        auto name_status = name_builder.Append("record_" + record->GetKey()->ToString());
        if (!name_status.ok()) return Status::InternalError("Failed to append name");

        auto value_status = value_builder.Append("value_" + record->GetKey()->ToString());
        if (!value_status.ok()) return Status::InternalError("Failed to append value");
    }

    std::shared_ptr<arrow::Array> id_array, name_array, value_array;
    auto id_finish = id_builder.Finish(&id_array);
    if (!id_finish.ok()) return Status::InternalError("Failed to finish ID array");

    auto name_finish = name_builder.Finish(&name_array);
    if (!name_finish.ok()) return Status::InternalError("Failed to finish name array");

    auto value_finish = value_builder.Finish(&value_array);
    if (!value_finish.ok()) return Status::InternalError("Failed to finish value array");

    *batch = arrow::RecordBatch::Make(schema_, records.size(), {id_array, name_array, value_array});
    return Status::OK();
}

// Query implementation
class ArrowQuery : public Query {
public:
    explicit ArrowQuery(MarbleDB* db) : db_(db) {}

    Status Execute(std::unique_ptr<QueryResult>* result) override {
        // Set up predicate evaluation and column projection
        predicate_evaluator_ = CreatePredicateEvaluator(predicates_);
        column_projector_ = CreateColumnProjector(options_.projection_columns);

        // Convert key-range predicates to key ranges for pushdown
        KeyRange key_range = KeyRange::All();
        if (options_.use_predicate_pushdown) {
            auto key_range_preds = predicate_evaluator_->GetKeyRangePredicates();
            if (!key_range_preds.empty()) {
                key_range = PredicatesToKeyRange(key_range_preds);
            }
        }

        // Use streaming result for memory efficiency
        auto streaming_result = std::make_unique<StreamingQueryResult>(
            column_projector_->GetProjectedSchema(),
            db_,
            predicate_evaluator_->GetRecordPredicates(), // Only record-level predicates
            options_);

        *result = std::move(streaming_result);
        return Status::OK();
    }

    std::shared_ptr<arrow::Schema> schema() const override {
        if (column_projector_) {
            return column_projector_->GetProjectedSchema();
        }
        // Fallback schema
        auto id_field = arrow::field("id", arrow::int64());
        auto name_field = arrow::field("name", arrow::utf8());
        auto value_field = arrow::field("value", arrow::utf8());
        return arrow::schema({id_field, name_field, value_field});
    }

    Status AddPredicate(const ColumnPredicate& predicate) override {
        predicates_.push_back(predicate);
        return Status::OK();
    }

    Status SetOptions(const QueryOptions& options) override {
        options_ = options;
        return Status::OK();
    }

    Status AddProjection(const std::string& column) override {
        if (std::find(options_.projection_columns.begin(),
                     options_.projection_columns.end(), column) ==
            options_.projection_columns.end()) {
            options_.projection_columns.push_back(column);
        }
        return Status::OK();
    }

    Status SetLimit(int64_t limit) override {
        options_.limit = limit;
        return Status::OK();
    }

    Status SetOffset(int64_t offset) override {
        options_.offset = offset;
        return Status::OK();
    }

private:
    MarbleDB* db_;
    std::vector<ColumnPredicate> predicates_;
    QueryOptions options_;
    std::unique_ptr<PredicateEvaluator> predicate_evaluator_;
    std::unique_ptr<ColumnProjector> column_projector_;
};

// QueryBuilder implementation
QueryBuilder::QueryBuilder(MarbleDB* db) : db_(db) {}

std::unique_ptr<QueryBuilder> QueryBuilder::From(MarbleDB* db) {
    return std::make_unique<QueryBuilder>(db);
}

QueryBuilder& QueryBuilder::Where(const std::string& column, PredicateType type,
                                 std::shared_ptr<arrow::Scalar> value) {
    predicates_.emplace_back(column, type, value);
    return *this;
}

QueryBuilder& QueryBuilder::Where(const std::string& column, PredicateType type,
                                 std::shared_ptr<arrow::Scalar> value1,
                                 std::shared_ptr<arrow::Scalar> value2) {
    predicates_.emplace_back(column, type, value1, value2);
    return *this;
}

QueryBuilder& QueryBuilder::Select(const std::vector<std::string>& columns) {
    options_.projection_columns = columns;
    return *this;
}

QueryBuilder& QueryBuilder::SelectAll() {
    options_.projection_columns.clear();
    return *this;
}

QueryBuilder& QueryBuilder::Limit(int64_t limit) {
    options_.limit = limit;
    return *this;
}

QueryBuilder& QueryBuilder::Offset(int64_t offset) {
    options_.offset = offset;
    return *this;
}

QueryBuilder& QueryBuilder::WithOptions(const QueryOptions& options) {
    options_ = options;
    return *this;
}

Status QueryBuilder::Execute(std::unique_ptr<QueryResult>* result) {
    auto query = Build();
    return query->Execute(result);
}

std::unique_ptr<Query> QueryBuilder::Build() {
    auto query = std::make_unique<ArrowQuery>(db_);

    // Set options
    query->SetOptions(options_);

    // Add predicates
    for (const auto& pred : predicates_) {
        query->AddPredicate(pred);
    }

    // Add projections
    for (const auto& col : options_.projection_columns) {
        query->AddProjection(col);
    }

    return query;
}

// Scanner implementation
class MarbleScanner : public Scanner {
public:
    explicit MarbleScanner(MarbleDB* db) : db_(db) {}

    Status Scan(const std::vector<ColumnPredicate>& predicates,
               const QueryOptions& options,
               std::unique_ptr<QueryResult>* result) override {
        auto query = std::make_unique<ArrowQuery>(db_);
        query->SetOptions(options);

        for (const auto& pred : predicates) {
            query->AddPredicate(pred);
        }

        return query->Execute(result);
    }

    std::shared_ptr<arrow::Schema> schema() const override {
        // Return same schema as ArrowQuery
        auto id_field = arrow::field("id", arrow::int64());
        auto name_field = arrow::field("name", arrow::utf8());
        auto value_field = arrow::field("value", arrow::utf8());
        return arrow::schema({id_field, name_field, value_field});
    }

    int64_t CountRows() const override {
        // TODO: Implement approximate row counting
        return -1;  // Unknown
    }

private:
    MarbleDB* db_;
};

// MarbleDataset implementation
MarbleDataset::MarbleDataset(MarbleDB* db) : db_(db) {}

std::unique_ptr<Scanner> MarbleDataset::NewScanner() {
    return std::make_unique<MarbleScanner>(db_);
}

std::shared_ptr<arrow::Schema> MarbleDataset::schema() const {
    // Return same schema as scanner
    auto id_field = arrow::field("id", arrow::int64());
    auto name_field = arrow::field("name", arrow::utf8());
    auto value_field = arrow::field("value", arrow::utf8());
    return arrow::schema({id_field, name_field, value_field});
}

Status MarbleDataset::GetInfo(std::string* info) const {
    *info = "MarbleDB Dataset - Arrow-compatible columnar storage";
    return Status::OK();
}

// Factory functions
std::unique_ptr<QueryBuilder> QueryFrom(MarbleDB* db) {
    return QueryBuilder::From(db);
}

std::unique_ptr<MarbleDataset> DatasetFrom(MarbleDB* db) {
    return std::make_unique<MarbleDataset>(db);
}

// Convert predicates to key ranges for pushdown
KeyRange PredicatesToKeyRange(const std::vector<ColumnPredicate>& predicates) {
    // For now, simple implementation - find min/max bounds for key-based predicates
    std::shared_ptr<Key> start_key;
    std::shared_ptr<Key> end_key;
    bool start_inclusive = true;
    bool end_inclusive = true;

    for (const auto& pred : predicates) {
        if (pred.CanPushDownToKeyRange()) {
            // Extract string value from Arrow scalar
            std::string value_str;
            if (pred.value->type->id() == arrow::Type::STRING) {
                auto string_scalar = std::static_pointer_cast<arrow::StringScalar>(pred.value);
                value_str = string_scalar->value->ToString();
            } else {
                // For non-string types, convert to string representation
                value_str = std::to_string(pred.value->hash());
            }

            if (pred.type == PredicateType::kEqual) {
                // For equality, set both start and end to the same value
                start_key = std::make_shared<SimpleKey>(value_str);
                end_key = std::make_shared<SimpleKey>(value_str);
                start_inclusive = true;
                end_inclusive = true;
            } else if (pred.type == PredicateType::kGreaterThan ||
                      pred.type == PredicateType::kGreaterThanOrEqual) {
                start_key = std::make_shared<SimpleKey>(value_str);
                start_inclusive = (pred.type == PredicateType::kGreaterThanOrEqual);
            } else if (pred.type == PredicateType::kLessThan ||
                      pred.type == PredicateType::kLessThanOrEqual) {
                end_key = std::make_shared<SimpleKey>(value_str);
                end_inclusive = (pred.type == PredicateType::kLessThanOrEqual);
            }
        }
    }

    if (start_key || end_key) {
        return KeyRange(start_key, start_inclusive, end_key, end_inclusive);
    }

    return KeyRange::All();
}

// Create predicate evaluator
std::unique_ptr<PredicateEvaluator> CreatePredicateEvaluator(
    const std::vector<ColumnPredicate>& predicates) {
    return std::make_unique<PredicateEvaluator>(predicates);
}

// Create column projector
std::unique_ptr<ColumnProjector> CreateColumnProjector(
    const std::vector<std::string>& columns) {
    return std::make_unique<ColumnProjector>(columns);
}

std::unique_ptr<StreamingQueryResult> CreateStreamingQueryResult(
    std::shared_ptr<arrow::Schema> schema, MarbleDB* db,
    const std::vector<ColumnPredicate>& predicates,
    const QueryOptions& options) {
    return std::make_unique<StreamingQueryResult>(schema, db, predicates, options);
}

} // namespace marble
