#pragma once

#include <memory>
#include <vector>
#include <string>
#include <arrow/api.h>
#include <marble/status.h>
#include <marble/record.h>

namespace marble {

// Forward declarations
class MarbleDB;

// Predicate types for filtering
enum class PredicateType {
    kEqual,
    kNotEqual,
    kLessThan,
    kLessThanOrEqual,
    kGreaterThan,
    kGreaterThanOrEqual,
    kBetween,
    kIn,
    kIsNull,
    kIsNotNull,
    kLike,
    kRegex
};

// Column predicate for filtering
struct ColumnPredicate {
    std::string column_name;
    PredicateType type;
    std::shared_ptr<arrow::Scalar> value;
    std::shared_ptr<arrow::Scalar> value2;  // For BETWEEN operations

    ColumnPredicate(std::string col, PredicateType t,
                   std::shared_ptr<arrow::Scalar> val,
                   std::shared_ptr<arrow::Scalar> val2 = nullptr)
        : column_name(std::move(col)), type(t), value(std::move(val)), value2(std::move(val2)) {}

    // Check if this predicate can be converted to a key range
    bool CanPushDownToKeyRange() const {
        return column_name == "id" || column_name == "key"; // For now, only key-based predicates
    }
};

// Predicate evaluator for record filtering
class PredicateEvaluator {
public:
    explicit PredicateEvaluator(const std::vector<ColumnPredicate>& predicates);

    // Evaluate if a record matches all predicates
    bool Evaluate(const std::shared_ptr<Record>& record) const;

    // Get predicates that can be pushed down to key ranges
    std::vector<ColumnPredicate> GetKeyRangePredicates() const;

    // Get predicates that must be evaluated per record
    std::vector<ColumnPredicate> GetRecordPredicates() const;

private:
    std::vector<ColumnPredicate> predicates_;
    std::vector<ColumnPredicate> key_range_predicates_;
    std::vector<ColumnPredicate> record_predicates_;
};

// Column projector for SELECT operations
class ColumnProjector {
public:
    explicit ColumnProjector(const std::vector<std::string>& columns);

    // Project columns from a record to Arrow arrays
    Status ProjectToArrow(const std::vector<std::shared_ptr<Record>>& records,
                         std::vector<std::shared_ptr<arrow::Array>>* arrays) const;

    // Get the Arrow schema for projected columns
    std::shared_ptr<arrow::Schema> GetProjectedSchema() const;

    // Check if all columns are projected (no projection needed)
    bool IsFullProjection() const;

private:
    std::vector<std::string> projected_columns_;
    bool full_projection_;
};

// Query options
struct QueryOptions {
    bool use_predicate_pushdown = true;
    int64_t limit = -1;  // -1 means no limit
    int64_t offset = 0;
    std::vector<std::string> projection_columns;  // Empty means all columns
    bool parallel_scan = true;
    int batch_size = 1024;  // Arrow record batch size
    bool reverse_order = false;  // Scan in reverse order (descending)
};

// Query result that returns Arrow RecordBatches
class QueryResult {
public:
    virtual ~QueryResult() = default;

    // Check if there are more batches available
    virtual bool HasNext() const = 0;

    // Get next Arrow RecordBatch
    virtual Status Next(std::shared_ptr<arrow::RecordBatch>* batch) = 0;

    // Get schema of the result
    virtual std::shared_ptr<arrow::Schema> schema() const = 0;

    // Get total number of rows (approximate if not known)
    virtual int64_t num_rows() const = 0;

    // Get number of batches processed so far
    virtual int64_t num_batches() const = 0;

    // Get next batch asynchronously (streaming interface)
    virtual Status NextAsync(std::function<void(Status, std::shared_ptr<arrow::RecordBatch>)> callback) = 0;
};

// Concrete implementation of QueryResult for table-based results
class TableQueryResult : public QueryResult {
public:
    static Status Create(std::shared_ptr<arrow::Table> table, std::unique_ptr<QueryResult>* result) {
        auto query_result = std::unique_ptr<TableQueryResult>(new TableQueryResult(table));
        // Convert table to batches for streaming
        ARROW_ASSIGN_OR_RAISE(query_result->batches_, arrow::TableBatchReader(*table).ToRecordBatches());
        *result = std::move(query_result);
        return Status::OK();
    }

private:
    explicit TableQueryResult(std::shared_ptr<arrow::Table> table)
        : table_(table), current_batch_(0), batch_size_(1024), num_batches_processed_(0) {}

    bool HasNext() const override {
        return current_batch_ < static_cast<int64_t>(batches_.size());
    }

    Status Next(std::shared_ptr<arrow::RecordBatch>* batch) override {
        if (!HasNext()) {
            return Status::InvalidArgument("No more batches available");
        }
        *batch = batches_[current_batch_++];
        num_batches_processed_++;
        return Status::OK();
    }

    Status NextAsync(std::function<void(Status, std::shared_ptr<arrow::RecordBatch>)> callback) override {
        std::shared_ptr<arrow::RecordBatch> batch;
        Status status = Next(&batch);
        callback(status, batch);
        return status;
    }

    std::shared_ptr<arrow::Schema> schema() const override {
        return table_->schema();
    }

    int64_t num_rows() const override {
        return table_->num_rows();
    }

    int64_t num_batches() const override {
        return num_batches_processed_;
    }

private:
    std::shared_ptr<arrow::Table> table_;
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches_;
    int64_t current_batch_;
    int64_t batch_size_;
    int64_t num_batches_processed_;
};

// Streaming query result for large datasets
class StreamingQueryResult : public QueryResult {
public:
    StreamingQueryResult(std::shared_ptr<arrow::Schema> schema, MarbleDB* db,
                        const std::vector<ColumnPredicate>& predicates,
                        const QueryOptions& options);

    ~StreamingQueryResult() override;

    // QueryResult interface
    bool HasNext() const override;
    Status Next(std::shared_ptr<arrow::RecordBatch>* batch) override;
    Status NextAsync(std::function<void(Status, std::shared_ptr<arrow::RecordBatch>)> callback) override;
    std::shared_ptr<arrow::Schema> schema() const override;
    int64_t num_rows() const override;
    int64_t num_batches() const override;

private:
    std::shared_ptr<arrow::Schema> schema_;
    MarbleDB* db_;
    std::vector<ColumnPredicate> predicates_;
    QueryOptions options_;
    std::unique_ptr<Iterator> iterator_;
    std::unique_ptr<PredicateEvaluator> predicate_evaluator_;
    std::unique_ptr<ColumnProjector> column_projector_;
    int64_t current_batch_;
    int64_t total_rows_;
    bool finished_;

    Status RecordsToArrowBatch(const std::vector<std::shared_ptr<Record>>& records,
                              std::shared_ptr<arrow::RecordBatch>* batch) const;
};

// Query interface that looks like Arrow Dataset
class Query {
public:
    virtual ~Query() = default;

    // Execute query and return result
    virtual Status Execute(std::unique_ptr<QueryResult>* result) = 0;

    // Get the schema that would be returned
    virtual std::shared_ptr<arrow::Schema> schema() const = 0;

    // Add column predicate for filtering
    virtual Status AddPredicate(const ColumnPredicate& predicate) = 0;

    // Set query options
    virtual Status SetOptions(const QueryOptions& options) = 0;

    // Add column to projection
    virtual Status AddProjection(const std::string& column) = 0;

    // Set limit
    virtual Status SetLimit(int64_t limit) = 0;

    // Set offset
    virtual Status SetOffset(int64_t offset) = 0;
};

// Query builder for fluent API
class QueryBuilder {
public:
    explicit QueryBuilder(MarbleDB* db);

    // Start building a query
    static std::unique_ptr<QueryBuilder> From(MarbleDB* db);

    // Add WHERE conditions
    QueryBuilder& Where(const std::string& column, PredicateType type,
                       std::shared_ptr<arrow::Scalar> value);
    QueryBuilder& Where(const std::string& column, PredicateType type,
                       std::shared_ptr<arrow::Scalar> value1,
                       std::shared_ptr<arrow::Scalar> value2);

    // Add column projections
    QueryBuilder& Select(const std::vector<std::string>& columns);
    QueryBuilder& SelectAll();

    // Set limits
    QueryBuilder& Limit(int64_t limit);
    QueryBuilder& Offset(int64_t offset);

    // Set options
    QueryBuilder& WithOptions(const QueryOptions& options);

    // Execute the query
    Status Execute(std::unique_ptr<QueryResult>* result);

    // Get the query object
    std::unique_ptr<Query> Build();

private:
    MarbleDB* db_;
    std::vector<ColumnPredicate> predicates_;
    QueryOptions options_;
};

// Arrow-compatible scanner interface
class Scanner {
public:
    virtual ~Scanner() = default;

    // Scan with predicates and return Arrow RecordBatch
    virtual Status Scan(const std::vector<ColumnPredicate>& predicates,
                       const QueryOptions& options,
                       std::unique_ptr<QueryResult>* result) = 0;

    // Get dataset schema
    virtual std::shared_ptr<arrow::Schema> schema() const = 0;

    // Get approximate row count
    virtual int64_t CountRows() const = 0;
};

// Arrow Dataset-like interface for MarbleDB
class MarbleDataset {
public:
    explicit MarbleDataset(MarbleDB* db);
    ~MarbleDataset() = default;

    // Create a scanner for this dataset
    std::unique_ptr<Scanner> NewScanner();

    // Get dataset schema
    std::shared_ptr<arrow::Schema> schema() const;

    // Get dataset info (like Arrow DatasetInfo)
    Status GetInfo(std::string* info) const;

private:
    MarbleDB* db_;
};

// Factory functions
std::unique_ptr<QueryBuilder> QueryFrom(MarbleDB* db);
std::unique_ptr<MarbleDataset> DatasetFrom(MarbleDB* db);

// Create streaming query result
std::unique_ptr<StreamingQueryResult> CreateStreamingQueryResult(
    std::shared_ptr<arrow::Schema> schema, MarbleDB* db,
    const std::vector<ColumnPredicate>& predicates = {},
    const QueryOptions& options = {});

// Convert predicates to key ranges for pushdown
KeyRange PredicatesToKeyRange(const std::vector<ColumnPredicate>& predicates);

// Create predicate evaluator
std::unique_ptr<PredicateEvaluator> CreatePredicateEvaluator(
    const std::vector<ColumnPredicate>& predicates);

// Create column projector
std::unique_ptr<ColumnProjector> CreateColumnProjector(
    const std::vector<std::string>& columns);

} // namespace marble
