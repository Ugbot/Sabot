#pragma once

#include <memory>
#include <string>
#include <vector>
#include <functional>
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include "marble/status.h"
#include "marble/api.h"

namespace marble {

// Forward declarations
class MarbleDB;
class QueryExecutor;
class Record;
struct ColumnPredicate;

/**
 * @brief Predicate for filtering Arrow data
 */
struct Predicate {
    enum class Op {
        EQ,    // ==
        NE,    // !=
        LT,    // <
        LE,    // <=
        GT,    // >
        GE,    // >=
        IN,    // IN (...)
        LIKE   // String pattern matching
    };

    std::string column;
    Op op;
    std::shared_ptr<arrow::Scalar> value;

    // Convert to Arrow compute expression
    arrow::Result<arrow::compute::Expression> ToArrowExpression() const;
};

/**
 * @brief Arrow-optimized query builder with predicate pushdown
 *
 * Example usage:
 *   auto reader = db->Query("users")
 *                    .Scan(start_key, end_key)
 *                    .Project({"name", "email", "age"})
 *                    .Filter("age", Predicate::Op::GT, arrow::MakeScalar(21))
 *                    .Limit(100)
 *                    .Reverse()
 *                    .Execute();
 *
 *   while (true) {
 *       ARROW_ASSIGN_OR_RAISE(auto batch, reader->Next());
 *       if (!batch) break;
 *       // Process batch
 *   }
 */
class QueryBuilder {
public:
    /**
     * @brief Set primary key range for scan
     */
    QueryBuilder& Scan(uint64_t start_key, uint64_t end_key);

    /**
     * @brief Select specific columns (projection pushdown)
     * Empty vector = all columns
     */
    QueryBuilder& Project(const std::vector<std::string>& columns);

    /**
     * @brief Add column filter predicate
     */
    QueryBuilder& Filter(const std::string& column,
                        Predicate::Op op,
                        const std::shared_ptr<arrow::Scalar>& value);

    /**
     * @brief Convenience filter methods
     */
    QueryBuilder& FilterEqual(const std::string& column,
                             const std::shared_ptr<arrow::Scalar>& value) {
        return Filter(column, Predicate::Op::EQ, value);
    }

    QueryBuilder& FilterGreater(const std::string& column,
                               const std::shared_ptr<arrow::Scalar>& value) {
        return Filter(column, Predicate::Op::GT, value);
    }

    QueryBuilder& FilterLess(const std::string& column,
                            const std::shared_ptr<arrow::Scalar>& value) {
        return Filter(column, Predicate::Op::LT, value);
    }

    /**
     * @brief Limit number of results
     */
    QueryBuilder& Limit(size_t n);

    /**
     * @brief Scan in reverse order (newest first)
     */
    QueryBuilder& Reverse();

    /**
     * @brief Execute query and return Arrow RecordBatchReader
     * Returns streaming reader with zero-copy where possible
     */
    arrow::Result<std::shared_ptr<arrow::RecordBatchReader>> Execute();

    /**
     * @brief Execute and materialize to Table (convenience method)
     */
    arrow::Result<std::shared_ptr<arrow::Table>> ExecuteToTable();

    // Public constructor for derived classes
    QueryBuilder(MarbleDB* db, const std::string& table_name);

    // Getters for QueryExecutor
    const std::string& table_name() const { return table_name_; }
    uint64_t start_key() const { return start_key_; }
    uint64_t end_key() const { return end_key_; }
    const std::vector<std::string>& columns() const { return columns_; }
    const std::vector<Predicate>& predicates() const { return predicates_; }
    size_t limit() const { return limit_; }
    bool reverse() const { return reverse_; }

private:
    friend class MarbleDB;

    MarbleDB* db_;
    std::string table_name_;
    uint64_t start_key_ = 0;
    uint64_t end_key_ = UINT64_MAX;
    std::vector<std::string> columns_;
    std::vector<Predicate> predicates_;
    size_t limit_ = SIZE_MAX;
    bool reverse_ = false;
};

/**
 * @brief Evaluates predicates for filtering records
 */
class PredicateEvaluator {
public:
    explicit PredicateEvaluator(const std::vector<ColumnPredicate>& predicates);
    bool Evaluate(const std::shared_ptr<Record>& record) const;
    std::vector<ColumnPredicate> GetKeyRangePredicates() const;
    std::vector<ColumnPredicate> GetRecordPredicates() const;

private:
    std::vector<ColumnPredicate> predicates_;
    std::vector<ColumnPredicate> key_range_predicates_;
    std::vector<ColumnPredicate> record_predicates_;
};

/**
 * @brief Projects columns from records to Arrow arrays
 */
class ColumnProjector {
public:
    explicit ColumnProjector(const std::vector<std::string>& columns);
    Status ProjectToArrow(const std::vector<std::shared_ptr<Record>>& records,
                         std::vector<std::shared_ptr<arrow::Array>>* arrays) const;
    std::shared_ptr<arrow::Schema> GetProjectedSchema() const;
    bool IsFullProjection() const;

private:
    std::vector<std::string> projected_columns_;
    bool full_projection_;
};

/**
 * @brief Streaming query result for large datasets
 */
class StreamingQueryResult {
public:
    StreamingQueryResult(std::shared_ptr<arrow::Schema> schema, MarbleDB* db,
                        const std::vector<ColumnPredicate>& predicates,
                        const QueryOptions& options);
    ~StreamingQueryResult();

    bool HasNext() const;
    Status Next(std::shared_ptr<arrow::RecordBatch>* batch);
    Status NextAsync(std::function<void(Status, std::shared_ptr<arrow::RecordBatch>)> callback);
    std::shared_ptr<arrow::Schema> schema() const;
    int64_t num_rows() const;
    int64_t num_batches() const;
    Status RecordsToArrowBatch(const std::vector<std::shared_ptr<Record>>& records,
                              std::shared_ptr<arrow::RecordBatch>* batch);

private:
    std::shared_ptr<arrow::Schema> schema_;
    MarbleDB* db_;
    std::vector<ColumnPredicate> predicates_;
    QueryOptions options_;
    std::unique_ptr<class Iterator> iterator_;
    std::unique_ptr<PredicateEvaluator> predicate_evaluator_;
    std::unique_ptr<ColumnProjector> column_projector_;
    int64_t current_batch_;
    int64_t total_rows_;
    bool finished_;
};

} // namespace marble
