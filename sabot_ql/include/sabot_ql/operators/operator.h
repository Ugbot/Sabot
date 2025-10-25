#pragma once

#include <memory>
#include <string>
#include <vector>
#include <optional>
#include <unordered_set>
#include <arrow/api.h>
#include <arrow/result.h>
#include <sabot_ql/operators/operator_metadata.h>

namespace sabot_ql {

// Forward declarations
class TripleStore;
class Vocabulary;

// TriplePattern definition (from storage/triple_store.h)
// Moved here to avoid circular dependency
struct TriplePattern {
    std::optional<uint64_t> subject;
    std::optional<uint64_t> predicate;
    std::optional<uint64_t> object;

    // Count bound variables
    size_t BoundCount() const {
        return (subject.has_value() ? 1 : 0) +
               (predicate.has_value() ? 1 : 0) +
               (object.has_value() ? 1 : 0);
    }

    // Get bound positions (for index selection)
    std::string BoundPositions() const {
        std::string result;
        if (subject.has_value()) result += 'S';
        if (predicate.has_value()) result += 'P';
        if (object.has_value()) result += 'O';
        return result;
    }
};

// Operator statistics for query profiling
struct OperatorStats {
    size_t rows_processed = 0;
    size_t batches_processed = 0;
    size_t bytes_processed = 0;
    double execution_time_ms = 0.0;

    std::string ToString() const;
};

// Base class for all query operators
// Inspired by Apache Arrow's compute API and QLever's Operation class
// All operators are lazy: they produce results on-demand via GetNextBatch()
class Operator {
public:
    virtual ~Operator() = default;

    // Get the output schema of this operator
    // This is known before execution begins
    virtual arrow::Result<std::shared_ptr<arrow::Schema>> GetOutputSchema() const = 0;

    // Execute the operator and return the next batch of results
    // Returns nullptr when no more data is available
    // Operators are lazy: they only compute when GetNextBatch() is called
    virtual arrow::Result<std::shared_ptr<arrow::RecordBatch>> GetNextBatch() = 0;

    // Check if the operator has more data
    // This is cheaper than GetNextBatch() when you just want to check
    virtual bool HasNextBatch() const = 0;

    // Get all results at once (convenience method)
    // Collects all batches into a single Arrow Table
    arrow::Result<std::shared_ptr<arrow::Table>> GetAllResults();

    // Get operator statistics
    virtual const OperatorStats& GetStats() const { return stats_; }

    // Get a human-readable description of this operator
    // Used for EXPLAIN plans and debugging
    virtual std::string ToString() const = 0;

    // Get the estimated cardinality (number of output rows)
    // Used by query optimizer for cost estimation
    virtual size_t EstimateCardinality() const = 0;

    // Get output ordering (if known)
    // Returns empty ordering if operator provides no ordering guarantees.
    // Used for query optimization - if data is already sorted,
    // we can skip redundant sort operations.
    virtual OrderingProperty GetOutputOrdering() const {
        // Default: no ordering guarantees
        return OrderingProperty{};
    }

protected:
    OperatorStats stats_;
};

// Unary operator: takes one input operator
class UnaryOperator : public Operator {
public:
    explicit UnaryOperator(std::shared_ptr<Operator> input)
        : input_(std::move(input)) {}

    bool HasNextBatch() const override {
        return input_->HasNextBatch();
    }

protected:
    std::shared_ptr<Operator> input_;
};

// Binary operator: takes two input operators
class BinaryOperator : public Operator {
public:
    BinaryOperator(std::shared_ptr<Operator> left,
                   std::shared_ptr<Operator> right)
        : left_(std::move(left)), right_(std::move(right)) {}

protected:
    std::shared_ptr<Operator> left_;
    std::shared_ptr<Operator> right_;
};

// Scan operator: reads from triple store
// This is a leaf operator (no inputs)
class TripleScanOperator : public Operator {
public:
    TripleScanOperator(std::shared_ptr<TripleStore> store,
                       std::shared_ptr<Vocabulary> vocab,
                       const TriplePattern& pattern);

    arrow::Result<std::shared_ptr<arrow::Schema>> GetOutputSchema() const override;
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> GetNextBatch() override;
    bool HasNextBatch() const override;
    std::string ToString() const override;
    size_t EstimateCardinality() const override;

private:
    std::shared_ptr<TripleStore> store_;
    std::shared_ptr<Vocabulary> vocab_;
    TriplePattern pattern_;
    std::shared_ptr<arrow::Table> results_;
    size_t current_batch_ = 0;
    size_t batch_size_ = 10000;  // 10K rows per batch
    bool exhausted_ = false;
};

// Filter operator: applies predicates to input batches
// Uses Arrow compute kernels for vectorized execution
class FilterOperator : public UnaryOperator {
public:
    // Predicate function: takes a RecordBatch, returns boolean array
    using PredicateFn = std::function<arrow::Result<std::shared_ptr<arrow::BooleanArray>>(
        const std::shared_ptr<arrow::RecordBatch>&)>;

    FilterOperator(std::shared_ptr<Operator> input,
                   PredicateFn predicate,
                   const std::string& predicate_description);

    arrow::Result<std::shared_ptr<arrow::Schema>> GetOutputSchema() const override;
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> GetNextBatch() override;
    std::string ToString() const override;
    size_t EstimateCardinality() const override;

private:
    PredicateFn predicate_;
    std::string predicate_description_;
    double selectivity_ = 0.1;  // Assume 10% selectivity by default
};

// Project operator: selects and renames columns
class ProjectOperator : public UnaryOperator {
public:
    ProjectOperator(std::shared_ptr<Operator> input,
                    const std::vector<std::string>& column_names);

    ProjectOperator(std::shared_ptr<Operator> input,
                    const std::vector<int>& column_indices);

    arrow::Result<std::shared_ptr<arrow::Schema>> GetOutputSchema() const override;
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> GetNextBatch() override;
    std::string ToString() const override;
    size_t EstimateCardinality() const override;

private:
    std::vector<int> column_indices_;
    std::vector<std::string> column_names_;
};

// Limit operator: limits output to N rows
class LimitOperator : public UnaryOperator {
public:
    LimitOperator(std::shared_ptr<Operator> input, size_t limit);

    arrow::Result<std::shared_ptr<arrow::Schema>> GetOutputSchema() const override;
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> GetNextBatch() override;
    bool HasNextBatch() const override;
    std::string ToString() const override;
    size_t EstimateCardinality() const override;

private:
    size_t limit_;
    size_t rows_returned_ = 0;
};

// Distinct operator: removes duplicate rows
// Uses hash-based deduplication
class DistinctOperator : public UnaryOperator {
public:
    explicit DistinctOperator(std::shared_ptr<Operator> input);

    arrow::Result<std::shared_ptr<arrow::Schema>> GetOutputSchema() const override;
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> GetNextBatch() override;
    std::string ToString() const override;
    size_t EstimateCardinality() const override;

private:
    // Use hash set to track seen rows
    // TODO: For large datasets, use spill-to-disk
    std::unordered_set<std::string> seen_rows_;
};

} // namespace sabot_ql
