#pragma once

#include <sabot_ql/operators/operator.h>
#include <sabot_ql/operators/aggregate.h>
#include <sabot_ql/storage/triple_store.h>
#include <sabot_ql/storage/vocabulary.h>
#include <memory>
#include <string>
#include <vector>

namespace sabot_ql {

// Query execution statistics
struct QueryStats {
    double total_time_ms = 0.0;
    size_t total_rows_processed = 0;
    size_t total_batches_processed = 0;
    size_t total_bytes_processed = 0;

    // Per-operator statistics
    std::vector<std::pair<std::string, OperatorStats>> operator_stats;

    std::string ToString() const;
};

// Query executor: Executes operator pipelines and collects statistics
// This is the entry point for executing queries programmatically
//
// Example usage:
//   QueryExecutor executor(store, vocab);
//
//   // Build operator pipeline
//   auto scan = std::make_shared<TripleScanOperator>(store, vocab, pattern);
//   auto filter = std::make_shared<FilterOperator>(scan, predicate, "?age > 30");
//   auto limit = std::make_shared<LimitOperator>(filter, 10);
//
//   // Execute and get results
//   auto result = executor.Execute(limit);
//   std::cout << result->ToString() << std::endl;
//
class QueryExecutor {
public:
    QueryExecutor(std::shared_ptr<TripleStore> store,
                  std::shared_ptr<Vocabulary> vocab)
        : store_(std::move(store)),
          vocab_(std::move(vocab)) {}

    // Execute operator pipeline and return results as Arrow Table
    arrow::Result<std::shared_ptr<arrow::Table>> Execute(
        std::shared_ptr<Operator> root_operator);

    // Execute and stream results batch-by-batch (more memory efficient)
    // Callback is called for each output batch
    arrow::Status ExecuteStreaming(
        std::shared_ptr<Operator> root_operator,
        std::function<arrow::Status(const std::shared_ptr<arrow::RecordBatch>&)> callback);

    // Get statistics from last execution
    const QueryStats& GetStats() const { return stats_; }

    // Generate EXPLAIN plan (operator tree visualization)
    std::string ExplainPlan(std::shared_ptr<Operator> root_operator) const;

    // Generate EXPLAIN ANALYZE (with execution statistics)
    arrow::Result<std::string> ExplainAnalyze(std::shared_ptr<Operator> root_operator);

    // Getters for store and vocab (for QueryBuilder)
    std::shared_ptr<TripleStore> GetStore() const { return store_; }
    std::shared_ptr<Vocabulary> GetVocab() const { return vocab_; }

private:
    // Collect statistics from operator tree
    void CollectStats(std::shared_ptr<Operator> op, const std::string& prefix = "");

    std::shared_ptr<TripleStore> store_;
    std::shared_ptr<Vocabulary> vocab_;

    QueryStats stats_;
};

// Query builder: Fluent API for constructing operator pipelines
// This provides a more user-friendly interface than manually constructing operators
//
// Example usage:
//   QueryBuilder builder(store, vocab);
//
//   auto result = builder
//       .Scan(pattern)
//       .Filter(predicate, "?age > 30")
//       .Project({"name", "age"})
//       .Limit(10)
//       .Execute();
//
class QueryBuilder {
public:
    QueryBuilder(std::shared_ptr<TripleStore> store,
                 std::shared_ptr<Vocabulary> vocab)
        : executor_(std::move(store), std::move(vocab)) {}

    // Scan triple store with pattern
    QueryBuilder& Scan(const TriplePattern& pattern);

    // Filter results with predicate
    QueryBuilder& Filter(
        std::function<arrow::Result<std::shared_ptr<arrow::BooleanArray>>(
            const std::shared_ptr<arrow::RecordBatch>&)> predicate,
        const std::string& description);

    // Project columns
    QueryBuilder& Project(const std::vector<std::string>& columns);

    // Limit results
    QueryBuilder& Limit(size_t limit);

    // Distinct (remove duplicates)
    QueryBuilder& Distinct();

    // Join with another pipeline
    QueryBuilder& Join(
        std::shared_ptr<Operator> right,
        const std::vector<std::string>& left_keys,
        const std::vector<std::string>& right_keys);

    // Group by and aggregate
    QueryBuilder& GroupBy(
        const std::vector<std::string>& group_keys,
        const std::vector<AggregateSpec>& aggregates);

    // Execute and return results
    arrow::Result<std::shared_ptr<arrow::Table>> Execute();

    // Execute with streaming callback
    arrow::Status ExecuteStreaming(
        std::function<arrow::Status(const std::shared_ptr<arrow::RecordBatch>&)> callback);

    // Get EXPLAIN plan
    std::string Explain() const;

    // Get EXPLAIN ANALYZE
    arrow::Result<std::string> ExplainAnalyze();

    // Get current operator (for advanced use)
    std::shared_ptr<Operator> GetOperator() const { return current_operator_; }

private:
    QueryExecutor executor_;
    std::shared_ptr<Operator> current_operator_;
};

} // namespace sabot_ql
