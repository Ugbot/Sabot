#include <sabot_ql/execution/executor.h>
#include <sabot_ql/operators/join.h>
#include <sabot_ql/operators/aggregate.h>
#include <sstream>
#include <chrono>

namespace sabot_ql {

std::string QueryStats::ToString() const {
    std::ostringstream oss;
    oss << "Query Statistics:\n";
    oss << "  Total time: " << total_time_ms << " ms\n";
    oss << "  Total rows: " << total_rows_processed << "\n";
    oss << "  Total batches: " << total_batches_processed << "\n";
    oss << "  Total bytes: " << total_bytes_processed << "\n";
    oss << "\n";
    oss << "Per-operator statistics:\n";

    for (const auto& [op_name, op_stats] : operator_stats) {
        oss << "  " << op_name << ": " << op_stats.ToString() << "\n";
    }

    return oss.str();
}

// QueryExecutor implementation
arrow::Result<std::shared_ptr<arrow::Table>> QueryExecutor::Execute(
    std::shared_ptr<Operator> root_operator) {

    stats_ = QueryStats{};  // Reset statistics

    auto start = std::chrono::high_resolution_clock::now();

    // Execute operator pipeline
    ARROW_ASSIGN_OR_RAISE(auto result_table, root_operator->GetAllResults());

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    stats_.total_time_ms = duration.count() / 1000.0;

    // Collect statistics from operator tree
    CollectStats(root_operator);

    return result_table;
}

arrow::Status QueryExecutor::ExecuteStreaming(
    std::shared_ptr<Operator> root_operator,
    std::function<arrow::Status(const std::shared_ptr<arrow::RecordBatch>&)> callback) {

    stats_ = QueryStats{};  // Reset statistics

    auto start = std::chrono::high_resolution_clock::now();

    // Stream batches through callback
    while (root_operator->HasNextBatch()) {
        ARROW_ASSIGN_OR_RAISE(auto batch, root_operator->GetNextBatch());
        if (!batch) {
            break;
        }

        ARROW_RETURN_NOT_OK(callback(batch));

        stats_.total_rows_processed += batch->num_rows();
        stats_.total_batches_processed++;
        stats_.total_bytes_processed += batch->num_rows() * batch->num_columns() * sizeof(int64_t);
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    stats_.total_time_ms = duration.count() / 1000.0;

    // Collect statistics from operator tree
    CollectStats(root_operator);

    return arrow::Status::OK();
}

void QueryExecutor::CollectStats(std::shared_ptr<Operator> op, const std::string& prefix) {
    if (!op) {
        return;
    }

    // Add this operator's statistics
    std::string op_desc = prefix + op->ToString();
    stats_.operator_stats.emplace_back(op_desc, op->GetStats());

    // Recursively collect from child operators
    // Check if this is a unary or binary operator
    auto unary = std::dynamic_pointer_cast<UnaryOperator>(op);
    if (unary) {
        // Has one child - we can't access it directly due to protected access
        // For now, just record this operator's stats
        return;
    }

    auto binary = std::dynamic_pointer_cast<BinaryOperator>(op);
    if (binary) {
        // Has two children - same issue
        return;
    }
}

std::string QueryExecutor::ExplainPlan(std::shared_ptr<Operator> root_operator) const {
    if (!root_operator) {
        return "Empty plan";
    }

    std::ostringstream oss;
    oss << "Query Plan:\n";
    oss << root_operator->ToString();
    oss << "\n\n";
    oss << "Estimated cardinality: " << root_operator->EstimateCardinality() << " rows";

    return oss.str();
}

arrow::Result<std::string> QueryExecutor::ExplainAnalyze(
    std::shared_ptr<Operator> root_operator) {

    // Execute the query to collect statistics
    ARROW_ASSIGN_OR_RAISE(auto result, Execute(root_operator));

    std::ostringstream oss;
    oss << "Query Plan with Execution Statistics:\n";
    oss << root_operator->ToString();
    oss << "\n\n";
    oss << stats_.ToString();
    oss << "\n";
    oss << "Result: " << result->num_rows() << " rows, "
        << result->num_columns() << " columns";

    return oss.str();
}

// QueryBuilder implementation
QueryBuilder& QueryBuilder::Scan(const TriplePattern& pattern) {
    current_operator_ = std::make_shared<TripleScanOperator>(
        executor_.GetStore(),
        executor_.GetVocab(),
        pattern
    );
    return *this;
}

QueryBuilder& QueryBuilder::Filter(
    std::function<arrow::Result<std::shared_ptr<arrow::BooleanArray>>(
        const std::shared_ptr<arrow::RecordBatch>&)> predicate,
    const std::string& description) {

    if (!current_operator_) {
        throw std::runtime_error("Cannot add Filter: no source operator");
    }

    current_operator_ = std::make_shared<FilterOperator>(
        current_operator_,
        predicate,
        description
    );

    return *this;
}

QueryBuilder& QueryBuilder::Project(const std::vector<std::string>& columns) {
    if (!current_operator_) {
        throw std::runtime_error("Cannot add Project: no source operator");
    }

    current_operator_ = std::make_shared<ProjectOperator>(
        current_operator_,
        columns
    );

    return *this;
}

QueryBuilder& QueryBuilder::Limit(size_t limit) {
    if (!current_operator_) {
        throw std::runtime_error("Cannot add Limit: no source operator");
    }

    current_operator_ = std::make_shared<LimitOperator>(
        current_operator_,
        limit
    );

    return *this;
}

QueryBuilder& QueryBuilder::Distinct() {
    if (!current_operator_) {
        throw std::runtime_error("Cannot add Distinct: no source operator");
    }

    current_operator_ = std::make_shared<DistinctOperator>(
        current_operator_
    );

    return *this;
}

QueryBuilder& QueryBuilder::Join(
    std::shared_ptr<Operator> right,
    const std::vector<std::string>& left_keys,
    const std::vector<std::string>& right_keys) {

    if (!current_operator_) {
        throw std::runtime_error("Cannot add Join: no source operator");
    }

    current_operator_ = CreateJoin(
        current_operator_,
        right,
        left_keys,
        right_keys,
        JoinType::Inner,
        JoinAlgorithm::Hash
    );

    return *this;
}

QueryBuilder& QueryBuilder::GroupBy(
    const std::vector<std::string>& group_keys,
    const std::vector<AggregateSpec>& aggregates) {

    if (!current_operator_) {
        throw std::runtime_error("Cannot add GroupBy: no source operator");
    }

    current_operator_ = std::make_shared<GroupByOperator>(
        current_operator_,
        group_keys,
        aggregates
    );

    return *this;
}

arrow::Result<std::shared_ptr<arrow::Table>> QueryBuilder::Execute() {
    if (!current_operator_) {
        return arrow::Status::Invalid("No operator pipeline to execute");
    }

    return executor_.Execute(current_operator_);
}

arrow::Status QueryBuilder::ExecuteStreaming(
    std::function<arrow::Status(const std::shared_ptr<arrow::RecordBatch>&)> callback) {

    if (!current_operator_) {
        return arrow::Status::Invalid("No operator pipeline to execute");
    }

    return executor_.ExecuteStreaming(current_operator_, callback);
}

std::string QueryBuilder::Explain() const {
    if (!current_operator_) {
        return "No operator pipeline to explain";
    }

    return executor_.ExplainPlan(current_operator_);
}

arrow::Result<std::string> QueryBuilder::ExplainAnalyze() {
    if (!current_operator_) {
        return arrow::Status::Invalid("No operator pipeline to explain");
    }

    return executor_.ExplainAnalyze(current_operator_);
}

} // namespace sabot_ql
