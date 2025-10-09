#pragma once

#include <memory>
#include <vector>
#include <string>
#include <functional>
#include <unordered_map>
#include <arrow/api.h>
#include <marble/status.h>
#include <marble/query.h>
#include <marble/table.h>
#include <marble/advanced_query.h>

namespace marble {

// Forward declarations
struct QuerySpec;

/**
 * @brief DataChunk - DuckDB-inspired vectorized data processing
 *
 * A DataChunk represents a columnar batch of data that can be processed
 * efficiently by vectorized operators. Similar to DuckDB's DataChunk.
 */
class DataChunk {
public:
    DataChunk() = default;
    explicit DataChunk(std::shared_ptr<arrow::Schema> schema, int64_t capacity = 2048);

    /**
     * @brief Initialize chunk with schema and capacity
     */
    Status Initialize(std::shared_ptr<arrow::Schema> schema, int64_t capacity = 2048);

    /**
     * @brief Add a column to the chunk
     */
    Status AddColumn(std::shared_ptr<arrow::Array> array);

    /**
     * @brief Get column by index
     */
    std::shared_ptr<arrow::Array> GetColumn(int index) const;

    /**
     * @brief Get column by name
     */
    std::shared_ptr<arrow::Array> GetColumn(const std::string& name) const;

    /**
     * @brief Set column data
     */
    Status SetColumn(int index, std::shared_ptr<arrow::Array> array);

    /**
     * @brief Get number of rows in this chunk
     */
    int64_t NumRows() const { return size_; }

    /**
     * @brief Get number of columns
     */
    int NumColumns() const { return columns_.size(); }

    /**
     * @brief Get schema
     */
    std::shared_ptr<arrow::Schema> Schema() const { return schema_; }

    /**
     * @brief Convert to Arrow RecordBatch
     */
    std::shared_ptr<arrow::RecordBatch> ToRecordBatch() const;

    /**
     * @brief Create from Arrow RecordBatch
     */
    static std::unique_ptr<DataChunk> FromRecordBatch(const arrow::RecordBatch& batch);

    /**
     * @brief Slice the chunk (create a view)
     */
    std::unique_ptr<DataChunk> Slice(int64_t offset, int64_t length) const;

    /**
     * @brief Check if chunk is valid
     */
    bool IsValid() const { return schema_ != nullptr && size_ >= 0; }

private:
    std::shared_ptr<arrow::Schema> schema_;
    std::vector<std::shared_ptr<arrow::Array>> columns_;
    int64_t size_ = 0;
    int64_t capacity_ = 2048;
};

/**
 * @brief Morsel - DuckDB-inspired unit of work
 *
 * A Morsel represents a small unit of work that can be processed independently.
 * Similar to DuckDB's morsels for parallel execution.
 */
struct Morsel {
    std::string table_name;
    std::vector<std::string> columns;  // Projection pushdown
    int64_t row_offset = 0;
    int64_t row_count = 0;
    std::unordered_map<std::string, std::string> filters;  // Predicate pushdown

    Morsel() = default;
    Morsel(std::string table, int64_t offset, int64_t count)
        : table_name(std::move(table)), row_offset(offset), row_count(count) {}
};

/**
 * @brief Physical Operator Types - DuckDB-inspired operators
 */
enum class PhysicalOperatorType {
    kTableScan,
    kIndexScan,
    kFilter,
    kProjection,
    kAggregate,
    kSort,
    kLimit,
    kJoin,
    kUnion,
    kDistinct,
    kWindow
};

/**
 * @brief Base Physical Operator - DuckDB-inspired operator interface
 */
class PhysicalOperator {
public:
    PhysicalOperator(PhysicalOperatorType type) : type_(type) {}
    virtual ~PhysicalOperator() = default;

    /**
     * @brief Get chunks from this operator
     */
    virtual Status GetChunk(std::unique_ptr<DataChunk>* chunk) = 0;

    /**
     * @brief Initialize operator state
     */
    virtual Status Initialize() { return Status::OK(); }

    /**
     * @brief Get operator type
     */
    PhysicalOperatorType GetType() const { return type_; }

    /**
     * @brief Get child operators
     */
    virtual std::vector<PhysicalOperator*> GetChildren() const { return {}; }

protected:
    PhysicalOperatorType type_;
};

/**
 * @brief Table Scan Operator - Reads data from storage
 */
class TableScanOperator : public PhysicalOperator {
public:
    TableScanOperator(std::string table_name, std::vector<std::string> columns,
                     std::vector<Morsel> morsels);

    Status GetChunk(std::unique_ptr<DataChunk>* chunk) override;
    Status Initialize() override;

private:
    std::string table_name_;
    std::vector<std::string> columns_;
    std::vector<Morsel> morsels_;
    size_t current_morsel_index_ = 0;
    std::shared_ptr<arrow::Schema> table_schema_;
};

/**
 * @brief Filter Operator - Applies WHERE conditions
 */
class FilterOperator : public PhysicalOperator {
public:
    FilterOperator(std::unique_ptr<PhysicalOperator> child,
                  std::function<bool(const DataChunk&)> filter_func);

    Status GetChunk(std::unique_ptr<DataChunk>* chunk) override;
    std::vector<PhysicalOperator*> GetChildren() const override;

private:
    std::unique_ptr<PhysicalOperator> child_;
    std::function<bool(const DataChunk&)> filter_func_;
};

/**
 * @brief Projection Operator - SELECT column operations
 */
class ProjectionOperator : public PhysicalOperator {
public:
    ProjectionOperator(std::unique_ptr<PhysicalOperator> child,
                      std::vector<std::string> select_columns,
                      std::vector<std::function<std::shared_ptr<arrow::Array>(const DataChunk&)>> expressions = {});

    Status GetChunk(std::unique_ptr<DataChunk>* chunk) override;
    std::vector<PhysicalOperator*> GetChildren() const override;

private:
    std::unique_ptr<PhysicalOperator> child_;
    std::vector<std::string> select_columns_;
    std::vector<std::function<std::shared_ptr<arrow::Array>(const DataChunk&)>> expressions_;
};

/**
 * @brief Aggregate Operator - GROUP BY operations
 */
class AggregateOperator : public PhysicalOperator {
public:
    struct AggregateFunction {
        std::string function_name;  // COUNT, SUM, AVG, etc.
        std::string column_name;
        std::string alias;

        AggregateFunction(std::string func, std::string col, std::string alias = "")
            : function_name(std::move(func)), column_name(std::move(col)), alias(std::move(alias)) {}
    };

    AggregateOperator(std::unique_ptr<PhysicalOperator> child,
                     std::vector<std::string> group_by_columns,
                     std::vector<AggregateFunction> aggregates);

    Status GetChunk(std::unique_ptr<DataChunk>* chunk) override;
    Status Initialize() override;
    std::vector<PhysicalOperator*> GetChildren() const override;

    // Helper methods
    Status GroupChunk(const DataChunk& chunk);

private:
    std::unique_ptr<PhysicalOperator> child_;
    std::vector<std::string> group_by_columns_;
    std::vector<AggregateFunction> aggregates_;

    // Aggregation state
    std::unordered_map<std::string, std::unique_ptr<DataChunk>> grouped_data_;
    bool finalized_ = false;
};

/**
 * @brief Limit Operator - LIMIT clause
 */
class LimitOperator : public PhysicalOperator {
public:
    LimitOperator(std::unique_ptr<PhysicalOperator> child, int64_t limit);

    Status GetChunk(std::unique_ptr<DataChunk>* chunk) override;
    std::vector<PhysicalOperator*> GetChildren() const override;

private:
    std::unique_ptr<PhysicalOperator> child_;
    int64_t limit_;
    int64_t returned_rows_ = 0;
};

/**
 * @brief Pipeline - DuckDB-inspired execution pipeline
 */
class Pipeline {
public:
    Pipeline() = default;

    /**
     * @brief Add operator to pipeline
     */
    void AddOperator(std::unique_ptr<PhysicalOperator> op);

    /**
     * @brief Execute pipeline and collect results
     */
    Status Execute(std::vector<std::unique_ptr<DataChunk>>* results);

    /**
     * @brief Initialize pipeline
     */
    Status Initialize();

    /**
     * @brief Reset pipeline for re-execution
     */
    void Reset();

private:
    std::vector<std::unique_ptr<PhysicalOperator>> operators_;
};

/**
 * @brief Query Executor - DuckDB-inspired query execution engine
 */
class QueryExecutor {
public:
    explicit QueryExecutor(Database* database) : database_(database) {}

    /**
     * @brief Execute query with physical plan
     */
    Status Execute(const std::unique_ptr<QueryPlan>& plan,
                  std::vector<std::unique_ptr<DataChunk>>* results);

    /**
     * @brief Create pipeline from query specification
     */
    Status CreatePipeline(const QuerySpec& query_spec,
                         std::unique_ptr<Pipeline>* pipeline);

    /**
     * @brief Optimize query for execution
     */
    Status OptimizeQuery(QuerySpec* query_spec);

private:
    Database* database_;

    // Pipeline creation helpers
    Status CreateScanPipeline(const QuerySpec& query_spec,
                             std::unique_ptr<Pipeline>* pipeline);
    Status AddFilterOperators(const QuerySpec& query_spec,
                             std::unique_ptr<Pipeline>* pipeline);
    Status AddProjectionOperator(const QuerySpec& query_spec,
                                std::unique_ptr<Pipeline>* pipeline);
    Status AddAggregateOperator(const QuerySpec& query_spec,
                               std::unique_ptr<Pipeline>* pipeline);
    Status AddLimitOperator(const QuerySpec& query_spec,
                           std::unique_ptr<Pipeline>* pipeline);
};

/**
 * @brief Execution Context - DuckDB-inspired execution state
 */
class ExecutionContext {
public:
    ExecutionContext();

    /**
     * @brief Register UDF for execution
     */
    Status RegisterUDF(const std::string& name,
                      std::function<arrow::Result<std::shared_ptr<arrow::Array>>(
                          const std::vector<std::shared_ptr<arrow::Array>>&)> udf);

    /**
     * @brief Get registered UDF
     */
    Status GetUDF(const std::string& name,
                 std::function<arrow::Result<std::shared_ptr<arrow::Array>>(
                     const std::vector<std::shared_ptr<arrow::Array>>&)> * udf) const;

    /**
     * @brief Set execution parameters
     */
    void SetParameter(const std::string& key, const std::string& value);
    std::string GetParameter(const std::string& key) const;

private:
    std::unordered_map<std::string,
        std::function<arrow::Result<std::shared_ptr<arrow::Array>>(
            const std::vector<std::shared_ptr<arrow::Array>>&)> > udfs_;
    std::unordered_map<std::string, std::string> parameters_;
};

/**
 * @brief Task Scheduler - DuckDB-inspired parallel execution
 */
class TaskScheduler {
public:
    /**
     * @brief Schedule morsel for parallel execution
     */
    virtual Status ScheduleMorsel(const Morsel& morsel,
                                 std::function<Status(const Morsel&)> task) = 0;

    /**
     * @brief Wait for all scheduled tasks to complete
     */
    virtual Status WaitForCompletion() = 0;

    /**
     * @brief Get number of worker threads
     */
    virtual size_t GetWorkerCount() const = 0;

    virtual ~TaskScheduler() = default;
};

/**
 * @brief Parallel Task Scheduler Implementation
 */
class ParallelTaskScheduler : public TaskScheduler {
public:
    explicit ParallelTaskScheduler(size_t num_threads = 0); // 0 = use hardware concurrency
    ~ParallelTaskScheduler() override;

    Status ScheduleMorsel(const Morsel& morsel,
                         std::function<Status(const Morsel&)> task) override;
    Status WaitForCompletion() override;
    size_t GetWorkerCount() const override { return num_threads_; }

private:
    size_t num_threads_;
    // TODO: Implement thread pool and task queue
};

// Factory functions
std::unique_ptr<QueryExecutor> CreateQueryExecutor(Database* database);
std::unique_ptr<TaskScheduler> CreateTaskScheduler(size_t num_threads = 0);
std::unique_ptr<DataChunk> CreateDataChunk(std::shared_ptr<arrow::Schema> schema, int64_t capacity = 2048);

/**
 * @brief Query specification for execution
 */
struct QuerySpec {
    std::string table_name;
    std::vector<std::string> columns;
    std::vector<ColumnPredicate> filters;  // For compatibility with existing code
    std::vector<ColumnPredicate> predicates;  // Alias for filters
    std::vector<std::string> group_by;  // For GROUP BY clauses
    std::vector<OrderBySpec> order_by;
    int64_t limit = -1;  // -1 means no limit
};

} // namespace marble
