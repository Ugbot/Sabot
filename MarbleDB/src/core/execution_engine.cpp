#include "marble/execution_engine.h"
#include <algorithm>
#include <numeric>
#include <unordered_set>
#include <thread>
#include <arrow/compute/api.h>
#include <arrow/table.h>
#include <marble/table.h>

namespace marble {

//==============================================================================
// DataChunk Implementation
//==============================================================================

DataChunk::DataChunk(std::shared_ptr<arrow::Schema> schema, int64_t capacity)
    : schema_(schema), capacity_(capacity), size_(0) {
    if (schema_) {
        columns_.resize(schema_->num_fields());
    }
}

Status DataChunk::Initialize(std::shared_ptr<arrow::Schema> schema, int64_t capacity) {
    schema_ = schema;
    capacity_ = capacity;
    size_ = 0;
    columns_.clear();
    if (schema_) {
        columns_.resize(schema_->num_fields());
    }
    return Status::OK();
}

Status DataChunk::AddColumn(std::shared_ptr<arrow::Array> array) {
    if (!schema_ || columns_.size() >= schema_->num_fields()) {
        return Status::InvalidArgument("Cannot add column: schema not set or full");
    }

    if (array->length() != size_ && size_ > 0) {
        return Status::InvalidArgument("Array length mismatch");
    }

    columns_.push_back(array);
    if (size_ == 0) {
        size_ = array->length();
    }

    return Status::OK();
}

std::shared_ptr<arrow::Array> DataChunk::GetColumn(int index) const {
    if (index < 0 || index >= columns_.size()) {
        return nullptr;
    }
    return columns_[index];
}

std::shared_ptr<arrow::Array> DataChunk::GetColumn(const std::string& name) const {
    if (!schema_) return nullptr;

    auto field_index = schema_->GetFieldIndex(name);
    if (field_index == -1) return nullptr;

    return GetColumn(field_index);
}

Status DataChunk::SetColumn(int index, std::shared_ptr<arrow::Array> array) {
    if (index < 0 || index >= columns_.size()) {
        return Status::InvalidArgument("Column index out of range");
    }

    if (array->length() != size_ && size_ > 0) {
        return Status::InvalidArgument("Array length mismatch");
    }

    columns_[index] = array;
    if (size_ == 0) {
        size_ = array->length();
    }

    return Status::OK();
}

std::shared_ptr<arrow::RecordBatch> DataChunk::ToRecordBatch() const {
    if (!schema_ || columns_.size() != schema_->num_fields()) {
        return nullptr;
    }

    return arrow::RecordBatch::Make(schema_, size_, columns_);
}

std::unique_ptr<DataChunk> DataChunk::FromRecordBatch(const arrow::RecordBatch& batch) {
    auto chunk = std::make_unique<DataChunk>(batch.schema(), batch.num_rows());
    for (int i = 0; i < batch.num_columns(); ++i) {
        chunk->columns_[i] = batch.column(i);
    }
    chunk->size_ = batch.num_rows();
    return chunk;
}

std::unique_ptr<DataChunk> DataChunk::Slice(int64_t offset, int64_t length) const {
    if (offset < 0 || offset >= size_ || length <= 0) {
        return nullptr;
    }

    length = std::min(length, size_ - offset);

    auto sliced_chunk = std::make_unique<DataChunk>(schema_, capacity_);

    for (auto& column : columns_) {
        if (column) {
            // Create slice of the array
            auto sliced = column->Slice(offset, length);
            sliced_chunk->columns_.push_back(sliced);
        } else {
            sliced_chunk->columns_.push_back(nullptr);
        }
    }

    sliced_chunk->size_ = length;
    return sliced_chunk;
}

//==============================================================================
// Physical Operator Implementations
//==============================================================================

TableScanOperator::TableScanOperator(std::string table_name,
                                   std::vector<std::string> columns,
                                   std::vector<Morsel> morsels)
    : PhysicalOperator(PhysicalOperatorType::kTableScan),
      table_name_(std::move(table_name)),
      columns_(std::move(columns)),
      morsels_(std::move(morsels)) {}

Status TableScanOperator::Initialize() {
    // Create sample data for demonstration instead of requiring a database
    // In a real implementation, this would connect to the actual storage

    // Create schema based on table name
    std::vector<std::shared_ptr<arrow::Field>> fields;
    if (table_name_ == "stock_prices") {
        fields = {
            arrow::field("id", arrow::int64()),
            arrow::field("price", arrow::float64()),
            arrow::field("volume", arrow::int64()),
            arrow::field("timestamp", arrow::timestamp(arrow::TimeUnit::MICRO))
        };
    } else {
        fields = {
            arrow::field("id", arrow::int64()),
            arrow::field("value", arrow::float64()),
            arrow::field("category", arrow::utf8())
        };
    }

    table_schema_ = std::make_shared<arrow::Schema>(fields);
    return Status::OK();
}

Status TableScanOperator::GetChunk(std::unique_ptr<DataChunk>* chunk) {
    if (current_morsel_index_ >= morsels_.size()) {
        *chunk = nullptr; // No more data
        return Status::OK();
    }

    const auto& morsel = morsels_[current_morsel_index_++];
    current_morsel_index_ %= morsels_.size(); // Cycle through morsels

    // Generate sample data based on morsel
    int64_t num_rows = std::min(morsel.row_count, int64_t(100)); // Limit for demo

    auto data_chunk = CreateDataChunk(table_schema_, num_rows);

    // Generate sample data
    arrow::Int64Builder id_builder;
    arrow::DoubleBuilder value_builder;
    arrow::Int64Builder volume_builder;
    arrow::TimestampBuilder timestamp_builder(arrow::timestamp(arrow::TimeUnit::MICRO), arrow::default_memory_pool());
    arrow::StringBuilder category_builder;

    for (int64_t i = 0; i < num_rows; ++i) {
        ARROW_RETURN_NOT_OK(id_builder.Append(morsel.row_offset + i));
        ARROW_RETURN_NOT_OK(value_builder.Append(100.0 + (rand() % 100))); // Random values

        if (table_name_ == "stock_prices") {
            ARROW_RETURN_NOT_OK(volume_builder.Append(1000 + (rand() % 9000)));
            auto now = std::chrono::system_clock::now();
            auto timestamp = now - std::chrono::minutes(i * 5);
            int64_t ts_micros = std::chrono::duration_cast<std::chrono::microseconds>(
                timestamp.time_since_epoch()).count();
            ARROW_RETURN_NOT_OK(timestamp_builder.Append(ts_micros));
        } else {
            ARROW_RETURN_NOT_OK(category_builder.Append(std::string(1, 'A' + (i % 3))));
        }
    }

    std::shared_ptr<arrow::Array> id_array, value_array, volume_array, timestamp_array, category_array;
    ARROW_ASSIGN_OR_RAISE(id_array, id_builder.Finish());
    ARROW_ASSIGN_OR_RAISE(value_array, value_builder.Finish());

    Status status = data_chunk->AddColumn(id_array);
    if (!status.ok()) return status;

    status = data_chunk->AddColumn(value_array);
    if (!status.ok()) return status;

    if (table_name_ == "stock_prices") {
        ARROW_ASSIGN_OR_RAISE(volume_array, volume_builder.Finish());
        ARROW_ASSIGN_OR_RAISE(timestamp_array, timestamp_builder.Finish());

        status = data_chunk->AddColumn(volume_array);
        if (!status.ok()) return status;

        status = data_chunk->AddColumn(timestamp_array);
        if (!status.ok()) return status;
    } else {
        ARROW_ASSIGN_OR_RAISE(category_array, category_builder.Finish());
        status = data_chunk->AddColumn(category_array);
        if (!status.ok()) return status;
    }

    *chunk = std::move(data_chunk);
    return Status::OK();
}

FilterOperator::FilterOperator(std::unique_ptr<PhysicalOperator> child,
                             std::function<bool(const DataChunk&)> filter_func)
    : PhysicalOperator(PhysicalOperatorType::kFilter),
      child_(std::move(child)),
      filter_func_(std::move(filter_func)) {}

Status FilterOperator::GetChunk(std::unique_ptr<DataChunk>* chunk) {
    while (true) {
        auto status = child_->GetChunk(chunk);
        if (!status.ok()) return status;

        if (!*chunk) {
            return Status::OK(); // No more data
        }

        // Apply filter
        if (filter_func_(**chunk)) {
            return Status::OK(); // Chunk passes filter
        }

        // Chunk filtered out, try next one
    }
}

std::vector<PhysicalOperator*> FilterOperator::GetChildren() const {
    return {child_.get()};
}

ProjectionOperator::ProjectionOperator(std::unique_ptr<PhysicalOperator> child,
                                     std::vector<std::string> select_columns,
                                     std::vector<std::function<std::shared_ptr<arrow::Array>(const DataChunk&)>> expressions)
    : PhysicalOperator(PhysicalOperatorType::kProjection),
      child_(std::move(child)),
      select_columns_(std::move(select_columns)),
      expressions_(std::move(expressions)) {}

Status ProjectionOperator::GetChunk(std::unique_ptr<DataChunk>* chunk) {
    auto status = child_->GetChunk(chunk);
    if (!status.ok()) return status;

    if (!*chunk) return Status::OK();

    DataChunk* input_chunk = chunk->get();

    // Create new schema with selected columns
    std::vector<std::shared_ptr<arrow::Field>> selected_fields;
    std::vector<std::shared_ptr<arrow::Array>> selected_columns;

    for (const auto& col_name : select_columns_) {
        auto array = input_chunk->GetColumn(col_name);
        if (!array) {
            return Status::InvalidArgument("Column not found: " + col_name);
        }

        // Find field in schema
        auto field_index = input_chunk->Schema()->GetFieldIndex(col_name);
        if (field_index != -1) {
            selected_fields.push_back(input_chunk->Schema()->field(field_index));
            selected_columns.push_back(array);
        }
    }

    // Add computed columns from expressions
    for (size_t i = 0; i < expressions_.size(); ++i) {
        auto computed_array = expressions_[i](*input_chunk);
        if (computed_array) {
            // Create field for computed column
            selected_fields.push_back(arrow::field("expr_" + std::to_string(i),
                                                  computed_array->type()));
            selected_columns.push_back(computed_array);
        }
    }

    // Create new chunk
    auto new_schema = std::make_shared<arrow::Schema>(selected_fields);
    auto new_chunk = std::make_unique<DataChunk>(new_schema, input_chunk->NumRows());

    for (auto& array : selected_columns) {
        new_chunk->AddColumn(array);
    }

    *chunk = std::move(new_chunk);
    return Status::OK();
}

std::vector<PhysicalOperator*> ProjectionOperator::GetChildren() const {
    return {child_.get()};
}

AggregateOperator::AggregateOperator(std::unique_ptr<PhysicalOperator> child,
                                   std::vector<std::string> group_by_columns,
                                   std::vector<AggregateFunction> aggregates)
    : PhysicalOperator(PhysicalOperatorType::kAggregate),
      child_(std::move(child)),
      group_by_columns_(std::move(group_by_columns)),
      aggregates_(std::move(aggregates)) {}

Status AggregateOperator::Initialize() {
    // Reset aggregation state
    grouped_data_.clear();
    finalized_ = false;
    return Status::OK();
}

Status AggregateOperator::GetChunk(std::unique_ptr<DataChunk>* chunk) {
    if (!finalized_) {
        // First, collect all data and group it
        std::unique_ptr<DataChunk> input_chunk;
        while (true) {
            auto status = child_->GetChunk(&input_chunk);
            if (!status.ok()) return status;

            if (!input_chunk) break;

            // Group the chunk
            status = this->GroupChunk(*input_chunk);
            if (!status.ok()) return status;
        }

        finalized_ = true;
    }

    // Return aggregated results
    if (grouped_data_.empty()) {
        *chunk = nullptr;
        return Status::OK();
    }

    // For now, return first group (TODO: Implement proper aggregation)
    auto it = grouped_data_.begin();
    *chunk = std::move(it->second);
    grouped_data_.erase(it);

    return Status::OK();
}

Status AggregateOperator::GroupChunk(const DataChunk& chunk) {
    // Simple grouping implementation
    // TODO: Implement proper hash-based grouping

    if (group_by_columns_.empty()) {
        // No grouping, aggregate all rows
        std::string key = "__global__";

        if (grouped_data_.find(key) == grouped_data_.end()) {
            // Create new chunk for this group
            auto new_chunk = std::make_unique<DataChunk>(chunk.Schema(), 2048); // Default capacity
            // Copy all columns (simplified)
            for (int i = 0; i < chunk.NumColumns(); ++i) {
                new_chunk->AddColumn(chunk.GetColumn(i));
            }
            grouped_data_[key] = std::move(new_chunk);
        } else {
            // Merge with existing group (TODO: proper aggregation)
        }
    }

    return Status::OK();
}

std::vector<PhysicalOperator*> AggregateOperator::GetChildren() const {
    return {child_.get()};
}

LimitOperator::LimitOperator(std::unique_ptr<PhysicalOperator> child, int64_t limit)
    : PhysicalOperator(PhysicalOperatorType::kLimit),
      child_(std::move(child)),
      limit_(limit) {}

Status LimitOperator::GetChunk(std::unique_ptr<DataChunk>* chunk) {
    if (returned_rows_ >= limit_) {
        *chunk = nullptr;
        return Status::OK();
    }

    auto status = child_->GetChunk(chunk);
    if (!status.ok()) return status;

    if (!*chunk) return Status::OK();

    // Check if we need to slice the chunk
    int64_t remaining = limit_ - returned_rows_;
    if ((*chunk)->NumRows() > remaining) {
        // Slice chunk to fit limit
        auto sliced = (*chunk)->Slice(0, remaining);
        *chunk = std::move(sliced);
    }

    returned_rows_ += (*chunk)->NumRows();
    return Status::OK();
}

std::vector<PhysicalOperator*> LimitOperator::GetChildren() const {
    return {child_.get()};
}

//==============================================================================
// Pipeline Implementation
//==============================================================================

void Pipeline::AddOperator(std::unique_ptr<PhysicalOperator> op) {
    operators_.push_back(std::move(op));
}

Status Pipeline::Initialize() {
    for (auto& op : operators_) {
        auto status = op->Initialize();
        if (!status.ok()) return status;
    }
    return Status::OK();
}

Status Pipeline::Execute(std::vector<std::unique_ptr<DataChunk>>* results) {
    results->clear();

    if (operators_.empty()) {
        return Status::InvalidArgument("Pipeline has no operators");
    }

    // Chain the operators properly - each operator (except the first) gets the previous as child
    for (size_t i = 1; i < operators_.size(); ++i) {
        // This is a simplified chaining - in a real implementation, we'd need to
        // dynamically cast and set the child pointer for each operator type
        // For now, we'll execute operators sequentially
    }

    // Initialize pipeline
    auto status = Initialize();
    if (!status.ok()) return status;

    // Execute operators sequentially (simplified - real implementation would chain them)
    std::unique_ptr<DataChunk> chunk;
    std::unique_ptr<DataChunk> intermediate_result;

    // Start with the first operator (usually TableScan)
    if (operators_[0]->GetType() == PhysicalOperatorType::kTableScan) {
        while (true) {
            status = operators_[0]->GetChunk(&chunk);
            if (!status.ok()) return status;

            if (!chunk) break;

            intermediate_result = std::move(chunk);

            // Apply subsequent operators sequentially
            for (size_t i = 1; i < operators_.size(); ++i) {
                // For now, just pass through - real implementation would chain operators
                // This is a simplification for the MVP
            }

            results->push_back(std::move(intermediate_result));
        }
    }

    return Status::OK();
}

void Pipeline::Reset() {
    // Reset all operators
    for (auto& op : operators_) {
        // TODO: Implement reset for each operator type
    }
}

//==============================================================================
// Query Executor Implementation
//==============================================================================

Status QueryExecutor::Execute(const std::unique_ptr<QueryPlan>& plan,
                             std::vector<std::unique_ptr<DataChunk>>* results) {
    // TODO: Convert QueryPlan to Pipeline
    // For now, create a simple pipeline

    std::unique_ptr<Pipeline> pipeline;
    // TODO: Create pipeline from plan

    return pipeline->Execute(results);
}

Status QueryExecutor::CreatePipeline(const QuerySpec& query_spec,
                                    std::unique_ptr<Pipeline>* pipeline) {
    *pipeline = std::make_unique<Pipeline>();

    // Create table scan
    auto status = CreateScanPipeline(query_spec, pipeline);
    if (!status.ok()) return status;

    // Add filters
    status = AddFilterOperators(query_spec, pipeline);
    if (!status.ok()) return status;

    // Add projection
    status = AddProjectionOperator(query_spec, pipeline);
    if (!status.ok()) return status;

    // Add aggregation
    status = AddAggregateOperator(query_spec, pipeline);
    if (!status.ok()) return status;

    // Add limit
    status = AddLimitOperator(query_spec, pipeline);
    if (!status.ok()) return status;

    return Status::OK();
}

Status QueryExecutor::OptimizeQuery(QuerySpec* query_spec) {
    // DuckDB-inspired optimizations:

    // 1. Projection pushdown - push column selection down to storage
    if (!query_spec->columns.empty()) {
        // TODO: Push column selection to storage layer
    }

    // 2. Predicate pushdown - push filters down to storage
    if (!query_spec->filters.empty()) {
        // TODO: Push filters that can be evaluated at storage level
    }

    // 3. Limit pushdown - push LIMIT down through operators
    if (query_spec->limit > 0) {
        // TODO: Push limit down to reduce data processed
    }

    // 4. Join reordering - reorder joins for better performance
    // TODO: Implement join reordering

    return Status::OK();
}

Status QueryExecutor::CreateScanPipeline(const QuerySpec& query_spec,
                                        std::unique_ptr<Pipeline>* pipeline) {
    // Create morsels for parallel execution
    std::vector<Morsel> morsels;

    // TODO: Create multiple morsels for parallel execution
    // For now, create single morsel
    Morsel morsel(query_spec.table_name, 0, query_spec.limit);
    morsel.columns = query_spec.columns;
    morsels.push_back(morsel);

    auto scan_op = std::make_unique<TableScanOperator>(
        query_spec.table_name, query_spec.columns, morsels);

    (*pipeline)->AddOperator(std::move(scan_op));
    return Status::OK();
}

Status QueryExecutor::AddFilterOperators(const QuerySpec& query_spec,
                                        std::unique_ptr<Pipeline>* pipeline) {
    if (query_spec.filters.empty()) return Status::OK();

    // Create filter function from query filters
    auto filter_func = [filters = query_spec.filters](const DataChunk& chunk) -> bool {
        // Implement filter evaluation
        for (const auto& filter : filters) {
            auto column = chunk.GetColumn(filter.column_name);
            if (!column) {
                return false; // Column not found, filter fails
            }

            // TODO: Implement proper filter evaluation based on filter.op
            // For now, just check if the column exists and has data
            if (column->length() == 0) {
                return false;
            }
        }
        return true; // All filters pass
    };

    // Create filter operator - will be chained properly in pipeline
    auto filter_op = std::make_unique<FilterOperator>(nullptr, filter_func);
    (*pipeline)->AddOperator(std::move(filter_op));
    return Status::OK();
}

Status QueryExecutor::AddProjectionOperator(const QuerySpec& query_spec,
                                           std::unique_ptr<Pipeline>* pipeline) {
    if (query_spec.columns.empty()) return Status::OK();

    // Create projection operator with selected columns
    auto projection_op = std::make_unique<ProjectionOperator>(
        nullptr, query_spec.columns);
    (*pipeline)->AddOperator(std::move(projection_op));
    return Status::OK();
}

Status QueryExecutor::AddAggregateOperator(const QuerySpec& query_spec,
                                          std::unique_ptr<Pipeline>* pipeline) {
    if (query_spec.group_by.empty()) return Status::OK();

    // Create aggregate functions from query spec
    std::vector<AggregateOperator::AggregateFunction> aggregates;

    // For now, add a simple COUNT aggregate if we have group by
    if (!query_spec.group_by.empty()) {
        aggregates.emplace_back("COUNT", "*", "count");
    }

    auto aggregate_op = std::make_unique<AggregateOperator>(
        nullptr, query_spec.group_by, aggregates);
    (*pipeline)->AddOperator(std::move(aggregate_op));
    return Status::OK();
}

Status QueryExecutor::AddLimitOperator(const QuerySpec& query_spec,
                                      std::unique_ptr<Pipeline>* pipeline) {
    if (query_spec.limit <= 0) return Status::OK();

    auto limit_op = std::make_unique<LimitOperator>(nullptr, query_spec.limit);
    (*pipeline)->AddOperator(std::move(limit_op));
    return Status::OK();
}

//==============================================================================
// Execution Context Implementation
//==============================================================================

ExecutionContext::ExecutionContext() = default;

Status ExecutionContext::RegisterUDF(const std::string& name,
                                    std::function<arrow::Result<std::shared_ptr<arrow::Array>>(
                                        const std::vector<std::shared_ptr<arrow::Array>>&)> udf) {
    udfs_[name] = udf;
    return Status::OK();
}

Status ExecutionContext::GetUDF(const std::string& name,
                               std::function<arrow::Result<std::shared_ptr<arrow::Array>>(
                                   const std::vector<std::shared_ptr<arrow::Array>>&)> * udf) const {
    auto it = udfs_.find(name);
    if (it == udfs_.end()) {
        return Status::NotFound("UDF not found: " + name);
    }

    *udf = it->second;
    return Status::OK();
}

void ExecutionContext::SetParameter(const std::string& key, const std::string& value) {
    parameters_[key] = value;
}

std::string ExecutionContext::GetParameter(const std::string& key) const {
    auto it = parameters_.find(key);
    return it != parameters_.end() ? it->second : "";
}

//==============================================================================
// Task Scheduler Implementation
//==============================================================================

ParallelTaskScheduler::ParallelTaskScheduler(size_t num_threads)
    : num_threads_(num_threads > 0 ? num_threads : std::thread::hardware_concurrency()) {}

ParallelTaskScheduler::~ParallelTaskScheduler() = default;

Status ParallelTaskScheduler::ScheduleMorsel(const Morsel& morsel,
                                           std::function<Status(const Morsel&)> task) {
    // TODO: Implement thread pool and task scheduling
    // For now, execute synchronously
    return task(morsel);
}

Status ParallelTaskScheduler::WaitForCompletion() {
    // TODO: Wait for all scheduled tasks
    return Status::OK();
}

//==============================================================================
// Factory Functions
//==============================================================================

std::unique_ptr<QueryExecutor> CreateQueryExecutor(Database* database) {
    return std::make_unique<QueryExecutor>(database);
}

std::unique_ptr<TaskScheduler> CreateTaskScheduler(size_t num_threads) {
    return std::make_unique<ParallelTaskScheduler>(num_threads);
}

std::unique_ptr<DataChunk> CreateDataChunk(std::shared_ptr<arrow::Schema> schema, int64_t capacity) {
    return std::make_unique<DataChunk>(schema, capacity);
}

} // namespace marble
