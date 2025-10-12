#include "sabot_sql/sql/streaming_operators.h"
#include <arrow/compute/api.h>
#include <algorithm>
#include <sstream>

namespace sabot_sql {
namespace sql {

// TumblingWindowOperator implementation
TumblingWindowOperator::TumblingWindowOperator(
    std::shared_ptr<Operator> source,
    const WindowSpec& window_spec)
    : window_spec_(window_spec)
    , source_(std::move(source))
    , current_window_start_(0)
    , current_window_end_(0)
    , window_initialized_(false) {
}

arrow::Result<std::shared_ptr<arrow::Schema>> 
TumblingWindowOperator::GetOutputSchema() const {
    // Output schema includes window metadata plus aggregated columns
    std::vector<std::shared_ptr<arrow::Field>> fields;
    
    // Add window start and end timestamps
    fields.push_back(arrow::field("window_start", arrow::timestamp(arrow::TimeUnit::MILLI)));
    fields.push_back(arrow::field("window_end", arrow::timestamp(arrow::TimeUnit::MILLI)));
    
    // Add partition keys
    for (const auto& key : window_spec_.partition_keys) {
        fields.push_back(arrow::field(key, arrow::utf8()));
    }
    
    // Add aggregated columns
    for (const auto& [column, function] : window_spec_.aggregations) {
        if (function == "count") {
            fields.push_back(arrow::field(column + "_count", arrow::int64()));
        } else if (function == "sum") {
            fields.push_back(arrow::field(column + "_sum", arrow::float64()));
        } else if (function == "avg") {
            fields.push_back(arrow::field(column + "_avg", arrow::float64()));
        } else if (function == "min") {
            fields.push_back(arrow::field(column + "_min", arrow::float64()));
        } else if (function == "max") {
            fields.push_back(arrow::field(column + "_max", arrow::float64()));
        }
    }
    
    return arrow::schema(fields);
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> 
TumblingWindowOperator::GetNextBatch() {
    if (!window_initialized_) {
        current_window_start_ = 0;
        current_window_end_ = window_spec_.window_size_ms;
        window_initialized_ = true;
    }
    
    // Process all input batches and group by windows
    while (source_->HasNextBatch()) {
        ARROW_ASSIGN_OR_RAISE(auto batch, source_->GetNextBatch());
        
        // Extract timestamp for each row
        ARROW_ASSIGN_OR_RAISE(auto timestamp, ExtractTimestamp(*batch, window_spec_.time_column));
        
        // Determine which window this batch belongs to
        int64_t window_start = (timestamp / window_spec_.window_size_ms) * window_spec_.window_size_ms;
        std::string window_key = GetWindowKey(window_start);
        
        // Add batch to window buffer
        window_buffers_[window_key].push_back(batch);
    }
    
    // Emit completed windows
    if (!window_buffers_.empty()) {
        auto it = window_buffers_.begin();
        std::string window_key = it->first;
        auto batches = it->second;
        window_buffers_.erase(it);
        
        // Aggregate window
        ARROW_ASSIGN_OR_RAISE(auto result_batch, AggregateWindow(batches));
        
        stats_.rows_processed += result_batch->num_rows();
        stats_.batches_processed++;
        
        return result_batch;
    }
    
    return nullptr;
}

bool TumblingWindowOperator::HasNextBatch() const {
    return !window_buffers_.empty() || source_->HasNextBatch();
}

std::string TumblingWindowOperator::ToString() const {
    std::ostringstream oss;
    oss << "TumblingWindow(";
    oss << "size=" << window_spec_.window_size_ms << "ms, ";
    oss << "time_col=" << window_spec_.time_column;
    oss << ")";
    return oss.str();
}

size_t TumblingWindowOperator::EstimateCardinality() const {
    // Estimate based on window size and input cardinality
    size_t input_cardinality = source_->EstimateCardinality();
    return input_cardinality / (window_spec_.window_size_ms / 1000); // Rough estimate
}

arrow::Result<int64_t> 
TumblingWindowOperator::ExtractTimestamp(const arrow::RecordBatch& batch, 
                                        const std::string& time_column) const {
    auto column = batch.GetColumnByName(time_column);
    if (!column) {
        return arrow::Status::Invalid("Time column not found: " + time_column);
    }
    
    // Convert to timestamp array
    ARROW_ASSIGN_OR_RAISE(auto timestamp_array, 
        arrow::compute::Cast(column, arrow::timestamp(arrow::TimeUnit::MILLI)));
    
    // Get first timestamp (assuming all rows in batch have same timestamp)
    auto timestamp_scalar = timestamp_array->GetScalar(0);
    if (!timestamp_scalar) {
        return arrow::Status::Invalid("Failed to extract timestamp");
    }
    
    return std::static_pointer_cast<arrow::TimestampScalar>(timestamp_scalar)->value;
}

std::string TumblingWindowOperator::GetWindowKey(int64_t window_start) const {
    return std::to_string(window_start);
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> 
TumblingWindowOperator::AggregateWindow(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches) const {
    if (batches.empty()) {
        return arrow::Status::Invalid("Empty window");
    }
    
    // Combine all batches in the window
    std::vector<std::shared_ptr<arrow::RecordBatch>> all_batches;
    for (const auto& batch : batches) {
        all_batches.push_back(batch);
    }
    
    ARROW_ASSIGN_OR_RAISE(auto combined_table, arrow::Table::FromRecordBatches(all_batches));
    ARROW_ASSIGN_OR_RAISE(auto combined_batch, combined_table->CombineChunksToBatch());
    
    // Create result batch with window metadata
    std::vector<std::shared_ptr<arrow::Array>> result_arrays;
    std::vector<std::shared_ptr<arrow::Field>> result_fields;
    
    // Add window start timestamp
    ARROW_ASSIGN_OR_RAISE(auto window_start_array, 
        arrow::MakeArrayFromScalar(arrow::TimestampScalar(current_window_start_, arrow::TimeUnit::MILLI), 1));
    result_arrays.push_back(window_start_array);
    result_fields.push_back(arrow::field("window_start", arrow::timestamp(arrow::TimeUnit::MILLI)));
    
    // Add window end timestamp
    ARROW_ASSIGN_OR_RAISE(auto window_end_array, 
        arrow::MakeArrayFromScalar(arrow::TimestampScalar(current_window_end_, arrow::TimeUnit::MILLI), 1));
    result_arrays.push_back(window_end_array);
    result_fields.push_back(arrow::field("window_end", arrow::timestamp(arrow::TimeUnit::MILLI)));
    
    // Add partition keys
    for (const auto& key : window_spec_.partition_keys) {
        auto column = combined_batch->GetColumnByName(key);
        if (column) {
            result_arrays.push_back(column);
            result_fields.push_back(arrow::field(key, column->type()));
        }
    }
    
    // Add aggregations
    for (const auto& [column, function] : window_spec_.aggregations) {
        auto source_column = combined_batch->GetColumnByName(column);
        if (!source_column) {
            continue;
        }
        
        if (function == "count") {
            ARROW_ASSIGN_OR_RAISE(auto count_array, 
                arrow::MakeArrayFromScalar(arrow::Int64Scalar(combined_batch->num_rows()), 1));
            result_arrays.push_back(count_array);
            result_fields.push_back(arrow::field(column + "_count", arrow::int64()));
        } else if (function == "sum") {
            ARROW_ASSIGN_OR_RAISE(auto sum_result, arrow::compute::Sum(source_column));
            ARROW_ASSIGN_OR_RAISE(auto sum_array, 
                arrow::MakeArrayFromScalar(*sum_result.scalar(), 1));
            result_arrays.push_back(sum_array);
            result_fields.push_back(arrow::field(column + "_sum", arrow::float64()));
        } else if (function == "avg") {
            ARROW_ASSIGN_OR_RAISE(auto avg_result, arrow::compute::Mean(source_column));
            ARROW_ASSIGN_OR_RAISE(auto avg_array, 
                arrow::MakeArrayFromScalar(*avg_result.scalar(), 1));
            result_arrays.push_back(avg_array);
            result_fields.push_back(arrow::field(column + "_avg", arrow::float64()));
        } else if (function == "min") {
            ARROW_ASSIGN_OR_RAISE(auto min_result, arrow::compute::Min(source_column));
            ARROW_ASSIGN_OR_RAISE(auto min_array, 
                arrow::MakeArrayFromScalar(*min_result.scalar(), 1));
            result_arrays.push_back(min_array);
            result_fields.push_back(arrow::field(column + "_min", arrow::float64()));
        } else if (function == "max") {
            ARROW_ASSIGN_OR_RAISE(auto max_result, arrow::compute::Max(source_column));
            ARROW_ASSIGN_OR_RAISE(auto max_array, 
                arrow::MakeArrayFromScalar(*max_result.scalar(), 1));
            result_arrays.push_back(max_array);
            result_fields.push_back(arrow::field(column + "_max", arrow::float64()));
        }
    }
    
    auto result_schema = arrow::schema(result_fields);
    return arrow::RecordBatch::Make(result_schema, 1, result_arrays);
}

// SlidingWindowOperator implementation
SlidingWindowOperator::SlidingWindowOperator(
    std::shared_ptr<Operator> source,
    const WindowSpec& window_spec)
    : window_spec_(window_spec)
    , source_(std::move(source))
    , window_initialized_(false) {
}

arrow::Result<std::shared_ptr<arrow::Schema>> 
SlidingWindowOperator::GetOutputSchema() const {
    // Similar to TumblingWindowOperator
    std::vector<std::shared_ptr<arrow::Field>> fields;
    
    fields.push_back(arrow::field("window_start", arrow::timestamp(arrow::TimeUnit::MILLI)));
    fields.push_back(arrow::field("window_end", arrow::timestamp(arrow::TimeUnit::MILLI)));
    
    for (const auto& key : window_spec_.partition_keys) {
        fields.push_back(arrow::field(key, arrow::utf8()));
    }
    
    for (const auto& [column, function] : window_spec_.aggregations) {
        if (function == "count") {
            fields.push_back(arrow::field(column + "_count", arrow::int64()));
        } else if (function == "sum") {
            fields.push_back(arrow::field(column + "_sum", arrow::float64()));
        } else if (function == "avg") {
            fields.push_back(arrow::field(column + "_avg", arrow::float64()));
        } else if (function == "min") {
            fields.push_back(arrow::field(column + "_min", arrow::float64()));
        } else if (function == "max") {
            fields.push_back(arrow::field(column + "_max", arrow::float64()));
        }
    }
    
    return arrow::schema(fields);
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> 
SlidingWindowOperator::GetNextBatch() {
    if (!window_initialized_) {
        window_initialized_ = true;
    }
    
    // Process all input batches and group by sliding windows
    while (source_->HasNextBatch()) {
        ARROW_ASSIGN_OR_RAISE(auto batch, source_->GetNextBatch());
        
        ARROW_ASSIGN_OR_RAISE(auto timestamp, ExtractTimestamp(*batch, window_spec_.time_column));
        
        // Determine which sliding windows this batch belongs to
        auto active_windows = GetActiveWindows(timestamp);
        
        for (int64_t window_start : active_windows) {
            std::string window_key = GetWindowKey(window_start);
            window_buffers_[window_key].push_back(batch);
        }
    }
    
    // Emit completed windows
    if (!window_buffers_.empty()) {
        auto it = window_buffers_.begin();
        std::string window_key = it->first;
        auto batches = it->second;
        window_buffers_.erase(it);
        
        ARROW_ASSIGN_OR_RAISE(auto result_batch, AggregateWindow(batches));
        
        stats_.rows_processed += result_batch->num_rows();
        stats_.batches_processed++;
        
        return result_batch;
    }
    
    return nullptr;
}

bool SlidingWindowOperator::HasNextBatch() const {
    return !window_buffers_.empty() || source_->HasNextBatch();
}

std::string SlidingWindowOperator::ToString() const {
    std::ostringstream oss;
    oss << "SlidingWindow(";
    oss << "size=" << window_spec_.window_size_ms << "ms, ";
    oss << "slide=" << window_spec_.slide_size_ms << "ms, ";
    oss << "time_col=" << window_spec_.time_column;
    oss << ")";
    return oss.str();
}

size_t SlidingWindowOperator::EstimateCardinality() const {
    size_t input_cardinality = source_->EstimateCardinality();
    return input_cardinality * (window_spec_.window_size_ms / window_spec_.slide_size_ms);
}

arrow::Result<int64_t> 
SlidingWindowOperator::ExtractTimestamp(const arrow::RecordBatch& batch, 
                                        const std::string& time_column) const {
    auto column = batch.GetColumnByName(time_column);
    if (!column) {
        return arrow::Status::Invalid("Time column not found: " + time_column);
    }
    
    ARROW_ASSIGN_OR_RAISE(auto timestamp_array, 
        arrow::compute::Cast(column, arrow::timestamp(arrow::TimeUnit::MILLI)));
    
    auto timestamp_scalar = timestamp_array->GetScalar(0);
    if (!timestamp_scalar) {
        return arrow::Status::Invalid("Failed to extract timestamp");
    }
    
    return std::static_pointer_cast<arrow::TimestampScalar>(timestamp_scalar)->value;
}

std::vector<int64_t> 
SlidingWindowOperator::GetActiveWindows(int64_t timestamp) const {
    std::vector<int64_t> windows;
    
    // Calculate all sliding windows that contain this timestamp
    int64_t window_start = (timestamp / window_spec_.slide_size_ms) * window_spec_.slide_size_ms;
    int64_t window_end = window_start + window_spec_.window_size_ms;
    
    // Add all overlapping windows
    for (int64_t start = window_start; start >= 0 && start + window_spec_.window_size_ms > timestamp; 
         start -= window_spec_.slide_size_ms) {
        if (start + window_spec_.window_size_ms > timestamp) {
            windows.push_back(start);
        }
    }
    
    return windows;
}

std::string SlidingWindowOperator::GetWindowKey(int64_t window_start) const {
    return std::to_string(window_start);
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> 
SlidingWindowOperator::AggregateWindow(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches) const {
    // Similar to TumblingWindowOperator::AggregateWindow
    if (batches.empty()) {
        return arrow::Status::Invalid("Empty window");
    }
    
    std::vector<std::shared_ptr<arrow::RecordBatch>> all_batches;
    for (const auto& batch : batches) {
        all_batches.push_back(batch);
    }
    
    ARROW_ASSIGN_OR_RAISE(auto combined_table, arrow::Table::FromRecordBatches(all_batches));
    ARROW_ASSIGN_OR_RAISE(auto combined_batch, combined_table->CombineChunksToBatch());
    
    // Create result batch (similar to TumblingWindowOperator)
    std::vector<std::shared_ptr<arrow::Array>> result_arrays;
    std::vector<std::shared_ptr<arrow::Field>> result_fields;
    
    // Add window metadata and aggregations
    // (Implementation similar to TumblingWindowOperator)
    
    auto result_schema = arrow::schema(result_fields);
    return arrow::RecordBatch::Make(result_schema, 1, result_arrays);
}

// SampleByOperator implementation
SampleByOperator::SampleByOperator(
    std::shared_ptr<Operator> source,
    const std::string& time_column,
    const std::string& interval,
    const std::vector<std::string>& group_by_columns,
    const std::unordered_map<std::string, std::string>& aggregations)
    : source_(std::move(source))
    , time_column_(time_column)
    , interval_(interval)
    , group_by_columns_(group_by_columns)
    , aggregations_(aggregations)
    , current_sample_start_(0)
    , sample_initialized_(false) {
}

arrow::Result<std::shared_ptr<arrow::Schema>> 
SampleByOperator::GetOutputSchema() const {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    
    // Add sample start timestamp
    fields.push_back(arrow::field("sample_start", arrow::timestamp(arrow::TimeUnit::MILLI)));
    
    // Add group by columns
    for (const auto& column : group_by_columns_) {
        fields.push_back(arrow::field(column, arrow::utf8()));
    }
    
    // Add aggregated columns
    for (const auto& [column, function] : aggregations_) {
        if (function == "count") {
            fields.push_back(arrow::field(column + "_count", arrow::int64()));
        } else if (function == "sum") {
            fields.push_back(arrow::field(column + "_sum", arrow::float64()));
        } else if (function == "avg") {
            fields.push_back(arrow::field(column + "_avg", arrow::float64()));
        } else if (function == "min") {
            fields.push_back(arrow::field(column + "_min", arrow::float64()));
        } else if (function == "max") {
            fields.push_back(arrow::field(column + "_max", arrow::float64()));
        }
    }
    
    return arrow::schema(fields);
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> 
SampleByOperator::GetNextBatch() {
    if (!sample_initialized_) {
        current_sample_start_ = 0;
        sample_initialized_ = true;
    }
    
    // Process all input batches and group by sample intervals
    while (source_->HasNextBatch()) {
        ARROW_ASSIGN_OR_RAISE(auto batch, source_->GetNextBatch());
        
        ARROW_ASSIGN_OR_RAISE(auto timestamp, ExtractTimestamp(*batch));
        
        // Determine which sample this batch belongs to
        int64_t sample_start = GetSampleStart(timestamp);
        std::string sample_key = GetSampleKey(sample_start);
        
        // Add batch to sample buffer
        sample_buffers_[sample_key].push_back(batch);
    }
    
    // Emit completed samples
    if (!sample_buffers_.empty()) {
        auto it = sample_buffers_.begin();
        std::string sample_key = it->first;
        auto batches = it->second;
        sample_buffers_.erase(it);
        
        ARROW_ASSIGN_OR_RAISE(auto result_batch, AggregateSample(batches));
        
        stats_.rows_processed += result_batch->num_rows();
        stats_.batches_processed++;
        
        return result_batch;
    }
    
    return nullptr;
}

bool SampleByOperator::HasNextBatch() const {
    return !sample_buffers_.empty() || source_->HasNextBatch();
}

std::string SampleByOperator::ToString() const {
    std::ostringstream oss;
    oss << "SampleBy(";
    oss << "interval=" << interval_ << ", ";
    oss << "time_col=" << time_column_;
    oss << ")";
    return oss.str();
}

size_t SampleByOperator::EstimateCardinality() const {
    size_t input_cardinality = source_->EstimateCardinality();
    // Rough estimate based on sample interval
    return input_cardinality / 10; // Assume 10% sampling
}

arrow::Result<int64_t> 
SampleByOperator::ExtractTimestamp(const arrow::RecordBatch& batch) const {
    auto column = batch.GetColumnByName(time_column_);
    if (!column) {
        return arrow::Status::Invalid("Time column not found: " + time_column_);
    }
    
    ARROW_ASSIGN_OR_RAISE(auto timestamp_array, 
        arrow::compute::Cast(column, arrow::timestamp(arrow::TimeUnit::MILLI)));
    
    auto timestamp_scalar = timestamp_array->GetScalar(0);
    if (!timestamp_scalar) {
        return arrow::Status::Invalid("Failed to extract timestamp");
    }
    
    return std::static_pointer_cast<arrow::TimestampScalar>(timestamp_scalar)->value;
}

int64_t SampleByOperator::GetSampleStart(int64_t timestamp) const {
    // Parse interval and calculate sample start
    // This is a simplified implementation
    if (interval_ == "1h") {
        return (timestamp / 3600000) * 3600000; // 1 hour in milliseconds
    } else if (interval_ == "1m") {
        return (timestamp / 60000) * 60000; // 1 minute in milliseconds
    } else if (interval_ == "1s") {
        return (timestamp / 1000) * 1000; // 1 second in milliseconds
    } else {
        // Default to 1 minute
        return (timestamp / 60000) * 60000;
    }
}

std::string SampleByOperator::GetSampleKey(int64_t sample_start) const {
    return std::to_string(sample_start);
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> 
SampleByOperator::AggregateSample(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches) const {
    if (batches.empty()) {
        return arrow::Status::Invalid("Empty sample");
    }
    
    // Combine all batches in the sample
    std::vector<std::shared_ptr<arrow::RecordBatch>> all_batches;
    for (const auto& batch : batches) {
        all_batches.push_back(batch);
    }
    
    ARROW_ASSIGN_OR_RAISE(auto combined_table, arrow::Table::FromRecordBatches(all_batches));
    ARROW_ASSIGN_OR_RAISE(auto combined_batch, combined_table->CombineChunksToBatch());
    
    // Create result batch with sample metadata and aggregations
    std::vector<std::shared_ptr<arrow::Array>> result_arrays;
    std::vector<std::shared_ptr<arrow::Field>> result_fields;
    
    // Add sample start timestamp
    ARROW_ASSIGN_OR_RAISE(auto sample_start_array, 
        arrow::MakeArrayFromScalar(arrow::TimestampScalar(current_sample_start_, arrow::TimeUnit::MILLI), 1));
    result_arrays.push_back(sample_start_array);
    result_fields.push_back(arrow::field("sample_start", arrow::timestamp(arrow::TimeUnit::MILLI)));
    
    // Add group by columns and aggregations
    // (Implementation similar to TumblingWindowOperator)
    
    auto result_schema = arrow::schema(result_fields);
    return arrow::RecordBatch::Make(result_schema, 1, result_arrays);
}

} // namespace sql
} // namespace sabot_sql
