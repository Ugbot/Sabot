#include "sabot_sql/streaming/window_operator.h"
#include <arrow/compute/api.h>
#include <algorithm>
#include <sstream>
#include <cmath>

namespace sabot_sql {
namespace streaming {

WindowAggregateOperator::WindowAggregateOperator() = default;
WindowAggregateOperator::~WindowAggregateOperator() = default;

// ========== Lifecycle ==========

arrow::Status WindowAggregateOperator::Initialize(
    const std::string& query_id,
    WindowType window_type,
    int64_t window_size_ms,
    int64_t window_slide_ms,
    const std::string& key_column,
    const std::string& value_column,
    const std::string& timestamp_column,
    std::shared_ptr<marble::Client> marble_client
) {
    query_id_ = query_id;
    window_type_ = window_type;
    window_size_ms_ = window_size_ms;
    window_slide_ms_ = window_slide_ms;
    key_column_ = key_column;
    value_column_ = value_column;
    timestamp_column_ = timestamp_column;
    marble_client_ = marble_client;
    
    // Initialize MarbleDB table for persistence
    state_table_name_ = "window_state_" + query_id_;
    ARROW_RETURN_NOT_OK(InitializeMarbleDBTable());
    
    // Load existing state if available
    ARROW_RETURN_NOT_OK(LoadState());
    
    return arrow::Status::OK();
}

arrow::Status WindowAggregateOperator::Shutdown() {
    // Save final state
    ARROW_RETURN_NOT_OK(SaveState());
    return arrow::Status::OK();
}

arrow::Status WindowAggregateOperator::InitializeMarbleDBTable() {
    // TODO: Initialize MarbleDB table for window state
    // For now, we'll use in-memory storage
    // In production, this would create a MarbleDB table with schema:
    // [key: utf8, window_start: int64, window_end: int64, count: int64, 
    //  sum: double, min: double, max: double, first_ts: int64, last_ts: int64, is_closed: bool]
    
    return arrow::Status::OK();
}

// ========== Data Processing ==========

arrow::Status WindowAggregateOperator::ProcessBatch(
    const std::shared_ptr<arrow::RecordBatch>& batch
) {
    if (batch->num_rows() == 0) {
        return arrow::Status::OK();
    }
    
    // Find column indices
    auto schema = batch->schema();
    int key_col = schema->GetFieldIndex(key_column_);
    int value_col = schema->GetFieldIndex(value_column_);
    int timestamp_col = schema->GetFieldIndex(timestamp_column_);
    
    if (key_col < 0) {
        return arrow::Status::Invalid("Key column not found: " + key_column_);
    }
    if (value_col < 0) {
        return arrow::Status::Invalid("Value column not found: " + value_column_);
    }
    if (timestamp_col < 0) {
        return arrow::Status::Invalid("Timestamp column not found: " + timestamp_column_);
    }
    
    // Get column arrays
    auto key_array = batch->column(key_col);
    auto value_array = batch->column(value_col);
    auto timestamp_array = batch->column(timestamp_col);
    
    // Process each row
    for (int64_t i = 0; i < batch->num_rows(); ++i) {
        // Extract values
        std::string key;
        double value = 0.0;
        int64_t timestamp = 0;
        
        // Extract key
        if (key_array->IsNull(i)) {
            continue;  // Skip null keys
        }
        key = std::static_pointer_cast<arrow::StringArray>(key_array)->GetString(i);
        
        // Extract value
        if (value_array->IsNull(i)) {
            continue;  // Skip null values
        }
        if (value_array->type()->id() == arrow::Type::DOUBLE) {
            value = std::static_pointer_cast<arrow::DoubleArray>(value_array)->Value(i);
        } else if (value_array->type()->id() == arrow::Type::INT64) {
            value = static_cast<double>(std::static_pointer_cast<arrow::Int64Array>(value_array)->Value(i));
        } else {
            return arrow::Status::Invalid("Unsupported value column type");
        }
        
        // Extract timestamp
        if (timestamp_array->IsNull(i)) {
            continue;  // Skip null timestamps
        }
        if (timestamp_array->type()->id() == arrow::Type::TIMESTAMP) {
            timestamp = std::static_pointer_cast<arrow::TimestampArray>(timestamp_array)->Value(i);
        } else if (timestamp_array->type()->id() == arrow::Type::INT64) {
            timestamp = std::static_pointer_cast<arrow::Int64Array>(timestamp_array)->Value(i);
        } else {
            return arrow::Status::Invalid("Unsupported timestamp column type");
        }
        
        // Calculate window start
        int64_t window_start = CalculateWindowStart(timestamp);
        int64_t window_end = CalculateWindowEnd(timestamp);
        
        // Update window state
        ARROW_RETURN_NOT_OK(UpdateWindowState(key, window_start, value, timestamp));
        
        // Update statistics
        total_records_processed_++;
        last_processed_timestamp_ = std::max(last_processed_timestamp_.load(), timestamp);
    }
    
    return arrow::Status::OK();
}

std::vector<WindowAggregateOperator::WindowState> 
WindowAggregateOperator::GetTriggeredWindows(int64_t current_watermark) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<WindowState> triggered;
    
    for (auto& [key, windows] : window_states_) {
        for (auto& [window_start, state] : windows) {
            if (!state.is_closed && current_watermark >= state.window_end) {
                state.is_closed = true;
                triggered.push_back(state);
            }
        }
    }
    
    return triggered;
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> 
WindowAggregateOperator::EmitWindowResults(
    const std::vector<WindowState>& triggered_windows
) {
    if (triggered_windows.empty()) {
        // Return empty batch with correct schema
        auto schema = arrow::schema({
            arrow::field("key", arrow::utf8()),
            arrow::field("window_start", arrow::timestamp(arrow::TimeUnit::MILLI)),
            arrow::field("window_end", arrow::timestamp(arrow::TimeUnit::MILLI)),
            arrow::field("count", arrow::int64()),
            arrow::field("sum", arrow::float64()),
            arrow::field("avg", arrow::float64()),
            arrow::field("min", arrow::float64()),
            arrow::field("max", arrow::float64()),
            arrow::field("first_timestamp", arrow::timestamp(arrow::TimeUnit::MILLI)),
            arrow::field("last_timestamp", arrow::timestamp(arrow::TimeUnit::MILLI))
        });
        
        std::vector<std::shared_ptr<arrow::Array>> empty_arrays;
        return arrow::RecordBatch::Make(schema, 0, empty_arrays);
    }
    
    // Build arrays for each column
    arrow::StringBuilder key_builder;
    arrow::TimestampBuilder window_start_builder(
        arrow::timestamp(arrow::TimeUnit::MILLI),
        arrow::default_memory_pool()
    );
    arrow::TimestampBuilder window_end_builder(
        arrow::timestamp(arrow::TimeUnit::MILLI),
        arrow::default_memory_pool()
    );
    arrow::Int64Builder count_builder;
    arrow::DoubleBuilder sum_builder;
    arrow::DoubleBuilder avg_builder;
    arrow::DoubleBuilder min_builder;
    arrow::DoubleBuilder max_builder;
    arrow::TimestampBuilder first_timestamp_builder(
        arrow::timestamp(arrow::TimeUnit::MILLI),
        arrow::default_memory_pool()
    );
    arrow::TimestampBuilder last_timestamp_builder(
        arrow::timestamp(arrow::TimeUnit::MILLI),
        arrow::default_memory_pool()
    );
    
    for (const auto& state : triggered_windows) {
        ARROW_RETURN_NOT_OK(key_builder.Append(state.key));
        ARROW_RETURN_NOT_OK(window_start_builder.Append(state.window_start));
        ARROW_RETURN_NOT_OK(window_end_builder.Append(state.window_end));
        ARROW_RETURN_NOT_OK(count_builder.Append(state.count));
        ARROW_RETURN_NOT_OK(sum_builder.Append(state.sum));
        
        // Calculate average
        double avg = (state.count > 0) ? (state.sum / state.count) : 0.0;
        ARROW_RETURN_NOT_OK(avg_builder.Append(avg));
        
        // Handle min/max for empty windows
        if (state.count == 0) {
            ARROW_RETURN_NOT_OK(min_builder.AppendNull());
            ARROW_RETURN_NOT_OK(max_builder.AppendNull());
        } else {
            ARROW_RETURN_NOT_OK(min_builder.Append(state.min));
            ARROW_RETURN_NOT_OK(max_builder.Append(state.max));
        }
        
        ARROW_RETURN_NOT_OK(first_timestamp_builder.Append(state.first_timestamp));
        ARROW_RETURN_NOT_OK(last_timestamp_builder.Append(state.last_timestamp));
    }
    
    // Finish arrays
    std::shared_ptr<arrow::Array> key_array;
    std::shared_ptr<arrow::Array> window_start_array;
    std::shared_ptr<arrow::Array> window_end_array;
    std::shared_ptr<arrow::Array> count_array;
    std::shared_ptr<arrow::Array> sum_array;
    std::shared_ptr<arrow::Array> avg_array;
    std::shared_ptr<arrow::Array> min_array;
    std::shared_ptr<arrow::Array> max_array;
    std::shared_ptr<arrow::Array> first_timestamp_array;
    std::shared_ptr<arrow::Array> last_timestamp_array;
    
    ARROW_RETURN_NOT_OK(key_builder.Finish(&key_array));
    ARROW_RETURN_NOT_OK(window_start_builder.Finish(&window_start_array));
    ARROW_RETURN_NOT_OK(window_end_builder.Finish(&window_end_array));
    ARROW_RETURN_NOT_OK(count_builder.Finish(&count_array));
    ARROW_RETURN_NOT_OK(sum_builder.Finish(&sum_array));
    ARROW_RETURN_NOT_OK(avg_builder.Finish(&avg_array));
    ARROW_RETURN_NOT_OK(min_builder.Finish(&min_array));
    ARROW_RETURN_NOT_OK(max_builder.Finish(&max_array));
    ARROW_RETURN_NOT_OK(first_timestamp_builder.Finish(&first_timestamp_array));
    ARROW_RETURN_NOT_OK(last_timestamp_builder.Finish(&last_timestamp_array));
    
    // Create schema
    auto schema = arrow::schema({
        arrow::field("key", arrow::utf8()),
        arrow::field("window_start", arrow::timestamp(arrow::TimeUnit::MILLI)),
        arrow::field("window_end", arrow::timestamp(arrow::TimeUnit::MILLI)),
        arrow::field("count", arrow::int64()),
        arrow::field("sum", arrow::float64()),
        arrow::field("avg", arrow::float64()),
        arrow::field("min", arrow::float64()),
        arrow::field("max", arrow::float64()),
        arrow::field("first_timestamp", arrow::timestamp(arrow::TimeUnit::MILLI)),
        arrow::field("last_timestamp", arrow::timestamp(arrow::TimeUnit::MILLI))
    });
    
    // Create record batch
    return arrow::RecordBatch::Make(
        schema,
        triggered_windows.size(),
        {
            key_array, window_start_array, window_end_array, count_array, sum_array,
            avg_array, min_array, max_array, first_timestamp_array, last_timestamp_array
        }
    );
}

// ========== Window Management ==========

int64_t WindowAggregateOperator::CalculateWindowStart(int64_t timestamp) const {
    switch (window_type_) {
        case WindowType::TUMBLE:
            return WindowFunctions::TumbleWindowStart(timestamp, window_size_ms_);
        case WindowType::HOP:
            return WindowFunctions::HopWindowStart(timestamp, window_size_ms_, window_slide_ms_);
        case WindowType::SESSION:
            return WindowFunctions::SessionWindowStart(timestamp, window_slide_ms_);
        default:
            return timestamp;  // Fallback
    }
}

int64_t WindowAggregateOperator::CalculateWindowEnd(int64_t timestamp) const {
    int64_t window_start = CalculateWindowStart(timestamp);
    return window_start + window_size_ms_;
}

std::vector<WindowAggregateOperator::WindowState> 
WindowAggregateOperator::GetActiveWindows(const std::string& key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<WindowState> active_windows;
    
    auto it = window_states_.find(key);
    if (it != window_states_.end()) {
        for (const auto& [window_start, state] : it->second) {
            if (!state.is_closed) {
                active_windows.push_back(state);
            }
        }
    }
    
    return active_windows;
}

arrow::Result<WindowAggregateOperator::WindowState> 
WindowAggregateOperator::GetWindowState(
    const std::string& key,
    int64_t window_start
) const {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto key_it = window_states_.find(key);
    if (key_it == window_states_.end()) {
        return arrow::Status::Invalid("Key not found: " + key);
    }
    
    auto window_it = key_it->second.find(window_start);
    if (window_it == key_it->second.end()) {
        return arrow::Status::Invalid("Window not found for key: " + key);
    }
    
    return window_it->second;
}

// ========== State Management ==========

arrow::Status WindowAggregateOperator::UpdateWindowState(
    const std::string& key,
    int64_t window_start,
    double value,
    int64_t timestamp
) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Get or create window state
    auto& key_windows = window_states_[key];
    auto it = key_windows.find(window_start);
    
    if (it == key_windows.end()) {
        // Create new window state
        int64_t window_end = window_start + window_size_ms_;
        key_windows[window_start] = WindowState(key, window_start, window_end);
        it = key_windows.find(window_start);
    }
    
    // Update window state
    WindowState& state = it->second;
    state.count++;
    state.sum += value;
    state.min = std::min(state.min, value);
    state.max = std::max(state.max, value);
    
    if (state.first_timestamp == 0) {
        state.first_timestamp = timestamp;
    }
    state.last_timestamp = timestamp;
    
    return arrow::Status::OK();
}

arrow::Status WindowAggregateOperator::CreateWindowState(
    const std::string& key,
    int64_t window_start,
    int64_t window_end
) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    window_states_[key][window_start] = WindowState(key, window_start, window_end);
    return arrow::Status::OK();
}

arrow::Status WindowAggregateOperator::SaveState() {
    // TODO: Save window state to MarbleDB
    // For now, we'll use in-memory storage
    // In production, this would serialize window_states_ to MarbleDB
    
    return arrow::Status::OK();
}

arrow::Status WindowAggregateOperator::LoadState() {
    // TODO: Load window state from MarbleDB
    // For now, we'll start with empty state
    // In production, this would deserialize from MarbleDB
    
    return arrow::Status::OK();
}

arrow::Status WindowAggregateOperator::ClearState() {
    std::lock_guard<std::mutex> lock(mutex_);
    window_states_.clear();
    total_records_processed_ = 0;
    last_processed_timestamp_ = 0;
    return arrow::Status::OK();
}

// ========== Monitoring ==========

WindowAggregateOperator::WindowStats WindowAggregateOperator::GetStats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    
    WindowStats stats;
    stats.total_windows = 0;
    stats.active_windows = 0;
    stats.closed_windows = 0;
    stats.total_records_processed = total_records_processed_.load();
    stats.last_processed_timestamp = last_processed_timestamp_.load();
    stats.window_type_str = WindowTypeToString(window_type_);
    stats.window_size_ms = window_size_ms_;
    stats.window_slide_ms = window_slide_ms_;
    
    for (const auto& [key, windows] : window_states_) {
        stats.total_windows += windows.size();
        for (const auto& [window_start, state] : windows) {
            if (state.is_closed) {
                stats.closed_windows++;
            } else {
                stats.active_windows++;
            }
        }
    }
    
    return stats;
}

std::string WindowAggregateOperator::WindowTypeToString(WindowType type) const {
    switch (type) {
        case WindowType::TUMBLE: return "TUMBLE";
        case WindowType::HOP: return "HOP";
        case WindowType::SESSION: return "SESSION";
        default: return "UNKNOWN";
    }
}

WindowAggregateOperator::WindowType 
WindowAggregateOperator::StringToWindowType(const std::string& str) const {
    if (str == "TUMBLE") return WindowType::TUMBLE;
    if (str == "HOP") return WindowType::HOP;
    if (str == "SESSION") return WindowType::SESSION;
    return WindowType::TUMBLE;  // Default
}

// ========== WindowFunctions Implementation ==========

int64_t WindowFunctions::TumbleWindowStart(int64_t timestamp, int64_t window_size_ms) {
    return (timestamp / window_size_ms) * window_size_ms;
}

int64_t WindowFunctions::HopWindowStart(int64_t timestamp, int64_t window_size_ms, int64_t window_slide_ms) {
    return (timestamp / window_slide_ms) * window_slide_ms;
}

int64_t WindowFunctions::SessionWindowStart(int64_t timestamp, int64_t session_gap_ms) {
    // Simplified session window implementation
    // In production, this would maintain session state
    return timestamp;
}

bool WindowFunctions::IsInWindow(int64_t timestamp, int64_t window_start, int64_t window_end) {
    return timestamp >= window_start && timestamp < window_end;
}

} // namespace streaming
} // namespace sabot_sql
