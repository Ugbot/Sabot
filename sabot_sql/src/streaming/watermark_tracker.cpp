#include "sabot_sql/streaming/watermark_tracker.h"
#include <arrow/compute/api.h>
#include <chrono>
#include <algorithm>
#include <sstream>

namespace sabot_sql {
namespace streaming {

WatermarkTracker::WatermarkTracker() = default;
WatermarkTracker::~WatermarkTracker() = default;

// ========== Lifecycle ==========

arrow::Status WatermarkTracker::Initialize(
    const std::string& connector_id,
    const std::string& watermark_column,
    int64_t max_out_of_orderness_ms,
    std::shared_ptr<marble::Client> marble_client
) {
    connector_id_ = connector_id;
    watermark_column_ = watermark_column;
    max_out_of_orderness_ms_ = max_out_of_orderness_ms;
    marble_client_ = marble_client;
    
    // Initialize MarbleDB table for persistence
    state_table_name_ = "watermark_state_" + connector_id_;
    ARROW_RETURN_NOT_OK(InitializeMarbleDBTable());
    
    // Load existing state if available
    ARROW_RETURN_NOT_OK(LoadState());
    
    return arrow::Status::OK();
}

arrow::Status WatermarkTracker::Shutdown() {
    // Save final state
    ARROW_RETURN_NOT_OK(SaveState());
    return arrow::Status::OK();
}

arrow::Status WatermarkTracker::InitializeMarbleDBTable() {
    // TODO: Initialize MarbleDB table for watermark state
    // For now, we'll use in-memory storage
    // In production, this would create a MarbleDB table with schema:
    // [partition_id: int32, watermark: int64, last_update: int64]
    
    return arrow::Status::OK();
}

// ========== Watermark Processing ==========

arrow::Result<int64_t> WatermarkTracker::UpdateWatermark(
    const std::shared_ptr<arrow::RecordBatch>& batch,
    int partition_id
) {
    if (batch->num_rows() == 0) {
        return GetPartitionWatermark(partition_id);
    }
    
    // Extract max timestamp from batch
    ARROW_ASSIGN_OR_RAISE(int64_t max_timestamp, ExtractMaxTimestamp(batch));
    
    // Calculate watermark: max(event_time) - max_out_of_orderness
    int64_t watermark = max_timestamp - max_out_of_orderness_ms_;
    
    // Update partition watermark
    {
        std::lock_guard<std::mutex> lock(mutex_);
        partition_watermarks_[partition_id] = watermark;
        last_update_time_ = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count();
    }
    
    // Update global watermark
    UpdateCurrentWatermark();
    
    return watermark;
}

int64_t WatermarkTracker::GetCurrentWatermark() const {
    return current_watermark_.load();
}

int64_t WatermarkTracker::GetPartitionWatermark(int partition_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = partition_watermarks_.find(partition_id);
    return (it != partition_watermarks_.end()) ? it->second : 0;
}

void WatermarkTracker::UpdateCurrentWatermark() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (partition_watermarks_.empty()) {
        current_watermark_ = 0;
        return;
    }
    
    // Global watermark = min(partition_watermarks)
    // This ensures we don't advance watermark past any partition
    int64_t min_watermark = std::numeric_limits<int64_t>::max();
    for (const auto& [partition_id, watermark] : partition_watermarks_) {
        min_watermark = std::min(min_watermark, watermark);
    }
    
    // Watermarks must be monotonically increasing
    int64_t current = current_watermark_.load();
    if (min_watermark > current) {
        current_watermark_ = min_watermark;
    }
}

arrow::Result<int64_t> WatermarkTracker::ExtractMaxTimestamp(
    const std::shared_ptr<arrow::RecordBatch>& batch
) {
    // Find watermark column
    auto schema = batch->schema();
    int col_index = schema->GetFieldIndex(watermark_column_);
    
    if (col_index < 0) {
        return arrow::Status::Invalid(
            "Watermark column not found: " + watermark_column_
        );
    }
    
    auto column = batch->column(col_index);
    return ExtractTimestampFromColumn(column);
}

arrow::Result<int64_t> WatermarkTracker::ExtractTimestampFromColumn(
    const std::shared_ptr<arrow::Array>& column
) {
    // Manual max computation to avoid Arrow compute function registration
    int64_t max_timestamp = 0;
    
    if (column->type()->id() == arrow::Type::TIMESTAMP) {
        auto timestamp_array = std::static_pointer_cast<arrow::TimestampArray>(column);
        for (int64_t i = 0; i < column->length(); ++i) {
            if (!column->IsNull(i)) {
                max_timestamp = std::max(max_timestamp, timestamp_array->Value(i));
            }
        }
    } else if (column->type()->id() == arrow::Type::INT64) {
        auto int64_array = std::static_pointer_cast<arrow::Int64Array>(column);
        for (int64_t i = 0; i < column->length(); ++i) {
            if (!column->IsNull(i)) {
                max_timestamp = std::max(max_timestamp, int64_array->Value(i));
            }
        }
    } else {
        return arrow::Status::Invalid("Watermark column is not a timestamp or int64");
    }
    
    return max_timestamp;
}

// ========== Window Triggering ==========

bool WatermarkTracker::ShouldTriggerWindow(int64_t window_end) const {
    return GetCurrentWatermark() >= window_end;
}

std::vector<int64_t> WatermarkTracker::GetTriggeredWindows(
    const std::vector<int64_t>& pending_windows
) const {
    int64_t current_watermark = GetCurrentWatermark();
    std::vector<int64_t> triggered;
    
    for (int64_t window_end : pending_windows) {
        if (current_watermark >= window_end) {
            triggered.push_back(window_end);
        }
    }
    
    return triggered;
}

// ========== Late Data Handling ==========

bool WatermarkTracker::IsLate(int64_t event_time) const {
    return event_time < GetCurrentWatermark();
}

arrow::Status WatermarkTracker::HandleLateData(
    int64_t event_time,
    const std::string& policy
) {
    if (policy == "drop") {
        // Drop late data
        return arrow::Status::OK();
    } else if (policy == "buffer") {
        // Buffer late data for later processing
        // TODO: Implement buffering mechanism
        return arrow::Status::OK();
    } else if (policy == "update_watermark") {
        // Update watermark to accommodate late data
        // This is risky as it can cause window re-triggering
        // TODO: Implement with caution
        return arrow::Status::NotImplemented("update_watermark policy not implemented");
    } else {
        return arrow::Status::Invalid("Unknown late data policy: " + policy);
    }
}

// ========== Persistence ==========

arrow::Status WatermarkTracker::SaveState() {
    // TODO: Save watermark state to MarbleDB
    // For now, we'll use in-memory storage
    // In production, this would serialize partition_watermarks_ to MarbleDB
    
    return arrow::Status::OK();
}

arrow::Status WatermarkTracker::LoadState() {
    // TODO: Load watermark state from MarbleDB
    // For now, we'll start with empty state
    // In production, this would deserialize from MarbleDB
    
    return arrow::Status::OK();
}

// ========== Monitoring ==========

WatermarkTracker::WatermarkStats WatermarkTracker::GetStats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    
    WatermarkStats stats;
    stats.current_watermark = current_watermark_.load();
    stats.last_update_time = last_update_time_.load();
    stats.active_partitions = partition_watermarks_.size();
    
    if (partition_watermarks_.empty()) {
        stats.min_partition_watermark = 0;
        stats.max_partition_watermark = 0;
    } else {
        auto [min_it, max_it] = std::minmax_element(
            partition_watermarks_.begin(),
            partition_watermarks_.end(),
            [](const auto& a, const auto& b) { return a.second < b.second; }
        );
        stats.min_partition_watermark = min_it->second;
        stats.max_partition_watermark = max_it->second;
    }
    
    return stats;
}

// ========== WatermarkCoordinator Implementation ==========

WatermarkCoordinator::WatermarkCoordinator() = default;
WatermarkCoordinator::~WatermarkCoordinator() = default;

arrow::Status WatermarkCoordinator::RegisterTracker(
    const std::string& tracker_id,
    std::shared_ptr<WatermarkTracker> tracker
) {
    std::lock_guard<std::mutex> lock(mutex_);
    trackers_[tracker_id] = std::move(tracker);
    return arrow::Status::OK();
}

arrow::Status WatermarkCoordinator::UnregisterTracker(const std::string& tracker_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    trackers_.erase(tracker_id);
    return arrow::Status::OK();
}

int64_t WatermarkCoordinator::GetGlobalWatermark() const {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (trackers_.empty()) {
        return 0;
    }
    
    // Global watermark = min(all_tracker_watermarks)
    int64_t min_watermark = std::numeric_limits<int64_t>::max();
    for (const auto& [tracker_id, tracker] : trackers_) {
        min_watermark = std::min(min_watermark, tracker->GetCurrentWatermark());
    }
    
    return min_watermark;
}

std::vector<int64_t> WatermarkCoordinator::GetGlobalTriggeredWindows(
    const std::vector<int64_t>& pending_windows
) const {
    int64_t global_watermark = GetGlobalWatermark();
    std::vector<int64_t> triggered;
    
    for (int64_t window_end : pending_windows) {
        if (global_watermark >= window_end) {
            triggered.push_back(window_end);
        }
    }
    
    return triggered;
}

} // namespace streaming
} // namespace sabot_sql
