#pragma once

#include <arrow/api.h>
#include <memory>
#include <string>
#include <unordered_map>
#include <atomic>
#include <mutex>

namespace marble {
    class Client;  // Forward declare MarbleDB client
}

namespace sabot_sql {
namespace streaming {

/**
 * Watermark tracker for event-time processing
 * 
 * Tracks watermarks per partition and manages window triggers.
 * Uses MarbleDB Timer API for efficient watermark storage and retrieval.
 * 
 * Watermark = max(event_time) - max_out_of_orderness
 * Windows close when watermark > window_end
 */
class WatermarkTracker {
public:
    WatermarkTracker();
    ~WatermarkTracker();
    
    // ========== Lifecycle ==========
    
    /**
     * Initialize watermark tracker
     * 
     * @param connector_id Unique identifier for this connector
     * @param watermark_column Column name for event time extraction
     * @param max_out_of_orderness_ms Maximum delay for out-of-order events
     * @param marble_client MarbleDB client for timer storage
     */
    arrow::Status Initialize(
        const std::string& connector_id,
        const std::string& watermark_column,
        int64_t max_out_of_orderness_ms,
        std::shared_ptr<marble::Client> marble_client
    );
    
    /**
     * Shutdown watermark tracker
     */
    arrow::Status Shutdown();
    
    // ========== Watermark Processing ==========
    
    /**
     * Update watermark from a batch of records
     * 
     * @param batch Arrow RecordBatch with event-time data
     * @param partition_id Partition ID (for multi-partition tracking)
     * @return Updated watermark timestamp (monotonically increasing)
     */
    arrow::Result<int64_t> UpdateWatermark(
        const std::shared_ptr<arrow::RecordBatch>& batch,
        int partition_id = 0
    );
    
    /**
     * Get current watermark across all partitions
     * Watermark = min(watermark_per_partition)
     */
    int64_t GetCurrentWatermark() const;
    
    /**
     * Get watermark for specific partition
     */
    int64_t GetPartitionWatermark(int partition_id) const;
    
    // ========== Window Triggering ==========
    
    /**
     * Check if window should be triggered
     * 
     * @param window_end End timestamp of the window
     * @return true if watermark >= window_end
     */
    bool ShouldTriggerWindow(int64_t window_end) const;
    
    /**
     * Get all windows that should be triggered
     * 
     * @param pending_windows List of pending window end times
     * @return List of window end times that should trigger
     */
    std::vector<int64_t> GetTriggeredWindows(
        const std::vector<int64_t>& pending_windows
    ) const;
    
    // ========== Late Data Handling ==========
    
    /**
     * Check if record is late (event_time < watermark)
     * 
     * @param event_time Event timestamp from record
     * @return true if record is late
     */
    bool IsLate(int64_t event_time) const;
    
    /**
     * Handle late data policy
     * 
     * @param event_time Event timestamp from late record
     * @param policy Policy: "drop", "buffer", "update_watermark"
     * @return Status indicating how to handle the record
     */
    arrow::Status HandleLateData(int64_t event_time, const std::string& policy);
    
    // ========== Persistence ==========
    
    /**
     * Save watermark state to MarbleDB
     * Called during checkpoint for recovery
     */
    arrow::Status SaveState();
    
    /**
     * Load watermark state from MarbleDB
     * Called during recovery after failure
     */
    arrow::Status LoadState();
    
    // ========== Monitoring ==========
    
    /**
     * Get watermark statistics
     */
    struct WatermarkStats {
        int64_t current_watermark;
        int64_t min_partition_watermark;
        int64_t max_partition_watermark;
        size_t active_partitions;
        int64_t last_update_time;
    };
    
    WatermarkStats GetStats() const;
    
private:
    // Configuration
    std::string connector_id_;
    std::string watermark_column_;
    int64_t max_out_of_orderness_ms_;
    std::shared_ptr<marble::Client> marble_client_;
    
    // State
    mutable std::mutex mutex_;
    std::unordered_map<int, int64_t> partition_watermarks_;
    std::atomic<int64_t> current_watermark_{0};
    std::atomic<int64_t> last_update_time_{0};
    
    // MarbleDB table for persistence
    std::string state_table_name_;
    
    // Helper methods
    arrow::Result<int64_t> ExtractMaxTimestamp(
        const std::shared_ptr<arrow::RecordBatch>& batch
    );
    arrow::Result<int64_t> ExtractTimestampFromColumn(
        const std::shared_ptr<arrow::Array>& column
    );
    void UpdateCurrentWatermark();
    arrow::Status InitializeMarbleDBTable();
};

/**
 * Multi-partition watermark coordinator
 * 
 * Coordinates watermarks across multiple partitions/connectors
 * for distributed streaming processing.
 */
class WatermarkCoordinator {
public:
    WatermarkCoordinator();
    ~WatermarkCoordinator();
    
    /**
     * Register a watermark tracker
     */
    arrow::Status RegisterTracker(
        const std::string& tracker_id,
        std::shared_ptr<WatermarkTracker> tracker
    );
    
    /**
     * Unregister a watermark tracker
     */
    arrow::Status UnregisterTracker(const std::string& tracker_id);
    
    /**
     * Get global watermark across all trackers
     * Global watermark = min(all_tracker_watermarks)
     */
    int64_t GetGlobalWatermark() const;
    
    /**
     * Check if windows should trigger globally
     */
    std::vector<int64_t> GetGlobalTriggeredWindows(
        const std::vector<int64_t>& pending_windows
    ) const;
    
private:
    mutable std::mutex mutex_;
    std::unordered_map<std::string, std::shared_ptr<WatermarkTracker>> trackers_;
};

} // namespace streaming
} // namespace sabot_sql
