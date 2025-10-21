#pragma once

#include <arrow/api.h>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <mutex>
#include <atomic>

namespace marble {
    class Client;  // Forward declare MarbleDB client
}

namespace sabot_sql {
namespace streaming {

/**
 * Window aggregation operator for streaming SQL
 * 
 * Maintains stateful window aggregates with MarbleDB backend.
 * Triggers window results when watermark advances past window end.
 * 
 * Features:
 * - Keyed state per window (symbol, window_start)
 * - MarbleDB local table storage (is_raft_replicated=false)
 * - Watermark-driven window triggering
 * - Late data handling
 * - Checkpoint/recovery support
 */
class WindowAggregateOperator {
public:
    /**
     * Window type enumeration
     */
    enum class WindowType {
        TUMBLE,    // Fixed-size, non-overlapping windows
        HOP,       // Fixed-size, overlapping windows (sliding)
        SESSION    // Variable-size windows based on inactivity
    };
    
    /**
     * Aggregation function enumeration
     */
    enum class AggregationType {
        COUNT,
        SUM,
        AVG,
        MIN,
        MAX,
        FIRST,
        LAST
    };
    
    /**
     * Window state for a single key-window combination
     */
    struct WindowState {
        std::string key;           // Grouping key (e.g., symbol)
        int64_t window_start;      // Window start timestamp
        int64_t window_end;        // Window end timestamp
        int64_t count;             // Number of records
        double sum;                // Sum of values
        double min;                // Minimum value
        double max;                // Maximum value
        int64_t first_timestamp;   // First record timestamp
        int64_t last_timestamp;    // Last record timestamp
        bool is_closed;            // Whether window has been emitted
        
        WindowState() = default;
        WindowState(const std::string& k, int64_t start, int64_t end)
            : key(k), window_start(start), window_end(end)
            , count(0), sum(0.0), min(std::numeric_limits<double>::max())
            , max(std::numeric_limits<double>::lowest())
            , first_timestamp(0), last_timestamp(0), is_closed(false) {}
    };
    
    WindowAggregateOperator();
    ~WindowAggregateOperator();
    
    // ========== Lifecycle ==========
    
    /**
     * Initialize window operator
     * 
     * @param query_id Unique query identifier
     * @param window_type Type of window (TUMBLE, HOP, SESSION)
     * @param window_size_ms Window size in milliseconds
     * @param window_slide_ms Window slide in milliseconds (for HOP)
     * @param key_column Column name for grouping key
     * @param value_column Column name for aggregation values
     * @param timestamp_column Column name for event timestamps
     * @param marble_client MarbleDB client for state storage
     */
    arrow::Status Initialize(
        const std::string& query_id,
        WindowType window_type,
        int64_t window_size_ms,
        int64_t window_slide_ms,
        const std::string& key_column,
        const std::string& value_column,
        const std::string& timestamp_column,
        std::shared_ptr<marble::Client> marble_client
    );
    
    /**
     * Shutdown window operator
     */
    arrow::Status Shutdown();
    
    // ========== Data Processing ==========
    
    /**
     * Process a batch of records
     * 
     * @param batch Input batch with key, value, timestamp columns
     * @return Status indicating success/failure
     */
    arrow::Status ProcessBatch(const std::shared_ptr<arrow::RecordBatch>& batch);
    
    /**
     * Check for windows that should be triggered
     * 
     * @param current_watermark Current watermark timestamp
     * @return List of triggered window states
     */
    std::vector<WindowState> GetTriggeredWindows(int64_t current_watermark);
    
    /**
     * Emit results for triggered windows
     * 
     * @param triggered_windows List of windows to emit
     * @return Arrow RecordBatch with window results
     */
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> EmitWindowResults(
        const std::vector<WindowState>& triggered_windows
    );
    
    // ========== Window Management ==========
    
    /**
     * Calculate window start for a given timestamp
     */
    int64_t CalculateWindowStart(int64_t timestamp) const;
    
    /**
     * Calculate window end for a given timestamp
     */
    int64_t CalculateWindowEnd(int64_t timestamp) const;
    
    /**
     * Get all active windows for a key
     */
    std::vector<WindowState> GetActiveWindows(const std::string& key) const;
    
    /**
     * Get window state for specific key-window combination
     */
    arrow::Result<WindowState> GetWindowState(
        const std::string& key,
        int64_t window_start
    ) const;
    
    // ========== State Management ==========
    
    /**
     * Save window state to MarbleDB
     * Called during checkpoint for recovery
     */
    arrow::Status SaveState();
    
    /**
     * Load window state from MarbleDB
     * Called during recovery after failure
     */
    arrow::Status LoadState();
    
    /**
     * Clear all window state
     * Used for testing or reset
     */
    arrow::Status ClearState();
    
    // ========== Monitoring ==========
    
    /**
     * Get window operator statistics
     */
    struct WindowStats {
        size_t total_windows;
        size_t active_windows;
        size_t closed_windows;
        int64_t total_records_processed;
        int64_t last_processed_timestamp;
        std::string window_type_str;
        int64_t window_size_ms;
        int64_t window_slide_ms;
    };
    
    WindowStats GetStats() const;
    
private:
    // Configuration
    std::string query_id_;
    WindowType window_type_;
    int64_t window_size_ms_;
    int64_t window_slide_ms_;
    std::string key_column_;
    std::string value_column_;
    std::string timestamp_column_;
    std::shared_ptr<marble::Client> marble_client_;
    
    // State storage
    mutable std::mutex mutex_;
    std::unordered_map<std::string, std::unordered_map<int64_t, WindowState>> window_states_;
    std::atomic<int64_t> total_records_processed_{0};
    std::atomic<int64_t> last_processed_timestamp_{0};
    
    // MarbleDB table for persistence
    std::string state_table_name_;
    
    // Helper methods
    arrow::Status InitializeMarbleDBTable();
    arrow::Status UpdateWindowState(
        const std::string& key,
        int64_t window_start,
        double value,
        int64_t timestamp
    );
    arrow::Status CreateWindowState(
        const std::string& key,
        int64_t window_start,
        int64_t window_end
    );
    std::string WindowTypeToString(WindowType type) const;
    WindowType StringToWindowType(const std::string& str) const;
};

/**
 * Window function utilities
 */
class WindowFunctions {
public:
    /**
     * Calculate tumbling window start
     */
    static int64_t TumbleWindowStart(int64_t timestamp, int64_t window_size_ms);
    
    /**
     * Calculate hopping window start
     */
    static int64_t HopWindowStart(int64_t timestamp, int64_t window_size_ms, int64_t window_slide_ms);
    
    /**
     * Calculate session window start (simplified)
     */
    static int64_t SessionWindowStart(int64_t timestamp, int64_t session_gap_ms);
    
    /**
     * Check if timestamp falls within window
     */
    static bool IsInWindow(int64_t timestamp, int64_t window_start, int64_t window_end);
};

} // namespace streaming
} // namespace sabot_sql
