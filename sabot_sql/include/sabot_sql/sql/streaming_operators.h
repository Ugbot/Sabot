#pragma once

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <arrow/api.h>
#include <arrow/result.h>
#include "sabot_sql/operators/operator.h"

namespace sabot_sql {
namespace sql {

/**
 * @brief Streaming SQL Operators extending Sabot's operator framework
 * 
 * These operators implement streaming constructs borrowed from Flink SQL
 * and QuestDB, providing time-series and windowing capabilities.
 */

/**
 * @brief Window Type for streaming operations
 */
enum class WindowType {
    TUMBLING,    // Fixed-size, non-overlapping windows
    SLIDING,     // Fixed-size, overlapping windows (hop windows)
    SESSION,     // Variable-size windows based on inactivity
    HOPPING      // Fixed-size windows with configurable hop
};

/**
 * @brief Time Semantics for streaming operations
 */
enum class TimeSemantics {
    EVENT_TIME,      // Use event timestamp from data
    PROCESSING_TIME, // Use system processing time
    INGESTION_TIME   // Use data ingestion time
};

/**
 * @brief Window Specification
 */
struct WindowSpec {
    WindowType window_type;
    TimeSemantics time_semantics;
    std::string time_column;
    
    // Window parameters
    int64_t window_size_ms;        // Window size in milliseconds
    int64_t slide_size_ms;        // Slide size in milliseconds (for sliding/hop)
    int64_t session_gap_ms;      // Session gap in milliseconds
    
    // Window function parameters
    std::vector<std::string> partition_keys;
    std::vector<std::string> order_keys;
    std::vector<bool> order_directions; // true for ASC, false for DESC
    
    // Aggregation functions
    std::unordered_map<std::string, std::string> aggregations; // column -> function
    
    WindowSpec() : window_type(WindowType::TUMBLING), 
                   time_semantics(TimeSemantics::EVENT_TIME),
                   window_size_ms(60000), slide_size_ms(60000), session_gap_ms(300000) {}
};

/**
 * @brief Tumbling Window Operator
 * 
 * Implements Flink's TUMBLE window function:
 * TUMBLE(time_col, INTERVAL '1' MINUTE)
 * 
 * Creates fixed-size, non-overlapping windows.
 */
class TumblingWindowOperator : public operators::Operator {
public:
    TumblingWindowOperator(std::shared_ptr<operators::Operator> source,
                          const WindowSpec& window_spec);
    
    ~TumblingWindowOperator() override = default;
    
    // Operator interface
    arrow::Result<std::shared_ptr<arrow::Schema>> GetOutputSchema() const override;
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> GetNextBatch() override;
    bool HasNextBatch() const override;
    std::string ToString() const override;
    size_t EstimateCardinality() const override;
    
    /**
     * @brief Get window specification
     */
    const WindowSpec& GetWindowSpec() const { return window_spec_; }
    
    /**
     * @brief Set window specification
     */
    void SetWindowSpec(const WindowSpec& spec) { window_spec_ = spec; }
    
private:
    WindowSpec window_spec_;
    std::shared_ptr<operators::Operator> source_;
    
    // Window state
    std::unordered_map<std::string, std::vector<std::shared_ptr<arrow::RecordBatch>>> window_buffers_;
    int64_t current_window_start_;
    int64_t current_window_end_;
    bool window_initialized_;
    
    // Helper methods
    arrow::Result<int64_t> ExtractTimestamp(const arrow::RecordBatch& batch, 
                                           const std::string& time_column) const;
    std::string GetWindowKey(int64_t window_start) const;
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> 
        AggregateWindow(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches) const;
};

/**
 * @brief Sliding Window Operator
 * 
 * Implements Flink's HOP window function:
 * HOP(time_col, INTERVAL '30' SECOND, INTERVAL '1' MINUTE)
 * 
 * Creates fixed-size, overlapping windows.
 */
class SlidingWindowOperator : public operators::Operator {
public:
    SlidingWindowOperator(std::shared_ptr<operators::Operator> source,
                         const WindowSpec& window_spec);
    
    ~SlidingWindowOperator() override = default;
    
    // Operator interface
    arrow::Result<std::shared_ptr<arrow::Schema>> GetOutputSchema() const override;
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> GetNextBatch() override;
    bool HasNextBatch() const override;
    std::string ToString() const override;
    size_t EstimateCardinality() const override;
    
    /**
     * @brief Get window specification
     */
    const WindowSpec& GetWindowSpec() const { return window_spec_; }
    
private:
    WindowSpec window_spec_;
    std::shared_ptr<operators::Operator> source_;
    
    // Window state
    std::unordered_map<std::string, std::vector<std::shared_ptr<arrow::RecordBatch>>> window_buffers_;
    std::vector<int64_t> active_window_starts_;
    bool window_initialized_;
    
    // Helper methods
    arrow::Result<int64_t> ExtractTimestamp(const arrow::RecordBatch& batch, 
                                           const std::string& time_column) const;
    std::vector<int64_t> GetActiveWindows(int64_t timestamp) const;
    std::string GetWindowKey(int64_t window_start) const;
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> 
        AggregateWindow(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches) const;
};

/**
 * @brief Session Window Operator
 * 
 * Implements Flink's SESSION window function:
 * SESSION(time_col, INTERVAL '5' MINUTE)
 * 
 * Creates variable-size windows based on inactivity periods.
 */
class SessionWindowOperator : public operators::Operator {
public:
    SessionWindowOperator(std::shared_ptr<operators::Operator> source,
                         const WindowSpec& window_spec);
    
    ~SessionWindowOperator() override = default;
    
    // Operator interface
    arrow::Result<std::shared_ptr<arrow::Schema>> GetOutputSchema() const override;
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> GetNextBatch() override;
    bool HasNextBatch() const override;
    std::string ToString() const override;
    size_t EstimateCardinality() const override;
    
    /**
     * @brief Get window specification
     */
    const WindowSpec& GetWindowSpec() const { return window_spec_; }
    
private:
    WindowSpec window_spec_;
    std::shared_ptr<operators::Operator> source_;
    
    // Session state
    struct SessionState {
        int64_t session_start;
        int64_t last_activity;
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
        std::string partition_key;
    };
    
    std::unordered_map<std::string, SessionState> active_sessions_;
    std::vector<SessionState> completed_sessions_;
    bool session_initialized_;
    
    // Helper methods
    arrow::Result<int64_t> ExtractTimestamp(const arrow::RecordBatch& batch, 
                                           const std::string& time_column) const;
    std::string GetPartitionKey(const arrow::RecordBatch& batch) const;
    bool ShouldStartNewSession(const SessionState& session, int64_t timestamp) const;
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> 
        AggregateSession(const SessionState& session) const;
};

/**
 * @brief Sample By Operator (QuestDB)
 * 
 * Implements QuestDB's SAMPLE BY function:
 * SAMPLE BY 1h
 * 
 * Groups data by time intervals for time-series analysis.
 */
class SampleByOperator : public operators::Operator {
public:
    SampleByOperator(std::shared_ptr<operators::Operator> source,
                    const std::string& time_column,
                    const std::string& interval,
                    const std::vector<std::string>& group_by_columns = {},
                    const std::unordered_map<std::string, std::string>& aggregations = {});
    
    ~SampleByOperator() override = default;
    
    // Operator interface
    arrow::Result<std::shared_ptr<arrow::Schema>> GetOutputSchema() const override;
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> GetNextBatch() override;
    bool HasNextBatch() const override;
    std::string ToString() const override;
    size_t EstimateCardinality() const override;
    
    /**
     * @brief Get sample interval
     */
    const std::string& GetInterval() const { return interval_; }
    
    /**
     * @brief Set sample interval
     */
    void SetInterval(const std::string& interval) { interval_ = interval; }
    
private:
    std::shared_ptr<operators::Operator> source_;
    std::string time_column_;
    std::string interval_;
    std::vector<std::string> group_by_columns_;
    std::unordered_map<std::string, std::string> aggregations_;
    
    // Sample state
    std::unordered_map<std::string, std::vector<std::shared_ptr<arrow::RecordBatch>>> sample_buffers_;
    int64_t current_sample_start_;
    bool sample_initialized_;
    
    // Helper methods
    arrow::Result<int64_t> ExtractTimestamp(const arrow::RecordBatch& batch) const;
    int64_t GetSampleStart(int64_t timestamp) const;
    std::string GetSampleKey(int64_t sample_start) const;
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> 
        AggregateSample(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches) const;
};

/**
 * @brief Latest By Operator (QuestDB)
 * 
 * Implements QuestDB's LATEST BY function:
 * LATEST BY symbol
 * 
 * Returns the latest record for each group.
 */
class LatestByOperator : public operators::Operator {
public:
    LatestByOperator(std::shared_ptr<operators::Operator> source,
                    const std::string& time_column,
                    const std::vector<std::string>& group_by_columns);
    
    ~LatestByOperator() override = default;
    
    // Operator interface
    arrow::Result<std::shared_ptr<arrow::Schema>> GetOutputSchema() const override;
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> GetNextBatch() override;
    bool HasNextBatch() const override;
    std::string ToString() const override;
    size_t EstimateCardinality() const override;
    
    /**
     * @brief Get group by columns
     */
    const std::vector<std::string>& GetGroupByColumns() const { return group_by_columns_; }
    
private:
    std::shared_ptr<operators::Operator> source_;
    std::string time_column_;
    std::vector<std::string> group_by_columns_;
    
    // Latest state
    std::unordered_map<std::string, std::shared_ptr<arrow::RecordBatch>> latest_records_;
    std::vector<std::shared_ptr<arrow::RecordBatch>> result_batches_;
    size_t current_batch_index_;
    bool result_ready_;
    
    // Helper methods
    arrow::Result<int64_t> ExtractTimestamp(const arrow::RecordBatch& batch) const;
    std::string GetGroupKey(const arrow::RecordBatch& batch) const;
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> 
        CreateResultBatch() const;
};

/**
 * @brief Window Function Operator
 * 
 * Implements SQL window functions like ROW_NUMBER, RANK, LAG, LEAD, etc.
 * 
 * Example:
 * SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_time) as row_num
 * FROM orders
 */
class WindowFunctionOperator : public Operator {
public:
    /**
     * @brief Window function types
     */
    enum class FunctionType {
        ROW_NUMBER,
        RANK,
        DENSE_RANK,
        PERCENT_RANK,
        CUME_DIST,
        NTILE,
        LAG,
        LEAD,
        FIRST_VALUE,
        LAST_VALUE,
        NTH_VALUE
    };
    
    /**
     * @brief Window function specification
     */
    struct FunctionSpec {
        FunctionType function_type;
        std::string function_name;
        std::vector<std::string> arguments;
        std::vector<std::string> partition_by;
        std::vector<std::string> order_by;
        std::vector<bool> order_directions;
        int64_t frame_start;
        int64_t frame_end;
        bool frame_exclude_current_row;
        
        FunctionSpec() : function_type(FunctionType::ROW_NUMBER), 
                        frame_start(0), frame_end(0), frame_exclude_current_row(false) {}
    };
    
    WindowFunctionOperator(std::shared_ptr<Operator> source,
                          const std::vector<FunctionSpec>& function_specs);
    
    ~WindowFunctionOperator() override = default;
    
    // Operator interface
    arrow::Result<std::shared_ptr<arrow::Schema>> GetOutputSchema() const override;
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> GetNextBatch() override;
    bool HasNextBatch() const override;
    std::string ToString() const override;
    size_t EstimateCardinality() const override;
    
    /**
     * @brief Get function specifications
     */
    const std::vector<FunctionSpec>& GetFunctionSpecs() const { return function_specs_; }
    
private:
    std::shared_ptr<operators::Operator> source_;
    std::vector<FunctionSpec> function_specs_;
    
    // Window state
    std::vector<std::shared_ptr<arrow::RecordBatch>> input_batches_;
    std::vector<std::shared_ptr<arrow::RecordBatch>> result_batches_;
    size_t current_batch_index_;
    bool result_ready_;
    
    // Helper methods
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> 
        ProcessWindowFunctions(const std::shared_ptr<arrow::RecordBatch>& batch) const;
    arrow::Result<std::shared_ptr<arrow::Array>> 
        ComputeRowNumber(const std::shared_ptr<arrow::RecordBatch>& batch, 
                        const FunctionSpec& spec) const;
    arrow::Result<std::shared_ptr<arrow::Array>> 
        ComputeRank(const std::shared_ptr<arrow::RecordBatch>& batch, 
                   const FunctionSpec& spec) const;
    arrow::Result<std::shared_ptr<arrow::Array>> 
        ComputeLagLead(const std::shared_ptr<arrow::RecordBatch>& batch, 
                      const FunctionSpec& spec) const;
    std::vector<std::shared_ptr<arrow::RecordBatch>> 
        PartitionBatches(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches, 
                        const std::vector<std::string>& partition_keys) const;
};

} // namespace sql
} // namespace sabot_sql
