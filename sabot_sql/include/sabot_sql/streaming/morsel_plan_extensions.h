#pragma once

#include <arrow/api.h>
#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <sstream>

namespace sabot_sql {
namespace streaming {

/**
 * Streaming extensions to MorselPlan
 * 
 * Extends the base MorselPlan with streaming-specific metadata and operator descriptors.
 * Provides the bridge between SQL planning and streaming execution.
 */
struct StreamingMorselPlan {
    // ========== Base MorselPlan Fields ==========
    
    // Execution metadata
    std::string query_id;
    std::string query_text;
    bool is_streaming = false;
    
    // Execution hints for Sabot executor
    std::vector<std::string> join_key_columns;
    std::string join_timestamp_column;
    std::string window_interval; // normalized interval string
    
    // Streaming config
    int max_parallelism = 1; // For Kafka partition sources
    std::string checkpoint_interval; // e.g., "60s"
    std::string state_backend = "marbledb"; // marbledb (default, with RAFT) | tonbo (pluggable)
    std::string timer_backend = "marbledb"; // marbledb for watermarks/timers
    bool use_raft_for_dimensions = true; // MarbleDB: use RAFT for dimension table broadcast
    
    // ========== Streaming-Specific Fields ==========
    
    // Source configuration
    struct StreamingSource {
        std::string source_type; // "kafka", "pulsar", "kinesis", "file"
        std::string topic_name;
        std::string topic; // alias for topic_name
        int32_t partition_id;
        int32_t partition; // alias for partition_id
        int32_t max_parallelism;
        int32_t batch_size;
        std::string watermark_column;
        std::string watermark_expr;
        std::unordered_map<std::string, std::string> properties;
        
        StreamingSource() = default;
        StreamingSource(const std::string& type, const std::string& topic_name, int32_t partition)
            : source_type(type), topic_name(topic_name), topic(topic_name), partition_id(partition), partition(partition), max_parallelism(1), batch_size(10000) {}
    };
    
    std::vector<StreamingSource> streaming_sources;
    
    // Window configuration
    struct WindowConfig {
        std::string window_type; // "TUMBLE", "HOP", "SESSION"
        std::string window_size; // e.g., "1h", "30m", "1000ms"
        std::string window_slide; // for HOP windows
        std::string window_gap; // for SESSION windows
        std::string timestamp_column;
        std::vector<std::string> key_columns;
        
        WindowConfig() = default;
        WindowConfig(const std::string& type, const std::string& size, const std::string& ts_col)
            : window_type(type), window_size(size), timestamp_column(ts_col) {}
    };
    
    std::vector<WindowConfig> window_configs;
    
    // State configuration
    struct StateConfig {
        std::string operator_id;
        std::string state_table_name;
        bool is_raft_replicated;
        std::string state_type; // "window", "join_buffer", "deduplication"
        std::unordered_map<std::string, std::string> state_properties;
        
        StateConfig() = default;
        StateConfig(const std::string& op_id, const std::string& table_name, bool raft)
            : operator_id(op_id), state_table_name(table_name), is_raft_replicated(raft) {}
    };
    
    std::vector<StateConfig> state_configs;
    
    // Dimension table configuration
    struct DimensionTableConfig {
        std::string table_name;
        std::string alias;
        bool is_raft_replicated;
        std::vector<std::string> join_keys;
        std::string join_type; // "INNER", "LEFT", "RIGHT", "FULL"
        
        DimensionTableConfig() = default;
        DimensionTableConfig(const std::string& name, const std::string& alias_name, bool raft)
            : table_name(name), alias(alias_name), is_raft_replicated(raft) {}
    };
    
    std::vector<DimensionTableConfig> dimension_tables;
    
    // Checkpoint configuration
    struct CheckpointConfig {
        bool enabled;
        std::string interval;
        int64_t timeout_ms;
        std::vector<std::string> participant_operators;
        
        CheckpointConfig() = default;
        CheckpointConfig(bool en, const std::string& intv, int64_t timeout)
            : enabled(en), interval(intv), timeout_ms(timeout) {}
    };
    
    CheckpointConfig checkpoint_config;
    
    // Output configuration
    struct OutputConfig {
        std::string output_type; // "kafka", "file", "memory"
        std::string destination;
        std::unordered_map<std::string, std::string> properties;
        
        OutputConfig() = default;
        OutputConfig(const std::string& type, const std::string& dest)
            : output_type(type), destination(dest) {}
    };
    
    std::vector<OutputConfig> output_configs;
    
    // ========== Operator Descriptors ==========
    
    struct OperatorDescriptor {
        std::string operator_type;
        std::string operator_id;
        std::unordered_map<std::string, std::string> parameters;
        std::vector<std::string> input_operators;
        std::vector<std::string> output_operators;
        bool is_stateful = false;
        bool is_broadcast = false;
        bool requires_shuffle = false;
        
        OperatorDescriptor() = default;
        OperatorDescriptor(const std::string& type, const std::string& id)
            : operator_type(type), operator_id(id) {}
    };
    
    std::vector<OperatorDescriptor> operators;
    
    // ========== Methods ==========
    
    /**
     * Add a streaming source
     */
    void AddStreamingSource(const StreamingSource& source) {
        streaming_sources.push_back(source);
        is_streaming = true;
    }
    
    /**
     * Add a window configuration
     */
    void AddWindowConfig(const WindowConfig& config) {
        window_configs.push_back(config);
    }
    
    /**
     * Add a state configuration
     */
    void AddStateConfig(const StateConfig& config) {
        state_configs.push_back(config);
    }
    
    /**
     * Add a dimension table
     */
    void AddDimensionTable(const DimensionTableConfig& config) {
        dimension_tables.push_back(config);
    }
    
    /**
     * Add an operator descriptor
     */
    void AddOperator(const OperatorDescriptor& op) {
        operators.push_back(op);
    }
    
    /**
     * Get operator by ID
     */
    OperatorDescriptor* GetOperator(const std::string& operator_id) {
        for (auto& op : operators) {
            if (op.operator_id == operator_id) {
                return &op;
            }
        }
        return nullptr;
    }
    
    /**
     * Get streaming source by topic
     */
    StreamingSource* GetStreamingSource(const std::string& topic_name) {
        for (auto& source : streaming_sources) {
            if (source.topic_name == topic_name) {
                return &source;
            }
        }
        return nullptr;
    }
    
    /**
     * Get state configuration by operator ID
     */
    StateConfig* GetStateConfig(const std::string& operator_id) {
        for (auto& config : state_configs) {
            if (config.operator_id == operator_id) {
                return &config;
            }
        }
        return nullptr;
    }
    
    /**
     * Get dimension table by name
     */
    DimensionTableConfig* GetDimensionTable(const std::string& table_name) {
        for (auto& dim : dimension_tables) {
            if (dim.table_name == table_name) {
                return &dim;
            }
        }
        return nullptr;
    }
    
    /**
     * Validate the streaming plan
     */
    arrow::Status Validate() const {
        // Check if streaming plan has at least one source
        if (is_streaming && streaming_sources.empty()) {
            return arrow::Status::Invalid("Streaming plan must have at least one streaming source");
        }
        
        // Check if window configs have required fields
        for (const auto& window : window_configs) {
            if (window.window_type.empty()) {
                return arrow::Status::Invalid("Window type cannot be empty");
            }
            if (window.window_size.empty()) {
                return arrow::Status::Invalid("Window size cannot be empty");
            }
            if (window.timestamp_column.empty()) {
                return arrow::Status::Invalid("Timestamp column cannot be empty");
            }
        }
        
        // Check if state configs have required fields
        for (const auto& state : state_configs) {
            if (state.operator_id.empty()) {
                return arrow::Status::Invalid("State operator ID cannot be empty");
            }
            if (state.state_table_name.empty()) {
                return arrow::Status::Invalid("State table name cannot be empty");
            }
        }
        
        // Check if operators have required fields
        for (const auto& op : operators) {
            if (op.operator_type.empty()) {
                return arrow::Status::Invalid("Operator type cannot be empty");
            }
            if (op.operator_id.empty()) {
                return arrow::Status::Invalid("Operator ID cannot be empty");
            }
        }
        
        return arrow::Status::OK();
    }
    
    /**
     * Get plan summary
     */
    std::string GetSummary() const {
        std::ostringstream oss;
        oss << "StreamingMorselPlan Summary:\n";
        oss << "  Query ID: " << query_id << "\n";
        oss << "  Is Streaming: " << (is_streaming ? "yes" : "no") << "\n";
        oss << "  Max Parallelism: " << max_parallelism << "\n";
        oss << "  Checkpoint Interval: " << checkpoint_interval << "\n";
        oss << "  State Backend: " << state_backend << "\n";
        oss << "  Timer Backend: " << timer_backend << "\n";
        oss << "  Streaming Sources: " << streaming_sources.size() << "\n";
        oss << "  Window Configs: " << window_configs.size() << "\n";
        oss << "  State Configs: " << state_configs.size() << "\n";
        oss << "  Dimension Tables: " << dimension_tables.size() << "\n";
        oss << "  Operators: " << operators.size() << "\n";
        oss << "  Output Configs: " << output_configs.size() << "\n";
        return oss.str();
    }
};

/**
 * Streaming plan builder
 * 
 * Helper class for building streaming MorselPlans.
 */
class StreamingPlanBuilder {
public:
    StreamingPlanBuilder();
    ~StreamingPlanBuilder();
    
    /**
     * Set basic plan information
     */
    StreamingPlanBuilder& SetQueryId(const std::string& query_id);
    StreamingPlanBuilder& SetQueryText(const std::string& query_text);
    StreamingPlanBuilder& SetMaxParallelism(int max_parallelism);
    StreamingPlanBuilder& SetCheckpointInterval(const std::string& interval);
    StreamingPlanBuilder& SetStateBackend(const std::string& backend);
    StreamingPlanBuilder& SetTimerBackend(const std::string& backend);
    
    /**
     * Add streaming source
     */
    StreamingPlanBuilder& AddKafkaSource(
        const std::string& topic,
        int32_t partition,
        const std::string& watermark_column = "timestamp"
    );
    
    StreamingPlanBuilder& AddFileSource(
        const std::string& file_path,
        const std::string& watermark_column = "timestamp"
    );
    
    /**
     * Add window configuration
     */
    StreamingPlanBuilder& AddTumbleWindow(
        const std::string& timestamp_column,
        const std::string& window_size,
        const std::vector<std::string>& key_columns = {}
    );
    
    StreamingPlanBuilder& AddHopWindow(
        const std::string& timestamp_column,
        const std::string& window_size,
        const std::string& window_slide,
        const std::vector<std::string>& key_columns = {}
    );
    
    StreamingPlanBuilder& AddSessionWindow(
        const std::string& timestamp_column,
        const std::string& window_gap,
        const std::vector<std::string>& key_columns = {}
    );
    
    /**
     * Add state configuration
     */
    StreamingPlanBuilder& AddWindowState(
        const std::string& operator_id,
        const std::string& state_table_name,
        bool is_raft_replicated = false
    );
    
    StreamingPlanBuilder& AddJoinBufferState(
        const std::string& operator_id,
        const std::string& state_table_name,
        bool is_raft_replicated = false
    );
    
    /**
     * Add dimension table
     */
    StreamingPlanBuilder& AddDimensionTable(
        const std::string& table_name,
        const std::string& alias,
        bool is_raft_replicated = true
    );
    
    /**
     * Add operator
     */
    StreamingPlanBuilder& AddKafkaPartitionSource(
        const std::string& operator_id,
        const std::string& topic,
        int32_t partition
    );
    
    StreamingPlanBuilder& AddWindowAggregateOperator(
        const std::string& operator_id,
        const std::string& window_type,
        const std::string& window_size,
        const std::string& timestamp_column
    );
    
    StreamingPlanBuilder& AddBroadcastJoinOperator(
        const std::string& operator_id,
        const std::string& dimension_table,
        const std::vector<std::string>& join_keys
    );
    
    StreamingPlanBuilder& AddCheckpointOperator(
        const std::string& operator_id,
        const std::string& checkpoint_interval
    );
    
    /**
     * Add output configuration
     */
    StreamingPlanBuilder& AddKafkaOutput(
        const std::string& topic,
        const std::unordered_map<std::string, std::string>& properties = {}
    );
    
    StreamingPlanBuilder& AddFileOutput(
        const std::string& file_path,
        const std::unordered_map<std::string, std::string>& properties = {}
    );
    
    /**
     * Build the final plan
     */
    arrow::Result<StreamingMorselPlan> Build();
    
    /**
     * Reset the builder
     */
    void Reset();
    
private:
    StreamingMorselPlan plan_;
    
    // Helper methods
    std::string GenerateOperatorId(const std::string& prefix);
    std::string GenerateStateTableName(const std::string& operator_id);
};

} // namespace streaming
} // namespace sabot_sql
