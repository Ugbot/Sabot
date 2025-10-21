#include "sabot_sql/streaming/morsel_plan_extensions.h"
#include <sstream>
#include <random>
#include <chrono>

namespace sabot_sql {
namespace streaming {

// ========== StreamingPlanBuilder Implementation ==========

StreamingPlanBuilder::StreamingPlanBuilder() = default;
StreamingPlanBuilder::~StreamingPlanBuilder() = default;

StreamingPlanBuilder& StreamingPlanBuilder::SetQueryId(const std::string& query_id) {
    plan_.query_id = query_id;
    return *this;
}

StreamingPlanBuilder& StreamingPlanBuilder::SetQueryText(const std::string& query_text) {
    plan_.query_text = query_text;
    return *this;
}

StreamingPlanBuilder& StreamingPlanBuilder::SetMaxParallelism(int max_parallelism) {
    plan_.max_parallelism = max_parallelism;
    return *this;
}

StreamingPlanBuilder& StreamingPlanBuilder::SetCheckpointInterval(const std::string& interval) {
    plan_.checkpoint_interval = interval;
    return *this;
}

StreamingPlanBuilder& StreamingPlanBuilder::SetStateBackend(const std::string& backend) {
    plan_.state_backend = backend;
    return *this;
}

StreamingPlanBuilder& StreamingPlanBuilder::SetTimerBackend(const std::string& backend) {
    plan_.timer_backend = backend;
    return *this;
}

StreamingPlanBuilder& StreamingPlanBuilder::AddKafkaSource(
    const std::string& topic,
    int32_t partition,
    const std::string& watermark_column
) {
    StreamingMorselPlan::StreamingSource source("kafka", topic, partition);
    source.watermark_column = watermark_column;
    source.watermark_expr = watermark_column;
    
    // Set default Kafka properties
    source.properties["bootstrap.servers"] = "localhost:9092";
    source.properties["group.id"] = "sabot_sql_" + plan_.query_id;
    source.properties["auto.offset.reset"] = "earliest";
    source.properties["enable.auto.commit"] = "false";
    
    plan_.AddStreamingSource(source);
    return *this;
}

StreamingPlanBuilder& StreamingPlanBuilder::AddFileSource(
    const std::string& file_path,
    const std::string& watermark_column
) {
    StreamingMorselPlan::StreamingSource source("file", file_path, 0);
    source.watermark_column = watermark_column;
    source.watermark_expr = watermark_column;
    
    // Set default file properties
    source.properties["file.path"] = file_path;
    source.properties["file.format"] = "parquet";
    source.properties["file.batch_size"] = "10000";
    
    plan_.AddStreamingSource(source);
    return *this;
}

StreamingPlanBuilder& StreamingPlanBuilder::AddTumbleWindow(
    const std::string& timestamp_column,
    const std::string& window_size,
    const std::vector<std::string>& key_columns
) {
    StreamingMorselPlan::WindowConfig config("TUMBLE", window_size, timestamp_column);
    config.key_columns = key_columns;
    plan_.AddWindowConfig(config);
    return *this;
}

StreamingPlanBuilder& StreamingPlanBuilder::AddHopWindow(
    const std::string& timestamp_column,
    const std::string& window_size,
    const std::string& window_slide,
    const std::vector<std::string>& key_columns
) {
    StreamingMorselPlan::WindowConfig config("HOP", window_size, timestamp_column);
    config.window_slide = window_slide;
    config.key_columns = key_columns;
    plan_.AddWindowConfig(config);
    return *this;
}

StreamingPlanBuilder& StreamingPlanBuilder::AddSessionWindow(
    const std::string& timestamp_column,
    const std::string& window_gap,
    const std::vector<std::string>& key_columns
) {
    StreamingMorselPlan::WindowConfig config("SESSION", "0", timestamp_column);
    config.window_gap = window_gap;
    config.key_columns = key_columns;
    plan_.AddWindowConfig(config);
    return *this;
}

StreamingPlanBuilder& StreamingPlanBuilder::AddWindowState(
    const std::string& operator_id,
    const std::string& state_table_name,
    bool is_raft_replicated
) {
    StreamingMorselPlan::StateConfig config(operator_id, state_table_name, is_raft_replicated);
    config.state_type = "window";
    plan_.AddStateConfig(config);
    return *this;
}

StreamingPlanBuilder& StreamingPlanBuilder::AddJoinBufferState(
    const std::string& operator_id,
    const std::string& state_table_name,
    bool is_raft_replicated
) {
    StreamingMorselPlan::StateConfig config(operator_id, state_table_name, is_raft_replicated);
    config.state_type = "join_buffer";
    plan_.AddStateConfig(config);
    return *this;
}

StreamingPlanBuilder& StreamingPlanBuilder::AddDimensionTable(
    const std::string& table_name,
    const std::string& alias,
    bool is_raft_replicated
) {
    StreamingMorselPlan::DimensionTableConfig config(table_name, alias, is_raft_replicated);
    plan_.AddDimensionTable(config);
    return *this;
}

StreamingPlanBuilder& StreamingPlanBuilder::AddKafkaPartitionSource(
    const std::string& operator_id,
    const std::string& topic,
    int32_t partition
) {
    StreamingMorselPlan::OperatorDescriptor op("KafkaPartitionSource", operator_id);
    op.parameters["topic"] = topic;
    op.parameters["partition"] = std::to_string(partition);
    op.parameters["batch_size"] = "10000";
    op.parameters["watermark_column"] = "timestamp";
    
    plan_.AddOperator(op);
    return *this;
}

StreamingPlanBuilder& StreamingPlanBuilder::AddWindowAggregateOperator(
    const std::string& operator_id,
    const std::string& window_type,
    const std::string& window_size,
    const std::string& timestamp_column
) {
    StreamingMorselPlan::OperatorDescriptor op("WindowAggregateOperator", operator_id);
    op.parameters["window_type"] = window_type;
    op.parameters["window_size"] = window_size;
    op.parameters["timestamp_column"] = timestamp_column;
    op.parameters["state_table"] = GenerateStateTableName(operator_id);
    op.is_stateful = true;
    
    plan_.AddOperator(op);
    return *this;
}

StreamingPlanBuilder& StreamingPlanBuilder::AddBroadcastJoinOperator(
    const std::string& operator_id,
    const std::string& dimension_table,
    const std::vector<std::string>& join_keys
) {
    StreamingMorselPlan::OperatorDescriptor op("BroadcastJoinOperator", operator_id);
    op.parameters["dimension_table"] = dimension_table;
    op.parameters["join_type"] = "LEFT";
    
    // Add join keys as parameters
    for (size_t i = 0; i < join_keys.size(); ++i) {
        op.parameters["join_key_" + std::to_string(i)] = join_keys[i];
    }
    
    op.is_broadcast = true;
    plan_.AddOperator(op);
    return *this;
}

StreamingPlanBuilder& StreamingPlanBuilder::AddCheckpointOperator(
    const std::string& operator_id,
    const std::string& checkpoint_interval
) {
    StreamingMorselPlan::OperatorDescriptor op("CheckpointOperator", operator_id);
    op.parameters["checkpoint_interval"] = checkpoint_interval;
    op.parameters["timeout_ms"] = "30000";
    
    plan_.AddOperator(op);
    return *this;
}

StreamingPlanBuilder& StreamingPlanBuilder::AddKafkaOutput(
    const std::string& topic,
    const std::unordered_map<std::string, std::string>& properties
) {
    StreamingMorselPlan::OutputConfig config("kafka", topic);
    config.properties = properties;
    
    // Set default Kafka output properties
    config.properties["bootstrap.servers"] = "localhost:9092";
    config.properties["acks"] = "all";
    config.properties["retries"] = "3";
    
    plan_.output_configs.push_back(config);
    return *this;
}

StreamingPlanBuilder& StreamingPlanBuilder::AddFileOutput(
    const std::string& file_path,
    const std::unordered_map<std::string, std::string>& properties
) {
    StreamingMorselPlan::OutputConfig config("file", file_path);
    config.properties = properties;
    
    // Set default file output properties
    config.properties["file.format"] = "parquet";
    config.properties["file.compression"] = "snappy";
    
    plan_.output_configs.push_back(config);
    return *this;
}

arrow::Result<StreamingMorselPlan> StreamingPlanBuilder::Build() {
    // Validate the plan
    ARROW_RETURN_NOT_OK(plan_.Validate());
    
    // Set default values if not specified
    if (plan_.query_id.empty()) {
        plan_.query_id = "query_" + std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count());
    }
    
    if (plan_.checkpoint_interval.empty()) {
        plan_.checkpoint_interval = "60s";
    }
    
    if (plan_.max_parallelism <= 0) {
        plan_.max_parallelism = 1;
    }
    
    // Configure checkpoint if streaming
    if (plan_.is_streaming) {
        plan_.checkpoint_config.enabled = true;
        plan_.checkpoint_config.interval = plan_.checkpoint_interval;
        plan_.checkpoint_config.timeout_ms = 30000;
        
        // Add all stateful operators as checkpoint participants
        for (const auto& op : plan_.operators) {
            if (op.is_stateful) {
                plan_.checkpoint_config.participant_operators.push_back(op.operator_id);
            }
        }
    }
    
    return plan_;
}

void StreamingPlanBuilder::Reset() {
    plan_ = StreamingMorselPlan();
}

std::string StreamingPlanBuilder::GenerateOperatorId(const std::string& prefix) {
    static std::random_device rd;
    static std::mt19937 gen(rd());
    static std::uniform_int_distribution<> dis(1000, 9999);
    
    return prefix + "_" + std::to_string(dis(gen));
}

std::string StreamingPlanBuilder::GenerateStateTableName(const std::string& operator_id) {
    return "state_" + operator_id;
}

} // namespace streaming
} // namespace sabot_sql
