#pragma once

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <arrow/api.h>

namespace sabot_sql {
namespace sql {

// Common types used across SabotSQL components

struct LogicalPlan {
    std::shared_ptr<void> root_operator;
    // SQL text (for heuristic hint extraction until full parser integration)
    std::string processed_sql;
    bool has_joins = false;
    bool has_asof_joins = false;
    bool has_aggregates = false;
    bool has_subqueries = false;
    bool has_ctes = false;
    bool has_windows = false;
    // Optional hints captured during binder rewrites
    bool has_sample_by = false;
    bool has_latest_by = false;
    std::string window_interval; // e.g., "1h", "5m"
    std::vector<std::string> join_key_columns;
    std::string join_timestamp_column;
};

struct MorselPlan {
    std::string plan_type = "morsel_execution";
    std::shared_ptr<void> root_operator;
    std::vector<std::shared_ptr<void>> operators;
    // High-level operator pipeline description for Sabot executor
    std::vector<std::string> operator_pipeline;
    // Detailed operator descriptors with parameters for Sabot executor
    struct OperatorDescriptor {
        std::string type; // e.g., "ShuffleRepartitionByKeys", "HashJoin", "AsOfMergeProbe"
        std::unordered_map<std::string, std::string> params; // stringified parameters
        bool is_stateful = false; // Requires state backend
        bool is_broadcast = false; // Broadcast to all agents (dimension tables)
    };
    std::vector<OperatorDescriptor> operator_descriptors;
    std::shared_ptr<arrow::Schema> output_schema;
    size_t estimated_rows = 0;
    bool has_joins = false;
    bool has_asof_joins = false;
    bool has_aggregates = false;
    bool has_windows = false;
    bool is_streaming = false;
    // Execution hints for Sabot executor
    std::vector<std::string> join_key_columns;
    std::string join_timestamp_column;
    std::string window_interval; // normalized interval string
    // Streaming config
    int max_parallelism = 1; // For Kafka partition sources
    std::string checkpoint_interval; // e.g., "60s"
    std::string state_backend = "marbledb"; // marbledb (default, with RAFT) | tonbo (pluggable)
    std::string timer_backend = "rocksdb"; // rocksdb for watermarks/timers
    bool use_raft_for_dimensions = true; // MarbleDB: use RAFT for dimension table broadcast
};

} // namespace sql
} // namespace sabot_sql
