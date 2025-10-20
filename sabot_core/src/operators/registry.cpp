#include "sabot/operators/registry.h"
#include <algorithm>
#include <chrono>

namespace sabot {
namespace operators {

OperatorRegistry::OperatorRegistry() 
    : lookup_count_(0), total_lookup_ns_(0) {
}

void OperatorRegistry::Register(
    const std::string& name,
    OperatorType type,
    const OperatorMetadata& metadata
) {
    operator_map_[name] = type;
    
    // Store metadata
    OperatorMetadata meta = metadata;
    meta.name = name;
    meta.type = type;
    metadata_map_[name] = meta;
}

OperatorType OperatorRegistry::Lookup(const std::string& name) const {
    #ifdef SABOT_PROFILE_REGISTRY
    auto start = std::chrono::high_resolution_clock::now();
    #endif
    
    auto it = operator_map_.find(name);
    
    #ifdef SABOT_PROFILE_REGISTRY
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
    lookup_count_++;
    total_lookup_ns_ += duration.count();
    #endif
    
    if (it != operator_map_.end()) {
        return it->second;
    }
    
    return OperatorType::CUSTOM;
}

bool OperatorRegistry::HasOperator(const std::string& name) const {
    return operator_map_.find(name) != operator_map_.end();
}

const OperatorMetadata* OperatorRegistry::GetMetadata(const std::string& name) const {
    auto it = metadata_map_.find(name);
    if (it != metadata_map_.end()) {
        return &it->second;
    }
    return nullptr;
}

std::vector<std::string> OperatorRegistry::ListOperators() const {
    std::vector<std::string> names;
    names.reserve(operator_map_.size());
    
    for (const auto& pair : operator_map_) {
        names.push_back(pair.first);
    }
    
    // Sort for consistent ordering
    std::sort(names.begin(), names.end());
    
    return names;
}

OperatorRegistry::Stats OperatorRegistry::GetStats() const {
    Stats stats;
    stats.num_operators = operator_map_.size();
    stats.num_lookups = lookup_count_;
    
    if (lookup_count_ > 0) {
        stats.avg_lookup_ns = static_cast<double>(total_lookup_ns_) / lookup_count_;
    }
    
    return stats;
}

void OperatorRegistry::Clear() {
    operator_map_.clear();
    metadata_map_.clear();
    lookup_count_ = 0;
    total_lookup_ns_ = 0;
}

std::unique_ptr<OperatorRegistry> CreateDefaultRegistry() {
    auto registry = std::make_unique<OperatorRegistry>();
    
    // Register transform operators
    OperatorMetadata filter_meta;
    filter_meta.description = "Filter rows based on predicate";
    filter_meta.performance_hint = "10-500M rows/sec (SIMD)";
    filter_meta.is_stateful = false;
    filter_meta.requires_shuffle = false;
    registry->Register("filter", OperatorType::FILTER, filter_meta);
    
    OperatorMetadata map_meta;
    map_meta.description = "Transform rows with function";
    map_meta.performance_hint = "10-100M rows/sec";
    map_meta.is_stateful = false;
    map_meta.requires_shuffle = false;
    registry->Register("map", OperatorType::MAP, map_meta);
    
    OperatorMetadata select_meta;
    select_meta.description = "Project columns (zero-copy)";
    select_meta.performance_hint = "50-1000M rows/sec";
    select_meta.is_stateful = false;
    select_meta.requires_shuffle = false;
    registry->Register("select", OperatorType::SELECT, select_meta);
    
    OperatorMetadata flat_map_meta;
    flat_map_meta.description = "1-to-N row expansion";
    flat_map_meta.performance_hint = "5-50M rows/sec";
    flat_map_meta.is_stateful = false;
    flat_map_meta.requires_shuffle = false;
    registry->Register("flat_map", OperatorType::FLAT_MAP, flat_map_meta);
    
    OperatorMetadata union_meta;
    union_meta.description = "Merge multiple streams";
    union_meta.performance_hint = "5-50M rows/sec";
    union_meta.is_stateful = false;
    union_meta.requires_shuffle = false;
    registry->Register("union", OperatorType::UNION, union_meta);
    
    // Register aggregation operators
    OperatorMetadata group_by_meta;
    group_by_meta.description = "Group by keys and aggregate";
    group_by_meta.performance_hint = "5-100M rows/sec";
    group_by_meta.is_stateful = true;
    group_by_meta.requires_shuffle = true;
    registry->Register("group_by", OperatorType::GROUP_BY, group_by_meta);
    
    OperatorMetadata aggregate_meta;
    aggregate_meta.description = "Global aggregation";
    aggregate_meta.performance_hint = "2-50M rows/sec";
    aggregate_meta.is_stateful = true;
    aggregate_meta.requires_shuffle = false;
    registry->Register("aggregate", OperatorType::AGGREGATE, aggregate_meta);
    
    OperatorMetadata distinct_meta;
    distinct_meta.description = "Deduplication";
    distinct_meta.performance_hint = "1-10M rows/sec";
    distinct_meta.is_stateful = true;
    distinct_meta.requires_shuffle = true;
    registry->Register("distinct", OperatorType::DISTINCT, distinct_meta);
    
    // Register join operators
    OperatorMetadata hash_join_meta;
    hash_join_meta.description = "Hash join on keys";
    hash_join_meta.performance_hint = "104M rows/sec";
    hash_join_meta.is_stateful = true;
    hash_join_meta.requires_shuffle = true;
    registry->Register("hash_join", OperatorType::HASH_JOIN, hash_join_meta);
    
    OperatorMetadata interval_join_meta;
    interval_join_meta.description = "Time-based interval join";
    interval_join_meta.performance_hint = "1500M rows/sec";
    interval_join_meta.is_stateful = true;
    interval_join_meta.requires_shuffle = false;
    registry->Register("interval_join", OperatorType::INTERVAL_JOIN, interval_join_meta);
    
    OperatorMetadata asof_join_meta;
    asof_join_meta.description = "Point-in-time join";
    asof_join_meta.performance_hint = "22,221M rows/sec";
    asof_join_meta.is_stateful = true;
    asof_join_meta.requires_shuffle = false;
    registry->Register("asof_join", OperatorType::ASOF_JOIN, asof_join_meta);
    
    // Register window operator
    OperatorMetadata window_meta;
    window_meta.description = "Windowed aggregations";
    window_meta.performance_hint = "Variable based on window type";
    window_meta.is_stateful = true;
    window_meta.requires_shuffle = false;
    registry->Register("window", OperatorType::WINDOW, window_meta);
    
    return registry;
}

// Global singleton
static std::unique_ptr<OperatorRegistry> global_registry = nullptr;

OperatorRegistry& GetGlobalRegistry() {
    if (!global_registry) {
        global_registry = CreateDefaultRegistry();
    }
    return *global_registry;
}

} // namespace operators
} // namespace sabot

