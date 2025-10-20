#include "sabot/shuffle/coordinator.h"
#include <chrono>
#include <stdexcept>

namespace sabot {
namespace shuffle {

ShuffleCoordinator::ShuffleCoordinator() {
}

bool ShuffleCoordinator::Initialize(const ShuffleConfig& config) {
    if (config.num_partitions <= 0) {
        return false;
    }
    
    config_ = config;
    assignments_.clear();
    
    // Reset statistics
    partitions_assigned_.store(0);
    lookups_performed_.store(0);
    partitions_computed_.store(0);
    total_lookup_ns_.store(0);
    
    return true;
}

void ShuffleCoordinator::AssignPartition(
    int32_t partition_id,
    const std::string& worker_id,
    const std::string& host,
    int32_t port
) {
    PartitionAssignment assignment;
    assignment.partition_id = partition_id;
    assignment.worker_id = worker_id;
    assignment.host = host;
    assignment.port = port;
    
    assignments_[partition_id] = assignment;
    partitions_assigned_.fetch_add(1, std::memory_order_relaxed);
}

const PartitionAssignment* ShuffleCoordinator::GetAssignment(int32_t partition_id) const {
    auto start = std::chrono::high_resolution_clock::now();
    
    auto it = assignments_.find(partition_id);
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
    
    lookups_performed_.fetch_add(1, std::memory_order_relaxed);
    total_lookup_ns_.fetch_add(duration.count(), std::memory_order_relaxed);
    
    if (it != assignments_.end()) {
        return &it->second;
    }
    
    return nullptr;
}

int32_t ShuffleCoordinator::ComputePartition(uint64_t hash) const {
    partitions_computed_.fetch_add(1, std::memory_order_relaxed);
    return HashPartition(hash);
}

std::vector<PartitionAssignment> ShuffleCoordinator::GetAllAssignments() const {
    std::vector<PartitionAssignment> result;
    result.reserve(assignments_.size());
    
    for (const auto& pair : assignments_) {
        result.push_back(pair.second);
    }
    
    return result;
}

ShuffleCoordinator::Stats ShuffleCoordinator::GetStats() const {
    Stats stats;
    stats.partitions_assigned = partitions_assigned_.load(std::memory_order_relaxed);
    stats.lookups_performed = lookups_performed_.load(std::memory_order_relaxed);
    stats.partitions_computed = partitions_computed_.load(std::memory_order_relaxed);
    
    uint64_t total_ns = total_lookup_ns_.load(std::memory_order_relaxed);
    if (stats.lookups_performed > 0) {
        stats.avg_lookup_ns = static_cast<double>(total_ns) / stats.lookups_performed;
    }
    
    return stats;
}

void ShuffleCoordinator::Reset() {
    assignments_.clear();
    partitions_assigned_.store(0);
    lookups_performed_.store(0);
    partitions_computed_.store(0);
    total_lookup_ns_.store(0);
}

} // namespace shuffle
} // namespace sabot

