#pragma once

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <cstdint>
#include <atomic>

namespace sabot {
namespace shuffle {

/**
 * @brief Shuffle type enumeration
 */
enum class ShuffleType {
    HASH,           // Hash-based partitioning
    BROADCAST,      // Broadcast to all workers
    RANGE,          // Range-based partitioning
    REBALANCE,      // Round-robin rebalancing
    CUSTOM          // Custom partitioning
};

/**
 * @brief Partition assignment
 */
struct PartitionAssignment {
    int32_t partition_id;
    std::string worker_id;
    std::string host;
    int32_t port;
    
    PartitionAssignment() : partition_id(-1), port(0) {}
};

/**
 * @brief Shuffle configuration
 */
struct ShuffleConfig {
    ShuffleType type;
    int32_t num_partitions;
    std::vector<std::string> partition_keys;
    bool compression_enabled;
    int32_t batch_size;
    
    ShuffleConfig() 
        : type(ShuffleType::HASH), 
          num_partitions(0),
          compression_enabled(false),
          batch_size(10000) {}
};

/**
 * @brief High-performance shuffle coordinator
 * 
 * Manages partition assignment and routing for distributed shuffles.
 * 
 * Performance targets:
 * - Partition assignment: <1μs (vs ~100μs in Python)
 * - Hash computation: <10ns per row
 * - Coordination overhead: <1μs per batch
 * 
 * Expected: 100x faster than Python coordinator
 */
class ShuffleCoordinator {
public:
    ShuffleCoordinator();
    ~ShuffleCoordinator() = default;
    
    /**
     * @brief Initialize shuffle with configuration
     * 
     * @param config Shuffle configuration
     * @return true if successful
     * 
     * Performance: <10μs
     */
    bool Initialize(const ShuffleConfig& config);
    
    /**
     * @brief Assign partition to worker
     * 
     * @param partition_id Partition ID
     * @param worker_id Worker ID
     * @param host Worker host
     * @param port Worker port
     * 
     * Performance: <1μs (target)
     */
    void AssignPartition(
        int32_t partition_id,
        const std::string& worker_id,
        const std::string& host,
        int32_t port
    );
    
    /**
     * @brief Get partition assignment
     * 
     * @param partition_id Partition ID
     * @return Partition assignment or nullptr
     * 
     * Performance: <100ns (hash lookup)
     */
    const PartitionAssignment* GetAssignment(int32_t partition_id) const;
    
    /**
     * @brief Compute partition for hash value
     * 
     * @param hash Hash value
     * @return Partition ID
     * 
     * Performance: <10ns (modulo operation)
     */
    int32_t ComputePartition(uint64_t hash) const;
    
    /**
     * @brief Get all partition assignments
     */
    std::vector<PartitionAssignment> GetAllAssignments() const;
    
    /**
     * @brief Get shuffle statistics
     */
    struct Stats {
        int64_t partitions_assigned = 0;
        int64_t lookups_performed = 0;
        int64_t partitions_computed = 0;
        double avg_lookup_ns = 0.0;
    };
    
    Stats GetStats() const;
    
    /**
     * @brief Reset coordinator
     */
    void Reset();
    
private:
    ShuffleConfig config_;
    std::unordered_map<int32_t, PartitionAssignment> assignments_;
    
    // Statistics (atomic for thread-safety)
    mutable std::atomic<int64_t> partitions_assigned_{0};
    mutable std::atomic<int64_t> lookups_performed_{0};
    mutable std::atomic<int64_t> partitions_computed_{0};
    mutable std::atomic<uint64_t> total_lookup_ns_{0};
    
    /**
     * @brief Fast hash-based partitioning (inline for speed)
     */
    inline int32_t HashPartition(uint64_t hash) const {
        // Fast modulo using bit masking (if power of 2)
        if (config_.num_partitions > 0 && 
            (config_.num_partitions & (config_.num_partitions - 1)) == 0) {
            // Power of 2 - use bit mask
            return hash & (config_.num_partitions - 1);
        }
        // Regular modulo
        return hash % config_.num_partitions;
    }
};

} // namespace shuffle
} // namespace sabot

