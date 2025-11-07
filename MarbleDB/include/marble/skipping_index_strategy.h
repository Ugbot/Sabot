/**
 * SkippingIndexStrategy - Block-level statistics for query pruning
 *
 * Optimizes analytical queries by storing min/max/null statistics for data blocks.
 * Allows queries to skip entire blocks that cannot contain matching data.
 * Inspired by ClickHouse skipping indexes and Parquet row group statistics.
 *
 * Performance characteristics:
 * - Space: ~100 bytes per block per column (for min/max/null count)
 * - Time: O(b) where b = number of blocks (typically << n rows)
 * - Effectiveness: Can prune 90%+ of blocks for range queries
 * - Best for: Range scans, time series, sorted data
 */

#pragma once

#include "marble/optimization_strategy.h"
#include "marble/skipping_index.h"
#include <memory>
#include <atomic>

namespace marble {

/**
 * SkippingIndexStrategy - Optimization strategy using skipping indexes
 *
 * Integrates skipping indexes into MarbleDB's read path to prune entire
 * data blocks that cannot contain matching rows.
 *
 * Use cases:
 * - Analytical queries with range predicates (WHERE x BETWEEN a AND b)
 * - Time series queries (WHERE timestamp > start AND timestamp < end)
 * - Sorted or partially sorted data
 * - Any workload that can benefit from block-level statistics
 */
class SkippingIndexStrategy : public OptimizationStrategy {
public:
    /**
     * Create SkippingIndexStrategy.
     *
     * @param block_size_rows Number of rows per block (default: 8192)
     *                        Larger blocks = less overhead but coarser pruning
     *                        Smaller blocks = more overhead but finer pruning
     */
    explicit SkippingIndexStrategy(int64_t block_size_rows = 8192);

    ~SkippingIndexStrategy() override = default;

    //==========================================================================
    // OptimizationStrategy interface
    //==========================================================================

    Status OnTableCreate(const TableCapabilities& caps) override;

    Status OnRead(ReadContext* ctx) override;

    void OnReadComplete(const Key& key, const Record& record) override;

    Status OnWrite(WriteContext* ctx) override;

    Status OnCompaction(CompactionContext* ctx) override;

    Status OnFlush(FlushContext* ctx) override;

    size_t MemoryUsage() const override;

    void Clear() override;

    std::vector<uint8_t> Serialize() const override;

    Status Deserialize(const std::vector<uint8_t>& data) override;

    std::string Name() const override { return "SkippingIndex"; }

    std::string GetStats() const override;

    //==========================================================================
    // Skipping-specific methods
    //==========================================================================

    /**
     * Build skipping index from an Arrow table (typically called on flush)
     */
    Status BuildFromTable(const std::shared_ptr<arrow::Table>& table);

    /**
     * Get candidate blocks for a predicate
     * Returns block IDs that might contain matching rows
     */
    Status GetCandidateBlocks(const std::string& column_name,
                             const std::string& op,
                             const std::shared_ptr<arrow::Scalar>& value,
                             std::vector<int64_t>* candidate_blocks) const;

    /**
     * Get all block statistics (for debugging/analysis)
     */
    const std::vector<SkippingIndex::BlockStats>& GetAllBlocks() const;

    /**
     * Set the underlying skipping index (used during deserialization)
     */
    void SetSkippingIndex(std::unique_ptr<InMemorySkippingIndex> index);

    //==========================================================================
    // Statistics
    //==========================================================================

    struct Stats {
        std::atomic<uint64_t> num_queries{0};        // Total queries processed
        std::atomic<uint64_t> num_blocks_total{0};   // Total blocks checked
        std::atomic<uint64_t> num_blocks_pruned{0};  // Blocks skipped
        std::atomic<uint64_t> num_blocks_scanned{0}; // Blocks that had to be scanned
        std::atomic<uint64_t> num_builds{0};         // Times index was rebuilt
        std::atomic<uint64_t> total_build_ms{0};     // Total time building indexes
    };

    const Stats& GetStatsStruct() const { return stats_; }

private:
    std::unique_ptr<InMemorySkippingIndex> skipping_index_;
    int64_t block_size_rows_;
    Stats stats_;

    // Track table for incremental index builds
    std::shared_ptr<arrow::Schema> schema_;

    // Mutex for thread safety
    mutable std::mutex mutex_;
};

}  // namespace marble
