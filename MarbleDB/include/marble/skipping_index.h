#pragma once

#include <memory>
#include <vector>
#include <string>
#include <unordered_map>
#include <arrow/api.h>
#include <marble/status.h>

namespace marble {

/**
 * @brief Skipping Index - ClickHouse-inspired data skipping for analytical queries
 *
 * Skipping indexes allow queries to skip large portions of data by storing
 * aggregated statistics (min, max, count, etc.) for data blocks.
 * This is similar to ClickHouse's skipping indexes and provides massive
 * performance improvements for analytical workloads.
 */
class SkippingIndex {
public:
    /**
     * @brief Statistics stored in each skip block
     */
    struct BlockStats {
        int64_t block_id;
        int64_t row_offset;     // Starting row offset
        int64_t row_count;      // Number of rows in block
        int64_t byte_offset;    // Starting byte offset in file
        int64_t byte_count;     // Number of bytes in block

        // Per-column statistics
        std::unordered_map<std::string, std::shared_ptr<arrow::Scalar>> min_values;
        std::unordered_map<std::string, std::shared_ptr<arrow::Scalar>> max_values;
        std::unordered_map<std::string, int64_t> null_counts;
        std::unordered_map<std::string, int64_t> distinct_counts;

        // Time series specific (for timestamp columns)
        int64_t min_timestamp = INT64_MAX;
        int64_t max_timestamp = INT64_MIN;
        int64_t timestamp_count = 0;

        BlockStats() = default;
        BlockStats(int64_t id, int64_t row_start, int64_t row_cnt)
            : block_id(id), row_offset(row_start), row_count(row_cnt) {}
    };

    SkippingIndex() = default;
    virtual ~SkippingIndex() = default;

    /**
     * @brief Build skipping index from Arrow table
     */
    virtual Status BuildFromTable(const std::shared_ptr<arrow::Table>& table,
                                 int64_t block_size_rows = 8192) = 0;

    /**
     * @brief Get blocks that can satisfy a predicate
     */
    virtual Status GetCandidateBlocks(const std::string& column_name,
                                     const std::string& op,
                                     const std::shared_ptr<arrow::Scalar>& value,
                                     std::vector<int64_t>* candidate_blocks) const = 0;

    /**
     * @brief Get all block statistics
     */
    virtual const std::vector<BlockStats>& GetAllBlocks() const = 0;

    /**
     * @brief Get block statistics by ID
     */
    virtual Status GetBlockStats(int64_t block_id, BlockStats* stats) const = 0;

    /**
     * @brief Serialize index to bytes
     */
    virtual std::vector<uint8_t> Serialize() const = 0;

    /**
     * @brief Deserialize index from bytes
     */
    virtual Status Deserialize(const std::vector<uint8_t>& data) = 0;

    /**
     * @brief Get memory usage in bytes
     */
    virtual size_t MemoryUsage() const = 0;
};

/**
 * @brief In-memory skipping index implementation
 */
class InMemorySkippingIndex : public SkippingIndex {
public:
    InMemorySkippingIndex() = default;

    Status BuildFromTable(const std::shared_ptr<arrow::Table>& table,
                         int64_t block_size_rows) override;

    Status GetCandidateBlocks(const std::string& column_name,
                             const std::string& op,
                             const std::shared_ptr<arrow::Scalar>& value,
                             std::vector<int64_t>* candidate_blocks) const override;

    const std::vector<BlockStats>& GetAllBlocks() const override { return blocks_; }

    Status GetBlockStats(int64_t block_id, BlockStats* stats) const override;

    std::vector<uint8_t> Serialize() const override;
    Status Deserialize(const std::vector<uint8_t>& data) override;

    size_t MemoryUsage() const override;

private:
    std::vector<BlockStats> blocks_;

    /**
     * @brief Compute statistics for a block of rows
     */
    Status ComputeBlockStats(const std::shared_ptr<arrow::Table>& table,
                           int64_t start_row,
                           int64_t end_row,
                           BlockStats* stats);

    /**
     * @brief Check if a block can satisfy a predicate
     */
    bool CanBlockSatisfy(const BlockStats& block,
                        const std::string& column_name,
                        const std::string& op,
                        const std::shared_ptr<arrow::Scalar>& value) const;
};

/**
 * @brief Granular Skipping Index - stores statistics at multiple granularities
 *
 * Similar to ClickHouse's granular skipping indexes, this maintains
 * statistics at different levels (blocks, granules, etc.) for
 * optimal query performance.
 */
class GranularSkippingIndex : public SkippingIndex {
public:
    /**
     * @brief Granule statistics (finer granularity than blocks)
     */
    struct GranuleStats {
        int64_t granule_id;
        int64_t block_id;
        int64_t row_offset;
        int64_t row_count;

        // Per-column stats for this granule
        std::unordered_map<std::string, std::shared_ptr<arrow::Scalar>> min_values;
        std::unordered_map<std::string, std::shared_ptr<arrow::Scalar>> max_values;
    };

    GranularSkippingIndex() = default;

    Status BuildFromTable(const std::shared_ptr<arrow::Table>& table,
                         int64_t block_size_rows) override;

    Status GetCandidateBlocks(const std::string& column_name,
                             const std::string& op,
                             const std::shared_ptr<arrow::Scalar>& value,
                             std::vector<int64_t>* candidate_blocks) const override;

    const std::vector<BlockStats>& GetAllBlocks() const override { return blocks_; }

    Status GetBlockStats(int64_t block_id, BlockStats* stats) const override;

    std::vector<uint8_t> Serialize() const override;
    Status Deserialize(const std::vector<uint8_t>& data) override;

    size_t MemoryUsage() const override;

    /**
     * @brief Get granules for a block
     */
    const std::vector<GranuleStats>& GetGranulesForBlock(int64_t block_id) const;

private:
    std::vector<BlockStats> blocks_;
    std::unordered_map<int64_t, std::vector<GranuleStats>> granules_; // block_id -> granules

    int64_t granule_size_rows_ = 1024; // Finer granularity
};

/**
 * @brief Time Series Skipping Index - optimized for time-based queries
 *
 * Specialized skipping index for time series data that understands
 * temporal patterns and provides efficient time range filtering.
 */
class TimeSeriesSkippingIndex : public InMemorySkippingIndex {
public:
    /**
     * @brief Time bucket statistics
     */
    struct TimeBucketStats {
        int64_t bucket_id;
        int64_t start_timestamp;
        int64_t end_timestamp;
        int64_t row_count;
        int64_t byte_count;

        // Value statistics (assuming numeric values)
        double min_value = std::numeric_limits<double>::max();
        double max_value = std::numeric_limits<double>::lowest();
        double avg_value = 0.0;
        int64_t value_count = 0;
    };

    TimeSeriesSkippingIndex() = default;

    Status BuildFromTable(const std::shared_ptr<arrow::Table>& table,
                         int64_t block_size_rows) override;

    Status GetCandidateBlocks(const std::string& column_name,
                             const std::string& op,
                             const std::shared_ptr<arrow::Scalar>& value,
                             std::vector<int64_t>* candidate_blocks) const override;

    const std::vector<BlockStats>& GetAllBlocks() const override { return blocks_; }

    Status GetBlockStats(int64_t block_id, BlockStats* stats) const override;

    std::vector<uint8_t> Serialize() const override;
    Status Deserialize(const std::vector<uint8_t>& data) override;

    size_t MemoryUsage() const override;

    /**
     * @brief Get time buckets that overlap with a time range
     */
    Status GetTimeBuckets(int64_t start_time, int64_t end_time,
                         std::vector<TimeBucketStats>* buckets) const;

private:
    std::vector<BlockStats> blocks_;
    std::vector<TimeBucketStats> time_buckets_;
    std::string time_column_;
    std::string value_column_;
};

/**
 * @brief Skipping Index Manager - manages multiple skipping indexes
 */
class SkippingIndexManager {
public:
    SkippingIndexManager() = default;

    /**
     * @brief Create and build a skipping index for a table
     */
    Status CreateSkippingIndex(const std::string& table_name,
                              const std::shared_ptr<arrow::Table>& table,
                              SkippingIndex** index);

    /**
     * @brief Get existing skipping index for a table
     */
    Status GetSkippingIndex(const std::string& table_name,
                           SkippingIndex** index) const;

    /**
     * @brief Remove skipping index for a table
     */
    Status RemoveSkippingIndex(const std::string& table_name);

    /**
     * @brief List all tables with skipping indexes
     */
    std::vector<std::string> ListIndexedTables() const;

    /**
     * @brief Get total memory usage of all indexes
     */
    size_t TotalMemoryUsage() const;

    /**
     * @brief Optimize query using skipping indexes
     */
    Status OptimizeQueryWithSkipping(const std::string& table_name,
                                    const std::string& column_name,
                                    const std::string& op,
                                    const std::shared_ptr<arrow::Scalar>& value,
                                    std::vector<int64_t>* candidate_blocks);

private:
    std::unordered_map<std::string, std::unique_ptr<SkippingIndex>> indexes_;
};

// Factory functions
std::unique_ptr<SkippingIndex> CreateInMemorySkippingIndex();
std::unique_ptr<SkippingIndex> CreateGranularSkippingIndex();
std::unique_ptr<SkippingIndex> CreateTimeSeriesSkippingIndex();
std::unique_ptr<SkippingIndexManager> CreateSkippingIndexManager();

} // namespace marble
