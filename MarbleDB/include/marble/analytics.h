/************************************************************************
Copyright 2024 MarbleDB Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
**************************************************************************/

#pragma once

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <arrow/api.h>
#include <marble/status.h>
#include <marble/table.h>
#include <marble/skipping_index.h>

namespace marble {

// Forward declarations
class QueryResult;

/**
 * @brief Zone map for a column - stores min/max/null statistics
 */
struct ZoneMap {
    std::string column_name;
    std::shared_ptr<arrow::DataType> column_type;

    // Statistics
    std::shared_ptr<arrow::Scalar> min_value;
    std::shared_ptr<arrow::Scalar> max_value;
    int64_t null_count = 0;
    int64_t row_count = 0;

    // For numeric types, additional quantiles
    std::vector<std::shared_ptr<arrow::Scalar>> quantiles;

    ZoneMap() = default;
    ZoneMap(const std::string& name, std::shared_ptr<arrow::DataType> type)
        : column_name(name), column_type(type) {}

    /**
     * @brief Check if this zone map can satisfy a predicate
     */
    bool CanPrune(const std::string& predicate_op,
                  const std::shared_ptr<arrow::Scalar>& predicate_value) const;
};

/**
 * @brief Bloom filter for membership testing
 */
class BloomFilter {
public:
    BloomFilter(size_t expected_items, double false_positive_rate = 0.01);
    ~BloomFilter();

    /**
     * @brief Add an item to the filter
     */
    void Add(const std::string& item);

    /**
     * @brief Test if an item might be in the filter
     */
    bool MightContain(const std::string& item) const;

    /**
     * @brief Get the size of the filter in bytes
     */
    size_t SizeBytes() const;

    /**
     * @brief Serialize the filter to bytes
     */
    std::vector<uint8_t> Serialize() const;

    /**
     * @brief Deserialize a filter from bytes
     */
    static std::unique_ptr<BloomFilter> Deserialize(const std::vector<uint8_t>& data);

    /**
     * @brief Get the number of items added to the filter
     */
    size_t ItemsAdded() const;

    /**
     * @brief Get the capacity (expected items) of the filter
     */
    size_t Capacity() const;

    /**
     * @brief Get the load factor (items_added / capacity)
     */
    double LoadFactor() const;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

/**
 * @brief Column index combining zone map and bloom filter
 */
struct ColumnIndex {
    std::string column_name;
    ZoneMap zone_map;
    std::shared_ptr<BloomFilter> bloom_filter;

    ColumnIndex() = default;
    ColumnIndex(const std::string& name) : column_name(name) {}

    /**
     * @brief Build index from an Arrow array
     */
    Status BuildFromArray(const arrow::Array& array);
};

/**
 * @brief Partition metadata with indexes
 */
struct PartitionMetadata {
    std::string partition_id;
    int64_t row_count = 0;
    int64_t size_bytes = 0;
    int64_t min_timestamp = INT64_MAX;
    int64_t max_timestamp = INT64_MIN;

    // Column indexes
    std::unordered_map<std::string, ColumnIndex> column_indexes;

    // File paths
    std::vector<std::string> data_files;

    // Skipping index for data skipping optimization
    std::shared_ptr<SkippingIndex> skipping_index;

    PartitionMetadata() = default;
    PartitionMetadata(const std::string& id) : partition_id(id) {}

    /**
     * @brief Check if partition can be pruned for a given scan spec
     */
    bool CanPrune(const ScanSpec& spec) const;

    /**
     * @brief Serialize metadata to JSON
     */
    std::string ToJson() const;

    /**
     * @brief Deserialize metadata from JSON
     */
    static std::unique_ptr<PartitionMetadata> FromJson(const std::string& json);
};

/**
 * @brief Query optimizer that uses indexes for pruning
 */
class QueryOptimizer {
public:
    QueryOptimizer() = default;

    /**
     * @brief Optimize a scan spec using partition metadata
     */
    Status OptimizeScan(const ScanSpec& original_spec,
                       const std::vector<PartitionMetadata>& partitions,
                       ScanSpec* optimized_spec,
                       std::vector<std::string>* selected_partitions) const;

    /**
     * @brief Get candidate data blocks using skipping index
     */
    Status GetCandidateBlocks(const PartitionMetadata& partition,
                             const std::string& column_name,
                             const std::string& op,
                             const std::shared_ptr<arrow::Scalar>& value,
                             std::vector<int64_t>* candidate_blocks) const;

    /**
     * @brief Estimate the selectivity of a scan spec
     */
    double EstimateSelectivity(const ScanSpec& spec,
                              const PartitionMetadata& partition) const;

private:
    /**
     * @brief Parse simple filter expressions
     */
    Status ParseFilter(const std::string& filter_expr,
                      std::string* column,
                      std::string* op,
                      std::string* value) const;
};

/**
 * @brief Vectorized aggregation engine
 */
class AggregationEngine {
public:
    AggregationEngine() = default;

    /**
     * @brief Supported aggregation functions
     */
    enum class AggFunction {
        kCount,
        kSum,
        kAvg,
        kMin,
        kMax,
        kCountDistinct
    };

    /**
     * @brief Aggregation specification
     */
    struct AggSpec {
        AggFunction function;
        std::string input_column;
        std::string output_column;
        std::vector<std::string> group_by_columns;  // For GROUP BY

        AggSpec(AggFunction func, const std::string& input, const std::string& output)
            : function(func), input_column(input), output_column(output) {}
    };

    /**
     * @brief Execute aggregations on an Arrow table
     */
    Status Execute(const std::vector<AggSpec>& specs,
                  const std::shared_ptr<arrow::Table>& input_table,
                  std::shared_ptr<arrow::Table>* output_table) const;

private:
    /**
     * @brief Execute COUNT aggregation
     */
    Status ExecuteCount(const AggSpec& spec,
                       const std::shared_ptr<arrow::Table>& table,
                       std::shared_ptr<arrow::Array>* result) const;

    /**
     * @brief Execute SUM aggregation
     */
    Status ExecuteSum(const AggSpec& spec,
                     const std::shared_ptr<arrow::Table>& table,
                     std::shared_ptr<arrow::Array>* result) const;

    /**
     * @brief Execute AVG aggregation
     */
    Status ExecuteAvg(const AggSpec& spec,
                     const std::shared_ptr<arrow::Table>& table,
                     std::shared_ptr<arrow::Array>* result) const;
};

/**
 * @brief Enhanced scan specification with aggregations
 */
struct AnalyticalScanSpec : public ScanSpec {
    // Aggregation specifications
    std::vector<AggregationEngine::AggSpec> aggregations;

    // Sorting specifications (future)
    std::vector<std::pair<std::string, bool>> sort_columns;  // column -> ascending

    // Limit with offset (future)
    int64_t offset = 0;

    AnalyticalScanSpec() = default;
};

/**
 * @brief Analytical query result with performance metrics
 */
struct AnalyticalQueryResult : public QueryResult {
    // Performance metrics
    int64_t partitions_scanned = 0;
    int64_t partitions_pruned = 0;
    int64_t rows_scanned = 0;
    int64_t rows_filtered = 0;
    int64_t bytes_scanned = 0;
    double scan_time_ms = 0.0;
    double filter_time_ms = 0.0;
    double aggregation_time_ms = 0.0;

    AnalyticalQueryResult() = default;
};

// Factory functions
std::unique_ptr<BloomFilter> CreateBloomFilter(size_t expected_items,
                                              double false_positive_rate = 0.01);

} // namespace marble
