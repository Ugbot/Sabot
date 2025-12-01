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

#include "marble/status.h"
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <memory>
#include <string>
#include <vector>
#include <cstdint>

namespace marble {

/**
 * @brief Composite key definition for Cassandra/ClickHouse-style tables
 *
 * Defines which columns form the partition key (for distribution) and
 * which columns form the clustering key (for sort order within partition).
 *
 * Example: For a time-series table with columns (sensor_id, timestamp, value):
 *   partition_columns = ["sensor_id"]     // Hash partition by sensor
 *   clustering_columns = ["timestamp"]    // Sort by time within partition
 */
struct CompositeKeyDef {
    std::vector<std::string> partition_columns;   // Hash partitioning (upper 32 bits)
    std::vector<std::string> clustering_columns;  // Sort order within partition (lower 32 bits)

    // Helper: Check if this is a simple single-column key
    bool IsSingleColumn() const {
        return partition_columns.size() == 1 && clustering_columns.empty();
    }

    // Helper: Total number of key columns
    size_t KeyColumnCount() const {
        return partition_columns.size() + clustering_columns.size();
    }

    // Serialize to JSON string
    std::string ToJson() const;

    // Deserialize from JSON string
    static Status FromJson(const std::string& json, CompositeKeyDef* def);
};

// ColumnStatistics is defined in sstable_arrow.h - forward declare here
struct ColumnStatistics;

/**
 * @brief WideTableSchema - Arrow Schema with composite key and column metadata
 *
 * Extends Arrow Schema with:
 * - Composite key definition (partition + clustering columns)
 * - Column encoding to uint64_t keys for LSM storage
 * - Schema validation for RecordBatch inserts
 * - Column projection helpers
 *
 * This enables Cassandra/ClickHouse-style wide tables where:
 * - Partition columns determine data distribution
 * - Clustering columns determine sort order within partition
 * - Value columns are stored columnar with zone maps
 *
 * Note: This is different from the simpler TableSchema in table.h which
 * only handles time-based partitioning. WideTableSchema provides full
 * composite key support for column family-based storage.
 */
class WideTableSchema {
public:
    /**
     * @brief Create table schema with composite key
     *
     * @param arrow_schema Full Arrow schema including all columns
     * @param key_def Composite key definition (partition + clustering)
     */
    WideTableSchema(std::shared_ptr<arrow::Schema> arrow_schema,
                    CompositeKeyDef key_def);

    /**
     * @brief Create table schema with single primary key (legacy compatibility)
     *
     * @param arrow_schema Full Arrow schema
     * @param primary_key_column Name of the primary key column
     */
    WideTableSchema(std::shared_ptr<arrow::Schema> arrow_schema,
                    const std::string& primary_key_column);

    /**
     * @brief Encode a row to composite key for LSM ordering
     *
     * Produces a uint64_t key where:
     * - Upper 32 bits: hash of partition columns (distribution)
     * - Lower 32 bits: encoded clustering columns (sort order)
     *
     * This preserves sort order within partition while enabling
     * efficient partition-level operations.
     *
     * @param batch RecordBatch containing the row
     * @param row_idx Row index to encode
     * @return Encoded uint64_t key
     */
    uint64_t EncodeKey(const arrow::RecordBatch& batch, int64_t row_idx) const;

    /**
     * @brief Encode keys for all rows in a batch
     *
     * @param batch RecordBatch to encode
     * @return UInt64Array of encoded keys
     */
    std::shared_ptr<arrow::UInt64Array> EncodeKeys(const arrow::RecordBatch& batch) const;

    /**
     * @brief Decode composite key back to column values (partial)
     *
     * Note: Full key reconstruction requires partition columns be stored
     * alongside data. This method extracts the clustering portion.
     *
     * @param key Encoded uint64_t key
     * @param clustering_values Output vector for decoded clustering values
     * @return Status indicating success or failure
     */
    Status DecodeClusteringKey(uint64_t key,
                               std::vector<std::shared_ptr<arrow::Scalar>>* clustering_values) const;

    /**
     * @brief Get the Arrow schema
     */
    std::shared_ptr<arrow::Schema> GetArrowSchema() const { return arrow_schema_; }

    /**
     * @brief Get composite key definition
     */
    const CompositeKeyDef& GetKeyDef() const { return key_def_; }

    /**
     * @brief Get indices of partition columns
     */
    const std::vector<int>& GetPartitionColumnIndices() const { return partition_indices_; }

    /**
     * @brief Get indices of clustering columns
     */
    const std::vector<int>& GetClusteringColumnIndices() const { return clustering_indices_; }

    /**
     * @brief Get indices of value (non-key) columns
     */
    const std::vector<int>& GetValueColumnIndices() const { return value_indices_; }

    /**
     * @brief Convert column names to indices
     *
     * @param column_names Names of columns to project
     * @param indices Output vector of column indices
     * @return Status (NotFound if column doesn't exist)
     */
    Status GetColumnIndices(const std::vector<std::string>& column_names,
                           std::vector<int>* indices) const;

    /**
     * @brief Validate that a RecordBatch matches this schema
     *
     * Checks:
     * - Column names match
     * - Column types are compatible
     * - Required columns are present
     *
     * @param batch RecordBatch to validate
     * @return Status OK if valid, error otherwise
     */
    Status ValidateBatch(const arrow::RecordBatch& batch) const;

    /**
     * @brief Check if schema has this column
     */
    bool HasColumn(const std::string& name) const;

    /**
     * @brief Get column type by name
     */
    std::shared_ptr<arrow::DataType> GetColumnType(const std::string& name) const;

    /**
     * @brief Serialize schema to JSON for persistence
     */
    std::string ToJson() const;

    /**
     * @brief Deserialize schema from JSON
     */
    static Status FromJson(const std::string& json,
                          std::shared_ptr<WideTableSchema>* schema);

private:
    std::shared_ptr<arrow::Schema> arrow_schema_;
    CompositeKeyDef key_def_;

    // Cached column indices
    std::vector<int> partition_indices_;
    std::vector<int> clustering_indices_;
    std::vector<int> value_indices_;

    // Build index caches from key definition
    void BuildColumnIndices();

    // Key encoding helpers
    uint32_t HashPartitionColumns(const arrow::RecordBatch& batch, int64_t row_idx) const;
    uint32_t EncodeClusteringColumns(const arrow::RecordBatch& batch, int64_t row_idx) const;

    // Hash combine helper (similar to boost::hash_combine)
    static uint32_t HashCombine(uint32_t seed, uint32_t value);

    // Encode a scalar value to bytes that preserve sort order
    static uint32_t EncodeScalarForSort(const std::shared_ptr<arrow::Scalar>& scalar);
};

/**
 * @brief ScanOptions for wide-table queries
 *
 * Supports column projection and predicate pushdown for efficient queries.
 */
struct ScanOptions {
    uint64_t start_key = 0;
    uint64_t end_key = UINT64_MAX;

    // Column projection - empty means all columns
    std::vector<std::string> columns;

    // Predicate pushdown - Arrow compute expression
    std::shared_ptr<arrow::compute::Expression> filter;

    // Result limit (-1 = no limit)
    int64_t limit = -1;

    // Skip N rows before returning results
    int64_t offset = 0;

    ScanOptions() = default;

    // Builder-style methods for fluent API
    ScanOptions& WithKeyRange(uint64_t start, uint64_t end) {
        start_key = start;
        end_key = end;
        return *this;
    }

    ScanOptions& WithColumns(std::vector<std::string> cols) {
        columns = std::move(cols);
        return *this;
    }

    ScanOptions& WithFilter(std::shared_ptr<arrow::compute::Expression> f) {
        filter = std::move(f);
        return *this;
    }

    ScanOptions& WithLimit(int64_t lim) {
        limit = lim;
        return *this;
    }

    ScanOptions& WithOffset(int64_t off) {
        offset = off;
        return *this;
    }
};

} // namespace marble
