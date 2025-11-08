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

#include <arrow/api.h>
#include <arrow/record_batch.h>
#include <memory>
#include <string>
#include <vector>

#include "marble/db.h"
#include "marble/status.h"
#include "marble/sstable_arrow.h"
#include "marble/arrow/lsm_iterator.h"

namespace marble {

// Forward declarations
class MarbleDB;

namespace arrow_api {

/**
 * @brief Standard Arrow RecordBatchReader implementation for MarbleDB
 *
 * This class provides a standard Apache Arrow RecordBatchReader interface
 * for reading data from MarbleDB tables. It enables zero-copy integration
 * with the entire Arrow ecosystem (Acero, DataFusion, DuckDB, PyArrow).
 *
 * Features:
 * - Standard Arrow API (RecordBatchReader)
 * - Column projection (read only needed columns)
 * - Predicate pushdown (bloom filters, zone maps)
 * - Streaming execution (constant memory)
 * - LSM-aware iteration (multi-level merge with deduplication)
 *
 * Usage:
 * ```
 * // Open database
 * auto db = marble::DB::Open("my_database");
 *
 * // Create reader with projection and filter
 * std::vector<std::string> columns = {"customer_id", "amount"};
 * auto reader = marble::arrow_api::OpenTable(db, "orders", columns);
 *
 * // Iterate batches (standard Arrow API)
 * while (true) {
 *     std::shared_ptr<arrow::RecordBatch> batch;
 *     ARROW_RETURN_NOT_OK(reader->ReadNext(&batch));
 *     if (!batch) break;  // End of stream
 *
 *     // Use batch with Acero, DataFusion, DuckDB, etc.
 *     ProcessBatch(batch);
 * }
 * ```
 *
 * Sabot Integration:
 * - Graph storage: Read node/edge tables as Arrow batches
 * - RDF storage: Read SPO/POS/OSP indexes as Arrow batches
 * - State backend: Read state ranges as Arrow batches
 * - Query engines: Pass to Acero/DataFusion for hash joins, aggregations
 */
class MarbleRecordBatchReader : public ::arrow::RecordBatchReader {
public:
    /**
     * @brief Construct reader from MarbleDB table
     *
     * @param db MarbleDB database instance
     * @param table_name Name of table to read
     * @param projection Column names to read (empty = all columns)
     * @param predicates Predicates for pushdown (optional)
     */
    MarbleRecordBatchReader(
        std::shared_ptr<MarbleDB> db,
        const std::string& table_name,
        const std::vector<std::string>& projection = {},
        const std::vector<ColumnPredicate>& predicates = {});

    ~MarbleRecordBatchReader() override;

    // Arrow RecordBatchReader interface

    /**
     * @brief Get the schema of returned batches
     *
     * @return Schema with projected columns
     */
    std::shared_ptr<::arrow::Schema> schema() const override;

    /**
     * @brief Read next RecordBatch
     *
     * Returns null when stream is exhausted.
     * Handles LSM merge and deduplication transparently.
     *
     * @param[out] batch Next RecordBatch (null at end of stream)
     * @return Status OK on success
     */
    ::arrow::Status ReadNext(std::shared_ptr<::arrow::RecordBatch>* batch) override;

    /**
     * @brief Get statistics for performance monitoring
     */
    struct Stats {
        uint64_t batches_read = 0;
        uint64_t rows_read = 0;
        uint64_t bytes_read = 0;
        uint64_t bloom_filter_skips = 0;  // SSTables skipped by bloom filters
        uint64_t zone_map_skips = 0;      // Batches skipped by zone maps
        uint64_t total_sstables = 0;      // Total SSTables opened
        uint64_t lsm_merges = 0;          // Multi-level merges
    };

    Stats GetStats() const { return stats_; }

    /**
     * @brief Initialize the reader (validates projection, loads schema)
     *
     * Must be called before ReadNext(). Validates that requested columns exist.
     *
     * @return Status OK on success, Invalid if columns don't exist
     */
    ::arrow::Status Initialize();

private:
    // MarbleDB state
    std::shared_ptr<MarbleDB> db_;
    std::string table_name_;
    std::vector<std::string> projection_;
    std::vector<ColumnPredicate> predicates_;

    // Arrow state
    std::shared_ptr<::arrow::Schema> schema_;

    // Iterator state
    std::unique_ptr<ArrowRecordBatchIterator> iterator_;
    bool finished_ = false;

    // Statistics
    Stats stats_;

    // Helper methods
    ::arrow::Status CreateIterator();
    ::arrow::Status LoadSchema();
};

/**
 * @brief Factory function to open MarbleDB table as Arrow RecordBatchReader
 *
 * Convenience function for creating readers.
 *
 * @param db MarbleDB database instance
 * @param table_name Name of table to read
 * @param projection Column names to read (empty = all columns)
 * @param predicates Predicates for pushdown (optional)
 * @return RecordBatchReader or error
 */
::arrow::Result<std::shared_ptr<::arrow::RecordBatchReader>> OpenTable(
    std::shared_ptr<MarbleDB> db,
    const std::string& table_name,
    const std::vector<std::string>& projection = {},
    const std::vector<ColumnPredicate>& predicates = {});

/**
 * @brief Options for scanning MarbleDB tables
 *
 * Used for more advanced scan configurations.
 */
struct ScanOptions {
    // Column projection
    std::vector<std::string> columns;

    // Predicates for pushdown
    std::vector<ColumnPredicate> predicates;

    // Batch size (rows per RecordBatch)
    int64_t batch_size = 10000;

    // Enable bloom filter pruning
    bool use_bloom_filter = true;

    // Enable zone map pruning
    bool use_zone_maps = true;

    // Enable hot key cache
    bool use_hot_key_cache = true;

    // Memory limit for reader (bytes)
    int64_t memory_limit = 100 * 1024 * 1024;  // 100 MB default
};

/**
 * @brief Open table with advanced options
 *
 * @param db MarbleDB database instance
 * @param table_name Name of table to read
 * @param options Scan options
 * @return RecordBatchReader or error
 */
::arrow::Result<std::shared_ptr<::arrow::RecordBatchReader>> OpenTableWithOptions(
    std::shared_ptr<MarbleDB> db,
    const std::string& table_name,
    const ScanOptions& options);

}  // namespace arrow_api
}  // namespace marble
