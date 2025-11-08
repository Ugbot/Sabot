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
#include <queue>
#include <vector>

#include "marble/sstable.h"
#include "marble/sstable_arrow.h"
#include "marble/status.h"

namespace marble {
namespace arrow_api {

/**
 * @brief Simple iterator over a vector of pre-loaded Arrow batches
 *
 * Used for old-format SSTables that store serialized Arrow batches as values.
 * Loads all batches into memory and provides sequential iteration.
 *
 * This is a compatibility layer - native Arrow-format SSTables use
 * streaming iteration directly from disk.
 */
class VectorBatchIterator : public ArrowRecordBatchIterator {
public:
    /**
     * @brief Create iterator from vector of batches
     *
     * @param batches Pre-loaded Arrow RecordBatches
     * @param schema Arrow schema for batches
     */
    VectorBatchIterator(
        std::vector<std::shared_ptr<::arrow::RecordBatch>> batches,
        std::shared_ptr<::arrow::Schema> schema);

    ~VectorBatchIterator() override = default;

    // ArrowRecordBatchIterator interface
    bool HasNext() const override;
    Status Next(std::shared_ptr<::arrow::RecordBatch>* batch) override;
    std::shared_ptr<::arrow::Schema> schema() const override;
    int64_t EstimatedRemainingRows() const override;
    Status SkipRows(int64_t num_rows) override;

private:
    std::vector<std::shared_ptr<::arrow::RecordBatch>> batches_;
    std::shared_ptr<::arrow::Schema> schema_;
    size_t current_idx_;
};

/**
 * @brief LSM-aware RecordBatch iterator with K-way merge and deduplication
 *
 * This iterator scans across all LSM levels, performing:
 * 1. Multi-level iteration (L0 through L6)
 * 2. K-way merge (merge batches from multiple SSTables)
 * 3. Deduplication (same key in multiple levels → take newest)
 * 4. Predicate pushdown (bloom filters, zone maps)
 *
 * Key Features:
 * - Streaming execution (constant memory, no full materialize)
 * - Priority queue for efficient K-way merge
 * - Lazy SSTable opening (only open when needed)
 * - Bloom filter pruning (skip entire SSTables)
 * - Zone map pruning (skip RecordBatches)
 *
 * Merge Algorithm:
 * - L0 SSTables may have overlapping keys (read all in parallel)
 * - L1-L6 SSTables are non-overlapping (sequential read per level)
 * - Use min-heap to merge rows across all active SSTables
 * - When same key appears multiple times, take version from higher level (newer)
 *
 * Usage:
 * ```
 * auto iterator = LSMBatchIterator::Create(
 *     sstables,
 *     schema,
 *     projection,
 *     predicates
 * );
 *
 * while (iterator->HasNext()) {
 *     std::shared_ptr<arrow::RecordBatch> batch;
 *     ARROW_RETURN_NOT_OK(iterator->Next(&batch));
 *     ProcessBatch(batch);
 * }
 * ```
 */
class LSMBatchIterator : public ArrowRecordBatchIterator {
public:
    /**
     * @brief Create LSM batch iterator
     *
     * @param sstables SSTables organized by level (L0 at index 0, etc.)
     * @param schema Arrow schema for output batches
     * @param projection Column names to project (empty = all columns)
     * @param predicates Predicates for pushdown
     * @param batch_size Rows per output batch
     * @return Iterator or error
     */
    static Status Create(
        const std::vector<std::vector<std::shared_ptr<SSTable>>>& sstables,
        std::shared_ptr<::arrow::Schema> schema,
        const std::vector<std::string>& projection,
        const std::vector<ColumnPredicate>& predicates,
        int64_t batch_size,
        std::unique_ptr<LSMBatchIterator>* iterator);

    ~LSMBatchIterator() override;

    // ArrowRecordBatchIterator interface

    bool HasNext() const override;
    Status Next(std::shared_ptr<::arrow::RecordBatch>* batch) override;
    std::shared_ptr<::arrow::Schema> schema() const override;
    int64_t EstimatedRemainingRows() const override;
    Status SkipRows(int64_t num_rows) override;

    /**
     * @brief Get statistics for performance monitoring
     */
    struct Stats {
        uint64_t sstables_opened = 0;      // SSTables opened
        uint64_t sstables_skipped = 0;     // SSTables skipped (bloom filter)
        uint64_t batches_read = 0;         // RecordBatches read from disk
        uint64_t batches_skipped = 0;      // RecordBatches skipped (zone maps)
        uint64_t rows_read = 0;            // Total rows read
        uint64_t rows_deduplicated = 0;    // Rows removed by deduplication
        uint64_t merge_operations = 0;     // K-way merge iterations
    };

    Stats GetStats() const { return stats_; }

private:
    // Private constructor (use Create factory)
    LSMBatchIterator(
        std::shared_ptr<::arrow::Schema> schema,
        const std::vector<std::string>& projection,
        const std::vector<ColumnPredicate>& predicates,
        int64_t batch_size);

    // Iterator state for one SSTable
    struct SSTableIteratorState {
        std::shared_ptr<SSTable> sstable;
        std::unique_ptr<ArrowRecordBatchIterator> iterator;
        int level;                                           // LSM level (0-6)
        std::shared_ptr<::arrow::RecordBatch> current_batch; // Current batch
        int64_t row_idx;                                     // Row index in current batch
        uint64_t current_key;                                // Current key value

        // For min-heap ordering (higher level = higher priority for same key)
        struct Compare {
            bool operator()(const std::shared_ptr<SSTableIteratorState>& a,
                           const std::shared_ptr<SSTableIteratorState>& b) const {
                // First compare by key (ascending)
                if (a->current_key != b->current_key) {
                    return a->current_key > b->current_key;  // Min-heap
                }
                // Same key: higher level has priority (L0 > L1 > ... > L6)
                return a->level > b->level;  // L0 comes before L1
            }
        };

        // Advance to next row in this SSTable
        Status Advance();
        bool HasNext() const;
    };

    // Configuration
    std::shared_ptr<::arrow::Schema> schema_;
    std::vector<std::string> projection_;
    std::vector<ColumnPredicate> predicates_;
    int64_t batch_size_;

    // SSTable sources (organized by level)
    std::vector<std::vector<std::shared_ptr<SSTable>>> sstables_;

    // Active iterators (min-heap for K-way merge)
    using IteratorHeap = std::priority_queue<
        std::shared_ptr<SSTableIteratorState>,
        std::vector<std::shared_ptr<SSTableIteratorState>>,
        SSTableIteratorState::Compare>;

    IteratorHeap active_iterators_;

    // Output buffering
    std::vector<std::shared_ptr<::arrow::Array>> output_columns_;
    int64_t output_row_count_;

    // State
    bool initialized_;
    bool finished_;
    uint64_t last_emitted_key_;  // For deduplication

    // Statistics
    Stats stats_;

    // Initialization
    Status Initialize(
        const std::vector<std::vector<std::shared_ptr<SSTable>>>& sstables);

    // Iterator management
    Status OpenSSTable(std::shared_ptr<SSTable> sstable, int level,
                      std::shared_ptr<SSTableIteratorState>* state);
    bool ShouldSkipSSTable(std::shared_ptr<SSTable> sstable);
    bool ShouldSkipBatch(const std::shared_ptr<::arrow::RecordBatch>& batch);

    // K-way merge
    Status FillOutputBatch(std::shared_ptr<::arrow::RecordBatch>* batch);
    Status AppendRow(const std::shared_ptr<SSTableIteratorState>& state);
    Status FlushOutputBatch(std::shared_ptr<::arrow::RecordBatch>* batch);

    // Deduplication
    bool ShouldDeduplicateRow(uint64_t key);
};

}  // namespace arrow_api
}  // namespace marble
